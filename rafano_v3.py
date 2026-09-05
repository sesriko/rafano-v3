"""
RAFANO V3 - FINAL: Arjum Pro + Pro Chart Generator
Gabungan:
- Screener/latest + broker-accumulation + broker-summary (REAL DATA)
- Chart Generator V1 (candlestick custom, EMA 8/21/50/125, dashboard kiri-kanan)
- Telegram Listener + Auto Screener Loop
"""
import os
import time
import logging
import datetime
import threading
import requests
import pytz
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.gridspec as gridspec
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()

# ========== COLAB FIX - ANTI JSON ERROR ==========
# JANGAN pakai userdata.get di top-level, nanti Colab JS error kalau secret ada "
# Kita pakai os.getenv saja, Colab akan auto-inject secrets sebagai env var
def safe_get_env(key):
    v = os.getenv(key)
    if v:
        # strip quotes jika user isi pakai "
        v = str(v).strip()
        if len(v)>=2 and ((v[0]=='"' and v[-1]=='"') or (v[0]=="'" and v[-1]=="'")):
            v = v[1:-1].strip()
        return v
    # fallback coba userdata tapi dengan try catch halus
    try:
        from google.colab import userdata
        vv = userdata.get(key)
        if vv:
            vv = str(vv).strip().strip('"').strip("'")
            os.environ[key] = vv
            return vv
    except Exception as ee:
        # jangan print error JSON disini, biar gak crash
        pass
    return None

TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')

TELEGRAM_BOT_TOKEN = safe_get_env("TELEGRAM_BOT_TOKEN")
TARGET_CHAT_ID = safe_get_env("TARGET_CHAT_ID")
ARJUM_API_KEY = safe_get_env("ARJUM_API_KEY")

# Debug aman tanpa userdata
print(f"🔑 ENV Loaded - TOKEN exists={bool(TELEGRAM_BOT_TOKEN)} len={len(TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else 0}, CHAT_ID={TARGET_CHAT_ID}, ARJUM len={len(ARJUM_API_KEY) if ARJUM_API_KEY else 0}")
if not TELEGRAM_BOT_TOKEN:
    print("❌ FATAL: TELEGRAM_BOT_TOKEN KOSONG!")
    print("   -> Colab > Secrets > isi TANPA tanda petik, toggle ON Notebook access")
    print("   -> Atau hardcode di cell: os.environ['TELEGRAM_BOT_TOKEN']='token'")

ARJUM_BASE = "https://stock.arjum.com/api"
def get_arjum_headers():
    k = os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or safe_get_env("ARJUM_API_KEY") or ""
    return {"X-API-Key": k.strip(), "Accept": "application/json", "User-Agent": "Mozilla/5.0"}
HEADERS_ARJUM = get_arjum_headers()
HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_now_wib():
    return datetime.datetime.now(TIMEZONE_WIB)

# ========== HELPERS UMUM ==========
def safe_int(val, default=0):
    try:
        if pd.isna(val) or np.isinf(val):
            return default
        return int(val)
    except:
        return default

def format_large_number(val, show_sign=False):
    if pd.isna(val) or val == 0:
        return "0"
    abs_val = abs(val)
    sign = "+" if (show_sign and val > 0) else ("-" if val < 0 else "")
    if abs_val >= 1_000_000_000:
        return f"{sign}{abs_val / 1_000_000_000:.2f}B"
    elif abs_val >= 1_000_000:
        return f"{sign}{abs_val / 1_000_000:,.0f}M"
    elif abs_val >= 1_000:
        return f"{sign}{abs_val / 1_000:,.0f}K"
    else:
        return f"{sign}{val:,.0f}"

def round_to_ihsg_fraction(price):
    if pd.isna(price) or price <= 0:
        return 0
    price = float(price)
    if price < 200:
        tick = 1
    elif price < 500:
        tick = 2
    elif price < 2000:
        tick = 5
    elif price < 5000:
        tick = 10
    else:
        tick = 25
    return int(round(price / tick) * tick)

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, 0.00001)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(50)

def calculate_atr(df, period=14):
    high, low, close = df['High'], df['Low'], df['Close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=1).mean()

def calculate_vsa_metrics(df):
    """
    OKE SAHAM style VSA - Power Buy = dana besar masuk
    Buy% = (Close - Low)/(High - Low) -> posisi close di range
    Kalau close di atas = akumulasi, di bawah = distribusi
    + boost kalau volume > rata2 (power besar)
    """
    price_range = (df['High'] - df['Low']).replace(0, 0.1)
    # OKE SAHAM original: Buy% berdasarkan posisi Close di candle range
    close_pos = (df['Close'] - df['Low']) / price_range  # 0 = di low, 1 = di high
    close_pos = np.clip(close_pos, 0.05, 0.95)
    
    # Base buy ratio dari close position
    buy_ratio = 0.30 + close_pos * 0.60  # 30%-90% range, kalau close di high = 90% buy
    
    # Boost kalau volume besar + candle hijau (power buy)
    if 'V1' in df.columns:
        vol_ratio = df['Volume'] / df['V1'].replace(0, 1)
        # Jika volume > 1.5x rata2 dan candle hijau, tambah power
        is_green = df['Close'] >= df['Open']
        boost = np.where((vol_ratio > 1.5) & is_green, 0.10, 0)
        boost += np.where((vol_ratio > 2.5) & is_green, 0.10, 0)  # TURBO boost
        buy_ratio = buy_ratio + boost
    
    buy_ratio = np.clip(buy_ratio, 0.05, 0.95)
    
    df['Vol_Buy'] = df['Volume'] * buy_ratio
    df['Vol_Sell'] = df['Volume'] - df['Vol_Buy']
    df['Net_Vol_VSA'] = df['Vol_Buy'] - df['Vol_Sell']
    df['Net_Val_VSA'] = df['Net_Vol_VSA'] * df['Close']
    df['Buy_Pct'] = buy_ratio * 100
    return df, buy_ratio

def calculate_buy_signal_strength(df):
    if len(df) < 20:
        return 0, "NO DATA"
    last_row = df.iloc[-1]
    last_close, last_open, last_vol = last_row['Close'], last_row['Open'], last_row['Volume']
    avg_vol_v1 = last_row.get('V1', last_row['Volume'])
    df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
    ema_50 = df['EMA50'].iloc[-1]
    df, buy_ratios = calculate_vsa_metrics(df)
    last_buy_ratio = buy_ratios[-1]
    net_5d_val = df['Net_Val_VSA'].tail(5).sum()
    score = 0
    if last_close > ema_50: score += 25
    vol_multiple = last_vol / avg_vol_v1 if avg_vol_v1 > 0 else 0
    if vol_multiple >= 2.5: score += 25
    elif vol_multiple >= 2.0: score += 20
    elif vol_multiple >= 1.8: score += 15
    if last_buy_ratio >= 0.75: score += 20
    elif last_buy_ratio >= 0.65: score += 15
    elif last_buy_ratio >= 0.55: score += 10
    if net_5d_val > 0: score += 20
    if last_close > last_open: score += 10
    if score >= 85: label = "VERY STRONG"
    elif score >= 70: label = "STRONG BUY"
    elif score >= 50: label = "WEAK BUY"
    else: label = "NO SIGNAL"
    return score, label

def calculate_trading_plan(df):
    """
    Trading plan OKE SAHAM style:
    Entry = Close terakhir
    SL = Low 3 hari atau EMA20 - ATR
    TP1 = Entry + 3.5% (scalp)
    TP2 = Entry + 1.5*ATR atau resistance pivot high
    RR = reward/risk
    """
    try:
        if df is None or len(df) < 20:
            return None
        last_close = df['Close'].iloc[-1]
        last_low = df['Low'].iloc[-1]
        # ATR
        atr = calculate_atr(df, 14).iloc[-1]
        if pd.isna(atr) or atr == 0:
            atr = last_close * 0.03
        
        ema20 = df['Close'].ewm(span=20).mean().iloc[-1]
        ema50 = df['Close'].ewm(span=50).mean().iloc[-1]
        
        # Pivot low 5 hari buat SL
        pivot_low = df['Low'].tail(5).min()
        # SL = max risk: lower of pivot_low atau last_close - 1.2*ATR, tapi min 2%
        sl_by_atr = last_close - (1.2 * atr)
        sl_by_pivot = min(pivot_low, sl_by_atr)
        # jangan lebih dari 7% SL biar RR bagus
        min_sl = last_close * 0.93
        sl = max(sl_by_pivot, min_sl)
        sl = round_to_ihsg_fraction(sl)
        
        # Entry
        entry = round_to_ihsg_fraction(last_close)
        
        # TP1 scalping 3.5%
        tp1 = round_to_ihsg_fraction(entry * 1.035)
        # TP2 swing 1.5 ATR atau 7%
        tp2_by_atr = entry + (1.8 * atr)
        tp2_by_pct = entry * 1.07
        tp2 = round_to_ihsg_fraction(max(tp2_by_atr, tp2_by_pct))
        
        # Risk reward
        risk = entry - sl
        reward1 = tp1 - entry
        reward2 = tp2 - entry
        rr1 = reward1 / risk if risk>0 else 0
        rr2 = reward2 / risk if risk>0 else 0
        
        # Status
        if last_close > ema20 and last_close > ema50:
            trend = "UPTREND"
        elif last_close > ema20:
            trend = "WEAK UPTREND"
        else:
            trend = "DOWNTREND"
        
        return {
            "entry": int(entry),
            "sl": int(sl),
            "tp1": int(tp1),
            "tp2": int(tp2),
            "atr": float(atr),
            "risk_pct": round((risk/entry)*100, 2) if entry else 0,
            "rr1": round(rr1, 2),
            "rr2": round(rr2, 2),
            "trend": trend,
            "support": int(pivot_low),
            "resistance": int(df['High'].tail(10).max())
        }
    except Exception as e:
        print(f"Trading plan error: {e}")
        return None

def is_market_open():
    now = get_now_wib()
    weekday = now.weekday()
    if weekday >= 5:
        return False
    current_time = now.time()
    if weekday == 4:
        s1_start, s1_end = datetime.time(9, 0), datetime.time(11, 30)
        s2_start, s2_end = datetime.time(14, 0), datetime.time(15, 50)
    else:
        s1_start, s1_end = datetime.time(9, 0), datetime.time(12, 0)
        s2_start, s2_end = datetime.time(13, 30), datetime.time(15, 50)
    return (s1_start <= current_time <= s1_end) or (s2_start <= current_time <= s2_end)

# ========== ARJUM PRO WRAPPER ==========
def arjum_get(path, params=None):
    url = f"{ARJUM_BASE}{path}"
    try:
        # Dynamic API key - baca ulang dari env setiap call
        api_key = os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY
        if not api_key:
            # coba safe_get_env lagi
            api_key = safe_get_env("ARJUM_API_KEY")
        if not api_key:
            print(f"❌ ARJUM_API_KEY KOSONG! path={path}")
            return None
        headers = {"X-API-Key": api_key, "Accept": "application/json", "User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, params=params, timeout=12)
        if r.status_code == 200:
            try:
                j = r.json()
                # Log untuk BBCA/IBOS biar debug
                if "BBCA" in path or "IBOS" in path or "broker" in path:
                    print(f"✅ arjum_get {path} OK keys={list(j.keys()) if isinstance(j, dict) else f'list len={len(j)}'}")
                return j
            except Exception as je:
                print(f"⚠ arjum_get {path} JSON parse fail: {je}, text={r.text[:500]}")
                return None
        else:
            print(f"⚠ arjum_get {path} params={params} -> {r.status_code} {r.text[:800]}")
            return None
    except Exception as e:
        print(f"arjum_get error {path} params={params}: {e}")
        import traceback
        traceback.print_exc()
        return None

def get_screener_latest():
    data = arjum_get("/screener/latest")
    print(f"DEBUG screener raw type={type(data)} sample={str(data)[:400]}")
    if not data:
        return []
    if isinstance(data, dict):
        # Format baru V5.2: {'rows': [{'stock_code':'IBOS', ...}]}
        if 'rows' in data and isinstance(data['rows'], list) and len(data['rows'])>0:
            # normalisasi jadi format symbol
            normalized = []
            for r in data['rows']:
                code = r.get('stock_code') or r.get('symbol') or r.get('code') or r.get('stock')
                if code:
                    # gabungin sisa data biar score bisa pake bucket dll
                    item = {'symbol': code.replace(".JK","").upper(), 'raw': r, 'bucket': r.get('bucket',''), 'summary': r.get('summary','')}
                    normalized.append(item)
            print(f"  -> V5.2 detected, {len(normalized)} saham: {[x['symbol'] for x in normalized[:10]]}")
            return normalized
        # Format lama
        for k in ['data','results','stocks','screener','latest','items']:
            if k in data and isinstance(data[k], list) and len(data[k])>0:
                return data[k]
        # jika dict tapi values list langsung
        try:
            first_val = list(data.values())[0]
            if isinstance(first_val, list) and len(first_val)>0:
                return first_val
        except:
            pass
        return data.get('data') or data.get('results') or data.get('stocks') or []
    return data if isinstance(data, list) else []

def get_broker_accumulation(symbol, top=3, days=None):
    params = {"top": top}
    if days:
        params["days"] = days
        params["period"] = days
    data = arjum_get(f"/broker-accumulation/{symbol}", params=params)
    print(f"DEBUG accum {symbol} days={days}: got={bool(data)} keys={list(data.keys()) if isinstance(data, dict) else type(data)} sample={str(data)[:1000] if data else 'None'}")
    if not data:
        return 0.0, []
    raw_list = []
    accum = 0
    if isinstance(data, dict):
        # Try many keys
        for k in ['data','brokers','top_brokers','result','results','list','accumulation','broker_accumulation']:
            if k in data and isinstance(data[k], list) and len(data[k])>0:
                raw_list = data[k]
                break
            # if data[k] is dict containing list
            if k in data and isinstance(data[k], dict):
                for kk in ['brokers','list','data']:
                    if kk in data[k] and isinstance(data[k][kk], list):
                        raw_list = data[k][kk]
                        break
        if not raw_list:
            for v in data.values():
                if isinstance(v, list) and len(v)>0 and isinstance(v[0], dict):
                    raw_list = v
                    break
                if isinstance(v, dict):
                    for vv in v.values():
                        if isinstance(vv, list) and len(vv)>0 and isinstance(vv[0], dict):
                            raw_list = vv
                            break
        for k in ['total_accum','accumulation','net_value','total','accumulated_value','net_buy','total_value','value','total_accum_value','accum','total_buy','total_net']:
            if k in data and isinstance(data[k], (int,float)) and data[k]!=0:
                accum = float(data[k])
                break
        if accum==0 and raw_list:
            try:
                accum = sum([abs(float(b.get('value',0) or b.get('total_value',0) or b.get('net_value',0) or b.get('net',0) or b.get('accum',0) or b.get('buy_value',0) or b.get('total_buy',0) or 0)) for b in raw_list])
            except:
                accum = 0
    elif isinstance(data, list):
        raw_list = data
        try:
            accum = sum([abs(float(b.get('value',0) or b.get('total_value',0) or b.get('net_value',0) or b.get('net',0) or b.get('buy_value',0) or 0)) for b in raw_list[:top]])
        except:
            accum = 0

    if accum == 0 and not raw_list:
        return 0.0, []

    normalized = []
    for b in raw_list[:20]:
        if not isinstance(b, dict):
            continue
        code = b.get('broker_code') or b.get('broker') or b.get('code') or b.get('broker_id') or b.get('name') or b.get('broker_name') or "??"
        buy_val = b.get('buy_value') or b.get('buy') or b.get('total_buy') or b.get('buy_total') or b.get('buy_value_idr') or 0
        sell_val = b.get('sell_value') or b.get('sell') or b.get('total_sell') or b.get('sell_value_idr') or 0
        buy_vol = b.get('buy_volume') or b.get('buy_vol') or b.get('b_vol') or b.get('volume_buy') or b.get('buy_lot') or b.get('lot_buy') or 0
        sell_vol = b.get('sell_volume') or b.get('sell_vol') or b.get('volume_sell') or 0
        net_val = b.get('net_value') or b.get('net') or b.get('net_buy') or b.get('net_value_idr') or (float(buy_val)-float(sell_val) if buy_val or sell_val else 0)
        if net_val==0:
            net_val = b.get('value') or b.get('total_value') or b.get('accum') or 0
        avg_price = b.get('avg_price') or b.get('avg') or b.get('average') or b.get('avg_buy') or 0
        if avg_price==0 and buy_val and buy_vol:
            try:
                avg_price = float(buy_val) / float(buy_vol) if float(buy_vol)!=0 else 0
            except:
                avg_price = 0
        # If buy_val still 0 but we have lot * price
        if buy_val==0 and buy_vol!=0 and avg_price!=0:
            buy_val = float(buy_vol) * float(avg_price)
        if buy_val==0 and net_val>0:
            buy_val = net_val

        normalized.append({
            "broker_code": str(code).upper(),
            "broker": str(code).upper(),
            "code": str(code).upper(),
            "buy_value": float(buy_val),
            "sell_value": float(sell_val),
            "buy_volume": float(buy_vol),
            "sell_volume": float(sell_vol),
            "net_value": float(net_val),
            "net": float(net_val),
            "value": float(net_val) if net_val!=0 else float(buy_val),
            "avg_price": float(avg_price),
            "avg": float(avg_price),
            "raw": b
        })
    final_accum = float(accum) if accum!=0 else float(sum([abs(x['net_value'] or x['buy_value']) for x in normalized]) or 0.0)
    return final_accum, normalized

def get_broker_summary(symbol):
    params_true = {"net": "true", "broker_limit": 20, "level_limit": 25, "all_data": "false", "flow": "all"}
    data = arjum_get(f"/broker-summary/{symbol}", params=params_true)
    
    net_value = 0
    brokers = []
    status = "NEUTRAL"
    
    if data and isinstance(data, dict):
        # Format baru: brokers list with bval, sval, nval
        raw_list = data.get('brokers') or data.get('data') or []
        if raw_list and isinstance(raw_list, list):
            for b in raw_list[:20]:
                if not isinstance(b, dict):
                    continue
                code = b.get('broker_code') or b.get('code') or '??'
                bval = b.get('bval') or b.get('buy_value') or 0
                sval = b.get('sval') or b.get('sell_value') or 0
                nval = b.get('nval') or b.get('net_value') or (float(bval)-float(sval) if bval or sval else 0)
                bvol = b.get('bvol') or b.get('buy_volume') or 0
                svol = b.get('svol') or b.get('sell_volume') or 0
                avg = b.get('bavg') or b.get('avg_price') or 0
                if avg==0 and bval and bvol:
                    try:
                        avg = float(bval)/float(bvol)
                    except:
                        avg=0
                brokers.append({
                    "broker_code": str(code).upper(),
                    "broker": str(code).upper(),
                    "buy_value": float(bval),
                    "sell_value": float(sval),
                    "buy_volume": float(bvol),
                    "sell_volume": float(svol),
                    "net_value": float(nval),
                    "avg_price": float(avg)
                })
            # Total net from broker_net or sum nval
            if 'broker_net' in data:
                try:
                    net_value = float(data['broker_net'])
                except:
                    net_value = sum([x['net_value'] for x in brokers])
            else:
                net_value = sum([x['net_value'] for x in brokers])
    
    if net_value == 0 and not brokers:
        # fallback to accumulation
        acc_val, acc_brokers = get_broker_accumulation(symbol, top=3)
        if acc_val:
            net_value = acc_val
            brokers = acc_brokers
            status = "ACCUM_EST"
    else:
        status = "ACCUM" if net_value > 0 else "DISTRIB" if net_value < 0 else "NEUTRAL"
    
    print(f"DEBUG broker-summary parsed {symbol}: net={net_value:.0f} brokers={len(brokers)} sample={brokers[0] if brokers else 'empty'}")
    return float(net_value), status, brokers

def calculate_bandars_avg(brokers, hist_df=None, period_days=None):
    try:
        if brokers and isinstance(brokers, list) and len(brokers)>0:
            total_value = 0
            total_vol = 0
            for b in brokers:
                if not isinstance(b, dict):
                    continue
                if b.get('avg_price') and float(b.get('avg_price')) !=0:
                    # kalau ada avg_price langsung, ambil rata2 dari top brokers yang net positif
                    if float(b.get('net_value',0)) >0:
                        total_value += float(b.get('avg_price'))
                        total_vol += 1
                bv = float(b.get('buy_value',0) or 0)
                bvol = float(b.get('buy_volume',0) or 0)
                if bv!=0 and bvol!=0:
                    # avg = value/vol
                    avg = bv / bvol if bvol!=0 else 0
                    if avg>0:
                        total_value += avg
                        total_vol += 1
            if total_vol>0 and total_value>0:
                # kalau total_value di sini adalah sum avg, bukan sum value*vol, jadi rata2kan
                # bedakan: kalau kita sum avg, bagi count. Kalau sum value, bagi sum vol
                # kita sudah sum avg di atas, jadi:
                return float(total_value / total_vol) if total_vol!=0 else 0
    except:
        pass
    try:
        if hist_df is not None and len(hist_df)>=1:
            df_slice = hist_df.tail(period_days) if period_days else hist_df.tail(1)
            if len(df_slice)>0:
                if df_slice['Volume'].sum()>0:
                    vwap = (df_slice['Close'] * df_slice['Volume']).sum() / df_slice['Volume'].sum()
                    return float(vwap)
                else:
                    return float(df_slice['Close'].iloc[-1])
    except:
        pass
    return 0

def get_broker_multi_tf(symbol, hist_df=None):
    accum_d, brokers_d = get_broker_accumulation(symbol, top=3, days=None)
    net_d, status_d, brokers_net_d = get_broker_summary(symbol)
    
    accum_5d, brokers_5d = get_broker_accumulation(symbol, top=3, days=5)
    accum_20d, brokers_20d = get_broker_accumulation(symbol, top=3, days=20)
    
    vsa_5d = 0
    vsa_20d = 0
    vsa_1d = 0
    if hist_df is not None and len(hist_df) >= 5:
        try:
            if 'Net_Val_VSA' not in hist_df.columns:
                hist_df, _ = calculate_vsa_metrics(hist_df)
            vsa_1d = float(hist_df['Net_Val_VSA'].iloc[-1]) if len(hist_df)>=1 else 0
            vsa_5d = float(hist_df['Net_Val_VSA'].tail(5).sum())
            vsa_20d = float(hist_df['Net_Val_VSA'].tail(20).sum())
            # fallback kalau API gagal (accum masih 5B)
            if abs(accum_5d - 5_000_000_000) < 1000000 or accum_5d==1:
                if vsa_5d !=0:
                    accum_5d = abs(vsa_5d) * 1.2
            if abs(accum_20d - 22_500_000_000) < 1000000 or accum_20d==1:
                if vsa_20d !=0:
                    accum_20d = abs(vsa_20d) * 1.2
            if abs(accum_d - 5_000_000_000) < 1000000:
                if vsa_1d !=0:
                    accum_d = abs(vsa_1d) * 1.2
        except Exception as e:
            print(f"VSA calc error {symbol}: {e}")
    
    net_5d = int(net_d * 1.5) if net_d !=0 else int(vsa_5d * 0.8) if vsa_5d !=0 else 0
    net_20d = int(net_d * 3.2) if net_d !=0 else int(vsa_20d * 0.8) if vsa_20d !=0 else 0
    if net_d == 0 and vsa_1d !=0:
        net_d = int(vsa_1d * 0.8)

    brokers_combined = brokers_net_d if brokers_net_d else brokers_d

    avg_d = calculate_bandars_avg(brokers_combined, hist_df, period_days=1)
    avg_5d = calculate_bandars_avg(brokers_5d if brokers_5d else brokers_combined, hist_df, period_days=5)
    avg_20d = calculate_bandars_avg(brokers_20d if brokers_20d else brokers_combined, hist_df, period_days=20)
    
    return {
        "accum_d": float(accum_d),
        "accum_5d": float(accum_5d),
        "accum_20d": float(accum_20d),
        "net_d": float(net_d),
        "net_5d": float(net_5d),
        "net_20d": float(net_20d),
        "avg_d": float(avg_d),
        "avg_5d": float(avg_5d),
        "avg_20d": float(avg_20d),
        "brokers": brokers_combined,
        "status": status_d,
        "vsa_1d": vsa_1d,
        "vsa_5d": vsa_5d,
        "vsa_20d": vsa_20d
    }

def format_top_brokers(brokers, top=3):
    """Format top 3 broker codes kayak CC, BK, AK (CC 12B)"""
    if not brokers or not isinstance(brokers, list) or len(brokers)==0:
        return "- (Arjum no data, pakai VSA)"
    valid = [b for b in brokers if isinstance(b, dict) and (float(b.get('net_value',0) or b.get('buy_value',0) or b.get('value',0) or 0) != 0)]
    if not valid:
        return "- (Arjum no data, pakai VSA)"
    try:
        sorted_b = sorted(valid, key=lambda x: abs(float(x.get('net_value',0) or x.get('buy_value',0) or 0)), reverse=True)
    except:
        sorted_b = valid
    parts = []
    for b in sorted_b[:top]:
        code = b.get('broker_code') or b.get('broker') or "??"
        net = float(b.get('net_value',0) or b.get('buy_value',0) or 0)
        if net==0:
            continue
        if abs(net)>=1e9:
            s=f"{net/1e9:.1f}B"
        elif abs(net)>=1e6:
            s=f"{net/1e6:.0f}M"
        else:
            s=f"{net:.0f}"
        parts.append(f"{code} {s}")
    return ", ".join(parts) if parts else "- (Arjum no data, pakai VSA)"

def get_analysis(symbol):
    data = arjum_get(f"/analysis/{symbol}")
    return data if isinstance(data, dict) else {}

def get_history_pro(symbol, limit=150, timeframe="1d"):
    """
    Multi timeframe support:
    timeframe: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w
    - Arjum: frame = daily, 5min, 15min etc
    - yfinance fallback: mapping interval + period
    """
    tf = timeframe.lower().strip()
    # Normalisasi TF buat Arjum
    arjum_frame_map = {
        "1m": "1min", "1min": "1min",
        "5m": "5min", "5min": "5min", "5M": "5min",
        "15m": "15min", "15min": "15min",
        "30m": "30min", "30min": "30min",
        "1h": "1hour", "60m": "1hour", "1hour": "1hour",
        "4h": "4hour", "4hour": "4hour",
        "1d": "daily", "daily": "daily", "d": "daily",
        "1w": "weekly", "weekly": "weekly",
        "1M": "monthly", "1mo": "monthly"
    }
    arjum_frame = arjum_frame_map.get(tf, "daily")
    
    # Coba Arjum dulu dengan frame yang bener
    data = arjum_get(f"/history/{symbol}", params={"limit": limit, "frame": arjum_frame})
    print(f"DEBUG history {symbol} TF={tf}({arjum_frame}): got={bool(data)} type={type(data).__name__}")
    rows = []
    if data:
        if isinstance(data, dict):
            rows = data.get('data') or data.get('history') or data.get('results') or data.get('candles') or data.get('klines') or []
        elif isinstance(data, list):
            rows = data
    
    # Jika Arjum kosong, fallback ke yfinance dengan interval sesuai TF
    if not rows:
        print(f"⚠ Arjum history {symbol} {tf} kosong, coba yfinance fallback...")
        try:
            import yfinance as yf
            ticker = f"{symbol}.JK"
            yf_ticker = yf.Ticker(ticker)
            
            # Mapping TF ke yfinance period + interval
            yf_map = {
                "1m":  ("7d", "1m"),
                "5m":  ("5d", "5m"),
                "15m": ("5d", "15m"),
                "30m": ("1mo", "30m"),
                "1h":  ("1mo", "60m"),
                "4h":  ("3mo", "90m"),  # yf gak ada 4h, pake 90m terdekat
                "1d":  ("6mo", "1d"),
                "1w":  ("1y", "1wk"),
                "1mo": ("2y", "1mo"),
            }
            period, interval = yf_map.get(tf, ("6mo", "1d"))
            print(f"  yfinance {ticker} period={period} interval={interval}")
            hist = yf_ticker.history(period=period, interval=interval)
            
            # Kalau intraday kosong (market tutup/weekend), fallback ke daily
            if (hist is None or len(hist) < 10) and tf in ["1m","5m","15m","30m","1h","4h"]:
                print(f"  Intraday {tf} kosong (mungkin weekend), fallback ke daily")
                hist = yf_ticker.history(period="6mo", interval="1d")
                # Tetap tandain sebagai intraday tapi data daily, biar chart gak error
            
            if hist is not None and len(hist) > 10:
                print(f"✅ yfinance {symbol} {tf} dapet {len(hist)} candles interval={interval}")
                return hist.tail(limit)
            else:
                print(f"⚠ yfinance {symbol} {tf} juga kosong len={len(hist) if hist is not None else 0}")
                return None
        except Exception as e:
            print(f"yfinance error {symbol} {tf}: {e}")
            return None
    
    try:
        df = pd.DataFrame(rows)
        # mapping
        rename_map = {}
        for c in df.columns:
            cl = str(c).lower()
            if cl in ['o','open']: rename_map[c]='Open'
            elif cl in ['h','high']: rename_map[c]='High'
            elif cl in ['l','low']: rename_map[c]='Low'
            elif cl in ['c','close','close_price']: rename_map[c]='Close'
            elif cl in ['v','volume','vol']: rename_map[c]='Volume'
            elif cl in ['date','time','t','datetime','timestamp']: rename_map[c]='Date'
        df.rename(columns=rename_map, inplace=True)
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
        df = df.sort_index()
        for col in ['Open','High','Low','Close','Volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['Close'])
        if len(df) < 10:
            return None
        print(f"✅ History Arjum {symbol} {tf} OK len={len(df)}")
        return df
    except Exception as e:
        print(f"History parse {symbol} {tf}: {e}")
        # fallback yfinance lagi
        try:
            import yfinance as yf
            period, interval = ("6mo","1d") if tf=="1d" else ("5d","5m")
            hist = yf.Ticker(f"{symbol}.JK").history(period=period, interval=interval)
            if len(hist) > 10:
                return hist.tail(limit)
        except:
            pass
        return None

# ========== CHART GENERATOR - OKE SAHAM STYLE (RAFANO TRADER) ==========
def generate_pro_chart(df, symbol="BBCA", timeframe="1d", sector_info="IHSG", output_filename="chart.png", extra_info=None):
    """
    Rebuild persis seperti screenshot OKE SAHAM:
    - Header: SYMBOL : PRICE (CHANGE%) | RAFANO TRADER center | Daily date
    - Subheader: High Low Open Volume V1 V2 + Company | Sector
    - Kiri: Avg Price, Vchg, Speed, Power, Safety, EMA 13/20/50/200
    - Main: Candle + EMA 13(yellow) 20(red) 50(white) 200(purple) + box konsolidasi
    - Bottom1: Buy% Sell% Net Vol Net 5D + volume histogram
    - Bottom2: NBSA + NBSA Value + histogram buy/sell
    - Bottom3: Market Maker
    """
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.patches as patches
        import matplotlib.gridspec as gridspec
        extra_info = extra_info or {}
        tf_clean = timeframe.lower()

        df = df.copy()
        df = df.ffill().bfill()
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.sort_index()
        else:
            df.index = pd.to_datetime(df.index)

        # --- Calculate EMAs OKE style ---
        df['EMA13'] = df['Close'].ewm(span=13, adjust=False).mean()
        df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
        df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
        df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()
        df['V1'] = df['Volume'].rolling(20, min_periods=1).mean()
        df['V2'] = df['Volume'].rolling(50, min_periods=1).mean()
        df, buy_ratios = calculate_vsa_metrics(df)

        last_close = df['Close'].iloc[-1]
        last_open = df['Open'].iloc[-1]
        last_high = df['High'].iloc[-1]
        last_low = df['Low'].iloc[-1]
        last_vol = df['Volume'].iloc[-1]
        prev_close = df['Close'].iloc[-2] if len(df) > 1 else last_close
        chg_pct = ((last_close / prev_close) - 1) * 100 if prev_close else 0

        # --- Metrics kiri ---
        avg_price = df['Close'].tail(20).mean()
        vchg1 = (last_vol / df['Volume'].iloc[-2]) if len(df) > 1 and df['Volume'].iloc[-2] > 0 else 1
        avg5 = df['Volume'].tail(5).mean()
        vchg5 = (last_vol / avg5) if avg5 > 0 else 1

        # OKE SAHAM logic:
        # Speed = kecepatan volume
        speed = "FAST" if vchg1 > 2.0 else "SLOW" if vchg1 < 0.8 else "NORMAL"
        # Power = kekuatan buyer - ini yang lu maksud dana besar masuk
        # TURBO = Buy% tinggi + volume gede = bandar masuk
        buy_pct_temp = int(buy_ratios[-1] * 100)
        if buy_pct_temp >= 85 and vchg1 >= 1.2:
            power = "TURBO"
        elif buy_pct_temp >= 70 or vchg1 >= 1.5:
            power = "STRONG"
        elif buy_pct_temp >= 60:
            power = "NORMAL"
        else:
            power = "WEAK"
        safety = "GOOD" if last_close > df['EMA200'].iloc[-1] else "BAD"

        ema13 = df['EMA13'].iloc[-1]
        ema20 = df['EMA20'].iloc[-1]
        ema50 = df['EMA50'].iloc[-1]
        ema200 = df['EMA200'].iloc[-1]

        buy_pct = int(buy_ratios[-1] * 100)
        sell_pct = 100 - buy_pct
        net_vol = df['Net_Vol_VSA'].iloc[-1]
        net_vol_5d = df['Net_Vol_VSA'].tail(5).sum()

        # NBSA & MM simulation from real broker data if ada
        real_accum = extra_info.get('accum_value', 0)
        real_net = extra_info.get('broker_net', 0)
        nbsa_rp = abs(real_net) if real_net != 0 else abs(net_vol * last_close)
        nbsa_pct = min(99, max(5, abs(int((real_net / (real_accum+1e9))*10)))) if real_accum else 30.4

        # --- Figure ---
        plt.style.use('dark_background')
        fig = plt.figure(figsize=(16, 9), dpi=200, facecolor='#000000')
        # ratios: main chart 5, volume 1.2, NBSA 0.9, MM 0.9
        gs = gridspec.GridSpec(4, 1, height_ratios=[4.5, 1.1, 0.9, 0.8], hspace=0.05)
        ax_main = fig.add_subplot(gs[0])
        ax_vol = fig.add_subplot(gs[1], sharex=ax_main)
        ax_nbsa = fig.add_subplot(gs[2], sharex=ax_main)
        ax_mm = fig.add_subplot(gs[3], sharex=ax_main)
        fig.subplots_adjust(left=0.08, right=0.92, top=0.88, bottom=0.06)

        for ax in [ax_main, ax_vol, ax_nbsa, ax_mm]:
            ax.set_facecolor('#000000')
            ax.tick_params(colors='#aaaaaa', labelsize=8)
            ax.yaxis.tick_right()
            ax.grid(False)

        x = np.arange(len(df))

        # --- Candles OKE style: hollow green up, solid red down ---
        for i in range(len(df)):
            o, h, l, c = df['Open'].iloc[i], df['High'].iloc[i], df['Low'].iloc[i], df['Close'].iloc[i]
            # wick
            ax_main.plot([i, i], [l, h], color='#00ff00' if c >= o else '#ff0000', linewidth=0.8, alpha=0.8)
            # body
            body_low = min(o, c)
            body_h = max(0.5, abs(c - o))
            if c >= o:
                rect = patches.Rectangle((i-0.35, body_low), 0.7, body_h, facecolor='none', edgecolor='#00ff00', linewidth=0.8)
            else:
                rect = patches.Rectangle((i-0.35, body_low), 0.7, body_h, facecolor='#ff3333', edgecolor='#ff3333', linewidth=0.8)
            ax_main.add_patch(rect)

        # EMAs with OKE colors
        ax_main.plot(x, df['EMA13'], color='#ffff00', linewidth=1.0, alpha=0.9)  # yellow
        ax_main.plot(x, df['EMA20'], color='#ff0000', linewidth=1.0, alpha=0.9)  # red
        ax_main.plot(x, df['EMA50'], color='#ffffff', linewidth=1.0, alpha=0.9)  # white
        ax_main.plot(x, df['EMA200'], color='#a020f0', linewidth=1.2, alpha=0.9)  # purple

        # Box konsolidasi last 15 candles
        if len(df) > 15:
            box_left = len(df) - 15
            box_right = len(df) - 1
            y_low = df['Low'].iloc[-15:].min() * 0.99
            y_high = df['High'].iloc[-15:].max() * 1.01
            ax_main.plot([box_left, box_right], [y_high, y_high], color='white', linestyle='--', linewidth=0.6, alpha=0.6)
            ax_main.plot([box_left, box_right], [y_low, y_low], color='white', linestyle='--', linewidth=0.6, alpha=0.6)
            ax_main.plot([box_left, box_left], [y_low, y_high], color='white', linestyle='--', linewidth=0.6, alpha=0.6)
            ax_main.plot([box_right, box_right], [y_low, y_high], color='white', linestyle='--', linewidth=0.6, alpha=0.6)

        ax_main.set_xlim(-1, len(df))
        ax_main.set_ylim(df['Low'].min()*0.95, df['High'].max()*1.08)

        # --- LEFT PANEL TEXT (Avg Price etc) ---
        left_text = (
            f"Avg Price : {avg_price:,.1f}\n"
            f"Vchg 1 Day: {vchg1:.1f} x\n"
            f"Vchg 5 Days: {vchg5:.1f} x\n"
            f"Speed : {speed}\n"
            f"Power : {power}\n"
            f"Safety : {safety}\n"
            f"\n"
            f"EMA 13 : {ema13:,.1f}\n"
            f"EMA 20 : {ema20:,.1f}\n"
            f"EMA 50 : {ema50:,.1f}\n"
            f"EMA 200: {ema200:,.1f}"
        )
        ax_main.text(0.01, 0.98, left_text, transform=ax_main.transAxes, va='top', ha='left',
                     fontsize=8, family='monospace', color='#e0e0e0',
                     bbox=dict(facecolor='black', alpha=0.6, edgecolor='none'))

        # --- TOP HEADER ---
        # Left: SYMBOL : PRICE (PCT%)
        header_color = '#00ff00' if chg_pct >= 0 else '#ff0000'
        fig.text(0.01, 0.96, f"{symbol} :    {last_close:.0f} ({chg_pct:+.2f}%)", color='#ffff00', fontsize=13, fontweight='bold', ha='left', va='center')
        # Company line under it
        company = sector_info if sector_info else "IDX Stock"
        fig.text(0.01, 0.93, f"{company}", color='#ffaa00', fontsize=8, ha='left')

        # Center: RAFANO TRADER
        fig.text(0.5, 0.96, "RAFANO TRADER", color='white', fontsize=14, fontweight='bold', ha='center', va='center')

        # Right: Daily date - Hapus registrasi
        date_str = df.index[-1].strftime('%d %b %Y') if hasattr(df.index[-1], 'strftime') else get_now_wib().strftime('%d %b %Y')
        fig.text(0.99, 0.96, f"Daily {date_str}", color='#ffcc00', fontsize=10, ha='right', va='center')
        fig.text(0.99, 0.93, f"Command BOT /C {symbol}", color='white', fontsize=8, ha='right')

        # Subheader High Low Open Volume V1 V2
        fig.text(0.01, 0.905, f"High:{last_high:.0f}   Low:{last_low:.0f}   Open:{last_open:.0f}   Volume:{last_vol:,.0f}   V1:{df['V1'].iloc[-1]:,.0f}   V2:{df['V2'].iloc[-1]:,.0f}",
                 color='#00ffff', fontsize=8, ha='left')

        # EMA labels right side
        ax_main.text(1.005, ema200, f" EMA 200 ", transform=ax_main.get_yaxis_transform(), color='black', backgroundcolor='#a020f0',
                     fontsize=7, fontweight='bold', va='center')
        ax_main.text(1.005, last_close, f" {last_close:.0f} ", transform=ax_main.get_yaxis_transform(),
                     color='black', backgroundcolor='white', fontsize=8, fontweight='bold', va='center')

        # Price ladder right
        for level in [last_close*1.1, last_close*1.05, last_close*0.95, last_close*0.9]:
            ax_main.text(1.005, level, f"{level:.0f}", transform=ax_main.get_yaxis_transform(), color='#888888', fontsize=6, va='center')

        # --- VOLUME PANEL - OKE SAHAM STYLE: Stacked Buy/Sell (Power Buy) ---
        # Buy% Sell% Net Vol Net 5D - ini indikator dana besar masuk daily
        vol_info = f"Buy Percent = {buy_pct}%   Sell Percent = {sell_pct}%   Net Vol = {net_vol:,.0f}   Net 5D = {net_vol_5d:,.0f}"
        ax_vol.text(0.005, 0.88, vol_info, transform=ax_vol.transAxes, color='#ffffff', fontsize=8, va='top')
        
        # OKE SAHAM asli: bar bawah merah = Sell Vol, atas hijau = Buy Vol (stacked)
        # Jadi keliatan power buy: kalau hijau mendominasi = TURBO
        ax_vol.bar(x, df['Vol_Sell'], color='#cc0000', width=0.8, alpha=0.8, label='Sell')
        ax_vol.bar(x, df['Vol_Buy'], bottom=df['Vol_Sell'], color='#00cc00', width=0.8, alpha=0.9, label='Buy')
        # White MA line V1
        ax_vol.plot(x, df['V1'], color='white', linewidth=0.8, alpha=0.9)
        
        # Tambah highlight volume spike (dana besar) - kuning kalau TURBO
        if buy_pct >= 85 and vchg1 >= 1.2:
            ax_vol.bar(len(df)-1, df['Volume'].iloc[-1], color='#ffff00', width=0.8, alpha=0.3)  # highlight last bar
        
        ax_vol.set_ylim(0, df['Volume'].max()*1.8)
        plt.setp(ax_vol.get_xticklabels(), visible=False)

        # --- NBSA PANEL ---
        nbsa_info = f"NBSA Rp. {nbsa_rp/1e9:.2f} Milyar   NBSA Value : {nbsa_pct:.1f}%"
        ax_nbsa.text(0.005, 0.85, nbsa_info, transform=ax_nbsa.transAxes, color='#ffffff', fontsize=8, va='top')
        # fake NBSA bars like image: cyan up, red down
        # generate from Net_Vol_VSA normalized
        nbsa_vals = df['Net_Vol_VSA'].tail(80) / (df['Net_Vol_VSA'].abs().max() or 1) * 50
        x_nbsa = np.arange(len(df)-len(nbsa_vals), len(df))
        # split positive cyan, negative red
        for i, v in zip(x_nbsa, nbsa_vals):
            col = '#00ffff' if v >= 0 else '#ff4444'
            ax_nbsa.bar(i, v, color=col, width=0.6)
        # horizontal zero line
        ax_nbsa.axhline(0, color='#444444', linewidth=0.5)
        # right label
        ax_nbsa.text(1.005, 50, "100", transform=ax_nbsa.get_yaxis_transform(), color='black', backgroundcolor='#00ffff', fontsize=7, va='center')
        ax_nbsa.set_ylim(-60, 60)

        # --- MARKET MAKER PANEL ---
        ax_mm.text(0.005, 0.85, "Market Maker", transform=ax_mm.transAxes, color='#ffffff', fontsize=8, va='top')
        # MM line from extra or simulated
        if 'MM' not in df.columns:
            df['MM'] = (df['Close'] - df['EMA50']) / df['EMA50'] * 1000
        mm_vals = df['MM'].tail(80)
        x_mm = np.arange(len(df)-len(mm_vals), len(df))
        ax_mm.bar(x_mm, mm_vals, color='#cccccc', width=0.5, alpha=0.8)
        # last value label yellow
        last_mm = df['MM'].iloc[-1]
        ax_mm.text(1.005, last_mm, f" {last_mm:.4f} ", transform=ax_mm.get_yaxis_transform(),
                   color='black', backgroundcolor='#ffff00', fontsize=7, fontweight='bold', va='center')
        ax_mm.set_ylim(df['MM'].min()*1.2 - 10, df['MM'].max()*1.2 + 10)

        # X labels
        step = max(1, len(df) // 8)
        ax_mm.set_xticks(x[::step])
        ax_mm.set_xticklabels([df.index[i].strftime('%b') if hasattr(df.index[i], 'strftime') else str(i) for i in range(0, len(df), step)], fontsize=7)

        plt.savefig(output_filename, dpi=200, bbox_inches='tight', facecolor='#000000')
        return output_filename
    except Exception as e:
        print(f"Chart error {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        try:
            plt.clf()
            plt.close('all')
        except:
            pass

# ========== TELEGRAM & SCANNER V3 ==========
LAST_SENT_SIGNALS = {}
COOLDOWN_SECONDS = 3600
LAST_RESET_DATE = ""

def filter_signals_with_cooldown(signals):
    global LAST_RESET_DATE, LAST_SENT_SIGNALS
    current_time = time.time()
    today_str = get_now_wib().strftime('%Y-%m-%d')
    if LAST_RESET_DATE != today_str:
        LAST_SENT_SIGNALS.clear()
        LAST_RESET_DATE = today_str
    filtered = []
    for sig in signals:
        sym = sig['symbol']
        last_sent = LAST_SENT_SIGNALS.get(sym, 0)
        if (current_time - last_sent) >= COOLDOWN_SECONDS:
            filtered.append(sig)
            LAST_SENT_SIGNALS[sym] = current_time
    return filtered

def calculate_score_v2(symbol, history_df, accum_value, broker_net, analysis_data):
    score = 0
    reasons = []
    score += 30
    reasons.append("Screener")
    if accum_value > 20_000_000_000:
        score += 30
        reasons.append(f"Akum {accum_value/1e9:.1f}B")
    elif accum_value > 5_000_000_000:
        score += 20
        reasons.append(f"Akum {accum_value/1e9:.1f}B")
    elif accum_value > 0:
        score += 10
        reasons.append("Akum Tipis")
    if broker_net > 10_000_000_000:
        score += 20
        reasons.append(f"Net {broker_net/1e9:.1f}B")
    elif broker_net > 0:
        score += 10
        reasons.append("Net+")
    try:
        if analysis_data.get('trend') == 'BULLISH':
            score += 20
            reasons.append("BULLISH")
        elif history_df is not None and len(history_df) > 50:
            ema50 = history_df['Close'].ewm(span=50).mean().iloc[-1]
            if history_df['Close'].iloc[-1] > ema50:
                score += 15
                reasons.append(">EMA50")
            score += 5
    except:
        pass
    if score >= 85:
        label = "VERY STRONG"
    elif score >= 70:
        label = "STRONG BUY"
    elif score >= 50:
        label = "WEAK BUY"
    else:
        label = "NO SIGNAL"
    return score, label, reasons

def scan_v3():
    print(f"[{get_now_wib()}] 🚀 V3 Scan...")
    screener_data = get_screener_latest()
    if not screener_data:
        print("⚠ Screener kosong, fallback 15 saham")
        candidates = ["BBCA","BBRI","BMRI","TLKM","ASII","GOTO","AMMN","ADRO","ANTM","MDKA","BBNI","BRIS","UNTR","ICBP","TLKM"]
        screener_map = {s: {} for s in candidates}
        is_fallback = True
    else:
        candidates = []
        screener_map = {}
        for item in screener_data:
            sym = item.get('symbol') or item.get('code') or item.get('stock')
            if sym:
                sym = sym.replace(".JK","").upper()
                candidates.append(sym)
                screener_map[sym] = item
        candidates = candidates[:30]
        print(f"  -> Kandidat: {candidates[:10]}")
        is_fallback = False

    detected = []
    def process_symbol(sym):
        try:
            accum_val, accum_brokers = get_broker_accumulation(sym, top=3)
            # jangan filter negatif dulu pas weekend, biar scan tetep keluar
            # if accum_val < -10_000_000_000:
            #     return None
            broker_net, broker_status, broker_list = get_broker_summary(sym)
            # gabungin brokers dari dua endpoint, prioritaskan broker_summary
            brokers_combined = broker_list if broker_list else accum_brokers
            hist_df = get_history_pro(sym, limit=120, timeframe="1d")
            analysis = get_analysis(sym)
            score, label, reasons = calculate_score_v2(sym, hist_df, accum_val, broker_net, analysis)
            # weekend threshold lebih rendah biar /scanpro tetep bisa
            threshold = 40 if is_fallback else 55
            if score >= threshold:
                last_close = 0
                change_pct = 0
                if hist_df is not None and len(hist_df) >= 2:
                    last_close = int(hist_df['Close'].iloc[-1])
                    prev = hist_df['Close'].iloc[-2]
                    change_pct = ((last_close/prev)-1)*100 if prev else 0
                # Trading plan
                tp = calculate_trading_plan(hist_df) if hist_df is not None else None
                return {
                    "symbol": sym,
                    "close": last_close,
                    "change_pct": change_pct,
                    "score": score,
                    "score_label": label,
                    "accum_value": accum_val,
                    "broker_net": broker_net,
                    "broker_status": broker_status,
                    "reasons": reasons,
                    "history_df": hist_df,
                    "trading_plan": tp,
                    "brokers": brokers_combined,
                    "broker_list": brokers_combined
                }
        except Exception as e:
            print(f"Error {sym}: {e}")
        return None

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(process_symbol, s) for s in candidates]
        for f in futures:
            res = f.result()
            if res:
                detected.append(res)
    detected.sort(key=lambda x: x['score'], reverse=True)
    print(f"✅ V3 Scan: {len(detected)} sinyal")
    return detected

def send_reply(chat_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"TG Error: {e}")

def send_photo_reply(chat_id, photo_path, caption=""):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path, 'rb') as photo:
            requests.post(url, data={'chat_id': chat_id, 'caption': caption, 'parse_mode': 'Markdown'}, files={'photo': photo}, timeout=30)
    except Exception as e:
        print(f"Photo Error: {e}")

def broadcast_v3(signals):
    if not signals:
        send_reply(TARGET_CHAT_ID, "🔍 V3 Scan: Tidak ada sinyal REAL ACCUM hari ini.")
        return
    now_str = get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header = f"*RAFANO V3 PRO - REAL ACCUM*\n{now_str}\nTotal: {len(signals)} | Cooldown 60m\n============================\n\n"
    msg = header
    keyboard = []
    for idx, item in enumerate(signals, 1):
        def fmt(v):
            return format_large_number(v, True)
        reasons_str = " | ".join(item['reasons'][:3])
        # Trading plan
        tp = item.get('trading_plan')
        if tp:
            tp_str = f"   ├ 🎯 Plan: Entry {tp['entry']} | TP1 {tp['tp1']} ({tp['rr1']}R) | TP2 {tp['tp2']} ({tp['rr2']}R) | SL {tp['sl']} ({tp['risk_pct']}%)\n"
        else:
            tp_str = ""
        # Top 3 broker
        brokers = item.get('brokers', []) or item.get('broker_list', [])
        top_broker_str = format_top_brokers(brokers, 3)
        item_str = (
            f"{idx}. *{item['symbol']}* — {item['close']} ({item['change_pct']:+.2f}%)\n"
            f"   ├ Score: *{item['score']}% ({item['score_label']})*\n"
            f"   ├ Akum 3B: {fmt(item['accum_value'])} | Net: {fmt(item['broker_net'])} ({item['broker_status']})\n"
            f"   ├ Top Brokers: {top_broker_str}\n"
            f"{tp_str}"
            f"   └ {reasons_str}\n\n"
        )
        keyboard.append([{"text": f"📈 {item['symbol']} Pro Chart", "callback_data": f"chart_{item['symbol']}_1d"}])
        if len(msg) + len(item_str) > 3500:
            send_reply(TARGET_CHAT_ID, msg, reply_markup={"inline_keyboard": keyboard})
            msg = item_str
            keyboard = []
        else:
            msg += item_str
    if msg:
        send_reply(TARGET_CHAT_ID, msg, reply_markup={"inline_keyboard": keyboard})

def process_chart_request(chat_id, stock_code, timeframe="1d", extra_info_cache=None):
    send_reply(chat_id, f"📊 *Generating Pro Chart {stock_code.upper()} ({timeframe.upper()}) + REAL DATA...*")
    # Ambil data - sekarang support multi TF
    df = get_history_pro(stock_code, limit=150, timeframe=timeframe)
    if df is None or len(df) < 20:
        send_reply(chat_id, f"⚠ Data {stock_code} tidak ketemu TF {timeframe}")
        return
    
    # Ambil real data buat ditempel di chart
    if extra_info_cache and stock_code in extra_info_cache:
        extra = extra_info_cache[stock_code]
        # brokers dari cache
        brokers_cached = extra.get('brokers') or extra.get('broker_list') or []
    else:
        accum_val, accum_brokers = get_broker_accumulation(stock_code, top=3)
        broker_net, broker_status, broker_list = get_broker_summary(stock_code)
        brokers_cached = broker_list if broker_list else accum_brokers
        # Score cepat
        score = 70 if accum_val > 5e9 else 50
        extra = {"accum_value": accum_val, "broker_net": broker_net, "broker_status": broker_status, "score": score, "score_label": "REAL", "brokers": brokers_cached}

    # Trading plan
    tp = calculate_trading_plan(df)
    top_broker_str = format_top_brokers(brokers_cached if 'brokers_cached' in locals() else extra.get('brokers', []), 3)

    chart_file = f"chart_{stock_code.upper()}_{timeframe}_{int(time.time())}.png"
    try:
        file_path = generate_pro_chart(df=df, symbol=stock_code.upper(), timeframe=timeframe, sector_info=f"{stock_code.upper()} | IHSG", output_filename=chart_file, extra_info=extra)
        if tp:
            caption = (
                f"*{stock_code.upper()}* — {safe_int(df['Close'].iloc[-1])} | {tp['trend']}\n"
                f"Score REAL: {extra.get('score',0)}% | Akum: {format_large_number(extra.get('accum_value',0), True)}\n"
                f"Net Broker: {format_large_number(extra.get('broker_net',0), True)} ({extra.get('broker_status')})\n"
                f"Top Brokers: {top_broker_str}\n"
                f"Timeframe: {timeframe.upper()}\n"
                f"──────────────────\n"
                f"🎯 *TRADING PLAN*\n"
                f"Entry: {tp['entry']} | SL: {tp['sl']} ({tp['risk_pct']}%)\n"
                f"TP1: {tp['tp1']} (RR {tp['rr1']}) | TP2: {tp['tp2']} (RR {tp['rr2']})\n"
                f"Sup: {tp['support']} | Res: {tp['resistance']} | ATR: {tp['atr']:.1f}"
            )
        else:
            caption = (
                f"*{stock_code.upper()}* — {safe_int(df['Close'].iloc[-1])}\n"
                f"Score REAL: {extra.get('score',0)}% | Akum: {format_large_number(extra.get('accum_value',0), True)}\n"
                f"Net Broker: {format_large_number(extra.get('broker_net',0), True)} ({extra.get('broker_status')})\n"
                f"Top Brokers: {top_broker_str}\n"
                f"Timeframe: {timeframe.upper()}"
            )
        send_photo_reply(chat_id, file_path, caption=caption)
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        import traceback
        traceback.print_exc()
        send_reply(chat_id, f"❌ Gagal render: `{e}`")

# Cache sinyal terakhir biar chart bisa ambil data real tanpa request lagi
LAST_SIGNALS_CACHE = {}

def telegram_bot_listener():
    global LAST_SIGNALS_CACHE
    offset = 0
    print("🤖 Telegram Listener V3 Running...")
    # Hapus webhook biar getUpdates jalan (fix telegram gak respon)
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
        print("✅ Webhook deleted, polling mode active")
    except Exception as e:
        print(f"Webhook delete fail: {e}")
    # Test token valid
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe", timeout=10)
        print(f"✅ Bot Info: {r.json().get('result',{})}")
        if r.status_code != 200:
            print(f"❌ TOKEN INVALID: {r.text}")
    except Exception as e:
        print(f"❌ Bot token error: {e}")
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res = requests.get(url, timeout=25)
            if res.status_code != 200:
                print(f"⚠ getUpdates {res.status_code}: {res.text[:200]}")
                time.sleep(3)
                continue
            if res.status_code == 200:
                data = res.json()
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    if "callback_query" in update:
                        cb = update["callback_query"]
                        cb_id = cb.get("id")
                        cb_data = cb.get("data","")
                        chat_id = cb["message"]["chat"]["id"]
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery", json={"callback_query_id": cb_id})
                        if cb_data.startswith("chart_"):
                            parts = cb_data.split("_")
                            if len(parts) >=3:
                                sym = parts[1]
                                tf = parts[2]
                                threading.Thread(target=process_chart_request, args=(chat_id, sym, tf, LAST_SIGNALS_CACHE)).start()
                    elif "message" in update and "text" in update["message"]:
                        msg = update["message"]
                        text = msg.get("text","").strip()
                        chat_id = msg["chat"]["id"]
                        first_word = text.split()[0].lower() if text else ""
                        print(f"📩 Pesan masuk: {text} dari chat_id={chat_id}")
                        if first_word in ["/start","/help"]:
                            help_msg = (
                                "🤖 *RAFANO V3 PRO*\n"
                                "============================\n"
                                "Perintah:\n"
                                "📈 `/c <KODE> [TF]` - Chart Pro + Real Akum\n"
                                "   Contoh: `/c BBCA` `/c ANTM 15m`\n"
                                "🔍 `/scan` - Scan V3 Real Accumulation\n"
                                "🔥 `/scanpro` - Scan + langsung chart top 3\n"
                            )
                            send_reply(chat_id, help_msg)
                        elif first_word in ["/c","/chart","!chart"]:
                            parts = text.split()
                            if len(parts) >=2:
                                sym = parts[1].upper()
                                tf = parts[2] if len(parts)>=3 else "1d"
                                threading.Thread(target=process_chart_request, args=(chat_id, sym, tf, LAST_SIGNALS_CACHE)).start()
                            else:
                                send_reply(chat_id, "⚠ Format: `/c <KODE> [TF]`")
                        elif first_word in ["/scan","!scan","/scanpro"]:
                            send_reply(chat_id, "🔍 *V3 Scanning Real Accumulation...*")
                            def manual_scan(is_pro=False, target_chat=chat_id):
                                global LAST_SIGNALS_CACHE
                                print(f"Manual scan dipanggil is_pro={is_pro} target={target_chat}")
                                sigs = scan_v3()
                                LAST_SIGNALS_CACHE = {s['symbol']: s for s in sigs}
                                # Manual scan BYPASS cooldown biar bisa dipanggil kapan aja, termasuk weekend
                                filt = sigs  # jangan pakai filter_signals_with_cooldown buat manual
                                # Selalu kirim ke yang request, bukan cuma TARGET_CHAT_ID
                                now_str = get_now_wib().strftime('%d %b %Y %H:%M WIB')
                                if not filt:
                                    send_reply(target_chat, f"*RAFANO V3* {now_str}\n0 sinyal (weekend/market tutup, coba /c BBCA)\n_Screener kosong, coba beberapa saham fallback_")
                                    return
                                header = f"*RAFANO V3 PRO - {now_str}*\nTotal: {len(filt)} (manual, tanpa cooldown)\n\n"
                                msg = header
                                kb = []
                                for idx, item in enumerate(filt,1):
                                    tp = item.get('trading_plan')
                                    tp_line = f" | Plan Entry {tp['entry']} TP1 {tp['tp1']} SL {tp['sl']}" if tp else ""
                                    item_str = f"{idx}. *{item['symbol']}* {item['score']}% Akum:{format_large_number(item['accum_value'],True)} Net:{format_large_number(item['broker_net'],True)}{tp_line}\n"
                                    kb.append([{"text": f"📈 {item['symbol']}", "callback_data": f"chart_{item['symbol']}_1d"}])
                                    if len(msg) + len(item_str) > 3500:
                                        send_reply(target_chat, msg, reply_markup={"inline_keyboard": kb})
                                        msg = item_str
                                        kb = []
                                    else:
                                        msg += item_str
                                send_reply(target_chat, msg, reply_markup={"inline_keyboard": kb})
                                if is_pro:
                                    for top in filt[:3]:
                                        process_chart_request(target_chat, top['symbol'], "1d", LAST_SIGNALS_CACHE)
                                        time.sleep(1)
                            is_pro_flag = (first_word == "/scanpro")
                            threading.Thread(target=manual_scan, args=(is_pro_flag, chat_id)).start()
        except Exception as e:
            print(f"Listener error: {e}")
            time.sleep(3)

def auto_screener_loop():
    global LAST_SIGNALS_CACHE
    print("🚀 Auto Screener V3 Active...")
    last_triggered_sesi1, last_triggered_eod = "", ""
    while True:
        try:
            if not is_market_open():
                time.sleep(300)
                continue
            now = get_now_wib()
            today_str, current_time_str = now.strftime('%Y-%m-%d'), now.strftime('%H:%M')
            weekday = now.weekday()

            # Rekap sesi
            target_sesi1 = "11:25" if weekday == 4 else "11:55"
            if current_time_str == target_sesi1 and last_triggered_sesi1 != today_str:
                sigs = scan_v3()
                LAST_SIGNALS_CACHE = {s['symbol']: s for s in sigs}
                filt = filter_signals_with_cooldown(sigs)
                broadcast_v3(filt)
                last_triggered_sesi1 = today_str

            if current_time_str == "15:55" and last_triggered_eod != today_str:
                sigs = scan_v3()
                LAST_SIGNALS_CACHE = {s['symbol']: s for s in sigs}
                filt = filter_signals_with_cooldown(sigs)
                broadcast_v3(filt)
                last_triggered_eod = today_str

            # Real-time per 10 menit
            sigs = scan_v3()
            LAST_SIGNALS_CACHE = {s['symbol']: s for s in sigs}
            filt = filter_signals_with_cooldown(sigs)
            if filt:
                broadcast_v3(filt)
            time.sleep(600)
        except Exception as e:
            print(f"Auto loop error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    print("==========================================")
    print("🔥 RAFANO V3 PRO STARTING...")
    print("==========================================")
    threading.Thread(target=auto_screener_loop, daemon=True).start()
    telegram_bot_listener()
