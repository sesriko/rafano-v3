"""
RAFANO V3.1 - FIX DISTRIBUSI VALID
Fix:
- Logic Top3 Akum vs Top3 Dist (jika Top Dist > Top Akum = DIST)
- Bug syntax BOS EMA & is_market_open duplikat
- Cache & API call lebih efisien
- Chart generator aman
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

def safe_get_env(key):
    v = os.getenv(key)
    if v:
        v = str(v).strip()
        if len(v)>=2 and ((v[0]=='"' and v[-1]=='"') or (v[0]=="'" and v[-1]=="'")):
            v = v[1:-1].strip()
        return v
    try:
        from google.colab import userdata
        vv = userdata.get(key)
        if vv:
            vv = str(vv).strip().strip('"').strip("'")
            os.environ[key] = vv
            return vv
    except: pass
    return None

TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')
TELEGRAM_BOT_TOKEN = safe_get_env("TELEGRAM_BOT_TOKEN")
TARGET_CHAT_ID = safe_get_env("TARGET_CHAT_ID")
ARJUM_API_KEY = safe_get_env("ARJUM_API_KEY")

print(f"🔑 ENV Loaded - TOKEN exists={bool(TELEGRAM_BOT_TOKEN)}, CHAT_ID={TARGET_CHAT_ID}, ARJUM exists={bool(ARJUM_API_KEY)}")

ARJUM_BASE = "https://stock.arjum.com/api"
def get_arjum_headers():
    k = os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or safe_get_env("ARJUM_API_KEY") or ""
    return {"X-API-Key": k.strip(), "Accept": "application/json", "User-Agent": "Mozilla/5.0"}

def get_now_wib():
    return datetime.datetime.now(TIMEZONE_WIB)

# ========== HELPERS ==========
def safe_int(val, default=0):
    try:
        if pd.isna(val) or np.isinf(val): return default
        return int(val)
    except: return default

def format_large_number(val, show_sign=False):
    if pd.isna(val) or val == 0: return "0"
    abs_val = abs(val)
    sign = "+" if (show_sign and val > 0) else ("-" if val < 0 else "")
    if abs_val >= 1_000_000_000: return f"{sign}{abs_val / 1_000_000_000:.2f}B"
    elif abs_val >= 1_000_000: return f"{sign}{abs_val / 1_000_000:,.0f}M"
    elif abs_val >= 1_000: return f"{sign}{abs_val / 1_000:,.0f}K"
    else: return f"{sign}{val:,.0f}"

def round_to_ihsg_fraction(price):
    if pd.isna(price) or price <= 0: return 0
    price = float(price)
    if price < 200: tick = 1
    elif price < 500: tick = 2
    elif price < 2000: tick = 5
    elif price < 5000: tick = 10
    else: tick = 25
    return int(round(price / tick) * tick)

def calculate_atr(df, period=14):
    high, low, close = df['High'], df['Low'], df['Close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=1).mean()

def calculate_vsa_metrics(df):
    price_range = (df['High'] - df['Low']).replace(0, 0.1)
    close_pos = (df['Close'] - df['Low']) / price_range
    close_pos = np.clip(close_pos, 0.05, 0.95)
    buy_ratio = 0.30 + close_pos * 0.60
    if 'V1' in df.columns:
        vol_ratio = df['Volume'] / df['V1'].replace(0, 1)
        is_green = df['Close'] >= df['Open']
        boost = np.where((vol_ratio > 1.5) & is_green, 0.10, 0)
        boost += np.where((vol_ratio > 2.5) & is_green, 0.10, 0)
        buy_ratio = buy_ratio + boost
    buy_ratio = np.clip(buy_ratio, 0.05, 0.95)
    df['Vol_Buy'] = df['Volume'] * buy_ratio
    df['Vol_Sell'] = df['Volume'] - df['Vol_Buy']
    df['Net_Vol_VSA'] = df['Vol_Buy'] - df['Vol_Sell']
    df['Net_Val_VSA'] = df['Net_Vol_VSA'] * df['Close']
    df['Buy_Pct'] = buy_ratio * 100
    return df, buy_ratio

def calculate_bollinger_bands(df, period=20, std=2):
    sma = df['Close'].rolling(period).mean()
    stddev = df['Close'].rolling(period).std()
    upper = sma + (stddev * std)
    lower = sma - (stddev * std)
    return sma, upper, lower

def detect_buy_signals(df, multi_tf=None):
    signals = []
    if df is None or len(df) < 30: return signals, df
    try:
        df = df.copy()
        df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
        df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()
        df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
        df['V1'] = df['Volume'].rolling(20, min_periods=1).mean()
        df['ATR'] = calculate_atr(df, 14)
        _, bb_upper, bb_lower = calculate_bollinger_bands(df, 20, 2)
        df['BB_UPPER'] = bb_upper
        df['BB_LOWER'] = bb_lower
        df, _ = calculate_vsa_metrics(df)
        net_5d = multi_tf.get('net_5d', 0) if multi_tf else df['Net_Val_VSA'].tail(5).sum()
        for i in range(20, len(df)):
            close, open_, low, vol = df['Close'].iloc[i], df['Open'].iloc[i], df['Low'].iloc[i], df['Volume'].iloc[i]
            v1, ema50, ema20 = df['V1'].iloc[i], df['EMA50'].iloc[i], df['EMA20'].iloc[i]
            bb_low = df['BB_LOWER'].iloc[i] if not pd.isna(df['BB_LOWER'].iloc[i]) else 0
            atr = df['ATR'].iloc[i] if not pd.isna(df['ATR'].iloc[i]) else close*0.03
            prev_close, prev_ema50 = df['Close'].iloc[i-1], df['EMA50'].iloc[i-1]
            is_bo = (prev_close <= prev_ema50 and close > ema50 and close > ema20)
            vol_spike = (vol > v1 * 1.5) if v1>0 else False
            is_green = close >= open_
            if is_bo and vol_spike and is_green and net_5d > 0:
                signals.append({'index': i, 'date': df.index[i], 'type': 'BO EMA50', 'side': 'BUY', 'entry': float(close), 'sl': float(min(df['Low'].iloc[max(0,i-5):i+1].min(), close - atr*1.2)), 'reason': f'Breakout EMA50 + Vol {vol/v1:.1f}x', 'strength': 90})
                continue
            if bb_low > 0:
                dist = (close - bb_low) / bb_low * 100
                body = abs(close - open_)
                lower_wick = min(open_, close) - low
                is_rev = is_green and lower_wick > body*1.5 and body > 0
                if close < bb_low and dist < -1.5 and is_rev:
                    signals.append({'index': i, 'date': df.index[i], 'type': 'BOW BB', 'side': 'BUY', 'entry': float(close), 'sl': float(low*0.98), 'reason': f'BOW {dist:.1f}% below BB', 'strength': 85})
                    continue
            # BOS
            dist_ema50 = abs(close - ema50)/ema50*100 if ema50>0 else 100
            is_near = dist_ema50 < 2.0
            wick_count = 0
            for j in range(max(0, i-10), i+1):
                if abs(df['Low'].iloc[j] - df['EMA50'].iloc[j])/df['EMA50'].iloc[j] < 0.015: wick_count+=1
            if is_near and wick_count>=2 and close>ema50 and is_green:
                # FIXED BUG: kurung kelebihan sebelumnya
                sl_val = min(df['Low'].iloc[max(0,i-3):i+1].min(), ema50*0.97)
                signals.append({'index': i, 'date': df.index[i], 'type': 'BOS EMA', 'side': 'BUY', 'entry': float(close), 'sl': float(sl_val), 'reason': f'BOS Near EMA {dist_ema50:.1f}%', 'strength': 80})
        # filter
        filtered, last_idx = [], -20
        for sig in sorted(signals, key=lambda x: x['index']):
            if sig['index'] - last_idx >= 5:
                filtered.append(sig); last_idx = sig['index']
        return filtered, df
    except Exception as e:
        print(f"detect_buy error {e}"); return [], df

def detect_sell_signals(df, multi_tf=None):
    signals = []
    if df is None or len(df) < 30: return signals, df
    try:
        if 'EMA50' not in df.columns:
            df = df.copy()
            df['EMA50'] = df['Close'].ewm(span=50).mean()
            df['V1'] = df['Volume'].rolling(20).mean()
            df['ATR'] = calculate_atr(df, 14)
            _, bb_upper, _ = calculate_bollinger_bands(df, 20, 2)
            df['BB_UPPER'] = bb_upper
            df, _ = calculate_vsa_metrics(df)
        net_5d = multi_tf.get('net_5d',0) if multi_tf else 0
        for i in range(20, len(df)):
            close, open_, high, vol = df['Close'].iloc[i], df['Open'].iloc[i], df['High'].iloc[i], df['Volume'].iloc[i]
            ema50 = df['EMA50'].iloc[i]
            bb_up = df['BB_UPPER'].iloc[i] if not pd.isna(df['BB_UPPER'].iloc[i]) else 0
            atr = df['ATR'].iloc[i] if not pd.isna(df['ATR'].iloc[i]) else close*0.03
            prev_close, prev_ema50 = df['Close'].iloc[i-1], df['EMA50'].iloc[i-1]
            is_bd = (prev_close >= prev_ema50 and close < ema50)
            vol_spike = (vol > df['V1'].iloc[i]*1.5) if df['V1'].iloc[i]>0 else False
            is_red = close < open_
            if is_bd and vol_spike and is_red and net_5d < 0:
                signals.append({'index': i, 'date': df.index[i], 'type': 'BD EMA50', 'side': 'SELL', 'entry': float(close), 'sl': float(max(df['High'].iloc[max(0,i-5):i+1].max(), close+atr*1.2)), 'reason': 'Breakdown EMA50', 'strength': 90})
        filtered, last_idx = [], -20
        for sig in sorted(signals, key=lambda x: x['index']):
            if sig['index'] - last_idx >= 5:
                filtered.append(sig); last_idx = sig['index']
        return filtered, df
    except Exception as e:
        print(f"sell error {e}"); return [], df

def calculate_trading_plan(df, signals=None, multi_tf=None):
    try:
        if df is None or len(df) < 20: return None
        last_close = df['Close'].iloc[-1]
        atr = calculate_atr(df, 14).iloc[-1]
        if pd.isna(atr) or atr==0: atr = last_close*0.03
        ema20 = df['Close'].ewm(span=20).mean().iloc[-1]
        ema50 = df['Close'].ewm(span=50).mean().iloc[-1]
        ema200 = df['Close'].ewm(span=200).mean().iloc[-1]
        if signals is None:
            buy_sigs,_ = detect_buy_signals(df, multi_tf)
            sell_sigs,_ = detect_sell_signals(df, multi_tf)
            signals = buy_sigs + sell_sigs
        else:
            buy_sigs = [s for s in signals if s.get('side')=='BUY']
            sell_sigs = [s for s in signals if s.get('side')=='SELL']

        mtf_confirm = "NEUTRAL"
        if multi_tf:
            s5, s20 = multi_tf.get('status_5d','NEUTRAL'), multi_tf.get('status_20d','NEUTRAL')
            if s5=="AKUM" and s20=="AKUM": mtf_confirm="STRONG BULLISH MTF"
            elif s5=="AKUM" or s20=="AKUM": mtf_confirm="BULLISH MTF"
            elif s5=="DIST" and s20=="DIST": mtf_confirm="BEARISH MTF"

        recent_buy = [s for s in buy_sigs if s['index'] >= len(df)-10]
        recent_sell = [s for s in sell_sigs if s['index'] >= len(df)-10]

        if recent_buy and (not recent_sell or recent_buy[-1]['index'] >= recent_sell[-1]['index']):
            ls = recent_buy[-1]; entry, sl, side = ls['entry'], ls['sl'], "BUY"
            sig_type, sig_reason, sig_strength, sig_date = ls['type'], ls['reason'], ls['strength'], ls['date']
            is_buy=True
        elif recent_sell:
            ls = recent_sell[-1]; entry, sl, side = ls['entry'], ls['sl'], "SELL"
            sig_type, sig_reason, sig_strength, sig_date = ls['type'], ls['reason'], ls['strength'], ls['date']
            is_buy=False
        else:
            entry = round_to_ihsg_fraction(last_close)
            sl = round_to_ihsg_fraction(max(df['Low'].tail(5).min(), last_close-atr*1.5))
            sig_type, sig_reason, sig_strength, sig_date, side, is_buy = "NO SIGNAL", "Tunggu BO/BOW/BOS", 0, df.index[-1], "WAIT", False

        if side=="BUY" and mtf_confirm=="STRONG BULLISH MTF": sig_strength=min(100,sig_strength+10)
        min_sl, max_sl = last_close*0.92, last_close*0.98
        if side!="SELL": sl = max(min(sl, max_sl), min_sl)
        sl = round_to_ihsg_fraction(sl)
        if entry <= sl and side!="SELL": entry = round_to_ihsg_fraction(sl*1.03)

        if side=="BUY":
            tp1 = round_to_ihsg_fraction(entry + atr*1.5)
            tp2 = round_to_ihsg_fraction(entry + atr*3.0)
            if mtf_confirm=="STRONG BULLISH MTF": tp2 = round_to_ihsg_fraction(entry + atr*4.0)
            risk, r1, r2 = entry-sl, tp1-entry, tp2-entry
        elif side=="SELL":
            sl_s = min(max(sl, last_close*1.02), last_close*1.08)
            sl = round_to_ihsg_fraction(sl_s)
            if entry>=sl: entry = round_to_ihsg_fraction(sl*0.97)
            tp1, tp2 = round_to_ihsg_fraction(entry*0.965), round_to_ihsg_fraction(entry-atr*1.8)
            risk, r1, r2 = sl-entry, entry-tp1, entry-tp2
        else:
            tp1, tp2 = round_to_ihsg_fraction(entry*1.035), round_to_ihsg_fraction(entry+atr*1.8)
            risk, r1, r2 = entry-sl, tp1-entry, tp2-entry

        rr1, rr2 = (r1/risk if risk>0 else 0), (r2/risk if risk>0 else 0)

        if last_close>ema20 and last_close>ema50 and last_close>ema200: trend="STRONG UPTREND"
        elif last_close>ema20 and last_close>ema50: trend="UPTREND"
        elif last_close<ema20 and last_close<ema50 and last_close<ema200: trend="STRONG DOWNTREND"
        else: trend="DOWNTREND"
        if "DOWNTREND" in trend:
            very_recent = [s for s in buy_sigs if s['index'] >= len(df)-3]
            if not very_recent and side=="BUY":
                side="WAIT"; is_buy=False; sig_type="NO SIGNAL"; sig_reason=f"Tunggu trigger valid - Trend {trend}"
                sig_strength=0
        trend_mtf = f"{trend} + {mtf_confirm}" if mtf_confirm!="NEUTRAL" else trend
        return {"entry":int(entry),"sl":int(sl),"tp1":int(tp1),"tp2":int(tp2),"atr":float(atr),"risk_pct":round((risk/entry)*100,2) if entry else 0,"rr1":round(rr1,2),"rr2":round(rr2,2),"trend":trend_mtf,"support":int(df['Low'].tail(10).min()),"resistance":int(df['High'].tail(10).max()),"signal_type":sig_type,"signal_reason":sig_reason,"signal_strength":sig_strength,"signal_date":sig_date,"all_signals":signals,"buy_signals":buy_sigs,"sell_signals":sell_sigs,"is_buy_signal":is_buy and sig_strength>=70,"is_sell_signal":(not is_buy) and side=="SELL" and sig_strength>=70,"side":side,"mtf_confirm":mtf_confirm}
    except Exception as e:
        print(f"plan error {e}"); return None

def is_market_open():
    now = get_now_wib()
    if now.weekday()>=5: return False
    ct = now.time()
    if now.weekday()==4:
        s1, e1 = datetime.time(9,0), datetime.time(11,30)
        s2, e2 = datetime.time(14,0), datetime.time(15,50)
    else:
        s1, e1 = datetime.time(9,0), datetime.time(12,0)
        s2, e2 = datetime.time(13,30), datetime.time(15,50)
    return (s1<=ct<=e1) or (s2<=ct<=e2)

# ========== CACHE ==========
import json
from pathlib import Path
BROKER_CACHE = {}
HISTORY_CACHE = {}
SCREENER_CACHE = {}
CACHE_FILE = Path("/tmp/rafano_cache.json")
try:
    if CACHE_FILE.exists():
        with open(CACHE_FILE,'r') as cf:
            loaded=json.load(cf)
            BROKER_CACHE={k:(v[0],v[1]) for k,v in loaded.get('broker',{}).items()}
except: pass

def save_cache_to_file():
    try:
        data={'broker':{k:[v[0],v[1]] for k,v in BROKER_CACHE.items()},'timestamp':time.time()}
        with open(CACHE_FILE,'w') as cf: json.dump(data,cf)
    except: pass

def make_cache_key(path, params):
    if not params: return path
    try:
        sorted_params=sorted(params.items())
        param_str="&".join([f"{k}={v}" for k,v in sorted_params])
        return f"{path}?{param_str}"
    except: return path

def arjum_get(path, params=None, use_cache=True):
    cache_key = make_cache_key(path, params) if use_cache else None
    if use_cache and cache_key and cache_key in BROKER_CACHE:
        ts,data = BROKER_CACHE[cache_key]
        if time.time()-ts < 300:
            return data
    url = f"{ARJUM_BASE}{path}"
    try:
        api_key = os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or safe_get_env("ARJUM_API_KEY") or ""
        headers = {"X-API-Key": api_key.strip(), "Accept":"application/json","User-Agent":"Mozilla/5.0"}
        r = requests.get(url, headers=headers, params=params, timeout=12)
        if r.status_code==200:
            j=r.json()
            if use_cache and cache_key:
                BROKER_CACHE[cache_key]=(time.time(), j)
                save_cache_to_file()
            return j
        else:
            print(f"⚠ {path} {params} -> {r.status_code}")
            return None
    except Exception as e:
        print(f"arjum_get error {path}: {e}"); return None

def get_screener_latest():
    if 'latest' in SCREENER_CACHE:
        ts,data = SCREENER_CACHE['latest']
        if time.time()-ts < 180: return data
    data = arjum_get("/screener/latest")
    if not data: return []
    if isinstance(data, dict):
        if 'rows' in data and isinstance(data['rows'], list):
            norm=[]
            for r in data['rows']:
                code = r.get('stock_code') or r.get('symbol')
                if code:
                    norm.append({'symbol':code.replace(".JK","").upper(),'raw':r})
            SCREENER_CACHE['latest']=(time.time(), norm)
            return norm
        for k in ['data','results','stocks']:
            if k in data and isinstance(data[k], list): 
                SCREENER_CACHE['latest']=(time.time(), data[k])
                return data[k]
    return data if isinstance(data,list) else []

# ========== NEW VALID LOGIC - TOP AKUM vs TOP DIST ==========
def parse_brokers_list(raw_brokers):
    parsed=[]
    for b in raw_brokers[:30]:
        if not isinstance(b, dict): continue
        code = (b.get('broker_code') or b.get('code') or b.get('broker') or '??').upper()
        bval = float(b.get('bval') or b.get('buy_value') or b.get('buy_val') or 0)
        sval = float(b.get('sval') or b.get('sell_value') or b.get('sell_val') or 0)
        nval = float(b.get('nval') or b.get('net_value') or b.get('net_val') or (bval-sval))
        if bval==0 and sval==0 and nval!=0:
            if nval>0: bval=nval
            else: sval=abs(nval)
        avg = float(b.get('bavg') or b.get('avg_price') or b.get('avg') or 0)
        parsed.append({"broker_code":code,"buy_value":bval,"sell_value":sval,"net_value":nval,"avg_price":avg})
    return parsed

def get_broker_flow_real(symbol, days=None):
    # Coba ambil broker-summary
    params_try = []
    if days:
        params_try.append({"days":days,"broker_limit":20,"flow":"all"})
        params_try.append({"period":days,"broker_limit":20})
    params_try.append({"broker_limit":20,"flow":"all"})
    params_try.append({})

    brokers_raw = []
    for p in params_try:
        data = arjum_get(f"/broker-summary/{symbol}", params=p, use_cache=False)
        if data and isinstance(data, dict):
            lst = data.get('brokers') or data.get('data') or []
            if lst:
                brokers_raw = lst
                break
            if data.get('buy_value') or data.get('bval') or data.get('net_value'):
                brokers_raw = [data]
                break

    # Fallback ke broker-accumulation
    if not brokers_raw:
        try:
            url = f"{ARJUM_BASE}/broker-accumulation/{symbol}"
            api_key = os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or ""
            headers = {"X-API-Key": api_key.strip()}
            params = {"top":20}
            if days: params["days"]=days
            r = requests.get(url, headers=headers, params=params, timeout=12)
            if r.status_code==200:
                j=r.json()
                if j.get('top_buyers'):
                    brokers_raw = j.get('top_buyers',[]) + j.get('top_sellers',[])
                elif j.get('series'):
                    for ser in j.get('series',[])[:20]:
                        if 'broker_code' in ser:
                            pts = ser.get('points') or []
                            if pts:
                                last = pts[-1] if not days else pts[-int(days):]
                                if isinstance(last, list):
                                    sum_b = sum([float(x.get('bval',0)) for x in last])
                                    sum_s = sum([float(x.get('sval',0)) for x in last])
                                    sum_n = sum([float(x.get('nval',0)) for x in last])
                                    brokers_raw.append({"broker_code":ser.get('broker_code'),"bval":sum_b,"sval":sum_s,"nval":sum_n})
                                else:
                                    brokers_raw.append({"broker_code":ser.get('broker_code'),"bval":last.get('bval',0),"sval":last.get('sval',0),"nval":last.get('cum_nval',0) or last.get('nval',0)})
        except Exception as e:
            print(f"acc fallback error {symbol}: {e}")

    if not brokers_raw:
        return 0,0,0,"NEUTRAL",[],[],[]

    parsed = parse_brokers_list(brokers_raw)
    akum = [x for x in parsed if x['net_value']>0]
    dist = [x for x in parsed if x['net_value']<0]

    akum = sorted(akum, key=lambda x: x['net_value'], reverse=True)
    dist = sorted(dist, key=lambda x: abs(x['net_value']), reverse=True)

    top3_akum = akum[:3]
    top3_dist = dist[:3]

    top3_akum_val = sum([x['buy_value'] or x['net_value'] for x in top3_akum])
    top3_dist_val = sum([x['sell_value'] or abs(x['net_value']) for x in top3_dist])

    # === VALIDASI INTI ===
    if top3_dist_val > top3_akum_val and top3_dist_val>0:
        status="DIST"
        net_dom = top3_akum_val - top3_dist_val  # negatif
    elif top3_akum_val > top3_dist_val and top3_akum_val>0:
        status="AKUM"
        net_dom = top3_akum_val - top3_dist_val
    else:
        status="NEUTRAL"
        net_dom=0

    print(f"✅ FLOW {symbol} {days or 1}D: AKUM {top3_akum_val/1e9:.2f}B { [x['broker_code'] for x in top3_akum]} vs DIST {top3_dist_val/1e9:.2f}B { [x['broker_code'] for x in top3_dist]} => {status}")

    return top3_akum_val, top3_dist_val, net_dom, status, top3_akum, top3_dist, parsed

def get_broker_multi_tf(symbol, hist_df=None):
    akum_d, dist_d, net_d, status_d, top_akum_d, top_dist_d, all_d = get_broker_flow_real(symbol, days=1)
    akum_5d, dist_5d, net_5d, status_5d, top_akum_5d, top_dist_5d, all_5d = get_broker_flow_real(symbol, days=5)
    akum_20d, dist_20d, net_20d, status_20d, top_akum_20d, top_dist_20d, all_20d = get_broker_flow_real(symbol, days=20)

    vsa_1d=vsa_5d=vsa_20d=0
    if hist_df is not None and len(hist_df)>=5:
        try:
            if 'Net_Val_VSA' not in hist_df.columns:
                hist_df,_=calculate_vsa_metrics(hist_df)
            vsa_1d=float(hist_df['Net_Val_VSA'].iloc[-1] * hist_df['Close'].iloc[-1])
            vsa_5d=float((hist_df['Net_Val_VSA'].tail(5) * hist_df['Close'].tail(5)).sum())
            vsa_20d=float((hist_df['Net_Val_VSA'].tail(20) * hist_df['Close'].tail(20)).sum())
        except: pass

    # Anti fake akum
    if status_d=="AKUM" and status_5d=="DIST" and status_20d=="DIST" and akum_d < (dist_5d*0.3):
        status_d="FAKE AKUM"

    def avg_from_list(lst, hist, days_slice):
        try:
            vals=[x['avg_price'] for x in lst if x['avg_price']>0]
            if vals: return float(np.mean(vals))
            if hist is not None:
                return float((hist['Close'].tail(days_slice)*hist['Volume'].tail(days_slice)).sum()/hist['Volume'].tail(days_slice).sum()) if hist['Volume'].tail(days_slice).sum()>0 else hist['Close'].iloc[-1]
        except: pass
        return 0

    return {
        "buy_d":float(akum_d),"sell_d":float(dist_d),"net_d":float(net_d),"status_d":status_d,
        "buy_5d":float(akum_5d),"sell_5d":float(dist_5d),"net_5d":float(net_5d),"status_5d":status_5d,
        "buy_20d":float(akum_20d),"sell_20d":float(dist_20d),"net_20d":float(net_20d),"status_20d":status_20d,
        "avg_d":avg_from_list(top_akum_d or all_d, hist_df, 1),
        "avg_5d":avg_from_list(top_akum_5d or all_5d, hist_df, 5),
        "avg_20d":avg_from_list(top_akum_20d or all_20d, hist_df, 20),
        "brokers":all_d,
        "brokers_5d":all_5d,
        "brokers_20d":all_20d,
        "top_akum_d":top_akum_d,"top_dist_d":top_dist_d,
        "top_akum_5d":top_akum_5d,"top_dist_5d":top_dist_5d,
        "top_akum_20d":top_akum_20d,"top_dist_20d":top_dist_20d,
        "vsa_1d":vsa_1d,"vsa_5d":vsa_5d,"vsa_20d":vsa_20d,
        # kompatibilitas lama
        "accum_d":float(akum_d),"accum_5d":float(akum_5d),"accum_20d":float(akum_20d),
        "status":status_d
    }

def format_top_brokers_VALID(top_list):
    if not top_list: return "-"
    parts=[]
    for b in top_list[:3]:
        val = b['buy_value'] if b['net_value']>0 else b['sell_value']
        if val==0: val=abs(b['net_value'])
        if abs(val)>=1e9: s=f"{val/1e9:.1f}B"
        elif abs(val)>=1e6: s=f"{val/1e6:.0f}M"
        else: s=f"{val/1e3:.0f}K"
        parts.append(f"{b['broker_code']} {s}")
    return ", ".join(parts)

def get_history_pro(symbol, limit=150, timeframe="1d"):
    hist_key=f"{symbol}_{timeframe}_{limit}"
    if hist_key in HISTORY_CACHE:
        ts,data = HISTORY_CACHE[hist_key]
        if time.time()-ts < 600: return data
    tf = timeframe.lower().strip()
    frame_map={"1m":"1min","5m":"5min","15m":"15min","30m":"30min","1h":"1hour","4h":"4hour","1d":"daily","1w":"weekly"}
    arjum_frame=frame_map.get(tf,"daily")
    data=arjum_get(f"/history/{symbol}", params={"limit":limit,"frame":arjum_frame})
    rows=[]
    if data:
        if isinstance(data, dict): rows=data.get('data') or data.get('history') or []
        elif isinstance(data, list): rows=data
    if not rows:
        try:
            import yfinance as yf
            yf_map={"1m":("7d","1m"),"5m":("5d","5m"),"15m":("5d","15m"),"30m":("1mo","30m"),"1h":("1mo","60m"),"4h":("3mo","90m"),"1d":("6mo","1d"),"1w":("1y","1wk")}
            period,interval=yf_map.get(tf,("6mo","1d"))
            hist=yf.Ticker(f"{symbol}.JK").history(period=period, interval=interval, timeout=10)
            if hist is not None and len(hist)>10:
                HISTORY_CACHE[hist_key]=(time.time(), hist.tail(limit))
                return hist.tail(limit)
        except Exception as e:
            print(f"yfinance error {symbol}: {e}")
            return None
    try:
        df=pd.DataFrame(rows)
        rename={}
        for c in df.columns:
            cl=str(c).lower()
            if cl in ['o','open']: rename[c]='Open'
            elif cl in ['h','high']: rename[c]='High'
            elif cl in ['l','low']: rename[c]='Low'
            elif cl in ['c','close']: rename[c]='Close'
            elif cl in ['v','volume']: rename[c]='Volume'
            elif cl in ['date','time','t']: rename[c]='Date'
        df.rename(columns=rename,inplace=True)
        if 'Date' in df.columns:
            df['Date']=pd.to_datetime(df['Date'])
            df.set_index('Date',inplace=True)
        df=df.sort_index()
        for col in ['Open','High','Low','Close','Volume']:
            if col in df.columns: df[col]=pd.to_numeric(df[col],errors='coerce')
        df=df.dropna(subset=['Close'])
        if len(df)<10: return None
        HISTORY_CACHE[hist_key]=(time.time(), df)
        return df
    except Exception as e:
        print(f"history parse {symbol}: {e}"); return None

def get_analysis(symbol):
    data=arjum_get(f"/analysis/{symbol}")
    return data if isinstance(data,dict) else {}

# ========== CHART (sama tapi pakai extra_info baru) ==========
def generate_pro_chart(df, symbol="BBCA", timeframe="1d", sector_info="IHSG", output_filename="chart.png", extra_info=None):
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        extra_info=extra_info or {}
        df=df.copy().ffill().bfill()
        df=df.sort_index()
        df['EMA13']=df['Close'].ewm(span=13, adjust=False).mean()
        df['EMA20']=df['Close'].ewm(span=20, adjust=False).mean()
        df['EMA50']=df['Close'].ewm(span=50, adjust=False).mean()
        df['EMA200']=df['Close'].ewm(span=200, adjust=False).mean()
        df['V1']=df['Volume'].rolling(20, min_periods=1).mean()
        df['V2']=df['Volume'].rolling(50, min_periods=1).mean()
        df, buy_ratios = calculate_vsa_metrics(df)
        last_close, last_open, last_high, last_low = df['Close'].iloc[-1], df['Open'].iloc[-1], df['High'].iloc[-1], df['Low'].iloc[-1]
        last_vol, prev_close = df['Volume'].iloc[-1], df['Close'].iloc[-2] if len(df)>1 else df['Close'].iloc[-1]
        chg_pct = ((last_close/prev_close)-1)*100 if prev_close else 0
        avg_price, vchg1 = df['Close'].tail(20).mean(), (last_vol/df['Volume'].iloc[-2]) if len(df)>1 and df['Volume'].iloc[-2]>0 else 1
        vchg5 = (last_vol/df['Volume'].tail(5).mean()) if df['Volume'].tail(5).mean()>0 else 1
        speed="FAST" if vchg1>2.0 else "SLOW" if vchg1<0.8 else "NORMAL"
        buy_pct_temp=int(buy_ratios[-1]*100)
        power="TURBO" if buy_pct_temp>=85 and vchg1>=1.2 else "STRONG" if buy_pct_temp>=70 or vchg1>=1.5 else "NORMAL" if buy_pct_temp>=60 else "WEAK"
        safety="GOOD" if last_close>df['EMA200'].iloc[-1] else "BAD"
        ema13, ema20, ema50, ema200 = df['EMA13'].iloc[-1], df['EMA20'].iloc[-1], df['EMA50'].iloc[-1], df['EMA200'].iloc[-1]
        buy_pct, sell_pct = int(buy_ratios[-1]*100), 100-int(buy_ratios[-1]*100)
        net_vol, net_vol_5d = df['Net_Vol_VSA'].iloc[-1], df['Net_Vol_VSA'].tail(5).sum()
        real_net = extra_info.get('broker_net',0) or extra_info.get('multi_tf',{}).get('net_d',0)
        nbsa_rp = abs(real_net) if real_net!=0 else abs(net_vol*last_close)
        plt.style.use('dark_background')
        fig=plt.figure(figsize=(16,9), dpi=150, facecolor='#000000')
        gs=gridspec.GridSpec(4,1,height_ratios=[4.5,1.1,0.9,0.8],hspace=0.05)
        ax_main=fig.add_subplot(gs[0]); ax_vol=fig.add_subplot(gs[1],sharex=ax_main); ax_nbsa=fig.add_subplot(gs[2],sharex=ax_main); ax_mm=fig.add_subplot(gs[3],sharex=ax_main)
        fig.subplots_adjust(left=0.08,right=0.92,top=0.88,bottom=0.06)
        for ax in [ax_main,ax_vol,ax_nbsa,ax_mm]:
            ax.set_facecolor('#000000'); ax.tick_params(colors='#aaaaaa',labelsize=8); ax.yaxis.tick_right(); ax.grid(False)
        x=np.arange(len(df))
        multi_for_signals=extra_info.get('multi_tf')
        buy_signals,_=detect_buy_signals(df, multi_for_signals)
        sell_signals,_=detect_sell_signals(df, multi_for_signals)
        extra_info['_chart_buy_signals']=buy_signals; extra_info['_chart_sell_signals']=sell_signals
        plot_df=df
        if 'ATR' not in plot_df.columns: plot_df['ATR']=calculate_atr(plot_df,14)
        if 'BB_UPPER' not in plot_df.columns:
            _, bb_up, bb_low=calculate_bollinger_bands(plot_df,20,2); plot_df['BB_UPPER']=bb_up; plot_df['BB_LOWER']=bb_low
        for i in range(len(df)):
            o,h,l,c=df['Open'].iloc[i],df['High'].iloc[i],df['Low'].iloc[i],df['Close'].iloc[i]
            ax_main.plot([i,i],[l,h],color='#00ff00' if c>=o else '#ff0000',linewidth=0.8,alpha=0.8)
            body_low=min(o,c); body_h=max(0.5,abs(c-o))
            if c>=o: rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor='none',edgecolor='#00ff00',linewidth=0.8)
            else: rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor='#ff3333',edgecolor='#ff3333',linewidth=0.8)
            ax_main.add_patch(rect)
        ax_main.plot(x,df['EMA13'],color='#ffff00',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA20'],color='#ff0000',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA50'],color='#ffffff',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA200'],color='#a020f0',linewidth=1.2,alpha=0.9)
        if 'BB_UPPER' in plot_df.columns:
            ax_main.plot(x,plot_df['BB_UPPER'],color='#8888ff',linewidth=0.8,alpha=0.4,linestyle='--')
            ax_main.plot(x,plot_df['BB_LOWER'],color='#8888ff',linewidth=0.8,alpha=0.4,linestyle='--')
        if buy_signals:
            for sig in buy_signals:
                idx=sig['index']
                if idx<len(df):
                    low=df['Low'].iloc[idx]; atr=plot_df['ATR'].iloc[idx] if not pd.isna(plot_df['ATR'].iloc[idx]) else df['Close'].iloc[idx]*0.02
                    ax_main.annotate('▲',xy=(idx,low-atr*0.6),fontsize=14,color='#00ff00',fontweight='bold',ha='center')
        if sell_signals:
            for sig in sell_signals:
                idx=sig['index']
                if idx<len(df):
                    high=df['High'].iloc[idx]; atr=plot_df['ATR'].iloc[idx] if not pd.isna(plot_df['ATR'].iloc[idx]) else df['Close'].iloc[idx]*0.02
                    ax_main.annotate('▼',xy=(idx,high+atr*0.6),fontsize=14,color='#ff0000',fontweight='bold',ha='center')
        ax_main.set_xlim(-1,len(df)); ax_main.set_ylim(df['Low'].min()*0.95,df['High'].max()*1.08)
        left_text=f"Avg Price : {avg_price:,.1f}\nVchg 1 Day: {vchg1:.1f} x\nVchg 5 Days: {vchg5:.1f} x\nSpeed : {speed}\nPower : {power}\nSafety : {safety}\n\nEMA 13 : {ema13:,.1f}\nEMA 20 : {ema20:,.1f}\nEMA 50 : {ema50:,.1f}\nEMA 200: {ema200:,.1f}"
        ax_main.text(0.01,0.98,left_text,transform=ax_main.transAxes,va='top',ha='left',fontsize=8,family='monospace',color='#e0e0e0',bbox=dict(facecolor='black',alpha=0.6,edgecolor='none'))
        fig.text(0.01,0.96,f"{symbol} :    {last_close:.0f} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=13,fontweight='bold',ha='left')
        fig.text(0.5,0.96,"RAFANO TRADER V3.1 FIX DIST",color='white',fontsize=12,fontweight='bold',ha='center')
        date_str=df.index[-1].strftime('%d %b %Y') if hasattr(df.index[-1],'strftime') else get_now_wib().strftime('%d %b %Y')
        fig.text(0.99,0.96,f"Daily {date_str}",color='#ffcc00',fontsize=10,ha='right')
        fig.text(0.01,0.905,f"High:{last_high:.0f}   Low:{last_low:.0f}   Open:{last_open:.0f}   Volume:{last_vol:,.0f}   V1:{df['V1'].iloc[-1]:,.0f}",color='#00ffff',fontsize=8,ha='left')
        vol_info=f"Buy%={buy_pct}% Sell%={sell_pct}% Net Vol={net_vol:,.0f} Net 5D={net_vol_5d:,.0f}"
        ax_vol.text(0.005,0.88,vol_info,transform=ax_vol.transAxes,color='#ffffff',fontsize=8,va='top')
        ax_vol.bar(x,df['Vol_Sell'],color='#cc0000',width=0.8,alpha=0.8)
        ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00cc00',width=0.8,alpha=0.9)
        ax_vol.plot(x,df['V1'],color='white',linewidth=0.8,alpha=0.9)
        ax_vol.set_ylim(0,df['Volume'].max()*1.8)
        plt.setp(ax_vol.get_xticklabels(),visible=False)
        nbsa_info=f"NBSA Rp. {nbsa_rp/1e9:.2f} B"
        ax_nbsa.text(0.005,0.85,nbsa_info,transform=ax_nbsa.transAxes,color='#ffffff',fontsize=8,va='top')
        nbsa_vals=df['Net_Vol_VSA'].tail(80)/(df['Net_Vol_VSA'].abs().max() or 1)*50
        x_nbsa=np.arange(len(df)-len(nbsa_vals),len(df))
        for i,v in zip(x_nbsa,nbsa_vals):
            col='#00ffff' if v>=0 else '#ff4444'; ax_nbsa.bar(i,v,color=col,width=0.6)
        ax_nbsa.axhline(0,color='#444444',linewidth=0.5)
        ax_nbsa.set_ylim(-60,60)
        ax_mm.text(0.005,0.85,"Market Maker",transform=ax_mm.transAxes,color='#ffffff',fontsize=8,va='top')
        if 'MM' not in df.columns: df['MM']=(df['Close']-df['EMA50'])/df['EMA50']*1000
        mm_vals=df['MM'].tail(80); x_mm=np.arange(len(df)-len(mm_vals),len(df))
        ax_mm.bar(x_mm,mm_vals,color='#cccccc',width=0.5,alpha=0.8)
        ax_mm.set_ylim(df['MM'].min()*1.2-10,df['MM'].max()*1.2+10)
        step=max(1,len(df)//8)
        ax_mm.set_xticks(x[::step]); ax_mm.set_xticklabels([df.index[i].strftime('%b') if hasattr(df.index[i],'strftime') else str(i) for i in range(0,len(df),step)],fontsize=7)
        plt.savefig(output_filename,dpi=150,bbox_inches='tight',facecolor='#000000')
        return output_filename
    except Exception as e:
        print(f"Chart error {e}"); import traceback; traceback.print_exc(); return None
    finally:
        try: plt.clf(); plt.close('all')
        except: pass

# ========== SCANNER V3.1 ==========
LAST_SENT_SIGNALS={}
COOLDOWN_SECONDS=3600
LAST_RESET_DATE=""

def filter_signals_with_cooldown(signals):
    global LAST_RESET_DATE, LAST_SENT_SIGNALS
    ct=time.time()
    today=get_now_wib().strftime('%Y-%m-%d')
    if LAST_RESET_DATE!=today:
        LAST_SENT_SIGNALS.clear(); LAST_RESET_DATE=today
    filtered=[]
    for sig in signals:
        sym=sig['symbol']
        if (ct-LAST_SENT_SIGNALS.get(sym,0))>=COOLDOWN_SECONDS:
            filtered.append(sig); LAST_SENT_SIGNALS[sym]=ct
    return filtered

def calculate_score_v2(symbol, history_df, multi):
    score=0; reasons=[]
    score+=30; reasons.append("Screener")
    buy_d=multi.get('buy_d',0); sell_d=multi.get('sell_d',0)
    net_d=multi.get('net_d',0); status_d=multi.get('status_d','NEUTRAL')
    # VALID score: hanya AKUM yang dapat poin besar
    if status_d=="AKUM":
        if buy_d>20_000_000_000: score+=30; reasons.append(f"AKUM {buy_d/1e9:.1f}B")
        elif buy_d>5_000_000_000: score+=20; reasons.append(f"AKUM {buy_d/1e9:.1f}B")
        elif buy_d>0: score+=10; reasons.append("Akum Tipis")
        if net_d>10_000_000_000: score+=20; reasons.append(f"Net {net_d/1e9:.1f}B")
        elif net_d>0: score+=10; reasons.append("Net+")
    elif status_d=="DIST":
        # Distribusi dikasih score rendah, biar tidak masuk sinyal BUY, tapi tetap bisa di-monitor
        score-=20; reasons.append(f"DIST {sell_d/1e9:.1f}B")
    # Trend
    try:
        if history_df is not None and len(history_df)>50:
            ema50=history_df['Close'].ewm(span=50).mean().iloc[-1]
            if history_df['Close'].iloc[-1]>ema50: score+=15; reasons.append(">EMA50")
    except: pass
    if score>=85: label="VERY STRONG"
    elif score>=70: label="STRONG BUY"
    elif score>=50: label="WEAK BUY"
    else: label="NO SIGNAL"
    return score,label,reasons

def scan_v3():
    print(f"[{get_now_wib()}] 🚀 V3.1 Scan FIX DIST...")
    screener_data=get_screener_latest()
    if not screener_data:
        candidates=["BBCA","BBRI","BMRI","TLKM","ASII","GOTO","AMMN","ADRO","ANTM","MDKA","BBNI","BRIS","UNTR","ICBP"]
        screener_map={s:{} for s in candidates}
        is_fallback=True
    else:
        candidates=[]; screener_map={}
        for item in screener_data:
            sym=item.get('symbol') or item.get('code')
            if sym:
                sym=sym.replace(".JK","").upper()
                candidates.append(sym); screener_map[sym]=item
        candidates=candidates[:30]
        is_fallback=False
    detected=[]
    def process_symbol(sym):
        try:
            hist_df=get_history_pro(sym, limit=120, timeframe="1d")
            multi=get_broker_multi_tf(sym, hist_df)
            score,label,reasons=calculate_score_v2(sym, hist_df, multi)
            # Threshold: hanya AKUM yang boleh lewat
            threshold=30 if is_fallback else 40
            # Weekend lebih rendah
            if get_now_wib().weekday()>=5: threshold=20
            # Filter DIST: jangan masuk kalau status DIST
            if multi.get('status_d')=="DIST" and score<70:
                # tetap simpan untuk /top dist tapi tidak untuk broadcast BUY
                pass
            if score>=threshold or multi.get('status_d') in ["AKUM","DIST"]:
                last_close=0; change_pct=0
                if hist_df is not None and len(hist_df)>=2:
                    last_close=int(hist_df['Close'].iloc[-1])
                    prev=hist_df['Close'].iloc[-2]
                    change_pct=((last_close/prev)-1)*100 if prev else 0
                tp=calculate_trading_plan(hist_df, multi_tf=multi) if hist_df is not None else None
                return {"symbol":sym,"close":last_close,"change_pct":change_pct,"score":score,"score_label":label,"accum_value":multi.get('buy_d',0),"broker_net":multi.get('net_d',0),"broker_status":multi.get('status_d'),"reasons":reasons,"history_df":hist_df,"trading_plan":tp,"brokers":multi.get('brokers',[]),"multi_tf":multi}
        except Exception as e:
            print(f"Error {sym}: {e}")
        return None
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures=[executor.submit(process_symbol,s) for s in candidates]
        for f in futures:
            res=f.result()
            if res: detected.append(res)
    detected.sort(key=lambda x: x['score'], reverse=True)
    print(f"✅ V3.1 Scan: {len(detected)} sinyal (AKUM+DIST valid)")
    return detected

def send_reply(chat_id, text, reply_markup=None):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload={"chat_id":chat_id,"text":text,"parse_mode":"Markdown"}
    if reply_markup: payload["reply_markup"]=reply_markup
    try: requests.post(url, json=payload, timeout=10)
    except Exception as e: print(f"TG Error: {e}")

def send_photo_reply(chat_id, photo_path, caption=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path,'rb') as photo:
            requests.post(url, data={'chat_id':chat_id,'caption':caption,'parse_mode':'Markdown'}, files={'photo':photo}, timeout=30)
    except Exception as e: print(f"Photo Error: {e}")

def broadcast_v3(signals):
    if not signals:
        send_reply(TARGET_CHAT_ID, "V3.1 Scan: Tidak ada sinyal AKUM valid hari ini.")
        return
    now_str=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header=f"*RAFANO V3.1 FIX DIST - REAL FLOW*\n{now_str}\nTotal: {len(signals)} | Cooldown 60m\n============================\n\n"
    msg=header; keyboard=[]
    for idx,item in enumerate(signals,1):
        multi=item.get('multi_tf') or {}
        st_d=multi.get('status_d','NEUTRAL'); st_5d=multi.get('status_5d','NEUTRAL'); st_20d=multi.get('status_20d','NEUTRAL')
        emoji_d="🟢" if st_d=="AKUM" else "🔴" if st_d=="DIST" else "⚪"
        emoji_5d="🟢" if st_5d=="AKUM" else "🔴" if st_5d=="DIST" else "⚪"
        emoji_20d="🟢" if st_20d=="AKUM" else "🔴" if st_20d=="DIST" else "⚪"
        top_akum_d=format_top_brokers_VALID(multi.get('top_akum_d',[]))
        top_dist_d=format_top_brokers_VALID(multi.get('top_dist_d',[]))
        top_akum_5d=format_top_brokers_VALID(multi.get('top_akum_5d',[]))
        top_dist_5d=format_top_brokers_VALID(multi.get('top_dist_5d',[]))
        top_akum_20d=format_top_brokers_VALID(multi.get('top_akum_20d',[]))
        top_dist_20d=format_top_brokers_VALID(multi.get('top_dist_20d',[]))
        daily_str=f"{emoji_d} Daily: {st_d} | Buy {format_large_number(multi.get('buy_d',0),True)} Sell {format_large_number(multi.get('sell_d',0),True)} Net {format_large_number(multi.get('net_d',0),True)}"
        daily_akum=f"   |  └ Top Akum: {top_akum_d}"
        daily_dist=f"   |  └ Top Dist: {top_dist_d}"
        weekly_str=f"{emoji_5d} Weekly 5D: {st_5d} | Buy {format_large_number(multi.get('buy_5d',0),True)} Sell {format_large_number(multi.get('sell_5d',0),True)} Net {format_large_number(multi.get('net_5d',0),True)}"
        monthly_str=f"{emoji_20d} Monthly 20D: {st_20d} | Buy {format_large_number(multi.get('buy_20d',0),True)} Sell {format_large_number(multi.get('sell_20d',0),True)} Net {format_large_number(multi.get('net_20d',0),True)}"
        reasons_str=" | ".join(item.get('reasons',[])[:3])
        item_str=f"{idx}. *{item['symbol']}* -- {item['close']} ({item['change_pct']:+.2f}%)\n   |- Score: {item['score']}% ({item['score_label']})\n   |- {daily_str}\n{daily_akum}\n{daily_dist}\n   |- {weekly_str}\n   |   Top Akum: {top_akum_5d} | Top Dist: {top_dist_5d}\n   |- {monthly_str}\n   |   Top Akum: {top_akum_20d} | Top Dist: {top_dist_20d}\n   +- {reasons_str}\n\n"
        keyboard.append([{"text":f"Pro Chart {item['symbol']}", "callback_data":f"chart_{item['symbol']}_1d"}])
        if len(msg)+len(item_str)>3500:
            send_reply(TARGET_CHAT_ID, msg, reply_markup={"inline_keyboard":keyboard})
            msg=item_str; keyboard=[]
        else: msg+=item_str
    if msg:
        send_reply(TARGET_CHAT_ID, msg, reply_markup={"inline_keyboard":keyboard})

def process_chart_request(chat_id, stock_code, timeframe="1d", extra_info_cache=None):
    send_reply(chat_id, f"📊 *Generating Pro Chart {stock_code.upper()} ({timeframe.upper()}) + REAL FLOW...*")
    df=get_history_pro(stock_code, limit=150, timeframe=timeframe)
    if df is None or len(df)<20:
        send_reply(chat_id, f"⚠ Data {stock_code} tidak ketemu TF {timeframe}"); return
    if extra_info_cache and stock_code in extra_info_cache and 'multi_tf' in extra_info_cache[stock_code]:
        extra=extra_info_cache[stock_code]
        multi=extra.get('multi_tf',{})
    else:
        multi=get_broker_multi_tf(stock_code, df)
        extra={"accum_value":multi.get('buy_d',0),"broker_net":multi.get('net_d',0),"brokers":multi.get('brokers',[]),"multi_tf":multi}
    chart_file=f"chart_{stock_code.upper()}_{timeframe}_{int(time.time())}.png"
    try:
        file_path=generate_pro_chart(df=df, symbol=stock_code.upper(), timeframe=timeframe, sector_info=f"{stock_code.upper()} | IHSG", output_filename=chart_file, extra_info=extra)
        buy_sigs=extra.get('_chart_buy_signals',[]); sell_sigs=extra.get('_chart_sell_signals',[])
        tp=calculate_trading_plan(df, signals=buy_sigs+sell_sigs, multi_tf=multi)
        daily_line=f"{multi.get('status_d')} Buy {format_large_number(multi.get('buy_d',0),True)} Sell {format_large_number(multi.get('sell_d',0),True)} Net {format_large_number(multi.get('net_d',0),True)}"
        daily_akum_line=f"Top Akum: {format_top_brokers_VALID(multi.get('top_akum_d',[]))}"
        daily_dist_line=f"Top Dist: {format_top_brokers_VALID(multi.get('top_dist_d',[]))}"
        if tp:
            sig_type=tp.get('signal_type','NO SIGNAL'); side=tp.get('side','WAIT'); sig_strength=tp.get('signal_strength',0)
            sig_emoji="🟢" if tp.get('is_buy_signal') else "🔴" if tp.get('is_sell_signal') else "⚪"
            rec=f"✅ {sig_type} - BUY NOW" if tp.get('is_buy_signal') else f"🔴 {sig_type} - SELL" if tp.get('is_sell_signal') else "⏸ WAIT"
            if side=="WAIT":
                caption=f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])} | {tp['trend']}\nScore: {extra.get('score',0)}% | {sig_emoji} {sig_type} | {side}\nDaily: {daily_line}\n{daily_akum_line}\n{daily_dist_line}\nTF: {timeframe.upper()} | {tp['signal_reason']}\n------------------\n{rec}\nSup: {tp['support']} | Res: {tp['resistance']}"
            else:
                caption=f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])} | {tp['trend']}\nScore: {extra.get('score',0)}% | {sig_emoji} {sig_type} ({sig_strength}%) | {side}\nDaily: {daily_line}\n{daily_akum_line}\n{daily_dist_line}\nTF: {timeframe.upper()}\n------------------\n{rec}\nEntry: {tp['entry']} | SL: {tp['sl']} ({tp['risk_pct']}%)\nTP1: {tp['tp1']} (RR {tp['rr1']}) | TP2: {tp['tp2']} (RR {tp['rr2']})"
        else:
            caption=f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])}\nDaily: {daily_line}\n{daily_akum_line}\n{daily_dist_line}\nTF: {timeframe.upper()}"
        send_photo_reply(chat_id, file_path, caption=caption)
        if os.path.exists(file_path): os.remove(file_path)
    except Exception as e:
        import traceback; traceback.print_exc(); send_reply(chat_id, f"❌ Gagal render: `{e}`")

LAST_SIGNALS_CACHE={}
def telegram_bot_listener():
    global LAST_SIGNALS_CACHE
    offset=0
    print("🤖 Telegram Listener V3.1 Running...")
    try: requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
    except: pass
    try:
        r=requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe", timeout=10)
        print(f"✅ Bot Info: {r.json().get('result',{})}")
    except Exception as e: print(f"❌ Bot token error: {e}")
    while True:
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res=requests.get(url, timeout=25)
            if res.status_code!=200:
                print(f"⚠ getUpdates {res.status_code}"); time.sleep(3); continue
            data=res.json()
            for update in data.get("result",[]):
                offset=update["update_id"]+1
                if "callback_query" in update:
                    cb=update["callback_query"]; cb_id=cb.get("id"); cb_data=cb.get("data",""); chat_id=cb["message"]["chat"]["id"]
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery", json={"callback_query_id":cb_id})
                    if cb_data.startswith("chart_"):
                        parts=cb_data.split("_")
                        if len(parts)>=3:
                            threading.Thread(target=process_chart_request, args=(chat_id, parts[1], parts[2], LAST_SIGNALS_CACHE)).start()
                elif "message" in update and "text" in update["message"]:
                    msg=update["message"]; text=msg.get("text","").strip(); chat_id=msg["chat"]["id"]; first_word=text.split()[0].lower() if text else ""
                    print(f"📩 {text} dari {chat_id}")
                    if first_word in ["/start","/help"]:
                        help_msg="🤖 *RAFANO V3.1 FIX DIST*\n============================\n📈 *CHART* `/c <KODE> [TF]` `/c BBCA` `/c ANTM 15m`\n🏦 *BANDAR* `/b <KODE>` - Detail Akum vs Dist\n📊 *INFO* `/info <KODE>` `/trend <KODE>`\n🔍 *SCREENER* `/scan` `/top [N] [akum/dist]` `/compare A B`\n⭐ *WATCHLIST* `/wl` `/wl add BBCA` `/wl scan`\n🛠 `/clearcache` `/cc`\n"
                        send_reply(chat_id, help_msg)
                    elif first_word in ["/c","/chart"]:
                        parts=text.split()
                        if len(parts)>=2:
                            threading.Thread(target=process_chart_request, args=(chat_id, parts[1].upper(), parts[2] if len(parts)>=3 else "1d", LAST_SIGNALS_CACHE)).start()
                        else: send_reply(chat_id, "⚠ Format: `/c <KODE> [TF]`")
                    elif first_word in ["/b","/broker","/bandar"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            def broker_detail(tchat, symbol):
                                try:
                                    multi=get_broker_multi_tf(symbol)
                                    msg=f"🏦 *BROKER FLOW VALID {symbol}* -- {get_now_wib().strftime('%d %b %H:%M')}\n"
                                    msg+=f"Daily: {multi.get('status_d')} | Buy {format_large_number(multi.get('buy_d',0),True)} Sell {format_large_number(multi.get('sell_d',0),True)} Net {format_large_number(multi.get('net_d',0),True)}\n"
                                    msg+=f"  Top Akum: {format_top_brokers_VALID(multi.get('top_akum_d',[]))}\n"
                                    msg+=f"  Top Dist: {format_top_brokers_VALID(multi.get('top_dist_d',[]))}\n\n"
                                    msg+=f"Weekly 5D: {multi.get('status_5d')} | Net {format_large_number(multi.get('net_5d',0),True)}\n"
                                    msg+=f"  Top Akum: {format_top_brokers_VALID(multi.get('top_akum_5d',[]))}\n"
                                    msg+=f"  Top Dist: {format_top_brokers_VALID(multi.get('top_dist_5d',[]))}\n\n"
                                    msg+=f"Monthly 20D: {multi.get('status_20d')} | Net {format_large_number(multi.get('net_20d',0),True)}\n"
                                    msg+=f"  Top Akum: {format_top_brokers_VALID(multi.get('top_akum_20d',[]))}\n"
                                    msg+=f"  Top Dist: {format_top_brokers_VALID(multi.get('top_dist_20d',[]))}\n"
                                    send_reply(tchat, msg)
                                except Exception as e: send_reply(tchat, f"❌ Error broker {symbol}: {e}")
                            threading.Thread(target=broker_detail, args=(chat_id, sym)).start()
                    elif first_word in ["/top"]:
                        parts=text.split(); n=10; filter_status=None
                        if len(parts)>=2:
                            try: n=int(parts[1]); 
                            except: filter_status=parts[1].upper()
                            if len(parts)>=3: filter_status=parts[2].upper()
                        def top_accum(tchat, limit, status_filter):
                            try:
                                sigs=LAST_SIGNALS_CACHE.values() if LAST_SIGNALS_CACHE else scan_v3()
                                if isinstance(sigs, dict): sigs=list(sigs.values())
                                def get_net(x): return abs(x.get('multi_tf',{}).get('net_d',0))
                                sorted_sigs=sorted(sigs, key=get_net, reverse=True)
                                if status_filter: sorted_sigs=[s for s in sorted_sigs if s.get('multi_tf',{}).get('status_d','')==status_filter]
                                msg=f"🏆 *TOP {limit} {status_filter or 'FLOW'}*\n\n"
                                for idx,item in enumerate(sorted_sigs[:limit],1):
                                    multi=item.get('multi_tf',{}); sym=item.get('symbol','??'); status=multi.get('status_d',''); emoji="🟢" if status=="AKUM" else "🔴" if status=="DIST" else "⚪"
                                    msg+=f"{idx}. {emoji} *{sym}* {status} Net {format_large_number(multi.get('net_d',0),True)}\n   Akum: {format_top_brokers_VALID(multi.get('top_akum_d',[]))}\n   Dist: {format_top_brokers_VALID(multi.get('top_dist_d',[]))}\n"
                                send_reply(tchat, msg)
                            except Exception as e: send_reply(tchat, f"❌ Error top: {e}")
                        threading.Thread(target=top_accum, args=(chat_id, n, filter_status)).start()
                    elif first_word in ["/scan","/scanpro"]:
                        send_reply(chat_id, "🔍 *V3.1 Scanning Real Flow Akum vs Dist...*")
                        def manual_scan(is_pro=False, tchat=chat_id):
                            global LAST_SIGNALS_CACHE
                            sigs=scan_v3(); LAST_SIGNALS_CACHE={s['symbol']:s for s in sigs}
                            # broadcast ke requester
                            now_str=get_now_wib().strftime('%d %b %Y %H:%M WIB')
                            if not sigs:
                                send_reply(tchat, f"*RAFANO V3.1* {now_str}\n0 sinyal valid"); return
                            akum_only=[s for s in sigs if s['multi_tf'].get('status_d')=='AKUM']
                            dist_only=[s for s in sigs if s['multi_tf'].get('status_d')=='DIST']
                            msg=f"*RAFANO V3.1 FLOW VALID {now_str}*\nAKUM: {len(akum_only)} | DIST: {len(dist_only)} | Total: {len(sigs)}\n\n"
                            kb=[]
                            # Tampilkan AKUM dulu
                            for idx,item in enumerate(sigs[:15],1):
                                multi=item.get('multi_tf',{}); emoji="🟢" if multi.get('status_d')=="AKUM" else "🔴"
                                msg+=f"{idx}. {emoji} *{item['symbol']}* {multi.get('status_d')} Net {format_large_number(multi.get('net_d',0),True)} | Akum {format_top_brokers_VALID(multi.get('top_akum_d',[]))} vs Dist {format_top_brokers_VALID(multi.get('top_dist_d',[]))}\n"
                                kb.append([{"text":f"Chart {item['symbol']}", "callback_data":f"chart_{item['symbol']}_1d"}])
                            send_reply(tchat, msg, reply_markup={"inline_keyboard":kb})
                        is_pro_flag=(first_word=="/scanpro")
                        threading.Thread(target=manual_scan, args=(is_pro_flag, chat_id)).start()
                    elif first_word in ["/clearcache","/cc","/clear"]:
                        BROKER_CACHE.clear(); HISTORY_CACHE.clear(); LAST_SIGNALS_CACHE.clear()
                        try: os.remove("/tmp/rafano_cache.json")
                        except: pass
                        send_reply(chat_id, "🧹 Cache cleared")
        except Exception as e:
            print(f"Listener error: {e}"); time.sleep(3)

def auto_screener_loop():
    global LAST_SIGNALS_CACHE
    print("🚀 Auto Screener V3.1 Active...")
    last_sesi1, last_eod="", ""
    while True:
        try:
            if not is_market_open(): time.sleep(300); continue
            now=get_now_wib(); today_str, cur=now.strftime('%Y-%m-%d'), now.strftime('%H:%M'); weekday=now.weekday()
            target_sesi1="11:25" if weekday==4 else "11:55"
            if cur==target_sesi1 and last_sesi1!=today_str:
                sigs=scan_v3(); LAST_SIGNALS_CACHE={s['symbol']:s for s in sigs}; broadcast_v3(filter_signals_with_cooldown(sigs)); last_sesi1=today_str
            if cur=="15:55" and last_eod!=today_str:
                sigs=scan_v3(); LAST_SIGNALS_CACHE={s['symbol']:s for s in sigs}; broadcast_v3(filter_signals_with_cooldown(sigs)); last_eod=today_str
            sigs=scan_v3(); LAST_SIGNALS_CACHE={s['symbol']:s for s in sigs}
            filt=filter_signals_with_cooldown([s for s in sigs if s['multi_tf'].get('status_d')=='AKUM'])
            if filt: broadcast_v3(filt)
            time.sleep(600)
        except Exception as e:
            print(f"Auto loop error: {e}"); time.sleep(10)

if __name__=="__main__":
    print("==========================================")
    print("🔥 RAFANO V3.1 FIX DIST STARTING...")
    print("==========================================")
    threading.Thread(target=auto_screener_loop, daemon=True).start()
    telegram_bot_listener()
