"""
RAFANO V3.9 - CHART FIX TOTAL + BROKER CODE DI CHART
- Fix TF parsing: /c BBCA, /c BBCA 5, /c BBCA 5m, /c BBCA 1h semua kebaca
- Fix data: pakai Arjum history dulu, baru yfinance fallback
- Fix chart: candle bener, EMA bener, Volume Buy/Sell bener, broker code tampil di chart
"""
import os, time, logging, datetime, threading, requests, pytz, json
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.gridspec as gridspec
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')

def safe_get_env(key: str):
    v = os.getenv(key)
    if v: return str(v).strip().strip('"').strip("'")
    try:
        from google.colab import userdata
        vv = userdata.get(key)
        if vv:
            vv = str(vv).strip().strip('"').strip("'")
            os.environ[key]=vv
            return vv
    except: pass
    return None

TELEGRAM_BOT_TOKEN = safe_get_env("TELEGRAM_BOT_TOKEN")
TARGET_CHAT_ID = safe_get_env("TARGET_CHAT_ID")
ARJUM_API_KEY = safe_get_env("ARJUM_API_KEY")
ARJUM_BASE = "https://stock.arjum.com/api"

def get_arjum_headers():
    k = os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or ""
    return {"X-API-Key": k.strip(), "Accept": "application/json", "User-Agent": "RAFANO/3.9-CHARTFIX"}
def get_now_wib(): return datetime.datetime.now(TIMEZONE_WIB)

def get_last_trading_dates(days_back=1):
    now = get_now_wib()
    d = now
    while d.weekday() >= 5:
        d = d - datetime.timedelta(days=1)
    if days_back == 1:
        start = d; end = d
    else:
        delta = int(days_back*1.5)+3
        start = d - datetime.timedelta(days=delta); end = d
    return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'), d.strftime('%Y-%m-%d')

CACHE_LOCK = threading.RLock()
BROKER_CACHE, HISTORY_CACHE = {}, {}
def make_cache_key(path, params):
    if not params: return path
    try: return f"{path}?{'&'.join([f'{k}={v}' for k,v in sorted(params.items())])}"
    except: return path
def get_cached_broker(k):
    with CACHE_LOCK:
        if k in BROKER_CACHE:
            ts,d = BROKER_CACHE[k]
            if time.time()-ts < 300: return d
            del BROKER_CACHE[k]
    return None
def set_cached_broker(k,d):
    with CACHE_LOCK: BROKER_CACHE[k]=(time.time(),d)

def safe_int(val, default=0):
    try:
        if pd.isna(val) or np.isinf(val): return default
        return int(val)
    except: return default
def format_large_number(val, show_sign=False):
    if pd.isna(val) or val==0: return "0"
    abs_val=abs(val); sign="+" if show_sign and val>0 else "-" if val<0 else ""
    if abs_val>=1e9: return f"{sign}{abs_val/1e9:.2f}B"
    if abs_val>=1e6: return f"{sign}{abs_val/1e6:.0f}M"
    if abs_val>=1e3: return f"{sign}{abs_val/1e3:.0f}K"
    return f"{sign}{val:,.0f}"

def parse_timeframe(tf_input):
    """Fix TF parsing: 5, 5m, 5M, 15, 1h, 1H, 1d, D semua kebaca"""
    if not tf_input: return "1d"
    tf = str(tf_input).lower().strip()
    # kalau cuma angka, anggap menit
    if tf.isdigit():
        if tf == "1": return "1m"
        if tf == "5": return "5m"
        if tf == "15": return "15m"
        if tf == "30": return "30m"
        return f"{tf}m"
    # mapping
    mapping = {
        "1m": "1m", "1min": "1m", "1menit": "1m",
        "5m": "5m", "5min": "5m", "5menit": "5m",
        "15m": "15m", "15min": "15m",
        "30m": "30m", "30min": "30m",
        "1h": "1h", "60m": "1h", "1hour": "1h", "1jam": "1h",
        "4h": "4h", "4hour": "4h", "4jam": "4h",
        "1d": "1d", "daily": "1d", "d": "1d", "harian": "1d",
        "1w": "1w", "weekly": "1w", "w": "1w", "mingguan": "1w",
        "1mo": "1mo", "monthly": "1mo", "m": "1mo", "bulanan": "1mo"
    }
    return mapping.get(tf, "1d")

def round_to_ihsg_fraction(price):
    if pd.isna(price) or price<=0: return 0
    price=float(price)
    if price<200: tick=1
    elif price<500: tick=2
    elif price<2000: tick=5
    elif price<5000: tick=10
    else: tick=25
    return int(round(price/tick)*tick)
def calculate_atr(df, period=14):
    tr1=df['High']-df['Low']; tr2=(df['High']-df['Close'].shift(1)).abs(); tr3=(df['Low']-df['Close'].shift(1)).abs()
    tr=pd.concat([tr1,tr2,tr3],axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=1).mean()

MIN_TRX_THRESHOLD = 500_000_000
def determine_status(net_value, buy_value, sell_value, min_trx=MIN_TRX_THRESHOLD):
    if abs(net_value) < min_trx:
        if max(buy_value, sell_value) < min_trx: return "NEUTRAL"
        if abs(buy_value-sell_value) < min_trx*0.5: return "NEUTRAL"
    if net_value>0: return "AKUM"
    if net_value<0: return "DIST"
    return "NEUTRAL"

def calculate_vsa_metrics(df):
    df=df.copy()
    price_range=(df['High']-df['Low']).replace(0,0.1)
    close_pos=(df['Close']-df['Low'])/price_range
    close_pos=np.clip(close_pos,0.02,0.98)
    buy_ratio=close_pos.copy()
    if 'V1' not in df.columns: df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
    vol_ratio=df['Volume']/df['V1'].replace(0,1)
    is_green=df['Close']>=df['Open']
    turbo_mask=(vol_ratio>2.5)&is_green&(close_pos>=0.70)
    strong_mask=(vol_ratio>1.5)&is_green&(close_pos>=0.60)
    buy_ratio=np.where(turbo_mask, np.minimum(0.97, buy_ratio+0.15), buy_ratio)
    buy_ratio=np.where(strong_mask&~turbo_mask, np.minimum(0.95, buy_ratio+0.08), buy_ratio)
    buy_ratio=np.clip(buy_ratio,0.05,0.97)
    df['Vol_Buy']=df['Volume']*buy_ratio
    df['Vol_Sell']=df['Volume']-df['Vol_Buy']
    df['Net_Vol_VSA']=df['Vol_Buy']-df['Vol_Sell']
    df['Net_Val_VSA']=df['Net_Vol_VSA']*df['Close']
    df['Buy_Pct']=buy_ratio*100
    return df, buy_ratio

def detect_buy_signals(df, multi_tf=None):
    signals=[]
    if df is None or len(df)<30: return signals,df
    try:
        df=df.copy()
        df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
        df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        df['ATR']=calculate_atr(df,14)
        df,_=calculate_vsa_metrics(df)
        net_5d=multi_tf.get('net_5d',0) if multi_tf else df['Net_Val_VSA'].tail(5).sum()
        for i in range(20,len(df)):
            close=df['Close'].iloc[i]; open_=df['Open'].iloc[i]
            vol=df['Volume'].iloc[i]; v1=df['V1'].iloc[i]
            ema50=df['EMA50'].iloc[i]; ema20=df['EMA20'].iloc[i]
            prev_close=df['Close'].iloc[i-1]; prev_ema50=df['EMA50'].iloc[i-1]
            is_bo=prev_close<=prev_ema50 and close>ema50 and close>ema20
            vol_spike=vol>v1*1.5 if v1>0 else False
            if is_bo and vol_spike and close>=open_ and net_5d>0:
                signals.append({'index':i,'date':df.index[i],'type':'BO EMA50','side':'BUY','entry':float(close),'sl':float(min(df['Low'].iloc[max(0,i-5):i+1].min(), close-df['ATR'].iloc[i]*1.2)),'reason':f'BO EMA50 Vol {vol/v1:.1f}x','strength':90})
        filtered=[]; last_idx=-20
        for sig in sorted(signals, key=lambda x: x['index']):
            if sig['index']-last_idx>=5:
                filtered.append(sig); last_idx=sig['index']
        return filtered,df
    except: return [],df
def calculate_trading_plan(df, signals=None, multi_tf=None):
    try:
        if df is None or len(df)<20: return None
        last_close=df['Close'].iloc[-1]
        atr=calculate_atr(df,14).iloc[-1]
        if pd.isna(atr) or atr==0: atr=last_close*0.03
        ema20=df['Close'].ewm(span=20).mean().iloc[-1]
        ema50=df['Close'].ewm(span=50).mean().iloc[-1]
        ema200=df['Close'].ewm(span=200).mean().iloc[-1]
        buy_sigs,_=detect_buy_signals(df,multi_tf)
        if buy_sigs and buy_sigs[-1]['index']>=len(df)-10:
            last=buy_sigs[-1]; entry=last['entry']; sl=last['sl']; signal_type=last['type']; reason=last['reason']; strength=last['strength']; side="BUY"
        else:
            entry=round_to_ihsg_fraction(last_close); sl=round_to_ihsg_fraction(max(df['Low'].tail(5).min(), last_close-atr*1.5))
            signal_type="NO SIGNAL"; reason="Tunggu BO EMA50"; strength=0; side="WAIT"
        min_sl=last_close*0.92; max_sl=last_close*0.98
        sl=max(min(sl,max_sl),min_sl); sl=round_to_ihsg_fraction(sl)
        if entry<=sl: entry=round_to_ihsg_fraction(sl*1.03)
        tp1=round_to_ihsg_fraction(entry+atr*1.5); tp2=round_to_ihsg_fraction(entry+atr*3.0)
        risk=abs(entry-sl)
        if last_close>ema20 and last_close>ema50 and last_close>ema200: trend="STRONG UPTREND"
        elif last_close>ema20 and last_close>ema50: trend="UPTREND"
        elif last_close>ema20: trend="WEAK UPTREND"
        elif last_close<ema20 and last_close<ema50 and last_close<ema200: trend="STRONG DOWNTREND"
        else: trend="DOWNTREND"
        mtf_confirm="NEUTRAL"
        if multi_tf:
            s5=multi_tf.get('status_5d','NEUTRAL'); s20=multi_tf.get('status_20d','NEUTRAL')
            if s5=="AKUM" and s20=="AKUM": mtf_confirm="STRONG BULLISH MTF"
            elif s5=="AKUM" or s20=="AKUM": mtf_confirm="BULLISH MTF"
            elif s5=="DIST" and s20=="DIST": mtf_confirm="BEARISH MTF"
        trend_mtf=f"{trend} + {mtf_confirm}" if mtf_confirm!="NEUTRAL" else trend
        if "DOWNTREND" in trend and last_close<ema20 and side=="BUY":
            side="WAIT"; signal_type="NO SIGNAL"; reason=f"WAIT - {trend}"; strength=0
        return {"entry":int(entry),"sl":int(sl),"tp1":int(tp1),"tp2":int(tp2),"atr":float(atr),"risk_pct":round((risk/entry)*100,2) if entry else 0,"trend":trend_mtf,"signal_type":signal_type,"signal_reason":reason,"signal_strength":strength,"side":side,"is_buy_signal":side=="BUY" and strength>=70,"buy_signals":buy_sigs,"mtf_confirm":mtf_confirm}
    except: return None
def is_market_open():
    now=get_now_wib(); weekday=now.weekday()
    if weekday>=5: return False
    current_time=now.time()
    if weekday==4:
        s1_start,s1_end=datetime.time(9,0),datetime.time(11,30)
        s2_start,s2_end=datetime.time(14,0),datetime.time(15,50)
    else:
        s1_start,s1_end=datetime.time(9,0),datetime.time(12,0)
        s2_start,s2_end=datetime.time(13,30),datetime.time(15,50)
    return (s1_start<=current_time<=s1_end) or (s2_start<=current_time<=s2_end)

def arjum_get(path, params=None, use_cache=True):
    cache_key=make_cache_key(path,params) if use_cache else None
    if use_cache and cache_key:
        if 'broker' in path:
            cached=get_cached_broker(cache_key)
            if cached is not None: return cached
    url=f"{ARJUM_BASE}{path}"
    try:
        headers=get_arjum_headers()
        if not headers.get("X-API-Key"):
            print("❌ API KEY KOSONG")
            return None
        r=requests.get(url,headers=headers,params=params,timeout=15)
        if r.status_code==200:
            j=r.json()
            if use_cache and cache_key and 'broker' in path: set_cached_broker(cache_key,j)
            return j
        else:
            print(f"Arjum FAIL {path} {params} -> {r.status_code} {r.text[:300]}")
        return None
    except Exception as e:
        print(f"arjum_get {path}: {e}"); return None

def get_screener_latest():
    data=arjum_get("/screener/latest")
    candidates=[]
    if data:
        if isinstance(data,dict):
            if 'rows' in data and isinstance(data['rows'],list):
                for r in data['rows']:
                    code=r.get('stock_code') or r.get('symbol')
                    if code: candidates.append({'symbol':code.replace(".JK","").upper(),'raw':r})
    if len(candidates) < 15:
        fallback=["BBCA","BBRI","BMRI","TLKM","ASII","GOTO","AMMN","ADRO","ANTM","MDKA","BBNI","BRIS","UNTR","ICBP","INDF","BRPT","TPIA","CUAN","PTRO","BREN","FILM","WIFI","DEWA"]
        for sym in fallback:
            if sym not in [c['symbol'] for c in candidates]:
                candidates.append({'symbol':sym,'raw':{}})
    seen=set(); uniq=[]
    for c in candidates:
        s=c['symbol']
        if s not in seen:
            seen.add(s); uniq.append(c)
    return uniq[:35]

def get_broker_summary_real(symbol, days=1):
    brokers=[]; buy_total=sell_total=net_total=0
    for attempt in range(5):
        now = get_now_wib() - datetime.timedelta(days=attempt)
        while now.weekday() >= 5:
            now = now - datetime.timedelta(days=1)
        if days==1:
            start_date = now.strftime('%Y-%m-%d'); end_date = now.strftime('%Y-%m-%d')
        else:
            delta = int(days*1.5)+3
            start_date = (now - datetime.timedelta(days=delta)).strftime('%Y-%m-%d'); end_date = now.strftime('%Y-%m-%d')
        params = {"net": False, "broker_limit": 30, "level_limit": 25, "flow": "all", "start_date": start_date, "end_date": end_date}
        data = arjum_get(f"/broker-summary/{symbol}", params=params, use_cache=False)
        if data and isinstance(data,dict) and data.get('brokers'):
            raw = data.get('brokers') or []
            tmp_brokers=[]; tmp_buy=tmp_sell=tmp_net=0
            for b in raw[:30]:
                if not isinstance(b,dict): continue
                code = b.get('broker_code') or '??'
                bval = float(b.get('bval',0) or 0); sval = float(b.get('sval',0) or 0); nval = float(b.get('nval',0) or (bval - sval))
                if bval==0 and sval==0 and nval!=0:
                    if nval>0: bval=nval
                    else: sval=abs(nval)
                if bval==0 and sval==0 and nval==0: continue
                tmp_brokers.append({"broker_code": str(code).upper(),"broker_name": b.get('broker_name',''),"buy_value": bval,"sell_value": sval,"net_value": nval})
                tmp_buy+=bval; tmp_sell+=sval; tmp_net+=nval
            if tmp_net!=0 or tmp_buy!=0:
                brokers=tmp_brokers; buy_total=tmp_buy; sell_total=tmp_sell; net_total=tmp_net
                break
    if buy_total==0 and sell_total==0 and net_total!=0:
        if net_total>0: buy_total=net_total
        else: sell_total=abs(net_total)
    status = determine_status(net_total, buy_total, sell_total)
    return float(net_total), status, brokers, float(buy_total), float(sell_total)

def get_broker_summary(symbol, days=None):
    d = days if days else 1
    return get_broker_summary_real(symbol, days=d)

def get_broker_multi_tf(symbol, hist_df=None):
    net_d, status_d, brokers_d, buy_d, sell_d = get_broker_summary_real(symbol, days=1)
    net_5d, status_5d, brokers_5d, buy_5d, sell_5d = get_broker_summary_real(symbol, days=5)
    net_20d, status_20d, brokers_20d, buy_20d, sell_20d = get_broker_summary_real(symbol, days=20)
    result={
        "net_d": float(net_d), "net_5d": float(net_5d), "net_20d": float(net_20d),
        "buy_d": float(buy_d), "sell_d": float(sell_d),
        "buy_5d": float(buy_5d), "sell_5d": float(sell_5d),
        "buy_20d": float(buy_20d), "sell_20d": float(sell_20d),
        "brokers": brokers_d, "brokers_5d": brokers_5d, "brokers_20d": brokers_20d,
        "status": status_d, "status_d": status_d, "status_5d": status_5d, "status_20d": status_20d,
    }
    print(f"MTF {symbol}: D={status_d} Net {net_d/1e9:.2f}B B{buy_d/1e9:.1f} S{sell_d/1e9:.1f} | 5D={status_5d} {net_5d/1e9:.2f}B | 20D={status_20d} {net_20d/1e9:.2f}B")
    return result

def format_top_brokers(brokers, top=3, status="AKUM"):
    if not brokers or not isinstance(brokers, list): return "-"
    valid=[b for b in brokers if isinstance(b,dict) and b.get('broker_code')]
    if not valid: return "-"
    try:
        if status in ["DIST","DISTRIB"]: sorted_b=sorted(valid, key=lambda x: float(x.get('net_value',0) or 0))
        else: sorted_b=sorted(valid, key=lambda x: float(x.get('net_value',0) or 0), reverse=True)
    except: sorted_b=valid
    parts=[]
    for b in sorted_b[:top]:
        code=b.get('broker_code') or "??"
        net=float(b.get('net_value',0) or 0)
        val=abs(net) if net!=0 else float(b.get('buy_value',0) or b.get('sell_value',0) or 0)
        if val==0: continue
        if abs(val)>=1e9: s=f"{val/1e9:.1f}B"
        elif abs(val)>=1e6: s=f"{val/1e6:.0f}M"
        else: s=f"{val:.0f}"
        parts.append(f"{code} {s}")
    return ", ".join(parts) if parts else "-"

def format_broker_detailed(brokers, top=15):
    if not brokers: return "No broker data"
    try:
        sorted_b=sorted(brokers, key=lambda x: abs(float(x.get('net_value',0) or 0)), reverse=True)
    except: sorted_b=brokers
    lines=[]
    for idx,b in enumerate(sorted_b[:top],1):
        code=b.get('broker_code','??')
        name=b.get('broker_name','')[:25]
        bval=b.get('buy_value',0); sval=b.get('sell_value',0); nval=b.get('net_value',0)
        b_str=format_large_number(bval,False); s_str=format_large_number(sval,False); n_str=format_large_number(nval,True)
        emoji="🟢" if nval>0 else "🔴" if nval<0 else "⚪"
        lines.append(f"{idx}. {emoji} *{code}* {n_str} | B:{b_str} S:{s_str} | {name}")
    return "\n".join(lines)

def get_history_pro(symbol, limit=150, timeframe="1d"):
    """
    CHART FIX: 
    1. Coba Arjum dulu dengan frame yang bener
    2. Baru fallback yfinance
    3. TF parsing bener: 5, 5m, 1h, 1d semua kebaca
    """
    tf = parse_timeframe(timeframe)
    print(f"get_history_pro {symbol} TF input={timeframe} parsed={tf} limit={limit}")
    
    # Mapping ke Arjum
    arjum_frame_map = {
        "1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min",
        "1h": "1hour", "4h": "4hour",
        "1d": "daily", "1w": "weekly", "1mo": "monthly"
    }
    arjum_frame = arjum_frame_map.get(tf, "daily")
    
    # Coba Arjum dulu
    data = arjum_get(f"/history/{symbol}", params={"limit": limit, "frame": arjum_frame}, use_cache=False)
    rows = []
    if data:
        if isinstance(data,dict): rows = data.get('data') or data.get('history') or []
        elif isinstance(data,list): rows = data
    
    df = None
    if rows and len(rows) >= 10:
        try:
            df_pd = pd.DataFrame(rows)
            rename_map={}
            for c in df_pd.columns:
                cl=str(c).lower()
                if cl in ['o','open']: rename_map[c]='Open'
                elif cl in ['h','high']: rename_map[c]='High'
                elif cl in ['l','low']: rename_map[c]='Low'
                elif cl in ['c','close','close_price']: rename_map[c]='Close'
                elif cl in ['v','volume','vol']: rename_map[c]='Volume'
                elif cl in ['date','time','t','datetime','timestamp']: rename_map[c]='Date'
            df_pd.rename(columns=rename_map,inplace=True)
            if 'Date' in df_pd.columns:
                df_pd['Date']=pd.to_datetime(df_pd['Date']); df_pd.set_index('Date',inplace=True)
            df_pd=df_pd.sort_index()
            for col in ['Open','High','Low','Close','Volume']:
                if col in df_pd.columns: df_pd[col]=pd.to_numeric(df_pd[col],errors='coerce')
            df_pd=df_pd.dropna(subset=['Close'])
            if len(df_pd)>=10:
                df = df_pd
                print(f"  Arjum {symbol} {tf} OK: {len(df)} bars, last close {df['Close'].iloc[-1]}")
        except Exception as e:
            print(f"  Arjum parse error {symbol}: {e}")
    
    # Fallback yfinance kalau Arjum kosong
    if df is None or len(df) < 10:
        try:
            import yfinance as yf
            yf_map = {
                "1m": ("7d","1m"), "5m": ("5d","5m"), "15m": ("5d","15m"), "30m": ("1mo","30m"),
                "1h": ("1mo","60m"), "4h": ("3mo","90m"),
                "1d": ("6mo","1d"), "1w": ("1y","1wk"), "1mo": ("2y","1mo")
            }
            period, interval = yf_map.get(tf, ("6mo","1d"))
            print(f"  Fallback yfinance {symbol} {tf} period={period} interval={interval}")
            hist = yf.Ticker(f"{symbol}.JK").history(period=period,interval=interval,timeout=10)
            if hist is not None and len(hist) >= 10:
                df = hist.tail(limit)
                print(f"  yfinance {symbol} {tf} OK: {len(df)} bars, last close {df['Close'].iloc[-1]}")
        except Exception as e:
            print(f"  yfinance error {symbol}: {e}")
    
    return df

def generate_pro_chart(df, symbol="BBCA", timeframe="1d", sector_info="IHSG", output_filename="chart.png", extra_info=None):
    """
    CHART FIX TOTAL:
    - Candle warna bener (hijau naik, merah turun)
    - EMA 13,20,50,200 bener
    - Volume Buy/Sell bener dari VSA
    - Broker code tampil di chart
    - TF label bener
    """
    try:
        extra_info=extra_info or {}
        tf = parse_timeframe(timeframe)
        
        # bersihin df
        df=df.copy().ffill().bfill()
        if not isinstance(df.index,pd.DatetimeIndex):
            df.index=pd.to_datetime(df.index)
        df=df.sort_index()
        
        # indikator
        df['EMA13']=df['Close'].ewm(span=13,adjust=False).mean()
        df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
        df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
        df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        df['V2']=df['Volume'].rolling(50,min_periods=1).mean()
        df, buy_ratios = calculate_vsa_metrics(df)
        
        last_close=df['Close'].iloc[-1]
        last_open=df['Open'].iloc[-1]
        last_high=df['High'].iloc[-1]
        last_low=df['Low'].iloc[-1]
        last_vol=df['Volume'].iloc[-1]
        prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close
        chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
        
        avg_price=df['Close'].tail(20).mean()
        vchg1=(last_vol/df['Volume'].iloc[-2]) if len(df)>1 and df['Volume'].iloc[-2]>0 else 1
        vchg5=(last_vol/df['Volume'].tail(5).mean()) if df['Volume'].tail(5).mean()>0 else 1
        buy_pct=int(buy_ratios[-1]*100)
        sell_pct=100-buy_pct
        
        # logic power
        if buy_pct>=85 and vchg1>=1.2: power="TURBO"
        elif buy_pct>=70 or vchg1>=1.5: power="STRONG"
        elif buy_pct>=60: power="NORMAL"
        else: power="WEAK"
        speed="FAST" if vchg1>2.0 else "SLOW" if vchg1<0.8 else "NORMAL"
        safety="GOOD" if last_close>df['EMA200'].iloc[-1] else "BAD"
        
        # broker info
        multi = extra_info.get('multi_tf') or {}
        broker_net = multi.get('net_d',0) or extra_info.get('broker_net',0)
        top_brokers = format_top_brokers(multi.get('brokers',[]) or extra_info.get('brokers',[]), 3, multi.get('status_d','AKUM'))
        
        # CHART DRAW
        plt.style.use('dark_background')
        fig=plt.figure(figsize=(16,9),dpi=180,facecolor='#000000')
        gs=gridspec.GridSpec(4,1,height_ratios=[4.5,1.1,0.9,0.8],hspace=0.05)
        ax_main=fig.add_subplot(gs[0])
        ax_vol=fig.add_subplot(gs[1],sharex=ax_main)
        ax_nbsa=fig.add_subplot(gs[2],sharex=ax_main)
        ax_mm=fig.add_subplot(gs[3],sharex=ax_main)
        fig.subplots_adjust(left=0.08,right=0.92,top=0.88,bottom=0.06)
        for ax in [ax_main,ax_vol,ax_nbsa,ax_mm]:
            ax.set_facecolor('#000000')
            ax.tick_params(colors='#aaaaaa',labelsize=8)
            ax.yaxis.tick_right()
            ax.grid(False)
        
        x=np.arange(len(df))
        
        # CANDLE - FIX WARNA BENER
        for i in range(len(df)):
            o=df['Open'].iloc[i]; h=df['High'].iloc[i]; l=df['Low'].iloc[i]; c=df['Close'].iloc[i]
            color='#00ff00' if c>=o else '#ff3333'
            # wick
            ax_main.plot([i,i],[l,h],color=color,linewidth=0.8,alpha=0.9)
            # body
            body_low=min(o,c)
            body_height=max(1, abs(c-o))
            # kalau doji, bikin garis tipis
            if abs(c-o) < (h-l)*0.05:
                ax_main.plot([i-0.3,i+0.3],[c,c],color=color,linewidth=1.2)
            else:
                facecolor='#00ff00' if c>=o else '#ff3333'
                edgecolor='#00ff00' if c>=o else '#ff3333'
                # body hollow kalau naik, filled kalau turun (biar jelas)
                if c>=o:
                    rect=patches.Rectangle((i-0.35,body_low),0.7,body_height,facecolor='none',edgecolor='#00ff00',linewidth=0.9)
                else:
                    rect=patches.Rectangle((i-0.35,body_low),0.7,body_height,facecolor='#ff3333',edgecolor='#ff3333',linewidth=0.9)
                ax_main.add_patch(rect)
        
        # EMA
        ax_main.plot(x,df['EMA13'],color='#ffff00',linewidth=1.0,alpha=0.9, label='EMA13')
        ax_main.plot(x,df['EMA20'],color='#ff0000',linewidth=1.0,alpha=0.9, label='EMA20')
        ax_main.plot(x,df['EMA50'],color='#ffffff',linewidth=1.0,alpha=0.9, label='EMA50')
        ax_main.plot(x,df['EMA200'],color='#a020f0',linewidth=1.2,alpha=0.9, label='EMA200')
        
        # BUY/SELL SIGNAL MARKER
        try:
            buy_sigs,_=detect_buy_signals(df, multi)
            for sig in buy_sigs[-5:]: # cuma 5 terakhir biar gak rame
                idx=sig['index']
                if idx < len(df):
                    low=df['Low'].iloc[idx]
                    atr=df['ATR'].iloc[idx] if 'ATR' in df.columns and not pd.isna(df['ATR'].iloc[idx]) else df['Close'].iloc[idx]*0.02
                    ax_main.annotate('▲',xy=(idx,low-atr*0.8),fontsize=14,color='#00ff00',fontweight='bold',ha='center',va='center')
        except: pass
        
        ax_main.set_xlim(-1,len(df))
        ax_main.set_ylim(df['Low'].min()*0.95, df['High'].max()*1.08)
        
        # LEFT INFO PANEL - FIX
        left_text=f"Avg Price : {avg_price:,.1f}\nVchg 1 Day: {vchg1:.1f} x\nVchg 5 Days: {vchg5:.1f} x\nSpeed : {speed}\nPower : {power}\nSafety : {safety}\n\nEMA 13 : {df['EMA13'].iloc[-1]:,.1f}\nEMA 20 : {df['EMA20'].iloc[-1]:,.1f}\nEMA 50 : {df['EMA50'].iloc[-1]:,.1f}\nEMA 200: {df['EMA200'].iloc[-1]:,.1f}"
        ax_main.text(0.01,0.98,left_text,transform=ax_main.transAxes,va='top',ha='left',fontsize=8,family='monospace',color='#e0e0e0',bbox=dict(facecolor='black',alpha=0.6,edgecolor='none'))
        
        # TOP BAR
        fig.text(0.01,0.96,f"{symbol} : {last_close:.0f} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=13,fontweight='bold',ha='left',va='center')
        fig.text(0.5,0.96,"RAFANO TRADER",color='white',fontsize=14,fontweight='bold',ha='center',va='center')
        
        # TF + DATE + BROKER CODE
        date_str=df.index[-1].strftime('%d %b %Y %H:%M') if hasattr(df.index[-1],'strftime') else get_now_wib().strftime('%d %b %Y')
        tf_label_map={"1m":"1-Min","5m":"5-Min","15m":"15-Min","30m":"30-Min","1h":"1-Hour","4h":"4-Hour","1d":"Daily","1w":"Weekly","1mo":"Monthly"}
        tf_display=tf_label_map.get(tf, tf.upper())
        fig.text(0.99,0.96,f"{tf_display} {date_str} | {top_brokers}",color='#ffcc00',fontsize=10,ha='right',va='center')
        
        # OHLC + VOLUME + BROKER
        fig.text(0.01,0.905,f"High:{last_high:.0f} Low:{last_low:.0f} Open:{last_open:.0f} Vol:{last_vol:,.0f} V1:{df['V1'].iloc[-1]:,.0f} | Broker Net:{format_large_number(broker_net,True)} | Top:{top_brokers}",color='#00ffff',fontsize=8,ha='left')
        
        # VOLUME PANEL - BUY/SELL
        vol_info=f"Buy%={buy_pct}% Sell%={sell_pct}% VolBuy:{df['Vol_Buy'].iloc[-1]:,.0f} VolSell:{df['Vol_Sell'].iloc[-1]:,.0f} | Buy Power:{power}"
        ax_vol.text(0.005,0.88,vol_info,transform=ax_vol.transAxes,color='#ffffff',fontsize=8,va='top')
        ax_vol.bar(x,df['Vol_Sell'],color='#cc0000',width=0.8,alpha=0.8)
        ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00cc00',width=0.8,alpha=0.9)
        ax_vol.plot(x,df['V1'],color='white',linewidth=0.8,alpha=0.9, label='V1')
        ax_vol.set_ylim(0,df['Volume'].max()*2.0)
        plt.setp(ax_vol.get_xticklabels(),visible=False)
        
        # NBSA / NET VAL PANEL
        nbsa_rp=abs(broker_net) if broker_net!=0 else abs(df['Net_Val_VSA'].iloc[-1])
        nbsa_info=f"NBSA Rp. {nbsa_rp/1e9:.2f} Milyar | Net Vol:{df['Net_Vol_VSA'].iloc[-1]:,.0f} Net 5D:{df['Net_Vol_VSA'].tail(5).sum():,.0f}"
        ax_nbsa.text(0.005,0.85,nbsa_info,transform=ax_nbsa.transAxes,color='#ffffff',fontsize=8,va='top')
        # bar net vol VSA
        nbsa_vals=df['Net_Vol_VSA'].tail(80)
        max_abs = nbsa_vals.abs().max() or 1
        nbsa_norm = nbsa_vals / max_abs * 50
        x_nbsa=np.arange(len(df)-len(nbsa_vals),len(df))
        for i,v in zip(x_nbsa,nbsa_norm):
            ax_nbsa.bar(i,v,color='#00ffff' if v>=0 else '#ff4444',width=0.6)
        ax_nbsa.axhline(0,color='#444444',linewidth=0.5)
        ax_nbsa.set_ylim(-60,60)
        
        # MARKET MAKER PANEL
        ax_mm.text(0.005,0.85,"Market Maker (Close-EMA50)/EMA50",transform=ax_mm.transAxes,color='#ffffff',fontsize=8,va='top')
        if 'MM' not in df.columns: df['MM']=(df['Close']-df['EMA50'])/df['EMA50']*1000
        mm_vals=df['MM'].tail(80)
        x_mm=np.arange(len(df)-len(mm_vals),len(df))
        colors=['#00ff00' if v>=0 else '#ff0000' for v in mm_vals]
        ax_mm.bar(x_mm,mm_vals,color=colors,width=0.5,alpha=0.8)
        ax_mm.axhline(0,color='#444444',linewidth=0.5)
        step=max(1,len(df)//8)
        ax_mm.set_xticks(x[::step])
        ax_mm.set_xticklabels([df.index[i].strftime('%d %b') if hasattr(df.index[i],'strftime') else str(i) for i in range(0,len(df),step)],fontsize=7)
        
        plt.savefig(output_filename,dpi=180,bbox_inches='tight',facecolor='#000000')
        plt.close(fig)
        print(f"Chart OK: {symbol} {tf} {output_filename} Top:{top_brokers} BrokerNet:{broker_net}")
        return output_filename
    except Exception as e:
        print(f"Chart error {e}"); import traceback; traceback.print_exc()
        try: plt.close('all')
        except: pass
        return None

LAST_SENT_SIGNALS={}; COOLDOWN_SECONDS=3600; LAST_RESET_DATE=""; LAST_SIGNALS_CACHE={}
def filter_signals_with_cooldown(signals):
    global LAST_RESET_DATE, LAST_SENT_SIGNALS
    current_time=time.time(); today_str=get_now_wib().strftime('%Y-%m-%d')
    if LAST_RESET_DATE!=today_str: LAST_SENT_SIGNALS.clear(); LAST_RESET_DATE=today_str
    filtered=[]
    for sig in signals:
        sym=sig['symbol']; last_sent=LAST_SENT_SIGNALS.get(sym,0)
        if (current_time-last_sent)>=COOLDOWN_SECONDS:
            filtered.append(sig); LAST_SENT_SIGNALS[sym]=current_time
    return filtered
def calculate_score_v2(symbol, history_df, accum_value, broker_net, analysis_data):
    score=30
    if accum_value>20_000_000_000: score+=30
    elif accum_value>5_000_000_000: score+=20
    elif accum_value>0: score+=10
    if broker_net>10_000_000_000: score+=20
    elif broker_net>0: score+=10
    if score>=85: label="VERY STRONG"
    elif score>=70: label="STRONG BUY"
    elif score>=50: label="WEAK BUY"
    else: label="NO SIGNAL"
    return score,label,[]

def scan_v3():
    print(f"[{get_now_wib()}] Scan V3.9 CHART FIX...")
    screener_data=get_screener_latest()
    candidates=[item['symbol'] for item in screener_data]
    print(f"  Kandidat: {candidates[:15]} (total {len(candidates)})")
    detected=[]
    def process_symbol(sym):
        try:
            hist_df=get_history_pro(sym,limit=120,timeframe="1d")
            multi=get_broker_multi_tf(sym,hist_df)
            accum_val=multi['accum_d']; broker_net=multi['net_d']; brokers_combined=multi['brokers']
            score,label,reasons=calculate_score_v2(sym,hist_df,accum_val,broker_net,{})
            threshold=20 if get_now_wib().weekday()>=5 else 40
            if score>=threshold:
                last_close=int(hist_df['Close'].iloc[-1]) if hist_df is not None and len(hist_df)>=2 else 0
                prev=hist_df['Close'].iloc[-2] if hist_df is not None and len(hist_df)>=2 else 0
                change_pct=((last_close/prev)-1)*100 if prev else 0
                tp=calculate_trading_plan(hist_df,multi_tf=multi) if hist_df is not None else None
                return {"symbol":sym,"close":last_close,"change_pct":change_pct,"score":score,"score_label":label,"accum_value":accum_val,"broker_net":broker_net,"broker_status":multi['status'],"history_df":hist_df,"trading_plan":tp,"brokers":brokers_combined,"multi_tf":multi}
        except Exception as e:
            print(f"Error {sym}: {e}")
        return None
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures=[executor.submit(process_symbol,s) for s in candidates]
        for f in as_completed(futures):
            res=f.result()
            if res: detected.append(res)
    detected.sort(key=lambda x: x['score'], reverse=True)
    print(f"Scan: {len(detected)} sinyal")
    return detected

def send_reply(chat_id, text, reply_markup=None):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload={"chat_id":chat_id,"text":text,"parse_mode":"Markdown"}
    if reply_markup: payload["reply_markup"]=reply_markup
    try: requests.post(url,json=payload,timeout=10)
    except Exception as e: print(f"TG Error: {e}")
def send_photo_reply(chat_id, photo_path, caption=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path,'rb') as photo:
            requests.post(url,data={'chat_id':chat_id,'caption':caption,'parse_mode':'Markdown'},files={'photo':photo},timeout=30)
    except Exception as e: print(f"Photo Error: {e}")

def broadcast_v3(signals):
    if not signals:
        send_reply(TARGET_CHAT_ID,"V3 Scan: Tidak ada sinyal")
        return
    now_str=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    akum_only=[s for s in signals if s.get('multi_tf',{}).get('status_d')=='AKUM']
    use_signals=akum_only if akum_only else signals
    header=f"*RAFANO V3.9 CHART FIX - BROKER CODE REAL*\n{now_str}\nTotal: {len(use_signals)}\n============================\n\n"
    msg=header; keyboard=[]
    for idx,item in enumerate(use_signals,1):
        multi=item.get('multi_tf') or {}
        top_d=format_top_brokers(multi.get('brokers',[]) or item.get('brokers',[]),3,multi.get('status_d','AKUM'))
        daily_str=f"Daily: {multi.get('status_d','')} Net {format_large_number(multi.get('net_d',0),True)} B{format_large_number(multi.get('buy_d',0),True)} S{format_large_number(multi.get('sell_d',0),True)} | Top {top_d}"
        item_str=f"{idx}. *{item['symbol']}* -- {item.get('close',0)} ({item.get('change_pct',0):+.2f}%)\n   {daily_str}\n\n"
        keyboard.append([{"text":f"Chart {item['symbol']}","callback_data":f"chart_{item['symbol']}_1d"}, {"text":f"Broker {item['symbol']}","callback_data":f"broker_{item['symbol']}"}])
        if len(msg)+len(item_str)>3500:
            send_reply(TARGET_CHAT_ID,msg,reply_markup={"inline_keyboard":keyboard}); msg=item_str; keyboard=[]
        else: msg+=item_str
    if msg: send_reply(TARGET_CHAT_ID,msg,reply_markup={"inline_keyboard":keyboard})

def process_chart_request(chat_id, stock_code, timeframe="1d", extra_info_cache=None):
    tf_parsed = parse_timeframe(timeframe)
    send_reply(chat_id,f"Generating {stock_code.upper()} ({tf_parsed.upper()}) REAL B S... TF input:{timeframe} -> parsed:{tf_parsed}")
    df=get_history_pro(stock_code,limit=150,timeframe=tf_parsed)
    if df is None or len(df)<20:
        send_reply(chat_id,f"Data {stock_code} tidak ketemu TF {timeframe} (parsed {tf_parsed}). Coba /c {stock_code} 1d"); return
    if extra_info_cache and stock_code in extra_info_cache:
        extra=extra_info_cache[stock_code]
    else:
        multi=get_broker_multi_tf(stock_code,df)
        extra={"broker_net":multi.get('net_d',0),"brokers":multi.get('brokers',[]),"multi_tf":multi}
    chart_file=f"/tmp/chart_{stock_code.upper()}_{tf_parsed}_{int(time.time())}.png"
    try:
        file_path=generate_pro_chart(df=df,symbol=stock_code.upper(),timeframe=tf_parsed,output_filename=chart_file,extra_info=extra)
        if not file_path or not os.path.exists(file_path):
            send_reply(chat_id, f"Gagal render chart {stock_code} TF {tf_parsed}"); return
        multi=extra.get('multi_tf') or {}
        top_d=format_top_brokers(multi.get('brokers',[]) or extra.get('brokers',[]),3,multi.get('status_d','AKUM'))
        detailed=format_broker_detailed(multi.get('brokers',[]), top=5)
        tp=calculate_trading_plan(df,multi_tf=multi)
        if tp:
            caption=f"*{stock_code.upper()}* {tf_parsed.upper()} -- {safe_int(df['Close'].iloc[-1])} ({len(df)} bars) | {tp['trend']}\nDaily: {multi.get('status_d')} Net {format_large_number(multi.get('net_d',0),True)} B{format_large_number(multi.get('buy_d',0),True)} S{format_large_number(multi.get('sell_d',0),True)}\nTop: {top_d}\n{tp['signal_type']} {tp['side']} Entry {tp['entry']} SL {tp['sl']} TP1 {tp['tp1']}"
        else:
            caption=f"*{stock_code.upper()}* {tf_parsed.upper()} -- {safe_int(df['Close'].iloc[-1])} ({len(df)} bars)\nTop: {top_d}"
        send_photo_reply(chat_id,file_path,caption=caption)
        send_reply(chat_id, f"🏦 *BROKER CODE {stock_code.upper()} {tf_parsed.upper()} DETAIL*\n{detailed}\n\nChart: {tf_parsed.upper()} {len(df)} bars last {df.index[-1].strftime('%d %b %H:%M') if hasattr(df.index[-1],'strftime') else ''}")
        if os.path.exists(file_path): os.remove(file_path)
    except Exception as e:
        import traceback; traceback.print_exc(); send_reply(chat_id,f"Gagal render: {e}")

def telegram_bot_listener():
    global LAST_SIGNALS_CACHE
    offset=0
    print("Telegram Listener V3.9 CHART FIX Running...")
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
    except: pass
    while True:
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res=requests.get(url,timeout=25)
            if res.status_code!=200:
                time.sleep(3); continue
            data=res.json()
            for update in data.get("result",[]):
                offset=update["update_id"]+1
                if "callback_query" in update:
                    cb=update["callback_query"]
                    cb_id=cb.get("id"); cb_data=cb.get("data",""); chat_id=cb["message"]["chat"]["id"]
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery", json={"callback_query_id":cb_id})
                    if cb_data.startswith("chart_"):
                        parts=cb_data.split("_")
                        if len(parts)>=3:
                            sym=parts[1]; tf=parts[2]
                            threading.Thread(target=process_chart_request, args=(chat_id,sym,tf,LAST_SIGNALS_CACHE), daemon=True).start()
                    elif cb_data.startswith("broker_"):
                        sym=cb_data.split("_")[1]
                        def broker_detail_cb(target_chat, symbol):
                            try:
                                net_d,status_d,brokers,buy_d,sell_d=get_broker_summary_real(symbol, days=1)
                                multi=get_broker_multi_tf(symbol)
                                detailed=format_broker_detailed(brokers, top=15)
                                msg=f"🏦 *BROKER CODE {symbol} REAL*\nStatus: {status_d} Net {format_large_number(net_d,True)} B{format_large_number(buy_d,True)} S{format_large_number(sell_d,True)}\n\n{detailed}"
                                send_reply(target_chat, msg)
                            except Exception as e:
                                send_reply(target_chat, f"Error broker {symbol}: {e}")
                        threading.Thread(target=broker_detail_cb, args=(chat_id,sym), daemon=True).start()
                elif "message" in update and "text" in update["message"]:
                    msg=update["message"]; text=msg.get("text","").strip(); chat_id=msg["chat"]["id"]
                    first_word=text.split()[0].lower() if text else ""
                    print(f"Pesan: {text} dari {chat_id}")
                    if first_word in ["/start","/help"]:
                        help_msg="🤖 *RAFANO V3.9 CHART FIX*\n`/c KODE` Chart Daily\n`/c KODE 5m` Chart 5 menit (1,5,15,30,1h,4h,1d,1w)\n`/c KODE 5` juga bisa (auto menit)\n`/b KODE` Broker Detail 15 broker B S Net\n`/scan` Scan AKUM\n`/clearcache` Clear\n\nContoh:\n`/c BBCA`\n`/c BBCA 5m`\n`/c BBCA 1h`\n`/c FILM 15m`\n"
                        send_reply(chat_id, help_msg)
                    elif first_word in ["/c","/chart"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            tf=parts[2] if len(parts)>=3 else "1d"
                            tf_parsed=parse_timeframe(tf)
                            print(f"/c request sym={sym} tf_input={tf} parsed={tf_parsed}")
                            threading.Thread(target=process_chart_request, args=(chat_id,sym,tf_parsed,LAST_SIGNALS_CACHE), daemon=True).start()
                        else:
                            send_reply(chat_id, "Format: `/c <KODE> [TF]` contoh `/c BBCA 5m`")
                    elif first_word in ["/b","/broker"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            def broker_detail(target_chat, symbol):
                                try:
                                    net_d,status_d,brokers,buy_d,sell_d=get_broker_summary_real(symbol, days=1)
                                    multi=get_broker_multi_tf(symbol)
                                    detailed=format_broker_detailed(brokers, top=15)
                                    msg=f"🏦 *BROKER CODE {symbol} REAL B S Net (Last Trading Day)*\nStatus: {status_d} | Net: {format_large_number(net_d, True)} | Buy: {format_large_number(buy_d,True)} Sell: {format_large_number(sell_d,True)}\nDate: {get_now_wib().strftime('%d %b %H:%M')} (auto Jumat kalau weekend)\n\n{detailed}\n\n5D: {multi.get('status_5d')} Net {format_large_number(multi.get('net_5d',0),True)}\n20D: {multi.get('status_20d')} Net {format_large_number(multi.get('net_20d',0),True)}"
                                    send_reply(target_chat, msg)
                                except Exception as e:
                                    import traceback; traceback.print_exc()
                                    send_reply(target_chat, f"Error broker {symbol}: {e}")
                            threading.Thread(target=broker_detail, args=(chat_id,sym), daemon=True).start()
                    elif first_word in ["/clearcache","/cc"]:
                        BROKER_CACHE.clear(); HISTORY_CACHE.clear(); LAST_SIGNALS_CACHE.clear()
                        send_reply(chat_id, "🧹 Cache cleared")
                    elif first_word in ["/scan","/scanpro","/top"]:
                        send_reply(chat_id, "🔍 *Scanning REAL BROKER CODE (Last Trading Day)...*")
                        def manual_scan(is_pro=False, target_chat=chat_id):
                            global LAST_SIGNALS_CACHE
                            sigs=scan_v3()
                            LAST_SIGNALS_CACHE={s['symbol']: s for s in sigs}
                            akum_only=[s for s in sigs if s.get('multi_tf',{}).get('status_d')=='AKUM']
                            filt=akum_only if akum_only else sigs
                            now_str=get_now_wib().strftime('%d %b %Y %H:%M WIB')
                            if not filt:
                                send_reply(target_chat, f"*RAFANO V3.9* {now_str}\n0 sinyal AKUM (coba besok Senin, market tutup)"); return
                            header=f"*RAFANO V3.9 CHART FIX - BROKER CODE REAL - {now_str}*\nTotal: {len(filt)} (data Jumat terakhir)\n\n"
                            msg=header; kb=[]
                            for idx,item in enumerate(filt,1):
                                multi=item.get('multi_tf') or {}
                                top_d=format_top_brokers(multi.get('brokers',[]) or item.get('brokers',[]),3,multi.get('status_d','AKUM'))
                                daily_str=f"Daily: {multi.get('status_d','')} Net {format_large_number(multi.get('net_d',0),True)} B{format_large_number(multi.get('buy_d',0),True)} S{format_large_number(multi.get('sell_d',0),True)} | Top {top_d}"
                                item_str=f"{idx}. *{item['symbol']}* -- {item.get('close',0)} ({item.get('change_pct',0):+.2f}%)\n   {daily_str}\n\n"
                                kb.append([{"text":f"Chart {item['symbol']}","callback_data":f"chart_{item['symbol']}_1d"}, {"text":f"Broker {item['symbol']}","callback_data":f"broker_{item['symbol']}"}])
                                if len(msg)+len(item_str)>3500:
                                    send_reply(target_chat,msg,reply_markup={"inline_keyboard":kb}); msg=item_str; kb=[]
                                else: msg+=item_str
                            send_reply(target_chat,msg,reply_markup={"inline_keyboard":kb})
                        threading.Thread(target=manual_scan, args=(False,chat_id), daemon=True).start()
        except Exception as e:
            print(f"Listener error: {e}"); time.sleep(3)

def auto_screener_loop():
    global LAST_SIGNALS_CACHE
    print("Auto Screener V3.9...")
    while True:
        try:
            if not is_market_open(): time.sleep(300); continue
            sigs=scan_v3(); LAST_SIGNALS_CACHE={s['symbol']: s for s in sigs}
            filt=filter_signals_with_cooldown(sigs)
            if filt: broadcast_v3(filt)
            time.sleep(600)
        except Exception as e:
            print(f"Auto loop error: {e}"); time.sleep(10)

if __name__=="__main__":
    print("==========================================")
    print("RAFANO V3.9 - CHART FIX + BROKER CODE")
    print("==========================================")
    threading.Thread(target=auto_screener_loop, daemon=True).start()
    telegram_bot_listener()
