"""
RAFANO V3 - FINAL MINIMAL FIX
- Keep all commands: /c /b /info /trend /top /compare /wl /scan /scanpro /clearcache
- Fix: is_market_open duplicate, VSA, akum/dist REAL logic, scan AKUM only
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
    if v:
        v = str(v).strip().strip('"').strip("'")
        return v
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
    return {"X-API-Key": k.strip(), "Accept": "application/json", "User-Agent": "Mozilla/5.0 RAFANO/3.2"}
def get_now_wib(): return datetime.datetime.now(TIMEZONE_WIB)

# CACHE THREAD-SAFE
CACHE_LOCK = threading.RLock()
BROKER_CACHE, HISTORY_CACHE, SCREENER_CACHE = {}, {}, {}
CACHE_FILE = Path("/tmp/rafano_cache.json")
BROKER_CACHE_TTL, HISTORY_CACHE_TTL, SCREENER_CACHE_TTL = 300, 600, 180

def make_cache_key(path, params):
    if not params: return path
    try: return f"{path}?{'&'.join([f'{k}={v}' for k,v in sorted(params.items())])}"
    except: return path
def get_cached_broker(k):
    with CACHE_LOCK:
        if k in BROKER_CACHE:
            ts,d = BROKER_CACHE[k]
            if time.time()-ts < BROKER_CACHE_TTL: return d
            del BROKER_CACHE[k]
    return None
def set_cached_broker(k,d):
    with CACHE_LOCK: BROKER_CACHE[k]=(time.time(),d)
    try:
        with open(CACHE_FILE,'w') as f: json.dump({'broker':{k:[v[0],v[1]] for k,v in BROKER_CACHE.items()}},f)
    except: pass
def get_cached_history(k):
    with CACHE_LOCK:
        if k in HISTORY_CACHE:
            ts,d=HISTORY_CACHE[k]
            if time.time()-ts < HISTORY_CACHE_TTL: return d
            del HISTORY_CACHE[k]
    return None
def set_cached_history(k,d):
    with CACHE_LOCK: HISTORY_CACHE[k]=(time.time(),d)
def get_cached_screener():
    with CACHE_LOCK:
        if 'latest' in SCREENER_CACHE:
            ts,d=SCREENER_CACHE['latest']
            if time.time()-ts < SCREENER_CACHE_TTL: return d
            del SCREENER_CACHE['latest']
    return None
def set_cached_screener(d):
    with CACHE_LOCK: SCREENER_CACHE['latest']=(time.time(),d)

# HELPERS
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
    high, low, close = df['High'], df['Low'], df['Close']
    tr1=high-low; tr2=(high-close.shift(1)).abs(); tr3=(low-close.shift(1)).abs()
    tr=pd.concat([tr1,tr2,tr3],axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=1).mean()
def calculate_bollinger_bands(df, period=20, std=2):
    sma=df['Close'].rolling(period).mean(); stddev=df['Close'].rolling(period).std()
    return sma, sma+stddev*std, sma-stddev*std

# ===== FIXED VSA & AKUM/DIST =====
MIN_TRX_THRESHOLD = 500_000_000

def determine_status(net_value, buy_value, sell_value, min_trx=MIN_TRX_THRESHOLD):
    if abs(net_value) < min_trx:
        if max(buy_value, sell_value) < min_trx: return "NEUTRAL"
        if abs(buy_value-sell_value) < min_trx*0.5: return "NEUTRAL"
    if net_value>0: return "AKUM"
    if net_value<0: return "DIST"
    if buy_value > sell_value*1.2: return "AKUM"
    if sell_value > buy_value*1.2: return "DIST"
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

def calculate_buy_signal_strength(df):
    if len(df)<20: return 0,"NO DATA"
    last_row=df.iloc[-1]
    last_close,last_open,last_vol=last_row['Close'],last_row['Open'],last_row['Volume']
    avg_vol_v1=last_row.get('V1',last_row['Volume'])
    df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
    ema_50=df['EMA50'].iloc[-1]
    df, buy_ratios=calculate_vsa_metrics(df)
    last_buy_ratio=buy_ratios[-1]
    net_5d_val=df['Net_Val_VSA'].tail(5).sum()
    score=0
    if last_close>ema_50: score+=25
    vol_multiple=last_vol/avg_vol_v1 if avg_vol_v1>0 else 0
    if vol_multiple>=2.5: score+=25
    elif vol_multiple>=2.0: score+=20
    elif vol_multiple>=1.8: score+=15
    if last_buy_ratio>=0.75: score+=20
    elif last_buy_ratio>=0.65: score+=15
    elif last_buy_ratio>=0.55: score+=10
    if net_5d_val>0: score+=20
    if last_close>last_open: score+=10
    if score>=85: label="VERY STRONG"
    elif score>=70: label="STRONG BUY"
    elif score>=50: label="WEAK BUY"
    else: label="NO SIGNAL"
    return score,label

def detect_buy_signals(df, multi_tf=None):
    signals=[]
    if df is None or len(df)<30: return signals,df
    try:
        df=df.copy()
        df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
        df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
        df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        df['ATR']=calculate_atr(df,14)
        bb_mid,bb_upper,bb_lower=calculate_bollinger_bands(df,20,2)
        df['BB_MID']=bb_mid; df['BB_UPPER']=bb_upper; df['BB_LOWER']=bb_lower
        df,_=calculate_vsa_metrics(df)
        net_5d=multi_tf.get('net_5d',0) if multi_tf else df['Net_Val_VSA'].tail(5).sum()
        for i in range(20,len(df)):
            close=df['Close'].iloc[i]; open_=df['Open'].iloc[i]; low=df['Low'].iloc[i]
            vol=df['Volume'].iloc[i]; v1=df['V1'].iloc[i]
            ema50=df['EMA50'].iloc[i]; ema20=df['EMA20'].iloc[i]
            bb_low=df['BB_LOWER'].iloc[i] if not pd.isna(df['BB_LOWER'].iloc[i]) else 0
            atr=df['ATR'].iloc[i] if not pd.isna(df['ATR'].iloc[i]) else close*0.03
            prev_close=df['Close'].iloc[i-1]; prev_ema50=df['EMA50'].iloc[i-1]
            is_bo_ema50=(prev_close<=prev_ema50 and close>ema50 and close>ema20)
            vol_spike=(vol>v1*1.5) if v1>0 else False
            is_green=close>=open_
            if is_bo_ema50 and vol_spike and is_green and net_5d>0:
                signals.append({'index':i,'date':df.index[i],'type':'BO EMA50','side':'BUY','entry':float(close),'sl':float(min(df['Low'].iloc[max(0,i-5):i+1].min(), close-atr*1.2)),'reason':f'Breakout EMA50 + Vol {vol/v1:.1f}x + Net 5D Akum','strength':90})
                continue
            if bb_low>0:
                dist_to_bb_low=(close-bb_low)/bb_low*100
                is_far_below_bb=close<bb_low and dist_to_bb_low<-1.5
                body=abs(close-open_); lower_wick=min(open_,close)-low
                is_reversal=is_green and lower_wick>body*1.5 and body>0
                if is_far_below_bb and is_reversal:
                    signals.append({'index':i,'date':df.index[i],'type':'BOW BB','side':'BUY','entry':float(close),'sl':float(low*0.98),'reason':f'BOW: {dist_to_bb_low:.1f}% below BB Lower + Reversal','strength':85})
                    continue
            dist_ema50=abs(close-ema50)/ema50*100 if ema50>0 else 100
            is_near_ema=dist_ema50<2.0
            if is_near_ema and close>ema50 and close>open_:
                signals.append({'index':i,'date':df.index[i],'type':'BOS EMA','side':'BUY','entry':float(close),'sl':float(min(df['Low'].iloc[max(0,i-3):i+1].min(), ema50*0.97)),'reason':f'BOS: Near EMA {dist_ema50:.1f}%','strength':80})
        filtered=[]; last_idx=-20
        for sig in sorted(signals, key=lambda x: x['index']):
            if sig['index']-last_idx>=5:
                filtered.append(sig); last_idx=sig['index']
            elif filtered and sig['strength']>filtered[-1]['strength']:
                filtered[-1]=sig; last_idx=sig['index']
        return filtered,df
    except Exception as e:
        print(f"detect_buy_signals error: {e}")
        return [],df

def detect_sell_signals(df, multi_tf=None):
    signals=[]
    if df is None or len(df)<30: return signals,df
    try:
        if 'EMA50' not in df.columns:
            df=df.copy()
            df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
            df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
            df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
            df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
            df['ATR']=calculate_atr(df,14)
            _,bb_upper,_=calculate_bollinger_bands(df,20,2)
            df['BB_UPPER']=bb_upper
            df,_=calculate_vsa_metrics(df)
        net_5d=multi_tf.get('net_5d',0) if multi_tf else 0
        for i in range(20,len(df)):
            close=df['Close'].iloc[i]; open_=df['Open'].iloc[i]; high=df['High'].iloc[i]
            vol=df['Volume'].iloc[i]; v1=df['V1'].iloc[i]; ema50=df['EMA50'].iloc[i]
            atr=df['ATR'].iloc[i] if not pd.isna(df['ATR'].iloc[i]) else close*0.03
            prev_close=df['Close'].iloc[i-1]; prev_ema50=df['EMA50'].iloc[i-1]
            is_bd_ema50=(prev_close>=prev_ema50 and close<ema50)
            vol_spike=(vol>v1*1.5) if v1>0 else False
            is_red=close<open_
            if is_bd_ema50 and vol_spike and is_red and net_5d<0:
                signals.append({'index':i,'date':df.index[i],'type':'BD EMA50','side':'SELL','entry':float(close),'sl':float(max(df['High'].iloc[max(0,i-5):i+1].max(), close+atr*1.2)),'reason':f'Breakdown EMA50 + Vol {vol/v1:.1f}x + Net Dist','strength':90})
        filtered=[]; last_idx=-20
        for sig in sorted(signals, key=lambda x: x['index']):
            if sig['index']-last_idx>=5:
                filtered.append(sig); last_idx=sig['index']
        return filtered,df
    except Exception as e:
        print(f"detect_sell error: {e}")
        return [],df

def calculate_trading_plan(df, signals=None, multi_tf=None):
    try:
        if df is None or len(df)<20: return None
        last_close=df['Close'].iloc[-1]
        atr=calculate_atr(df,14).iloc[-1]
        if pd.isna(atr) or atr==0: atr=last_close*0.03
        ema20=df['Close'].ewm(span=20).mean().iloc[-1]
        ema50=df['Close'].ewm(span=50).mean().iloc[-1]
        ema200=df['Close'].ewm(span=200).mean().iloc[-1]
        if signals is None:
            buy_sigs,_=detect_buy_signals(df,multi_tf)
            sell_sigs,_=detect_sell_signals(df,multi_tf)
            signals=buy_sigs+sell_sigs
        else:
            buy_sigs=[s for s in signals if s.get('side')=='BUY']
            sell_sigs=[s for s in signals if s.get('side')=='SELL']
        mtf_confirm="NEUTRAL"
        if multi_tf:
            status_5d=multi_tf.get('status_5d','NEUTRAL'); status_20d=multi_tf.get('status_20d','NEUTRAL')
            net_5d=multi_tf.get('net_5d',0); net_20d=multi_tf.get('net_20d',0)
            weekly_bullish=status_5d=="AKUM" and net_5d>0
            monthly_bullish=status_20d=="AKUM" and net_20d>0
            if weekly_bullish and monthly_bullish: mtf_confirm="STRONG BULLISH MTF"
            elif weekly_bullish or monthly_bullish: mtf_confirm="BULLISH MTF"
            elif status_5d=="DIST" and status_20d=="DIST": mtf_confirm="BEARISH MTF"
        recent_buy=[s for s in buy_sigs if s['index']>=len(df)-10]
        recent_sell=[s for s in sell_sigs if s['index']>=len(df)-10]
        if recent_buy and (not recent_sell or recent_buy[-1]['index']>=recent_sell[-1]['index']):
            last_signal=recent_buy[-1]; entry=last_signal['entry']; sl=last_signal['sl']; signal_type=last_signal['type']; signal_reason=last_signal['reason']; signal_strength=last_signal['strength']; side="BUY"; is_buy=True
        elif recent_sell:
            last_signal=recent_sell[-1]; entry=last_signal['entry']; sl=last_signal['sl']; signal_type=last_signal['type']; signal_reason=last_signal['reason']; signal_strength=last_signal['strength']; side="SELL"; is_buy=False
        else:
            entry=round_to_ihsg_fraction(last_close); sl=round_to_ihsg_fraction(max(df['Low'].tail(5).min(), last_close-atr*1.5))
            signal_type="NO SIGNAL"; signal_reason="Tunggu BO EMA50 / BOW BB / BOS EMA"; signal_strength=0; side="WAIT"; is_buy=False
        if side=="BUY" and mtf_confirm=="STRONG BULLISH MTF":
            signal_strength=min(100,signal_strength+10); signal_reason+=" + MTF Weekly+Monthly AKUM"
        elif side=="BUY" and mtf_confirm=="BULLISH MTF":
            signal_strength=min(95,signal_strength+5); signal_reason+=" + MTF Bullish"
        min_sl=last_close*0.92; max_sl=last_close*0.98
        sl=max(min(sl,max_sl),min_sl); sl=round_to_ihsg_fraction(sl)
        if entry<=sl and side!="SELL": entry=round_to_ihsg_fraction(sl*1.03)
        if side=="BUY":
            tp1=round_to_ihsg_fraction(entry+atr*1.5); tp2=round_to_ihsg_fraction(entry+atr*3.0)
            if mtf_confirm=="STRONG BULLISH MTF": tp2=round_to_ihsg_fraction(entry+atr*4.0)
            risk=entry-sl; reward1=tp1-entry; reward2=tp2-entry
        elif side=="SELL":
            sl_sell=min(max(sl,last_close*1.02),last_close*1.08); sl=round_to_ihsg_fraction(sl_sell)
            if entry>=sl: entry=round_to_ihsg_fraction(sl*0.97)
            tp1=round_to_ihsg_fraction(entry*0.965); tp2=round_to_ihsg_fraction(entry-atr*1.8)
            risk=sl-entry; reward1=entry-tp1; reward2=entry-tp2
        else:
            tp1=round_to_ihsg_fraction(entry*1.035); tp2=round_to_ihsg_fraction(entry+atr*1.8)
            risk=entry-sl; reward1=tp1-entry; reward2=tp2-entry
        rr1=reward1/risk if risk>0 else 0; rr2=reward2/risk if risk>0 else 0
        if last_close>ema20 and last_close>ema50 and last_close>ema200: trend="STRONG UPTREND"
        elif last_close>ema20 and last_close>ema50: trend="UPTREND"
        elif last_close>ema20: trend="WEAK UPTREND"
        elif last_close<ema20 and last_close<ema50 and last_close<ema200: trend="STRONG DOWNTREND"
        else: trend="DOWNTREND"
        if "DOWNTREND" in trend and last_close<ema20 and side=="BUY":
            side="WAIT"; is_buy=False; signal_type="NO SIGNAL"; signal_reason=f"WAIT - {trend} Close {last_close:.0f} < EMA20 {ema20:.0f}, tunggu breakout"; signal_strength=0
        trend_mtf=f"{trend} + {mtf_confirm}" if mtf_confirm!="NEUTRAL" else trend
        return {"entry":int(entry),"sl":int(sl),"tp1":int(tp1),"tp2":int(tp2),"atr":float(atr),"risk_pct":round((risk/entry)*100,2) if entry else 0,"rr1":round(rr1,2),"rr2":round(rr2,2),"trend":trend_mtf,"support":int(df['Low'].tail(10).min()),"resistance":int(df['High'].tail(10).max()),"signal_type":signal_type,"signal_reason":signal_reason,"signal_strength":signal_strength,"signal_date":df.index[-1] if hasattr(df.index[-1],'strftime') else get_now_wib(),"all_signals":signals,"buy_signals":buy_sigs,"sell_signals":sell_sigs,"is_buy_signal":is_buy and signal_strength>=70,"is_sell_signal":(not is_buy) and side=="SELL" and signal_strength>=70,"side":side,"mtf_confirm":mtf_confirm}
    except Exception as e:
        print(f"Trading plan error: {e}"); import traceback; traceback.print_exc(); return None

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

# ARJUM WRAPPER
def arjum_get(path, params=None, use_cache=True):
    cache_key=make_cache_key(path,params) if use_cache else None
    if use_cache and cache_key:
        if 'broker' in path:
            cached=get_cached_broker(cache_key)
            if cached is not None: return cached
        elif 'screener' in path:
            cached=get_cached_screener()
            if cached is not None: return cached
    url=f"{ARJUM_BASE}{path}"
    try:
        headers=get_arjum_headers()
        if not headers.get("X-API-Key"): return None
        r=requests.get(url,headers=headers,params=params,timeout=12)
        if r.status_code==200:
            j=r.json()
            if use_cache and cache_key:
                if 'broker' in path: set_cached_broker(cache_key,j)
                elif 'screener' in path: set_cached_screener(j)
            return j
        return None
    except Exception as e:
        print(f"arjum_get error {path}: {e}"); return None

def get_screener_latest():
    data=arjum_get("/screener/latest")
    if not data: return []
    if isinstance(data,dict):
        if 'rows' in data and isinstance(data['rows'],list) and len(data['rows'])>0:
            normalized=[]
            for r in data['rows']:
                code=r.get('stock_code') or r.get('symbol')
                if code: normalized.append({'symbol':code.replace(".JK","").upper(),'raw':r,'bucket':r.get('bucket',''),'summary':r.get('summary','')})
            return normalized
        for k in ['data','results','stocks']:
            if k in data and isinstance(data[k],list) and len(data[k])>0: return data[k]
    return data if isinstance(data,list) else []

# ===== AKUM/DIST FIXED CORE =====
def get_broker_accumulation(symbol, top=10, days=None):
    params={"top":top}
    if days: params["days"]=days; params["period"]=days
    data=arjum_get(f"/broker-accumulation/{symbol}", params=params, use_cache=False)
    if not data: return 0.0, [], 0,0
    raw_brokers=[]; net_period=0; buy_sum=sell_sum=0
    if isinstance(data,dict):
        top_buyers=data.get('top_buyers') or []
        series=data.get('series') or []
        is_timeline=series and isinstance(series[0],dict) and 'accum_val' in series[0]
        if is_timeline:
            if len(series)>=2:
                last=float(series[-1].get('accum_val',0) or 0)
                if days and len(series)>=int(days):
                    n=int(days)
                    first=float(series[-n].get('accum_val',0) or series[0].get('accum_val',0) or 0)
                    net_period=last-first if len(series)>=n+1 else last
                    if n==1: net_period=last
                else:
                    net_period=last
                buy_sum=net_period if net_period>0 else 0
                sell_sum=abs(net_period) if net_period<0 else 0
            for b in top_buyers[:20]:
                if not isinstance(b,dict): continue
                code=b.get('broker_code') or '??'
                nval=float(b.get('nval',0) or b.get('net_val',0) or 0)
                bval=float(b.get('bval',0) or (nval if nval>0 else 0))
                sval=float(b.get('sval',0) or (abs(nval) if nval<0 else 0))
                raw_brokers.append({"broker_code":str(code).upper(),"broker":str(code).upper(),"buy_value":bval,"sell_value":sval,"net_value":nval,"buy_volume":float(b.get('bvol',0) or 0),"sell_volume":float(b.get('svol',0) or 0),"avg_price":float(b.get('bavg',0) or 0)})
            if not raw_brokers and net_period!=0:
                raw_brokers=[{"broker_code":"ALL","broker":"ALL","buy_value":buy_sum,"sell_value":sell_sum,"net_value":net_period,"buy_volume":0,"sell_volume":0,"avg_price":0}]
            return float(net_period), raw_brokers, float(buy_sum), float(sell_sum)
        if series and isinstance(series[0],dict) and 'broker_code' in series[0]:
            for ser in series[:20]:
                code=ser.get('broker_code') or '??'
                points=ser.get('points') or []
                if not points: continue
                pts=points[-int(days):] if days and len(points)>=int(days) else points
                sb=sum([float(p.get('bval',0) or 0) for p in pts]); ss=sum([float(p.get('sval',0) or 0) for p in pts]); sn=sum([float(p.get('nval',0) or 0) for p in pts])
                if sb==0 and ss==0 and sn==0: continue
                raw_brokers.append({"broker_code":str(code).upper(),"broker":str(code).upper(),"buy_value":float(sb),"sell_value":float(ss),"net_value":float(sn),"buy_volume":float(sum([float(p.get('bvol',0) or 0) for p in pts])),"sell_volume":float(sum([float(p.get('svol',0) or 0) for p in pts])),"avg_price":float(pts[-1].get('bavg',0) or 0) if pts else 0})
                buy_sum+=sb; sell_sum+=ss; net_period+=sn
            return float(net_period), raw_brokers, float(buy_sum), float(sell_sum)
    return 0.0, [], 0,0

def get_broker_summary(symbol, days=None):
    base_params=[]
    if days:
        try:
            now_wib=get_now_wib(); end_dt=now_wib; start_dt=end_dt-datetime.timedelta(days=int(days)*2) if int(days)>1 else end_dt
            base_params.append({"start_date":start_dt.strftime('%Y-%m-%d'),"end_date":end_dt.strftime('%Y-%m-%d'),"broker_limit":30,"flow":"all"})
        except: pass
    base_params.extend([{"broker_limit":30,"flow":"all"},{}])
    data=None; used=None
    for p in base_params:
        d=arjum_get(f"/broker-summary/{symbol}", params=p, use_cache=False)
        if d and isinstance(d,dict):
            has=(d.get('brokers') and len(d.get('brokers'))>0) or d.get('net_value') or d.get('bval') or d.get('nval')
            if has: data=d; used=p; break
            if data is None: data=d; used=p
    brokers=[]; net_value=0; buy_total=sell_total=0
    if data and isinstance(data,dict):
        raw=data.get('brokers') or data.get('data') or []
        if not raw and (data.get('buy_value') or data.get('net_value') or data.get('bval') or data.get('nval')):
            bval=float(data.get('buy_value') or data.get('bval') or 0); sval=float(data.get('sell_value') or data.get('sval') or 0); nval=float(data.get('net_value') or data.get('nval') or (bval-sval))
            if bval==0 and sval==0 and nval!=0:
                if nval>0: bval=nval; sval=0
                else: sval=abs(nval); bval=0
            brokers=[{"broker_code":"ALL","broker":"ALL","buy_value":bval,"sell_value":sval,"net_value":nval,"buy_volume":float(data.get('buy_volume',0) or 0),"sell_volume":float(data.get('sell_volume',0) or 0),"avg_price":float(data.get('avg_price',0) or 0)}]
            net_value=nval; buy_total=bval; sell_total=sval
        elif raw:
            for b in raw[:30]:
                if not isinstance(b,dict): continue
                code=b.get('broker_code') or b.get('code') or '??'
                bval=float(b.get('bval') or b.get('buy_value') or 0); sval=float(b.get('sval') or b.get('sell_value') or 0); nval=float(b.get('nval') or b.get('net_value') or 0)
                if bval==0 and sval==0 and nval!=0:
                    if nval>0: bval=nval; sval=0
                    else: sval=abs(nval); bval=0
                if nval==0 and (bval!=0 or sval!=0): nval=bval-sval
                brokers.append({"broker_code":str(code).upper(),"broker":str(code).upper(),"buy_value":bval,"sell_value":sval,"net_value":nval,"buy_volume":float(b.get('bvol',0) or 0),"sell_volume":float(b.get('svol',0) or 0),"avg_price":float(b.get('bavg',0) or 0)})
            net_value=sum([x['net_value'] for x in brokers]); buy_total=sum([x['buy_value'] for x in brokers]); sell_total=sum([x['sell_value'] for x in brokers])
    if net_value==0 and not brokers:
        acc_net, acc_brokers, acc_buy, acc_sell = get_broker_accumulation(symbol, top=10, days=days)
        if acc_net!=0: net_value=acc_net; brokers=acc_brokers; buy_total=acc_buy; sell_total=acc_sell
    status=determine_status(net_value, buy_total, sell_total)
    if used and data: set_cached_broker(make_cache_key(f"/broker-summary/{symbol}",used), data)
    return float(net_value), status, brokers, float(buy_total), float(sell_total)

def calculate_bandars_avg(brokers, hist_df=None, period_days=None):
    try:
        if brokers:
            vals=[float(b.get('avg_price',0)) for b in brokers if b.get('avg_price') and float(b.get('avg_price'))!=0 and float(b.get('net_value',0))>0]
            if vals: return float(np.mean(vals))
    except: pass
    try:
        if hist_df is not None and len(hist_df)>=1:
            df_slice=hist_df.tail(period_days) if period_days else hist_df.tail(1)
            if len(df_slice)>0 and df_slice['Volume'].sum()>0:
                return float((df_slice['Close']*df_slice['Volume']).sum()/df_slice['Volume'].sum())
            return float(df_slice['Close'].iloc[-1])
    except: pass
    return 0

def get_broker_multi_tf(symbol, hist_df=None):
    cache_key=f"multi_{symbol}"
    cached=get_cached_broker(cache_key)
    if cached and hist_df is None:
        nd=cached.get('net_d',0); n5=cached.get('net_5d',0); n20=cached.get('net_20d',0)
        if not (nd!=0 and abs(nd-n5)<1e6 and abs(nd-n20)<1e6): return cached
    net_d_acc, brokers_acc_d, buy_d_acc, sell_d_acc = get_broker_accumulation(symbol, top=20, days=1)
    net_d_sum, status_d_sum, brokers_sum_d, buy_d_sum, sell_d_sum = get_broker_summary(symbol, days=1)
    if abs(net_d_acc) >= abs(net_d_sum) and net_d_acc!=0:
        net_d=net_d_acc; buy_d=buy_d_acc; sell_d=sell_d_acc; brokers_d=brokers_acc_d
    else:
        net_d=net_d_sum; buy_d=buy_d_sum; sell_d=sell_d_sum; brokers_d=brokers_sum_d
    status_d=determine_status(net_d, buy_d, sell_d)
    net_5_acc, brokers_acc_5, buy_5_acc, sell_5_acc = get_broker_accumulation(symbol, top=20, days=5)
    net_5_sum, status_5_sum, brokers_sum_5, buy_5_sum, sell_5_sum = get_broker_summary(symbol, days=5)
    if abs(net_5_acc) >= abs(net_5_sum) and net_5_acc!=0:
        net_5d=net_5_acc; buy_5d=buy_5_acc; sell_5d=sell_5_acc; brokers_5d=brokers_acc_5
    else:
        net_5d=net_5_sum; buy_5d=buy_5_sum; sell_5d=sell_5_sum; brokers_5d=brokers_sum_5
    status_5d=determine_status(net_5d, buy_5d, sell_5d)
    net_20_acc, brokers_acc_20, buy_20_acc, sell_20_acc = get_broker_accumulation(symbol, top=20, days=20)
    net_20_sum, status_20_sum, brokers_sum_20, buy_20_sum, sell_20_sum = get_broker_summary(symbol, days=20)
    if abs(net_20_acc) >= abs(net_20_sum) and net_20_acc!=0:
        net_20d=net_20_acc; buy_20d=buy_20_acc; sell_20d=sell_20_acc; brokers_20d=brokers_acc_20
    else:
        net_20d=net_20_sum; buy_20d=buy_20_sum; sell_20d=sell_20_sum; brokers_20d=brokers_sum_20
    status_20d=determine_status(net_20d, buy_20d, sell_20d)
    vsa_1d=vsa_5d=vsa_20d=0
    if hist_df is not None and len(hist_df)>=5:
        try:
            if 'Net_Val_VSA' not in hist_df.columns: hist_df,_=calculate_vsa_metrics(hist_df)
            vsa_1d=float(hist_df['Net_Val_VSA'].iloc[-1]); vsa_5d=float(hist_df['Net_Val_VSA'].tail(5).sum()); vsa_20d=float(hist_df['Net_Val_VSA'].tail(20).sum())
            if net_d==0 and abs(vsa_1d)>1e8:
                net_d=vsa_1d; status_d=determine_status(net_d, buy_d, sell_d, min_trx=100_000_000)
            if net_5d==0 and abs(vsa_5d)>5e8:
                net_5d=vsa_5d; status_5d=determine_status(net_5d, buy_5d, sell_5d, min_trx=100_000_000)
        except: pass
    avg_d=calculate_bandars_avg(brokers_d, hist_df, 1); avg_5d=calculate_bandars_avg(brokers_5d, hist_df, 5); avg_20d=calculate_bandars_avg(brokers_20d, hist_df, 20)
    result={"accum_d":float(abs(net_d)),"accum_5d":float(abs(net_5d)),"accum_20d":float(abs(net_20d)),"buy_d":float(buy_d),"sell_d":float(sell_d),"buy_5d":float(buy_5d),"sell_5d":float(sell_5d),"buy_20d":float(buy_20d),"sell_20d":float(sell_20d),"net_d":float(net_d),"net_5d":float(net_5d),"net_20d":float(net_20d),"avg_d":float(avg_d),"avg_5d":float(avg_5d),"avg_20d":float(avg_20d),"brokers":brokers_d,"brokers_5d":brokers_5d,"brokers_20d":brokers_20d,"status":status_d,"status_d":status_d,"status_5d":status_5d,"status_20d":status_20d,"vsa_1d":vsa_1d,"vsa_5d":vsa_5d,"vsa_20d":vsa_20d}
    if not (buy_d==0 and sell_d==0 and net_d==0 and len(brokers_d)==0): set_cached_broker(cache_key, result)
    print(f"MTF {symbol}: D={status_d} Net {net_d/1e9:.2f}B B{buy_d/1e9:.1f} S{sell_d/1e9:.1f} | 5D={status_5d} {net_5d/1e9:.2f}B | 20D={status_20d} {net_20d/1e9:.2f}B")
    return result

def format_top_brokers(brokers, top=3, status="AKUM"):
    if not brokers or not isinstance(brokers, list): return "-"
    valid=[b for b in brokers if isinstance(b,dict) and (b.get('broker_code') or b.get('broker'))]
    if not valid: return "-"
    try:
        if status in ["DIST","DISTRIB"]: sorted_b=sorted(valid, key=lambda x: float(x.get('net_value',0) or 0))
        else: sorted_b=sorted(valid, key=lambda x: float(x.get('net_value',0) or x.get('buy_value',0) or 0), reverse=True)
    except: sorted_b=valid
    parts=[]
    for b in sorted_b[:top]:
        code=b.get('broker_code') or b.get('broker') or "??"
        net=float(b.get('net_value',0) or 0); buy=float(b.get('buy_value',0) or 0); sell=float(b.get('sell_value',0) or 0)
        val=abs(net) if net!=0 else (buy if status=="AKUM" else sell)
        if val==0: continue
        if abs(val)>=1e9: s=f"{val/1e9:.1f}B"
        elif abs(val)>=1e6: s=f"{val/1e6:.0f}M"
        else: s=f"{val:.0f}"
        parts.append(f"{code} {s}")
    return ", ".join(parts) if parts else "-"

def get_analysis(symbol):
    data=arjum_get(f"/analysis/{symbol}")
    return data if isinstance(data,dict) else {}
def get_history_pro(symbol, limit=150, timeframe="1d"):
    hist_key=f"{symbol}_{timeframe}_{limit}"
    cached_hist=get_cached_history(hist_key)
    if cached_hist is not None: return cached_hist
    tf=timeframe.lower().strip()
    arjum_frame_map={"1m":"1min","5m":"5min","15m":"15min","30m":"30min","1h":"1hour","4h":"4hour","1d":"daily","1w":"weekly","1mo":"monthly"}
    arjum_frame=arjum_frame_map.get(tf,"daily")
    data=arjum_get(f"/history/{symbol}", params={"limit":limit,"frame":arjum_frame})
    rows=[]
    if data:
        if isinstance(data,dict): rows=data.get('data') or data.get('history') or []
        elif isinstance(data,list): rows=data
    if not rows:
        try:
            import yfinance as yf
            yf_map={"1m":("7d","1m"),"5m":("5d","5m"),"15m":("5d","15m"),"30m":("1mo","30m"),"1h":("1mo","60m"),"4h":("3mo","90m"),"1d":("6mo","1d"),"1w":("1y","1wk")}
            period,interval=yf_map.get(tf,("6mo","1d"))
            hist=yf.Ticker(f"{symbol}.JK").history(period=period,interval=interval,timeout=10)
            if hist is not None and len(hist)>10:
                set_cached_history(hist_key, hist.tail(limit)); return hist.tail(limit)
        except: return None
    try:
        df=pd.DataFrame(rows)
        rename_map={}
        for c in df.columns:
            cl=str(c).lower()
            if cl in ['o','open']: rename_map[c]='Open'
            elif cl in ['h','high']: rename_map[c]='High'
            elif cl in ['l','low']: rename_map[c]='Low'
            elif cl in ['c','close','close_price']: rename_map[c]='Close'
            elif cl in ['v','volume','vol']: rename_map[c]='Volume'
            elif cl in ['date','time','t','datetime','timestamp']: rename_map[c]='Date'
        df.rename(columns=rename_map,inplace=True)
        if 'Date' in df.columns:
            df['Date']=pd.to_datetime(df['Date']); df.set_index('Date',inplace=True)
        df=df.sort_index()
        for col in ['Open','High','Low','Close','Volume']:
            if col in df.columns: df[col]=pd.to_numeric(df[col],errors='coerce')
        df=df.dropna(subset=['Close'])
        if len(df)<10: return None
        set_cached_history(hist_key, df); return df
    except: return None

def generate_pro_chart(df, symbol="BBCA", timeframe="1d", sector_info="IHSG", output_filename="chart.png", extra_info=None):
    try:
        extra_info=extra_info or {}
        df=df.copy().ffill().bfill()
        if not isinstance(df.index,pd.DatetimeIndex): df.index=pd.to_datetime(df.index)
        df=df.sort_index()
        df['EMA13']=df['Close'].ewm(span=13,adjust=False).mean()
        df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
        df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
        df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        df['V2']=df['Volume'].rolling(50,min_periods=1).mean()
        df, buy_ratios=calculate_vsa_metrics(df)
        last_close=df['Close'].iloc[-1]; last_open=df['Open'].iloc[-1]; last_high=df['High'].iloc[-1]; last_low=df['Low'].iloc[-1]; last_vol=df['Volume'].iloc[-1]
        prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close
        chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
        avg_price=df['Close'].tail(20).mean()
        vchg1=(last_vol/df['Volume'].iloc[-2]) if len(df)>1 and df['Volume'].iloc[-2]>0 else 1
        vchg5=(last_vol/df['Volume'].tail(5).mean()) if df['Volume'].tail(5).mean()>0 else 1
        buy_pct=int(buy_ratios[-1]*100); sell_pct=100-buy_pct
        speed="FAST" if vchg1>2.0 else "SLOW" if vchg1<0.8 else "NORMAL"
        if buy_pct>=85 and vchg1>=1.2: power="TURBO"
        elif buy_pct>=70 or vchg1>=1.5: power="STRONG"
        elif buy_pct>=60: power="NORMAL"
        else: power="WEAK"
        safety="GOOD" if last_close>df['EMA200'].iloc[-1] else "BAD"
        net_vol=df['Net_Vol_VSA'].iloc[-1]; net_vol_5d=df['Net_Vol_VSA'].tail(5).sum()
        real_net=extra_info.get('broker_net',0)
        nbsa_rp=abs(real_net) if real_net!=0 else abs(net_vol*last_close)
        plt.style.use('dark_background')
        fig=plt.figure(figsize=(16,9),dpi=180,facecolor='#000000')
        gs=gridspec.GridSpec(4,1,height_ratios=[4.5,1.1,0.9,0.8],hspace=0.05)
        ax_main=fig.add_subplot(gs[0]); ax_vol=fig.add_subplot(gs[1],sharex=ax_main); ax_nbsa=fig.add_subplot(gs[2],sharex=ax_main); ax_mm=fig.add_subplot(gs[3],sharex=ax_main)
        fig.subplots_adjust(left=0.08,right=0.92,top=0.88,bottom=0.06)
        for ax in [ax_main,ax_vol,ax_nbsa,ax_mm]:
            ax.set_facecolor('#000000'); ax.tick_params(colors='#aaaaaa',labelsize=8); ax.yaxis.tick_right(); ax.grid(False)
        x=np.arange(len(df))
        multi_for_signals=extra_info.get('multi_tf')
        try:
            buy_signals,df_with_ind=detect_buy_signals(df,multi_for_signals)
            sell_signals,_=detect_sell_signals(df_with_ind,multi_for_signals)
        except:
            buy_signals,sell_signals,df_with_ind=[],[],df
        extra_info['_chart_buy_signals']=buy_signals; extra_info['_chart_sell_signals']=sell_signals
        plot_df=df_with_ind
        if 'ATR' not in plot_df.columns: plot_df['ATR']=calculate_atr(plot_df,14)
        if 'BB_UPPER' not in plot_df.columns:
            _,bb_up,bb_low=calculate_bollinger_bands(plot_df,20,2); plot_df['BB_UPPER']=bb_up; plot_df['BB_LOWER']=bb_low
        for i in range(len(df)):
            o,h,l,c=df['Open'].iloc[i],df['High'].iloc[i],df['Low'].iloc[i],df['Close'].iloc[i]
            ax_main.plot([i,i],[l,h],color='#00ff00' if c>=o else '#ff0000',linewidth=0.8,alpha=0.8)
            body_low=min(o,c); body_h=max(0.5,abs(c-o))
            rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor='none' if c>=o else '#ff3333',edgecolor='#00ff00' if c>=o else '#ff3333',linewidth=0.8)
            ax_main.add_patch(rect)
        ax_main.plot(x,df['EMA13'],color='#ffff00',linewidth=1.0,alpha=0.9); ax_main.plot(x,df['EMA20'],color='#ff0000',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA50'],color='#ffffff',linewidth=1.0,alpha=0.9); ax_main.plot(x,df['EMA200'],color='#a020f0',linewidth=1.2,alpha=0.9)
        if buy_signals:
            for sig in buy_signals:
                idx=sig['index']
                if idx<len(df):
                    low=df['Low'].iloc[idx]; atr=plot_df['ATR'].iloc[idx] if not pd.isna(plot_df['ATR'].iloc[idx]) else df['Close'].iloc[idx]*0.02
                    ax_main.annotate('▲',xy=(idx,low-atr*0.6),fontsize=14,color='#00ff00',fontweight='bold',ha='center',va='center')
        if sell_signals:
            for sig in sell_signals:
                idx=sig['index']
                if idx<len(df):
                    high=df['High'].iloc[idx]; atr=plot_df['ATR'].iloc[idx] if not pd.isna(plot_df['ATR'].iloc[idx]) else df['Close'].iloc[idx]*0.02
                    ax_main.annotate('▼',xy=(idx,high+atr*0.6),fontsize=14,color='#ff0000',fontweight='bold',ha='center',va='center')
        ax_main.set_xlim(-1,len(df)); ax_main.set_ylim(df['Low'].min()*0.95,df['High'].max()*1.08)
        left_text=f"Avg Price : {avg_price:,.1f}\nVchg 1 Day: {vchg1:.1f} x\nVchg 5 Days: {vchg5:.1f} x\nSpeed : {speed}\nPower : {power}\nSafety : {safety}\n\nEMA 13 : {df['EMA13'].iloc[-1]:,.1f}\nEMA 20 : {df['EMA20'].iloc[-1]:,.1f}\nEMA 50 : {df['EMA50'].iloc[-1]:,.1f}\nEMA 200: {df['EMA200'].iloc[-1]:,.1f}"
        ax_main.text(0.01,0.98,left_text,transform=ax_main.transAxes,va='top',ha='left',fontsize=8,family='monospace',color='#e0e0e0',bbox=dict(facecolor='black',alpha=0.6,edgecolor='none'))
        fig.text(0.01,0.96,f"{symbol} :    {last_close:.0f} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=13,fontweight='bold',ha='left',va='center')
        fig.text(0.5,0.96,"RAFANO TRADER",color='white',fontsize=14,fontweight='bold',ha='center',va='center')
        date_str=df.index[-1].strftime('%d %b %Y %H:%M') if hasattr(df.index[-1],'strftime') else get_now_wib().strftime('%d %b %Y')
        tf_label_map={"1m":"1-Min","5m":"5-Min","15m":"15-Min","30m":"30-Min","1h":"1-Hour","4h":"4-Hour","1d":"Daily","1w":"Weekly"}
        tf_display=tf_label_map.get(timeframe.lower(), timeframe.upper())
        fig.text(0.99,0.96,f"{tf_display} {date_str}",color='#ffcc00',fontsize=10,ha='right',va='center')
        fig.text(0.01,0.905,f"High:{last_high:.0f}   Low:{last_low:.0f}   Open:{last_open:.0f}   Volume:{last_vol:,.0f}   V1:{df['V1'].iloc[-1]:,.0f}   V2:{df['V2'].iloc[-1]:,.0f}",color='#00ffff',fontsize=8,ha='left')
        vol_info=f"Buy Percent = {buy_pct}%   Sell Percent = {sell_pct}%   Net Vol = {net_vol:,.0f}   Net 5D = {net_vol_5d:,.0f}"
        ax_vol.text(0.005,0.88,vol_info,transform=ax_vol.transAxes,color='#ffffff',fontsize=8,va='top')
        ax_vol.bar(x,df['Vol_Sell'],color='#cc0000',width=0.8,alpha=0.8); ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00cc00',width=0.8,alpha=0.9)
        ax_vol.plot(x,df['V1'],color='white',linewidth=0.8,alpha=0.9)
        ax_vol.set_ylim(0,df['Volume'].max()*1.8); plt.setp(ax_vol.get_xticklabels(),visible=False)
        nbsa_info=f"NBSA Rp. {nbsa_rp/1e9:.2f} Milyar"
        ax_nbsa.text(0.005,0.85,nbsa_info,transform=ax_nbsa.transAxes,color='#ffffff',fontsize=8,va='top')
        nbsa_vals=df['Net_Vol_VSA'].tail(80)/(df['Net_Vol_VSA'].abs().max() or 1)*50
        x_nbsa=np.arange(len(df)-len(nbsa_vals),len(df))
        for i,v in zip(x_nbsa,nbsa_vals):
            ax_nbsa.bar(i,v,color='#00ffff' if v>=0 else '#ff4444',width=0.6)
        ax_nbsa.axhline(0,color='#444444',linewidth=0.5)
        ax_nbsa.set_ylim(-60,60)
        ax_mm.text(0.005,0.85,"Market Maker",transform=ax_mm.transAxes,color='#ffffff',fontsize=8,va='top')
        if 'MM' not in df.columns: df['MM']=(df['Close']-df['EMA50'])/df['EMA50']*1000
        mm_vals=df['MM'].tail(80); x_mm=np.arange(len(df)-len(mm_vals),len(df))
        ax_mm.bar(x_mm,mm_vals,color='#cccccc',width=0.5,alpha=0.8)
        step=max(1,len(df)//8); ax_mm.set_xticks(x[::step]); ax_mm.set_xticklabels([df.index[i].strftime('%b') if hasattr(df.index[i],'strftime') else str(i) for i in range(0,len(df),step)],fontsize=7)
        plt.savefig(output_filename,dpi=180,bbox_inches='tight',facecolor='#000000')
        plt.close(fig); return output_filename
    except Exception as e:
        print(f"Chart error {e}"); import traceback; traceback.print_exc()
        try: plt.close('all')
        except: pass
        return None

# TELEGRAM & SCANNER - FULL COMMANDS (ORIGINAL)
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
    score=30; reasons=["Screener"]
    if accum_value>20_000_000_000: score+=30; reasons.append(f"Akum {accum_value/1e9:.1f}B")
    elif accum_value>5_000_000_000: score+=20; reasons.append(f"Akum {accum_value/1e9:.1f}B")
    elif accum_value>0: score+=10; reasons.append("Akum Tipis")
    if broker_net>10_000_000_000: score+=20; reasons.append(f"Net {broker_net/1e9:.1f}B")
    elif broker_net>0: score+=10; reasons.append("Net+")
    try:
        if analysis_data.get('trend')=='BULLISH': score+=20; reasons.append("BULLISH")
        elif history_df is not None and len(history_df)>50:
            ema50=history_df['Close'].ewm(span=50).mean().iloc[-1]
            if history_df['Close'].iloc[-1]>ema50: score+=15; reasons.append(">EMA50")
            score+=5
    except: pass
    if score>=85: label="VERY STRONG"
    elif score>=70: label="STRONG BUY"
    elif score>=50: label="WEAK BUY"
    else: label="NO SIGNAL"
    return score,label,reasons

def scan_v3():
    print(f"[{get_now_wib()}] Scan V3 FIXED...")
    screener_data=get_screener_latest()
    if not screener_data:
        candidates=["BBCA","BBRI","BMRI","TLKM","ASII","GOTO","AMMN","ADRO","ANTM","MDKA","BBNI","BRIS","UNTR","ICBP","INDF"]
        is_fallback=True
    else:
        candidates=[]
        for item in screener_data:
            sym=item.get('symbol') or item.get('code')
            if sym: candidates.append(sym.replace(".JK","").upper())
        candidates=candidates[:25]
        is_fallback=False
    print(f"  Kandidat: {candidates[:10]}")
    detected=[]
    def process_symbol(sym):
        try:
            hist_df=get_history_pro(sym,limit=120,timeframe="1d")
            multi=get_broker_multi_tf(sym,hist_df)
            accum_val=multi['accum_d']; broker_net=multi['net_d']; brokers_combined=multi['brokers']
            analysis=get_analysis(sym)
            score,label,reasons=calculate_score_v2(sym,hist_df,accum_val,broker_net,analysis)
            threshold=20 if is_fallback else 40
            if get_now_wib().weekday()>=5: threshold=max(15,threshold-20)
            if score>=threshold:
                last_close=int(hist_df['Close'].iloc[-1]) if hist_df is not None and len(hist_df)>=2 else 0
                prev=hist_df['Close'].iloc[-2] if hist_df is not None and len(hist_df)>=2 else 0
                change_pct=((last_close/prev)-1)*100 if prev else 0
                tp=calculate_trading_plan(hist_df,multi_tf=multi) if hist_df is not None else None
                return {"symbol":sym,"close":last_close,"change_pct":change_pct,"score":score,"score_label":label,"accum_value":accum_val,"broker_net":broker_net,"broker_status":multi['status'],"reasons":reasons,"history_df":hist_df,"trading_plan":tp,"brokers":brokers_combined,"multi_tf":multi}
        except Exception as e:
            print(f"Error {sym}: {e}")
        return None
    with ThreadPoolExecutor(max_workers=8) as executor:
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
        send_reply(TARGET_CHAT_ID,"V3 Scan: Tidak ada sinyal REAL ACCUM hari ini.")
        return
    now_str=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    # FILTER AKUM ONLY untuk broadcast biar gak campur dist
    akum_only=[s for s in signals if s.get('multi_tf',{}).get('status_d')=='AKUM']
    use_signals=akum_only if akum_only else signals
    header=f"*RAFANO V3 PRO - REAL ACCUM (AKUM ONLY)*\n{now_str}\nTotal: {len(use_signals)} | Cooldown 60m\n============================\n\n"
    msg=header; keyboard=[]
    for idx,item in enumerate(use_signals,1):
        multi=item.get('multi_tf') or {}
        top_d=format_top_brokers(multi.get('brokers',[]) or item.get('brokers',[]),3,multi.get('status_d','AKUM'))
        daily_str=f"Daily: {multi.get('status_d','')} Net {format_large_number(multi.get('net_d',0),True)} | {top_d}"
        weekly_str=f"Weekly 5D: {multi.get('status_5d','')} Net {format_large_number(multi.get('net_5d',0),True)}"
        tp=item.get('trading_plan'); tp_line=f"Entry {tp['entry']} TP1 {tp['tp1']} SL {tp['sl']}" if tp else ""
        item_str=f"{idx}. *{item['symbol']}* -- {item.get('close',0)} ({item.get('change_pct',0):+.2f}%)\n   |- {daily_str}\n   |- {weekly_str}\n   +- {tp_line}\n\n"
        keyboard.append([{"text":f"Pro Chart {item['symbol']}","callback_data":f"chart_{item['symbol']}_1d"}])
        if len(msg)+len(item_str)>3500:
            send_reply(TARGET_CHAT_ID,msg,reply_markup={"inline_keyboard":keyboard}); msg=item_str; keyboard=[]
        else: msg+=item_str
    if msg: send_reply(TARGET_CHAT_ID,msg,reply_markup={"inline_keyboard":keyboard})

def process_chart_request(chat_id, stock_code, timeframe="1d", extra_info_cache=None):
    send_reply(chat_id,f"Generating Pro Chart {stock_code.upper()} ({timeframe.upper()}) + REAL DATA...")
    df=get_history_pro(stock_code,limit=150,timeframe=timeframe)
    if df is None or len(df)<20:
        send_reply(chat_id,f"Data {stock_code} tidak ketemu TF {timeframe}"); return
    if extra_info_cache and stock_code in extra_info_cache:
        extra=extra_info_cache[stock_code]
    else:
        multi=get_broker_multi_tf(stock_code,df)
        extra={"accum_value":multi.get('accum_d',0),"broker_net":multi.get('net_d',0),"broker_status":multi.get('status','NEUTRAL'),"brokers":multi.get('brokers',[]),"multi_tf":multi}
    chart_file=f"/tmp/chart_{stock_code.upper()}_{timeframe}_{int(time.time())}.png"
    try:
        file_path=generate_pro_chart(df=df,symbol=stock_code.upper(),timeframe=timeframe,sector_info=f"{stock_code.upper()} | IHSG",output_filename=chart_file,extra_info=extra)
        multi=extra.get('multi_tf') or {}
        top_d=format_top_brokers(multi.get('brokers',[]) or extra.get('brokers',[]),3,multi.get('status_d','AKUM'))
        tp=calculate_trading_plan(df,multi_tf=multi)
        if tp:
            caption=f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])} | {tp['trend']}\nDaily: {multi.get('status_d')} Net {format_large_number(multi.get('net_d',0),True)} | {top_d}\n{tp['signal_type']} {tp['side']} | Entry {tp['entry']} SL {tp['sl']} TP1 {tp['tp1']}"
        else:
            caption=f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])}\nNet: {format_large_number(extra.get('broker_net',0),True)} TF:{timeframe.upper()}"
        send_photo_reply(chat_id,file_path,caption=caption)
        if os.path.exists(file_path): os.remove(file_path)
    except Exception as e:
        import traceback; traceback.print_exc(); send_reply(chat_id,f"Gagal render: {e}")

# FULL COMMANDS LISTENER (ORIGINAL)
def telegram_bot_listener():
    global LAST_SIGNALS_CACHE
    offset=0
    print("Telegram Listener V3 Running...")
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
        print("Webhook deleted, polling mode active")
    except Exception as e:
        print(f"Webhook delete fail: {e}")
    try:
        r=requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe", timeout=10)
        print(f"Bot Info: {r.json().get('result',{}).get('username')}")
    except Exception as e:
        print(f"Bot token error: {e}")
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
                elif "message" in update and "text" in update["message"]:
                    msg=update["message"]; text=msg.get("text","").strip(); chat_id=msg["chat"]["id"]
                    first_word=text.split()[0].lower() if text else ""
                    print(f"Pesan masuk: {text} dari {chat_id}")
                    if first_word in ["/start","/help"]:
                        help_msg="🤖 *RAFANO V3 PRO FINAL*\n============================\n📈 *CHART & ANALISA*\n`/c <KODE> [TF]` - Chart Pro + Real Akum\n   `/c BBCA` `/c ANTM 15m` `/c BBCA 1h`\n`/b <KODE>` - Detail Bandar / Broker\n`/info <KODE>` - Info lengkap saham\n`/trend <KODE>` - Analisa trend MTF\n\n🔍 *SCREENER*\n`/scan` - Scan V3 Real Accumulation (AKUM only)\n`/scanpro` - Scan + chart top 3\n`/top [N] [akum/dist]` - Top akumulasi\n   `/top 10` `/top 5 dist`\n`/compare <KODE1> <KODE2>` - Bandingkan 2 saham\n\n⭐ *WATCHLIST*\n`/wl` - Lihat watchlist\n`/wl add <KODE>` - Tambah watchlist\n`/wl del <KODE>` - Hapus\n`/wl scan` - Scan hanya watchlist\n\n🛠 *TOOLS*\n`/clearcache` atau `/cc` - Hapus cache Buy 0\n`/help` - Menu ini\n"
                        send_reply(chat_id, help_msg)
                    elif first_word in ["/c","/chart","!chart"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper(); tf=parts[2] if len(parts)>=3 else "1d"
                            threading.Thread(target=process_chart_request, args=(chat_id,sym,tf,LAST_SIGNALS_CACHE), daemon=True).start()
                        else:
                            send_reply(chat_id, "Format: `/c <KODE> [TF]`")
                    elif first_word in ["/b","/broker","/bandar"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            def broker_detail(target_chat, symbol):
                                try:
                                    multi=get_broker_multi_tf(symbol)
                                    net_d,status_d,brokers,buy_d,sell_d=get_broker_summary(symbol)
                                    msg=f"🏦 *BROKER DETAIL {symbol}* -- {get_now_wib().strftime('%d %b %H:%M')}\n"
                                    msg+=f"Status: {status_d} | Net: {format_large_number(net_d, True)}\n"
                                    if multi:
                                        msg+=f"Daily: {multi.get('status_d')} Buy {format_large_number(multi.get('buy_d',0),True)} Sell {format_large_number(multi.get('sell_d',0),True)} Net {format_large_number(multi.get('net_d',0),True)}\n"
                                        msg+=f"  └ Top: {format_top_brokers(multi.get('brokers',[]),3,multi.get('status_d'))}\n"
                                        msg+=f"Weekly: {multi.get('status_5d')} Buy {format_large_number(multi.get('buy_5d',0),True)} Sell {format_large_number(multi.get('sell_5d',0),True)} Net {format_large_number(multi.get('net_5d',0),True)}\n"
                                        msg+=f"Monthly: {multi.get('status_20d')} Buy {format_large_number(multi.get('buy_20d',0),True)} Sell {format_large_number(multi.get('sell_20d',0),True)} Net {format_large_number(multi.get('net_20d',0),True)}\n\n"
                                    msg+="*TOP BROKERS:*\n"
                                    for idx,b in enumerate(brokers[:10],1):
                                        code=b.get('broker_code','??')
                                        buy=format_large_number(b.get('buy_value',0),True); sell=format_large_number(b.get('sell_value',0),True); net=format_large_number(b.get('net_value',0),True)
                                        emoji="🟢" if b.get('net_value',0)>0 else "🔴" if b.get('net_value',0)<0 else "⚪"
                                        msg+=f"{idx}. {emoji} {code} Buy {buy} Sell {sell} Net {net}\n"
                                    send_reply(target_chat, msg)
                                except Exception as e:
                                    send_reply(target_chat, f"Error broker {symbol}: {e}")
                            threading.Thread(target=broker_detail, args=(chat_id,sym), daemon=True).start()
                        else:
                            send_reply(chat_id, "Format: `/b <KODE>`")
                    elif first_word in ["/info","/i"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            def info_detail(target_chat, symbol):
                                try:
                                    df=get_history_pro(symbol,limit=50,timeframe="1d")
                                    multi=get_broker_multi_tf(symbol,df)
                                    last_close=df['Close'].iloc[-1] if df is not None and len(df)>0 else 0
                                    msg=f"📊 *INFO {symbol}* -- {safe_int(last_close)}\n"
                                    if multi:
                                        msg+=f"Bandar: {multi.get('status_d')} | {multi.get('status_5d')} | {multi.get('status_20d')}\n"
                                        msg+=f"Daily Net: {format_large_number(multi.get('net_d',0),True)}\n"
                                        msg+=f"Top: {format_top_brokers(multi.get('brokers',[]),3,multi.get('status_d','AKUM'))}\n\n"
                                    msg+=f"Gunakan `/c {symbol}` untuk chart"
                                    send_reply(target_chat, msg)
                                except Exception as e:
                                    send_reply(target_chat, f"Error info {symbol}: {e}")
                            threading.Thread(target=info_detail, args=(chat_id,sym), daemon=True).start()
                    elif first_word in ["/trend","/t"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            def trend_detail(target_chat, symbol):
                                try:
                                    df=get_history_pro(symbol,limit=150,timeframe="1d")
                                    multi=get_broker_multi_tf(symbol,df)
                                    buy_sigs,_=detect_buy_signals(df,multi)
                                    sell_sigs,_=detect_sell_signals(df,multi)
                                    tp=calculate_trading_plan(df,signals=buy_sigs+sell_sigs,multi_tf=multi)
                                    msg=f"📈 *TREND MTF {symbol}*\n\n"
                                    if multi:
                                        msg+=f"Daily: {multi.get('status_d')} Net {format_large_number(multi.get('net_d',0),True)}\n"
                                        msg+=f"Weekly: {multi.get('status_5d')} Net {format_large_number(multi.get('net_5d',0),True)}\n"
                                        msg+=f"Monthly: {multi.get('status_20d')} Net {format_large_number(multi.get('net_20d',0),True)}\n\n"
                                    if tp:
                                        msg+=f"Signal: {tp.get('signal_type')} | {tp.get('side')} ({tp.get('signal_strength')}%)\n"
                                        msg+=f"Trend: {tp.get('trend')}\n"
                                    send_reply(target_chat, msg)
                                except Exception as e:
                                    send_reply(target_chat, f"Error trend {symbol}: {e}")
                            threading.Thread(target=trend_detail, args=(chat_id,sym), daemon=True).start()
                    elif first_word in ["/top"]:
                        parts=text.split()
                        n=10; filter_status=None
                        if len(parts)>=2:
                            try:
                                n=int(parts[1])
                                if len(parts)>=3: filter_status=parts[2].upper()
                            except:
                                filter_status=parts[1].upper()
                        def top_accum(target_chat, limit, status_filter):
                            try:
                                sigs=list(LAST_SIGNALS_CACHE.values()) if LAST_SIGNALS_CACHE else scan_v3()
                                def get_net(x): return abs((x.get('multi_tf') or {}).get('net_d',0) or x.get('broker_net',0) or 0)
                                sorted_sigs=sorted(sigs, key=get_net, reverse=True)
                                if status_filter:
                                    def match_status(s):
                                        st=(s.get('multi_tf',{}).get('status_d','') or s.get('broker_status','') or '').upper()
                                        if status_filter in ["DIST","DISTRIB"]: return st in ["DIST","DISTRIB"]
                                        if status_filter in ["AKUM","ACCUM"]: return st in ["AKUM","ACCUM"]
                                        return st==status_filter
                                    sorted_sigs=[s for s in sorted_sigs if match_status(s)]
                                msg=f"🏆 *TOP {limit} {status_filter or 'AKUMULASI'}*\n\n"
                                for idx,item in enumerate(sorted_sigs[:limit],1):
                                    multi=item.get('multi_tf') or {}; sym=item.get('symbol','??')
                                    net=multi.get('net_d',0) or item.get('broker_net',0); status=multi.get('status_d','') or item.get('broker_status','')
                                    emoji="🟢" if status=="AKUM" else "🔴" if status=="DIST" else "⚪"
                                    msg+=f"{idx}. {emoji} *{sym}* {status} Net {format_large_number(net,True)} | {format_top_brokers(multi.get('brokers',[]) or item.get('brokers',[]),2,status)}\n"
                                send_reply(target_chat, msg)
                            except Exception as e:
                                send_reply(target_chat, f"Error top: {e}")
                        threading.Thread(target=top_accum, args=(chat_id,n,filter_status), daemon=True).start()
                    elif first_word in ["/compare","/comp"]:
                        parts=text.split()
                        if len(parts)>=3:
                            sym1=parts[1].upper(); sym2=parts[2].upper()
                            def compare_stocks(target_chat, s1, s2):
                                try:
                                    m1=get_broker_multi_tf(s1); m2=get_broker_multi_tf(s2)
                                    df1=get_history_pro(s1,limit=20); df2=get_history_pro(s2,limit=20)
                                    close1=df1['Close'].iloc[-1] if df1 is not None else 0; close2=df2['Close'].iloc[-1] if df2 is not None else 0
                                    msg=f"⚖ *COMPARE {s1} vs {s2}*\n\n"
                                    msg+=f"*{s1}* {safe_int(close1)} | {m1.get('status_d')} Net {format_large_number(m1.get('net_d',0),True)}\n  Top: {format_top_brokers(m1.get('brokers',[]),2,m1.get('status_d'))}\n\n"
                                    msg+=f"*{s2}* {safe_int(close2)} | {m2.get('status_d')} Net {format_large_number(m2.get('net_d',0),True)}\n  Top: {format_top_brokers(m2.get('brokers',[]),2,m2.get('status_d'))}\n\n"
                                    winner=s1 if abs(m1.get('net_d',0))>abs(m2.get('net_d',0)) else s2
                                    msg+=f"🏆 Lebih kuat: *{winner}*"
                                    send_reply(target_chat, msg)
                                except Exception as e:
                                    send_reply(target_chat, f"Error compare: {e}")
                            threading.Thread(target=compare_stocks, args=(chat_id,sym1,sym2), daemon=True).start()
                        else:
                            send_reply(chat_id, "Format: `/compare BBCA BBRI`")
                    elif first_word in ["/wl","/watchlist"]:
                        parts=text.split()
                        WATCHLIST_FILE="/tmp/rafano_watchlist.json"
                        def load_wl():
                            try:
                                import os,json
                                if os.path.exists(WATCHLIST_FILE):
                                    with open(WATCHLIST_FILE,'r') as f: return json.load(f)
                            except: pass
                            return []
                        def save_wl(wl):
                            try:
                                import json
                                with open(WATCHLIST_FILE,'w') as f: json.dump(wl,f)
                            except: pass
                        if len(parts)==1 or parts[1].lower() in ["list","show"]:
                            wl=load_wl()
                            if not wl: send_reply(chat_id, "Watchlist kosong. `/wl add BBCA`")
                            else:
                                msg=f"⭐ *WATCHLIST* ({len(wl)})\n\n"
                                for s in wl: msg+=f"• {s}\n"
                                send_reply(chat_id, msg)
                        elif parts[1].lower()=="add" and len(parts)>=3:
                            sym=parts[2].upper(); wl=load_wl()
                            if sym not in wl: wl.append(sym); save_wl(wl); send_reply(chat_id, f"✅ {sym} ditambah")
                            else: send_reply(chat_id, f"{sym} sudah ada")
                        elif parts[1].lower() in ["del","remove","rm"] and len(parts)>=3:
                            sym=parts[2].upper(); wl=load_wl()
                            if sym in wl: wl.remove(sym); save_wl(wl); send_reply(chat_id, f"🗑 {sym} dihapus")
                            else: send_reply(chat_id, f"{sym} tidak ada")
                        elif parts[1].lower()=="scan":
                            wl=load_wl()
                            if not wl: send_reply(chat_id, "Watchlist kosong")
                            else:
                                def scan_wl(target_chat, symbols):
                                    try:
                                        results=[]
                                        for sym in symbols:
                                            try:
                                                df=get_history_pro(sym,limit=50)
                                                multi=get_broker_multi_tf(sym,df)
                                                results.append({"symbol":sym,"multi_tf":multi,"close":df['Close'].iloc[-1] if df is not None else 0})
                                            except: pass
                                        results=sorted(results, key=lambda x: abs(x.get('multi_tf',{}).get('net_d',0)), reverse=True)
                                        msg=f"⭐ *WATCHLIST SCAN* ({len(results)})\n\n"
                                        for idx,item in enumerate(results,1):
                                            multi=item.get('multi_tf',{})
                                            msg+=f"{idx}. *{item['symbol']}* -- {safe_int(item.get('close',0))} | {multi.get('status_d')} Net {format_large_number(multi.get('net_d',0),True)}\n"
                                        send_reply(target_chat, msg)
                                    except Exception as e:
                                        send_reply(target_chat, f"Error wl scan: {e}")
                                threading.Thread(target=scan_wl, args=(chat_id,wl), daemon=True).start()
                    elif first_word in ["/clearcache","/cc","/clear"]:
                        try:
                            BROKER_CACHE.clear(); HISTORY_CACHE.clear(); SCREENER_CACHE.clear(); LAST_SIGNALS_CACHE.clear()
                            if CACHE_FILE.exists(): CACHE_FILE.unlink()
                            send_reply(chat_id, "🧹 Cache cleared, coba `/scan` lagi")
                        except Exception as e:
                            send_reply(chat_id, f"Error clear: {e}")
                    elif first_word in ["/scan","!scan","/scanpro"]:
                        send_reply(chat_id, "🔍 *V3 Scanning Real Accumulation (AKUM only)...*")
                        def manual_scan(is_pro=False, target_chat=chat_id):
                            global LAST_SIGNALS_CACHE
                            sigs=scan_v3()
                            LAST_SIGNALS_CACHE={s['symbol']: s for s in sigs}
                            akum_only=[s for s in sigs if s.get('multi_tf',{}).get('status_d')=='AKUM']
                            filt=akum_only if akum_only else sigs
                            now_str=get_now_wib().strftime('%d %b %Y %H:%M WIB')
                            if not filt:
                                send_reply(target_chat, f"*RAFANO V3* {now_str}\n0 sinyal AKUM")
                                return
                            header=f"*RAFANO V3 PRO - {now_str}*\nTotal: {len(filt)} (AKUM only, pakai /top dist untuk lihat distribusi)\n\n"
                            msg=header; kb=[]
                            for idx,item in enumerate(filt,1):
                                multi=item.get('multi_tf') or {}
                                top_d=format_top_brokers(multi.get('brokers',[]) or item.get('brokers',[]),2,multi.get('status_d','AKUM'))
                                daily_str=f"Daily: {multi.get('status_d','')} Net {format_large_number(multi.get('net_d',0),True)} | {top_d}"
                                weekly_str=f"Weekly 5D: {multi.get('status_5d','')} Net {format_large_number(multi.get('net_5d',0),True)}"
                                tp=item.get('trading_plan'); tp_line=f"Entry {tp['entry']} TP1 {tp['tp1']} SL {tp['sl']}" if tp else ""
                                item_str=f"{idx}. *{item['symbol']}* -- {item.get('close',0)} ({item.get('change_pct',0):+.2f}%)\n   |- {daily_str}\n   |- {weekly_str}\n   +- {tp_line}\n\n"
                                kb.append([{"text":f"Pro Chart {item['symbol']}","callback_data":f"chart_{item['symbol']}_1d"}])
                                if len(msg)+len(item_str)>3500:
                                    send_reply(target_chat,msg,reply_markup={"inline_keyboard":kb}); msg=item_str; kb=[]
                                else: msg+=item_str
                            send_reply(target_chat,msg,reply_markup={"inline_keyboard":kb})
                            if is_pro:
                                for top in filt[:3]:
                                    process_chart_request(target_chat, top['symbol'], "1d", LAST_SIGNALS_CACHE)
                                    time.sleep(1)
                        is_pro_flag=(first_word=="/scanpro")
                        threading.Thread(target=manual_scan, args=(is_pro_flag,chat_id), daemon=True).start()
        except Exception as e:
            print(f"Listener error: {e}"); time.sleep(3)

def auto_screener_loop():
    global LAST_SIGNALS_CACHE
    print("Auto Screener V3 Active...")
    last_triggered_sesi1,last_triggered_eod="",""
    while True:
        try:
            if not is_market_open(): time.sleep(300); continue
            now=get_now_wib(); today_str,current_time_str=now.strftime('%Y-%m-%d'),now.strftime('%H:%M'); weekday=now.weekday()
            target_sesi1="11:25" if weekday==4 else "11:55"
            if current_time_str==target_sesi1 and last_triggered_sesi1!=today_str:
                sigs=scan_v3(); LAST_SIGNALS_CACHE={s['symbol']: s for s in sigs}
                filt=filter_signals_with_cooldown(sigs); broadcast_v3(filt); last_triggered_sesi1=today_str
            if current_time_str=="15:55" and last_triggered_eod!=today_str:
                sigs=scan_v3(); LAST_SIGNALS_CACHE={s['symbol']: s for s in sigs}
                filt=filter_signals_with_cooldown(sigs); broadcast_v3(filt); last_triggered_eod=today_str
            sigs=scan_v3(); LAST_SIGNALS_CACHE={s['symbol']: s for s in sigs}
            filt=filter_signals_with_cooldown(sigs)
            if filt: broadcast_v3(filt)
            time.sleep(600)
        except Exception as e:
            print(f"Auto loop error: {e}"); time.sleep(10)

if __name__=="__main__":
    print("==========================================")
    print("RAFANO V3 FINAL MINIMAL FIX - AKUM/DIST REAL")
    print("==========================================")
    threading.Thread(target=auto_screener_loop, daemon=True).start()
    telegram_bot_listener()
