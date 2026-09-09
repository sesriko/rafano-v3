"""
RAFANO V4.3.1 FIX WAIT - FIX BUG SETELAH /clearcache PAS QUOTA HABIS
- Fix: setelah clearcache + quota 429 jadi WAIT 0% W0 M0 -> sekarang tetap BUY pakai VSA
- Caption tetap ringkas
"""
import os, time, datetime, threading, requests, pytz, numpy as np, pandas as pd, matplotlib.patches as patches, matplotlib.gridspec as gridspec
from dotenv import load_dotenv
load_dotenv()

def safe_get_env(key):
    v=os.getenv(key)
    if v: return str(v).strip().strip('"').strip("'")
    try:
        from google.colab import userdata
        vv=userdata.get(key)
        if vv:
            vv=str(vv).strip().strip('"').strip("'")
            os.environ[key]=vv
            return vv
    except: pass
    return None

TIMEZONE_WIB=pytz.timezone('Asia/Jakarta')
TELEGRAM_BOT_TOKEN=safe_get_env("TELEGRAM_BOT_TOKEN")
TARGET_CHAT_ID=safe_get_env("TARGET_CHAT_ID")
ARJUM_API_KEY=safe_get_env("ARJUM_API_KEY")
ARJUM_BASE="https://stock.arjum.com/api"
def get_now_wib(): return datetime.datetime.now(TIMEZONE_WIB)
def safe_int(val,default=0):
    try:
        if pd.isna(val) or np.isinf(val): return default
        return int(val)
    except: return default
def format_large_number(val,show_sign=False):
    if pd.isna(val) or val==0: return "0"
    abs_val=abs(val)
    sign="+" if (show_sign and val>0) else ("-" if val<0 else "")
    if abs_val>=1_000_000_000: return f"{sign}{abs_val/1_000_000_000:.2f}B"
    elif abs_val>=1_000_000: return f"{sign}{abs_val/1_000_000:,.0f}M"
    elif abs_val>=1_000: return f"{sign}{abs_val/1_000:,.0f}K"
    else: return f"{sign}{val:,.0f}"
def round_to_ihsg_fraction(price):
    if pd.isna(price) or price<=0: return 0
    price=float(price)
    tick=1 if price<200 else 2 if price<500 else 5 if price<2000 else 10 if price<5000 else 25
    return int(round(price/tick)*tick)
def format_timeframe_label(tf):
    m={"1m":"1 Menit","1min":"1 Menit","5m":"5 Menit","5min":"5 Menit","15m":"15 Menit","15min":"15 Menit","30m":"30 Menit","30min":"30 Menit","1h":"1 Jam","60m":"1 Jam","1hour":"1 Jam","4h":"4 Jam","4hour":"4 Jam","1d":"Daily","daily":"Daily","d":"Daily","1w":"Weekly","weekly":"Weekly","w":"Weekly","1mo":"Monthly","1M":"Monthly","monthly":"Monthly"}
    return m.get((tf or "1d").lower().strip(),(tf or "1d").upper())
def is_intraday_tf(tf): return (tf or "1d").lower().strip() in ["1m","5m","15m","30m","1h","4h","1min","5min","15min","30min","1hour","4hour"]
def source_marker(source):
    if source in ["API_SUMMARY","API_SUMMARY_RANGE"]: return "✅ REAL"
    if source=="VSA_ESTIMATE": return "⚠️ VSA"
    if source=="QUOTA": return "♻️ Cache"
    if source=="EMPTY": return "❓ EMPTY"
    return source
def grade_from_strength(strength,side):
    strength=strength or 0
    if side=="BUY":
        if strength>=85: return "STRONG BUY","#00ff00"
        elif strength>=70: return "BUY","#7CFC00"
        elif strength>=55: return "WATCH","#ffd700"
        else: return "NO SIGNAL","#888888"
    elif side=="SELL":
        if strength>=85: return "STRONG SELL","#ff0000"
        elif strength>=70: return "SELL","#ff6666"
        else: return "WATCH SELL","#ffaa00"
    else: return "WAIT","#888888"
def calculate_atr(df,period=14):
    tr1=df['High']-df['Low']; tr2=(df['High']-df['Close'].shift(1)).abs(); tr3=(df['Low']-df['Close'].shift(1)).abs()
    tr=pd.concat([tr1,tr2,tr3],axis=1).max(axis=1); return tr.rolling(window=period,min_periods=1).mean()
def calculate_vsa_metrics(df):
    df=df.copy()
    price_range=(df['High']-df['Low']).replace(0,0.1)
    close_pos=(df['Close']-df['Low'])/price_range
    close_pos=np.clip(close_pos,0.05,0.95)
    buy_ratio=0.30+close_pos*0.60
    if 'V1' in df.columns:
        vol_ratio=df['Volume']/df['V1'].replace(0,1)
        is_green=df['Close']>=df['Open']
        boost=np.where((vol_ratio>1.5)&is_green,0.10,0)
        boost+=np.where((vol_ratio>2.5)&is_green,0.10,0)
        buy_ratio=buy_ratio+boost
        flat_mask=vol_ratio<0.3
        buy_ratio=np.where(flat_mask,0.30+close_pos*0.60,buy_ratio)
    buy_ratio=np.clip(buy_ratio,0.10,0.95)
    df['Vol_Buy']=df['Volume']*buy_ratio; df['Vol_Sell']=df['Volume']-df['Vol_Buy']
    df['Net_Vol_VSA']=df['Vol_Buy']-df['Vol_Sell']; df['Net_Val_VSA']=df['Net_Vol_VSA']*df['Close']; df['Buy_Pct']=buy_ratio*100
    return df,buy_ratio
def calculate_bollinger_bands(df,period=20,std=2):
    sma=df['Close'].rolling(period).mean(); stddev=df['Close'].rolling(period).std()
    return sma,sma+(stddev*std),sma-(stddev*std)

def detect_buy_signals(df, multi_tf=None):
    signals=[]
    if df is None or len(df)<30: return signals,df
    try:
        df=df.copy(); df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean(); df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
        df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean(); df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        df['ATR']=calculate_atr(df,14); _,bb_upper,bb_lower=calculate_bollinger_bands(df,20,2)
        df['BB_UPPER']=bb_upper; df['BB_LOWER']=bb_lower; df,_=calculate_vsa_metrics(df)
        # FIX: kalau QUOTA, jangan pakai net_d=0
        global QUOTA_HIT
        if QUOTA_HIT or (multi_tf and multi_tf.get('source_d') in ['QUOTA','EMPTY','❓ EMPTY']):
            net_5d=df['Net_Val_VSA'].tail(5).sum()
            if net_5d==0: net_5d=1
        else:
            net_5d=multi_tf.get('net_d',0) if multi_tf else df['Net_Val_VSA'].tail(5).sum()
        for i in range(20,len(df)):
            close=df['Close'].iloc[i]; open_=df['Open'].iloc[i]; low=df['Low'].iloc[i]; vol=df['Volume'].iloc[i]; v1=df['V1'].iloc[i]
            ema50=df['EMA50'].iloc[i]; ema20=df['EMA20'].iloc[i]; bb_low=df['BB_LOWER'].iloc[i] if not pd.isna(df['BB_LOWER'].iloc[i]) else 0
            atr=df['ATR'].iloc[i] if not pd.isna(df['ATR'].iloc[i]) else close*0.03
            prev_close=df['Close'].iloc[i-1]; prev_ema50=df['EMA50'].iloc[i-1]
            is_net_ok=(net_5d>0) if not QUOTA_HIT else True
            if prev_close<=prev_ema50 and close>ema50 and close>ema20 and (vol>v1*1.5 if v1>0 else False) and close>=open_ and is_net_ok:
                signals.append({'index':i,'date':df.index[i],'type':'BO EMA50','side':'BUY','entry':float(close),'sl':float(min(df['Low'].iloc[max(0,i-5):i+1].min(),close-atr*1.2)),'reason':f'BO EMA50 Vol {vol/v1:.1f}x','strength':90}); continue
            if bb_low>0:
                dist=(close-bb_low)/bb_low*100 if bb_low else 0
                is_bow=close<bb_low and dist<-1.5
                body=abs(close-open_); lower_wick=min(open_,close)-low
                is_rev=close>=open_ and lower_wick>body*1.5 if body>0 else False
                if is_bow and is_rev:
                    signals.append({'index':i,'date':df.index[i],'type':'BOW BB','side':'BUY','entry':float(close),'sl':float(low*0.98),'reason':f'BOW {dist:.1f}% + Rev','strength':85}); continue
            dist_ema50=abs(close-ema50)/ema50*100 if ema50>0 else 100
            if dist_ema50<2.0:
                wick_count=0
                for j in range(max(0,i-10),i+1):
                    l=df['Low'].iloc[j]; e50=df['EMA50'].iloc[j]
                    if abs(l-e50)/e50<0.015: wick_count+=1
                if wick_count>=2 and close>ema50 and close>open_:
                    signals.append({'index':i,'date':df.index[i],'type':'BOS EMA','side':'BUY','entry':float(close),'sl':float(min(df['Low'].iloc[max(0,i-3):i+1].min(),ema50*0.97)),'reason':f'BOS Near {dist_ema50:.1f}% {wick_count}x','strength':80})
        filtered=[]; last_idx=-20
        for sig in sorted(signals,key=lambda x:x['index']):
            if sig['index']-last_idx>=5: filtered.append(sig); last_idx=sig['index']
        return filtered,df
    except Exception as e:
        print(f"buy sig err {e}"); return [],df

def detect_sell_signals(df,multi_tf=None):
    signals=[]
    if df is None or len(df)<30: return signals,df
    try:
        if 'EMA50' not in df.columns:
            df=df.copy(); df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean(); df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
            df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean(); df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
            df['ATR']=calculate_atr(df,14); _,bb_upper,_=calculate_bollinger_bands(df,20,2); df['BB_UPPER']=bb_upper; df,_=calculate_vsa_metrics(df)
        net_5d=multi_tf.get('net_d',0) if multi_tf else 0
        for i in range(20,len(df)):
            close=df['Close'].iloc[i]; open_=df['Open'].iloc[i]; high=df['High'].iloc[i]; vol=df['Volume'].iloc[i]; v1=df['V1'].iloc[i]
            ema50=df['EMA50'].iloc[i]; atr=df['ATR'].iloc[i] if not pd.isna(df['ATR'].iloc[i]) else close*0.03
            prev_close=df['Close'].iloc[i-1]
            is_bd=prev_close>=df['EMA50'].iloc[i-1] and close<ema50
            is_red=close<open_; vol_spike=vol>v1*1.5 if v1>0 else False
            if is_bd and vol_spike and is_red and net_5d<0:
                signals.append({'index':i,'date':df.index[i],'type':'BD EMA50','side':'SELL','entry':float(close),'sl':float(max(df['High'].iloc[max(0,i-5):i+1].max(),close+atr*1.2)),'reason':'BD EMA50 + Dist','strength':90})
        return signals,df
    except: return [],df

def calculate_trading_plan(df,signals=None,multi_tf=None,timeframe="1d"):
    try:
        if df is None or len(df)<20: return None
        last_close=df['Close'].iloc[-1]; atr=calculate_atr(df,14).iloc[-1]
        if pd.isna(atr) or atr==0: atr=last_close*0.03
        ema20=df['Close'].ewm(span=20).mean().iloc[-1]; ema50=df['Close'].ewm(span=50).mean().iloc[-1]; ema200=df['Close'].ewm(span=200).mean().iloc[-1]
        if signals is None:
            buy_sigs,_=detect_buy_signals(df,multi_tf); sell_sigs,_=detect_sell_signals(df,multi_tf); signals=buy_sigs+sell_sigs
        else:
            buy_sigs=[s for s in signals if s.get('side')=='BUY']; sell_sigs=[s for s in signals if s.get('side')=='SELL']
        mtf_confirm="NEUTRAL"
        if multi_tf:
            s5=multi_tf.get('status_5d','NEUTRAL'); s20=multi_tf.get('status_20d','NEUTRAL')
            if "AKUM" in s5 and "AKUM" in s20: mtf_confirm="STRONG BULLISH MTF"
            elif "AKUM" in s5 or "AKUM" in s20: mtf_confirm="BULLISH MTF"
            elif "DIST" in s5 and "DIST" in s20: mtf_confirm="BEARISH MTF"
        recent_buy=[s for s in buy_sigs if s['index']>=len(df)-10]; recent_sell=[s for s in sell_sigs if s['index']>=len(df)-10]
        if recent_buy and (not recent_sell or recent_buy[-1]['index']>=recent_sell[-1]['index']):
            ls=recent_buy[-1]; entry=ls['entry']; sl=ls['sl']; side="BUY"; sig_type=ls['type']; sig_reason=ls['reason']; sig_strength=ls['strength']; sig_date=ls['date']
        elif recent_sell:
            ls=recent_sell[-1]; entry=ls['entry']; sl=ls['sl']; side="SELL"; sig_type=ls['type']; sig_reason=ls['reason']; sig_strength=ls['strength']; sig_date=ls['date']
        else:
            entry=round_to_ihsg_fraction(last_close); sl=round_to_ihsg_fraction(max(df['Low'].tail(5).min(),last_close-atr*1.5))
            sig_type="NO SIGNAL"; sig_reason="Tunggu BO/BOS/BOW"; sig_strength=0; sig_date=df.index[-1]; side="WAIT"
        if side=="BUY" and mtf_confirm=="STRONG BULLISH MTF": sig_strength=min(100,sig_strength+10)
        min_sl=last_close*0.92; max_sl=last_close*0.98; sl=max(min(sl,max_sl),min_sl); sl=round_to_ihsg_fraction(sl)
        if entry<=sl and side!="SELL": entry=round_to_ihsg_fraction(sl*1.03)
        if side=="BUY":
            tp1=round_to_ihsg_fraction(entry+atr*1.5); tp2=round_to_ihsg_fraction(entry+atr*3.0)
            risk=entry-sl; reward1=tp1-entry; reward2=tp2-entry
        else:
            tp1=round_to_ihsg_fraction(entry*1.035); tp2=round_to_ihsg_fraction(entry+atr*1.8)
            risk=entry-sl; reward1=tp1-entry; reward2=tp2-entry
        if tp1==tp2: tp2=round_to_ihsg_fraction(entry+atr*3.0)
        rr1=reward1/risk if risk>0 else 0; rr2=reward2/risk if risk>0 else 0
        if last_close>ema20 and last_close>ema50 and last_close>ema200: trend="STRONG UPTREND"
        elif last_close>ema20 and last_close>ema50: trend="UPTREND"
        elif last_close>ema20: trend="WEAK UPTREND"
        else: trend="DOWNTREND"
        return {"entry":int(entry),"sl":int(sl),"tp1":int(tp1),"tp2":int(tp2),"atr":float(atr),"risk_pct":round((risk/entry)*100,2) if entry else 0,"rr1":round(rr1,2),"rr2":round(rr2,2),"trend":f"{trend} + {mtf_confirm}" if mtf_confirm!="NEUTRAL" else trend,"support":int(df['Low'].tail(10).min()),"resistance":int(df['High'].tail(10).max()),"signal_type":sig_type,"signal_reason":sig_reason,"signal_strength":sig_strength,"signal_date":sig_date,"all_signals":signals,"buy_signals":buy_sigs,"sell_signals":sell_sigs,"side":side,"mtf_confirm":mtf_confirm}
    except: return None

def is_market_open():
    now=get_now_wib(); wd=now.weekday()
    if wd>=5: return False
    ct=now.time()
    if wd==4: return (datetime.time(9,0)<=ct<=datetime.time(11,30)) or (datetime.time(14,0)<=ct<=datetime.time(15,50))
    else: return (datetime.time(9,0)<=ct<=datetime.time(12,0)) or (datetime.time(13,30)<=ct<=datetime.time(15,50))

import json
from pathlib import Path
BROKER_CACHE={}; HISTORY_CACHE={}; SCREENER_CACHE={}
CACHE_FILE=Path("/tmp/rafano_cache.json")
BROKER_CACHE_TTL=1800; HISTORY_CACHE_TTL=3600; SCREENER_CACHE_TTL=600
QUOTA_HIT=False; LAST_429_TIME=0
try:
    if CACHE_FILE.exists():
        with open(CACHE_FILE,'r') as cf:
            loaded=json.load(cf)
            BROKER_CACHE={k:(v[0],v[1]) for k,v in loaded.get('broker',{}).items()}
except: pass
def save_cache_to_file():
    try:
        with open(CACHE_FILE,'w') as cf: json.dump({'broker':{k:[v[0],v[1]] for k,v in BROKER_CACHE.items()}},cf)
    except: pass
def get_cached_broker(key,allow_expired=False):
    import time
    if key in BROKER_CACHE:
        ts,data=BROKER_CACHE[key]
        if time.time()-ts<BROKER_CACHE_TTL: return data
        elif allow_expired or QUOTA_HIT: return data
        else: del BROKER_CACHE[key]
    return None
def set_cached_broker(key,data):
    import time
    if data and isinstance(data,dict):
        if data.get('akum_d',0)==0 and data.get('dist_d',0)==0 and len(data.get('brokers',[]))==0 and data.get('net_d',0)==0:
            if data.get('source_d') not in ["QUOTA"]: return
    BROKER_CACHE[key]=(time.time(),data); save_cache_to_file()
def get_cached_history(key):
    import time
    if key in HISTORY_CACHE:
        ts,data=HISTORY_CACHE[key]
        if time.time()-ts<HISTORY_CACHE_TTL: return data
    return None
def set_cached_history(key,data):
    import time; HISTORY_CACHE[key]=(time.time(),data)
def get_cached_screener():
    import time
    if 'latest' in SCREENER_CACHE:
        ts,data=SCREENER_CACHE['latest']
        if time.time()-ts<SCREENER_CACHE_TTL: return data
    return None
def set_cached_screener(data):
    import time; SCREENER_CACHE['latest']=(time.time(),data)
def make_cache_key(path,params):
    if not params: return path
    try: return f"{path}?{'&'.join([f'{k}={v}' for k,v in sorted(params.items())])}"
    except: return path
def arjum_get(path,params=None,use_cache=True,retries=0):
    global QUOTA_HIT,LAST_429_TIME
    import time as _time
    cache_key=make_cache_key(path,params) if use_cache else None
    if use_cache and cache_key and 'broker' in path:
        c=get_cached_broker(cache_key)
        if c: return c
    if use_cache and 'screener' in path:
        c=get_cached_screener()
        if c: return c
    if QUOTA_HIT and _time.time()-LAST_429_TIME<300:
        if cache_key:
            exp=get_cached_broker(cache_key,allow_expired=True)
            if exp: return exp
        return None
    url=f"{ARJUM_BASE}{path}"
    try:
        api_key=os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or safe_get_env("ARJUM_API_KEY") or ""
        headers={"X-API-Key": api_key.strip(),"Accept":"application/json","User-Agent":"Mozilla/5.0"}
        r=requests.get(url,headers=headers,params=params,timeout=15)
        if r.status_code==200:
            j=r.json()
            if use_cache and cache_key:
                if 'broker' in path: set_cached_broker(cache_key,j)
                elif 'screener' in path: set_cached_screener(j)
            QUOTA_HIT=False
            return j
        elif r.status_code==429:
            QUOTA_HIT=True; LAST_429_TIME=_time.time()
            if cache_key:
                exp=get_cached_broker(cache_key,allow_expired=True)
                if exp: return exp
            return None
        else: return None
    except: return None

def _fmt_yyyy_mm_dd(d):
    if d is None: return None
    if hasattr(d,'strftime'): return d.strftime('%Y-%m-%d')
    s=str(d).strip()
    if '/' in s:
        try: dd,mm,yyyy=s.split('/'); return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
        except: return s
    return s

def calc_akum_dist(brokers_list):
    if not brokers_list: return 0,0,0,"NEUTRAL ⚪"
    akum=0.0; dist=0.0
    for b in brokers_list:
        try:
            nval=float(b.get('nval',0) or b.get('net_value',0) or 0)
            if nval>0: akum+=nval
            elif nval<0: dist+=abs(nval)
        except: pass
    net=akum-dist
    if net>0: status="AKUM 🟢"
    elif net<0: status="DIST 🔴"
    else: status="NEUTRAL ⚪"
    return akum,dist,net,status

def get_broker_summary(symbol,date_from=None,date_to=None):
    base_params={"net":"false","broker_limit":20,"level_limit":25,"all_data":"false","flow":"all"}
    if date_from and date_to:
        base_params["start_date"]=_fmt_yyyy_mm_dd(date_from)
        base_params["end_date"]=_fmt_yyyy_mm_dd(date_to)
    data=arjum_get(f"/broker-summary/{symbol}",params=base_params,use_cache=True)
    if (not data or not (data.get('brokers') or data.get('data'))) and 'start_date' in base_params:
        data=arjum_get(f"/broker-summary/{symbol}",params={"net":"false","broker_limit":20,"level_limit":25,"all_data":"false","flow":"all"},use_cache=True)
    brokers=[]; akum=0; dist=0; net_value=0; status="NEUTRAL ⚪"; source="EMPTY"
    if data and isinstance(data,dict):
        raw_list=data.get('brokers') or data.get('data') or []
        for b in raw_list[:20]:
            if not isinstance(b,dict): continue
            code=b.get('broker_code') or '??'
            bval=float(b.get('bval') or b.get('buy_value') or 0)
            sval=float(b.get('sval') or b.get('sell_value') or 0)
            nval=float(b.get('nval') or b.get('net_value') or (bval-sval))
            brokers.append({"broker_code":str(code).upper(),"bval":bval,"sval":sval,"nval":nval,"net_value":nval})
        if brokers:
            akum,dist,net_value,status=calc_akum_dist(brokers)
            source="API_SUMMARY" if 'start_date' not in base_params else "API_SUMMARY_RANGE"
    if not brokers and QUOTA_HIT: source="QUOTA"
    return akum,dist,net_value,status,brokers,source

def calculate_bandars_avg(brokers,hist_df=None,period_days=None):
    try:
        if hist_df is not None and len(hist_df)>=1:
            df_slice=hist_df.tail(period_days) if period_days else hist_df.tail(1)
            if len(df_slice)>0 and df_slice['Volume'].sum()>0:
                return float((df_slice['Close']*df_slice['Volume']).sum()/df_slice['Volume'].sum())
            elif len(df_slice)>0: return float(df_slice['Close'].iloc[-1])
    except: pass
    return 0

def get_broker_multi_tf(symbol,hist_df=None):
    cache_key=f"multi_{symbol}"
    cached=get_cached_broker(cache_key)
    if cached and cached.get('source_d','').startswith('API_SUMMARY'):
        if cached.get('akum_d',0)!=0 or cached.get('dist_d',0)!=0 or len(cached.get('brokers',[]))>0:
            return cached
    cached_exp=get_cached_broker(cache_key,allow_expired=True)
    if cached_exp and QUOTA_HIT: return cached_exp
    def _trading_day_n_ago(n):
        try:
            if hist_df is not None and len(hist_df)>n: return hist_df.index[-(n+1)].date()
        except: pass
        return (get_now_wib()-datetime.timedelta(days=int(n*1.45)+3)).date()
    today=get_now_wib().date()
    if hist_df is not None and len(hist_df)>0:
        try: today=hist_df.index[-1].date()
        except: pass
    date_5d=_trading_day_n_ago(5); date_20d=_trading_day_n_ago(20)
    import time as _t
    akum_d,dist_d,net_d,status_d,brokers_d,src_d=get_broker_summary(symbol)
    _t.sleep(0.4)
    akum_5d,dist_5d,net_5d,status_5d,brokers_5d,src_5d=get_broker_summary(symbol,date_from=date_5d,date_to=today)
    _t.sleep(0.4)
    akum_20d,dist_20d,net_20d,status_20d,brokers_20d,src_20d=get_broker_summary(symbol,date_from=date_20d,date_to=today)
    result={"akum_d":float(akum_d),"dist_d":float(dist_d),"net_d":float(net_d),"akum_5d":float(akum_5d),"dist_5d":float(dist_5d),"net_5d":float(net_5d),"akum_20d":float(akum_20d),"dist_20d":float(dist_20d),"net_20d":float(net_20d),"avg_d":float(calculate_bandars_avg(brokers_d,hist_df,1)),"source_d":src_d,"source_5d":src_5d,"source_20d":src_20d,"brokers":brokers_d,"brokers_5d":brokers_5d,"brokers_20d":brokers_20d,"status_d":status_d,"status_5d":status_5d,"status_20d":status_20d}
    if not (akum_d==0 and dist_d==0 and len(brokers_d)==0 and net_d==0):
        set_cached_broker(cache_key,result)
    return result

def format_top_brokers(brokers,top=3):
    if not brokers: return "-"
    valid=[b for b in brokers if float(b.get('nval',0) or 0)!=0]
    if not valid: valid=[b for b in brokers if isinstance(b,dict)]
    if not valid: return "-"
    sorted_b=sorted(valid,key=lambda x: abs(float(x.get('nval',0) or 0)),reverse=True)
    parts=[]
    for b in sorted_b[:top]:
        code=b.get('broker_code') or "??"
        nval=float(b.get('nval',0) or 0)
        if nval==0: continue
        s=f"{abs(nval)/1e9:.1f}B" if abs(nval)>=1e9 else f"{abs(nval)/1e6:.0f}M"
        parts.append(f"{code}{'+' if nval>0 else '-'}{s}")
    return ", ".join(parts) if parts else "-"

def get_analysis(symbol):
    data=arjum_get(f"/analysis/{symbol}",use_cache=False)
    return data if isinstance(data,dict) else {}

def get_history_pro(symbol,limit=150,timeframe="1d"):
    hist_key=f"{symbol}_{timeframe}_{limit}"
    cached=get_cached_history(hist_key)
    if cached is not None: return cached
    tf=timeframe.lower().strip()
    arjum_frame_map={"1m":"1min","5m":"5min","15m":"15min","30m":"30min","1h":"1hour","4h":"4hour","1d":"daily","1w":"weekly","1mo":"monthly"}
    arjum_frame=arjum_frame_map.get(tf,"daily")
    data=arjum_get(f"/history/{symbol}",params={"limit":limit,"frame":arjum_frame},use_cache=True)
    rows=[]
    if data:
        if isinstance(data,dict): rows=data.get('data') or data.get('history') or []
        elif isinstance(data,list): rows=data
    if rows:
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
                elif cl in ['date','time','t','datetime','timestamp']: rename[c]='Date'
            df.rename(columns=rename,inplace=True)
            if 'Date' in df.columns: df['Date']=pd.to_datetime(df['Date']); df.set_index('Date',inplace=True)
            df=df.sort_index()
            for col in ['Open','High','Low','Close','Volume']: df[col]=pd.to_numeric(df[col],errors='coerce')
            df=df.dropna(subset=['Close'])
            if len(df)>=10:
                set_cached_history(hist_key,df); return df
        except: pass
    try:
        import yfinance as yf
        yf_map={"1m":("7d","1m"),"5m":("5d","5m"),"15m":("5d","15m"),"30m":("1mo","30m"),"1h":("1mo","60m"),"4h":("3mo","90m"),"1d":("6mo","1d"),"1w":("1y","1wk"),"1mo":("2y","1mo")}
        period,interval=yf_map.get(tf,("6mo","1d"))
        hist=yf.Ticker(f"{symbol}.JK").history(period=period,interval=interval,timeout=10)
        if (hist is None or len(hist)<10) and tf in ["1m","5m","15m","30m","1h","4h"]:
            hist=yf.Ticker(f"{symbol}.JK").history(period="6mo",interval="1d",timeout=10)
        if hist is not None and len(hist)>10:
            set_cached_history(hist_key,hist.tail(limit)); return hist.tail(limit)
    except: pass
    return None

def get_screener_latest():
    cached=get_cached_screener()
    if cached:
        if isinstance(cached,dict) and 'rows' in cached:
            norm=[]
            for r in cached['rows']:
                code=r.get('stock_code') or r.get('symbol') or r.get('code')
                if code: norm.append({'symbol':code.replace(".JK","").upper(),'raw':r})
            return norm
        return cached
    data=arjum_get("/screener/latest",use_cache=True)
    if not data: return []
    if isinstance(data,dict):
        if 'rows' in data and isinstance(data['rows'],list):
            norm=[]
            for r in data['rows']:
                code=r.get('stock_code') or r.get('symbol') or r.get('code')
                if code: norm.append({'symbol':code.replace(".JK","").upper(),'raw':r})
            return norm
        for k in ['data','results','stocks']:
            if k in data and isinstance(data[k],list): return data[k]
        return []
    return data if isinstance(data,list) else []

def generate_pro_chart(df,symbol="BBCA",timeframe="1d",sector_info="IHSG",output_filename="chart.png",extra_info=None):
    try:
        import matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt
        extra_info=extra_info or {}; tf_label_disp=extra_info.get('tf_label') or format_timeframe_label(timeframe)
        df=df.copy().ffill().bfill()
        if not isinstance(df.index,pd.DatetimeIndex): df.index=pd.to_datetime(df.index)
        else: df=df.sort_index()
        df['EMA13']=df['Close'].ewm(span=13,adjust=False).mean(); df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
        df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean(); df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean(); df['V2']=df['Volume'].rolling(50,min_periods=1).mean()
        df,buy_ratios=calculate_vsa_metrics(df)
        last_close=df['Close'].iloc[-1]; last_open=df['Open'].iloc[-1]; last_high=df['High'].iloc[-1]; last_low=df['Low'].iloc[-1]; last_vol=df['Volume'].iloc[-1]
        prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close; chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
        avg_price=df['Close'].tail(20).mean(); vchg1=(last_vol/df['Volume'].iloc[-2]) if len(df)>1 and df['Volume'].iloc[-2]>0 else 1
        avg5=df['Volume'].tail(5).mean(); vchg5=(last_vol/avg5) if avg5>0 else 1
        speed="FAST" if vchg1>2.0 else "SLOW" if vchg1<0.8 else "NORMAL"; buy_pct_temp=int(buy_ratios[-1]*100)
        power="TURBO" if buy_pct_temp>=85 and vchg1>=1.2 else "STRONG" if buy_pct_temp>=70 or vchg1>=1.5 else "NORMAL" if buy_pct_temp>=60 else "WEAK"
        safety="GOOD" if last_close>df['EMA200'].iloc[-1] else "BAD"
        ema13=df['EMA13'].iloc[-1]; ema20=df['EMA20'].iloc[-1]; ema50=df['EMA50'].iloc[-1]; ema200=df['EMA200'].iloc[-1]
        buy_pct=int(buy_ratios[-1]*100); sell_pct=100-buy_pct; net_vol=df['Net_Vol_VSA'].iloc[-1]; net_vol_5d=df['Net_Vol_VSA'].tail(5).sum()
        real_net=extra_info.get('broker_net',0); nbsa_rp=abs(real_net) if real_net!=0 else abs(net_vol*last_close)
        is_real=extra_info.get('is_real',False)
        nbsa_label=f"NBSA Rp. {nbsa_rp/1e9:.2f} M {'REAL' if is_real else '≈ VSA'}"
        plt.style.use('dark_background'); fig=plt.figure(figsize=(16,9),dpi=200,facecolor='#000000')
        gs=gridspec.GridSpec(4,1,height_ratios=[4.5,1.1,0.9,0.8],hspace=0.05)
        ax_main=fig.add_subplot(gs[0]); ax_vol=fig.add_subplot(gs[1],sharex=ax_main); ax_nbsa=fig.add_subplot(gs[2],sharex=ax_main); ax_mm=fig.add_subplot(gs[3],sharex=ax_main)
        fig.subplots_adjust(left=0.08,right=0.92,top=0.88,bottom=0.06)
        for ax in [ax_main,ax_vol,ax_nbsa,ax_mm]: ax.set_facecolor('#000000'); ax.tick_params(colors='#aaaaaa',labelsize=8); ax.yaxis.tick_right(); ax.grid(False)
        x=np.arange(len(df)); multi_for_signals=extra_info.get('multi_tf') if extra_info else None
        try: buy_signals,df_with_ind=detect_buy_signals(df,multi_for_signals); sell_signals,_=detect_sell_signals(df_with_ind,multi_for_signals)
        except: buy_signals=[]; sell_signals=[]; df_with_ind=df
        plot_df=df_with_ind
        if 'ATR' not in plot_df.columns: plot_df['ATR']=calculate_atr(plot_df,14)
        if 'BB_UPPER' not in plot_df.columns: _,bb_up,bb_low=calculate_bollinger_bands(plot_df,20,2); plot_df['BB_UPPER']=bb_up; plot_df['BB_LOWER']=bb_low
        for i in range(len(df)):
            o,h,l,c=df['Open'].iloc[i],df['High'].iloc[i],df['Low'].iloc[i],df['Close'].iloc[i]
            ax_main.plot([i,i],[l,h],color='#00ff00' if c>=o else '#ff0000',linewidth=0.8,alpha=0.8)
            body_low=min(o,c); body_h=max(0.5,abs(c-o))
            if c>=o: rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor='none',edgecolor='#00ff00',linewidth=0.8)
            else: rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor='#ff3333',edgecolor='#ff3333',linewidth=0.8)
            ax_main.add_patch(rect)
        ax_main.plot(x,df['EMA13'],color='#ffff00',linewidth=1.0,alpha=0.9); ax_main.plot(x,df['EMA20'],color='#ff0000',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA50'],color='#ffffff',linewidth=1.0,alpha=0.9); ax_main.plot(x,df['EMA200'],color='#a020f0',linewidth=1.2,alpha=0.9)
        if 'BB_UPPER' in plot_df.columns:
            ax_main.plot(x,plot_df['BB_UPPER'],color='#8888ff',linewidth=0.8,alpha=0.4,linestyle='--'); ax_main.plot(x,plot_df['BB_LOWER'],color='#8888ff',linewidth=0.8,alpha=0.4,linestyle='--')
            ax_main.fill_between(x,plot_df['BB_LOWER'],plot_df['BB_UPPER'],color='#8888ff',alpha=0.04)
        if buy_signals:
            for sig in buy_signals:
                idx=sig['index']
                if idx<len(df):
                    low=df['Low'].iloc[idx]; atr=plot_df['ATR'].iloc[idx] if not pd.isna(plot_df['ATR'].iloc[idx]) else df['Close'].iloc[idx]*0.02
                    ax_main.annotate('▲',xy=(idx,low-atr*0.6),fontsize=14,color='#00ff00',fontweight='bold',ha='center',va='center')
                    ax_main.text(idx,low-atr*1.3,sig['type'],fontsize=6,color='#00ff00',fontweight='bold',ha='center',va='top',bbox=dict(facecolor='black',alpha=0.7,edgecolor='#00ff00',boxstyle='round,pad=0.2'))
        if sell_signals:
            for sig in sell_signals:
                idx=sig['index']
                if idx<len(df):
                    high=df['High'].iloc[idx]; atr=plot_df['ATR'].iloc[idx] if not pd.isna(plot_df['ATR'].iloc[idx]) else df['Close'].iloc[idx]*0.02
                    ax_main.annotate('▼',xy=(idx,high+atr*0.6),fontsize=14,color='#ff0000',fontweight='bold',ha='center',va='center')
        if len(df)>15:
            box_left=len(df)-15; box_right=len(df)-1; y_low=df['Low'].iloc[-15:].min()*0.99; y_high=df['High'].iloc[-15:].max()*1.01
            ax_main.plot([box_left,box_right],[y_high,y_high],color='white',linestyle='--',linewidth=0.6,alpha=0.6); ax_main.plot([box_left,box_right],[y_low,y_low],color='white',linestyle='--',linewidth=0.6,alpha=0.6)
            ax_main.plot([box_left,box_left],[y_low,y_high],color='white',linestyle='--',linewidth=0.6,alpha=0.6); ax_main.plot([box_right,box_right],[y_low,y_high],color='white',linestyle='--',linewidth=0.6,alpha=0.6)
        right_pad=max(3,int(len(df)*0.04)); ax_main.set_xlim(-1,len(df)-1+right_pad); ax_main.set_ylim(df['Low'].min()*0.95,df['High'].max()*1.08)
        left_text=f"Avg Price : {avg_price:,.1f}\nVchg 1 Bar: {vchg1:.1f} x\nVchg 5 Bar: {vchg5:.1f} x\nSpeed : {speed}\nPower : {power}\nSafety : {safety}\n\nEMA 13 : {ema13:,.1f}\nEMA 20 : {ema20:,.1f}\nEMA 50 : {ema50:,.1f}\nEMA 200: {ema200:,.1f}"
        ax_main.text(0.01,0.98,left_text,transform=ax_main.transAxes,va='top',ha='left',fontsize=8,family='monospace',color='#e0e0e0',bbox=dict(facecolor='black',alpha=0.6,edgecolor='none'))
        fig.text(0.01,0.96,f"{symbol} :    {last_close:.0f} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=13,fontweight='bold',ha='left',va='center')
        fig.text(0.01,0.93,f"{sector_info}",color='#ffaa00',fontsize=8,ha='left')
        grade_label=extra_info.get('signal_grade'); grade_color=extra_info.get('signal_grade_color','#888888')
        if grade_label: fig.text(0.01,0.905,f"● {grade_label}",color=grade_color,fontsize=9,fontweight='bold',ha='left',va='center')
        fig.text(0.5,0.96,"RAFANO TRADER",color='white',fontsize=14,fontweight='bold',ha='center',va='center')
        if is_intraday_tf(timeframe) and hasattr(df.index[-1],'strftime'): date_str=df.index[-1].strftime('%d %b %Y %H:%M')
        elif hasattr(df.index[-1],'strftime'): date_str=df.index[-1].strftime('%d %b %Y')
        else: date_str=get_now_wib().strftime('%d %b %Y')
        fig.text(0.99,0.96,f"{tf_label_disp} | {date_str}",color='#ffcc00',fontsize=10,ha='right',va='center')
        fig.text(0.99,0.93,f"Command BOT /C {symbol}",color='white',fontsize=8,ha='right')
        fig.text(0.01,0.885,f"High:{last_high:.0f} Low:{last_low:.0f} Open:{last_open:.0f} Vol:{last_vol:,.0f}",color='#00ffff',fontsize=8,ha='left')
        ax_main.text(1.005,ema200,f" EMA 200 ",transform=ax_main.get_yaxis_transform(),color='black',backgroundcolor='#a020f0',fontsize=7,fontweight='bold',va='center')
        ax_main.text(1.005,last_close,f" {last_close:.0f} ",transform=ax_main.get_yaxis_transform(),color='black',backgroundcolor='white',fontsize=8,fontweight='bold',va='center')
        vol_info=f"Buy % = {buy_pct}% Sell % = {sell_pct}% Net Vol = {net_vol:,.0f} 5D = {net_vol_5d:,.0f}"
        ax_vol.text(0.005,0.88,vol_info,transform=ax_vol.transAxes,color='#ffffff',fontsize=8,va='top')
        ax_vol.bar(x,df['Vol_Sell'],color='#cc0000',width=0.8,alpha=0.8); ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00cc00',width=0.8,alpha=0.9); ax_vol.plot(x,df['V1'],color='white',linewidth=0.8,alpha=0.9)
        ax_vol.set_ylim(0,df['Volume'].max()*1.8); plt.setp(ax_vol.get_xticklabels(),visible=False)
        ax_nbsa.text(0.005,0.85,nbsa_label,transform=ax_nbsa.transAxes,color='#ffffff',fontsize=8,va='top')
        nbsa_vals=df['Net_Vol_VSA'].tail(80)/(df['Net_Vol_VSA'].abs().max() or 1)*50; x_nbsa=np.arange(len(df)-len(nbsa_vals),len(df))
        for i,v in zip(x_nbsa,nbsa_vals): ax_nbsa.bar(i,v,color='#00ffff' if v>=0 else '#ff4444',width=0.6)
        ax_nbsa.axhline(0,color='#444444',linewidth=0.5); ax_nbsa.set_ylim(-60,60)
        ax_mm.text(0.005,0.85,"Market Maker",transform=ax_mm.transAxes,color='#ffffff',fontsize=8,va='top')
        if 'MM' not in df.columns: df['MM']=(df['Close']-df['EMA50'])/df['EMA50']*1000
        mm_vals=df['MM'].tail(80); x_mm=np.arange(len(df)-len(mm_vals),len(df)); ax_mm.bar(x_mm,mm_vals,color='#cccccc',width=0.5,alpha=0.8)
        last_mm=df['MM'].iloc[-1]; ax_mm.text(1.005,last_mm,f" {last_mm:.4f} ",transform=ax_mm.get_yaxis_transform(),color='black',backgroundcolor='#ffff00',fontsize=7,fontweight='bold',va='center')
        step=max(1,len(df)//8); ax_mm.set_xticks(x[::step])
        if is_intraday_tf(timeframe): ax_mm.set_xticklabels([df.index[i].strftime('%H:%M') if hasattr(df.index[i],'strftime') else str(i) for i in range(0,len(df),step)],fontsize=7)
        else: ax_mm.set_xticklabels([df.index[i].strftime('%b') if hasattr(df.index[i],'strftime') else str(i) for i in range(0,len(df),step)],fontsize=7)
        plt.savefig(output_filename,dpi=200,bbox_inches='tight',facecolor='#000000'); return output_filename
    except Exception as e:
        print(f"Chart error {e}"); return None
    finally:
        try: import matplotlib.pyplot as plt; plt.clf(); plt.close('all')
        except: pass

def build_tech_caption(df,multi,tp,timeframe="1d"):
    try:
        last_close=df['Close'].iloc[-1]; last_vol=df['Volume'].iloc[-1]
        avg_vol20=df['Volume'].rolling(20).mean().iloc[-1]; vchg=last_vol/avg_vol20 if avg_vol20 else 1
        ema13=df['Close'].ewm(span=13).mean().iloc[-1]; ema20=df['Close'].ewm(span=20).mean().iloc[-1]
        ema50=df['Close'].ewm(span=50).mean().iloc[-1]; ema200=df['Close'].ewm(span=200).mean().iloc[-1]
        sma20=df['Close'].rolling(20).mean().iloc[-1]; std20=df['Close'].rolling(20).std().iloc[-1]
        bb_upper=sma20+2*std20; bb_lower=sma20-2*std20
        if last_close>bb_upper: bb_pos="DI ATAS Upper 🔥"
        elif last_close<bb_lower: bb_pos="DI BAWAH Lower"
        else: bb_pos="DALAM BB"
        buy_ratio=50
        if 'Buy_Pct' in df.columns: buy_ratio=df['Buy_Pct'].iloc[-1]
        delta=df['Close'].diff(); gain=delta.where(delta>0,0).ewm(alpha=1/14,min_periods=14).mean(); loss=(-delta.where(delta<0,0)).ewm(alpha=1/14,min_periods=14).mean()
        rs=gain.iloc[-1]/(loss.iloc[-1]+0.00001); rsi=100-(100/(1+rs))
        if 'MM' not in df.columns: df['MM']=(df['Close']-df['Close'].ewm(span=50).mean())/df['Close'].ewm(span=50).mean()*1000
        mm_val=df['MM'].iloc[-1]
        if mm_val>0 and df['MM'].tail(3).mean()>df['MM'].tail(6).mean(): mm_trend="AKUM 🟢"
        elif mm_val<0: mm_trend="DIST 🔴"
        else: mm_trend="NETRAL"
        if last_close>ema13>ema20>ema50: ema_trend="UPTREND 🟢"
        elif last_close>ema50: ema_trend="WEAK UPTREND 🟡"
        elif last_close<ema20 and last_close<ema50: ema_trend="DOWNTREND 🔴"
        else: ema_trend="SIDEWAYS ⚪"
        speed="FAST" if vchg>2 else "SLOW" if vchg<0.8 else "NORMAL"
        power="TURBO" if buy_ratio>=85 and vchg>=1.2 else "STRONG" if buy_ratio>=70 else "WEAK"
        return {"ema50":ema50,"ema200":ema200,"bb_upper":bb_upper,"bb_lower":bb_lower,"bb_pos":bb_pos,"buy_ratio":buy_ratio,"rsi":rsi,"mm_val":mm_val,"mm_trend":mm_trend,"ema_trend":ema_trend,"vchg":vchg,"speed":speed,"power":power}
    except: return {}

LAST_SENT_SIGNALS={}; COOLDOWN_SECONDS=3600; LAST_RESET_DATE=""
def filter_signals_with_cooldown(signals):
    global LAST_RESET_DATE,LAST_SENT_SIGNALS
    today=get_now_wib().strftime('%Y-%m-%d')
    if LAST_RESET_DATE!=today: LAST_SENT_SIGNALS.clear(); LAST_RESET_DATE=today
    filt=[]
    for sig in signals:
        if time.time()-LAST_SENT_SIGNALS.get(sig['symbol'],0)>=COOLDOWN_SECONDS:
            filt.append(sig); LAST_SENT_SIGNALS[sig['symbol']]=time.time()
    return filt

def calculate_score_v2(symbol,hist_df,akum,dist,net,analysis):
    score=30; reasons=["Screener"]
    abs_net=abs(net)
    if abs_net>20_000_000_000: score+=30; reasons.append(f"{'AKUM' if net>0 else 'DIST'} {abs_net/1e9:.1f}B REAL")
    elif abs_net>5_000_000_000: score+=20; reasons.append(f"{'AKUM' if net>0 else 'DIST'} {abs_net/1e9:.1f}B")
    elif abs_net>0: score+=10
    try:
        if analysis.get('trend')=='BULLISH': score+=20
        elif hist_df is not None and len(hist_df)>50 and hist_df['Close'].iloc[-1]>hist_df['Close'].ewm(span=50).mean().iloc[-1]: score+=15
    except: pass
    label="VERY STRONG" if score>=85 else "STRONG BUY" if score>=70 else "WEAK BUY" if score>=50 else "WATCH" if score>=35 else "NO SIGNAL"
    return score,label,reasons

def scan_v3_full():
    print(f"[{get_now_wib()}] 🚀 SCAN RINGKAS...")
    screener_data=get_screener_latest()
    if not screener_data:
        candidates=["BBCA","BBRI","BMRI","BBNI","BRIS","TLKM","ASII","ADRO","ANTM","MDKA","BRMS","BREN","CUAN","WIFI","BIPI","BULL","NIKL"]
    else:
        candidates=[]
        for item in screener_data:
            sym=item.get('symbol') or item.get('code')
            if sym: candidates.append(sym.replace(".JK","").upper())
        candidates=list(dict.fromkeys(candidates))
    detected=[]
    def process_symbol(sym):
        if QUOTA_HIT: return None
        try:
            hist_df=get_history_pro(sym,limit=120,timeframe="1d")
            if hist_df is None or len(hist_df)<20: return None
            multi=get_broker_multi_tf(sym,hist_df)
            akum=multi.get('akum_d',0); dist=multi.get('dist_d',0); net=multi.get('net_d',0)
            status=multi.get('status_d','NEUTRAL ⚪')
            last_close=hist_df['Close'].iloc[-1]
            ema50=hist_df['Close'].ewm(span=50).mean().iloc[-1] if len(hist_df)>=50 else last_close
            # FIX: tetap BUY kalau close>EMA50 walau net=0 pas quota
            is_buy=("AKUM" in status and net>0) or (last_close>ema50) or (akum>0) or (net>0)
            if not is_buy and last_close>ema50: is_buy=True
            if not is_buy: return None
            analysis=get_analysis(sym)
            score,label,reasons=calculate_score_v2(sym,hist_df,akum,dist,net,analysis)
            if score>=35:
                prev=hist_df['Close'].iloc[-2] if len(hist_df)>=2 else last_close
                change_pct=((last_close/prev)-1)*100 if prev else 0
                tp=calculate_trading_plan(hist_df,multi_tf=multi,timeframe="1d")
                return {"symbol":sym,"close":int(last_close),"change_pct":change_pct,"score":score,"score_label":label,"akum_value":akum,"dist_value":dist,"broker_net":net,"broker_status":status,"reasons":reasons,"history_df":hist_df,"trading_plan":tp,"brokers":multi.get('brokers',[]),"multi_tf":multi}
        except: return None
    for idx,sym in enumerate(candidates):
        if QUOTA_HIT: print(f"⛔ Quota habis di {idx}/{len(candidates)}"); break
        res=process_symbol(sym)
        if res: detected.append(res); print(f"✅ {res['symbol']} {res['score']}%")
        time.sleep(0.7)
    detected.sort(key=lambda x: (x['multi_tf'].get('net_d',0),x['score']),reverse=True)
    return detected

def scan_v3(): return scan_v3_full()
def send_reply(chat_id,text,reply_markup=None):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload={"chat_id":chat_id,"text":text,"parse_mode":"Markdown"}
    if reply_markup: payload["reply_markup"]=reply_markup
    try: requests.post(url,json=payload,timeout=15)
    except: pass
def send_photo_reply(chat_id,photo_path,caption=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path,'rb') as photo:
            requests.post(url,data={'chat_id':chat_id,'caption':caption,'parse_mode':'Markdown'},files={'photo':photo},timeout=30)
    except Exception as e: print(f"Photo err {e}")

def broadcast_v3(signals):
    if not signals:
        msg="Scan: Tidak ada BUY / Quota habis."+("\n♻️ Cache" if QUOTA_HIT else "")
        send_reply(TARGET_CHAT_ID,msg); return
    now_str=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header=f"*RAFANO V4.3.1 FIX*\n{now_str} | {len(signals)} BUY\n{'='*30}\n\n"
    msg=header; keyboard=[]
    for idx,item in enumerate(signals,1):
        multi=item.get('multi_tf') or {}; net_d=multi.get('net_d',0)
        status_d=multi.get('status_d','NEUTRAL'); src_d=source_marker(multi.get('source_d','EMPTY'))
        top=format_top_brokers(multi.get('brokers',[]),2)
        bandar_str=f"{status_d} Net {format_large_number(net_d,True)} | {src_d}"
        if top!="-": bandar_str+=f" | {top}"
        item_str=f"{idx}. *{item['symbol']}* {item['close']} ({item['change_pct']:+.1f}%) {item['score']}% \n   {bandar_str}\n\n"
        keyboard.append([{"text":f"{item['symbol']}", "callback_data":f"chart_{item['symbol']}_1d"}])
        if len(msg)+len(item_str)>3500:
            send_reply(TARGET_CHAT_ID,msg,reply_markup={"inline_keyboard":keyboard}); msg=item_str; keyboard=[]
        else: msg+=item_str
    if msg: send_reply(TARGET_CHAT_ID,msg,reply_markup={"inline_keyboard":keyboard})

def process_chart_request(chat_id,stock_code,timeframe="1d",extra_info_cache=None):
    is_intraday=is_intraday_tf(timeframe)
    tf_label=format_timeframe_label(timeframe)
    send_reply(chat_id,f"📊 *{stock_code.upper()} ({tf_label})...*")
    df=get_history_pro(stock_code,limit=150,timeframe=timeframe)
    if df is None or len(df)<20: send_reply(chat_id,f"⚠️ Data {stock_code} tidak ada"); return
    multi=get_broker_multi_tf(stock_code,df) if is_intraday else (extra_info_cache[stock_code].get('multi_tf') if extra_info_cache and stock_code in extra_info_cache else get_broker_multi_tf(stock_code,df))
    if not multi: multi=get_broker_multi_tf(stock_code,df)
    akum_d=multi.get('akum_d',0) if multi else 0; dist_d=multi.get('dist_d',0) if multi else 0; net_d=multi.get('net_d',0) if multi else 0
    src_d=multi.get('source_d','EMPTY') if multi else 'EMPTY'; is_real=src_d.startswith('API_SUMMARY')
    extra={"akum_value":akum_d,"dist_value":dist_d,"broker_net":net_d,"brokers":multi.get('brokers',[]) if multi else [],"multi_tf":multi,"is_real":is_real,"tf_label":tf_label}
    tp=calculate_trading_plan(df,signals=None,multi_tf=multi,timeframe=timeframe)
    side=tp.get('side','WAIT') if tp else 'WAIT'; sig_strength=tp.get('signal_strength',0) if tp else 0
    grade_label,grade_color=grade_from_strength(sig_strength,side)
    extra['signal_grade']=grade_label; extra['signal_grade_color']=grade_color
    tech=build_tech_caption(df,multi,tp,timeframe=timeframe)
    chart_file=f"chart_{stock_code.upper()}_{timeframe}_{int(time.time())}.png"
    try:
        file_path=generate_pro_chart(df=df,symbol=stock_code.upper(),timeframe=timeframe,sector_info=f"{stock_code.upper()} | IHSG",output_filename=chart_file,extra_info=extra)
        if not file_path or not os.path.exists(file_path): send_reply(chat_id,"❌ Gagal render"); return
        if multi:
            marker_d=source_marker(multi.get('source_d','EMPTY'))
            if akum_d==0 and dist_d==0 and not is_real:
                bandar_line=f"{multi.get('status_d')} | {marker_d}"
                if tech.get('buy_ratio',0)>0 and tech.get('buy_ratio',0)!=50:
                    bandar_line+=f" | VSA Buy {tech.get('buy_ratio',0):.0f}%"
                else:
                    bandar_line+=f" | VSA {tech.get('buy_ratio',0):.0f}% (quota habis)"
            else:
                if akum_d>0 and dist_d>0:
                    bandar_line=f"{multi.get('status_d')} AKUM {format_large_number(akum_d,True)} DIST {format_large_number(dist_d,True)} Net {format_large_number(net_d,True)} | {marker_d}"
                elif akum_d>0:
                    bandar_line=f"{multi.get('status_d')} AKUM {format_large_number(akum_d,True)} Net {format_large_number(net_d,True)} | {marker_d}"
                else:
                    bandar_line=f"{multi.get('status_d')} DIST {format_large_number(dist_d,True)} Net {format_large_number(net_d,True)} | {marker_d}"
            top=format_top_brokers(multi.get('brokers',[]),3)
            if top!="-": bandar_line+=f"\nTop: {top}"
            weekly_line=f"W {format_large_number(multi.get('net_5d',0),True)} M {format_large_number(multi.get('net_20d',0),True)}"
        else:
            bandar_line="No broker"; weekly_line=""
        if tp:
            caption=(f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])} | {tp['trend']}\n"
                     f"🟢 *{grade_label}* {sig_strength}% | {tp.get('signal_type','')} | TF: {tf_label}\n"
                     f"------------------\n"
                     f"📦 {bandar_line}\n"
                     f"{weekly_line}\n"
                     f"------------------\n"
                     f"📊 {tech.get('ema_trend','')} | RSI {tech.get('rsi',0):.1f} | {tech.get('bb_pos','')}\n"
                     f"Vol {format_large_number(df['Volume'].iloc[-1],False)} {tech.get('vchg',0):.1f}x {tech.get('speed','')} {tech.get('power','')} | MM {tech.get('mm_val',0):.0f} {tech.get('mm_trend','')}\n"
                     f"------------------\n"
                     f"🎯 {tp.get('signal_reason','')}\n"
                     f"Entry {tp['entry']} SL {tp['sl']} ({tp['risk_pct']}%) | TP1 {tp['tp1']} TP2 {tp['tp2']} RR {tp['rr1']}/{tp['rr2']}\n"
                     f"Sup {tp['support']} Res {tp['resistance']} ATR {tp['atr']:.1f}")
        else:
            caption=f"*{stock_code.upper()}* {safe_int(df['Close'].iloc[-1])}\n{bandar_line}"
        if QUOTA_HIT: caption+="\n⚠️ Quota habis - VSA mode"
        send_photo_reply(chat_id,file_path,caption=caption)
        if os.path.exists(file_path): os.remove(file_path)
    except Exception as e:
        import traceback; traceback.print_exc(); send_reply(chat_id,f"❌ {e}")

LAST_SIGNALS_CACHE={}
def telegram_bot_listener():
    global LAST_SIGNALS_CACHE,QUOTA_HIT,LAST_429_TIME
    offset=0; print("🤖 V4.3.1 FIX Running...")
    try: requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true",timeout=10)
    except: pass
    while True:
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res=requests.get(url,timeout=25)
            if res.status_code!=200: time.sleep(3); continue
            data=res.json()
            for update in data.get("result",[]):
                offset=update["update_id"]+1
                if "callback_query" in update:
                    cb=update["callback_query"]; cb_id=cb.get("id"); cb_data=cb.get("data",""); chat_id=cb["message"]["chat"]["id"]
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",json={"callback_query_id":cb_id})
                    if cb_data.startswith("chart_"):
                        parts=cb_data.split("_")
                        if len(parts)>=3: threading.Thread(target=process_chart_request,args=(chat_id,parts[1],parts[2],LAST_SIGNALS_CACHE)).start()
                elif "message" in update and "text" in update["message"]:
                    text=update["message"].get("text","").strip(); chat_id=update["message"]["chat"]["id"]; first=text.split()[0].lower() if text else ""
                    if first in ["/start","/help"]:
                        send_reply(chat_id,"🤖 *V4.3.1 FIX*\n`/c <KODE> [TF]` Chart\n`/b <KODE>` Bandar\n`/scan` All BUY\n`/quota` Cek\n`/clearcache` Hanya pakai kalau quota OK")
                    elif first in ["/c","/chart"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper(); raw_tf=parts[2] if len(parts)>=3 else "1d"
                            tf_map={"5":"5m","15":"15m","30":"30m","1h":"1h","4h":"4h","1":"1d","d":"1d","w":"1w","m":"1M","1d":"1d","1w":"1w","5m":"5m","15m":"15m","30m":"30m","1M":"1M"}
                            tf=tf_map.get(raw_tf.lower(),raw_tf.lower())
                            threading.Thread(target=process_chart_request,args=(chat_id,sym,tf,LAST_SIGNALS_CACHE)).start()
                    elif first in ["/b","/broker","/bandar"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            def broker_detail(tg,symbol):
                                try:
                                    multi=get_broker_multi_tf(symbol)
                                    marker=source_marker(multi.get('source_d','EMPTY'))
                                    msg=f"🏦 *{symbol} AKUM/DIST* {marker}\nDaily: {multi.get('status_d')} Net {format_large_number(multi.get('net_d',0),True)}\n AKUM {format_large_number(multi.get('akum_d',0),True)} DIST {format_large_number(multi.get('dist_d',0),True)}\nTop: {format_top_brokers(multi.get('brokers',[]),5)}\n"
                                    for b in multi.get('brokers',[])[:5]:
                                        nval=float(b.get('nval',0))
                                        msg+=f"  {b.get('broker_code')}: {format_large_number(nval,True)} {'🟢' if nval>0 else '🔴'}\n"
                                    msg+=f"W: {format_large_number(multi.get('net_5d',0),True)} M: {format_large_number(multi.get('net_20d',0),True)}\n"
                                    send_reply(tg,msg)
                                except Exception as e: send_reply(tg,f"❌ {e}")
                            threading.Thread(target=broker_detail,args=(chat_id,sym)).start()
                    elif first in ["/quota"]:
                        send_reply(chat_id,f"📊 QUOTA: {'HABIS' if QUOTA_HIT else 'OK'}\nCache: {len(BROKER_CACHE)}\nLast 429: {datetime.datetime.fromtimestamp(LAST_429_TIME).strftime('%H:%M:%S') if LAST_429_TIME else '-'}")
                    elif first in ["/clearcache","/cc","/clear"]:
                        try:
                            if QUOTA_HIT:
                                send_reply(chat_id,"⛔ Jangan clearcache pas quota HABIS! Cache 168B lu bakal hilang jadi W0 M0. Tunggu quota reset 00:00 WIB.")
                            else:
                                BROKER_CACHE.clear(); HISTORY_CACHE.clear(); SCREENER_CACHE.clear(); LAST_SIGNALS_CACHE.clear()
                                QUOTA_HIT=False; LAST_429_TIME=0
                                if os.path.exists("/tmp/rafano_cache.json"): os.remove("/tmp/rafano_cache.json")
                                send_reply(chat_id,"🧹 Cleared - quota OK, aman")
                        except Exception as e: send_reply(chat_id,f"❌ {e}")
                    elif first in ["/scan","!scan","/scanall","/scanfull"]:
                        send_reply(chat_id,"🔍 *SCAN 60 saham...*")
                        def manual_scan(tg=chat_id):
                            global LAST_SIGNALS_CACHE
                            sigs=scan_v3_full(); LAST_SIGNALS_CACHE={s['symbol']:s for s in sigs}; broadcast_v3(sigs)
                        threading.Thread(target=manual_scan,args=(chat_id,)).start()
                    elif first in ["/scanfast"]:
                        send_reply(chat_id,"⚡ *FAST 20...*")
                        def fast_scan(tg=chat_id):
                            global LAST_SIGNALS_CACHE
                            old_screener=get_screener_latest()
                            if old_screener: cands=[x.get('symbol') for x in old_screener[:20]]
                            else: cands=["BBCA","BBRI","BMRI","BBNI","TLKM","ASII","ADRO","ANTM","MDKA","BRIS","GOTO","AMMN","BRMS","BREN","CUAN","WIFI","BIPI","DEWA","BULL","NIKL"]
                            sigs=[]
                            for sym in cands:
                                if QUOTA_HIT: break
                                try:
                                    h=get_history_pro(sym,120,"1d"); m=get_broker_multi_tf(sym,h)
                                    if "AKUM" in m.get('status_d','') or (h is not None and h['Close'].iloc[-1]>h['Close'].ewm(span=50).mean().iloc[-1]):
                                        sigs.append({"symbol":sym,"close":int(h['Close'].iloc[-1]) if h is not None else 0,"change_pct":0,"score":60,"score_label":"BUY","akum_value":m.get('akum_d',0),"dist_value":m.get('dist_d',0),"broker_net":m.get('net_d',0),"broker_status":m.get('status_d'),"reasons":["AKUM REAL" if m.get('source_d','').startswith('API') else "VSA"],"history_df":h,"trading_plan":None,"brokers":m.get('brokers',[]),"multi_tf":m})
                                except: pass
                                time.sleep(0.5)
                            LAST_SIGNALS_CACHE={s['symbol']:s for s in sigs}
                            broadcast_v3(sigs)
                        threading.Thread(target=fast_scan,args=(chat_id,)).start()
        except Exception as e:
            print(f"Listener err {e}"); time.sleep(3)

def auto_screener_loop():
    global LAST_SIGNALS_CACHE
    print("🚀 Auto Scan...")
    while True:
        try:
            if not is_market_open(): time.sleep(300); continue
            if QUOTA_HIT: time.sleep(1800); continue
            sigs=scan_v3_full(); LAST_SIGNALS_CACHE={s['symbol']:s for s in sigs}
            filt=filter_signals_with_cooldown(sigs)
            if filt: broadcast_v3(filt)
            time.sleep(1800)
        except Exception as e: print(f"Auto err {e}"); time.sleep(60)

if __name__=="__main__":
    print("==========================================")
    print("🔥 RAFANO V4.3.1 FIX WAIT 0%")
    print("==========================================")
    threading.Thread(target=auto_screener_loop,daemon=True).start()
    telegram_bot_listener()
