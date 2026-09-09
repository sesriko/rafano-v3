"""
RAFANO V4.3.3 + SCAN VOLUME SPIKE >2x
- Fix NO SIGNAL = NO ENTRY (dari V4.3.2)
- NEW: /scanvol /vol /vspike /volspike -> scan manual volume spike >2x
- Contoh: /scanvol 2  -> spike >2x, /scanvol 3 -> >3x, /volspike -> default 2x
- Broadcast pakai format ringkas + vol ratio + bandar
"""
import os, time, datetime, threading, requests, pytz, numpy as np, pandas as pd, matplotlib.patches as patches, matplotlib.gridspec as gridspec
from dotenv import load_dotenv
load_dotenv()

def safe_get_env(k):
    v=os.getenv(k)
    if v: return str(v).strip().strip('"').strip("'")
    try:
        from google.colab import userdata
        vv=userdata.get(k)
        if vv:
            vv=str(vv).strip().strip('"').strip("'")
            os.environ[k]=vv
            return vv
    except: pass
    return None

TIMEZONE_WIB=pytz.timezone('Asia/Jakarta')
TELEGRAM_BOT_TOKEN=safe_get_env("TELEGRAM_BOT_TOKEN")
TARGET_CHAT_ID=safe_get_env("TARGET_CHAT_ID")
ARJUM_API_KEY=safe_get_env("ARJUM_API_KEY")
ARJUM_BASE="https://stock.arjum.com/api"
def get_now_wib(): return datetime.datetime.now(TIMEZONE_WIB)
def safe_int(v,d=0):
    try:
        if pd.isna(v) or np.isinf(v): return d
        return int(v)
    except: return d
def format_large_number(val,show_sign=False):
    if pd.isna(val) or val==0: return "0"
    a=abs(val); s="+" if (show_sign and val>0) else ("-" if val<0 else "")
    if a>=1_000_000_000: return f"{s}{a/1_000_000_000:.2f}B"
    elif a>=1_000_000: return f"{s}{a/1_000_000:,.0f}M"
    elif a>=1_000: return f"{s}{a/1_000:,.0f}K"
    else: return f"{s}{val:,.0f}"
def round_to_ihsg_fraction(p):
    if pd.isna(p) or p<=0: return 0
    p=float(p); tick=1 if p<200 else 2 if p<500 else 5 if p<2000 else 10 if p<5000 else 25
    return int(round(p/tick)*tick)
def format_timeframe_label(tf):
    m={"1m":"1 Menit","5m":"5 Menit","15m":"15 Menit","30m":"30 Menit","1h":"1 Jam","4h":"4 Jam","1d":"Daily","1w":"Weekly","1mo":"Monthly","1M":"Monthly"}
    return m.get((tf or "1d").lower().strip(),(tf or "1d").upper())
def is_intraday_tf(tf): return (tf or "1d").lower() in ["1m","5m","15m","30m","1h","4h","1min","5min","15min","30min","1hour","4hour"]
def source_marker(s):
    if s in ["API_SUMMARY","API_SUMMARY_RANGE"]: return "✅ REAL"
    if s=="VSA_ESTIMATE": return "⚠️ VSA"
    if s=="QUOTA": return "♻️ Cache"
    if s=="EMPTY": return "❓"
    return s
def grade_from_strength(st,side):
    st=st or 0
    if side=="BUY":
        if st>=85: return "STRONG BUY","#00ff00"
        elif st>=70: return "BUY","#7CFC00"
        elif st>=55: return "WATCH","#ffd700"
        else: return "WAIT","#888888"
    elif side=="SELL":
        if st>=85: return "STRONG SELL","#ff0000"
        elif st>=70: return "SELL","#ff6666"
        else: return "WATCH SELL","#ffaa00"
    else: return "WAIT","#888888"
def calculate_atr(df,p=14):
    tr1=df['High']-df['Low']; tr2=(df['High']-df['Close'].shift(1)).abs(); tr3=(df['Low']-df['Close'].shift(1)).abs()
    tr=pd.concat([tr1,tr2,tr3],axis=1).max(axis=1); return tr.rolling(window=p,min_periods=1).mean()
def calculate_vsa_metrics(df):
    df=df.copy()
    pr=(df['High']-df['Low']).replace(0,0.1); cp=(df['Close']-df['Low'])/pr; cp=np.clip(cp,0.05,0.95)
    br=0.30+cp*0.60
    if 'V1' in df.columns:
        vr=df['Volume']/df['V1'].replace(0,1); ig=df['Close']>=df['Open']
        boost=np.where((vr>1.5)&ig,0.10,0)+np.where((vr>2.5)&ig,0.10,0); br=br+boost
        br=np.where(vr<0.3,0.30+cp*0.60,br)
    br=np.clip(br,0.10,0.95)
    df['Vol_Buy']=df['Volume']*br; df['Vol_Sell']=df['Volume']-df['Vol_Buy']
    df['Net_Vol_VSA']=df['Vol_Buy']-df['Vol_Sell']; df['Net_Val_VSA']=df['Net_Vol_VSA']*df['Close']; df['Buy_Pct']=br*100
    return df,br
def calculate_bollinger_bands(df,p=20,s=2):
    sma=df['Close'].rolling(p).mean(); std=df['Close'].rolling(p).std()
    return sma,sma+(std*s),sma-(std*s)

def detect_buy_signals(df,multi_tf=None):
    sigs=[]
    if df is None or len(df)<30: return sigs,df
    try:
        df=df.copy(); df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean(); df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
        df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean(); df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        df['ATR']=calculate_atr(df,14); _,bu,bl=calculate_bollinger_bands(df,20,2); df['BB_UPPER']=bu; df['BB_LOWER']=bl; df,_=calculate_vsa_metrics(df)
        global QUOTA_HIT
        if QUOTA_HIT or (multi_tf and multi_tf.get('source_d') in ['QUOTA','EMPTY']):
            net_5d=df['Net_Val_VSA'].tail(5).sum() or 1
        else:
            net_5d=multi_tf.get('net_d',0) if multi_tf else df['Net_Val_VSA'].tail(5).sum()
        for i in range(20,len(df)):
            c=df['Close'].iloc[i]; o=df['Open'].iloc[i]; l=df['Low'].iloc[i]; vol=df['Volume'].iloc[i]; v1=df['V1'].iloc[i]
            e50=df['EMA50'].iloc[i]; e20=df['EMA20'].iloc[i]; bbl=df['BB_LOWER'].iloc[i] if not pd.isna(df['BB_LOWER'].iloc[i]) else 0
            atr=df['ATR'].iloc[i] if not pd.isna(df['ATR'].iloc[i]) else c*0.03
            pc=df['Close'].iloc[i-1]; pe50=df['EMA50'].iloc[i-1]
            ok=(net_5d>0) if not QUOTA_HIT else True
            if pc<=pe50 and c>e50 and c>e20 and (vol>v1*1.5 if v1>0 else False) and c>=o and ok:
                sigs.append({'index':i,'date':df.index[i],'type':'BO EMA50','side':'BUY','entry':float(c),'sl':float(min(df['Low'].iloc[max(0,i-5):i+1].min(),c-atr*1.2)),'reason':f'BO EMA50 Vol {vol/v1:.1f}x','strength':90}); continue
            if bbl>0:
                dist=(c-bbl)/bbl*100 if bbl else 0
                is_bow=c<bbl and dist<-1.5; body=abs(c-o); lw=min(o,c)-l; is_rev=c>=o and lw>body*1.5 if body>0 else False
                if is_bow and is_rev:
                    sigs.append({'index':i,'date':df.index[i],'type':'BOW BB','side':'BUY','entry':float(c),'sl':float(l*0.98),'reason':f'BOW {dist:.1f}% + Rev','strength':85}); continue
            de=abs(c-e50)/e50*100 if e50>0 else 100
            if de<2.0:
                wc=0
                for j in range(max(0,i-10),i+1):
                    lj=df['Low'].iloc[j]; ej=df['EMA50'].iloc[j]
                    if abs(lj-ej)/ej<0.015: wc+=1
                if wc>=2 and c>e50 and c>o:
                    sigs.append({'index':i,'date':df.index[i],'type':'BOS EMA','side':'BUY','entry':float(c),'sl':float(min(df['Low'].iloc[max(0,i-3):i+1].min(),e50*0.97)),'reason':f'BOS Near {de:.1f}% {wc}x','strength':80})
        fl=[]; li=-20
        for s in sorted(sigs,key=lambda x:x['index']):
            if s['index']-li>=5: fl.append(s); li=s['index']
        return fl,df
    except: return [],df

def detect_sell_signals(df,multi_tf=None):
    sigs=[]
    if df is None or len(df)<30: return sigs,df
    try:
        if 'EMA50' not in df.columns:
            df=df.copy(); df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean(); df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
            df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean(); df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
            df['ATR']=calculate_atr(df,14); _,bu,_=calculate_bollinger_bands(df,20,2); df['BB_UPPER']=bu; df,_=calculate_vsa_metrics(df)
        net_5d=multi_tf.get('net_d',0) if multi_tf else 0
        for i in range(20,len(df)):
            c=df['Close'].iloc[i]; o=df['Open'].iloc[i]; h=df['High'].iloc[i]; vol=df['Volume'].iloc[i]; v1=df['V1'].iloc[i]
            e50=df['EMA50'].iloc[i]; atr=df['ATR'].iloc[i] if not pd.isna(df['ATR'].iloc[i]) else c*0.03
            pc=df['Close'].iloc[i-1]; is_bd=pc>=df['EMA50'].iloc[i-1] and c<e50; is_red=c<o; vs=vol>v1*1.5 if v1>0 else False
            if is_bd and vs and is_red and net_5d<0:
                sigs.append({'index':i,'date':df.index[i],'type':'BD EMA50','side':'SELL','entry':float(c),'sl':float(max(df['High'].iloc[max(0,i-5):i+1].max(),c+atr*1.2)),'reason':'BD EMA50 + Dist','strength':90})
        return sigs,df
    except: return [],df

def calculate_trading_plan(df,signals=None,multi_tf=None,timeframe="1d"):
    try:
        if df is None or len(df)<20: return None
        lc=df['Close'].iloc[-1]; atr=calculate_atr(df,14).iloc[-1]
        if pd.isna(atr) or atr==0: atr=lc*0.03
        e20=df['Close'].ewm(span=20).mean().iloc[-1]; e50=df['Close'].ewm(span=50).mean().iloc[-1]; e200=df['Close'].ewm(span=200).mean().iloc[-1]
        if signals is None:
            bs,_=detect_buy_signals(df,multi_tf); ss,_=detect_sell_signals(df,multi_tf); signals=bs+ss
        else:
            bs=[s for s in signals if s.get('side')=='BUY']; ss=[s for s in signals if s.get('side')=='SELL']
        mtf="NEUTRAL"
        if multi_tf:
            s5=multi_tf.get('status_5d','NEUTRAL'); s20=multi_tf.get('status_20d','NEUTRAL')
            if "AKUM" in s5 and "AKUM" in s20: mtf="STRONG BULLISH MTF"
            elif "AKUM" in s5 or "AKUM" in s20: mtf="BULLISH MTF"
            elif "DIST" in s5 and "DIST" in s20: mtf="BEARISH MTF"
        rb=[s for s in bs if s['index']>=len(df)-10]; rs=[s for s in ss if s['index']>=len(df)-10]
        if rb and (not rs or rb[-1]['index']>=rs[-1]['index']):
            ls=rb[-1]; entry=ls['entry']; sl=ls['sl']; side="BUY"; stype=ls['type']; sreason=ls['reason']; sstr=ls['strength']; sdate=ls['date']
        elif rs:
            ls=rs[-1]; entry=ls['entry']; sl=ls['sl']; side="SELL"; stype=ls['type']; sreason=ls['reason']; sstr=ls['strength']; sdate=ls['date']
        else:
            entry=round_to_ihsg_fraction(lc); sl=round_to_ihsg_fraction(max(df['Low'].tail(5).min(),lc-atr*1.5))
            stype="NO SIGNAL"; sreason="Tunggu BO/BOS/BOW"; sstr=0; sdate=df.index[-1]; side="WAIT"
        if side=="BUY" and mtf=="STRONG BULLISH MTF": sstr=min(100,sstr+10)
        min_sl=lc*0.92; max_sl=lc*0.98; sl=max(min(sl,max_sl),min_sl); sl=round_to_ihsg_fraction(sl)
        if entry<=sl and side!="SELL": entry=round_to_ihsg_fraction(sl*1.03)
        if side=="BUY":
            tp1=round_to_ihsg_fraction(entry+atr*1.5); tp2=round_to_ihsg_fraction(entry+atr*3.0)
            risk=entry-sl; r1=tp1-entry; r2=tp2-entry
        else:
            tp1=round_to_ihsg_fraction(entry*1.035); tp2=round_to_ihsg_fraction(entry+atr*1.8)
            risk=entry-sl; r1=tp1-entry; r2=tp2-entry
        if tp1==tp2: tp2=round_to_ihsg_fraction(entry+atr*3.0)
        rr1=r1/risk if risk>0 else 0; rr2=r2/risk if risk>0 else 0
        if lc>e20 and lc>e50 and lc>e200: trend="STRONG UPTREND"
        elif lc>e20 and lc>e50: trend="UPTREND"
        elif lc>e20: trend="WEAK UPTREND"
        else: trend="DOWNTREND"
        return {"entry":int(entry),"sl":int(sl),"tp1":int(tp1),"tp2":int(tp2),"atr":float(atr),"risk_pct":round((risk/entry)*100,2) if entry else 0,"rr1":round(rr1,2),"rr2":round(rr2,2),"trend":f"{trend} + {mtf}" if mtf!="NEUTRAL" else trend,"support":int(df['Low'].tail(10).min()),"resistance":int(df['High'].tail(10).max()),"signal_type":stype,"signal_reason":sreason,"signal_strength":sstr,"signal_date":sdate,"all_signals":signals,"buy_signals":bs,"sell_signals":ss,"side":side,"mtf_confirm":mtf}
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
            ld=json.load(cf); BROKER_CACHE={k:(v[0],v[1]) for k,v in ld.get('broker',{}).items()}
except: pass
def save_cache_to_file():
    try:
        with open(CACHE_FILE,'w') as cf: json.dump({'broker':{k:[v[0],v[1]] for k,v in BROKER_CACHE.items()}},cf)
    except: pass
def get_cached_broker(k,allow_expired=False):
    import time
    if k in BROKER_CACHE:
        ts,d=BROKER_CACHE[k]
        if time.time()-ts<BROKER_CACHE_TTL: return d
        elif allow_expired or QUOTA_HIT: return d
        else: del BROKER_CACHE[k]
    return None
def set_cached_broker(k,d):
    import time
    if d and isinstance(d,dict):
        if d.get('akum_d',0)==0 and d.get('dist_d',0)==0 and len(d.get('brokers',[]))==0 and d.get('net_d',0)==0:
            if d.get('source_d') not in ["QUOTA"]: return
    BROKER_CACHE[k]=(time.time(),d); save_cache_to_file()
def get_cached_history(k):
    import time
    if k in HISTORY_CACHE:
        ts,d=HISTORY_CACHE[k]
        if time.time()-ts<HISTORY_CACHE_TTL: return d
    return None
def set_cached_history(k,d):
    import time; HISTORY_CACHE[k]=(time.time(),d)
def get_cached_screener():
    import time
    if 'latest' in SCREENER_CACHE:
        ts,d=SCREENER_CACHE['latest']
        if time.time()-ts<SCREENER_CACHE_TTL: return d
    return None
def set_cached_screener(d):
    import time; SCREENER_CACHE['latest']=(time.time(),d)
def make_cache_key(p,ps):
    if not ps: return p
    try: return f"{p}?{'&'.join([f'{k}={v}' for k,v in sorted(ps.items())])}"
    except: return p
def arjum_get(path,params=None,use_cache=True):
    global QUOTA_HIT,LAST_429_TIME
    import time as _time
    ck=make_cache_key(path,params) if use_cache else None
    if use_cache and ck and 'broker' in path:
        c=get_cached_broker(ck)
        if c: return c
    if use_cache and 'screener' in path:
        c=get_cached_screener()
        if c: return c
    if QUOTA_HIT and _time.time()-LAST_429_TIME<300:
        if ck:
            exp=get_cached_broker(ck,allow_expired=True)
            if exp: return exp
        return None
    url=f"{ARJUM_BASE}{path}"
    try:
        api_key=os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or safe_get_env("ARJUM_API_KEY") or ""
        headers={"X-API-Key": api_key.strip(),"Accept":"application/json","User-Agent":"Mozilla/5.0"}
        r=requests.get(url,headers=headers,params=params,timeout=15)
        if r.status_code==200:
            j=r.json()
            if use_cache and ck:
                if 'broker' in path: set_cached_broker(ck,j)
                elif 'screener' in path: set_cached_screener(j)
            QUOTA_HIT=False; return j
        elif r.status_code==429:
            QUOTA_HIT=True; LAST_429_TIME=_time.time()
            if ck:
                exp=get_cached_broker(ck,allow_expired=True)
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
def calc_akum_dist(lst):
    if not lst: return 0,0,0,"NEUTRAL ⚪"
    a=0.0; d=0.0
    for b in lst:
        try:
            n=float(b.get('nval',0) or 0)
            if n>0: a+=n
            elif n<0: d+=abs(n)
        except: pass
    net=a-d
    if net>0: st="AKUM 🟢"
    elif net<0: st="DIST 🔴"
    else: st="NEUTRAL ⚪"
    return a,d,net,st
def get_broker_summary(sym,df_from=None,df_to=None):
    bp={"net":"false","broker_limit":20,"level_limit":25,"all_data":"false","flow":"all"}
    if df_from and df_to:
        bp["start_date"]=_fmt_yyyy_mm_dd(df_from); bp["end_date"]=_fmt_yyyy_mm_dd(df_to)
    data=arjum_get(f"/broker-summary/{sym}",params=bp,use_cache=True)
    if (not data or not (data.get('brokers') or data.get('data'))) and 'start_date' in bp:
        data=arjum_get(f"/broker-summary/{sym}",params={"net":"false","broker_limit":20,"level_limit":25,"all_data":"false","flow":"all"},use_cache=True)
    brokers=[]; a=0; d=0; net=0; st="NEUTRAL ⚪"; src="EMPTY"
    if data and isinstance(data,dict):
        raw=data.get('brokers') or data.get('data') or []
        for b in raw[:20]:
            if not isinstance(b,dict): continue
            code=b.get('broker_code') or '??'
            bval=float(b.get('bval') or 0); sval=float(b.get('sval') or 0); nval=float(b.get('nval') or (bval-sval))
            brokers.append({"broker_code":str(code).upper(),"bval":bval,"sval":sval,"nval":nval,"net_value":nval})
        if brokers:
            a,d,net,st=calc_akum_dist(brokers); src="API_SUMMARY" if 'start_date' not in bp else "API_SUMMARY_RANGE"
    if not brokers and QUOTA_HIT: src="QUOTA"
    return a,d,net,st,brokers,src
def calculate_bandars_avg(brokers,hist_df=None,period_days=None):
    try:
        if hist_df is not None and len(hist_df)>=1:
            sl=hist_df.tail(period_days) if period_days else hist_df.tail(1)
            if len(sl)>0 and sl['Volume'].sum()>0:
                return float((sl['Close']*sl['Volume']).sum()/sl['Volume'].sum())
            elif len(sl)>0: return float(sl['Close'].iloc[-1])
    except: pass
    return 0
def get_broker_multi_tf(sym,hist_df=None):
    ck=f"multi_{sym}"
    cached=get_cached_broker(ck)
    if cached and cached.get('source_d','').startswith('API_SUMMARY'):
        if cached.get('akum_d',0)!=0 or cached.get('dist_d',0)!=0 or len(cached.get('brokers',[]))>0:
            return cached
    exp=get_cached_broker(ck,allow_expired=True)
    if exp and QUOTA_HIT: return exp
    def _ago(n):
        try:
            if hist_df is not None and len(hist_df)>n: return hist_df.index[-(n+1)].date()
        except: pass
        return (get_now_wib()-datetime.timedelta(days=int(n*1.45)+3)).date()
    today=get_now_wib().date()
    if hist_df is not None and len(hist_df)>0:
        try: today=hist_df.index[-1].date()
        except: pass
    d5=_ago(5); d20=_ago(20)
    import time as _t
    ad,dd,nd,sd,bd,sr=get_broker_summary(sym)
    _t.sleep(0.4)
    a5,d5,n5,s5,b5,sr5=get_broker_summary(sym,date_from=d5,date_to=today)
    _t.sleep(0.4)
    a20,d20,n20,s20,b20,sr20=get_broker_summary(sym,date_from=d20,date_to=today)
    res={"akum_d":float(ad),"dist_d":float(dd),"net_d":float(nd),"akum_5d":float(a5),"dist_5d":float(d5),"net_5d":float(n5),"akum_20d":float(a20),"dist_20d":float(d20),"net_20d":float(n20),"avg_d":float(calculate_bandars_avg(bd,hist_df,1)),"source_d":sr,"source_5d":sr5,"source_20d":sr20,"brokers":bd,"brokers_5d":b5,"brokers_20d":b20,"status_d":sd,"status_5d":s5,"status_20d":s20}
    if not (ad==0 and dd==0 and len(bd)==0 and nd==0):
        set_cached_broker(ck,res)
    return res
def format_top_brokers(brokers,top=3):
    if not brokers: return "-"
    valid=[b for b in brokers if float(b.get('nval',0) or 0)!=0]
    if not valid: valid=[b for b in brokers if isinstance(b,dict)]
    if not valid: return "-"
    sb=sorted(valid,key=lambda x: abs(float(x.get('nval',0) or 0)),reverse=True)
    parts=[]
    for b in sb[:top]:
        code=b.get('broker_code') or "??"; nval=float(b.get('nval',0) or 0)
        if nval==0: continue
        s=f"{abs(nval)/1e9:.1f}B" if abs(nval)>=1e9 else f"{abs(nval)/1e6:.0f}M"
        parts.append(f"{code}{'+' if nval>0 else '-'}{s}")
    return ", ".join(parts) if parts else "-"
def get_analysis(sym):
    data=arjum_get(f"/analysis/{sym}",use_cache=False)
    return data if isinstance(data,dict) else {}
def get_history_pro(sym,limit=150,timeframe="1d"):
    hk=f"{sym}_{timeframe}_{limit}"
    cached=get_cached_history(hk)
    if cached is not None: return cached
    tf=timeframe.lower().strip()
    mp={"1m":"1min","5m":"5min","15m":"15min","30m":"30min","1h":"1hour","4h":"4hour","1d":"daily","1w":"weekly","1mo":"monthly"}
    af=mp.get(tf,"daily")
    data=arjum_get(f"/history/{sym}",params={"limit":limit,"frame":af},use_cache=True)
    rows=[]
    if data:
        if isinstance(data,dict): rows=data.get('data') or data.get('history') or []
        elif isinstance(data,list): rows=data
    if rows:
        try:
            df=pd.DataFrame(rows)
            rn={}
            for c in df.columns:
                cl=str(c).lower()
                if cl in ['o','open']: rn[c]='Open'
                elif cl in ['h','high']: rn[c]='High'
                elif cl in ['l','low']: rn[c]='Low'
                elif cl in ['c','close']: rn[c]='Close'
                elif cl in ['v','volume']: rn[c]='Volume'
                elif cl in ['date','time','t','datetime','timestamp']: rn[c]='Date'
            df.rename(columns=rn,inplace=True)
            if 'Date' in df.columns: df['Date']=pd.to_datetime(df['Date']); df.set_index('Date',inplace=True)
            df=df.sort_index()
            for col in ['Open','High','Low','Close','Volume']: df[col]=pd.to_numeric(df[col],errors='coerce')
            df=df.dropna(subset=['Close'])
            if len(df)>=10:
                set_cached_history(hk,df); return df
        except: pass
    try:
        import yfinance as yf
        ym={"1m":("7d","1m"),"5m":("5d","5m"),"15m":("5d","15m"),"30m":("1mo","30m"),"1h":("1mo","60m"),"4h":("3mo","90m"),"1d":("6mo","1d"),"1w":("1y","1wk"),"1mo":("2y","1mo")}
        per,inter=ym.get(tf,("6mo","1d"))
        hist=yf.Ticker(f"{sym}.JK").history(period=per,interval=inter,timeout=10)
        if (hist is None or len(hist)<10) and tf in ["1m","5m","15m","30m","1h","4h"]:
            hist=yf.Ticker(f"{sym}.JK").history(period="6mo",interval="1d",timeout=10)
        if hist is not None and len(hist)>10:
            set_cached_history(hk,hist.tail(limit)); return hist.tail(limit)
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
        if 'BB_UPPER' not in plot_df.columns: _,bu,bl=calculate_bollinger_bands(plot_df,20,2); plot_df['BB_UPPER']=bu; plot_df['BB_LOWER']=bl
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
        if sell_signals:
            for sig in sell_signals:
                idx=sig['index']
                if idx<len(df):
                    high=df['High'].iloc[idx]; atr=plot_df['ATR'].iloc[idx] if not pd.isna(plot_df['ATR'].iloc[idx]) else df['Close'].iloc[idx]*0.02
                    ax_main.annotate('▼',xy=(idx,high+atr*0.6),fontsize=14,color='#ff0000',fontweight='bold',ha='center',va='center')
        if len(df)>15:
            bl=len(df)-15; br=len(df)-1; yl=df['Low'].iloc[-15:].min()*0.99; yh=df['High'].iloc[-15:].max()*1.01
            ax_main.plot([bl,br],[yh,yh],color='white',linestyle='--',linewidth=0.6,alpha=0.6); ax_main.plot([bl,br],[yl,yl],color='white',linestyle='--',linewidth=0.6,alpha=0.6)
            ax_main.plot([bl,bl],[yl,yh],color='white',linestyle='--',linewidth=0.6,alpha=0.6); ax_main.plot([br,br],[yl,yh],color='white',linestyle='--',linewidth=0.6,alpha=0.6)
        rp=max(3,int(len(df)*0.04)); ax_main.set_xlim(-1,len(df)-1+rp); ax_main.set_ylim(df['Low'].min()*0.95,df['High'].max()*1.08)
        left_text=f"Avg Price : {avg_price:,.1f}\nVchg 1 Bar: {vchg1:.1f} x\nVchg 5 Bar: {vchg5:.1f} x\nSpeed : {speed}\nPower : {power}\nSafety : {safety}\n\nEMA 13 : {ema13:,.1f}\nEMA 20 : {ema20:,.1f}\nEMA 50 : {ema50:,.1f}\nEMA 200: {ema200:,.1f}"
        ax_main.text(0.01,0.98,left_text,transform=ax_main.transAxes,va='top',ha='left',fontsize=8,family='monospace',color='#e0e0e0',bbox=dict(facecolor='black',alpha=0.6,edgecolor='none'))
        fig.text(0.01,0.96,f"{symbol} :    {last_close:.0f} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=13,fontweight='bold',ha='left',va='center')
        fig.text(0.01,0.93,f"{sector_info}",color='#ffaa00',fontsize=8,ha='left')
        gl=extra_info.get('signal_grade'); gc=extra_info.get('signal_grade_color','#888888')
        if gl: fig.text(0.01,0.905,f"● {gl}",color=gc,fontsize=9,fontweight='bold',ha='left',va='center')
        fig.text(0.5,0.96,"RAFANO TRADER",color='white',fontsize=14,fontweight='bold',ha='center',va='center')
        if is_intraday_tf(timeframe) and hasattr(df.index[-1],'strftime'): ds=df.index[-1].strftime('%d %b %Y %H:%M')
        elif hasattr(df.index[-1],'strftime'): ds=df.index[-1].strftime('%d %b %Y')
        else: ds=get_now_wib().strftime('%d %b %Y')
        fig.text(0.99,0.96,f"{tf_label_disp} | {ds}",color='#ffcc00',fontsize=10,ha='right',va='center')
        fig.text(0.99,0.93,f"Command BOT /C {symbol}",color='white',fontsize=8,ha='right')
        fig.text(0.01,0.885,f"High:{last_high:.0f} Low:{last_low:.0f} Open:{last_open:.0f} Vol:{last_vol:,.0f}",color='#00ffff',fontsize=8,ha='left')
        ax_main.text(1.005,ema200,f" EMA 200 ",transform=ax_main.get_yaxis_transform(),color='black',backgroundcolor='#a020f0',fontsize=7,fontweight='bold',va='center')
        ax_main.text(1.005,last_close,f" {last_close:.0f} ",transform=ax_main.get_yaxis_transform(),color='black',backgroundcolor='white',fontsize=8,fontweight='bold',va='center')
        vol_info=f"Buy % = {buy_pct}% Sell % = {sell_pct}% Net Vol = {net_vol:,.0f} 5D = {net_vol_5d:,.0f}"
        ax_vol.text(0.005,0.88,vol_info,transform=ax_vol.transAxes,color='#ffffff',fontsize=8,va='top')
        ax_vol.bar(x,df['Vol_Sell'],color='#cc0000',width=0.8,alpha=0.8); ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00cc00',width=0.8,alpha=0.9); ax_vol.plot(x,df['V1'],color='white',linewidth=0.8,alpha=0.9)
        ax_vol.set_ylim(0,df['Volume'].max()*1.8); plt.setp(ax_vol.get_xticklabels(),visible=False)
        ax_nbsa.text(0.005,0.85,nbsa_label,transform=ax_nbsa.transAxes,color='#ffffff',fontsize=8,va='top')
        nbsa_vals=df['Net_Vol_VSA'].tail(80)/(df['Net_Vol_VSA'].abs().max() or 1)*50; xn=np.arange(len(df)-len(nbsa_vals),len(df))
        for i,v in zip(xn,nbsa_vals): ax_nbsa.bar(i,v,color='#00ffff' if v>=0 else '#ff4444',width=0.6)
        ax_nbsa.axhline(0,color='#444444',linewidth=0.5); ax_nbsa.set_ylim(-60,60)
        ax_mm.text(0.005,0.85,"Market Maker",transform=ax_mm.transAxes,color='#ffffff',fontsize=8,va='top')
        if 'MM' not in df.columns: df['MM']=(df['Close']-df['EMA50'])/df['EMA50']*1000
        mm_vals=df['MM'].tail(80); xm=np.arange(len(df)-len(mm_vals),len(df)); ax_mm.bar(xm,mm_vals,color='#cccccc',width=0.5,alpha=0.8)
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
        lc=df['Close'].iloc[-1]; lv=df['Volume'].iloc[-1]
        av20=df['Volume'].rolling(20).mean().iloc[-1]; vchg=lv/av20 if av20 else 1
        e13=df['Close'].ewm(span=13).mean().iloc[-1]; e20=df['Close'].ewm(span=20).mean().iloc[-1]
        e50=df['Close'].ewm(span=50).mean().iloc[-1]; e200=df['Close'].ewm(span=200).mean().iloc[-1]
        sma20=df['Close'].rolling(20).mean().iloc[-1]; std20=df['Close'].rolling(20).std().iloc[-1]
        bu=sma20+2*std20; bl=sma20-2*std20
        if lc>bu: bb="DI ATAS Upper 🔥"
        elif lc<bl: bb="DI BAWAH Lower"
        else: bb="DALAM BB"
        br=50
        if 'Buy_Pct' in df.columns: br=df['Buy_Pct'].iloc[-1]
        delta=df['Close'].diff(); gain=delta.where(delta>0,0).ewm(alpha=1/14,min_periods=14).mean(); loss=(-delta.where(delta<0,0)).ewm(alpha=1/14,min_periods=14).mean()
        rs=gain.iloc[-1]/(loss.iloc[-1]+0.00001); rsi=100-(100/(1+rs))
        if 'MM' not in df.columns: df['MM']=(df['Close']-df['Close'].ewm(span=50).mean())/df['Close'].ewm(span=50).mean()*1000
        mm=df['MM'].iloc[-1]
        if mm>0 and df['MM'].tail(3).mean()>df['MM'].tail(6).mean(): mmt="AKUM 🟢"
        elif mm<0: mmt="DIST 🔴"
        else: mmt="NETRAL"
        if lc>e13>e20>e50: ema="UPTREND 🟢"
        elif lc>e50: ema="WEAK UPTREND 🟡"
        elif lc<e20 and lc<e50: ema="DOWNTREND 🔴"
        else: ema="SIDEWAYS ⚪"
        speed="FAST" if vchg>2 else "SLOW" if vchg<0.8 else "NORMAL"
        power="TURBO" if br>=85 and vchg>=1.2 else "STRONG" if br>=70 else "WEAK"
        return {"ema50":e50,"ema200":e200,"bb_upper":bu,"bb_lower":bl,"bb_pos":bb,"buy_ratio":br,"rsi":rsi,"mm_val":mm,"mm_trend":mmt,"ema_trend":ema,"vchg":vchg,"speed":speed,"power":power}
    except: return {}

LAST_SENT_SIGNALS={}; COOLDOWN_SECONDS=3600; LAST_RESET_DATE=""
def filter_signals_with_cooldown(sigs):
    global LAST_RESET_DATE,LAST_SENT_SIGNALS
    today=get_now_wib().strftime('%d %b %Y %H:%M WIB')[:11]
    if LAST_RESET_DATE!=today: LAST_SENT_SIGNALS.clear(); LAST_RESET_DATE=today
    fl=[]
    for s in sigs:
        if time.time()-LAST_SENT_SIGNALS.get(s['symbol'],0)>=COOLDOWN_SECONDS:
            fl.append(s); LAST_SENT_SIGNALS[s['symbol']]=time.time()
    return fl

def calculate_score_v2(sym,hist,akum,dist,net,an):
    score=30; rs=["Screener"]
    anet=abs(net)
    if anet>20_000_000_000: score+=30; rs.append(f"{'AKUM' if net>0 else 'DIST'} {anet/1e9:.1f}B REAL")
    elif anet>5_000_000_000: score+=20; rs.append(f"{'AKUM' if net>0 else 'DIST'} {anet/1e9:.1f}B")
    elif anet>0: score+=10
    try:
        if an.get('trend')=='BULLISH': score+=20
        elif hist is not None and len(hist)>50 and hist['Close'].iloc[-1]>hist['Close'].ewm(span=50).mean().iloc[-1]: score+=15
    except: pass
    lab="VERY STRONG" if score>=85 else "STRONG BUY" if score>=70 else "WEAK BUY" if score>=50 else "WATCH" if score>=35 else "NO SIGNAL"
    return score,lab,rs

# ==================== NEW: SCAN VOLUME SPIKE >2x ====================
def scan_volume_spike(threshold=2.0, limit_candidates=60, akum_only=False):
    mode = "AKUM REAL ONLY" if akum_only else "ALL"
    print(f"[{get_now_wib()}] 🚀 SCAN VOLUME SPIKE >{threshold}x {mode} ...")
    screener_data=get_screener_latest()
    if not screener_data:
        candidates=["BBCA","BBRI","BMRI","BBNI","BRIS","TLKM","ASII","ADRO","ANTM","MDKA","BRMS","BREN","CUAN","WIFI","BIPI","BULL","NIKL","DEWA","PGEO","RAJA","MEDC","ELSA","PGAS","PTBA","ITMG","ANTM","BRPT","TPIA","GOTO","BUKA","EMTK","AMMN","MBMA","NCKL","TINS","HRUM","INCO","ESSA","AKRA","INDY","SMGR","INTP","UNTR","AUTO","ICBP","INDF","MYOR","KLBF","SIDO","CPIN","JPFA","UNVR","BRIS","BNGA"]
    else:
        candidates=[]
        for item in screener_data:
            sym=item.get('symbol') or item.get('code')
            if sym: candidates.append(sym.replace(".JK","").upper())
        candidates=list(dict.fromkeys([c.upper() for c in candidates]))[:limit_candidates]
    
    detected=[]
    def process_vol(sym):
        try:
            df=get_history_pro(sym, limit=60, timeframe="1d")
            if df is None or len(df)<25: return None
            v_avg=df['Volume'].rolling(20).mean().iloc[-2]  # avg sebelum hari ini
            v_last=df['Volume'].iloc[-1]
            if v_avg==0 or pd.isna(v_avg): return None
            ratio=v_last/v_avg
            if ratio < threshold: return None
            # tambahan filter: candle hijau atau minimal tidak merah dalam
            close=df['Close'].iloc[-1]; open_=df['Open'].iloc[-1]
            chg_pct=(close/df['Close'].iloc[-2]-1)*100 if len(df)>=2 else 0
            # hitung VSA Buy%
            df['V1']=df['Volume'].rolling(20).mean()
            df,_=calculate_vsa_metrics(df)
            buy_pct=df['Buy_Pct'].iloc[-1] if 'Buy_Pct' in df.columns else 50
            
            # broker info
            multi=None
            try: multi=get_broker_multi_tf(sym, df)
            except: multi=None
            
            # FILTER AKUM REAL ONLY - LOOSE + VSA FALLBACK
            if akum_only:
                # Kalau ada data broker REAL AKUM -> lolos langsung
                if multi:
                    status = multi.get('status_d','')
                    net_d = multi.get('net_d',0)
                    src = multi.get('source_d','')
                    # REAL AKUM atau CACHE dengan Net >0
                    if "AKUM" in status and net_d > 0:
                        # Buy% minimal 55 biar gak terlalu ketat
                        if buy_pct >= 55:
                            pass  # lolos
                        else:
                            return None
                    else:
                        # Tidak AKUM di broker, cek VSA fallback kalau quota habis
                        # Kalau quota habis, pakai VSA sebagai proxy AKUM
                        # Syarat VSA AKUM: Buy% >=70, candle hijau, chg >1%
                        if not (buy_pct >= 70 and close >= open_ and chg_pct > 1.0):
                            return None
                        # kasih tag VSA AKUM
                        multi = multi or {}
                        multi['status_d'] = 'AKUM 🟢 (VSA)'
                        multi['source_d'] = 'VSA_ESTIMATE'
                else:
                    # Tidak ada data broker sama sekali (quota habis / cache kosong)
                    # Fallback VSA: Buy% >=75, hijau, naik >1.5%
                    if not (buy_pct >= 75 and close >= open_ and chg_pct > 1.5):
                        return None
                    # buat dummy multi biar tampil
                    multi = {
                        'status_d': 'AKUM 🟢 (VSA)',
                        'source_d': 'VSA_ESTIMATE',
                        'net_d': v_last*close,
                        'akum_d': v_last*close,
                        'dist_d': 0,
                        'net_5d': 0,
                        'net_20d': 0,
                        'brokers': [],
                        'brokers_5d': [],
                        'brokers_20d': []
                    }
            
            # scoring untuk vol spike
            score=50
            if ratio>=5: score+=30
            elif ratio>=3: score+=20
            elif ratio>=2: score+=10
            if buy_pct>=70: score+=10
            if chg_pct>3: score+=10
            
            label="VOL SPIKE"
            if ratio>=5: label="TURBO SPIKE 🔥🔥"
            elif ratio>=3: label="STRONG SPIKE 🔥"
            
            return {
                "symbol": sym,
                "close": int(close),
                "change_pct": chg_pct,
                "vol_ratio": ratio,
                "vol_last": v_last,
                "vol_avg": v_avg,
                "buy_pct": buy_pct,
                "score": score,
                "score_label": label,
                "broker_net": multi.get('net_d',0) if multi else 0,
                "broker_status": multi.get('status_d','') if multi else '',
                "multi_tf": multi,
                "history_df": df,
                "brokers": multi.get('brokers',[]) if multi else []
            }
        except Exception as e:
            print(f"vol {sym} err {e}")
            return None
    
    for sym in candidates:
        r=process_vol(sym)
        if r:
            detected.append(r)
            print(f"✅ {r['symbol']} Vol {r['vol_ratio']:.1f}x Buy {r['buy_pct']:.0f}% {r['change_pct']:+.1f}%")
        time.sleep(0.3)
    
    # dedup by symbol keep highest ratio
    seen={}
    for d in detected:
        if d['symbol'] not in seen or d['vol_ratio']>seen[d['symbol']]['vol_ratio']:
            seen[d['symbol']]=d
    detected=list(seen.values())
    detected.sort(key=lambda x: x['vol_ratio'], reverse=True)
    return detected

def broadcast_vol_spike(signals, threshold=2.0, akum_only=False):
    if not signals:
        msg = f"Vol Spike + AKUM REAL >{threshold}x: Tidak ada yang valid hari ini (filter ketat)." if akum_only else f"Vol Spike >{threshold}x: Tidak ada yang spike hari ini."
        send_reply(TARGET_CHAT_ID, msg)
        return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    tag = f" + AKUM (REAL+VSA)" if akum_only else ""
    header=f"*VOL SPIKE{tag} >{threshold}x* 🔥\n{now} | {len(signals)} saham\n{'='*30}\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        multi=it.get('multi_tf') or {}
        net=format_large_number(multi.get('net_d',0) or it.get('broker_net',0), True) if multi else format_large_number(it.get('broker_net',0),True)
        status=multi.get('status_d','') if multi else it.get('broker_status','')
        top=format_top_brokers(it.get('brokers',[]),2)
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Vol {it['vol_ratio']:.1f}x Buy {it['buy_pct']:.0f}% \n   {status} Net {net}"
        if top!="-": line+=f" | {top}"
        line+="\n\n"
        kb.append([{"text": f"{it['symbol']} {it['vol_ratio']:.1f}x", "callback_data": f"chart_{it['symbol']}_1d"}])
        if len(msg)+len(line)>3500:
            send_reply(TARGET_CHAT_ID, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
        else:
            msg+=line
    if msg:
        send_reply(TARGET_CHAT_ID, msg, rm={"inline_keyboard": kb})

def scan_v3_full():
    print(f"[{get_now_wib()}] 🚀 SCAN RINGKAS...")
    sd=get_screener_latest()
    if not sd:
        cands=["BBCA","BBRI","BMRI","BBNI","BRIS","TLKM","ASII","ADRO","ANTM","MDKA","BRMS","BREN","CUAN","WIFI","BIPI","BULL","NIKL"]
    else:
        cands=[]
        for it in sd:
            sym=it.get('symbol') or it.get('code')
            if sym: cands.append(sym.replace(".JK","").upper())
        cands=list(dict.fromkeys(cands))
    det=[]
    def proc(sym):
        if QUOTA_HIT: return None
        try:
            hd=get_history_pro(sym,limit=120,timeframe="1d")
            if hd is None or len(hd)<20: return None
            multi=get_broker_multi_tf(sym,hd)
            akum=multi.get('akum_d',0); dist=multi.get('dist_d',0); net=multi.get('net_d',0); st=multi.get('status_d','NEUTRAL ⚪')
            lc=hd['Close'].iloc[-1]; e50=hd['Close'].ewm(span=50).mean().iloc[-1] if len(hd)>=50 else lc
            is_buy=("AKUM" in st and net>0) or (lc>e50) or (akum>0) or (net>0)
            if not is_buy and lc>e50: is_buy=True
            if not is_buy: return None
            an=get_analysis(sym)
            sc,lab,rs=calculate_score_v2(sym,hd,akum,dist,net,an)
            if sc>=35:
                prev=hd['Close'].iloc[-2] if len(hd)>=2 else lc; chg=(lc/prev-1)*100 if prev else 0
                tp=calculate_trading_plan(hd,multi_tf=multi,timeframe="1d")
                return {"symbol":sym,"close":int(lc),"change_pct":chg,"score":sc,"score_label":lab,"akum_value":akum,"dist_value":dist,"broker_net":net,"broker_status":st,"reasons":rs,"history_df":hd,"trading_plan":tp,"brokers":multi.get('brokers',[]),"multi_tf":multi}
        except: return None
    for idx,sym in enumerate(cands):
        if QUOTA_HIT: print(f"⛔ Quota habis di {idx}/{len(cands)}"); break
        r=proc(sym)
        if r: det.append(r); print(f"✅ {r['symbol']} {r['score']}%")
        time.sleep(0.7)
    det.sort(key=lambda x: (x['multi_tf'].get('net_d',0),x['score']),reverse=True)
    return det

def scan_v3(): return scan_v3_full()
def send_reply(cid,txt,rm=None):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    pl={"chat_id":cid,"text":txt,"parse_mode":"Markdown"}
    if rm: pl["reply_markup"]=rm
    try: requests.post(url,json=pl,timeout=15)
    except: pass
def send_photo_reply(cid,pp,cap=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(pp,'rb') as ph:
            requests.post(url,data={'chat_id':cid,'caption':cap,'parse_mode':'Markdown'},files={'photo':ph},timeout=30)
    except Exception as e: print(f"Photo err {e}")

def broadcast_v3(signals):
    if not signals:
        msg="Scan: Tidak ada BUY / Quota habis."+("\n♻️ Cache" if QUOTA_HIT else "")
        send_reply(TARGET_CHAT_ID,msg); return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header=f"*RAFANO V4.3.2 FIX*\n{now} | {len(signals)} BUY\n{'='*30}\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        multi=it.get('multi_tf') or {}; nd=multi.get('net_d',0); sd=multi.get('status_d','NEUTRAL'); src=source_marker(multi.get('source_d','EMPTY'))
        top=format_top_brokers(multi.get('brokers',[]),2)
        bs=f"{sd} Net {format_large_number(nd,True)} | {src}"
        if top!="-": bs+=f" | {top}"
        item=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) {it['score']}% \n   {bs}\n\n"
        kb.append([{"text":f"{it['symbol']}", "callback_data":f"chart_{it['symbol']}_1d"}])
        if len(msg)+len(item)>3500:
            send_reply(TARGET_CHAT_ID,msg,rm={"inline_keyboard":kb}); msg=item; kb=[]
        else: msg+=item
    if msg: send_reply(TARGET_CHAT_ID,msg,rm={"inline_keyboard":kb})

def process_chart_request(cid,code,tf="1d",cache=None):
    is_intra=is_intraday_tf(tf)
    tfl=format_timeframe_label(tf)
    send_reply(cid,f"📊 *{code.upper()} ({tfl})...*")
    df=get_history_pro(code,limit=150,timeframe=tf)
    if df is None or len(df)<20: send_reply(cid,f"⚠️ Data {code} tidak ada"); return
    multi=get_broker_multi_tf(code,df) if is_intra else (cache[code].get('multi_tf') if cache and code in cache else get_broker_multi_tf(code,df))
    if not multi: multi=get_broker_multi_tf(code,df)
    ad=multi.get('akum_d',0) if multi else 0; dd=multi.get('dist_d',0) if multi else 0; nd=multi.get('net_d',0) if multi else 0
    src=multi.get('source_d','EMPTY') if multi else 'EMPTY'; is_real=src.startswith('API_SUMMARY')
    extra={"akum_value":ad,"dist_value":dd,"broker_net":nd,"brokers":multi.get('brokers',[]) if multi else [],"multi_tf":multi,"is_real":is_real,"tf_label":tfl}
    tp=calculate_trading_plan(df,signals=None,multi_tf=multi,timeframe=tf)
    side=tp.get('side','WAIT') if tp else 'WAIT'; ss=tp.get('signal_strength',0) if tp else 0
    gl,gc=grade_from_strength(ss,side)
    extra['signal_grade']=gl; extra['signal_grade_color']=gc
    tech=build_tech_caption(df,multi,tp,timeframe=tf)
    chart_file=f"chart_{code.upper()}_{tf}_{int(time.time())}.png"
    try:
        fp=generate_pro_chart(df=df,symbol=code.upper(),timeframe=tf,sector_info=f"{code.upper()} | IHSG",output_filename=chart_file,extra_info=extra)
        if not fp or not os.path.exists(fp): send_reply(cid,"❌ Gagal render"); return
        if multi:
            mk=source_marker(multi.get('source_d','EMPTY'))
            if ad==0 and dd==0 and not is_real:
                bl=f"{multi.get('status_d')} | {mk} | VSA Buy {tech.get('buy_ratio',0):.0f}% (quota)"
            else:
                if ad>0 and dd>0: bl=f"{multi.get('status_d')} AKUM {format_large_number(ad,True)} DIST {format_large_number(dd,True)} Net {format_large_number(nd,True)} | {mk}"
                elif ad>0: bl=f"{multi.get('status_d')} AKUM {format_large_number(ad,True)} Net {format_large_number(nd,True)} | {mk}"
                else: bl=f"{multi.get('status_d')} DIST {format_large_number(dd,True)} Net {format_large_number(nd,True)} | {mk}"
            top=format_top_brokers(multi.get('brokers',[]),3)
            if top!="-": bl+=f"\nTop: {top}"
            wl=f"W {format_large_number(multi.get('net_5d',0),True)} M {format_large_number(multi.get('net_20d',0),True)}"
        else:
            bl="No broker"; wl=""
        
        is_no_signal = (tp is None) or (tp.get('signal_type')=='NO SIGNAL') or (tp.get('side')=='WAIT') or (tp.get('signal_strength',0)==0)
        
        if tp:
            if is_no_signal:
                caption=(
                    f"*{code.upper()}* -- {safe_int(df['Close'].iloc[-1])} | {tp['trend']}\n"
                    f"🟡 *{gl}* {ss}% | {tp.get('signal_type','NO SIGNAL')} | TF: {tfl}\n"
                    f"------------------\n"
                    f"📦 {bl}\n"
                    f"{wl}\n"
                    f"------------------\n"
                    f"📊 {tech.get('ema_trend','')} | RSI {tech.get('rsi',0):.1f} | {tech.get('bb_pos','')}\n"
                    f"Vol {format_large_number(df['Volume'].iloc[-1],False)} {tech.get('vchg',0):.1f}x {tech.get('speed','')} {tech.get('power','')} | MM {tech.get('mm_val',0):.0f} {tech.get('mm_trend','')}\n"
                    f"------------------\n"
                    f"🎯 {tp.get('signal_reason','Tunggu BO/BOS/BOW')}\n"
                    f"Sup {tp['support']} Res {tp['resistance']} ATR {tp['atr']:.1f} | No Entry - Tunggu sinyal"
                )
            else:
                caption=(
                    f"*{code.upper()}* -- {safe_int(df['Close'].iloc[-1])} | {tp['trend']}\n"
                    f"🟢 *{gl}* {ss}% | {tp.get('signal_type','')} | TF: {tfl}\n"
                    f"------------------\n"
                    f"📦 {bl}\n"
                    f"{wl}\n"
                    f"------------------\n"
                    f"📊 {tech.get('ema_trend','')} | RSI {tech.get('rsi',0):.1f} | {tech.get('bb_pos','')}\n"
                    f"Vol {format_large_number(df['Volume'].iloc[-1],False)} {tech.get('vchg',0):.1f}x {tech.get('speed','')} {tech.get('power','')} | MM {tech.get('mm_val',0):.0f} {tech.get('mm_trend','')}\n"
                    f"------------------\n"
                    f"🎯 {tp.get('signal_reason','')}\n"
                    f"Entry {tp['entry']} SL {tp['sl']} ({tp['risk_pct']}%) | TP1 {tp['tp1']} TP2 {tp['tp2']} RR {tp['rr1']}/{tp['rr2']}\n"
                    f"Sup {tp['support']} Res {tp['resistance']} ATR {tp['atr']:.1f}"
                )
        else:
            caption=f"*{code.upper()}* {safe_int(df['Close'].iloc[-1])}\n{bl}"
        
        if QUOTA_HIT: caption+="\n⚠️ Quota habis - VSA mode"
        send_photo_reply(cid,fp,caption=caption)
        if os.path.exists(fp): os.remove(fp)
    except Exception as e:
        import traceback; traceback.print_exc(); send_reply(cid,f"❌ {e}")

LAST_SIGNALS_CACHE={}
def telegram_bot_listener():
    global LAST_SIGNALS_CACHE,QUOTA_HIT,LAST_429_TIME
    offset=0; print("🤖 V4.3.4 VOL SPIKE + AKUM REAL Running...")
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
                    cb=update["callback_query"]; cid=cb.get("id"); cd=cb.get("data",""); chat_id=cb["message"]["chat"]["id"]
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",json={"callback_query_id":cid})
                    if cd.startswith("chart_"):
                        parts=cd.split("_")
                        if len(parts)>=3: threading.Thread(target=process_chart_request,args=(chat_id,parts[1],parts[2],LAST_SIGNALS_CACHE)).start()
                elif "message" in update and "text" in update["message"]:
                    txt=update["message"].get("text","").strip(); chat_id=update["message"]["chat"]["id"]; first=txt.split()[0].lower() if txt else ""
                    if first in ["/start","/help"]:
                        send_reply(chat_id,"🤖 *V4.3.4 VOL SPIKE + AKUM*\n`/c <KODE> [TF]` Chart\n`/b <KODE>` Bandar\n`/scan` All BUY\n`/scanvol [x]` Vol >x (semua)\n`/volakum [x]` Vol >x + AKUM REAL ONLY 🔥 filter fake\n`/quota` Cek")
                    elif first in ["/c","/chart"]:
                        parts=txt.split()
                        if len(parts)>=2:
                            sym=parts[1].upper(); raw=parts[2] if len(parts)>=3 else "1d"
                            mp={"5":"5m","15":"15m","30":"30m","1h":"1h","4h":"4h","1":"1d","d":"1d","w":"1w","m":"1M","1d":"1d","1w":"1w","5m":"5m","15m":"15m","30m":"30m","1M":"1M"}
                            tf=mp.get(raw.lower(),raw.lower())
                            threading.Thread(target=process_chart_request,args=(chat_id,sym,tf,LAST_SIGNALS_CACHE)).start()
                    elif first in ["/b","/broker","/bandar"]:
                        parts=txt.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            def bd(tg,s):
                                try:
                                    multi=get_broker_multi_tf(s)
                                    mk=source_marker(multi.get('source_d','EMPTY'))
                                    msg=f"🏦 *{s} AKUM/DIST* {mk}\nDaily: {multi.get('status_d')} Net {format_large_number(multi.get('net_d',0),True)}\n AKUM {format_large_number(multi.get('akum_d',0),True)} DIST {format_large_number(multi.get('dist_d',0),True)}\nTop: {format_top_brokers(multi.get('brokers',[]),5)}\n"
                                    for b in multi.get('brokers',[])[:5]:
                                        n=float(b.get('nval',0))
                                        msg+=f"  {b.get('broker_code')}: {format_large_number(n,True)} {'🟢' if n>0 else '🔴'}\n"
                                    msg+=f"W: {format_large_number(multi.get('net_5d',0),True)} M: {format_large_number(multi.get('net_20d',0),True)}\n"
                                    send_reply(tg,msg)
                                except Exception as e: send_reply(tg,f"❌ {e}")
                            threading.Thread(target=bd,args=(chat_id,sym)).start()
                    elif first in ["/quota"]:
                        send_reply(chat_id,f"📊 QUOTA: {'HABIS' if QUOTA_HIT else 'OK'}\nCache: {len(BROKER_CACHE)}")
                    elif first in ["/clearcache","/cc","/clear"]:
                        try:
                            if QUOTA_HIT:
                                send_reply(chat_id,"⛔ Jangan clearcache pas HABIS!")
                            else:
                                BROKER_CACHE.clear(); HISTORY_CACHE.clear(); SCREENER_CACHE.clear(); LAST_SIGNALS_CACHE.clear()
                                QUOTA_HIT=False; LAST_429_TIME=0
                                if os.path.exists("/tmp/rafano_cache.json"): os.remove("/tmp/rafano_cache.json")
                                send_reply(chat_id,"🧹 Cleared")
                        except Exception as e: send_reply(chat_id,f"❌ {e}")
                    elif first in ["/scan","!scan","/scanall","/scanfull"]:
                        send_reply(chat_id,"🔍 *SCAN 60 saham...*")
                        def ms(tg=chat_id):
                            global LAST_SIGNALS_CACHE
                            sigs=scan_v3_full(); LAST_SIGNALS_CACHE={s['symbol']:s for s in sigs}; broadcast_v3(sigs)
                        threading.Thread(target=ms,args=(chat_id,)).start()
                    elif first in ["/scanfast"]:
                        send_reply(chat_id,"⚡ *FAST 20...*")
                        def fs(tg=chat_id):
                            global LAST_SIGNALS_CACHE
                            old=get_screener_latest()
                            if old: cands=[x.get('symbol') for x in old[:20]]
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
                        threading.Thread(target=fs,args=(chat_id,)).start()
                    # ===== SCAN VOL SPIKE =====
                    elif first in ["/scanvol","/vol","/vspike","/volspike","/spike"]:
                        parts=txt.split()
                        try:
                            thr=float(parts[1]) if len(parts)>=2 else 2.0
                        except:
                            thr=2.0
                        if thr<1.5: thr=1.5
                        if thr>10: thr=10
                        send_reply(chat_id, f"🔥 *SCAN VOL SPIKE >{thr}x* (30-60 saham, 1-2 menit)...")
                        def vol_scan(tg=chat_id, th=thr):
                            sigs=scan_volume_spike(threshold=th, limit_candidates=60, akum_only=False)
                            broadcast_vol_spike(sigs, threshold=th, akum_only=False)
                        threading.Thread(target=vol_scan, args=(chat_id, thr)).start()
                    # ===== NEW: VOL SPIKE + AKUM REAL ONLY =====
                    elif first in ["/scanvolakum","/volakum","/vspikeakum","/vakum","/spikeakum"]:
                        parts=txt.split()
                        try:
                            thr=float(parts[1]) if len(parts)>=2 else 2.0
                        except:
                            thr=2.0
                        if thr<1.5: thr=1.5
                        if thr>10: thr=10
                        send_reply(chat_id, f"🔥 *SCAN VOL SPIKE + AKUM REAL >{thr}x* (filter fake pump, 2-3 menit)...")
                        def volakum_scan(tg=chat_id, th=thr):
                            sigs=scan_volume_spike(threshold=th, limit_candidates=60, akum_only=True)
                            broadcast_vol_spike(sigs, threshold=th, akum_only=True)
                        threading.Thread(target=volakum_scan, args=(chat_id, thr)).start()
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
    print("🔥 RAFANO V4.3.6 VOL AKUM LOOSE + VSA")
    print("==========================================")
    print("Commands: /scanvol 2, /volspike, /scan, /c <kode>")
    threading.Thread(target=auto_screener_loop,daemon=True).start()
    telegram_bot_listener()
