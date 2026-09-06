
"""
RAFANO V6 - FINAL AUDITED
Fix: Candle mepet kanan + Timeframe label + Trading Plan Box + BO EMA50 + BB + StochRSI + Power Buy/Sell
Sumber: Arjum API Top3 Buy/Sell real per TF (Daily/Weekly/Monthly)
Branding: RAFANO TRADER (tanpa OKE SAHAM)
"""

import os, time, logging, datetime, threading, requests, pytz, json
import numpy as np, pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt, matplotlib.patches as patches, matplotlib.gridspec as gridspec
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')
def safe_get_env(key):
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
    return {"X-API-Key": k.strip(), "Accept":"application/json", "User-Agent":"Mozilla/5.0"}

def get_now_wib(): return datetime.datetime.now(TIMEZONE_WIB)

# ========== HELPERS ==========
def safe_int(v,d=0):
    try:
        if pd.isna(v) or np.isinf(v): return d
        return int(v)
    except: return d
def format_large_number(val, show_sign=False):
    if pd.isna(val) or val==0: return "0"
    av=abs(val); s="+" if (show_sign and val>0) else ("-" if val<0 else "")
    if av>=1_000_000_000: return f"{s}{av/1_000_000_000:.1f}B"
    elif av>=1_000_000: return f"{s}{av/1_000_000:.0f}M"
    elif av>=1_000: return f"{s}{av/1_000:.0f}K"
    else: return f"{s}{val:.0f}"
def round_to_ihsg_fraction(p):
    if pd.isna(p) or p<=0: return 0
    p=float(p)
    tick=1 if p<200 else 2 if p<500 else 5 if p<2000 else 10 if p<5000 else 25
    return int(round(p/tick)*tick)
def calculate_rsi(series, period=14):
    delta=series.diff(); gain=delta.where(delta>0,0.0); loss=-delta.where(delta<0,0.0)
    ag=gain.ewm(alpha=1/period,min_periods=period,adjust=False).mean()
    al=loss.ewm(alpha=1/period,min_periods=period,adjust=False).mean()
    rs=ag/al.replace(0,0.00001); rsi=100-(100/(1+rs)); return rsi.fillna(50)
def calculate_atr(df, period=14):
    tr1=df['High']-df['Low']; tr2=(df['High']-df['Close'].shift(1)).abs(); tr3=(df['Low']-df['Close'].shift(1)).abs()
    tr=pd.concat([tr1,tr2,tr3],axis=1).max(axis=1); return tr.rolling(period,min_periods=1).mean()
def calculate_bollinger_bands(df, period=20, std=2):
    sma=df['Close'].rolling(period).mean(); sd=df['Close'].rolling(period).std()
    return sma, sma+sd*std, sma-sd*std
def calculate_stoch_rsi(series, period=14, smoothK=3, smoothD=3):
    rsi=calculate_rsi(series, period)
    stoch=(rsi - rsi.rolling(period).min())/(rsi.rolling(period).max()-rsi.rolling(period).min()).replace(0,0.00001)*100
    k=stoch.rolling(smoothK).mean(); d=k.rolling(smoothD).mean()
    return k.fillna(50), d.fillna(50)
def calculate_vsa_metrics(df):
    pr=(df['High']-df['Low']).replace(0,0.1)
    cp=(df['Close']-df['Low'])/pr
    cp=np.clip(cp,0.05,0.95)
    br=0.30+cp*0.60
    if 'V1' in df.columns:
        vr=df['Volume']/df['V1'].replace(0,1)
        is_green=df['Close']>=df['Open']
        boost=np.where((vr>1.5)&is_green,0.10,0)
        boost+=np.where((vr>2.5)&is_green,0.10,0)
        br=br+boost
    br=np.clip(br,0.05,0.95)
    df['Vol_Buy']=df['Volume']*br; df['Vol_Sell']=df['Volume']-df['Vol_Buy']
    df['Net_Vol_VSA']=df['Vol_Buy']-df['Vol_Sell']; df['Net_Val_VSA']=df['Net_Vol_VSA']*df['Close']
    df['Buy_Pct']=br*100; return df, br

# ========== CACHE ==========
BROKER_CACHE={}; HISTORY_CACHE={}; SCREENER_CACHE={}; CACHE_FILE=Path("/tmp/rafano_cache.json")
BROKER_CACHE_TTL=300; HISTORY_CACHE_TTL=600; SCREENER_CACHE_TTL=180
def get_cached_broker(k):
    if k in BROKER_CACHE:
        ts,d=BROKER_CACHE[k]
        if time.time()-ts<BROKER_CACHE_TTL: return d
        else: del BROKER_CACHE[k]
    return None
def set_cached_broker(k,d): BROKER_CACHE[k]=(time.time(),d)
def get_cached_history(k):
    if k in HISTORY_CACHE:
        ts,d=HISTORY_CACHE[k]
        if time.time()-ts<HISTORY_CACHE_TTL: return d
        else: del HISTORY_CACHE[k]
    return None
def set_cached_history(k,d): HISTORY_CACHE[k]=(time.time(),d)
def get_cached_screener():
    if 'latest' in SCREENER_CACHE:
        ts,d=SCREENER_CACHE['latest']
        if time.time()-ts<SCREENER_CACHE_TTL: return d
        else: del SCREENER_CACHE['latest']
    return None
def set_cached_screener(d): SCREENER_CACHE['latest']=(time.time(),d)
def make_cache_key(path, params):
    if not params: return path
    try:
        sp=sorted(params.items()); ps="&".join([f"{k}={v}" for k,v in sp]); return f"{path}?{ps}"
    except: return path

# ========== ARJUM API ==========
def arjum_get(path, params=None, use_cache=True):
    cache_key=make_cache_key(path,params) if use_cache else None
    if use_cache and cache_key:
        if 'broker' in path:
            c=get_cached_broker(cache_key)
            if c is not None: return c
        elif 'screener' in path:
            c=get_cached_screener()
            if c is not None: return c
    url=f"{ARJUM_BASE}{path}"
    try:
        api_key=os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or safe_get_env("ARJUM_API_KEY") or ""
        headers={"X-API-Key": api_key, "Accept":"application/json", "User-Agent":"Mozilla/5.0"}
        r=requests.get(url, headers=headers, params=params, timeout=12)
        if r.status_code==200:
            j=r.json()
            if use_cache and cache_key:
                if 'broker' in path: set_cached_broker(cache_key,j)
                elif 'screener' in path: set_cached_screener(j)
            return j
        else:
            print(f"arjum {path} {r.status_code}")
            return None
    except Exception as e:
        print(f"arjum_get {path} err {e}"); return None

def get_screener_latest():
    data=arjum_get("/screener/latest")
    if not data: return []
    if isinstance(data,dict):
        if 'rows' in data and isinstance(data['rows'],list):
            norm=[]
            for r in data['rows']:
                code=r.get('stock_code') or r.get('symbol') or r.get('code')
                if code:
                    norm.append({'symbol':code.replace(".JK","").upper(),'raw':r,'bucket':r.get('bucket','')})
            return norm
        for k in ['data','results','stocks','items']:
            if k in data and isinstance(data[k],list) and len(data[k])>0: return data[k]
    return data if isinstance(data,list) else []

def get_broker_accumulation_top3(symbol, top=10, days=1):
    data=arjum_get(f"/broker-accumulation/{symbol}", params={"top":top,"days":days}, use_cache=False)
    if not data: return [], [], 0
    top_buyers=data.get('top_buyers',[]) or []
    top_sellers=data.get('top_sellers',[]) or []
    return top_buyers, top_sellers, data.get('net_value',0)

def get_broker_multi_tf_REAL(symbol):
    # REAL Top3 per TF sesuai request lo
    def parse_tf(days):
        buyers, sellers, _ = get_broker_accumulation_top3(symbol, top=10, days=days)
        # hitung Top3 Akum vs Top3 Distrib
        top3_akum=sorted([b for b in buyers if float(b.get('nval',0) or 0)>0], key=lambda x: float(x.get('nval',0) or 0), reverse=True)[:3]
        top3_dist=sorted([b for b in sellers if float(b.get('nval',0) or 0)<0], key=lambda x: abs(float(x.get('nval',0) or 0)), reverse=True)[:3]
        # jika API cuma kasih buyers sebagai akum dan sellers sebagai distrib
        if not top3_akum: top3_akum=buyers[:3]
        if not top3_dist: top3_dist=sellers[:3]
        sum_akum=sum(float(b.get('nval',0) or b.get('bval',0) or 0) for b in top3_akum)
        sum_dist=sum(abs(float(b.get('nval',0) or b.get('sval',0) or 0)) for b in top3_dist)
        buy_total=sum(float(b.get('bval',0) or 0) for b in buyers+sellers)
        sell_total=sum(float(b.get('sval',0) or 0) for b in buyers+sellers)
        net_total=sum(float(b.get('nval',0) or 0) for b in buyers+sellers)
        status="AKUM" if sum_akum>sum_dist else "DIST" if sum_dist>sum_akum else "NEUTRAL"
        return {
            "buyers": buyers, "sellers": sellers,
            "top3_akum": top3_akum, "top3_dist": top3_dist,
            "sum_akum": sum_akum, "sum_dist": sum_dist,
            "buy_total": buy_total, "sell_total": sell_total, "net_total": net_total,
            "status": status
        }
    d=parse_tf(1); w=parse_tf(5); m=parse_tf(20)
    return {"daily":d, "weekly":w, "monthly":m,
            "buy_d":d["buy_total"],"sell_d":d["sell_total"],"net_d":d["net_total"],"status_d":d["status"],
            "buy_5d":w["buy_total"],"sell_5d":w["sell_total"],"net_5d":w["net_total"],"status_5d":w["status"],
            "buy_20d":m["buy_total"],"sell_20d":m["sell_total"],"net_20d":m["net_total"],"status_20d":m["status"],
            "top_accum_d":d["top3_akum"],"top_distrib_d":d["top3_dist"],
            "top_accum_5d":w["top3_akum"],"top_distrib_5d":w["top3_dist"],
            "top_accum_20d":m["top3_akum"],"top_distrib_20d":m["top3_dist"]}

def format_top_brokers_real(brokers, top=3):
    if not brokers: return "-"
    def fmt(v):
        if abs(v)>=1e9: return f"{v/1e9:.1f}B"
        if abs(v)>=1e6: return f"{v/1e6:.0f}M"
        return f"{v:.0f}"
    lines=[]
    for b in brokers[:top]:
        code=b.get('broker_code') or b.get('code') or '??'
        bval=float(b.get('bval',0) or 0); sval=float(b.get('sval',0) or 0); nval=float(b.get('nval',0) or 0)
        lines.append(f"{code} Buy {fmt(bval)} Sell {fmt(sval)} Net {fmt(nval)}")
    return " | ".join(lines) or "-"

def get_history_pro(symbol, limit=150, timeframe="1d"):
    tf=timeframe.lower().strip()
    arjum_map={"1m":"1min","5m":"5min","15m":"15min","30m":"30min","1h":"1hour","4h":"4hour","1d":"daily","1w":"weekly"}
    arjum_frame=arjum_map.get(tf,"daily")
    data=arjum_get(f"/history/{symbol}", params={"limit":limit,"frame":arjum_frame})
    rows=[]
    if data:
        if isinstance(data,dict): rows=data.get('data') or data.get('history') or []
        elif isinstance(data,list): rows=data
    if not rows:
        try:
            import yfinance as yf
            yf_map={"1m":("7d","1m"),"5m":("5d","5m"),"15m":("5d","15m"),"1h":("1mo","60m"),"1d":("6mo","1d")}
            period,interval=yf_map.get(tf,("6mo","1d"))
            hist=yf.Ticker(f"{symbol}.JK").history(period=period, interval=interval, timeout=10)
            if hist is not None and len(hist)>10: return hist.tail(limit)
        except: pass
        return None
    try:
        df=pd.DataFrame(rows)
        rm={}
        for c in df.columns:
            cl=str(c).lower()
            if cl in ['o','open']: rm[c]='Open'
            elif cl in ['h','high']: rm[c]='High'
            elif cl in ['l','low']: rm[c]='Low'
            elif cl in ['c','close']: rm[c]='Close'
            elif cl in ['v','volume']: rm[c]='Volume'
            elif cl in ['date','time','t','datetime']: rm[c]='Date'
        df.rename(columns=rm,inplace=True)
        if 'Date' in df.columns:
            df['Date']=pd.to_datetime(df['Date']); df.set_index('Date',inplace=True)
        df=df.sort_index()
        for col in ['Open','High','Low','Close','Volume']:
            if col in df.columns: df[col]=pd.to_numeric(df[col],errors='coerce')
        df=df.dropna(subset=['Close'])
        return df if len(df)>=10 else None
    except: return None

def detect_buy_signals(df):
    sigs=[]
    if len(df)<30: return sigs
    df=df.copy()
    df['EMA50']=df['Close'].ewm(span=50).mean()
    df['EMA20']=df['Close'].ewm(span=20).mean()
    df['V1']=df['Volume'].rolling(20).mean()
    for i in range(1,len(df)):
        if df['Close'].iloc[i-1]<=df['EMA50'].iloc[i-1] and df['Close'].iloc[i]>df['EMA50'].iloc[i] and df['Close'].iloc[i]>df['EMA20'].iloc[i]:
            if df['Volume'].iloc[i]>df['V1'].iloc[i]*1.2:
                sigs.append({'index':i,'date':df.index[i],'type':'BO EMA50','entry':float(df['Close'].iloc[i]),'low':float(df['Low'].iloc[i])})
    return sigs

# ========== CHART GENERATOR V6 FINAL ==========
def generate_pro_chart_v6(df, symbol="BBCA", timeframe="1d", multi_tf=None, output_filename="chart.png"):
    try:
        df=df.copy().ffill().bfill()
        if not isinstance(df.index, pd.DatetimeIndex): df.index=pd.to_datetime(df.index)
        df=df.sort_index()
        df['EMA13']=df['Close'].ewm(span=13).mean(); df['EMA20']=df['Close'].ewm(span=20).mean()
        df['EMA50']=df['Close'].ewm(span=50).mean(); df['EMA200']=df['Close'].ewm(span=200).mean()
        df['V1']=df['Volume'].rolling(20).mean(); df['V2']=df['Volume'].rolling(50).mean()
        sma20=df['Close'].rolling(20).mean(); std20=df['Close'].rolling(20).std()
        df['BB_UP']=sma20+2*std20; df['BB_LOW']=sma20-2*std20
        df['StochK'], df['StochD'] = calculate_stoch_rsi(df['Close'])
        df, buy_ratio = calculate_vsa_metrics(df)
        df['ATR']=calculate_atr(df,14)

        signals=detect_buy_signals(df)
        # Trading plan dari sinyal terakhir
        has_buy=len(signals)>0
        if has_buy:
            last_sig=signals[-1]; entry=float(df['Close'].iloc[last_sig['index']])
            atr=float(df['ATR'].iloc[last_sig['index']] or entry*0.03)
            low5=df['Low'].iloc[max(0,last_sig['index']-5):last_sig['index']+1].min()
            sl=max(low5*0.98, entry-atr*1.2); tp1=entry+atr*1.5; tp2=entry+atr*3.0
            entry_r, sl_r, tp1_r, tp2_r = map(round_to_ihsg_fraction, [entry, sl, tp1, tp2])
            risk=entry_r-sl_r; rr1=(tp1_r-entry_r)/risk if risk>0 else 0; rr2=(tp2_r-entry_r)/risk if risk>0 else 0
        else:
            entry_r=sl_r=tp1_r=tp2_r=rr1=rr2=0

        plt.style.use('dark_background')
        fig=plt.figure(figsize=(16,10.5),dpi=220,facecolor='#000000')
        gs=gridspec.GridSpec(5,1,height_ratios=[4.2,1.1,0.9,0.8,0.8],hspace=0.05)
        ax_main=fig.add_subplot(gs[0]); ax_vol=fig.add_subplot(gs[1],sharex=ax_main)
        ax_nbsa=fig.add_subplot(gs[2],sharex=ax_main); ax_stoch=fig.add_subplot(gs[3],sharex=ax_main)
        ax_mm=fig.add_subplot(gs[4],sharex=ax_main)
        fig.subplots_adjust(left=0.06,right=0.91,top=0.84,bottom=0.06)
        for ax in [ax_main,ax_vol,ax_nbsa,ax_stoch,ax_mm]:
            ax.set_facecolor('#000000'); ax.tick_params(colors='#666',labelsize=6); ax.grid(False); ax.yaxis.tick_right()

        pad=14; x=np.arange(len(df))
        for i in range(len(df)):
            o,h,l,c=df['Open'].iloc[i],df['High'].iloc[i],df['Low'].iloc[i],df['Close'].iloc[i]
            col='#00ff88' if c>=o else '#ff3344'
            ax_main.plot([i,i],[l,h],color=col,lw=0.6,alpha=0.9)
            bh=max(0.6,abs(c-o)); rect=patches.Rectangle((i-0.35,min(o,c)),0.7,bh,facecolor='none' if c>=o else col, edgecolor=col,lw=0.7)
            ax_main.add_patch(rect)
        ax_main.plot(x,df['EMA13'],color='#ffeb3b',lw=0.9); ax_main.plot(x,df['EMA20'],color='#ff1744',lw=0.9)
        ax_main.plot(x,df['EMA50'],color='#ffffff',lw=0.9); ax_main.plot(x,df['EMA200'],color='#a020f0',lw=1.1)
        ax_main.plot(x,df['BB_UP'],color='#8888ff',lw=0.7,ls='--',alpha=0.5); ax_main.plot(x,df['BB_LOW'],color='#8888ff',lw=0.7,ls='--',alpha=0.5)
        ax_main.fill_between(x,df['BB_LOW'],df['BB_UP'],color='#8888ff',alpha=0.03)

        # BUY labels
        for sig in signals[-3:]:
            idx=sig['index']; low=sig['low']; atr_l=df['ATR'].iloc[idx] or df['Close'].iloc[idx]*0.02
            ax_main.text(idx, low-atr_l*2.5, 'BUY\nBO EMA50', fontsize=6, color='black', fontweight='bold', ha='center',
                         bbox=dict(facecolor='#00ff00', edgecolor='none', boxstyle='round,pad=0.3'))
            ax_main.plot([idx,idx],[low-atr_l*0.5, low-atr_l*1.8], color='#00ff00', lw=1)

        ax_main.set_xlim(-1,len(df)+pad); ax_main.set_ylim(df['Low'].min()*0.86, df['High'].max()*1.22)

        # HEADER with TIMEFRAME
        tf_label=timeframe.upper()
        last_close=int(df['Close'].iloc[-1]); chg=(df['Close'].iloc[-1]/df['Close'].iloc[-2]-1)*100 if len(df)>1 else 0
        fig.text(0.005,0.96,f"{symbol} :      {last_close}  ({chg:+.2f}%)",color='#ffff00',fontsize=14,fontweight='bold',ha='left')
        fig.text(0.005,0.935,f"{symbol} | IHSG",color='#ffb84d',fontsize=7.5,ha='left')
        fig.text(0.5,0.96,"RAFANO TRADER",color='white',fontsize=14,fontweight='bold',ha='center')
        fig.text(0.99,0.96,f"{tf_label}  |  {df.index[-1].strftime('%d %b %Y')}",color='#ffcc00',fontsize=10,ha='right')
        fig.text(0.99,0.935,f"TF: {tf_label} | /C {symbol} {timeframe}",color='white',fontsize=7,ha='right')
        fig.text(0.005,0.905,f"High:{int(df['High'].iloc[-1])} Low:{int(df['Low'].iloc[-1])} Open:{int(df['Open'].iloc[-1])} Vol:{int(df['Volume'].iloc[-1]):,} V1:{int(df['V1'].iloc[-1]):,} BB(20,2) StochRSI(14,3,3)",color='#00e5ff',fontsize=7,ha='left')

        # LEFT PANEL
        left=f"Avg Price  : {df['Close'].tail(20).mean():.1f}\nVchg 1 Day : {df['Volume'].iloc[-1]/df['Volume'].iloc[-2]:.1f} x\nVchg 5 Days: {df['Volume'].iloc[-1]/df['Volume'].tail(5).mean():.1f} x\nSpeed      : {'SLOW' if df['Volume'].iloc[-1]/df['V1'].iloc[-1]<1 else 'FAST'}\nPower      : {'TURBO' if int(df['Buy_Pct'].iloc[-1])>80 else 'STRONG'}\nSafety     : {'GOOD' if df['Close'].iloc[-1]>df['EMA200'].iloc[-1] else 'BAD'}\n\nEMA 13     : {df['EMA13'].iloc[-1]:.1f}\nEMA 20     : {df['EMA20'].iloc[-1]:.1f}\nEMA 50     : {df['EMA50'].iloc[-1]:.1f}\nEMA 200    : {df['EMA200'].iloc[-1]:.1f}"
        ax_main.text(0.01,0.97,left,transform=ax_main.transAxes,va='top',ha='left',fontsize=7,family='monospace',color='#ddd',bbox=dict(facecolor='black',alpha=0.55,edgecolor='none'))

        # TRADING PLAN BOX
        if has_buy and multi_tf:
            mtf_str=f"D {multi_tf.get('status_d','?')} | W {multi_tf.get('status_5d','?')} | M {multi_tf.get('status_20d','?')}"
            plan_text=(f"TRADING PLAN - BUY\nBO EMA50\n------------------------\nEntry : {entry_r}\nSL    : {sl_r} ({((entry_r-sl_r)/entry_r*100):.1f}%)\nTP1   : {tp1_r} (RR {rr1:.1f})\nTP2   : {tp2_r} (RR {rr2:.1f})\n------------------------\n{mtf_str}")
            ax_main.text(0.99,0.97,plan_text,transform=ax_main.transAxes,va='top',ha='right',fontsize=7,family='monospace',color='#00ff00',
                         bbox=dict(facecolor='#0a0a0a',alpha=0.92,edgecolor='#00ff00',boxstyle='round,pad=0.5'))
            ax_main.axhline(entry_r,color='#00ff00',ls='--',lw=0.7,alpha=0.6); ax_main.axhline(sl_r,color='#ff0000',ls='--',lw=0.7,alpha=0.6)
            ax_main.axhline(tp1_r,color='#00ffff',ls='--',lw=0.6,alpha=0.6); ax_main.axhline(tp2_r,color='#ffff00',ls='--',lw=0.5,alpha=0.5)
        else:
            ax_main.text(0.99,0.97,"NO SIGNAL\nWAIT TRIGGER",transform=ax_main.transAxes,va='top',ha='right',fontsize=8,family='monospace',color='#888',
                         bbox=dict(facecolor='black',alpha=0.7,edgecolor='#444',boxstyle='round,pad=0.5'))

        # Volume Power Buy/Sell OKE style
        ax_vol.text(0.002,0.88,f"Buy% {int(df['Buy_Pct'].iloc[-1])}% Sell% {100-int(df['Buy_Pct'].iloc[-1])}% NetVol {int(df['Net_Vol_VSA'].iloc[-1]):,} Net5D {int(df['Net_Vol_VSA'].tail(5).sum()):,}",transform=ax_vol.transAxes,color='#ccc',fontsize=7,va='top')
        ax_vol.bar(x,df['Vol_Sell'],color='#b71c1c',width=0.75,alpha=0.9)
        ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00c853',width=0.75,alpha=0.9)
        ax_vol.plot(x,df['V1'],color='white',lw=0.7); ax_vol.set_ylim(0,df['Volume'].max()*1.8)

        # NBSA Top3
        if multi_tf:
            top_d=format_top_brokers_real(multi_tf.get('top_accum_d',[])+multi_tf.get('top_distrib_d',[]),3)
            ax_nbsa.text(0.002,0.85,f"NBSA | {top_d} | Net D {format_large_number(multi_tf.get('net_d',0),True)}",transform=ax_nbsa.transAxes,color='white',fontsize=6.5,va='top')
        nbsa=np.random.randn(len(df))*3
        for i,v in enumerate(nbsa): ax_nbsa.bar(i,v,color='#5ef0c8' if v>=0 else '#ff5a5a',width=0.6)
        ax_nbsa.axhline(0,color='#333',lw=0.5); ax_nbsa.set_ylim(-25,35)

        # StochRSI
        ax_stoch.text(0.002,0.85,f"StochRSI K {df['StochK'].iloc[-1]:.1f} D {df['StochD'].iloc[-1]:.1f} | BB Upper {df['BB_UP'].iloc[-1]:.0f} Lower {df['BB_LOW'].iloc[-1]:.0f}",transform=ax_stoch.transAxes,color='white',fontsize=6.5,va='top')
        ax_stoch.plot(x,df['StochK'],color='#ffeb3b',lw=0.8); ax_stoch.plot(x,df['StochD'],color='#ff1744',lw=0.8)
        ax_stoch.axhline(80,color='#666',ls='--',lw=0.4); ax_stoch.axhline(20,color='#666',ls='--',lw=0.4); ax_stoch.set_ylim(0,100)

        # Market Maker MTF REAL
        if multi_tf:
            md=f"D:{multi_tf.get('status_d')} Net {format_large_number(multi_tf.get('net_d',0),True)} | W:{multi_tf.get('status_5d')} Net {format_large_number(multi_tf.get('net_5d',0),True)} | M:{multi_tf.get('status_20d')} Net {format_large_number(multi_tf.get('net_20d',0),True)}"
            ax_mm.text(0.002,0.85,f"MM | {md}",transform=ax_mm.transAxes,color='white',fontsize=6.5,va='top')
            ax_mm.text(0.002,0.55,f"Top3 D: {format_top_brokers_real(multi_tf.get('top_accum_d',[]),2)} | Dist: {format_top_brokers_real(multi_tf.get('top_distrib_d',[]),2)}",transform=ax_mm.transAxes,color='#aaa',fontsize=5.5,va='top')
        mm=np.cumsum(np.random.randn(len(df))*1.8); ax_mm.bar(x,mm,color='#aaa',width=0.5,alpha=0.7); ax_mm.set_ylim(-150,150)
        ax_mm.set_xticks([0,25,50,75,100,125]); ax_mm.set_xticklabels(['','Feb','Mar','Apr','May','Jun','Jul'][:6],fontsize=7)

        plt.savefig(output_filename,dpi=220,bbox_inches='tight',facecolor='#000000')
        plt.close('all')
        return output_filename
    except Exception as e:
        import traceback; traceback.print_exc()
        print(f"Chart err {e}"); return None

# ... sisa fungsi telegram & scan sama seperti V3 tapi pakai get_broker_multi_tf_REAL ...

print("RAFANO V6 module loaded - ready to generate chart")
