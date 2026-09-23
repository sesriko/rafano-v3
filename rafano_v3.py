"""RAFANO V4.36 FINAL - CLICK FIX + 400 BY VALUE + FILTER SUSPEND/FCA/PRICE<50 + CAPTION SIMPLE + AUTOSCAN"""
import os, time, datetime, threading, requests, pytz, re
import numpy as np, pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.gridspec as gridspec
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import OrderedDict

for p in ['/content/rafano-v3/.env','./.env','.env','/content/.env','/content/rafano-v3/rafano-v3/.env']:
    if os.path.exists(p):
        load_dotenv(p, override=True)
        break

TIMEZONE_WIB=pytz.timezone('Asia/Jakarta')
TELEGRAM_BOT_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN") or ""
TARGET_CHAT_ID=os.getenv("TARGET_CHAT_ID") or ""
ARJUM_API_KEY=os.getenv("ARJUM_API_KEY") or ""
ARJUM_BASE="https://stock.arjum.com/api"

def get_now_wib(): return datetime.datetime.now(TIMEZONE_WIB)

FCA_EXCLUDE={"FUTR","FITT","HOTEL","ITIC","PUDP","COIN","SHID","RELI","ASPI","MEJA","MINA","ESTA","ASLI","VKTR","IMJS","GTSI","IRSX","ATAP","RONY","BCIC","DEFI","ROCK","YPAS","NIRO","BBHA","BKSW","NAGA","BEEF","BPTR","CBMF","CPRI","CRAB","DAAZ","DEAL","DGNS","DMND","DUCK","ENRG","ENVY","ESTI","ETWA","FIRE","FORU","GAMA","GOLL","HAIS","HATM","HITS","HOMI","IATA","INPS","IPOL","JGLE","KAYU","KBAG","KIOS","KPAL","KPAS","LCGP","LPLI","LPLR","MAGP","MAMI","MARI","SINI","SKYB","SMKM","SOCI","SONA","SOSS","SUGI","TALF","TDPM","TEBE","TOPS","TRAM","TRIL","TRIO","TRUS","UFOE","WIFI-W","WOWS","YELO","ZATA","ZONE","ZINC","MPRO","BAPI","MMLP","NICE","BLES","HOPE","ALMI","RMBA","MTRA","KRAH","INCF","FPNI","CASA","KBLI","RIMO","JARR-W","COAL-W","BBYB-W","BTPS-W"}

SUSPEND_CACHE={}
MIN_PRICE=50

def load_785():
    for p in ['data/daytrade-observe-tickers.txt','./data/daytrade-observe-tickers.txt','/content/rafano-v3/data/daytrade-observe-tickers.txt']:
        if os.path.exists(p):
            try:
                t=[x.strip().upper() for x in open(p, encoding='utf-8').read().splitlines() if len(x.strip())==4 and x.strip().isalpha()]
                if len(t)>=100: return t
            except: pass
    try:
        r=requests.get("https://raw.githubusercontent.com/budikuatno2-ship-it/auto-cuan/main/data/daytrade-observe-tickers.txt", timeout=10)
        if r.status_code==200:
            t=[x.strip().upper() for x in r.text.splitlines() if len(x.strip())==4 and x.strip().isalpha()]
            if len(t)>=100: return t
    except: pass
    return ["BBCA","BBRI","BMRI","BBNI","TLKM","ASII","GOTO","BUKA","BREN","CUAN","WIFI","DEWA","BULL","ERAA","BAIK","BBKP","BRIS","ANTM","INCO","MDKA","ADRO","PTBA","PGAS","EXCL","ISAT","AMRT","TOWR","BBYB","BRMS","BRPT","BREN","CUAN"]

IDX_FULL=load_785()
HISTORY_CACHE=OrderedDict(); SCREENER_CACHE=OrderedDict()
QUOTA_HIT=False; LAST_429_TIME=0; LAST_ARJUM_REQUEST=0
ARJUM_MIN_INTERVAL=1.2
STOCKBIT_CACHE={}
AUTO_KIM_ENABLED=False

def get_cached(k,cache,ttl):
    if k in cache:
        ts,d=cache[k]
        if time.time()-ts<ttl: return d
    return None
def set_cached(k,d,cache, maxsize=400):
    cache[k]=(time.time(),d)
    try: cache.move_to_end(k)
    except: pass
    if len(cache)>maxsize:
        try: cache.popitem(last=False)
        except: pass

def arjum_get(path, params=None, bypass_quota=False, retries=1):
    global QUOTA_HIT, LAST_429_TIME, LAST_ARJUM_REQUEST
    elapsed=time.time()-LAST_ARJUM_REQUEST
    if elapsed<ARJUM_MIN_INTERVAL: time.sleep(ARJUM_MIN_INTERVAL-elapsed)
    if QUOTA_HIT and not bypass_quota and time.time()-LAST_429_TIME<180: return None
    url=f"{ARJUM_BASE}{path}"
    for attempt in range(retries+1):
        try:
            LAST_ARJUM_REQUEST=time.time()
            headers={"X-API-Key": ARJUM_API_KEY.strip(),"Accept":"application/json"}
            r=requests.get(url,headers=headers,params=params,timeout=15)
            if r.status_code==200:
                if QUOTA_HIT and time.time()-LAST_429_TIME>60: QUOTA_HIT=False
                return r.json()
            elif r.status_code==429:
                if not bypass_quota: QUOTA_HIT=True; LAST_429_TIME=time.time()
                if attempt<retries: time.sleep(5); continue
                return None
            else: return None
        except:
            if attempt<retries: time.sleep(2); continue
            return None
    return None

def get_screener_latest(force_today=False):
    if not force_today:
        c=get_cached('latest', SCREENER_CACHE, 300)
        if c and isinstance(c, dict) and 'rows' in c and len(c['rows'])>0: return c
    data=arjum_get("/screener/latest", bypass_quota=True, retries=1)
    if data and isinstance(data, dict) and 'rows' in data and len(data['rows'])>0:
        set_cached('latest', data, SCREENER_CACHE, maxsize=5); return data
    return {"rows": []}

def get_realtime_stockbit(sym):
    try:
        now=time.time()
        if sym in STOCKBIT_CACHE:
            ts,p,c=STOCKBIT_CACHE[sym]
            if now-ts<5: return p,c
        url=f"https://stockbit.com/symbol/{sym}"
        headers={"User-Agent":"Mozilla/5.0","Referer":"https://stockbit.com/"}
        r=requests.get(url, headers=headers, timeout=8)
        if r.status_code!=200: return 0,0
        txt=r.text
        m_price=re.search(r'"lastPrice"\s*:\s*(\d+(?:\.\d+)?)', txt)
        m_chg=re.search(r'"changePercentage"\s*:\s*(-?\d+(?:\.\d+)?)', txt)
        if m_price:
            price=float(m_price.group(1))
            if price<MIN_PRICE or price==100: return 0,0
            chg=float(m_chg.group(1)) if m_chg else 0
            STOCKBIT_CACHE[sym]=(now,price,chg)
            return price,chg
    except: pass
    return 0,0

def normalize_timeframe(tf_input):
    if not tf_input: return "1d"
    tf=str(tf_input).lower().strip()
    mapping={"5":"5m","5m":"5m","15":"15m","15m":"15m","30":"30m","30m":"30m","1h":"1h","60":"1h","60m":"1h","4h":"4h","1d":"1d","d":"1d","daily":"1d","1w":"1w"}
    return mapping.get(tf,"1d")

def get_history_pro(sym, limit=150, frame="daily"):
    frame=normalize_timeframe(frame)
    hk=f"{sym}_{frame}_{limit}"
    cached=get_cached(hk, HISTORY_CACHE, 600)
    if cached is not None: return cached
    if frame=="1d":
        data=arjum_get(f"/history/{sym}",params={"limit":limit,"frame":"daily"}, bypass_quota=False, retries=1)
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
                    elif cl in ['date','time']: rn[c]='Date'
                df.rename(columns=rn,inplace=True)
                if 'Date' in df.columns: df['Date']=pd.to_datetime(df['Date']); df.set_index('Date',inplace=True)
                df=df.sort_index()
                for col in ['Open','High','Low','Close','Volume']: df[col]=pd.to_numeric(df[col],errors='coerce')
                df=df.dropna(subset=['Close'])
                if len(df)>=10:
                    last_close=float(df['Close'].iloc[-1])
                    if last_close>=MIN_PRICE and not (df['Close'].nunique()==1 and int(df['Close'].iloc[0])==100):
                        if df['Volume'].tail(5).sum()!=0:
                            set_cached(hk,df,HISTORY_CACHE); return df
                        else: SUSPEND_CACHE[sym]=True
            except: pass
    try:
        import yfinance as yf
        interval_map={"5m":"5m","15m":"15m","30m":"30m","1h":"60m","4h":"60m","1d":"1d","1w":"1wk"}
        period_map={"5m":"5d","15m":"30d","30m":"60d","1h":"60d","4h":"60d","1d":"6mo","1w":"2y"}
        interval=interval_map.get(frame,"1d"); period=period_map.get(frame,"6mo")
        hist=yf.Ticker(f"{sym}.JK").history(period=period, interval=interval, timeout=15, auto_adjust=False)
        if hist is not None and len(hist)>=20:
            if float(hist['Close'].iloc[-1])<MIN_PRICE: return None
            if hist['Volume'].tail(5).sum()==0: SUSPEND_CACHE[sym]=True; return None
            if hist['Close'].nunique()==1 and int(hist['Close'].iloc[0])==100: return None
            set_cached(hk,hist.tail(limit),HISTORY_CACHE)
            return hist.tail(limit)
    except: pass
    return None

def format_timeframe_label(tf):
    tf=normalize_timeframe(tf)
    m={"5m":"5 Menit","15m":"15 Menit","30m":"30 Menit","1h":"1 Jam","4h":"4 Jam","1d":"Daily","1w":"Weekly"}
    return m.get(tf, tf.upper())
def format_large_number(val):
    try:
        if pd.isna(val) or val==0: return "0"
    except:
        if val==0: return "0"
    a=abs(val)
    if a>=1_000_000_000: return f"{a/1_000_000_000:.2f}B"
    elif a>=1_000_000: return f"{a/1_000_000:,.0f}M"
    elif a>=1_000: return f"{a/1_000:,.0f}K"
    else: return f"{val:,.0f}"
def f_round_ihsg(price):
    try:
        p=float(price)
        if p<200: tick=1.0
        elif p<500: tick=2.0
        elif p<2000: tick=5.0
        elif p<5000: tick=10.0
        else: tick=25.0
        return round(p/tick)*tick
    except: return price
def rma(series, period): return series.ewm(alpha=1/period, adjust=False, min_periods=period).mean()
def calculate_rsi(prices, period=14):
    try:
        delta=prices.diff()
        gain=(delta.where(delta>0,0)).ewm(alpha=1/period, adjust=False).mean()
        loss=(-delta.where(delta<0,0)).ewm(alpha=1/period, adjust=False).mean()
        rs=gain/loss.replace(0,0.001)
        rsi=100-(100/(1+rs))
        return float(rsi.iloc[-1]) if len(rsi)>0 and not pd.isna(rsi.iloc[-1]) else 50.0
    except: return 50.0
def calculate_vsa_metrics(df):
    df=df.copy()
    pr=(df['High']-df['Low']).replace(0,0.1)
    cp=(df['Close']-df['Low'])/pr
    cp=np.clip(cp,0.05,0.95)
    br=0.30+cp*0.60
    if 'V1' in df.columns:
        vr=df['Volume']/df['V1'].replace(0,1)
        ig=df['Close']>=df['Open']
        boost=np.where((vr>1.5)&ig,0.10,0)+np.where((vr>2.5)&ig,0.10,0)
        br=br+boost
        br=np.where(vr<0.3,0.30+cp*0.60,br)
    br=np.clip(br,0.10,0.95)
    df['Vol_Buy']=df['Volume']*br
    df['Vol_Sell']=df['Volume']-df['Vol_Buy']
    df['Net_Vol_VSA']=df['Vol_Buy']-df['Vol_Sell']
    return df,br
def calculate_bollinger_bands(df,p=20,s=2):
    sma=df['Close'].rolling(p).mean()
    std=df['Close'].rolling(p).std()
    return sma,sma+(std*s),sma-(std*s)
def calc_dmi_adx(df, period=14):
    try:
        high=df['High']; low=df['Low']; close=df['Close']
        prev_close=close.shift(1); prev_high=high.shift(1); prev_low=low.shift(1)
        tr=pd.concat([high-low, (high-prev_close).abs(), (low-prev_close).abs()],axis=1).max(axis=1)
        plus_dm=np.where((high-prev_high>prev_low-low) & (high-prev_high>0), high-prev_high, 0.0)
        minus_dm=np.where((prev_low-low>high-prev_high) & (prev_low-low>0), prev_low-low, 0.0)
        tr_s=rma(tr, period); plus_s=rma(pd.Series(plus_dm, index=df.index), period); minus_s=rma(pd.Series(minus_dm, index=df.index), period)
        plus_di=100*plus_s/tr_s.replace(0,1); minus_di=100*minus_s/tr_s.replace(0,1)
        dx=100*(plus_di-minus_di).abs()/(plus_di+minus_di).replace(0,1)
        adx=rma(dx, period)
        return plus_di.fillna(0), minus_di.fillna(0), adx.fillna(0)
    except:
        z=pd.Series([0]*len(df), index=df.index); return z,z,z
def calc_atr(df, period=14):
    try:
        high=df['High']; low=df['Low']; close=df['Close']; prev_close=close.shift(1)
        tr=pd.concat([high-low, (high-prev_close).abs(), (low-prev_close).abs()],axis=1).max(axis=1)
        return rma(tr, period)
    except: return pd.Series([0]*len(df), index=df.index)

def detect_kim_signal(df, timeframe="1d"):
    try:
        tf_norm=normalize_timeframe(timeframe)
        kim_allowed=tf_norm in ["1d","1h"]
        if df is None or len(df)<80: return {"buy":False, "b_modal":0, "markers":[], "allowed_tf":kim_allowed}
        df=df.copy(); close=df['Close']; open_=df['Open']; high=df['High']; low=df['Low']; volume=df['Volume']
        ema_f=close.ewm(span=20, adjust=False).mean(); ema_slow=close.ewm(span=200, adjust=False).mean()
        plus_di, minus_di, adx_ser=calc_dmi_adx(df, 14); atr_ser=calc_atr(df, 14)
        v_sma=volume.rolling(20).mean(); is_v_akum=(close>open_) & (volume>v_sma); is_v_spike=volume>(v_sma*1.8)
        hlc3=(high+low+close)/3; vp_cond=(hlc3*volume).where(is_v_akum, 0); v_cond=volume.where(is_v_akum, 0)
        sum_vp=vp_cond.rolling(60, min_periods=1).sum(); sum_v=v_cond.rolling(60, min_periods=1).sum()
        b_modal_raw=np.where(sum_v>0, sum_vp/sum_v, close); b_modal_series=pd.Series([f_round_ihsg(x) for x in b_modal_raw], index=df.index)
        common_ok=(is_v_akum | is_v_spike) & (adx_ser>20) & (atr_ser>atr_ser.rolling(20).mean()) & (close>ema_slow) & (abs((close-b_modal_series)/b_modal_series*100)<=40)
        crossover=(close.shift(1)<=b_modal_series.shift(1)) & (close>b_modal_series)
        raw_b=crossover & (close>ema_f) & common_ok; re_entry=(close>high.shift(1).rolling(10).max()) & (close>b_modal_series) & common_ok
        buy=bool((raw_b.iloc[-1] or re_entry.iloc[-1])) if len(raw_b)>0 else False
        if not kim_allowed: buy=False
        tp1=f_round_ihsg(float(close.iloc[-1])*1.1); sl=f_round_ihsg(float(low.rolling(20).min().iloc[-1]))
        markers=[]
        if kim_allowed:
            for i in range(len(df)):
                if raw_b.iloc[i] or re_entry.iloc[i]: markers.append({"idx":i})
        return {"buy":buy, "b_modal":float(b_modal_series.iloc[-1]), "b_modal_series":b_modal_series, "tp1":tp1, "sl":sl, "markers":markers, "allowed_tf":kim_allowed}
    except: return {"buy":False, "b_modal":0, "markers":[], "allowed_tf":False}

def detect_bsjp_signal(df, timeframe="15m"):
    try:
        tf_norm=normalize_timeframe(timeframe)
        bsjp_allowed=tf_norm in ["5m","15m"]
        if df is None or len(df)<60: return {"buy":False, "markers":[], "allowed_tf":bsjp_allowed}
        df=df.copy(); close=df['Close']; open_=df['Open']; high=df['High']; low=df['Low']; volume=df['Volume']
        ema200=close.ewm(span=200, adjust=False).mean()
        lookback=4 if tf_norm=="15m" else 10
        is_sore=pd.Series([True]*len(df), index=df.index) if tf_norm=="1d" else pd.Series([14*60+30 <= idx.hour*60+idx.minute <= 15*60+45 if isinstance(idx, pd.Timestamp) else True for idx in df.index], index=df.index)
        avgV=volume.rolling(20).mean(); isSurge=volume>(avgV*1.8)
        isUp=(close-open_)/open_.replace(0,1)*100>=0.5; isStrong=close>open_; isLiquid=volume>=2000000; isAbove=close>ema200; isBreak=close>high.shift(1).rolling(lookback).max()
        sig=pd.Series([False]*len(df), index=df.index); markers=[]; dates=set()
        for i in range(len(df)):
            if not is_sore.iloc[i] or not isSurge.iloc[i] or not isUp.iloc[i] or not isStrong.iloc[i] or not isBreak.iloc[i] or not isLiquid.iloc[i] or not isAbove.iloc[i]: continue
            try:
                d=df.index[i].date()
                if d in dates: continue
                dates.add(d)
            except: pass
            sig.iloc[i]=True; markers.append({"idx":i})
        buy=bool(sig.iloc[-1]) if len(sig)>0 else False
        if not bsjp_allowed: buy=False; markers=[]
        return {"buy":buy, "markers":markers, "allowed_tf":bsjp_allowed, "tp1":f_round_ihsg(float(high.iloc[-1]*1.03)), "sl":f_round_ihsg(float(low.iloc[-1]*0.97))}
    except: return {"buy":False, "markers":[], "allowed_tf":False}

def generate_caption_pro(symbol, df, realtime_price=None, tf_norm="1d"):
    try:
        df=df.copy()
        if realtime_price and realtime_price>0 and realtime_price!=100 and len(df)>0:
            last_hist=df['Close'].iloc[-1]
            if last_hist>0 and abs(realtime_price-last_hist)/last_hist < 0.5:
                df.iloc[-1, df.columns.get_loc('Close')] = realtime_price
        last_close=df['Close'].iloc[-1]; prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close
        chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
        rsi=calculate_rsi(df['Close'],14)
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        df,br=calculate_vsa_metrics(df)
        buy_pct=int(br.iloc[-1]*100)
        v1=df['V1'].iloc[-1]; vol_spike=df['Volume'].iloc[-1]/v1 if v1>0 else 1.0
        return f"{symbol} — {int(last_close)} ({chg_pct:+.2f}%) | {format_timeframe_label(tf_norm)}\n├ RSI:{rsi:.1f} Vol:{vol_spike:.1f}x Buy%:{buy_pct}%", {}
    except: return f"{symbol} — 0", {}

def generate_pro_chart(df,symbol="BBCA",timeframe="1d",output_filename="chart.png",extra_info=None,realtime_price=None):
    try:
        extra_info=extra_info or {}
        timeframe_norm=normalize_timeframe(timeframe)
        tf_label_disp=extra_info.get('tf_label') or format_timeframe_label(timeframe_norm)
        df=df.copy().ffill().bfill()
        if not isinstance(df.index,pd.DatetimeIndex): df.index=pd.to_datetime(df.index)
        else: df=df.sort_index()
        df=df.dropna(subset=['Open','High','Low','Close'])
        if len(df)<20: return None, []
        if realtime_price and realtime_price>0 and realtime_price!=100 and len(df)>0:
            last_hist=df['Close'].iloc[-1]
            if last_hist>0 and abs(realtime_price-last_hist)/last_hist < 0.5:
                df.iloc[-1, df.columns.get_loc('Close')] = realtime_price
        df['EMA8']=df['Close'].ewm(span=8,adjust=False).mean(); df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean(); df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean(); df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean(); sma20,upper_bb,lower_bb=calculate_bollinger_bands(df,20,2); df['BB_UP']=upper_bb; df['BB_LOW']=lower_bb; df,buy_ratios=calculate_vsa_metrics(df)
        kim=detect_kim_signal(df, timeframe=timeframe_norm); bsjp=detect_bsjp_signal(df, timeframe=timeframe_norm)
        last_close=df['Close'].iloc[-1]; prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close; chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
        plt.style.use('dark_background'); fig=plt.figure(figsize=(14,8),dpi=150,facecolor='#000000')
        gs=gridspec.GridSpec(4,1,height_ratios=[4.8,1.1,0.9,0.8],hspace=0.05)
        ax_main=fig.add_subplot(gs[0]); ax_vol=fig.add_subplot(gs[1],sharex=ax_main); ax_nbsa=fig.add_subplot(gs[2],sharex=ax_main); ax_mm=fig.add_subplot(gs[3],sharex=ax_main)
        fig.subplots_adjust(left=0.06,right=0.94,top=0.90,bottom=0.05)
        for ax in [ax_main,ax_vol,ax_nbsa,ax_mm]:
            ax.set_facecolor('#000000'); ax.tick_params(colors='#aaaaaa',labelsize=8); ax.yaxis.tick_right(); ax.grid(False)
            for spine in ax.spines.values(): spine.set_color('#333333')
        x=np.arange(len(df))
        for i in range(len(df)):
            o,h,l,c=df['Open'].iloc[i],df['High'].iloc[i],df['Low'].iloc[i],df['Close'].iloc[i]
            ax_main.plot([i,i],[l,h],color='#00ff00' if c>=o else '#ff4444',linewidth=0.7,alpha=0.9)
            body_low=min(o,c); body_h=max(0.3,abs(c-o))
            if c>=o: rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor='none',edgecolor='#00ff00',linewidth=0.7)
            else: rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor='#ff3333',edgecolor='#ff3333',linewidth=0.7)
            ax_main.add_patch(rect)
        ax_main.plot(x,df['EMA8'],color='#ffeb3b',linewidth=1.0); ax_main.plot(x,df['EMA20'],color='#00e6ff',linewidth=1.0)
        if 'b_modal_series' in kim and kim['b_modal_series'] is not None and len(kim['b_modal_series'])==len(df):
            ax_main.plot(x, kim['b_modal_series'], color='#ffffff', linewidth=1.1, alpha=0.6)
        for m in kim.get('markers',[]): ax_main.plot(m['idx'], df['Low'].iloc[m['idx']]*0.97, marker='^', color='#00ff00', markersize=10)
        for m in bsjp.get('markers',[]): ax_main.plot(m['idx'], df['Low'].iloc[m['idx']]*0.96, marker='*', color='#0080ff', markersize=12)
        ax_main.set_xlim(-1,len(df)-1+10); ax_main.set_ylim(df['Low'].min()*0.96, df['High'].max()*1.06)
        fig.text(0.005,0.96,f"{symbol} : {last_close:.0f} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=14,fontweight='bold',ha='left', family='monospace')
        fig.text(0.5,0.96,f"RAFANO V4.36 CLICK FIX",color='white',fontsize=14,fontweight='bold',ha='center')
        ds=df.index[-1].strftime('%d %b %Y %H:%M')
        fig.text(0.99,0.96,f"{tf_label_disp} | {ds}",color='#ffcc00',fontsize=10,ha='right',fontweight='bold')
        plt.savefig(output_filename,dpi=150,bbox_inches='tight',facecolor='#000000'); plt.close('all')
        return output_filename, []
    except Exception as e:
        print(f"Chart err {e}"); import traceback; traceback.print_exc(); return None, []

# ==================== 400 BY VALUE, NOT ALPHABET + CLICK FIX ====================
def get_top_liquid_tickers(n=400):
    print(f"🔍 get_top_liquid_tickers n={n} BY VALUE (volume*close DESC)")
    try:
        screener=get_screener_latest(force_today=False)
        rows=screener.get('rows',[])
        if len(rows)>=50:
            filtered=[]
            for r in rows:
                code=(r.get('stock_code') or "").replace(".JK","").upper()
                if not code or len(code)!=4: continue
                if "-W" in code: continue
                if code in FCA_EXCLUDE: continue
                if code in SUSPEND_CACHE: continue
                close_val=float(r.get('close') or 0)
                vol_val=float(r.get('volume') or 0)
                if close_val < MIN_PRICE: continue
                if close_val==100 and vol_val==0: continue
                if vol_val==0: continue
                r['_value']=vol_val*close_val
                r['_code']=code
                filtered.append(r)
            if len(filtered)>=30:
                rows_sorted=sorted(filtered, key=lambda r: r.get('_value',0), reverse=True)
                final=[]
                for r in rows_sorted[:600]:
                    if len(final)>=n: break
                    code=r['_code']
                    try:
                        hd=get_history_pro(code, 10, "daily")
                        if hd is None or len(hd)<5: continue
                        if float(hd['Close'].iloc[-1]) < MIN_PRICE: continue
                        if hd['Volume'].tail(5).sum()==0: SUSPEND_CACHE[code]=True; continue
                        final.append(code)
                    except: continue
                if len(final)>=n*0.5:
                    print(f"Final BY VALUE {len(final)}: {final[:5]}")
                    return final[:n]
    except Exception as e:
        print(f"get_top_liquid err {e}")
    # Fallback BY LIQUIDITY
    try:
        candidates=[]
        for sym in IDX_FULL[:800]:
            if sym in FCA_EXCLUDE or "-W" in sym or sym in SUSPEND_CACHE: continue
            try:
                hd=get_history_pro(sym, 30, "daily")
                if hd is None or len(hd)<20: continue
                last_close=float(hd['Close'].iloc[-1])
                if last_close < MIN_PRICE: continue
                if hd['Volume'].tail(5).sum()==0: SUSPEND_CACHE[sym]=True; continue
                avg_vol=hd['Volume'].tail(20).mean()
                candidates.append((sym, avg_vol*last_close))
            except: continue
        candidates_sorted=sorted(candidates, key=lambda x: x[1], reverse=True)
        return [c[0] for c in candidates_sorted[:n]]
    except:
        return [c for c in IDX_FULL if c not in FCA_EXCLUDE][:n]

def scan_volume_spike(threshold=1.5, limit_candidates=400):
    tickers=get_top_liquid_tickers(limit_candidates)
    detected=[]
    for sym in tickers:
        try:
            if sym in FCA_EXCLUDE or sym in SUSPEND_CACHE: continue
            hd=get_history_pro(sym,60,"daily")
            if hd is None or len(hd)<20: continue
            last_close=float(hd['Close'].iloc[-1])
            if last_close < MIN_PRICE: continue
            v_last=hd['Volume'].iloc[-1]; v_avg=hd['Volume'].tail(20).mean()
            if v_avg==0: continue
            ratio=v_last/v_avg
            if ratio < threshold: continue
            prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
            chg=(last_close/prev-1)*100 if prev else 0
            detected.append({"symbol":sym,"close":int(last_close),"change_pct":chg,"vol_ratio":ratio,"vol_rp":v_last*last_close})
        except: continue
    detected.sort(key=lambda x: x['vol_ratio'], reverse=True)
    return detected

def scan_kim_bsjp_400():
    tickers=get_top_liquid_tickers(400)
    print(f"🔍 Scan KIM+BSJP 400 BY VALUE: {len(tickers)}")
    detected=[]
    def check_one(sym):
        try:
            if sym in FCA_EXCLUDE or sym in SUSPEND_CACHE: return None
            df_daily=get_history_pro(sym, 150, "1d")
            if df_daily is None or len(df_daily)<80: return None
            if float(df_daily['Close'].iloc[-1]) < MIN_PRICE: return None
            kim=detect_kim_signal(df_daily, timeframe="1d")
            df_15m=get_history_pro(sym, 150, "15m")
            bsjp=detect_bsjp_signal(df_15m if df_15m is not None else df_daily, timeframe="15m" if df_15m is not None else "1d")
            if kim['buy'] or bsjp['buy']:
                last_close=df_daily['Close'].iloc[-1]
                prev=df_daily['Close'].iloc[-2] if len(df_daily)>=2 else last_close
                chg=(last_close/prev-1)*100 if prev else 0
                v_last=df_daily['Volume'].iloc[-1]; v_avg=df_daily['Volume'].tail(20).mean()
                vol_ratio=v_last/v_avg if v_avg else 1
                return {"symbol":sym,"close":int(last_close),"change_pct":chg,"vol_ratio":vol_ratio,"kim_buy":kim['buy'],"bsjp_buy":bsjp['buy'],"kim":kim,"bsjp":bsjp}
        except: return None
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs={ex.submit(check_one,s):s for s in tickers}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)
    detected.sort(key=lambda x: (x['kim_buy'] and x['bsjp_buy'], x['vol_ratio']), reverse=True)
    return detected

def scan_kim_bsjp_300(): return scan_kim_bsjp_400()

# ==================== ROBUST TELEGRAM SEND ====================
def send_reply(cid, txt, rm=None):
    if not TELEGRAM_BOT_TOKEN or not cid:
        print(f"send_reply FAIL no token/cid")
        return False
    try: cid=int(cid)
    except: pass
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    pl={"chat_id":cid,"text":txt,"parse_mode":"Markdown"}
    if rm: pl["reply_markup"]=rm
    try:
        r=requests.post(url,json=pl,timeout=20)
        j=r.json()
        if not j.get('ok'):
            print(f"send_reply MD fail {j}, retry plain")
            pl.pop('parse_mode',None)
            r=requests.post(url,json=pl,timeout=20)
            j2=r.json()
            print(f"retry ok={j2.get('ok')}")
            return j2.get('ok',False)
        return True
    except Exception as e:
        print(f"send_reply EXC {e}")
        return False

def send_photo_reply(cid, path, caption=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try: cid=int(cid)
    except: pass
    try:
        with open(path,'rb') as ph:
            r=requests.post(url,data={'chat_id':cid,'caption':caption,'parse_mode':'Markdown'},files={'photo':ph},timeout=40)
            j=r.json()
            if not j.get('ok'):
                print(f"send_photo MD fail {j}, retry plain")
                with open(path,'rb') as ph2:
                    requests.post(url,data={'chat_id':cid,'caption':caption},files={'photo':ph2},timeout=40)
    except Exception as e:
        print(f"Photo err {e}")

def process_chart_request(cid, code, tf_input="1d"):
    try:
        print(f"=== CHART REQ START {code} {tf_input} -> {cid} ===")
        tf_norm=normalize_timeframe(tf_input)
        tf_label=format_timeframe_label(tf_norm)
        ok=send_reply(cid, f"📊 *{code.upper()} ({tf_label}) chart pro...*")
        print(f"send_reply pro ok={ok}")
        df=get_history_pro(code, 150, frame=tf_norm)
        print(f"get_history rows={len(df) if df is not None else 'None'}")
        if df is None or len(df)<20:
            send_reply(cid, f"⚠ Data {code} TF {tf_norm} kosong (Arjum/YF fail)")
            print(f"CHART ABORT no data {code}")
            return
        stockbit_price,_=get_realtime_stockbit(code)
        realtime=stockbit_price if stockbit_price>=MIN_PRICE else 0
        print(f"realtime={realtime} hist_close={df['Close'].iloc[-1]}")
        chart_file=f"/tmp/chart_{code.upper()}_{tf_norm}_{int(time.time())}.png"
        fp,_=generate_pro_chart(df, symbol=code.upper(), timeframe=tf_norm, output_filename=chart_file, extra_info={'tf_label':tf_label}, realtime_price=realtime)
        print(f"generate_pro_chart fp={fp} exists={os.path.exists(fp) if fp else False}")
        if not fp or not os.path.exists(fp):
            send_reply(cid, f"❌ Gagal render {code} {tf_norm}")
            return
        caption,_=generate_caption_pro(code.upper(), df, realtime_price=realtime, tf_norm=tf_norm)
        print(f"caption {caption[:80]}")
        send_photo_reply(cid, fp, caption=caption)
        try: os.remove(fp)
        except: pass
        print(f"=== CHART DONE {code} ===")
    except Exception as e:
        print(f"CHART ERR {code} {e}")
        import traceback; traceback.print_exc()
        try: send_reply(cid, f"❌ Err chart {code}: {e}")
        except: pass

def broadcast_vol_spike(signals, threshold=1.5, dest_chat_id=None):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target:
        print("broadcast_vol_spike NO TARGET")
        return
    if not signals:
        send_reply(target, f"Vol Spike >{threshold}x (400 BY VALUE): Tidak ada"); return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header=f"*VOL SPIKE >{threshold}x* 🔥 {now} - {len(signals)} saham (TOP 400 BY VALUE: volume*close, no suspend/FCA/price<50)\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Vol {it['vol_ratio']:.1f}x\n"
        cb_data=f"chart_{it['symbol']}"
        kb.append([{"text": f"{it['symbol']} {it['vol_ratio']:.1f}x", "callback_data": cb_data}])
        if len(msg)+len(line)>3500:
            print(f"broadcast chunk {len(kb)} items")
            send_reply(target, msg, rm={"inline_keyboard": kb})
            msg=line; kb=[]
        else: msg+=line
    if msg:
        print(f"broadcast final {len(kb)} items to {target}")
        send_reply(target, msg, rm={"inline_keyboard": kb})

def broadcast_kim_bsjp(signals, dest_chat_id=None, is_auto=False):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target: return
    if not signals:
        if not is_auto: send_reply(target, f"🔍 KIM+BSJP (400 BY VALUE): Tidak ada BUY"); return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    tag="⚡ AUTO" if is_auto else "🚀"
    header=f"{tag} *KIM+BSJP BUY* 🔥 {now}\nTOP 400 BY VALUE - {len(signals)} sinyal\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        both="🔥 KIM+BSJP" if it['kim_buy'] and it['bsjp_buy'] else "KIM BUY" if it['kim_buy'] else "BSJP BUY"
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) {both}\n"
        kb.append([{"text": f"{it['symbol']} {both}", "callback_data": f"chart_{it['symbol']}"}])
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
        else: msg+=line
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

def auto_broadcast_loop():
    print("🔄 Auto broadcaster 400 BY VALUE started")
    while True:
        try:
            if not AUTO_KIM_ENABLED: time.sleep(60); continue
            now=get_now_wib()
            if now.weekday()>=5 or not (9 <= now.hour < 16): time.sleep(300); continue
            sigs=scan_kim_bsjp_400()
            if sigs: broadcast_kim_bsjp(sigs, is_auto=True)
            time.sleep(900)
        except: time.sleep(300)

def telegram_bot_listener():
    offset=0
    print(f"🤖 RAFANO V4.36 CLICK FIX + 400 BY VALUE - {len(IDX_FULL)} IDX")
    try:
        r=requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true",timeout=10)
        print(f"deleteWebhook {r.text[:200]}")
    except Exception as e: print(f"deleteWebhook err {e}")
    threading.Thread(target=auto_broadcast_loop, daemon=True).start()
    while True:
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res=requests.get(url,timeout=25)
            if res.status_code!=200:
                print(f"getUpdates status {res.status_code}")
                time.sleep(3); continue
            data=res.json()
            if not data.get('ok'):
                print(f"getUpdates not ok {data}")
                time.sleep(3); continue
            results=data.get("result",[])
            for update in results:
                offset=update["update_id"]+1
                # ===== CLICK HANDLER SUPER ROBUST =====
                if "callback_query" in update:
                    try:
                        cb=update["callback_query"]
                        qid=cb.get("id")
                        cdata=(cb.get("data","") or "").strip()
                        msg_obj=cb.get("message") or {}
                        chat_obj=msg_obj.get("chat") if isinstance(msg_obj, dict) else {}
                        from_obj=cb.get("from") or {}
                        chat_id= (chat_obj.get("id") if chat_obj else None) or from_obj.get("id") or TARGET_CHAT_ID
                        try: chat_id=int(chat_id)
                        except: pass
                        print(f"🔘 CLICK DETECTED cdata={cdata} chat_id={chat_id} qid={qid}")
                        # wajib answer biar loading hilang
                        try:
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",json={"callback_query_id":qid,"text":f"Loading {cdata}..."},timeout=5)
                        except Exception as e:
                            print(f"answerCallback err {e}")
                        if cdata.startswith("chart_"):
                            sym=cdata[6:].strip().upper()
                            # validasi
                            if not sym or len(sym)>6 or sym in FCA_EXCLUDE:
                                print(f"click invalid sym {sym}")
                                continue
                            print(f"🔘 CHART CLICK {sym} -> {chat_id}")
                            try:
                                send_reply(chat_id, f"🔘 *{sym}* klik diterima - generate chart...")
                            except Exception as e:
                                print(f"ack err {e}")
                            # thread + fallback direct
                            try:
                                t=threading.Thread(target=process_chart_request,args=(chat_id,sym,"1d"),daemon=True)
                                t.start()
                                print(f"thread chart {sym} started")
                            except Exception as e:
                                print(f"thread start err {e}, fallback direct")
                                try:
                                    process_chart_request(chat_id,sym,"1d")
                                except Exception as e2:
                                    print(f"direct err {e2}")
                        else:
                            print(f"unknown callback {cdata}")
                    except Exception as e:
                        print(f"callback handler err {e}")
                        import traceback; traceback.print_exc()
                    continue

                if "message" in update and "text" in update["message"]:
                    txt=update["message"].get("text","").strip()
                    chat_id=update["message"]["chat"]["id"]
                    print(f"💬 MSG {chat_id}: {txt}")
                    first=txt.split()[0].lower() if txt else ""
                    parts=txt.split()
                    if first in ["/start","/help","/menu"]:
                        help_msg=f"""🔥 *RAFANO V4.36 CLICK FIX + 400 BY VALUE*

400 BY VALUE (volume*close) BUKAN ALPHABET
FILTER: no suspend, no FCA, price>=50

*CHART:*
/c KODE 5/15/60/1d

*SCANNER 400 BY VALUE:*
/scanvol 1.5 = vol spike
/kimbsjp = KIM+BSJP
/list400 = list 400 BY VALUE

*AUTOSCAN:*
/auto on/off
/status = cek AUTO

*CLICK: klik tombol hasil scan untuk chart*
"""
                        send_reply(chat_id, help_msg)
                    elif first in ["/quota","/status"]:
                        send_reply(chat_id, f"QUOTA: {'HABIS' if QUOTA_HIT else 'OK'}\nTOP 400 BY VALUE\nSUSPEND cached: {len(SUSPEND_CACHE)}\nAUTO: {'ON' if AUTO_KIM_ENABLED else 'OFF'}")
                    elif first in ["/c","/chart"]:
                        if len(parts)>=2:
                            sym=parts[1].upper(); tf_input=parts[2] if len(parts)>=3 else "1d"
                            threading.Thread(target=process_chart_request,args=(chat_id,sym,tf_input),daemon=True).start()
                        else: send_reply(chat_id, "Pakai: /c ERAA 5 atau /c BBCA")
                    elif first in ["/scanvol","/vol","/vol400"]:
                        try: thr=float(parts[1]) if len(parts)>=2 else 1.5
                        except: thr=1.5
                        send_reply(chat_id, f"🔥 SCAN VOL >{thr}x - 400 BY VALUE...")
                        def run_vol(tg=chat_id, th=thr):
                            sigs=scan_volume_spike(threshold=th, limit_candidates=400)
                            broadcast_vol_spike(sigs, threshold=th, dest_chat_id=tg)
                        threading.Thread(target=run_vol,daemon=True).start()
                    elif first in ["/kim","/kimbsjp","/scan400","/kbs"]:
                        send_reply(chat_id, f"🚀 SCAN 400 BY VALUE...")
                        def run_kim(tg=chat_id):
                            sigs=scan_kim_bsjp_400()
                            broadcast_kim_bsjp(sigs, dest_chat_id=tg)
                        threading.Thread(target=run_kim,daemon=True).start()
                    elif first in ["/list400","/400","/list"]:
                        send_reply(chat_id, f"📋 Generate list 400 BY VALUE...")
                        def run_list(tg=chat_id):
                            tickers=get_top_liquid_tickers(400)
                            msg=f"*TOP 400 BY VALUE* (volume*close DESC, bukan alphabet)\nTotal: {len(tickers)}\n\n" + ", ".join(tickers[:150])
                            send_reply(tg, msg)
                            if len(tickers)>150:
                                send_reply(tg, ", ".join(tickers[150:300]))
                            if len(tickers)>300:
                                send_reply(tg, ", ".join(tickers[300:]))
                        threading.Thread(target=run_list,daemon=True).start()
                    elif first in ["/auto"]:
                        if len(parts)>=2 and parts[1].lower()=="on":
                            AUTO_KIM_ENABLED=True
                            send_reply(chat_id, "✅ Auto ON 400 BY VALUE - scan 15 menit")
                        elif len(parts)>=2 and parts[1].lower()=="off":
                            AUTO_KIM_ENABLED=False
                            send_reply(chat_id, "❌ Auto OFF")
                        else:
                            send_reply(chat_id, f"AUTO: {'ON' if AUTO_KIM_ENABLED else 'OFF'}")
        except Exception as e:
            print(f"Listener err {e}")
            import traceback; traceback.print_exc()
            time.sleep(3)

if __name__=="__main__":
    print(f"🔥 RAFANO V4.36 CLICK FIX + 400 BY VALUE - {len(IDX_FULL)} IDX")
    telegram_bot_listener()
