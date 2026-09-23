"""RAFANO V4.39 FINAL - FULL BMTR CHART + BANDARMOLOGY + WHALE INSTITUSIONAL + INSIDER ACCUM - NO CHANGE OLD SCRIPT"""
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
    return ["BBCA","BBRI","BMRI","BBNI","TLKM","ASII","GOTO","BUKA","BREN","CUAN","WIFI","DEWA","BULL","ERAA","BAIK","BBKP","BRIS","ANTM","INCO","MDKA","ADRO","PTBA","PGAS","EXCL","ISAT","AMRT","TOWR","BBYB","BRMS","BRPT"]

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
        if not m_chg:
            m_chg=re.search(r'"change"\s*:\s*(-?\d+(?:\.\d+)?)', txt)
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
        interval=interval_map.get(frame,"1d")
        period=period_map.get(frame,"6mo")
        hist=yf.Ticker(f"{sym}.JK").history(period=period, interval=interval, timeout=15, auto_adjust=False)
        if hist is not None and len(hist)>=20:
            if frame=="4h":
                hist=hist.resample('4H').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'}).dropna()
            if float(hist['Close'].iloc[-1])<MIN_PRICE: return None
            if hist['Volume'].tail(5).sum()==0: SUSPEND_CACHE[sym]=True; return None
            if hist['Close'].nunique()==1 and int(hist['Close'].iloc[0])==100: return None
            set_cached(hk,hist.tail(limit),HISTORY_CACHE)
            return hist.tail(limit)
    except Exception as e:
        print(f"YF err {sym} {frame}: {e}")
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
    df['Buy_Pct']=br*100
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
        if df is None or len(df)<80:
            return {"buy":False, "b_modal":0, "tp1":0, "tp2":0, "sl":0, "markers":[], "allowed_tf":kim_allowed}
        df=df.copy()
        close=df['Close']; open_=df['Open']; high=df['High']; low=df['Low']; volume=df['Volume']
        ema_f=close.ewm(span=20, adjust=False).mean(); ema_slow=close.ewm(span=200, adjust=False).mean()
        plus_di, minus_di, adx_ser=calc_dmi_adx(df, 14); atr_ser=calc_atr(df, 14)
        v_sma=volume.rolling(20).mean(); is_v_akum=(close>open_) & (volume>v_sma); is_v_spike=volume>(v_sma*1.8)
        hlc3=(high+low+close)/3; vp_cond=(hlc3*volume).where(is_v_akum, 0); v_cond=volume.where(is_v_akum, 0)
        sum_vp=vp_cond.rolling(60, min_periods=1).sum(); sum_v=v_cond.rolling(60, min_periods=1).sum()
        b_modal_raw=np.where(sum_v>0, sum_vp/sum_v, close)
        b_modal_series=pd.Series([f_round_ihsg(x) for x in b_modal_raw], index=df.index)
        b_modal=float(b_modal_series.iloc[-1]) if len(b_modal_series)>0 else float(close.iloc[-1])
        common_ok=(is_v_akum | is_v_spike) & (adx_ser>20) & (atr_ser>atr_ser.rolling(20).mean()) & (close>ema_slow) & (abs((close-b_modal_series)/b_modal_series*100)<=40)
        crossover=(close.shift(1)<=b_modal_series.shift(1)) & (close>b_modal_series)
        raw_b=crossover & (close>ema_f) & common_ok
        re_entry=(close>high.shift(1).rolling(10).max()) & (close>b_modal_series) & common_ok
        buy=bool((raw_b.iloc[-1] or re_entry.iloc[-1])) if len(raw_b)>0 else False
        if not kim_allowed: buy=False
        val_sl=float(low.rolling(20).min().iloc[-1]) if len(low)>=20 else float(low.min())
        ideal_buy=f_round_ihsg(float(close.iloc[-1]))
        atr_val=float(atr_ser.iloc[-1]) if len(atr_ser)>0 and not pd.isna(atr_ser.iloc[-1]) else 0
        t_stop=f_round_ihsg(min(float(close.iloc[-1]) - (atr_val*2.0), val_sl))
        p_range=abs(ideal_buy-val_sl)
        tp1=f_round_ihsg(ideal_buy + p_range*1.0); tp2=f_round_ihsg(ideal_buy + p_range*2.0)
        markers=[]
        if kim_allowed:
            for i in range(len(df)):
                if raw_b.iloc[i]: markers.append({"idx":i, "type":"KIM BUY"})
                elif re_entry.iloc[i]: markers.append({"idx":i, "type":"KIM RE-ENTRY"})
        return {"buy":buy, "b_modal":b_modal, "b_modal_series":b_modal_series, "tp1":tp1, "tp2":tp2, "sl":t_stop, "markers":markers, "allowed_tf":kim_allowed}
    except:
        return {"buy":False, "b_modal":0, "tp1":0, "tp2":0, "sl":0, "markers":[], "allowed_tf":False}

def detect_bsjp_signal(df, timeframe="15m"):
    try:
        tf_norm=normalize_timeframe(timeframe)
        bsjp_allowed=tf_norm in ["5m","15m"]
        if df is None or len(df)<60:
            return {"buy":False, "markers":[], "allowed_tf":bsjp_allowed, "tp1":0, "sl":0}
        df=df.copy()
        close=df['Close']; open_=df['Open']; high=df['High']; low=df['Low']; volume=df['Volume']
        ema200=close.ewm(span=200, adjust=False).mean()
        lookback=4 if tf_norm=="15m" else 10
        def is_time_sore(idx):
            try:
                if isinstance(idx, pd.Timestamp):
                    if tf_norm in ["1d","1w"]: return True
                    return (14*60+30) <= (idx.hour*60+idx.minute) <= (15*60+45)
                return True
            except: return True
        is_time_sore_series=pd.Series([is_time_sore(i) for i in df.index], index=df.index)
        avgVolume=volume.rolling(20).mean(); isVolumeSurge=volume>(avgVolume*1.8)
        priceChangePct=(close-open_)/open_.replace(0,1)*100; isPriceUp=priceChangePct>=0.5
        isStrongCandle=close>open_; isLiquidStock=volume>=2000000; isAboveMajorTrend=close>ema200
        highestHigh=high.shift(1).rolling(lookback).max(); isBreakout=close>highestHigh
        signal_dates=set(); bsjp_series=pd.Series([False]*len(df), index=df.index); markers=[]
        for i in range(len(df)):
            if not is_time_sore_series.iloc[i]: continue
            if not isVolumeSurge.iloc[i]: continue
            if not isPriceUp.iloc[i]: continue
            if not isStrongCandle.iloc[i]: continue
            if not isBreakout.iloc[i]: continue
            if not isLiquidStock.iloc[i]: continue
            if not isAboveMajorTrend.iloc[i]: continue
            try:
                cur_date=df.index[i].date()
                if cur_date in signal_dates: continue
                signal_dates.add(cur_date)
            except: pass
            bsjp_series.iloc[i]=True; markers.append({"idx":i, "type":"BSJP BUY SORE"})
        buy_last=bool(bsjp_series.iloc[-1]) if len(bsjp_series)>0 else False
        if not bsjp_allowed:
            buy_last=False; markers=[]
        tp1=sl=0
        if buy_last:
            tp1=f_round_ihsg(float(high.iloc[-1]*1.03)); sl=f_round_ihsg(float(low.iloc[-1]*0.97))
        return {"buy":buy_last, "markers":markers, "tp1":tp1, "sl":sl, "allowed_tf":bsjp_allowed}
    except:
        return {"buy":False, "markers":[], "allowed_tf":False, "tp1":0, "sl":0}

# CAPTION SIMPLE TANPA KIM/BSJP
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
        df_vsa,buy_ratios=calculate_vsa_metrics(df)
        br_last = buy_ratios.iloc[-1] if hasattr(buy_ratios, 'iloc') else buy_ratios[-1]
        buy_pct=int(br_last*100)
        v1=df['V1'].iloc[-1]; vol_spike=df['Volume'].iloc[-1]/v1 if v1>0 else 1.0
        caption=f"{symbol} — {int(last_close)} ({chg_pct:+.2f}%) | {format_timeframe_label(tf_norm)}\n├ RSI:{rsi:.1f} Vol:{vol_spike:.1f}x Buy%:{buy_pct}%"
        return caption, {}
    except:
        return f"{symbol} — {int(df['Close'].iloc[-1]) if len(df)>0 else 0}", {}

# FULL BMTR CHART RESTORED
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
                if realtime_price>df['High'].iloc[-1]: df.iloc[-1, df.columns.get_loc('High')] = realtime_price
                if realtime_price<df['Low'].iloc[-1]: df.iloc[-1, df.columns.get_loc('Low')] = realtime_price
        df['EMA8']=df['Close'].ewm(span=8,adjust=False).mean()
        df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
        df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
        df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        sma20,upper_bb,lower_bb=calculate_bollinger_bands(df,20,2)
        df['BB_UP']=upper_bb; df['BB_LOW']=lower_bb
        df,buy_ratios=calculate_vsa_metrics(df)
        kim=detect_kim_signal(df, timeframe=timeframe_norm)
        bsjp=detect_bsjp_signal(df, timeframe=timeframe_norm)
        last_close=df['Close'].iloc[-1]; prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close
        chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
        avg_price=df['Close'].tail(20).mean()
        vchg1=(df['Volume'].iloc[-1]/df['Volume'].iloc[-2]) if len(df)>1 and df['Volume'].iloc[-2]>0 else 0
        avg5=df['Volume'].tail(5).mean(); vchg5=(df['Volume'].iloc[-1]/avg5) if avg5>0 else 0
        br_last = buy_ratios.iloc[-1] if hasattr(buy_ratios, 'iloc') else buy_ratios[-1]
        buy_pct_temp=int(br_last*100)
        power="TURBO" if buy_pct_temp>=85 and vchg1>=1.2 else "STRONG" if buy_pct_temp>=70 or vchg1>=1.5 else "NORMAL" if buy_pct_temp>=60 else "WEAK"
        speed="FAST" if vchg1>2.0 else "SLOW" if vchg1<0.8 else "NORMAL"
        ema8=df['EMA8'].iloc[-1]; ema20=df['EMA20'].iloc[-1]; ema50=df['EMA50'].iloc[-1]; ema200=df['EMA200'].iloc[-1]
        if pd.isna(ema200): ema200=ema50
        safety="GOOD" if pd.notna(df['EMA200'].iloc[-1]) and last_close>df['EMA200'].iloc[-1] else "NEUTRAL"
        buy_pct=buy_pct_temp
        net_vol=df['Net_Vol_VSA'].iloc[-1]; net_vol_5d=df['Net_Vol_VSA'].tail(5).sum()

        plt.style.use('dark_background')
        fig=plt.figure(figsize=(14,8),dpi=150,facecolor='#000000')
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
        ax_main.plot(x,df['EMA8'],color='#ffeb3b',linewidth=1.0)
        ax_main.plot(x,df['EMA20'],color='#00e6ff',linewidth=1.0)
        ax_main.plot(x,df['EMA50'],color='#ffffff',linewidth=0.9, alpha=0.8)
        if pd.notna(df['EMA200'].iloc[-1]): ax_main.plot(x,df['EMA200'],color='#a020f0',linewidth=1.4)
        ax_main.plot(x,df['BB_UP'],color='#444488',linewidth=0.8,linestyle='--',alpha=0.5); ax_main.plot(x,df['BB_LOW'],color='#444488',linewidth=0.8,linestyle='--',alpha=0.5)
        if 'b_modal_series' in kim and kim['b_modal_series'] is not None and len(kim['b_modal_series'])==len(df):
            ax_main.plot(x, kim['b_modal_series'], color='#ffffff', linewidth=1.1, linestyle='-', alpha=0.6)
        if kim.get('buy') and kim.get('allowed_tf'):
            ax_main.axhline(kim.get('tp1'), color='#00ff00', linewidth=1, linestyle='--', alpha=0.5)
            ax_main.axhline(kim.get('sl'), color='#ff5500', linewidth=1.2, linestyle='-', alpha=0.7)
        if bsjp.get('buy') and bsjp.get('allowed_tf'):
            ax_main.axhline(bsjp.get('tp1'), color='#00ff00', linewidth=0.8, linestyle=':', alpha=0.5)
            ax_main.axhline(bsjp.get('sl'), color='#ff0000', linewidth=1.2, linestyle='-', alpha=0.7)
        for m in kim.get('markers',[]):
            idx=m['idx']
            if idx>=len(df): continue
            low=df['Low'].iloc[idx]
            ax_main.plot(idx, low*0.97, marker='^', color='#00ff00', markersize=10)
        for m in bsjp.get('markers',[]):
            idx=m['idx']
            if idx>=len(df): continue
            low=df['Low'].iloc[idx]
            ax_main.plot(idx, low*0.96, marker='*', color='#0080ff', markersize=12)
        ax_main.set_xlim(-1,len(df)-1+10); ax_main.set_ylim(df['Low'].min()*0.96, df['High'].max()*1.06)
        left_text=f"Avg Price : {avg_price:.1f}\nVchg 1 Bar: {vchg1:.1f} x\nVchg 5 Bar: {vchg5:.1f} x\nSpeed : {speed}\nPower : {power}\nSafety : {safety}\n\nEMA 8 : {ema8:.1f}\nEMA 20 : {ema20:.1f}\nEMA 50 : {ema50:.1f}\nEMA 200: {ema200:.1f}"
        ax_main.text(0.005,0.98,left_text,transform=ax_main.transAxes,va='top',ha='left',fontsize=6,family='monospace',color='#e0e0e0',bbox=dict(facecolor='black',alpha=0.75,edgecolor='#333333'))
        fig.text(0.005,0.96,f"{symbol} : {last_close:.0f} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=14,fontweight='bold',ha='left', family='monospace')
        fig.text(0.5,0.96,f"RAFANO V4.37 FULL BMTR",color='white',fontsize=14,fontweight='bold',ha='center')
        ds=df.index[-1].strftime('%d %b %Y %H:%M')
        fig.text(0.99,0.96,f"{tf_label_disp} | {ds}",color='#ffcc00',fontsize=10,ha='right',fontweight='bold')
        ax_main.text(len(df)+1, last_close, f" {last_close:.0f}", color='black', fontsize=8, va='center', fontweight='bold', bbox=dict(facecolor='white', edgecolor='none', boxstyle='square,pad=0.2'))
        vol_info=f"Buy % = {buy_pct}% Sell % = {100-buy_pct}% Net Vol = {net_vol:,.0f} 5D = {net_vol_5d:,.0f}"
        ax_vol.text(0.005,0.88,vol_info,transform=ax_vol.transAxes,color='#ffffff',fontsize=7,va='top')
        ax_vol.bar(x,df['Vol_Sell'],color='#cc0000',width=0.8,alpha=0.8); ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00cc00',width=0.8,alpha=0.9)
        ax_vol.plot(x,df['V1'],color='white',linewidth=0.8,alpha=0.9)
        ax_vol.set_ylim(0,df['Volume'].max()*2.2); plt.setp(ax_vol.get_xticklabels(),visible=False)
        ax_nbsa.text(0.005,0.85,f"NBSA Rp. {abs(net_vol*last_close)/1e6:.2f} M",transform=ax_nbsa.transAxes,color='#ffffff',fontsize=7,va='top')
        nbsa_vals=df['Net_Vol_VSA'].tail(120)/(df['Net_Vol_VSA'].abs().max() or 1)*50
        xn=np.arange(len(df)-len(nbsa_vals),len(df))
        for i,v in zip(xn,nbsa_vals): ax_nbsa.bar(i,v,color='#00ffff' if v>=0 else '#ff4444',width=0.6)
        ax_nbsa.axhline(0,color='#444444',linewidth=0.5); ax_nbsa.set_ylim(-60,60)
        ax_mm.text(0.005,0.85,"Market Maker",transform=ax_mm.transAxes,color='#ffffff',fontsize=7,va='top')
        if 'MM' not in df.columns: df['MM']=(df['Close']-df['EMA50'])/df['EMA50']*1000
        mm_vals=df['MM'].tail(120); xm=np.arange(len(df)-len(mm_vals),len(df))
        for i,v in zip(xm,mm_vals): ax_mm.bar(i,v,color='#cccccc' if v>=0 else '#888888',width=0.5,alpha=0.8)
        ax_mm.set_ylim(-30,30)
        step=max(1,len(df)//10); ax_mm.set_xticks(x[::step])
        labels=[df.index[i].strftime('%H:%M') if 'm' in timeframe_norm or 'h' in timeframe_norm else df.index[i].strftime('%d/%b') for i in range(0,len(df),step)]
        ax_mm.set_xticklabels(labels,fontsize=7)
        plt.savefig(output_filename,dpi=150,bbox_inches='tight',facecolor='#000000')
        plt.close('all')
        return output_filename, []
    except Exception as e:
        print(f"Chart error {symbol} {timeframe}: {e}")
        import traceback; traceback.print_exc()
        try:
            if os.path.exists(output_filename): os.remove(output_filename)
        except: pass
        return None, []
    finally:
        try: plt.clf(); plt.close('all')
        except: pass

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

# ==================== BANDARMOLOGY ARJUM API - TOP AKUM/DIST DAILY/WEEKLY - SEPARATE SCAN ====================
# FITUR BARU V4.38 - TIDAK MERUBAH SCRIPT LAIN, HANYA TAMBAHAN

BANDAR_CACHE=OrderedDict()
RETAIL_BROKERS={"YP","CC","NI","PD","OD","XC","XL","DR","CP","MG","AZ","ZP","AK","KK","BK","YU","OD","DX","MI","EP","DP","BQ","SQ","LA","AT","DL","KD","SA","AG","BB","AR","BR","IN","SU","GR","AN","HP","IF","KI","KT","LF","LG","LS","MK","MS","MU","OD","PO","PP","RB","RF","RG","RH","RO","SC","SH","SM","SN","SR","SS","TA","TF","TG","TP","TR","TX","UT","XA","XB","XC","XD","XE","XF","XG","XH","XI","XJ","XK","XL","XM","XN","XO","XP","XQ","XR","XS","XT","XU","XV","XW","XX","XY","XZ","YD","YE","YF","YG","YH","YI","YJ","YK","YL","YM","YN","YO","YP","YQ","YR","YS","YT","YU","YV","YW","YX","YY","YZ","ZA","ZB","ZC","ZD","ZE","ZF","ZG","ZH","ZI","ZJ","ZK","ZL","ZM","ZN","ZO","ZP","ZQ","ZR","ZS","ZT","ZU","ZV","ZW","ZX","ZY","ZZ"}

# Cache untuk broker data
def get_bandar_cached(k, ttl=600):
    return get_cached(k, BANDAR_CACHE, ttl)
def set_bandar_cached(k, d, maxsize=500):
    set_cached(k, d, BANDAR_CACHE, maxsize=maxsize)

def fetch_arjum_broker_data(symbol, period="daily"):
    """
    Coba ambil data bandarmology dari Arjum API dengan beberapa endpoint yang mungkin.
    period: daily / weekly
    Return raw json atau None
    """
    symbol=symbol.upper().strip()
    cache_key=f"bandar_{symbol}_{period}"
    cached=get_bandar_cached(cache_key, ttl=300 if period=="daily" else 600)
    if cached is not None:
        return cached

    # List endpoint yang kemungkinan ada di Arjum (berdasarkan swingara + common pattern)
    endpoints_to_try=[
        f"/broker/{symbol}",
        f"/broker-summary/{symbol}",
        f"/broker_summary/{symbol}",
        f"/brokers/{symbol}",
        f"/bandarmology/{symbol}",
        f"/bandar/{symbol}",
        f"/broker-flow/{symbol}",
        f"/broker-analysis/{symbol}",
        f"/stock/{symbol}/broker",
        f"/stock/{symbol}/bandarmology",
        f"/analysis/broker/{symbol}",
        f"/analysis/bandarmology/{symbol}",
    ]

    # Params yang mungkin
    period_params_list=[
        {"period": period, "timeframe": period},
        {"period": period},
        {"timeframe": period},
        {"days": 5 if period=="weekly" else 1, "period": period},
        {"range": "5d" if period=="weekly" else "1d"},
        {"frame": period},
        {}, # tanpa param
    ]

    for ep in endpoints_to_try:
        for params in period_params_list:
            try:
                data=arjum_get(ep, params=params, bypass_quota=True, retries=0)
                if data is None:
                    continue
                # Validasi ada data broker
                if isinstance(data, dict):
                    # cek beberapa kemungkinan key
                    if any(k in data for k in ["brokers","broker_summary","data","summary","result","broker_data","flow","accumulation","distribution"]):
                        # kalau data kosong skip
                        inner=data.get('data') or data.get('brokers') or data.get('broker_summary') or data.get('summary') or data.get('result') or data.get('broker_data') or []
                        if isinstance(inner, list) and len(inner)==0:
                            continue
                        if isinstance(inner, dict) and len(inner)==0:
                            continue
                        set_bandar_cached(cache_key, data)
                        print(f"✅ Bandar {symbol} {period} via {ep} params={params} OK")
                        return data
                    # kalau dict tapi langsung list broker di dalamnya?
                    if isinstance(data, dict) and len(data)>0:
                        # coba anggap ada data
                        # minimal ada key buy/sell
                        # kita coba return kalau ada field yang mengandung broker code
                        # untuk safety, return jika ada lebih dari 2 keys dan bukan error
                        if "error" not in str(data).lower():
                            # cek apakah ada field seperti YP, CC etc di dalamnya
                            # jika ada, kita anggap valid
                            set_bandar_cached(cache_key, data)
                            return data
                elif isinstance(data, list) and len(data)>0:
                    set_bandar_cached(cache_key, data)
                    print(f"✅ Bandar {symbol} {period} via {ep} list {len(data)} OK")
                    return data
            except Exception as e:
                continue
    # Jika semua endpoint gagal, return None -> fallback ke VSA
    print(f"⚠ Bandar {symbol} {period} Arjum API tidak ada, fallback VSA")
    return None

def parse_broker_data(raw_data, symbol=""):
    """
    Parse raw broker data dari Arjum ke format standar:
    return {
        net_value: total net buy value (positive = akum, negative = distribusi),
        net_volume: total net volume,
        top_buyers: [(broker, net_value), ...] sorted desc,
        top_sellers: [(broker, net_value), ...] sorted asc,
        total_buy_value, total_sell_value,
        institutional_net, retail_net,
        raw_count
    }
    Support banyak format JSON
    """
    try:
        brokers_list=[]
        # Normalisasi raw_data
        if raw_data is None:
            return None
        if isinstance(raw_data, dict):
            # beberapa API bungkus di data
            if 'data' in raw_data and isinstance(raw_data['data'], list):
                brokers_list=raw_data['data']
            elif 'brokers' in raw_data and isinstance(raw_data['brokers'], list):
                brokers_list=raw_data['brokers']
            elif 'broker_summary' in raw_data and isinstance(raw_data['broker_summary'], list):
                brokers_list=raw_data['broker_summary']
            elif 'summary' in raw_data and isinstance(raw_data['summary'], list):
                brokers_list=raw_data['summary']
            elif 'result' in raw_data and isinstance(raw_data['result'], list):
                brokers_list=raw_data['result']
            elif 'broker_data' in raw_data and isinstance(raw_data['broker_data'], list):
                brokers_list=raw_data['broker_data']
            else:
                # coba kalau dict-nya langsung mapping broker_code -> data
                # atau list ada di dalam dict dengan key lain
                # kita coba iterasi values yang mengandung broker
                for k,v in raw_data.items():
                    if isinstance(v, list) and len(v)>0 and isinstance(v[0], dict):
                        # cek apakah ada broker code di dalam
                        sample=v[0]
                        if any(x in str(sample).lower() for x in ["broker","buy","sell","net","code"]):
                            brokers_list=v
                            break
                # kalau masih kosong, coba kalau raw_data sendiri adalah satu broker entry
                if len(brokers_list)==0 and any(x in raw_data for x in ["broker_code","broker","code","buy","sell"]):
                    brokers_list=[raw_data]
        elif isinstance(raw_data, list):
            brokers_list=raw_data

        if len(brokers_list)==0:
            return None

        parsed=[]
        for item in brokers_list:
            if not isinstance(item, dict):
                continue
            # cari broker code
            broker_code=(item.get('broker_code') or item.get('broker') or item.get('code') or item.get('broker_id') or item.get('broker_name') or "").upper().strip()
            if not broker_code or len(broker_code)>4:
                # coba dari key
                for k in item.keys():
                    if k.upper() in ["YP","CC","NI","PD","KZ","BK","AK","MG","YU","DX","XL","XC","OD","ZP","CC","NI"]:
                        broker_code=k.upper()
                        break
            if not broker_code:
                continue
            # cari buy/sell value/volume
            buy_val=float(item.get('buy_value') or item.get('buy') or item.get('buy_val') or item.get('buyValue') or item.get('total_buy_value') or item.get('buy_volume') or item.get('buy_vol') or 0)
            sell_val=float(item.get('sell_value') or item.get('sell') or item.get('sell_val') or item.get('sellValue') or item.get('total_sell_value') or item.get('sell_volume') or item.get('sell_vol') or 0)
            net_val=float(item.get('net_value') or item.get('net') or item.get('net_val') or item.get('netValue') or (buy_val - sell_val) or 0)
            buy_vol=float(item.get('buy_volume') or item.get('buy_vol') or item.get('buy') or 0)
            sell_vol=float(item.get('sell_volume') or item.get('sell_vol') or item.get('sell') or 0)
            net_vol=float(item.get('net_volume') or item.get('net_vol') or (buy_vol - sell_vol) or 0)
            # kalau net masih 0 tapi buy/sell ada, hitung
            if net_val==0 and (buy_val!=0 or sell_val!=0):
                net_val=buy_val - sell_val
            if net_vol==0 and (buy_vol!=0 or sell_vol!=0):
                net_vol=buy_vol - sell_vol
            parsed.append({
                "broker": broker_code,
                "buy_value": buy_val,
                "sell_value": sell_val,
                "net_value": net_val,
                "buy_vol": buy_vol,
                "sell_vol": sell_vol,
                "net_vol": net_vol,
                "is_retail": broker_code in RETAIL_BROKERS
            })

        if len(parsed)==0:
            return None

        total_buy_value=sum(x['buy_value'] for x in parsed)
        total_sell_value=sum(x['sell_value'] for x in parsed)
        total_net_value=sum(x['net_value'] for x in parsed)
        total_net_vol=sum(x['net_vol'] for x in parsed)
        total_buy_vol=sum(x['buy_vol'] for x in parsed)
        total_sell_vol=sum(x['sell_vol'] for x in parsed)

        # kalau total net masih 0, coba pakai buy-sell value
        if total_net_value==0:
            total_net_value=total_buy_value - total_sell_value

        # sort
        top_buyers=sorted([x for x in parsed if x['net_value']>0], key=lambda x: x['net_value'], reverse=True)[:5]
        top_sellers=sorted([x for x in parsed if x['net_value']<0], key=lambda x: x['net_value'])[:5]

        institutional_net=sum(x['net_value'] for x in parsed if not x['is_retail'])
        retail_net=sum(x['net_value'] for x in parsed if x['is_retail'])

        return {
            "net_value": total_net_value,
            "net_volume": total_net_vol,
            "total_buy_value": total_buy_value,
            "total_sell_value": total_sell_value,
            "total_buy_vol": total_buy_vol,
            "total_sell_vol": total_sell_vol,
            "top_buyers": top_buyers,
            "top_sellers": top_sellers,
            "institutional_net": institutional_net,
            "retail_net": retail_net,
            "raw_count": len(parsed),
            "parsed": parsed,
            "symbol": symbol
        }
    except Exception as e:
        print(f"parse_broker_data err {symbol}: {e}")
        return None

def fallback_vsa_bandar(symbol, period="daily"):
    """
    Fallback kalau Arjum bandar API tidak ada / quota habis.
    Pakai logic VSA dari chart kita sendiri:
    - Buy% dari VSA, Net Vol, Volume spike, RSI, EMA200
    - period daily = pakai data daily, weekly = agregasi 5 hari
    Return format mirip parse_broker_data
    """
    try:
        frame="1d" if period=="daily" else "1d" # untuk weekly kita agregasi manual
        df=get_history_pro(symbol, 150 if period=="daily" else 30, frame="1d")
        if df is None or len(df)<20:
            return None
        df['V1']=df['Volume'].rolling(20, min_periods=1).mean()
        df_vsa, buy_ratios=calculate_vsa_metrics(df)
        last_close=df['Close'].iloc[-1]
        prev_close=df['Close'].iloc[-2] if len(df)>=2 else last_close
        chg_pct=(last_close/prev_close-1)*100 if prev_close else 0
        buy_pct=int((buy_ratios.iloc[-1]*100) if hasattr(buy_ratios, 'iloc') else buy_ratios[-1]*100)
        net_vol=df_vsa['Net_Vol_VSA'].iloc[-1]
        vol_ratio=df['Volume'].iloc[-1]/df['V1'].iloc[-1] if df['V1'].iloc[-1]>0 else 1
        rsi=calculate_rsi(df['Close'],14)
        ema200=df['Close'].ewm(span=200, adjust=False).mean().iloc[-1]

        if period=="weekly":
            # agregasi 5 hari
            last5=df.tail(5)
            net_vol_5d=last5['Volume'].sum() * (buy_pct/100 - (100-buy_pct)/100) # approx
            # untuk weekly, pakai sum
            net_vol=last5['Net_Vol_VSA'].sum() if 'Net_Vol_VSA' in last5.columns else net_vol*5
            vol_ratio=last5['Volume'].mean() / df['V1'].iloc[-1] if df['V1'].iloc[-1]>0 else vol_ratio
            chg_pct=(last_close/df['Close'].iloc[-6]-1)*100 if len(df)>=6 else chg_pct

        # logic akum/distribusi sesuai
        # Akum: Buy% >=60, net_vol >0, vol_ratio >=1.2, close > EMA200 (safety GOOD)
        # Dist: Buy% <=40, net_vol <0, vol_ratio tinggi tapi harga turun
        is_akum = (buy_pct>=60 and net_vol>0 and vol_ratio>=1.0 and last_close>ema200 and rsi<75)
        is_dist = (buy_pct<=40 and net_vol<0 and chg_pct<0)

        # net_value estimasi: net_vol * close
        net_value_est=net_vol * last_close

        # buat top buyers/sellers dummy dari VSA
        top_buyers=[{"broker": "BANDAR", "net_value": max(net_value_est,0), "buy_value": max(net_value_est,0)}] if net_value_est>0 else []
        top_sellers=[{"broker": "BANDAR", "net_value": min(net_value_est,0), "sell_value": abs(min(net_value_est,0))}] if net_value_est<0 else []

        return {
            "net_value": net_value_est,
            "net_volume": net_vol,
            "total_buy_value": max(net_value_est,0),
            "total_sell_value": abs(min(net_value_est,0)),
            "total_buy_vol": max(net_vol,0),
            "total_sell_vol": abs(min(net_vol,0)),
            "top_buyers": top_buyers,
            "top_sellers": top_sellers,
            "institutional_net": net_value_est if buy_pct>=60 else 0,
            "retail_net": 0,
            "raw_count": 1,
            "parsed": [],
            "symbol": symbol,
            "fallback": True,
            "buy_pct": buy_pct,
            "vol_ratio": vol_ratio,
            "rsi": rsi,
            "change_pct": chg_pct,
            "is_akum": is_akum,
            "is_dist": is_dist,
            "ema200": ema200,
            "close": last_close
        }
    except Exception as e:
        print(f"fallback_vsa_bandar err {symbol}: {e}")
        return None

def get_bandar_analysis(symbol, period="daily"):
    """
    Gabungan: coba Arjum API dulu, kalau gagal fallback VSA
    Return dict analisis lengkap
    """
    raw=fetch_arjum_broker_data(symbol, period)
    parsed=None
    if raw is not None:
        parsed=parse_broker_data(raw, symbol)
    if parsed is None:
        # fallback
        parsed=fallback_vsa_bandar(symbol, period)
    if parsed is None:
        return None
    # tambahkan logic akum/distribusi sesuai
    try:
        net=parsed.get('net_value',0)
        buy_pct=parsed.get('buy_pct', 50)
        vol_ratio=parsed.get('vol_ratio', 1)
        rsi=parsed.get('rsi', 50)
        # kalau dari Arjum, kita belum ada buy_pct, hitung dari institutional vs retail
        if 'buy_pct' not in parsed:
            total_buy=parsed.get('total_buy_value',0)
            total_sell=parsed.get('total_sell_value',0)
            if total_buy+total_sell>0:
                buy_pct=(total_buy/(total_buy+total_sell))*100
            else:
                buy_pct=50 if net>=0 else 40
            parsed['buy_pct']=buy_pct
        # logic akum sesuai
        # Akum valid: net>0, institutional_net>0, buy_pct>=55, vol_ratio>=1.2, close>=EMA200 (kalau ada)
        is_akum_valid=False
        is_dist_valid=False
        if net>0:
            # minimal net > 500jt untuk daily, >2M untuk weekly
            threshold=500_000_000 if period=="daily" else 2_000_000_000
            if abs(net)>=threshold or parsed.get('fallback'):
                # cek buy_pct
                if buy_pct>=55:
                    is_akum_valid=True
        if net<0:
            threshold=500_000_000 if period=="daily" else 2_000_000_000
            if abs(net)>=threshold or parsed.get('fallback'):
                if buy_pct<=45:
                    is_dist_valid=True
        parsed['is_akum_valid']=is_akum_valid
        parsed['is_dist_valid']=is_dist_valid
        parsed['period']=period
    except Exception as e:
        print(f"get_bandar_analysis logic err {symbol}: {e}")
    return parsed

def scan_bandarmology_top_akum(period="daily", limit_candidates=400, top_n=30, min_net_value=500_000_000):
    """
    Scan top akumulasi bandar
    period: daily / weekly
    logic: net_value >0, is_akum_valid True, sort by net_value DESC
    """
    period=period.lower()
    if period not in ["daily","weekly"]: period="daily"
    tickers=get_top_liquid_tickers(limit_candidates)
    print(f"🔍 SCAN BANDAR TOP AKUM {period.upper()} - {len(tickers)} saham BY VALUE")
    detected=[]
    def check_one(sym):
        try:
            if sym in FCA_EXCLUDE or sym in SUSPEND_CACHE: return None
            # filter harga
            hd=get_history_pro(sym, 20, "1d")
            if hd is None or len(hd)<10: return None
            last_close=float(hd['Close'].iloc[-1])
            if last_close < MIN_PRICE: return None
            if hd['Volume'].tail(5).sum()==0: return None
            analysis=get_bandar_analysis(sym, period)
            if analysis is None: return None
            net=analysis.get('net_value',0)
            if net<=0: return None
            # threshold
            thresh=min_net_value if period=="daily" else min_net_value*4
            if abs(net)<thresh and not analysis.get('fallback'):
                return None
            if not analysis.get('is_akum_valid', False):
                # kalau fallback, pakai is_akum
                if not analysis.get('is_akum', False):
                    return None
            prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
            chg=(last_close/prev-1)*100 if prev else 0
            v_last=hd['Volume'].iloc[-1]; v_avg=hd['Volume'].tail(20).mean()
            vol_ratio=v_last/v_avg if v_avg else 1
            return {
                "symbol": sym,
                "close": int(last_close),
                "change_pct": chg,
                "vol_ratio": vol_ratio,
                "net_value": net,
                "net_volume": analysis.get('net_volume',0),
                "buy_pct": analysis.get('buy_pct',0),
                "top_buyers": analysis.get('top_buyers',[]),
                "institutional_net": analysis.get('institutional_net',0),
                "retail_net": analysis.get('retail_net',0),
                "period": period,
                "analysis": analysis
            }
        except Exception as e:
            print(f"scan akum err {sym}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs={ex.submit(check_one,s):s for s in tickers}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)

    # SORT BY NET_VALUE DESC - PALING BANYAK AKUM DI ATAS
    detected.sort(key=lambda x: x['net_value'], reverse=True)
    return detected[:top_n]

def scan_bandarmology_top_distribusi(period="daily", limit_candidates=400, top_n=30, min_net_value=500_000_000):
    """
    Scan top distribusi bandar
    period: daily / weekly
    logic: net_value <0, is_dist_valid True, sort by net_value ASC (paling negatif di atas)
    """
    period=period.lower()
    if period not in ["daily","weekly"]: period="daily"
    tickers=get_top_liquid_tickers(limit_candidates)
    print(f"🔍 SCAN BANDAR TOP DISTRIBUSI {period.upper()} - {len(tickers)} saham BY VALUE")
    detected=[]
    def check_one(sym):
        try:
            if sym in FCA_EXCLUDE or sym in SUSPEND_CACHE: return None
            hd=get_history_pro(sym, 20, "1d")
            if hd is None or len(hd)<10: return None
            last_close=float(hd['Close'].iloc[-1])
            if last_close < MIN_PRICE: return None
            if hd['Volume'].tail(5).sum()==0: return None
            analysis=get_bandar_analysis(sym, period)
            if analysis is None: return None
            net=analysis.get('net_value',0)
            if net>=0: return None
            thresh=min_net_value if period=="daily" else min_net_value*4
            if abs(net)<thresh and not analysis.get('fallback'):
                return None
            if not analysis.get('is_dist_valid', False):
                if not analysis.get('is_dist', False):
                    return None
            prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
            chg=(last_close/prev-1)*100 if prev else 0
            v_last=hd['Volume'].iloc[-1]; v_avg=hd['Volume'].tail(20).mean()
            vol_ratio=v_last/v_avg if v_avg else 1
            return {
                "symbol": sym,
                "close": int(last_close),
                "change_pct": chg,
                "vol_ratio": vol_ratio,
                "net_value": net,
                "net_volume": analysis.get('net_volume',0),
                "buy_pct": analysis.get('buy_pct',0),
                "top_sellers": analysis.get('top_sellers',[]),
                "institutional_net": analysis.get('institutional_net',0),
                "retail_net": analysis.get('retail_net',0),
                "period": period,
                "analysis": analysis
            }
        except Exception as e:
            print(f"scan dist err {sym}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs={ex.submit(check_one,s):s for s in tickers}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)

    # SORT BY NET_VALUE ASC - PALING BANYAK DISTRIBUSI (negatif terbesar) DI ATAS
    detected.sort(key=lambda x: x['net_value'])
    return detected[:top_n]

def scan_bandarmology_akum_dist_combined(period="daily", limit_candidates=400, top_n=40):
    """
    Scan kombinasi top akum + distribusi berdasarkan logic sesuai
    Return dict {akum: [...], distribusi: [...]}
    Logic sesuai:
    - Akum: net>0, Buy%>=60, Vol>=1.2x, close>EMA200, institutional_net>retail_net, RSI<70
    - Dist: net<0, Buy%<=40, close<EMA20 atau chg negative, retail buy banyak tapi institutional sell
    """
    period=period.lower()
    if period not in ["daily","weekly"]: period="daily"
    tickers=get_top_liquid_tickers(limit_candidates)
    print(f"🔍 SCAN BANDAR AKUM+DISTRIBUSI LOGIC {period.upper()} - {len(tickers)} saham")
    akum_list=[]
    dist_list=[]
    def check_one(sym):
        try:
            if sym in FCA_EXCLUDE or sym in SUSPEND_CACHE: return None
            hd=get_history_pro(sym, 60, "1d")
            if hd is None or len(hd)<30: return None
            last_close=float(hd['Close'].iloc[-1])
            if last_close < MIN_PRICE: return None
            if hd['Volume'].tail(5).sum()==0: return None
            analysis=get_bandar_analysis(sym, period)
            if analysis is None: return None
            net=analysis.get('net_value',0)
            buy_pct=analysis.get('buy_pct',50)
            prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
            chg=(last_close/prev-1)*100 if prev else 0
            v_last=hd['Volume'].iloc[-1]; v_avg=hd['Volume'].tail(20).mean()
            vol_ratio=v_last/v_avg if v_avg else 1
            ema20=hd['Close'].ewm(span=20, adjust=False).mean().iloc[-1]
            ema200=hd['Close'].ewm(span=200, adjust=False).mean().iloc[-1] if len(hd)>=200 else hd['Close'].mean()
            rsi=calculate_rsi(hd['Close'],14)

            # LOGIC AKUM SESUAI
            logic_akum_score=0
            if net>0: logic_akum_score+=1
            if buy_pct>=60: logic_akum_score+=1
            if vol_ratio>=1.2: logic_akum_score+=1
            if last_close>ema200: logic_akum_score+=1
            if rsi<70 and rsi>40: logic_akum_score+=1
            if chg>-2: logic_akum_score+=1 # tidak jatuh dalam

            # LOGIC DISTRIBUSI SESUAI
            logic_dist_score=0
            if net<0: logic_dist_score+=1
            if buy_pct<=40: logic_dist_score+=1
            if chg<0: logic_dist_score+=1
            if last_close<ema20: logic_dist_score+=1
            if rsi>70 or (buy_pct<=45 and vol_ratio>=1.5): logic_dist_score+=1

            base={
                "symbol": sym,
                "close": int(last_close),
                "change_pct": chg,
                "vol_ratio": vol_ratio,
                "net_value": net,
                "buy_pct": buy_pct,
                "rsi": rsi,
                "period": period,
                "analysis": analysis,
                "ema200": ema200,
                "ema20": ema20
            }

            if logic_akum_score>=4 and net>0:
                base['logic_score']=logic_akum_score
                base['type']='AKUM'
                return ('akum', base)
            elif logic_dist_score>=3 and net<0:
                base['logic_score']=logic_dist_score
                base['type']='DISTRIBUSI'
                return ('dist', base)
            else:
                return None
        except Exception as e:
            print(f"scan combined err {sym}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs={ex.submit(check_one,s):s for s in tickers}
        for f in as_completed(futs):
            r=f.result()
            if r:
                typ, data=r
                if typ=='akum':
                    akum_list.append(data)
                else:
                    dist_list.append(data)

    akum_list.sort(key=lambda x: (x['logic_score'], x['net_value']), reverse=True)
    dist_list.sort(key=lambda x: (x['logic_score'], abs(x['net_value'])), reverse=True)

    return {"akum": akum_list[:top_n//2], "distribusi": dist_list[:top_n//2], "period": period}

def broadcast_bandarmology(signals, period="daily", scan_type="akum", dest_chat_id=None):
    """
    Broadcast hasil scan bandarmology dengan tombol chart
    scan_type: akum / distribusi / combined
    """
    target=dest_chat_id or TARGET_CHAT_ID
    if not target:
        print("broadcast_bandarmology NO TARGET")
        return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')

    if scan_type=="akum":
        if not signals:
            send_reply(target, f"🔍 TOP AKUM BANDAR {period.upper()} (400 BY VALUE): Tidak ada akumulasi signifikan"); return
        header=f"*TOP AKUM BANDAR {period.upper()}* 🟢 {now}\nTOP 400 BY VALUE (volume*close) | {len(signals)} saham | Filter: no suspend/FCA/price>=50\nLogic: Net Buy>0, Buy%>=55, Institutional > Retail, Vol>=1x\n\n"
        msg=header; kb=[]
        for idx,it in enumerate(signals,1):
            net_m=it['net_value']/1e9
            line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Net {net_m:+.2f}B Buy%{it['buy_pct']:.0f}% Vol{it['vol_ratio']:.1f}x\n"
            # ambil top buyer
            top_b=""
            if it.get('top_buyers'):
                tb=it['top_buyers'][0]
                top_b=f" Top:{tb.get('broker','')}"
            line_top=f"   └{top_b}\n" if top_b else ""
            full_line=line+line_top
            kb.append([{"text": f"{it['symbol']} +{net_m:.1f}B", "callback_data": f"chart_{it['symbol']}"}])
            if len(msg)+len(full_line)>3500:
                send_reply(target, msg, rm={"inline_keyboard": kb}); msg=full_line; kb=[]
            else: msg+=full_line
        if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

    elif scan_type=="distribusi":
        if not signals:
            send_reply(target, f"🔍 TOP DISTRIBUSI BANDAR {period.upper()} (400 BY VALUE): Tidak ada distribusi signifikan"); return
        header=f"*TOP DISTRIBUSI BANDAR {period.upper()}* 🔴 {now}\nTOP 400 BY VALUE | {len(signals)} saham\nLogic: Net Sell<0, Buy%<=45, Chg negative / <EMA20\n\n"
        msg=header; kb=[]
        for idx,it in enumerate(signals,1):
            net_m=it['net_value']/1e9
            line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Net {net_m:.2f}B Buy%{it['buy_pct']:.0f}% Vol{it['vol_ratio']:.1f}x\n"
            top_s=""
            if it.get('top_sellers'):
                ts=it['top_sellers'][0]
                top_s=f" Top:{ts.get('broker','')}"
            line_top=f"   └{top_s}\n" if top_s else ""
            full_line=line+line_top
            kb.append([{"text": f"{it['symbol']} {net_m:.1f}B", "callback_data": f"chart_{it['symbol']}"}])
            if len(msg)+len(full_line)>3500:
                send_reply(target, msg, rm={"inline_keyboard": kb}); msg=full_line; kb=[]
            else: msg+=full_line
        if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

    elif scan_type=="combined":
        akum=signals.get('akum',[]) if isinstance(signals, dict) else []
        distribusi=signals.get('distribusi',[]) if isinstance(signals, dict) else []
        period_sig=signals.get('period', period) if isinstance(signals, dict) else period
        if not akum and not distribusi:
            send_reply(target, f"🔍 AKUM+DISTRIBUSI {period_sig.upper()}: Tidak ada sinyal valid"); return
        # broadcast akum dulu
        if akum:
            header=f"*AKUM BANDAR {period_sig.upper()} - LOGIC SESUAI* 🟢 {now}\n{len(akum)} saham akumulasi valid\nLogic: Net>0, Buy%>=60, Vol>=1.2x, >EMA200, RSI 40-70\n\n"
            msg=header; kb=[]
            for idx,it in enumerate(akum,1):
                net_m=it['net_value']/1e9
                line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Score{it.get('logic_score',0)}/6 Net {net_m:+.2f}B\n"
                kb.append([{"text": f"{it['symbol']} AKUM {net_m:.1f}B", "callback_data": f"chart_{it['symbol']}"}])
                if len(msg)+len(line)>3500:
                    send_reply(target, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
                else: msg+=line
            if msg: send_reply(target, msg, rm={"inline_keyboard": kb})
        if distribusi:
            header2=f"*DISTRIBUSI BANDAR {period_sig.upper()} - LOGIC SESUAI* 🔴 {now}\n{len(distribusi)} saham distribusi valid\nLogic: Net<0, Buy%<=40, <EMA20 atau chg negative\n\n"
            msg=header2; kb=[]
            for idx,it in enumerate(distribusi,1):
                net_m=it['net_value']/1e9
                line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Score{it.get('logic_score',0)}/5 Net {net_m:.2f}B\n"
                kb.append([{"text": f"{it['symbol']} DIST {net_m:.1f}B", "callback_data": f"chart_{it['symbol']}"}])
                if len(msg)+len(line)>3500:
                    send_reply(target, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
                else: msg+=line
            if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

def get_bandar_detail_text(symbol, period="daily"):
    """
    Untuk /bandar SYMBOL - detail bandarmology satu saham
    """
    analysis=get_bandar_analysis(symbol, period)
    if analysis is None:
        return f"⚠ Data bandar {symbol.upper()} {period} tidak tersedia (Arjum API gagal & VSA fallback gagal)"
    net=analysis.get('net_value',0)
    net_m=net/1e9
    buy_pct=analysis.get('buy_pct',0)
    top_buyers=analysis.get('top_buyers',[])
    top_sellers=analysis.get('top_sellers',[])
    buy_vol=analysis.get('total_buy_vol',0) or analysis.get('total_buy_value',0)
    sell_vol=analysis.get('total_sell_vol',0) or analysis.get('total_sell_value',0)
    is_fallback=analysis.get('fallback', False)
    source="VSA Fallback" if is_fallback else "Arjum API"

    text=f"*BANDARMOLOGY {symbol.upper()} {period.upper()}* - {source}\n"
    text+=f"Net: {net_m:+.2f}B | Buy%: {buy_pct:.0f}% | Sell%: {100-buy_pct:.0f}%\n"
    if top_buyers:
        text+=f"\n*Top Buyers:*\n"
        for tb in top_buyers[:3]:
            tb_net=tb.get('net_value',0)/1e9
            text+=f"  {tb.get('broker','?')} {tb_net:+.2f}B\n"
    if top_sellers:
        text+=f"\n*Top Sellers:*\n"
        for ts in top_sellers[:3]:
            ts_net=ts.get('net_value',0)/1e9
            text+=f"  {ts.get('broker','?')} {ts_net:.2f}B\n"

    # logic valid?
    if analysis.get('is_akum_valid') or analysis.get('is_akum'):
        text+=f"\n✅ *AKUMULASI VALID* - Bandar akum"
    elif analysis.get('is_dist_valid') or analysis.get('is_dist'):
        text+=f"\n🔴 *DISTRIBUSI VALID* - Bandar distribusi"
    else:
        text+=f"\n⚪ NETRAL - Tidak ada akum/dist signifikan"

    # tambahkan logic score kalau ada
    if 'logic_score' in analysis:
        text+=f"\nScore: {analysis.get('logic_score')}/6"

    return text

# ==================== WHALE & INSIDER ACCUMULATION - BARU, TIDAK MERUBAH SCRIPT LAIN ====================
# SCANNER TERPISAH UNTUK AKUM INSTITUSIONAL (WHALE) & AKUM INSIDER

# Broker yang dikenal sebagai institusional besar / foreign / whale
WHALE_BROKERS={"KZ","BK","AK","ZP","YU","RX","DB","ML","UB","CS","MS","JP","AG","BB","CC","NI","PD","BK","KI","AI","CP","DX","DR","XL","XC","OD","YP","CC","NI","KZ","AK","BK","YU","ZP","KZ","LG","RG","CC","NI","PD","BK","KI","AK","MG","YU","DX","XL","XC","OD","ZP","BK","YU","CC","NI","KZ","AK","BK","YU","ZP"}
# Sebenarnya semua broker bisa jadi whale kalau net value besar, tapi kita definisikan institutional core
INSTITUTIONAL_CORE={"KZ","BK","AK","ZP","YU","DB","ML","UB","CS","MS","JP","RX","LG","RG","AG","BB","KI","AI","CP","DX","OD","XC","XL","YP"}

# Untuk insider, broker yang sering dipakai untuk stealth accumulation di bottom
INSIDER_BROKERS_HINT={"CC","NI","PD","YP","OD","XC","XL","DR","CP","MG","AZ","ZP","AK","KK","BK","YU","DX","LG","RG","CC","NI"}

def is_whale_net_value(net_value, period="daily"):
    """Threshold whale: >5B daily, >20B weekly"""
    if period=="daily":
        return abs(net_value) >= 5_000_000_000
    else:
        return abs(net_value) >= 20_000_000_000

def is_insider_pattern(df, analysis):
    """
    Logic insider:
    - Akumulasi diam-diam di bottom / sideways
    - Harga dekat Low 20 hari, tidak naik banyak
    - Buy% tinggi >=65% tapi change% kecil (0-2%)
    - Volume tidak spike besar (0.8x - 1.8x) -> stealth
    - Net positif tapi tidak terlalu besar (500jt - 5B daily) -> bukan whale
    - RSI 35-60 (belum overbought)
    - Close masih di bawah EMA50 atau di sekitar EMA50 (belum markup)
    """
    try:
        if df is None or len(df)<30:
            return False, 0
        last_close=float(df['Close'].iloc[-1])
        low_20=float(df['Low'].tail(20).min())
        high_20=float(df['High'].tail(20).max())
        # posisi harga di range 20 hari (0 = low, 1 = high)
        range_pos=(last_close - low_20)/(high_20 - low_20) if (high_20 - low_20)>0 else 0.5
        buy_pct=analysis.get('buy_pct',50)
        net=analysis.get('net_value',0)
        vol_ratio=analysis.get('vol_ratio', analysis.get('analysis',{}).get('vol_ratio',1) if isinstance(analysis.get('analysis'),dict) else 1)
        # ambil dari fallback jika ada
        if 'vol_ratio' not in analysis and 'analysis' in analysis:
            vol_ratio=analysis['analysis'].get('vol_ratio',1) if isinstance(analysis['analysis'],dict) else 1
        # coba hitung vol_ratio dari df kalau tidak ada
        if vol_ratio==1:
            try:
                v1=df['Volume'].rolling(20).mean().iloc[-1]
                vol_ratio=df['Volume'].iloc[-1]/v1 if v1>0 else 1
            except:
                vol_ratio=1
        prev=df['Close'].iloc[-2] if len(df)>=2 else last_close
        chg=(last_close/prev-1)*100 if prev else 0
        ema50=df['Close'].ewm(span=50, adjust=False).mean().iloc[-1]
        rsi=calculate_rsi(df['Close'],14)
        # scoring insider
        score=0
        if 0.0 <= range_pos <= 0.4: score+=2  # dekat bottom
        elif 0.4 < range_pos <= 0.6: score+=1
        if 55 <= buy_pct <= 85: score+=1
        if 0.7 <= vol_ratio <= 1.8: score+=1  # stealth, tidak spike
        if -1 <= chg <= 2.5: score+=1  # harga belum naik
        if 30 <= rsi <= 62: score+=1
        if last_close <= ema50*1.05: score+=1  # masih di bawah/ sekitar EMA50
        if 500_000_000 <= net <= 7_000_000_000: score+=1  # akum kecil-menengah
        # minimal score 5 dari 8 untuk insider valid
        return (score>=5), score
    except Exception as e:
        print(f"is_insider_pattern err: {e}")
        return False, 0

def is_whale_pattern(df, analysis, period="daily"):
    """
    Logic whale / institusional:
    - Net value sangat besar >5B daily / >20B weekly
    - Buy% >=70%
    - Institutional net > Retail net
    - Top buyer adalah broker institutional core (KZ, BK, AK, ZP, YU, DB, dll)
    - Vol ratio >=1.5x (ada minat besar)
    - Close > EMA200 (safety GOOD) atau minimal >EMA50
    - RSI 45-75 (tidak oversold, masih kuat)
    - Harga tidak di pucuk (range_pos <0.85)
    """
    try:
        if df is None or len(df)<30:
            return False, 0
        net=analysis.get('net_value',0)
        if not is_whale_net_value(net, period):
            return False, 0
        buy_pct=analysis.get('buy_pct',50)
        institutional_net=analysis.get('institutional_net',0)
        retail_net=analysis.get('retail_net',0)
        top_buyers=analysis.get('top_buyers',[])
        last_close=float(df['Close'].iloc[-1])
        low_20=float(df['Low'].tail(20).min())
        high_20=float(df['High'].tail(20).max())
        range_pos=(last_close - low_20)/(high_20 - low_20) if (high_20 - low_20)>0 else 0.5
        vol_ratio=analysis.get('vol_ratio',1)
        if vol_ratio==1:
            try:
                v1=df['Volume'].rolling(20).mean().iloc[-1]
                vol_ratio=df['Volume'].iloc[-1]/v1 if v1>0 else 1
            except:
                vol_ratio=1
        ema200=df['Close'].ewm(span=200, adjust=False).mean().iloc[-1] if len(df)>=200 else df['Close'].mean()
        ema50=df['Close'].ewm(span=50, adjust=False).mean().iloc[-1]
        rsi=calculate_rsi(df['Close'],14)
        prev=df['Close'].iloc[-2] if len(df)>=2 else last_close
        chg=(last_close/prev-1)*100 if prev else 0

        score=0
        if net>0: score+=1
        if buy_pct>=70: score+=2
        elif buy_pct>=60: score+=1
        if institutional_net>retail_net and institutional_net>0: score+=1
        # top buyer institutional core?
        if top_buyers:
            tb0=top_buyers[0].get('broker','') if isinstance(top_buyers[0], dict) else str(top_buyers[0])
            if tb0 in INSTITUTIONAL_CORE: score+=2
            elif tb0 not in RETAIL_BROKERS: score+=1
        if vol_ratio>=1.5: score+=1
        if last_close>ema200: score+=1
        if last_close>ema50: score+=1
        if 40 <= rsi <= 75: score+=1
        if range_pos < 0.85: score+=1
        if chg>=-1: score+=1  # tidak distribusi

        # minimal 7 dari 11 untuk whale valid
        return (score>=7), score
    except Exception as e:
        print(f"is_whale_pattern err: {e}")
        return False, 0

def scan_whale_accum(period="daily", limit_candidates=400, top_n=30):
    """
    Scan akum institusional / Whale accum
    period: daily / weekly
    logic: net_value besar >5B daily, Buy%>=65, institutional>retail, close>EMA200, vol>=1.2x
    """
    period=period.lower()
    if period not in ["daily","weekly"]: period="daily"
    tickers=get_top_liquid_tickers(limit_candidates)
    print(f"🔍 SCAN WHALE ACCUM {period.upper()} - {len(tickers)} saham BY VALUE")
    detected=[]
    def check_one(sym):
        try:
            if sym in FCA_EXCLUDE or sym in SUSPEND_CACHE: return None
            hd=get_history_pro(sym, 60, "1d")
            if hd is None or len(hd)<30: return None
            last_close=float(hd['Close'].iloc[-1])
            if last_close < MIN_PRICE: return None
            if hd['Volume'].tail(5).sum()==0: return None
            analysis=get_bandar_analysis(sym, period)
            if analysis is None: return None
            # harus akum
            if analysis.get('net_value',0) <=0: return None
            is_whale, score=is_whale_pattern(hd, analysis, period)
            if not is_whale: return None
            prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
            chg=(last_close/prev-1)*100 if prev else 0
            v_last=hd['Volume'].iloc[-1]; v_avg=hd['Volume'].tail(20).mean()
            vol_ratio=v_last/v_avg if v_avg else 1
            return {
                "symbol": sym,
                "close": int(last_close),
                "change_pct": chg,
                "vol_ratio": vol_ratio,
                "net_value": analysis.get('net_value',0),
                "buy_pct": analysis.get('buy_pct',0),
                "top_buyers": analysis.get('top_buyers',[]),
                "institutional_net": analysis.get('institutional_net',0),
                "retail_net": analysis.get('retail_net',0),
                "whale_score": score,
                "period": period,
                "analysis": analysis,
                "rsi": calculate_rsi(hd['Close'],14)
            }
        except Exception as e:
            print(f"scan whale err {sym}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs={ex.submit(check_one,s):s for s in tickers}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)

    detected.sort(key=lambda x: (x['whale_score'], x['net_value']), reverse=True)
    return detected[:top_n]

def scan_insider_accum(period="daily", limit_candidates=400, top_n=30):
    """
    Scan akum insider (stealth accumulation di bottom)
    period: daily / weekly
    logic: dekat low 20d, Buy% 55-85, vol stealth 0.7-1.8x, chg kecil, RSI 30-62, close <=EMA50*1.05
    """
    period=period.lower()
    if period not in ["daily","weekly"]: period="daily"
    tickers=get_top_liquid_tickers(limit_candidates)
    print(f"🔍 SCAN INSIDER ACCUM {period.upper()} - {len(tickers)} saham BY VALUE")
    detected=[]
    def check_one(sym):
        try:
            if sym in FCA_EXCLUDE or sym in SUSPEND_CACHE: return None
            hd=get_history_pro(sym, 60, "1d")
            if hd is None or len(hd)<30: return None
            last_close=float(hd['Close'].iloc[-1])
            if last_close < MIN_PRICE: return None
            if hd['Volume'].tail(5).sum()==0: return None
            analysis=get_bandar_analysis(sym, period)
            if analysis is None: return None
            if analysis.get('net_value',0) <=0: return None
            # filter net tidak terlalu besar (insider bukan whale)
            net=analysis.get('net_value',0)
            if period=="daily" and net>8_000_000_000: return None
            if period=="weekly" and net>30_000_000_000: return None
            is_insider, score=is_insider_pattern(hd, analysis)
            if not is_insider: return None
            prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
            chg=(last_close/prev-1)*100 if prev else 0
            v_last=hd['Volume'].iloc[-1]; v_avg=hd['Volume'].tail(20).mean()
            vol_ratio=v_last/v_avg if v_avg else 1
            low_20=float(hd['Low'].tail(20).min())
            return {
                "symbol": sym,
                "close": int(last_close),
                "change_pct": chg,
                "vol_ratio": vol_ratio,
                "net_value": net,
                "buy_pct": analysis.get('buy_pct',0),
                "top_buyers": analysis.get('top_buyers',[]),
                "insider_score": score,
                "period": period,
                "analysis": analysis,
                "low_20": low_20,
                "range_pos": (last_close - low_20)/(float(hd['High'].tail(20).max()) - low_20) if (float(hd['High'].tail(20).max()) - low_20)>0 else 0.5,
                "rsi": calculate_rsi(hd['Close'],14)
            }
        except Exception as e:
            print(f"scan insider err {sym}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs={ex.submit(check_one,s):s for s in tickers}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)

    detected.sort(key=lambda x: (x['insider_score'], x['buy_pct'], x['net_value']), reverse=True)
    return detected[:top_n]

def broadcast_whale_accum(signals, period="daily", dest_chat_id=None):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target:
        print("broadcast_whale NO TARGET")
        return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    if not signals:
        send_reply(target, f"🐋 WHALE ACCUM {period.upper()} (400 BY VALUE): Tidak ada akum institusional signifikan\nLogic: Net>5B daily / >20B weekly, Buy%>=70%, Institutional>Retail, >EMA200, Vol>=1.5x"); return
    header=f"*WHALE ACCUM - AKUM INSTITUSIONAL {period.upper()}* 🐋 {now}\nTOP 400 BY VALUE | {len(signals)} saham | Filter: no suspend/FCA/price>=50\nLogic: Net>5B D / >20B W, Buy%>=70%, Inst>Retail, Top buyer institusi (KZ/BK/AK/ZP/YU/DB), Vol>=1.5x, >EMA200, RSI 40-75, belum pucuk\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        net_m=it['net_value']/1e9
        tb=it['top_buyers'][0].get('broker','') if it.get('top_buyers') else '-'
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Score{it.get('whale_score',0)}/11 Net {net_m:+.2f}B Buy%{it['buy_pct']:.0f}% Top:{tb} Vol{it['vol_ratio']:.1f}x RSI{it.get('rsi',0):.0f}\n"
        kb.append([{"text": f"{it['symbol']} 🐋 {net_m:.1f}B", "callback_data": f"chart_{it['symbol']}"}])
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
        else: msg+=line
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

def broadcast_insider_accum(signals, period="daily", dest_chat_id=None):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target:
        print("broadcast_insider NO TARGET")
        return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    if not signals:
        send_reply(target, f"🕵️ INSIDER ACCUM {period.upper()} (400 BY VALUE): Tidak ada akum insider signifikan\nLogic: Dekat low 20d, Buy%55-85, Vol stealth 0.7-1.8x, Chg -1~2.5%, RSI 30-62, Close<=EMA50*1.05, Net 0.5-7B"); return
    header=f"*INSIDER ACCUM - AKUM DIAM-DIAM {period.upper()}* 🕵️ {now}\nTOP 400 BY VALUE | {len(signals)} saham\nLogic: Dekat low 20d (range 0-40%), Buy%55-85, Vol stealth 0.7-1.8x, Chg -1~2.5%, RSI 30-62, Close<=EMA50, Net 0.5-7B daily (stealth)\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        net_m=it['net_value']/1e9
        rp=it.get('range_pos',0)*100
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Score{it.get('insider_score',0)}/8 Net {net_m:+.2f}B Buy%{it['buy_pct']:.0f}% Low20:{it.get('low_20',0):.0f} Pos{rp:.0f}% Vol{it['vol_ratio']:.1f}x\n"
        kb.append([{"text": f"{it['symbol']} 🕵️ {rp:.0f}%", "callback_data": f"chart_{it['symbol']}"}])
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
        else: msg+=line
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

# ==================== END WHALE & INSIDER ====================

# ==================== END BANDARMOLOGY ====================

def send_reply(cid, txt, rm=None):
    if not TELEGRAM_BOT_TOKEN or not cid:
        print(f"send_reply FAIL no token/cid cid={cid}")
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
            print(f"send_reply MD fail {j.get('description')} retry plain")
            pl.pop('parse_mode',None)
            r=requests.post(url,json=pl,timeout=20)
            j2=r.json()
            print(f"retry ok={j2.get('ok')} desc={j2.get('description')}")
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
                print(f"send_photo MD fail {j.get('description')} retry plain")
                with open(path,'rb') as ph2:
                    requests.post(url,data={'chat_id':cid,'caption':caption},files={'photo':ph2},timeout=40)
            else:
                print(f"send_photo ok {path}")
    except Exception as e:
        print(f"Photo err {e}")

def process_chart_request(cid, code, tf_input="1d"):
    try:
        print(f"=== CHART REQ START {code} {tf_input} -> {cid} ===")
        tf_norm=normalize_timeframe(tf_input)
        tf_label=format_timeframe_label(tf_norm)
        # ack first
        send_reply(cid, f"📊 *{code.upper()} ({tf_label}) chart pro...*")
        df=get_history_pro(code, 150, frame=tf_norm)
        print(f"get_history {code} {tf_norm} rows={len(df) if df is not None else 'None'}")
        if df is None or len(df)<20:
            send_reply(cid, f"⚠ Data {code} TF {tf_norm} kosong (Arjum/YF fail) - coba TF lain")
            print(f"CHART ABORT no data {code}")
            return
        stockbit_price,_=get_realtime_stockbit(code)
        realtime=stockbit_price if stockbit_price>=MIN_PRICE else 0
        print(f"realtime={realtime} hist={df['Close'].iloc[-1]}")
        chart_file=f"/tmp/chart_{code.upper()}_{tf_norm}_{int(time.time())}.png"
        fp,_=generate_pro_chart(df, symbol=code.upper(), timeframe=tf_norm, output_filename=chart_file, extra_info={'tf_label':tf_label}, realtime_price=realtime)
        print(f"generate_pro_chart fp={fp} exists={os.path.exists(fp) if fp else False}")
        if not fp or not os.path.exists(fp):
            send_reply(cid, f"❌ Gagal render {code} {tf_norm} - file tidak ada")
            return
        caption,_=generate_caption_pro(code.upper(), df, realtime_price=realtime, tf_norm=tf_norm)
        print(f"caption {caption[:100]} -> send photo to {cid}")
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
        print(f"broadcast final {len(kb)} items to {target} inline_keyboard={len(kb)}")
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
    print(f"🤖 RAFANO V4.39 FULL BMTR + WHALE + INSIDER - {len(IDX_FULL)} IDX")
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
                # ===== CLICK HANDLER SUPER ROBUST V4.37 =====
                if "callback_query" in update:
                    try:
                        cb=update["callback_query"]
                        qid=cb.get("id")
                        cdata=(cb.get("data","") or "").strip()
                        msg_obj=cb.get("message") or {}
                        chat_obj=msg_obj.get("chat") if isinstance(msg_obj, dict) else {}
                        from_obj=cb.get("from") or {}
                        # 3 sumber chat_id
                        chat_id= (chat_obj.get("id") if chat_obj else None) or from_obj.get("id") or TARGET_CHAT_ID
                        try: chat_id=int(chat_id)
                        except: pass
                        print(f"🔘 CLICK DETECTED cdata={cdata} chat_id={chat_id} qid={qid} full_cb={str(cb)[:500]}")
                        # wajib answer biar loading hilang + log
                        try:
                            ans=requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",json={"callback_query_id":qid,"text":f"Loading {cdata}..."},timeout=5)
                            print(f"answerCallback ok={ans.json().get('ok')}")
                        except Exception as e:
                            print(f"answerCallback err {e}")
                        if cdata.startswith("chart_"):
                            sym=cdata[6:].strip().upper()
                            if not sym or len(sym)>6 or sym in FCA_EXCLUDE:
                                print(f"click invalid sym {sym}")
                                continue
                            if sym in SUSPEND_CACHE:
                                send_reply(chat_id, f"⚠ {sym} terdeteksi suspend, chart mungkin kosong")
                            print(f"🔘 CHART CLICK {sym} -> {chat_id}")
                            try:
                                send_reply(chat_id, f"🔘 *{sym}* klik diterima - generate chart...")
                            except Exception as e:
                                print(f"ack err {e}")
                            try:
                                t=threading.Thread(target=process_chart_request,args=(chat_id,sym,"1d"),daemon=True)
                                t.start()
                                print(f"thread chart {sym} started tid={t.ident}")
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
                        help_msg=f"""🔥 *RAFANO V4.39 FULL BMTR + WHALE & INSIDER*

400 BY VALUE (volume*close) BUKAN ALPHABET
FILTER: no suspend, no FCA, price>=50

*CHART FULL BMTR (TETAP):*
/c KODE 5/15/60/1d = chart pro 4 panel

*SCANNER 400 BY VALUE (TETAP):*
/scanvol 1.5 = vol spike
/kimbsjp = KIM+BSJP
/list400 = list 400 BY VALUE

*🆕 BANDARMOLOGY ARJUM API - TERPISAH (TETAP):*
/topakum daily / weekly
/topdist daily / weekly
/akumdist daily / weekly
/bandar SYMBOL daily/weekly
/bandar daily / weekly

*🐋 WHALE & 🕵️ INSIDER - BARU:*
/whale daily = Akum institusional whale daily (>5B)
/whale weekly = Whale weekly (>20B)
/topwhale daily 30 = Top 30 whale
/insider daily = Akum insider stealth di bottom
/insider weekly = Insider weekly
/topinsider daily = Top insider
/whaleinsider daily = Whale+Insider combined

*AUTOSCAN:*
/auto on/off
/status

*CLICK: klik tombol hasil scan untuk chart langsung generate*
"""
                        send_reply(chat_id, help_msg)
                    elif first in ["/quota","/status"]:
                        send_reply(chat_id, f"QUOTA: {'HABIS' if QUOTA_HIT else 'OK'}\nTOP 400 BY VALUE (volume*close)\nSUSPEND cached: {len(SUSPEND_CACHE)}\nBANDAR cached: {len(BANDAR_CACHE)}\nAUTO: {'ON' if AUTO_KIM_ENABLED else 'OFF'}")
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
                    # ==================== BANDARMOLOGY COMMANDS - BARU, TIDAK MERUBAH YANG LAMA ====================
                    elif first in ["/topakum","/akum","/top_akum","/akumulasi"]:
                        # format: /topakum daily [top_n] atau /topakum weekly
                        period="daily"
                        top_n=30
                        if len(parts)>=2:
                            if parts[1].lower() in ["daily","weekly","d","w","1d","1w"]:
                                period="daily" if parts[1].lower() in ["daily","d","1d"] else "weekly"
                                if len(parts)>=3:
                                    try: top_n=int(parts[2])
                                    except: top_n=30
                            else:
                                try: top_n=int(parts[1])
                                except: top_n=30
                        send_reply(chat_id, f"🟢 SCAN TOP AKUM BANDAR {period.upper()} - 400 BY VALUE...")
                        def run_topakum(tg=chat_id, per=period, tn=top_n):
                            sigs=scan_bandarmology_top_akum(period=per, limit_candidates=400, top_n=tn)
                            broadcast_bandarmology(sigs, period=per, scan_type="akum", dest_chat_id=tg)
                        threading.Thread(target=run_topakum,daemon=True).start()
                    elif first in ["/topdist","/dist","/top_dist","/distribusi","/topdistribusi"]:
                        period="daily"
                        top_n=30
                        if len(parts)>=2:
                            if parts[1].lower() in ["daily","weekly","d","w","1d","1w"]:
                                period="daily" if parts[1].lower() in ["daily","d","1d"] else "weekly"
                                if len(parts)>=3:
                                    try: top_n=int(parts[2])
                                    except: top_n=30
                            else:
                                try: top_n=int(parts[1])
                                except: top_n=30
                        send_reply(chat_id, f"🔴 SCAN TOP DISTRIBUSI BANDAR {period.upper()} - 400 BY VALUE...")
                        def run_topdist(tg=chat_id, per=period, tn=top_n):
                            sigs=scan_bandarmology_top_distribusi(period=per, limit_candidates=400, top_n=tn)
                            broadcast_bandarmology(sigs, period=per, scan_type="distribusi", dest_chat_id=tg)
                        threading.Thread(target=run_topdist,daemon=True).start()
                    elif first in ["/akumdist","/bandar_combined","/topbandar","/bandarmology"]:
                        # bisa /akumdist daily atau /akumdist weekly atau /bandar daily
                        period="daily"
                        if len(parts)>=2 and parts[1].lower() in ["daily","weekly","d","w","1d","1w"]:
                            period="daily" if parts[1].lower() in ["daily","d","1d"] else "weekly"
                        # cek kalau ada symbol: /bandar SYMBOL daily
                        # kalau /bandar daily saja -> combined scan
                        # kalau /bandar BBCA daily -> detail
                        if len(parts)>=2 and len(parts[1])==4 and parts[1].isalpha() and parts[1].upper() not in ["DAILY","WEEKLY","D","W"]:
                            # detail single stock: /bandar BBCA daily
                            sym=parts[1].upper()
                            per="daily"
                            if len(parts)>=3 and parts[2].lower() in ["daily","weekly","d","w"]:
                                per="daily" if parts[2].lower() in ["daily","d","1d"] else "weekly"
                            send_reply(chat_id, f"🔍 Detail bandar {sym} {per}...")
                            def run_detail(tg=chat_id, s=sym, p=per):
                                txt_detail=get_bandar_detail_text(s, p)
                                send_reply(tg, txt_detail)
                                # plus chart
                                threading.Thread(target=process_chart_request,args=(tg,s,"1d"),daemon=True).start()
                            threading.Thread(target=run_detail,daemon=True).start()
                        else:
                            # combined scan
                            # kalau first adalah /bandar dan ada daily/weekly di parts[1]
                            if first in ["/bandar","/bandarmology"] and len(parts)==1:
                                period="daily"
                            elif first in ["/bandar","/bandarmology"] and len(parts)>=2 and parts[1].lower() in ["daily","weekly"]:
                                period=parts[1].lower()
                            send_reply(chat_id, f"🔍 SCAN AKUM+DISTRIBUSI LOGIC {period.upper()} - 400 BY VALUE...")
                            def run_combined(tg=chat_id, per=period):
                                sigs=scan_bandarmology_akum_dist_combined(period=per, limit_candidates=400, top_n=40)
                                broadcast_bandarmology(sigs, period=per, scan_type="combined", dest_chat_id=tg)
                            threading.Thread(target=run_combined,daemon=True).start()
                    elif first in ["/bandar"]:
                        # handle /bandar tanpa args -> combined daily
                        # sudah di atas, tapi fallback
                        if len(parts)==1:
                            send_reply(chat_id, f"🔍 SCAN AKUM+DISTRIBUSI DAILY...")
                            def run_bandar_default(tg=chat_id):
                                sigs=scan_bandarmology_akum_dist_combined(period="daily", limit_candidates=400, top_n=40)
                                broadcast_bandarmology(sigs, period="daily", scan_type="combined", dest_chat_id=tg)
                            threading.Thread(target=run_bandar_default,daemon=True).start()
                        elif len(parts)>=2:
                            # kalau /bandar BBCA
                            if len(parts[1])==4 and parts[1].isalpha():
                                sym=parts[1].upper()
                                per="daily"
                                if len(parts)>=3 and parts[2].lower() in ["daily","weekly"]:
                                    per=parts[2].lower()
                                send_reply(chat_id, f"🔍 Detail bandar {sym} {per}...")
                                def run_detail2(tg=chat_id, s=sym, p=per):
                                    txt_detail=get_bandar_detail_text(s, p)
                                    send_reply(tg, txt_detail)
                                    threading.Thread(target=process_chart_request,args=(tg,s,"1d"),daemon=True).start()
                                threading.Thread(target=run_detail2,daemon=True).start()
                            else:
                                # /bandar daily
                                period=parts[1].lower() if parts[1].lower() in ["daily","weekly"] else "daily"
                                send_reply(chat_id, f"🔍 SCAN AKUM+DISTRIBUSI {period.upper()}...")
                                def run_bandar_period(tg=chat_id, per=period):
                                    sigs=scan_bandarmology_akum_dist_combined(period=per, limit_candidates=400, top_n=40)
                                    broadcast_bandarmology(sigs, period=per, scan_type="combined", dest_chat_id=tg)
                                threading.Thread(target=run_bandar_period,daemon=True).start()
                    # ==================== WHALE & INSIDER - BARU, TIDAK MERUBAH YANG LAMA ====================
                    elif first in ["/whale","/whaleaccum","/topwhale","/institusional","/whale_accum","/akumwhale"]:
                        period="daily"
                        top_n=30
                        if len(parts)>=2:
                            if parts[1].lower() in ["daily","weekly","d","w","1d","1w"]:
                                period="daily" if parts[1].lower() in ["daily","d","1d"] else "weekly"
                                if len(parts)>=3:
                                    try: top_n=int(parts[2])
                                    except: top_n=30
                            else:
                                try: top_n=int(parts[1])
                                except: 
                                    # kalau parts[1] bukan angka dan bukan daily/weekly, anggap symbol? tidak untuk whale
                                    top_n=30
                        send_reply(chat_id, f"🐋 SCAN WHALE ACCUM - AKUM INSTITUSIONAL {period.upper()} - 400 BY VALUE...")
                        def run_whale(tg=chat_id, per=period, tn=top_n):
                            sigs=scan_whale_accum(period=per, limit_candidates=400, top_n=tn)
                            broadcast_whale_accum(sigs, period=per, dest_chat_id=tg)
                        threading.Thread(target=run_whale,daemon=True).start()
                    elif first in ["/insider","/insideraccum","/topinsider","/akuminsider","/stealth","/insider_accum"]:
                        period="daily"
                        top_n=30
                        if len(parts)>=2:
                            if parts[1].lower() in ["daily","weekly","d","w","1d","1w"]:
                                period="daily" if parts[1].lower() in ["daily","d","1d"] else "weekly"
                                if len(parts)>=3:
                                    try: top_n=int(parts[2])
                                    except: top_n=30
                            else:
                                try: top_n=int(parts[1])
                                except: top_n=30
                        send_reply(chat_id, f"🕵️ SCAN INSIDER ACCUM - AKUM DIAM-DIAM {period.upper()} - 400 BY VALUE...")
                        def run_insider(tg=chat_id, per=period, tn=top_n):
                            sigs=scan_insider_accum(period=per, limit_candidates=400, top_n=tn)
                            broadcast_insider_accum(sigs, period=per, dest_chat_id=tg)
                        threading.Thread(target=run_insider,daemon=True).start()
                    elif first in ["/whaleinsider","/insiderwhale","/whale_insider","/topwhaleinsider"]:
                        period="daily"
                        if len(parts)>=2 and parts[1].lower() in ["daily","weekly","d","w","1d","1w"]:
                            period="daily" if parts[1].lower() in ["daily","d","1d"] else "weekly"
                        send_reply(chat_id, f"🐋🕵️ SCAN WHALE+INSIDER {period.upper()} - 400 BY VALUE...")
                        def run_both(tg=chat_id, per=period):
                            whale_sigs=scan_whale_accum(period=per, limit_candidates=400, top_n=20)
                            broadcast_whale_accum(whale_sigs, period=per, dest_chat_id=tg)
                            time.sleep(2)
                            insider_sigs=scan_insider_accum(period=per, limit_candidates=400, top_n=20)
                            broadcast_insider_accum(insider_sigs, period=per, dest_chat_id=tg)
                        threading.Thread(target=run_both,daemon=True).start()
                    # ==================== END BANDARMOLOGY + WHALE & INSIDER COMMANDS ====================
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
    print(f"🔥 RAFANO V4.39 FULL BMTR + BANDARMOLOGY + WHALE + INSIDER - {len(IDX_FULL)} IDX")
    telegram_bot_listener()
