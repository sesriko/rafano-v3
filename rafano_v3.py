"""RAFANO V4.46 FULL - 3000 LINES + OPTIMAL WORKERS + CLICK FIX + COLAB LOG FIX"""
import os, sys
IS_COLAB = False
userdata = None
try:
    from google.colab import userdata as colab_userdata
    userdata = colab_userdata
    IS_COLAB = True
    print("Colab detected - Secrets will override .env", flush=True)
except:
    IS_COLAB = False

import time, datetime, threading, requests, pytz, re, multiprocessing
import numpy as np, pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.gridspec as gridspec
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import OrderedDict

# ==================== COLAB LOG + OPTIMAL WORKERS V4.46 ====================
LOG_FILE = "/content/rafano_debug.log" if os.path.exists("/content") else "/tmp/rafano_debug.log"
CLICK_LOG = "/content/rafano_click.log" if os.path.exists("/content") else "/tmp/rafano_click.log"
OPTIMAL_WORKERS = min(32, (multiprocessing.cpu_count() or 4) * 5)  # 20-32 untuk Colab IO-bound
CHART_WORKERS = 4
SCAN_WORKERS = OPTIMAL_WORKERS

def clog(msg):
    try:
        print(msg, flush=True)
        sys.stdout.flush()
    except:
        print(msg)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as lf:
            lf.write(f"{datetime.datetime.now().strftime('%H:%M:%S')} {msg}\n")
    except: pass

def get_last_logs(n=50):
    try:
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, "r", encoding="utf-8") as f:
                return "".join(f.readlines()[-n:])
        return "Log file kosong"
    except Exception as e:
        return f"Err baca log: {e}"

print(f"⚙️ V4.46 OPTIMAL_WORKERS={OPTIMAL_WORKERS} SCAN={SCAN_WORKERS} CHART={CHART_WORKERS} IS_COLAB={IS_COLAB}", flush=True)

# Load .env dulu
for p in ['/content/rafano-v3/.env','./.env','.env','/content/.env','/content/rafano-v3/rafano-v3/.env']:
    if os.path.exists(p):
        load_dotenv(p, override=True)
        print(f"Loaded .env from {p}", flush=True)
        break

# Lalu override dengan Colab Secrets (biar Secret menang)
if IS_COLAB and userdata:
    for k in ["TELEGRAM_BOT_TOKEN","TARGET_CHAT_ID","ARJUM_API_KEY","ITICK_TOKEN","ITICK_API_KEY"]:
        try:
            v=userdata.get(k)
            if v:
                os.environ[k]=v
                print(f"Colab Secret loaded: {k}", flush=True)
        except: pass
# ==================== END COLAB FIX ====================


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
ARJUM_MIN_INTERVAL=0.8  # lebih cepat dari 1.2
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
    """
    FIX V4.41: Chart daily pasti jadi, anti gagal VKTR/BMTR
    - Volume 0 tidak lagi bikin return None untuk chart, cuma flag SUSPEND untuk scanner
    - YF retry multiple period + auto_adjust True/False + yf.download fallback
    - Arjum tetap coba tapi tidak blokir YF kalau gagal
    """
    frame=normalize_timeframe(frame)
    hk=f"{sym}_{frame}_{limit}"
    cached=get_cached(hk, HISTORY_CACHE, 600)
    if cached is not None: 
        return cached

    # ===== 1. Coba Arjum dulu untuk daily =====
    if frame=="1d":
        try:
            data=arjum_get(f"/history/{sym}",params={"limit":limit,"frame":"daily"}, bypass_quota=True, retries=1)
            rows=[]
            if data:
                if isinstance(data,dict): rows=data.get('data') or data.get('history') or []
                elif isinstance(data,list): rows=data
            if rows:
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
                for col in ['Open','High','Low','Close','Volume']: 
                    if col in df.columns:
                        df[col]=pd.to_numeric(df[col],errors='coerce')
                df=df.dropna(subset=['Close'])
                if len(df)>=10:
                    last_close=float(df['Close'].iloc[-1])
                    # Cek suspend: close 100 semua atau < MIN_PRICE
                    is_suspend_price = df['Close'].nunique()==1 and int(df['Close'].iloc[0])==100
                    if last_close>=MIN_PRICE and not is_suspend_price:
                        vol_sum5 = float(df['Volume'].tail(5).sum()) if 'Volume' in df.columns else 1
                        if vol_sum5==0:
                            SUSPEND_CACHE[sym]=True
                            print(f"⚠ {sym} daily Arjum vol 5d=0 -> flag SUSPEND tapi tetap return chart")
                        # FIX: tetap return meski vol 0, biar chart jadi
                        set_cached(hk,df,HISTORY_CACHE)
                        return df
                    else:
                        if is_suspend_price or last_close<MIN_PRICE:
                            SUSPEND_CACHE[sym]=True
        except Exception as e:
            print(f"Arjum history err {sym}: {e}")

    # ===== 2. Fallback YFinance - RETRY MULTIPLE PERIOD =====
    try:
        import yfinance as yf
        interval_map={"5m":"5m","15m":"15m","30m":"30m","1h":"60m","4h":"60m","1d":"1d","1w":"1wk"}
        period_list_map={
            "5m":["5d","7d"],
            "15m":["30d","60d"],
            "30m":["60d","30d"],
            "1h":["60d","30d","7d"],
            "4h":["60d","730d"],
            "1d":["1y","6mo","2y","1mo","3mo","5d"],
            "1w":["2y","5y","max"]
        }
        interval=interval_map.get(frame,"1d")
        periods=period_list_map.get(frame,["1y","6mo"])

        for per in periods:
            for auto_adj in [False, True]:
                try:
                    hist=yf.Ticker(f"{sym}.JK").history(period=per, interval=interval, timeout=20, auto_adjust=auto_adj)
                    if hist is not None and len(hist)>=10:
                        if frame=="4h":
                            try:
                                hist=hist.resample('4H').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'}).dropna()
                            except: pass
                        if len(hist)==0:
                            continue
                        last_close=float(hist['Close'].iloc[-1])
                        if last_close<MIN_PRICE:
                            continue
                        if hist['Close'].nunique()==1 and int(hist['Close'].iloc[0])==100:
                            SUSPEND_CACHE[sym]=True
                            print(f"⚠ {sym} YF {frame} {per} close 100 all -> suspend flag")
                            # tetap lanjut coba period lain, tapi kalau semua 100 ya suspend
                            continue
                        vol_sum5 = float(hist['Volume'].tail(5).sum()) if 'Volume' in hist.columns else 1
                        if vol_sum5==0:
                            SUSPEND_CACHE[sym]=True
                            print(f"⚠ {sym} YF {frame} {per} vol 5d=0 -> flag SUSPEND tapi tetap return chart")
                        # FIX: tetap return meski vol 0
                        set_cached(hk,hist.tail(limit),HISTORY_CACHE)
                        print(f"✅ YF success {sym} {frame} {per} len={len(hist)} close={last_close}")
                        return hist.tail(limit)
                except Exception as e:
                    print(f"YF retry err {sym} {frame} {per} adj={auto_adj}: {e}")
                    continue

        # ===== 3. Last fallback: yf.download (lebih stabil kadang) =====
        try:
            for per in ["1y","6mo"]:
                df_dl=yf.download(f"{sym}.JK", period=per, interval=interval, progress=False, timeout=20, auto_adjust=False)
                if df_dl is not None and len(df_dl)>=10:
                    last_close=float(df_dl['Close'].iloc[-1])
                    if last_close>=MIN_PRICE:
                        if df_dl['Close'].nunique()==1 and int(df_dl['Close'].iloc[0])==100:
                            continue
                        vol_sum5 = float(df_dl['Volume'].tail(5).sum()) if 'Volume' in df_dl.columns else 1
                        if vol_sum5==0:
                            SUSPEND_CACHE[sym]=True
                        set_cached(hk,df_dl.tail(limit),HISTORY_CACHE)
                        print(f"✅ YF download success {sym} {frame} {per}")
                        return df_dl.tail(limit)
        except Exception as e:
            print(f"YF download err {sym}: {e}")

    except Exception as e:
        print(f"YF err {sym} {frame}: {e}")

    print(f"❌ get_history_pro FAIL {sym} {frame} - Arjum & YF semua gagal")
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
        fig.text(0.5,0.96,f"Rafano Trader",color='white',fontsize=14,fontweight='bold',ha='center')
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
    print(f"🔍 get_top_liquid_tickers n={n} BY VALUE (volume*close DESC) - FAST V4.40")
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
                # FAST: langsung pakai value tanpa check history lagi (anti quota)
                r['_value']=vol_val*close_val
                r['_code']=code
                filtered.append(r)
            if len(filtered)>=20:
                rows_sorted=sorted(filtered, key=lambda r: r.get('_value',0), reverse=True)
                final=[r['_code'] for r in rows_sorted[:n]]
                print(f"Final BY VALUE FAST {len(final)}: {final[:5]}")
                if len(final)>=n*0.3:
                    return final[:n]
    except Exception as e:
        print(f"get_top_liquid err {e}")
    # FALLBACK CEPAT: pakai IDX_FULL tanpa panggil history (biar tidak quota)
    # Kalau IDX_FULL kosong, pakai list default
    try:
        quick=[c for c in IDX_FULL if c not in FCA_EXCLUDE and "-W" not in c and c not in SUSPEND_CACHE]
        if len(quick)>=n:
            return quick[:n]
        return quick
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
    # FIX V4.40: kalau kosong, turunkan threshold otomatis
    if len(detected)==0 and threshold>1.0:
        print(f"⚠ Vol spike kosong threshold {threshold}, coba threshold 1.0")
        for sym in tickers[:100]:
            try:
                hd=get_history_pro(sym,30,"daily")
                if hd is None or len(hd)<20: continue
                last_close=float(hd['Close'].iloc[-1])
                v_last=hd['Volume'].iloc[-1]; v_avg=hd['Volume'].tail(20).mean()
                if v_avg==0: continue
                ratio=v_last/v_avg
                if ratio < 1.0: continue
                prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
                chg=(last_close/prev-1)*100 if prev else 0
                detected.append({"symbol":sym,"close":int(last_close),"change_pct":chg,"vol_ratio":ratio,"vol_rp":v_last*last_close})
            except: continue
    # Jika masih kosong, ambil top 20 by vol ratio apapun
    if len(detected)==0:
        print("⚠ Vol spike masih kosong, ambil top 20 by vol ratio")
        temp=[]
        for sym in tickers[:150]:
            try:
                hd=get_history_pro(sym,30,"daily")
                if hd is None: continue
                v_last=hd['Volume'].iloc[-1]; v_avg=hd['Volume'].tail(20).mean()
                if v_avg==0: continue
                ratio=v_last/v_avg
                last_close=float(hd['Close'].iloc[-1])
                prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
                chg=(last_close/prev-1)*100 if prev else 0
                temp.append({"symbol":sym,"close":int(last_close),"change_pct":chg,"vol_ratio":ratio,"vol_rp":v_last*last_close})
            except: continue
        temp_sorted=sorted(temp, key=lambda x: x['vol_ratio'], reverse=True)[:20]
        detected=temp_sorted
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
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
        futs={ex.submit(check_one,s):s for s in tickers}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)
    # FIX: kalau kosong, jangan return kosong, kasih info top volume saja sebagai fallback
    if len(detected)==0:
        print("⚠ KIM+BSJP kosong, fallback vol spike")
        return scan_volume_spike(threshold=1.2, limit_candidates=400)[:10]
    detected.sort(key=lambda x: (x['kim_buy'] and x['bsjp_buy'], x['vol_ratio']), reverse=True)
    return detected

def scan_kim_bsjp_300(): return scan_kim_bsjp_400()


# ==================== BANDARMOLOGY ARJUM API - TOP AKUM/DIST DAILY/WEEKLY - SEPARATE SCAN ====================
# FIX V4.40 - OPTIMIZED, ANTI QUOTA, THRESHOLD RENDAH, SELALU ADA HASIL

BANDAR_CACHE=OrderedDict()
RETAIL_BROKERS={"YP","CC","NI","PD","OD","XC","XL","DR","CP","MG","AZ","ZP","AK","KK","BK","YU","DX","MI","EP","DP","BQ","SQ","LA","AT","DL","KD","SA","AG","BB","AR","BR","IN","SU","GR","AN","HP","IF","KI","KT","LF","LG","LS","MK","MS","MU","PO","PP","RB","RF","RG","RH","RO","SC","SH","SM","SN","SR","SS","TA","TF","TG","TP","TR","TX","UT","XA","XB","XD","XE","XF","XG","XH","XI","XJ","XK","XM","XN","XO","XP","XQ","XR","XS","XT","XU","XV","XW","XX","XY","XZ","YD","YE","YF","YG","YH","YI","YJ","YK","YL","YM","YN","YO","YQ","YR","YS","YT","YV","YW","YX","YY","YZ","ZA","ZB","ZC","ZD","ZE","ZF","ZG","ZH","ZI","ZJ","ZK","ZL","ZM","ZN","ZO","ZQ","ZR","ZS","ZT","ZU","ZV","ZW","ZX","ZY","ZZ"}

def get_bandar_cached(k, ttl=600):
    return get_cached(k, BANDAR_CACHE, ttl)
def set_bandar_cached(k, d, maxsize=500):
    set_cached(k, d, BANDAR_CACHE, maxsize=maxsize)

def fetch_arjum_broker_data(symbol, period="daily"):
    """
    FIX V4.40: Anti quota, hanya 2 endpoint paling mungkin, 1 param saja.
    Kalau QUOTA_HIT atau API key kosong -> langsung return None -> fallback VSA (cepat)
    """
    symbol=symbol.upper().strip()
    cache_key=f"bandar_{symbol}_{period}"
    cached=get_bandar_cached(cache_key, ttl=300 if period=="daily" else 600)
    if cached is not None:
        return cached
    # Jika quota habis atau tidak ada API key, jangan coba Arjum sama sekali -> fallback VSA
    if QUOTA_HIT or not ARJUM_API_KEY:
        return None
    # Hanya 2 endpoint yang paling masuk akal, 1 param saja
    endpoints_to_try=[
        f"/broker/{symbol}",
        f"/bandarmology/{symbol}",
    ]
    params={"period": period}
    for ep in endpoints_to_try:
        try:
            # bypass_quota=False supaya tidak paksa saat quota habis, retries=0 cepat
            data=arjum_get(ep, params=params, bypass_quota=False, retries=0)
            if data is None:
                continue
            # validasi ada data broker
            if isinstance(data, dict):
                if any(k in data for k in ["brokers","broker_summary","data","summary","result","broker_data","flow"]):
                    inner=data.get('data') or data.get('brokers') or data.get('broker_summary') or data.get('summary') or data.get('result') or data.get('broker_data') or []
                    if isinstance(inner, list) and len(inner)==0:
                        continue
                    if isinstance(inner, dict) and len(inner)==0:
                        continue
                    set_bandar_cached(cache_key, data)
                    return data
                # kalau dict tapi ada field broker code langsung
                if len(data)>2 and "error" not in str(data).lower():
                    set_bandar_cached(cache_key, data)
                    return data
            elif isinstance(data, list) and len(data)>0:
                set_bandar_cached(cache_key, data)
                return data
        except:
            continue
    return None

def parse_broker_data(raw_data, symbol=""):
    try:
        brokers_list=[]
        if raw_data is None:
            return None
        if isinstance(raw_data, dict):
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
            else:
                for k,v in raw_data.items():
                    if isinstance(v, list) and len(v)>0 and isinstance(v[0], dict):
                        sample=v[0]
                        if any(x in str(sample).lower() for x in ["broker","buy","sell","net","code"]):
                            brokers_list=v
                            break
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
            broker_code=(item.get('broker_code') or item.get('broker') or item.get('code') or item.get('broker_id') or "").upper().strip()
            if not broker_code or len(broker_code)>4:
                continue
            buy_val=float(item.get('buy_value') or item.get('buy') or item.get('buy_val') or item.get('total_buy_value') or 0)
            sell_val=float(item.get('sell_value') or item.get('sell') or item.get('sell_val') or item.get('total_sell_value') or 0)
            net_val=float(item.get('net_value') or item.get('net') or item.get('net_val') or (buy_val - sell_val) or 0)
            if net_val==0 and (buy_val!=0 or sell_val!=0):
                net_val=buy_val - sell_val
            buy_vol=float(item.get('buy_volume') or item.get('buy_vol') or 0)
            sell_vol=float(item.get('sell_volume') or item.get('sell_vol') or 0)
            net_vol=float(item.get('net_volume') or item.get('net_vol') or (buy_vol - sell_vol) or 0)
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
        if total_net_value==0:
            total_net_value=total_buy_value - total_sell_value
        top_buyers=sorted([x for x in parsed if x['net_value']>0], key=lambda x: x['net_value'], reverse=True)[:5]
        top_sellers=sorted([x for x in parsed if x['net_value']<0], key=lambda x: x['net_value'])[:5]
        institutional_net=sum(x['net_value'] for x in parsed if not x['is_retail'])
        retail_net=sum(x['net_value'] for x in parsed if x['is_retail'])
        return {
            "net_value": total_net_value,
            "net_volume": sum(x['net_vol'] for x in parsed),
            "total_buy_value": total_buy_value,
            "total_sell_value": total_sell_value,
            "top_buyers": top_buyers,
            "top_sellers": top_sellers,
            "institutional_net": institutional_net,
            "retail_net": retail_net,
            "raw_count": len(parsed),
            "symbol": symbol
        }
    except Exception as e:
        print(f"parse_broker_data err {symbol}: {e}")
        return None

def fallback_vsa_bandar(symbol, period="daily"):
    """
    FIX V4.40: Fallback utama, threshold rendah, selalu return data kalau ada history
    """
    try:
        df=get_history_pro(symbol, 150 if period=="daily" else 60, frame="1d")
        if df is None or len(df)<20:
            return None
        if 'V1' not in df.columns:
            df['V1']=df['Volume'].rolling(20, min_periods=1).mean()
        df_vsa, buy_ratios=calculate_vsa_metrics(df)
        last_close=float(df['Close'].iloc[-1])
        prev_close=float(df['Close'].iloc[-2]) if len(df)>=2 else last_close
        chg_pct=(last_close/prev_close-1)*100 if prev_close else 0
        try:
            buy_pct=float((buy_ratios.iloc[-1]*100) if hasattr(buy_ratios, 'iloc') else buy_ratios[-1]*100)
        except:
            buy_pct=50.0
        net_vol=float(df_vsa['Net_Vol_VSA'].iloc[-1]) if 'Net_Vol_VSA' in df_vsa.columns else 0.0
        v1=float(df['V1'].iloc[-1]) if 'V1' in df.columns and len(df['V1'])>0 else 1.0
        vol_ratio=float(df['Volume'].iloc[-1]/v1) if v1>0 else 1.0
        rsi=calculate_rsi(df['Close'],14)
        ema200=float(df['Close'].ewm(span=200, adjust=False).mean().iloc[-1]) if len(df)>=20 else last_close

        if period=="weekly":
            last5=df.tail(5)
            try:
                net_vol=float(last5['Net_Vol_VSA'].sum()) if 'Net_Vol_VSA' in last5.columns else net_vol*5
            except:
                net_vol=net_vol*5
            try:
                vol_ratio=float(last5['Volume'].mean() / v1) if v1>0 else vol_ratio
            except:
                pass
            if len(df)>=6:
                chg_pct=(last_close/float(df['Close'].iloc[-6])-1)*100

        # FIX: Threshold lebih longgar
        # Akum: Buy%>=50, net_vol>0 ATAU Buy%>=55, vol>=0.8, close>=ema200*0.95
        is_akum = (buy_pct>=50 and net_vol>0 and vol_ratio>=0.8) or (buy_pct>=55 and last_close>=ema200*0.95)
        is_dist = (buy_pct<=50 and net_vol<0) or (buy_pct<=45 and chg_pct<0)

        net_value_est=net_vol * last_close
        # Kalau net_vol kecil, estimasi minimal berdasarkan volume*close*buy_pct
        if abs(net_value_est) < 10_000_000:
            # fallback estimasi kasar: volume terakhir * close * (buy_pct-50)/100
            vol_last=float(df['Volume'].iloc[-1])
            net_value_est=vol_last * last_close * (buy_pct-50)/100.0

        top_buyers=[{"broker": "BANDAR", "net_value": max(net_value_est,0), "buy_value": max(net_value_est,0)}] if net_value_est>0 else []
        top_sellers=[{"broker": "BANDAR", "net_value": min(net_value_est,0), "sell_value": abs(min(net_value_est,0))}] if net_value_est<0 else []

        return {
            "net_value": net_value_est,
            "net_volume": net_vol,
            "total_buy_value": max(net_value_est,0),
            "total_sell_value": abs(min(net_value_est,0)),
            "top_buyers": top_buyers,
            "top_sellers": top_sellers,
            "institutional_net": net_value_est if buy_pct>=55 else 0,
            "retail_net": 0,
            "raw_count": 1,
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
    FIX V4.40: Prioritas fallback VSA dulu (cepat, tidak kena quota), Arjum hanya kalau ada
    """
    # Coba fallback dulu biar cepat dan selalu ada hasil
    parsed=fallback_vsa_bandar(symbol, period)
    # Kalau ada API key dan tidak quota, coba Arjum untuk enrich
    raw=None
    if not QUOTA_HIT and ARJUM_API_KEY:
        raw=fetch_arjum_broker_data(symbol, period)
        if raw is not None:
            arjum_parsed=parse_broker_data(raw, symbol)
            if arjum_parsed is not None:
                # Gabung: pakai data Arjum kalau ada, fallback untuk buy_pct dll kalau tidak ada
                if parsed is None:
                    parsed=arjum_parsed
                else:
                    # enrich parsed dengan data Arjum
                    parsed['net_value']=arjum_parsed.get('net_value', parsed['net_value'])
                    parsed['top_buyers']=arjum_parsed.get('top_buyers', parsed['top_buyers'])
                    parsed['top_sellers']=arjum_parsed.get('top_sellers', parsed['top_sellers'])
                    parsed['institutional_net']=arjum_parsed.get('institutional_net', parsed['institutional_net'])
                    parsed['retail_net']=arjum_parsed.get('retail_net', parsed['retail_net'])
                    parsed['fallback']=False
                    parsed['source']="Arjum API"
    if parsed is None:
        return None
    try:
        net=parsed.get('net_value',0)
        buy_pct=parsed.get('buy_pct', 50)
        if 'buy_pct' not in parsed:
            total_buy=parsed.get('total_buy_value',0)
            total_sell=parsed.get('total_sell_value',0)
            if total_buy+total_sell>0:
                buy_pct=(total_buy/(total_buy+total_sell))*100
            else:
                buy_pct=50 if net>=0 else 40
            parsed['buy_pct']=buy_pct
        # Logic akum/dist valid threshold RENDAH V4.40
        is_akum_valid=False
        is_dist_valid=False
        if net>0:
            # threshold RENDAH: 100jt daily, 400jt weekly
            threshold=100_000_000 if period=="daily" else 400_000_000
            if abs(net)>=threshold:
                if buy_pct>=48:  # turun dari 55 ke 48
                    is_akum_valid=True
            else:
                # kalau net kecil tapi buy_pct tinggi tetap anggap valid (untuk memastikan ada hasil)
                if buy_pct>=55:
                    is_akum_valid=True
        if net<0:
            threshold=100_000_000 if period=="daily" else 400_000_000
            if abs(net)>=threshold:
                if buy_pct<=52:
                    is_dist_valid=True
            else:
                if buy_pct<=45:
                    is_dist_valid=True
        parsed['is_akum_valid']=is_akum_valid
        parsed['is_dist_valid']=is_dist_valid
        parsed['period']=period
    except Exception as e:
        print(f"get_bandar_analysis logic err {symbol}: {e}")
    return parsed

def scan_bandarmology_top_akum(period="daily", limit_candidates=400, top_n=30, min_net_value=100_000_000):
    period=period.lower()
    if period not in ["daily","weekly"]: period="daily"
    tickers=get_top_liquid_tickers(limit_candidates)
    print(f"🔍 SCAN BANDAR TOP AKUM {period.upper()} - {len(tickers)} saham BY VALUE (threshold rendah)")
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
            if net<=0: return None
            thresh=min_net_value if period=="daily" else min_net_value*4
            # FIX: jangan filter keras kalau fallback, biarkan lewat
            if not analysis.get('fallback') and abs(net)<thresh:
                # tetap cek buy_pct, kalau tinggi tetap lolos
                if analysis.get('buy_pct',0) < 55:
                    return None
            if not analysis.get('is_akum_valid', False) and not analysis.get('is_akum', False):
                # FIX: kalau tidak valid tapi buy_pct >=50 tetap kasih chance
                if analysis.get('buy_pct',0) < 50:
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

    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
        futs={ex.submit(check_one,s):s for s in tickers}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)

    # Jika masih kosong, fallback: ambil top 10 dengan net_value terbesar meskipun kecil
    if len(detected)==0:
        print("⚠ Top akum kosong, fallback ambil top net positif")
        temp=[]
        for sym in tickers[:100]:
            try:
                hd=get_history_pro(sym, 20, "1d")
                if hd is None: continue
                ana=get_bandar_analysis(sym, period)
                if ana and ana.get('net_value',0)>0:
                    temp.append((sym, ana.get('net_value',0), ana))
            except: continue
        temp_sorted=sorted(temp, key=lambda x: x[1], reverse=True)[:top_n]
        for sym, net, ana in temp_sorted:
            try:
                hd=get_history_pro(sym, 20, "1d")
                last_close=float(hd['Close'].iloc[-1])
                prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
                chg=(last_close/prev-1)*100 if prev else 0
                v_last=hd['Volume'].iloc[-1]; v_avg=hd['Volume'].tail(20).mean()
                vol_ratio=v_last/v_avg if v_avg else 1
                detected.append({
                    "symbol": sym,
                    "close": int(last_close),
                    "change_pct": chg,
                    "vol_ratio": vol_ratio,
                    "net_value": net,
                    "net_volume": ana.get('net_volume',0),
                    "buy_pct": ana.get('buy_pct',0),
                    "top_buyers": ana.get('top_buyers',[]),
                    "institutional_net": ana.get('institutional_net',0),
                    "retail_net": ana.get('retail_net',0),
                    "period": period,
                    "analysis": ana
                })
            except: continue

    detected.sort(key=lambda x: x['net_value'], reverse=True)
    return detected[:top_n]

def scan_bandarmology_top_distribusi(period="daily", limit_candidates=400, top_n=30, min_net_value=100_000_000):
    period=period.lower()
    if period not in ["daily","weekly"]: period="daily"
    tickers=get_top_liquid_tickers(limit_candidates)
    print(f"🔍 SCAN BANDAR TOP DISTRIBUSI {period.upper()} - {len(tickers)} saham")
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
            if not analysis.get('fallback') and abs(net)<thresh:
                if analysis.get('buy_pct',0) > 45:
                    return None
            if not analysis.get('is_dist_valid', False) and not analysis.get('is_dist', False):
                if analysis.get('buy_pct',0) > 50:
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

    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
        futs={ex.submit(check_one,s):s for s in tickers}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)

    if len(detected)==0:
        print("⚠ Top distribusi kosong, fallback top net negatif")
        temp=[]
        for sym in tickers[:100]:
            try:
                hd=get_history_pro(sym, 20, "1d")
                if hd is None: continue
                ana=get_bandar_analysis(sym, period)
                if ana and ana.get('net_value',0)<0:
                    temp.append((sym, ana.get('net_value',0), ana))
            except: continue
        temp_sorted=sorted(temp, key=lambda x: x[1])[:top_n]
        for sym, net, ana in temp_sorted:
            try:
                hd=get_history_pro(sym, 20, "1d")
                last_close=float(hd['Close'].iloc[-1])
                prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
                chg=(last_close/prev-1)*100 if prev else 0
                v_last=hd['Volume'].iloc[-1]; v_avg=hd['Volume'].tail(20).mean()
                vol_ratio=v_last/v_avg if v_avg else 1
                detected.append({
                    "symbol": sym,
                    "close": int(last_close),
                    "change_pct": chg,
                    "vol_ratio": vol_ratio,
                    "net_value": net,
                    "net_volume": ana.get('net_volume',0),
                    "buy_pct": ana.get('buy_pct',0),
                    "top_sellers": ana.get('top_sellers',[]),
                    "institutional_net": ana.get('institutional_net',0),
                    "retail_net": ana.get('retail_net',0),
                    "period": period,
                    "analysis": ana
                })
            except: continue

    detected.sort(key=lambda x: x['net_value'])
    return detected[:top_n]

def scan_bandarmology_akum_dist_combined(period="daily", limit_candidates=400, top_n=40):
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

            logic_akum_score=0
            if net>0: logic_akum_score+=1
            if buy_pct>=50: logic_akum_score+=1
            if vol_ratio>=0.8: logic_akum_score+=1
            if last_close>ema200*0.95: logic_akum_score+=1
            if rsi<75 and rsi>30: logic_akum_score+=1
            if chg>-3: logic_akum_score+=1

            logic_dist_score=0
            if net<0: logic_dist_score+=1
            if buy_pct<=50: logic_dist_score+=1
            if chg<1: logic_dist_score+=1
            if last_close<ema20*1.02: logic_dist_score+=1

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

            if logic_akum_score>=3 and net>0:
                base['logic_score']=logic_akum_score
                base['type']='AKUM'
                return ('akum', base)
            elif logic_dist_score>=2 and net<0:
                base['logic_score']=logic_dist_score
                base['type']='DISTRIBUSI'
                return ('dist', base)
            else:
                return None
        except Exception as e:
            print(f"scan combined err {sym}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
        futs={ex.submit(check_one,s):s for s in tickers}
        for f in as_completed(futs):
            r=f.result()
            if r:
                typ, data=r
                if typ=='akum':
                    akum_list.append(data)
                else:
                    dist_list.append(data)

    if len(akum_list)==0 and len(dist_list)==0:
        print("⚠ Combined kosong, fallback ambil top akum+dist dari scan terpisah")
        akum_list=scan_bandarmology_top_akum(period, limit_candidates, top_n//2, min_net_value=50_000_000)
        dist_list=scan_bandarmology_top_distribusi(period, limit_candidates, top_n//2, min_net_value=50_000_000)
        # convert format
        akum_list=[{**x, 'logic_score': 3} for x in akum_list]
        dist_list=[{**x, 'logic_score': 2} for x in dist_list]
        return {"akum": akum_list, "distribusi": dist_list, "period": period}

    akum_list.sort(key=lambda x: (x['logic_score'], x['net_value']), reverse=True)
    dist_list.sort(key=lambda x: (x['logic_score'], abs(x['net_value'])), reverse=True)

    return {"akum": akum_list[:top_n//2], "distribusi": dist_list[:top_n//2], "period": period}

def broadcast_bandarmology(signals, period="daily", scan_type="akum", dest_chat_id=None):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target:
        print("broadcast_bandarmology NO TARGET")
        return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')

    if scan_type=="akum":
        if not signals:
            send_reply(target, f"🔍 TOP AKUM BANDAR {period.upper()} (400 BY VALUE): Tidak ada akumulasi signifikan - coba /topakum daily 50 atau cek /status"); return
        header=f"*TOP AKUM BANDAR {period.upper()}* 🟢 {now}\nTOP 400 BY VALUE (volume*close) | {len(signals)} saham | Filter: no suspend/FCA/price>=50\nLogic: Net Buy>0, Buy%>=48%\n\n"
        msg=header; kb=[]
        for idx,it in enumerate(signals,1):
            net_m=it['net_value']/1e9
            line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Net {net_m:+.2f}B Buy%{it['buy_pct']:.0f}% Vol{it['vol_ratio']:.1f}x\n"
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
        header=f"*TOP DISTRIBUSI BANDAR {period.upper()}* 🔴 {now}\nTOP 400 BY VALUE | {len(signals)} saham\nLogic: Net Sell<0, Buy%<=52%\n\n"
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
        if akum:
            header=f"*AKUM BANDAR {period_sig.upper()} - LOGIC SESUAI* 🟢 {now}\n{len(akum)} saham akumulasi valid\n\n"
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
            header2=f"*DISTRIBUSI BANDAR {period_sig.upper()} - LOGIC SESUAI* 🔴 {now}\n{len(distribusi)} saham distribusi valid\n\n"
            msg=header2; kb=[]
            for idx,it in enumerate(distribusi,1):
                net_m=it['net_value']/1e9
                line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Score{it.get('logic_score',0)}/4 Net {net_m:.2f}B\n"
                kb.append([{"text": f"{it['symbol']} DIST {net_m:.1f}B", "callback_data": f"chart_{it['symbol']}"}])
                if len(msg)+len(line)>3500:
                    send_reply(target, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
                else: msg+=line
            if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

def get_bandar_detail_text(symbol, period="daily"):
    analysis=get_bandar_analysis(symbol, period)
    if analysis is None:
        return f"⚠ Data bandar {symbol.upper()} {period} tidak tersedia"
    net=analysis.get('net_value',0)
    net_m=net/1e9
    buy_pct=analysis.get('buy_pct',0)
    top_buyers=analysis.get('top_buyers',[])
    top_sellers=analysis.get('top_sellers',[])
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

    if analysis.get('is_akum_valid') or analysis.get('is_akum'):
        text+=f"\n✅ *AKUMULASI VALID*"
    elif analysis.get('is_dist_valid') or analysis.get('is_dist'):
        text+=f"\n🔴 *DISTRIBUSI VALID*"
    else:
        text+=f"\n⚪ NETRAL"

    return text

# ==================== WHALE & INSIDER ACCUMULATION - BARU, TIDAK MERUBAH SCRIPT LAIN ====================

INSTITUTIONAL_CORE={"KZ","BK","AK","ZP","YU","DB","ML","UB","CS","MS","JP","RX","LG","RG","AG","BB","KI","AI","CP","DX","OD","XC","XL","YP"}

def is_whale_net_value(net_value, period="daily"):
    if period=="daily":
        return abs(net_value) >= 1_000_000_000
    else:
        return abs(net_value) >= 5_000_000_000

def is_insider_pattern(df, analysis):
    try:
        if df is None or len(df)<30:
            return False, 0
        last_close=float(df['Close'].iloc[-1])
        low_20=float(df['Low'].tail(20).min())
        high_20=float(df['High'].tail(20).max())
        range_pos=(last_close - low_20)/(high_20 - low_20) if (high_20 - low_20)>0 else 0.5
        buy_pct=analysis.get('buy_pct',50)
        net=analysis.get('net_value',0)
        vol_ratio=analysis.get('vol_ratio',1)
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
        score=0
        if 0.0 <= range_pos <= 0.5: score+=2
        elif 0.5 < range_pos <= 0.7: score+=1
        if 48 <= buy_pct <= 90: score+=1
        if 0.5 <= vol_ratio <= 2.5: score+=1
        if -2 <= chg <= 3.5: score+=1
        if 25 <= rsi <= 65: score+=1
        if last_close <= ema50*1.1: score+=1
        if 50_000_000 <= net <= 10_000_000_000: score+=1
        return (score>=4), score
    except Exception as e:
        print(f"is_insider_pattern err: {e}")
        return False, 0

def is_whale_pattern(df, analysis, period="daily"):
    try:
        if df is None or len(df)<30:
            return False, 0
        net=analysis.get('net_value',0)
        if not is_whale_net_value(net, period):
            # tetap kasih chance kalau buy_pct tinggi
            if analysis.get('buy_pct',0) < 60:
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
        if buy_pct>=60: score+=2
        elif buy_pct>=50: score+=1
        if institutional_net>=retail_net: score+=1
        if top_buyers:
            tb0=top_buyers[0].get('broker','') if isinstance(top_buyers[0], dict) else str(top_buyers[0])
            if tb0 in INSTITUTIONAL_CORE: score+=2
            else: score+=1
        if vol_ratio>=0.8: score+=1
        if last_close>ema200*0.95: score+=1
        if last_close>ema50*0.95: score+=1
        if 30 <= rsi <= 78: score+=1
        if range_pos < 0.9: score+=1
        if chg>=-2: score+=1

        return (score>=5), score
    except Exception as e:
        print(f"is_whale_pattern err: {e}")
        return False, 0

def scan_whale_accum(period="daily", limit_candidates=400, top_n=30):
    period=period.lower()
    if period not in ["daily","weekly"]: period="daily"
    tickers=get_top_liquid_tickers(limit_candidates)
    print(f"🔍 SCAN WHALE ACCUM {period.upper()} - {len(tickers)} saham (threshold rendah)")
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

    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
        futs={ex.submit(check_one,s):s for s in tickers}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)

    if len(detected)==0:
        print("⚠ Whale kosong, fallback top net positif")
        temp=[]
        for sym in tickers[:150]:
            try:
                hd=get_history_pro(sym, 30, "1d")
                if hd is None: continue
                ana=get_bandar_analysis(sym, period)
                if ana and ana.get('net_value',0)>0 and ana.get('buy_pct',0)>=50:
                    temp.append((sym, ana.get('net_value',0), ana, hd))
            except: continue
        temp_sorted=sorted(temp, key=lambda x: x[1], reverse=True)[:top_n]
        for sym, net, ana, hd in temp_sorted:
            try:
                last_close=float(hd['Close'].iloc[-1])
                prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
                chg=(last_close/prev-1)*100 if prev else 0
                v_last=hd['Volume'].iloc[-1]; v_avg=hd['Volume'].tail(20).mean()
                vol_ratio=v_last/v_avg if v_avg else 1
                detected.append({
                    "symbol": sym,
                    "close": int(last_close),
                    "change_pct": chg,
                    "vol_ratio": vol_ratio,
                    "net_value": net,
                    "buy_pct": ana.get('buy_pct',0),
                    "top_buyers": ana.get('top_buyers',[]),
                    "institutional_net": ana.get('institutional_net',0),
                    "retail_net": ana.get('retail_net',0),
                    "whale_score": 5,
                    "period": period,
                    "analysis": ana,
                    "rsi": calculate_rsi(hd['Close'],14)
                })
            except: continue

    detected.sort(key=lambda x: (x['whale_score'], x['net_value']), reverse=True)
    return detected[:top_n]

def scan_insider_accum(period="daily", limit_candidates=400, top_n=30):
    period=period.lower()
    if period not in ["daily","weekly"]: period="daily"
    tickers=get_top_liquid_tickers(limit_candidates)
    print(f"🔍 SCAN INSIDER ACCUM {period.upper()} - {len(tickers)} saham (threshold rendah)")
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
            net=analysis.get('net_value',0)
            if period=="daily" and net>15_000_000_000: return None
            if period=="weekly" and net>50_000_000_000: return None
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

    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
        futs={ex.submit(check_one,s):s for s in tickers}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)

    if len(detected)==0:
        print("⚠ Insider kosong, fallback top akum dekat low")
        temp=[]
        for sym in tickers[:150]:
            try:
                hd=get_history_pro(sym, 60, "1d")
                if hd is None: continue
                ana=get_bandar_analysis(sym, period)
                if ana and ana.get('net_value',0)>0:
                    low_20=float(hd['Low'].tail(20).min())
                    last_close=float(hd['Close'].iloc[-1])
                    high_20=float(hd['High'].tail(20).max())
                    range_pos=(last_close - low_20)/(high_20 - low_20) if (high_20 - low_20)>0 else 0.5
                    if range_pos<=0.6 and ana.get('buy_pct',0)>=48:
                        temp.append((sym, range_pos, ana, hd))
            except: continue
        temp_sorted=sorted(temp, key=lambda x: x[1])[:top_n]
        for sym, rp, ana, hd in temp_sorted:
            try:
                last_close=float(hd['Close'].iloc[-1])
                prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
                chg=(last_close/prev-1)*100 if prev else 0
                v_last=hd['Volume'].iloc[-1]; v_avg=hd['Volume'].tail(20).mean()
                vol_ratio=v_last/v_avg if v_avg else 1
                low_20=float(hd['Low'].tail(20).min())
                detected.append({
                    "symbol": sym,
                    "close": int(last_close),
                    "change_pct": chg,
                    "vol_ratio": vol_ratio,
                    "net_value": ana.get('net_value',0),
                    "buy_pct": ana.get('buy_pct',0),
                    "top_buyers": ana.get('top_buyers',[]),
                    "insider_score": 4,
                    "period": period,
                    "analysis": ana,
                    "low_20": low_20,
                    "range_pos": rp,
                    "rsi": calculate_rsi(hd['Close'],14)
                })
            except: continue

    detected.sort(key=lambda x: (x['insider_score'], x['buy_pct'], x['net_value']), reverse=True)
    return detected[:top_n]

def broadcast_whale_accum(signals, period="daily", dest_chat_id=None):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target:
        print("broadcast_whale NO TARGET")
        return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    if not signals:
        send_reply(target, f"🐋 WHALE ACCUM {period.upper()} (400 BY VALUE): Tidak ada akum institusional - coba /whale daily 50 atau cek /status"); return
    header=f"*WHALE ACCUM - AKUM INSTITUSIONAL {period.upper()}* 🐋 {now}\nTOP 400 BY VALUE | {len(signals)} saham\nLogic: Net>=1B D / 5B W, Buy%>=50%, >EMA200*0.95\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        net_m=it['net_value']/1e9
        tb=it['top_buyers'][0].get('broker','') if it.get('top_buyers') else '-'
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Score{it.get('whale_score',0)}/11 Net {net_m:+.2f}B Buy%{it['buy_pct']:.0f}% Top:{tb} Vol{it['vol_ratio']:.1f}x\n"
        btn=[{"text": f"{it['symbol']} 🐋 {net_m:.1f}B", "callback_data": f"chart_{it['symbol']}"}]
        # FIX CLICK: cek overflow sebelum append biar text & button sinkron
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb})
            msg=line
            kb=[btn]
        else:
            msg+=line
            kb.append(btn)
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

def broadcast_insider_accum(signals, period="daily", dest_chat_id=None):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target:
        print("broadcast_insider NO TARGET")
        return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    if not signals:
        send_reply(target, f"🕵️ INSIDER ACCUM {period.upper()} (400 BY VALUE): Tidak ada akum insider - coba /insider daily 50"); return
    header=f"*INSIDER ACCUM - AKUM DIAM-DIAM {period.upper()}* 🕵️ {now}\nTOP 400 BY VALUE | {len(signals)} saham\nLogic: Dekat low 20d, Buy%>=48%, Vol 0.5-2.5x\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        net_m=it['net_value']/1e9
        rp=it.get('range_pos',0)*100
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Score{it.get('insider_score',0)}/8 Net {net_m:+.2f}B Buy%{it['buy_pct']:.0f}% Pos{rp:.0f}% Vol{it['vol_ratio']:.1f}x\n"
        btn=[{"text": f"{it['symbol']} 🕵️ {rp:.0f}%", "callback_data": f"chart_{it['symbol']}"}]
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb})
            msg=line
            kb=[btn]
        else:
            msg+=line
            kb.append(btn)
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

# ==================== NEW V4.42 - EMA PROXIMITY SCANNER ====================

def scan_near_ema(tickers, ema_period=50, threshold_pct=3.0, limit=400, top_n=30, mode="near"):
    """
    mode: near, above, below, bounce, cross
    threshold_pct: jarak % dari EMA
    """
    print(f"🔍 SCAN NEAR EMA{ema_period} {mode} {threshold_pct}% - {len(tickers)} saham")
    detected=[]
    def check_one(sym):
        try:
            if sym in FCA_EXCLUDE: return None
            # jangan filter SUSPEND untuk EMA, biar tetap muncul kalau dekat EMA
            hd=get_history_pro(sym, 220, "1d")
            if hd is None or len(hd)<ema_period+10: return None
            last_close=float(hd['Close'].iloc[-1])
            if last_close < MIN_PRICE: return None
            ema = hd['Close'].ewm(span=ema_period, adjust=False).mean()
            ema_last=float(ema.iloc[-1])
            ema_prev=float(ema.iloc[-2]) if len(ema)>=2 else ema_last
            if ema_last==0: return None
            dist_pct=(last_close - ema_last)/ema_last*100
            abs_dist=abs(dist_pct)

            # Filter volume minimal biar tidak sampah (kecuali memang dekat)
            v_avg=hd['Volume'].tail(20).mean()
            if v_avg<100_000: # terlalu sepi
                pass

            prev_close=float(hd['Close'].iloc[-2]) if len(hd)>=2 else last_close
            chg=(last_close/prev_close-1)*100 if prev_close else 0
            rsi=calculate_rsi(hd['Close'],14)
            v_last=hd['Volume'].iloc[-1]; vol_ratio=v_last/v_avg if v_avg else 1

            # Logic mode
            valid=False
            if mode=="near":
                valid=abs_dist <= threshold_pct
            elif mode=="above":
                valid=dist_pct>=0 and dist_pct<=threshold_pct
            elif mode=="below":
                valid=dist_pct<=0 and abs_dist<=threshold_pct
            elif mode=="bounce":
                # bounce: kemarin di bawah EMA, sekarang di atas atau dekat
                prev_dist=(prev_close - ema_prev)/ema_prev*100 if ema_prev else 0
                valid=(prev_dist<0 and dist_pct>=0) or (abs_dist<=threshold_pct and chg>0 and rsi>40)
            elif mode=="pullback":
                # pullback dalam uptrend: close > EMA200, tapi dekat EMA50 dari atas
                ema200=hd['Close'].ewm(span=200, adjust=False).mean().iloc[-1]
                valid=(last_close>ema200*0.95) and (0 <= dist_pct <= threshold_pct) and (chg>-2)
            elif mode=="golden":
                # dekat EMA50 dan EMA200 (golden area)
                ema200=hd['Close'].ewm(span=200, adjust=False).mean().iloc[-1]
                dist200=(last_close - ema200)/ema200*100 if ema200 else 100
                valid=(abs_dist<=threshold_pct) and (abs(dist200)<=threshold_pct+2)

            if not valid: return None

            return {
                "symbol": sym,
                "close": int(last_close),
                "ema": int(ema_last),
                "ema_period": ema_period,
                "dist_pct": dist_pct,
                "abs_dist": abs_dist,
                "change_pct": chg,
                "vol_ratio": vol_ratio,
                "rsi": rsi,
                "mode": mode
            }
        except Exception as e:
            print(f"scan EMA{ema_period} err {sym}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs={ex.submit(check_one,s):s for s in tickers[:limit]}
        for f in __import__('concurrent.futures').as_completed(futs):
            r=f.result()
            if r: detected.append(r)

    detected.sort(key=lambda x: x['abs_dist'])
    return detected[:top_n]

def scan_near_ema50(threshold_pct=3.0, limit_candidates=400, top_n=30, mode="near"):
    tickers=get_top_liquid_tickers(limit_candidates)
    return scan_near_ema(tickers, ema_period=50, threshold_pct=threshold_pct, limit=limit_candidates, top_n=top_n, mode=mode)

def scan_near_ema200(threshold_pct=5.0, limit_candidates=400, top_n=30, mode="near"):
    tickers=get_top_liquid_tickers(limit_candidates)
    return scan_near_ema(tickers, ema_period=200, threshold_pct=threshold_pct, limit=limit_candidates, top_n=top_n, mode=mode)

def scan_ema_golden_area(threshold_pct=4.0, limit_candidates=400, top_n=30):
    """
    Saham di area EMA50 & EMA200 berdekatan (golden/death cross area) - harga dekat keduanya
    """
    tickers=get_top_liquid_tickers(limit_candidates)
    print(f"🔍 SCAN GOLDEN AREA EMA50+EMA200 {threshold_pct}% - {len(tickers)} saham")
    detected=[]
    def check_one(sym):
        try:
            if sym in FCA_EXCLUDE: return None
            hd=get_history_pro(sym, 220, "1d")
            if hd is None or len(hd)<210: return None
            last_close=float(hd['Close'].iloc[-1])
            if last_close < MIN_PRICE: return None
            ema50=hd['Close'].ewm(span=50, adjust=False).mean().iloc[-1]
            ema200=hd['Close'].ewm(span=200, adjust=False).mean().iloc[-1]
            if ema50==0 or ema200==0: return None
            dist50=(last_close - ema50)/ema50*100
            dist200=(last_close - ema200)/ema200*100
            dist_ema = (ema50 - ema200)/ema200*100 # jarak EMA50 vs EMA200
            abs_dist50=abs(dist50)
            abs_dist200=abs(dist200)
            # Valid kalau harga dekat kedua EMA dan EMA50 dekat EMA200 (golden area)
            if abs_dist50<=threshold_pct and abs_dist200<=threshold_pct+2 and abs(dist_ema)<=5:
                prev_close=float(hd['Close'].iloc[-2]) if len(hd)>=2 else last_close
                chg=(last_close/prev_close-1)*100 if prev_close else 0
                v_avg=hd['Volume'].tail(20).mean()
                v_last=hd['Volume'].iloc[-1]
                vol_ratio=v_last/v_avg if v_avg else 1
                rsi=calculate_rsi(hd['Close'],14)
                return {
                    "symbol": sym,
                    "close": int(last_close),
                    "ema50": int(ema50),
                    "ema200": int(ema200),
                    "dist50": dist50,
                    "dist200": dist200,
                    "dist_ema": dist_ema,
                    "abs_dist": (abs_dist50+abs_dist200)/2,
                    "change_pct": chg,
                    "vol_ratio": vol_ratio,
                    "rsi": rsi
                }
            return None
        except Exception as e:
            print(f"golden area err {sym}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs={ex.submit(check_one,s):s for s in tickers[:limit_candidates]}
        for f in __import__('concurrent.futures').as_completed(futs):
            r=f.result()
            if r: detected.append(r)

    detected.sort(key=lambda x: x['abs_dist'])
    return detected[:top_n]

def broadcast_ema_signals(signals, ema_period=50, mode="near", dest_chat_id=None):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target: return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    if not signals:
        send_reply(target, f"📊 EMA{ema_period} {mode.upper()} (400 BY VALUE): Tidak ada saham dekat EMA{ema_period} - coba threshold lebih besar /ema{ema_period} 5"); return
    header=f"*EMA{ema_period} {mode.upper()}* 📊 {now}\nTOP 400 BY VALUE | {len(signals)} saham | Filter: dekat EMA{ema_period} {mode}\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        dist=it['dist_pct']
        line=f"{idx}. *{it['symbol']}* {it['close']} EMA{ema_period}:{it['ema']} Dist:{dist:+.1f}% ({it['change_pct']:+.1f}%) RSI{it['rsi']:.0f} Vol{it['vol_ratio']:.1f}x\n"
        btn=[{"text": f"{it['symbol']} {dist:+.1f}% EMA{ema_period}", "callback_data": f"chart_{it['symbol']}"}]
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb})
            msg=line
            kb=[btn]
        else:
            msg+=line
            kb.append(btn)
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

def broadcast_golden_area(signals, dest_chat_id=None):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target: return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    if not signals:
        send_reply(target, "📊 GOLDEN AREA EMA50+200: Tidak ada saham di area golden"); return
    header=f"*GOLDEN AREA EMA50+EMA200* ✨ {now}\nTOP 400 BY VALUE | {len(signals)} saham\nLogic: Harga dekat EMA50 & EMA200, EMA50 dekat EMA200 (±5%)\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        line=f"{idx}. *{it['symbol']}* {it['close']} EMA50:{it['ema50']} ({it['dist50']:+.1f}%) EMA200:{it['ema200']} ({it['dist200']:+.1f}%) EMAgap:{it['dist_ema']:+.1f}% Vol{it['vol_ratio']:.1f}x\n"
        btn=[{"text": f"{it['symbol']} Gap{it['dist_ema']:+.1f}%", "callback_data": f"chart_{it['symbol']}"}]
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb})
            msg=line
            kb=[btn]
        else:
            msg+=line
            kb.append(btn)
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

# ==================== NEW V4.42 - CHART PATTERN SCANNER ====================

def detect_patterns(df):
    """
    Deteksi beberapa pattern sederhana dari 1 saham
    Return dict pattern -> bool, score
    """
    try:
        if df is None or len(df)<60: return {}
        close=df['Close']; high=df['High']; low=df['Low']; open_=df['Open']; vol=df['Volume']
        last_close=float(close.iloc[-1])
        prev_close=float(close.iloc[-2]) if len(close)>=2 else last_close

        ema20=close.ewm(span=20, adjust=False).mean()
        ema50=close.ewm(span=50, adjust=False).mean()
        ema200=close.ewm(span=200, adjust=False).mean()

        # RSI
        rsi=calculate_rsi(close,14)

        # Vol avg
        v_avg=vol.tail(20).mean()
        v_last=vol.iloc[-1]
        vol_ratio=v_last/v_avg if v_avg else 1

        patterns={}

        # 1. Bullish Breakout High 20
        high20=float(high.tail(20).max())
        # breakout kalau close > high20 sebelumnya (bukan termasuk hari ini)
        high20_prev=float(high.iloc[-21:-1].max()) if len(high)>=21 else high20
        if last_close > high20_prev * 0.995 and vol_ratio>=1.2 and last_close>float(ema20.iloc[-1]):
            patterns['breakout'] = {"score": 8, "desc": f"Breakout High20 {high20_prev:.0f} Vol{vol_ratio:.1f}x"}

        # 2. Pullback to EMA50 dalam uptrend
        ema50_last=float(ema50.iloc[-1]); ema200_last=float(ema200.iloc[-1])
        dist50=(last_close - ema50_last)/ema50_last*100 if ema50_last else 100
        if last_close>ema200_last*0.95 and 0<=dist50<=4 and close.iloc[-1]>open_.iloc[-1] and rsi>=45:
            patterns['pullback_ema50'] = {"score": 7, "desc": f"Pullback EMA50 {dist50:+.1f}% Uptrend"}

        # 3. Pullback to EMA200 (deep pullback)
        dist200=(last_close - ema200_last)/ema200_last*100 if ema200_last else 100
        if -3<=dist200<=5 and last_close>ema200_last*0.97 and rsi>=35 and rsi<=65:
            patterns['pullback_ema200'] = {"score": 6, "desc": f"Near EMA200 {dist200:+.1f}%"}

        # 4. Double Bottom (sederhana: 2 low mirip dalam 40 hari, low kedua lebih tinggi)
        try:
            lows = low.tail(40)
            # cari 2 lowest
            sorted_lows = lows.nsmallest(4)
            if len(sorted_lows)>=2:
                low1=float(sorted_lows.iloc[0]); low2=float(sorted_lows.iloc[1])
                # jarak low tidak jauh
                diff_pct=abs(low1-low2)/((low1+low2)/2)*100 if (low1+low2)>0 else 100
                if diff_pct<=6:
                    # low kedua lebih tinggi dari pertama? atau minimal tidak lebih rendah jauh
                    # dan harga sekarang sudah naik >3% dari low
                    if last_close > max(low1,low2)*1.03:
                        patterns['double_bottom'] = {"score": 7, "desc": f"Double Bottom {low1:.0f}-{low2:.0f} diff{diff_pct:.1f}%"}
        except: pass

        # 5. Bull Flag (naik tajam 15% dalam 10 hari, lalu konsolidasi 5-15 hari dengan vol turun)
        try:
            close_10_ago=float(close.iloc[-11]) if len(close)>=11 else last_close
            gain_10=(last_close/close_10_ago-1)*100 if close_10_ago else 0
            # konsolidasi: high-low range 10 hari terakhir kecil <8%
            high10=float(high.tail(10).max()); low10=float(low.tail(10).min())
            range10=(high10-low10)/((high10+low10)/2)*100 if (high10+low10)>0 else 100
            vol_10_avg=vol.tail(10).mean()
            vol_20_avg=vol.tail(20).mean()
            if gain_10>=12 and range10<=8 and vol_10_avg < vol_20_avg*1.1:
                patterns['bull_flag'] = {"score": 6, "desc": f"Bull Flag +{gain_10:.0f}% konsolidasi {range10:.1f}%"}
        except: pass

        # 6. Hammer / Bullish Engulfing di dekat support
        try:
            o=float(open_.iloc[-1]); h=float(high.iloc[-1]); l=float(low.iloc[-1]); c=float(close.iloc[-1])
            body=abs(c-o); lower_shadow=min(o,c)-l; upper_shadow=h-max(o,c)
            # Hammer: lower shadow >2x body, upper shadow kecil
            if lower_shadow > body*2 and upper_shadow < body*0.8 and body>0 and c>o:
                patterns['hammer'] = {"score": 6, "desc": "Hammer bullish"}
            # Bullish engulfing: hari ini hijau, kemarin merah, body hari ini > body kemarin
            if len(close)>=2:
                o_prev=float(open_.iloc[-2]); c_prev=float(close.iloc[-2])
                if c>o and c_prev<o_prev and c>o_prev and o<c_prev and (c-o)>(o_prev-c_prev):
                    patterns['bull_engulfing'] = {"score": 7, "desc": "Bullish Engulfing"}
        except: pass

        # 7. Triangle / Squeeze (BB menyempit + volume turun)
        try:
            sma20, up_bb, low_bb = calculate_bollinger_bands(df,20,2)
            bb_width=(up_bb.iloc[-1]-low_bb.iloc[-1])/sma20.iloc[-1]*100 if sma20.iloc[-1] else 100
            bb_width_prev=(up_bb.iloc[-11]-low_bb.iloc[-11])/sma20.iloc[-11]*100 if len(sma20)>=11 and sma20.iloc[-11] else bb_width
            if bb_width < bb_width_prev*0.85 and bb_width<15 and vol_ratio<1.2:
                patterns['squeeze'] = {"score": 5, "desc": f"Squeeze BB {bb_width:.1f}% < {bb_width_prev:.1f}%"}
        except: pass

        return patterns
    except Exception as e:
        print(f"detect_patterns err: {e}")
        return {}

def scan_chart_patterns(pattern_name="all", limit_candidates=400, top_n=30):
    tickers=get_top_liquid_tickers(limit_candidates)
    print(f"🔍 SCAN CHART PATTERN {pattern_name} - {len(tickers)} saham")
    detected=[]
    def check_one(sym):
        try:
            if sym in FCA_EXCLUDE: return None
            hd=get_history_pro(sym, 100, "1d")
            if hd is None or len(hd)<60: return None
            last_close=float(hd['Close'].iloc[-1])
            if last_close < MIN_PRICE: return None
            patterns=detect_patterns(hd)
            if not patterns: return None
            # filter by requested pattern
            if pattern_name!="all" and pattern_name not in patterns:
                return None
            prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
            chg=(last_close/prev-1)*100 if prev else 0
            v_avg=hd['Volume'].tail(20).mean()
            v_last=hd['Volume'].iloc[-1]
            vol_ratio=v_last/v_avg if v_avg else 1
            rsi=calculate_rsi(hd['Close'],14)

            if pattern_name=="all":
                # ambil pattern dengan score tertinggi
                best_key=max(patterns, key=lambda k: patterns[k]['score'])
                best=patterns[best_key]
                return {
                    "symbol": sym,
                    "close": int(last_close),
                    "change_pct": chg,
                    "vol_ratio": vol_ratio,
                    "rsi": rsi,
                    "pattern": best_key,
                    "pattern_desc": best['desc'],
                    "score": best['score'],
                    "all_patterns": patterns
                }
            else:
                p=patterns[pattern_name]
                return {
                    "symbol": sym,
                    "close": int(last_close),
                    "change_pct": chg,
                    "vol_ratio": vol_ratio,
                    "rsi": rsi,
                    "pattern": pattern_name,
                    "pattern_desc": p['desc'],
                    "score": p['score'],
                    "all_patterns": patterns
                }
        except Exception as e:
            print(f"scan pattern err {sym}: {e}")
            return None

    with ThreadPoolExecutor(max_workers=12) as ex:
        futs={ex.submit(check_one,s):s for s in tickers[:limit_candidates]}
        for f in __import__('concurrent.futures').as_completed(futs):
            r=f.result()
            if r: detected.append(r)

    detected.sort(key=lambda x: x['score'], reverse=True)
    return detected[:top_n]


def broadcast_patterns(signals, pattern_name="all", dest_chat_id=None):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target: return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    if not signals:
        send_reply(target, f"📈 PATTERN {pattern_name.upper()} (400 BY VALUE): Tidak ada pattern - coba /pattern all atau /pattern breakout"); return
    header=f"*CHART PATTERN {pattern_name.upper()}* 📈 {now}\nTOP 400 BY VALUE | {len(signals)} saham\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) {it['pattern'].upper()} Score{it['score']} - {it['pattern_desc']} RSI{it['rsi']:.0f} Vol{it['vol_ratio']:.1f}x\n"
        btn=[{"text": f"{it['symbol']} {it['pattern']}", "callback_data": f"chart_{it['symbol']}"}]
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb})
            msg=line
            kb=[btn]
        else:
            msg+=line
            kb.append(btn)
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

# ==================== NEW V4.43 - POINT A: CONFLUENCE SCORE (SMART FILTER) ====================

def calculate_confluence_score(sym, df, bandar_analysis=None):
    try:
        if df is None or len(df)<60: return 0, [], {}
        close=df['Close']; high=df['High']; low=df['Low']; open_=df['Open']; vol=df['Volume']
        last_close=float(close.iloc[-1])
        prev_close=float(close.iloc[-2]) if len(close)>=2 else last_close
        chg=(last_close/prev_close-1)*100 if prev_close else 0

        ema20=float(close.ewm(span=20, adjust=False).mean().iloc[-1])
        ema50=float(close.ewm(span=50, adjust=False).mean().iloc[-1])
        ema200=float(close.ewm(span=200, adjust=False).mean().iloc[-1]) if len(close)>=200 else float(close.mean())

        rsi=calculate_rsi(close,14)
        v_avg=float(vol.tail(20).mean()) if len(vol)>=20 else float(vol.mean())
        v_last=float(vol.iloc[-1])
        vol_ratio=v_last/v_avg if v_avg else 1

        score=0
        triggers=[]
        detail={}

        dist50=(last_close - ema50)/ema50*100 if ema50 else 100
        dist200=(last_close - ema200)/ema200*100 if ema200 else 100
        dist_ema=(ema50 - ema200)/ema200*100 if ema200 else 100

        if abs(dist50)<=3:
            score+=2; triggers.append(f"EMA50 {dist50:+.1f}%")
            detail['ema50_dist']=dist50
            if 0<=dist50<=3 and last_close>ema200*0.95:
                score+=1; triggers.append("Pullback EMA50 Uptrend")
        if abs(dist200)<=5:
            score+=1; triggers.append(f"EMA200 {dist200:+.1f}%")
            detail['ema200_dist']=dist200
        if abs(dist50)<=4 and abs(dist200)<=6 and abs(dist_ema)<=5:
            score+=2; triggers.append(f"Golden Area Gap{dist_ema:+.1f}%")
            detail['golden_gap']=dist_ema

        if last_close>ema200*0.98:
            score+=1; triggers.append("Above EMA200")
        if ema50>ema200:
            score+=1; triggers.append("EMA50>EMA200 Uptrend")

        patterns=detect_patterns(df)
        if patterns:
            for pk,pv in patterns.items():
                sc=pv.get('score',0)
                if pk=='breakout' and sc>=6:
                    score+=2; triggers.append("Breakout High20")
                elif pk=='double_bottom':
                    score+=2; triggers.append("Double Bottom")
                elif pk=='bull_engulfing':
                    score+=2; triggers.append("Bull Engulfing")
                elif pk in ['pullback_ema50','pullback_ema200']:
                    score+=2; triggers.append(pk)
                elif pk=='hammer':
                    score+=1; triggers.append("Hammer")
                elif pk=='bull_flag':
                    score+=1; triggers.append("Bull Flag")
                elif pk=='squeeze':
                    score+=1; triggers.append("Squeeze")
            detail['patterns']=list(patterns.keys())

        if bandar_analysis:
            net=bandar_analysis.get('net_value',0)
            buy_pct=bandar_analysis.get('buy_pct',50)
            if net>0:
                if abs(net)>=1_000_000_000:
                    score+=3; triggers.append(f"Whale +{net/1e9:.1f}B")
                elif abs(net)>=100_000_000:
                    score+=2; triggers.append(f"Akum +{net/1e9:.1f}B")
                else:
                    score+=1; triggers.append("Akum kecil")
            if buy_pct>=60:
                score+=1; triggers.append(f"Buy%{buy_pct:.0f}%")
            detail['net_value']=net
            detail['buy_pct']=buy_pct

        if vol_ratio>=2.0:
            score+=2; triggers.append(f"Vol {vol_ratio:.1f}x Power")
        elif vol_ratio>=1.5:
            score+=1; triggers.append(f"Vol {vol_ratio:.1f}x")

        if 45<=rsi<=65:
            score+=1; triggers.append(f"RSI {rsi:.0f} Optimal")
            detail['rsi']=rsi
        elif 35<=rsi<=75:
            detail['rsi']=rsi

        score=min(score,15)
        return score, triggers, detail
    except Exception as e:
        print(f"confluence err {sym}: {e}")
        return 0, [], {}

def scan_confluence(min_score=6, limit_candidates=400, top_n=30, mode="all"):
    tickers=get_top_liquid_tickers(limit_candidates)
    print(f"SCAN CONFLUENCE min_score {min_score} mode {mode} - {len(tickers)} saham")
    detected=[]
    def check_one(sym):
        try:
            if sym in FCA_EXCLUDE: return None
            hd=get_history_pro(sym, 220, "1d")
            if hd is None or len(hd)<60: return None
            last_close=float(hd['Close'].iloc[-1])
            if last_close < MIN_PRICE: return None
            bandar=get_bandar_analysis(sym, "daily")
            score, triggers, detail=calculate_confluence_score(sym, hd, bandar)
            if score < min_score: return None
            if mode=="bullish" and score<7: return None
            if mode=="safe" and 'Golden Area' not in ''.join(triggers) and 'Pullback' not in ''.join(triggers):
                if score<8: return None
            prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
            chg=(last_close/prev-1)*100 if prev else 0
            v_avg=hd['Volume'].tail(20).mean()
            vol_ratio=hd['Volume'].iloc[-1]/v_avg if v_avg else 1
            rsi=calculate_rsi(hd['Close'],14)
            return {
                "symbol": sym,
                "close": int(last_close),
                "change_pct": chg,
                "vol_ratio": vol_ratio,
                "rsi": rsi,
                "score": score,
                "triggers": triggers,
                "detail": detail,
                "bandar": bandar
            }
        except Exception as e:
            print(f"confluence check err {sym}: {e}")
            return None
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
        futs={ex.submit(check_one,s):s for s in tickers[:limit_candidates]}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)
    detected.sort(key=lambda x: x['score'], reverse=True)
    return detected[:top_n]

def broadcast_confluence(signals, min_score=6, dest_chat_id=None):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target: return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    if not signals:
        send_reply(target, f"CONFLUENCE min_score {min_score} (400 BY VALUE): Tidak ada saham dengan confluence tinggi - coba /confluence 5"); return
    header=f"*CONFLUENCE SCORE >= {min_score}* {now}\nTOP 400 BY VALUE | {len(signals)} saham | Gabungan EMA+Pattern+Bandar+Volume\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        trig_str=", ".join(it['triggers'][:4])
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Score{it['score']}/15 - {trig_str}\n"
        if len(it['triggers'])>4:
            line+=f"   +{len(it['triggers'])-4} more: {', '.join(it['triggers'][4:7])}\n"
        btn=[{"text": f"{it['symbol']} Score{it['score']}", "callback_data": f"chart_{it['symbol']}"}]
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb})
            msg=line
            kb=[btn]
        else:
            msg+=line
            kb.append(btn)
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

# ==================== NEW V4.43 - POINT B: SAFETY & POWER FILTER ====================

def calculate_safety_score(sym, df, bandar_analysis=None):
    try:
        if df is None or len(df)<60: return 0, []
        close=df['Close']; high=df['High']; low=df['Low']
        last_close=float(close.iloc[-1])
        ema50=float(close.ewm(span=50, adjust=False).mean().iloc[-1])
        ema200=float(close.ewm(span=200, adjust=False).mean().iloc[-1]) if len(close)>=200 else float(close.mean())
        rsi=calculate_rsi(close,14)
        score=0
        reasons=[]
        dist200=(last_close - ema200)/ema200*100 if ema200 else 0
        if -10 <= dist200 <= 15:
            score+=2; reasons.append(f"EMA200 {dist200:+.1f}% Aman")
        elif -15 <= dist200 <= 20:
            score+=1; reasons.append(f"EMA200 {dist200:+.1f}%")
        else:
            reasons.append(f"EMA200 {dist200:+.1f}% Jauh")
        if 35 <= rsi <= 70:
            score+=2; reasons.append(f"RSI {rsi:.0f} Aman")
        elif 30 <= rsi <= 75:
            score+=1; reasons.append(f"RSI {rsi:.0f}")
        else:
            reasons.append(f"RSI {rsi:.0f} Risky")
        if bandar_analysis:
            buy_pct=bandar_analysis.get('buy_pct',50)
            net=bandar_analysis.get('net_value',0)
            if buy_pct>=55 and net>=0:
                score+=2; reasons.append(f"Bandar Buy%{buy_pct:.0f}%")
            elif buy_pct>=48:
                score+=1; reasons.append(f"Bandar Buy%{buy_pct:.0f}%")
            else:
                reasons.append(f"Bandar Buy%{buy_pct:.0f}% Dist")
        high20=float(high.tail(20).max()); low20=float(low.tail(20).min())
        range20=(high20-low20)/((high20+low20)/2)*100 if (high20+low20)>0 else 0
        if range20 <= 15:
            score+=2; reasons.append(f"Range20 {range20:.0f}% Kalem")
        elif range20 <= 25:
            score+=1; reasons.append(f"Range20 {range20:.0f}%")
        else:
            reasons.append(f"Range20 {range20:.0f}% Volatile")
        dist_low=(last_close - low20)/low20*100 if low20 else 0
        if 0 <= dist_low <= 20:
            score+=2; reasons.append(f"Support {dist_low:.0f}% dekat")
        elif dist_low <= 35:
            score+=1
        score=min(score,10)
        return score, reasons
    except Exception as e:
        print(f"safety err {sym}: {e}")
        return 0, []

def calculate_power_score(sym, df, bandar_analysis=None):
    try:
        if df is None or len(df)<60: return 0, []
        close=df['Close']; vol=df['Volume']
        last_close=float(close.iloc[-1])
        prev_close=float(close.iloc[-2]) if len(close)>=2 else last_close
        chg=(last_close/prev_close-1)*100 if prev_close else 0
        ema20=float(close.ewm(span=20, adjust=False).mean().iloc[-1])
        rsi=calculate_rsi(close,14)
        rsi_prev=calculate_rsi(close.iloc[:-1],14) if len(close)>=15 else rsi
        v_avg=float(vol.tail(20).mean())
        v_last=float(vol.iloc[-1])
        vol_ratio=v_last/v_avg if v_avg else 1
        score=0
        reasons=[]
        if vol_ratio>=2.5:
            score+=3; reasons.append(f"Vol {vol_ratio:.1f}x Power")
        elif vol_ratio>=1.8:
            score+=2; reasons.append(f"Vol {vol_ratio:.1f}x")
        elif vol_ratio>=1.3:
            score+=1; reasons.append(f"Vol {vol_ratio:.1f}x")
        if chg>=3:
            score+=2; reasons.append(f"Chg +{chg:.1f}%")
        elif chg>=1:
            score+=1; reasons.append(f"Chg +{chg:.1f}%")
        if rsi>=55 and rsi<=75 and rsi>=rsi_prev:
            score+=2; reasons.append(f"RSI {rsi:.0f} Momentum Naik")
        elif rsi>=50:
            score+=1; reasons.append(f"RSI {rsi:.0f}")
        if last_close>ema20:
            score+=1; reasons.append("Above EMA20")
        if bandar_analysis:
            net=bandar_analysis.get('net_value',0)
            if abs(net)>=2_000_000_000 and net>0:
                score+=3; reasons.append(f"Bandar +{net/1e9:.1f}B")
            elif abs(net)>=500_000_000 and net>0:
                score+=2; reasons.append(f"Bandar +{net/1e9:.1f}B")
            elif net>0:
                score+=1
        score=min(score,10)
        return score, reasons
    except Exception as e:
        print(f"power err {sym}: {e}")
        return 0, []

def scan_safety_power(min_safety=6, min_power=5, limit_candidates=400, top_n=30, mode="both"):
    tickers=get_top_liquid_tickers(limit_candidates)
    print(f"SCAN SAFETY {min_safety} POWER {min_power} mode {mode} - {len(tickers)} saham")
    detected=[]
    def check_one(sym):
        try:
            if sym in FCA_EXCLUDE: return None
            hd=get_history_pro(sym, 220, "1d")
            if hd is None or len(hd)<60: return None
            last_close=float(hd['Close'].iloc[-1])
            if last_close < MIN_PRICE: return None
            bandar=get_bandar_analysis(sym, "daily")
            safety, safety_reasons=calculate_safety_score(sym, hd, bandar)
            power, power_reasons=calculate_power_score(sym, hd, bandar)
            if mode=="safe" and safety < min_safety: return None
            if mode=="power" and power < min_power: return None
            if mode=="both" and (safety < min_safety or power < min_power): return None
            if mode=="safe_power" and not (safety>=7 and power>=6): return None
            prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
            chg=(last_close/prev-1)*100 if prev else 0
            v_avg=hd['Volume'].tail(20).mean()
            vol_ratio=hd['Volume'].iloc[-1]/v_avg if v_avg else 1
            rsi=calculate_rsi(hd['Close'],14)
            return {
                "symbol": sym,
                "close": int(last_close),
                "change_pct": chg,
                "vol_ratio": vol_ratio,
                "rsi": rsi,
                "safety": safety,
                "power": power,
                "total": safety+power,
                "safety_reasons": safety_reasons,
                "power_reasons": power_reasons,
                "bandar": bandar
            }
        except Exception as e:
            print(f"safety_power err {sym}: {e}")
            return None
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
        futs={ex.submit(check_one,s):s for s in tickers[:limit_candidates]}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)
    if mode=="safe":
        detected.sort(key=lambda x: x['safety'], reverse=True)
    elif mode=="power":
        detected.sort(key=lambda x: x['power'], reverse=True)
    else:
        detected.sort(key=lambda x: x['total'], reverse=True)
    return detected[:top_n]

def broadcast_safety_power(signals, min_safety=6, min_power=5, mode="both", dest_chat_id=None):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target: return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    if not signals:
        send_reply(target, f"SAFETY {min_safety} POWER {min_power} ({mode}): Tidak ada saham memenuhi - coba turunkan threshold /safety 5 4"); return
    header=f"*SAFETY {min_safety} POWER {min_power} ({mode.upper()})* {now}\nTOP 400 BY VALUE | {len(signals)} saham | Safety max 10, Power max 10\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        safe_str="S"*it['safety']
        power_str="P"*it['power']
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) S:{it['safety']}/10 P:{it['power']}/10 Total:{it['total']}/20 RSI{it['rsi']:.0f} Vol{it['vol_ratio']:.1f}x\n"
        if it['safety_reasons']:
            line+=f"   Safe: {', '.join(it['safety_reasons'][:2])}\n"
        if it['power_reasons']:
            line+=f"   Power: {', '.join(it['power_reasons'][:2])}\n"
        btn=[{"text": f"{it['symbol']} S{it['safety']} P{it['power']}", "callback_data": f"chart_{it['symbol']}"}]
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb})
            msg=line
            kb=[btn]
        else:
            msg+=line
            kb.append(btn)
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

# ==================== NEW V4.43 - POINT D: ORDER BLOCK & FVG (SMC SIMPLIFIED) ====================

def find_order_blocks(df, lookback=50, min_impulse_pct=4.0):
    try:
        if df is None or len(df)<lookback+10: return []
        close=df['Close']; open_=df['Open']; high=df['High']; low=df['Low']; vol=df['Volume']
        obs=[]
        for i in range(len(df)-10, len(df)-lookback, -1):
            if i<3 or i>=len(df)-3: continue
            try:
                c_now=float(close.iloc[i])
                c_next3=float(close.iloc[i+3]) if i+3 < len(df) else c_now
                impulse=(c_next3/c_now-1)*100 if c_now else 0
                o_i=float(open_.iloc[i]); c_i=float(close.iloc[i])
                if o_i > c_i and impulse >= min_impulse_pct:
                    v_avg=float(vol.iloc[max(0,i-10):i].mean()) if i>=10 else float(vol.mean())
                    v_impulse=float(vol.iloc[i+1:i+4].mean()) if i+4 < len(df) else v_avg
                    vol_ratio=v_impulse/v_avg if v_avg else 1
                    if vol_ratio>=1.2 or impulse>=6:
                        ob={
                            "type": "bullish",
                            "high": float(high.iloc[i]),
                            "low": float(low.iloc[i]),
                            "mid": (float(high.iloc[i])+float(low.iloc[i]))/2,
                            "idx": i,
                            "date": str(df.index[i])[:10],
                            "impulse_pct": impulse,
                            "vol_ratio": vol_ratio,
                            "strength": min(10, int(impulse + vol_ratio)),
                            "price": float(low.iloc[i])
                        }
                        obs.append(ob)
                c_next3_bear=float(close.iloc[i+3]) if i+3 < len(df) else c_now
                impulse_bear=(c_next3_bear/c_now-1)*100 if c_now else 0
                if o_i < c_i and impulse_bear <= -min_impulse_pct:
                    v_avg=float(vol.iloc[max(0,i-10):i].mean()) if i>=10 else float(vol.mean())
                    v_impulse=float(vol.iloc[i+1:i+4].mean()) if i+4 < len(df) else v_avg
                    vol_ratio=v_impulse/v_avg if v_avg else 1
                    if vol_ratio>=1.2 or abs(impulse_bear)>=6:
                        ob={
                            "type": "bearish",
                            "high": float(high.iloc[i]),
                            "low": float(low.iloc[i]),
                            "mid": (float(high.iloc[i])+float(low.iloc[i]))/2,
                            "idx": i,
                            "date": str(df.index[i])[:10],
                            "impulse_pct": impulse_bear,
                            "vol_ratio": vol_ratio,
                            "strength": min(10, int(abs(impulse_bear) + vol_ratio)),
                            "price": float(high.iloc[i])
                        }
                        obs.append(ob)
            except: continue
        obs_sorted=sorted(obs, key=lambda x: (x['strength'], -x['idx']), reverse=True)
        return obs_sorted[:5]
    except Exception as e:
        print(f"find OB err: {e}")
        return []

def find_fvgs(df, lookback=50):
    try:
        if df is None or len(df)<lookback+5: return []
        high=df['High']; low=df['Low']
        fvgs=[]
        for i in range(len(df)-lookback, len(df)-2):
            if i<2: continue
            try:
                h_0=float(high.iloc[i-2]); l_2=float(low.iloc[i])
                if l_2 > h_0:
                    gap_size=(l_2 - h_0)/h_0*100 if h_0 else 0
                    if 0.2 <= gap_size <= 8:
                        fvgs.append({
                            "type": "bullish",
                            "high": l_2,
                            "low": h_0,
                            "mid": (l_2+h_0)/2,
                            "gap_pct": gap_size,
                            "idx": i,
                            "date": str(df.index[i])[:10],
                            "strength": min(10, int(gap_size*2 + 2))
                        })
                l_0=float(low.iloc[i-2]); h_2=float(high.iloc[i])
                if h_2 < l_0:
                    gap_size=(l_0 - h_2)/l_0*100 if l_0 else 0
                    if 0.2 <= gap_size <= 8:
                        fvgs.append({
                            "type": "bearish",
                            "high": l_0,
                            "low": h_2,
                            "mid": (l_0+h_2)/2,
                            "gap_pct": gap_size,
                            "idx": i,
                            "date": str(df.index[i])[:10],
                            "strength": min(10, int(gap_size*2 + 2))
                        })
            except: continue
        fvgs_sorted=sorted(fvgs, key=lambda x: (x['strength'], -x['idx']), reverse=True)
        return fvgs_sorted[:5]
    except Exception as e:
        print(f"find FVG err: {e}")
        return []

def scan_order_block(limit_candidates=400, top_n=30, mode="bullish", threshold_pct=5.0):
    tickers=get_top_liquid_tickers(limit_candidates)
    print(f"SCAN ORDER BLOCK {mode} {threshold_pct}% - {len(tickers)} saham")
    detected=[]
    def check_one(sym):
        try:
            if sym in FCA_EXCLUDE: return None
            hd=get_history_pro(sym, 100, "1d")
            if hd is None or len(hd)<60: return None
            last_close=float(hd['Close'].iloc[-1])
            if last_close < MIN_PRICE: return None
            obs=find_order_blocks(hd, lookback=50, min_impulse_pct=4.0)
            if not obs: return None
            filtered=[ob for ob in obs if ob['type']==mode or mode=="both"]
            if not filtered: return None
            best_ob=None
            best_dist=999
            for ob in filtered:
                dist=(last_close - ob['mid'])/ob['mid']*100 if ob['mid'] else 999
                abs_dist=abs(dist)
                if abs_dist <= threshold_pct and abs_dist < best_dist:
                    best_dist=abs_dist
                    best_ob=ob
            if not best_ob: return None
            prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
            chg=(last_close/prev-1)*100 if prev else 0
            v_avg=hd['Volume'].tail(20).mean()
            vol_ratio=hd['Volume'].iloc[-1]/v_avg if v_avg else 1
            rsi=calculate_rsi(hd['Close'],14)
            return {
                "symbol": sym,
                "close": int(last_close),
                "change_pct": chg,
                "vol_ratio": vol_ratio,
                "rsi": rsi,
                "ob_type": best_ob['type'],
                "ob_high": int(best_ob['high']),
                "ob_low": int(best_ob['low']),
                "ob_mid": int(best_ob['mid']),
                "ob_date": best_ob['date'],
                "ob_impulse": best_ob['impulse_pct'],
                "ob_strength": best_ob['strength'],
                "dist_pct": (last_close - best_ob['mid'])/best_ob['mid']*100,
                "abs_dist": best_dist,
                "ob": best_ob
            }
        except Exception as e:
            print(f"OB scan err {sym}: {e}")
            return None
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
        futs={ex.submit(check_one,s):s for s in tickers[:limit_candidates]}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)
    detected.sort(key=lambda x: (x['ob_strength'], -x['abs_dist']), reverse=True)
    return detected[:top_n]

def scan_fvg(limit_candidates=400, top_n=30, mode="bullish", threshold_pct=5.0):
    tickers=get_top_liquid_tickers(limit_candidates)
    print(f"SCAN FVG {mode} {threshold_pct}% - {len(tickers)} saham")
    detected=[]
    def check_one(sym):
        try:
            if sym in FCA_EXCLUDE: return None
            hd=get_history_pro(sym, 100, "1d")
            if hd is None or len(hd)<60: return None
            last_close=float(hd['Close'].iloc[-1])
            if last_close < MIN_PRICE: return None
            fvgs=find_fvgs(hd, lookback=50)
            if not fvgs: return None
            filtered=[f for f in fvgs if f['type']==mode or mode=="both"]
            if not filtered: return None
            best_fvg=None
            best_dist=999
            for fvg in filtered:
                dist=(last_close - fvg['mid'])/fvg['mid']*100 if fvg['mid'] else 999
                abs_dist=abs(dist)
                if abs_dist <= threshold_pct and abs_dist < best_dist:
                    best_dist=abs_dist
                    best_fvg=fvg
            if not best_fvg: return None
            prev=hd['Close'].iloc[-2] if len(hd)>=2 else last_close
            chg=(last_close/prev-1)*100 if prev else 0
            v_avg=hd['Volume'].tail(20).mean()
            vol_ratio=hd['Volume'].iloc[-1]/v_avg if v_avg else 1
            rsi=calculate_rsi(hd['Close'],14)
            return {
                "symbol": sym,
                "close": int(last_close),
                "change_pct": chg,
                "vol_ratio": vol_ratio,
                "rsi": rsi,
                "fvg_type": best_fvg['type'],
                "fvg_high": int(best_fvg['high']),
                "fvg_low": int(best_fvg['low']),
                "fvg_mid": int(best_fvg['mid']),
                "fvg_date": best_fvg['date'],
                "fvg_gap": best_fvg['gap_pct'],
                "fvg_strength": best_fvg['strength'],
                "dist_pct": (last_close - best_fvg['mid'])/best_fvg['mid']*100,
                "abs_dist": best_dist,
                "fvg": best_fvg
            }
        except Exception as e:
            print(f"FVG scan err {sym}: {e}")
            return None
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS) as ex:
        futs={ex.submit(check_one,s):s for s in tickers[:limit_candidates]}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)
    detected.sort(key=lambda x: (x['fvg_strength'], -x['abs_dist']), reverse=True)
    return detected[:top_n]

def broadcast_ob(signals, mode="bullish", dest_chat_id=None):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target: return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    if not signals:
        send_reply(target, f"ORDER BLOCK {mode.upper()} (400 BY VALUE): Tidak ada saham dekat OB - coba /ob bullish 5"); return
    header=f"*ORDER BLOCK {mode.upper()}* {now}\nTOP 400 BY VALUE | {len(signals)} saham | Logic: Pullback ke OB setelah impulse\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) OB:{it['ob_low']}-{it['ob_high']} Mid:{it['ob_mid']} Dist:{it['dist_pct']:+.1f}% Impulse{it['ob_impulse']:+.1f}% Strength{it['ob_strength']} {it['ob_date']} RSI{it['rsi']:.0f}\n"
        btn=[{"text": f"{it['symbol']} OB {it['dist_pct']:+.1f}%", "callback_data": f"chart_{it['symbol']}"}]
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb})
            msg=line
            kb=[btn]
        else:
            msg+=line
            kb.append(btn)
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

def broadcast_fvg(signals, mode="bullish", dest_chat_id=None):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target: return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    if not signals:
        send_reply(target, f"FVG {mode.upper()} (400 BY VALUE): Tidak ada saham dekat FVG"); return
    header=f"*FVG {mode.upper()}* {now}\nTOP 400 BY VALUE | {len(signals)} saham | Logic: Fair Value Gap\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) FVG:{it['fvg_low']}-{it['fvg_high']} Mid:{it['fvg_mid']} Dist:{it['dist_pct']:+.1f}% Gap{it['fvg_gap']:.1f}% Strength{it['fvg_strength']} {it['fvg_date']} RSI{it['rsi']:.0f}\n"
        btn=[{"text": f"{it['symbol']} FVG {it['dist_pct']:+.1f}%", "callback_data": f"chart_{it['symbol']}"}]
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb})
            msg=line
            kb=[btn]
        else:
            msg+=line
            kb.append(btn)
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

# ==================== END NEW V4.43 ====================


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
        # FIX V4.44: pakai plain text tanpa markdown biar pasti sampai
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            requests.post(url, json={"chat_id":cid, "text": f"{code.upper()} ({tf_label}) chart pro..."}, timeout=10)
        except:
            try: send_reply(cid, f"{code.upper()} ({tf_label}) chart pro...")
            except: pass
        df=get_history_pro(code, 150, frame=tf_norm)
        print(f"get_history {code} {tf_norm} rows={len(df) if df is not None else 'None'}")

        # FIX V4.44: retry super agresif kalau daily gagal
        if (df is None or len(df)<10):
            print(f"Daily fail {code}, coba clear & retry YF multi period")
            for k in list(HISTORY_CACHE.keys()):
                if code.upper() in k:
                    HISTORY_CACHE.pop(k, None)
            SUSPEND_CACHE.pop(code.upper(), None)
            # retry YF dengan banyak period
            try:
                import yfinance as yf
                for per in ["1y","6mo","3mo","1mo","2y"]:
                    try:
                        hist=yf.Ticker(f"{code.upper()}.JK").history(period=per, interval="1d", timeout=20, auto_adjust=False)
                        if hist is not None and len(hist)>=10:
                            last_close=float(hist['Close'].iloc[-1])
                            if last_close>=MIN_PRICE and not (hist['Close'].nunique()==1 and int(hist['Close'].iloc[0])==100):
                                df=hist
                                set_cached(f"{code.upper()}_{tf_norm}_150", df, HISTORY_CACHE)
                                print(f"Retry YF {per} success {code} len={len(df)}")
                                break
                    except Exception as e:
                        print(f"Retry YF {per} err {code}: {e}")
                        continue
                # coba yf.download
                if (df is None or len(df)<10):
                    try:
                        df_dl=yf.download(f"{code.upper()}.JK", period="1y", interval="1d", progress=False, timeout=20, auto_adjust=False)
                        if df_dl is not None and len(df_dl)>=10:
                            last_close=float(df_dl['Close'].iloc[-1])
                            if last_close>=MIN_PRICE:
                                df=df_dl
                                print(f"yf.download success {code}")
                    except Exception as e:
                        print(f"yf.download err {code}: {e}")
            except Exception as e:
                print(f"Retry YF err {code}: {e}")

        if df is None or len(df)<10:
            # Fallback ke TF 5m kalau daily benar-benar gagal
            if tf_norm=="1d":
                print(f"Trying 5m fallback for {code}")
                df_5m=get_history_pro(code, 150, frame="5m")
                if df_5m is not None and len(df_5m)>=20:
                    try:
                        url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                        requests.post(url, json={"chat_id":cid, "text": f"Daily fail, pakai 5m chart untuk {code.upper()}"}, timeout=10)
                    except: pass
                    df=df_5m
                    tf_norm="5m"
                    tf_label=format_timeframe_label(tf_norm)
            if df is None or len(df)<10:
                try:
                    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                    requests.post(url, json={"chat_id":cid, "text": f"Data {code} TF {tf_norm} kosong - YF/Arjum fail, coba /c {code.upper()} 5"}, timeout=10)
                except:
                    send_reply(cid, f"Data {code} TF {tf_norm} kosong - coba /c {code.upper()} 5")
                print(f"CHART ABORT no data {code} {tf_norm}")
                return

        stockbit_price,_=get_realtime_stockbit(code)
        realtime=stockbit_price if stockbit_price>=MIN_PRICE else 0
        print(f"realtime={realtime} hist={df['Close'].iloc[-1]}")
        chart_file=f"/tmp/chart_{code.upper()}_{tf_norm}_{int(time.time())}.png"
        try:
            fp,_=generate_pro_chart(df, symbol=code.upper(), timeframe=tf_norm, output_filename=chart_file, extra_info={'tf_label':tf_label}, realtime_price=realtime)
        except Exception as e:
            print(f"generate_pro_chart err {e}, try simple")
            fp=None
        print(f"generate_pro_chart fp={fp} exists={os.path.exists(fp) if fp else False}")
        if not fp or not os.path.exists(fp):
            # FALLBACK SIMPLE CHART
            print(f"Trying simple chart fallback for {code}")
            try:
                import matplotlib
                matplotlib.use('Agg')
                import matplotlib.pyplot as plt
                plt.figure(figsize=(12,6), facecolor='black')
                plt.style.use('dark_background')
                closes=df['Close'].tail(100)
                plt.plot(closes.values, color='#00ff88', linewidth=1.5)
                plt.title(f"{code.upper()} {tf_label} - {int(closes.iloc[-1])}", color='white')
                plt.grid(True, alpha=0.2)
                plt.tight_layout()
                plt.savefig(chart_file, facecolor='black')
                plt.close()
                fp=chart_file
                print(f"Simple chart created {fp}")
            except Exception as e:
                print(f"Simple chart err {e}")
                try:
                    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                    requests.post(url, json={"chat_id":cid, "text": f"Gagal render {code} {tf_norm} - file tidak ada: {e}"}, timeout=10)
                except:
                    send_reply(cid, f"Gagal render {code} {tf_norm}")
                return
        try:
            caption,_=generate_caption_pro(code.upper(), df, realtime_price=realtime, tf_norm=tf_norm)
        except:
            caption=f"{code.upper()} {int(df['Close'].iloc[-1])} Daily"
        print(f"caption {caption[:100]} -> send photo to {cid}")
        try:
            send_photo_reply(cid, fp, caption=caption)
        except Exception as e:
            print(f"send_photo err {e}, try plain")
            # fallback kirim tanpa caption markdown
            try:
                url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
                with open(fp,'rb') as ph:
                    requests.post(url,data={'chat_id':cid,'caption':caption},files={'photo':ph},timeout=40)
            except Exception as e2:
                print(f"send_photo plain err {e2}")
        try: os.remove(fp)
        except: pass
        print(f"=== CHART DONE {code} ===")
    except Exception as e:
        print(f"CHART ERR {code} {e}")
        import traceback; traceback.print_exc()
        try: 
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            requests.post(url, json={"chat_id":cid, "text": f"Err chart {code}: {e}"}, timeout=10)
        except:
            try: send_reply(cid, f"Err chart {code}: {e}")
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
        btn=[{"text": f"{it['symbol']} {it['vol_ratio']:.1f}x", "callback_data": cb_data}]
        if len(msg)+len(line)>3500:
            print(f"broadcast chunk {len(kb)} items")
            send_reply(target, msg, rm={"inline_keyboard": kb})
            msg=line
            kb=[btn]
        else:
            msg+=line
            kb.append(btn)
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
        btn=[{"text": f"{it['symbol']} {both}", "callback_data": f"chart_{it['symbol']}"}]
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb})
            msg=line
            kb=[btn]
        else:
            msg+=line
            kb.append(btn)
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
    print(f"🤖 RAFANO V4.44 CLICK FINAL FIX EMA+PATTERN+CLICK FIX - RAFANO TRADER + ALL SCANNER ANTI KOSONG - {len(IDX_FULL)} IDX")
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
                # ===== CLICK HANDLER V4.46 OPTIMAL WORKERS + CLOG =====
                if "callback_query" in update:
                    try:
                        cb=update["callback_query"]
                        qid=cb.get("id")
                        cdata=(cb.get("data","") or "").strip()
                        msg_obj=cb.get("message") or {}
                        chat_obj=msg_obj.get("chat") if isinstance(msg_obj, dict) else {}
                        from_obj=cb.get("from") or {}
                        chat_id = None
                        if chat_obj and chat_obj.get("id"): chat_id=chat_obj.get("id")
                        if not chat_id and msg_obj and isinstance(msg_obj, dict):
                            mc=msg_obj.get("chat") or {}
                            if mc.get("id"): chat_id=mc.get("id")
                        if not chat_id: chat_id=TARGET_CHAT_ID or from_obj.get("id")
                        if not chat_id: chat_id=TARGET_CHAT_ID
                        try: chat_id=int(chat_id)
                        except: pass
                        clog(f"🔘 CLICK DETECTED cdata={cdata} chat_id={chat_id} qid={qid} user={from_obj.get('id')} workers={CHART_WORKERS}")
                        try:
                            with open(CLICK_LOG, "a") as lf:
                                lf.write(f"{get_now_wib()} CLICK {cdata} -> {chat_id} user={from_obj.get('id')}\n")
                        except: pass
                        try:
                            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",json={"callback_query_id":qid, "text": f"Loading {cdata}..."},timeout=5)
                        except Exception as e:
                            clog(f"answerCallback err {e}")
                        if cdata.startswith("chart_"):
                            sym=cdata[6:].strip().upper()
                            if not sym or len(sym)>6 or sym in FCA_EXCLUDE:
                                clog(f"click invalid sym {sym}")
                                continue
                            for k in list(HISTORY_CACHE.keys()):
                                if sym in k: HISTORY_CACHE.pop(k, None)
                            SUSPEND_CACHE.pop(sym, None)
                            SUSPEND_CACHE.pop(sym.upper(), None)
                            clog(f"🔘 CHART CLICK {sym} -> {chat_id} workers={CHART_WORKERS} cache cleared")
                            try:
                                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json={"chat_id":chat_id, "text": f"🔘 {sym} klik diterima - generate chart (workers={CHART_WORKERS})..."}, timeout=10)
                            except Exception as e:
                                clog(f"ack err {e}")
                            def chart_task():
                                try:
                                    process_chart_request(chat_id, sym, "1d")
                                    clog(f"✅ Chart task done {sym} -> {chat_id}")
                                except Exception as e:
                                    clog(f"❌ Chart task err {sym}: {e}")
                                    import traceback; traceback.print_exc()
                                    try:
                                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json={"chat_id":chat_id, "text": f"Gagal chart {sym}: {e}"}, timeout=10)
                                    except: pass
                            try:
                                t=threading.Thread(target=chart_task, daemon=True)
                                t.start()
                                clog(f"Chart thread started {sym} tid={t.ident}")
                            except Exception as e:
                                clog(f"Thread start err {e}, fallback direct")
                                chart_task()
                        else:
                            clog(f"unknown callback {cdata}")
                    except Exception as e:
                        clog(f"callback handler err {e}")
                        import traceback; traceback.print_exc()
                    continue

                if "message" in update and "text" in update["message"]:
                    txt=update["message"].get("text","").strip()
                    chat_id=update["message"]["chat"]["id"]
                    print(f"💬 MSG {chat_id}: {txt}")
                    first=txt.split()[0].lower() if txt else ""
                    parts=txt.split()
                    if first in ["/start","/help","/menu"]:
                        help_msg=f"""🔥 *RAFANO V4.44 - CLICK FINAL FIX + CONFLUENCE + OB/FVG*

400 BY VALUE (volume*close) BUKAN ALPHABET
FILTER: no suspend, no FCA, price>=50
FIX: Click chart fixed, daily anti gagal VKTR/BMTR

*CHART:*
/c KODE 5/15/60/1d = chart pro 4 panel
Klik tombol hasil scan langsung generate chart

*SCANNER 400 BY VALUE:*
/scanvol 1.5 = vol spike
/kimbsjp = KIM+BSJP
/list400 = list 400 BY VALUE

*BANDARMOLOGY:*
/topakum daily / weekly
/topdist daily / weekly
/akumdist daily / weekly
/bandar SYMBOL daily/weekly

*WHALE & INSIDER:*
/whale daily 30
/insider daily 30
/whaleinsider daily

*EMA PROXIMITY:*
/ema50 3 / ema50 bounce / ema50 pullback
/ema200 5 / golden 4

*CHART PATTERN:*
/pattern all / breakout / db / pb50 / hammer / flag / squeeze
Shortcut: /breakout /doublebottom /hammer /flag

*🆕 CONFLUENCE SCORE (Point A):*
/confluence 6 = score >=6 (EMA+Pattern+Bandar+Vol)
  Score 0-15, trigger list:
  EMA50 ±3% +2, Golden Area +2, Above EMA200 +1, Uptrend +1
  Breakout +2, Double Bottom +2, Engulfing +2, Hammer +1
  Whale +3, Akum +2, Vol Power +2, RSI Optimal +1
/confluence 7 bullish / confluence 6 safe
/conf 6 30 = top 30 dengan min score 6

*🆕 SAFETY & POWER (Point B):*
/safety 6 5 = Safety>=6 Power>=5 (both)
/safety 7 safe = hanya safety tinggi (aman, tidak volatile, bandar akum)
/power 6 power = hanya power tinggi (vol gede, chg naik, bandar gede)
/safepower / safe_power 7 6 = safety>=7 power>=6 (aman + kuat)
  Safety: EMA200 jarak wajar, RSI 35-70, Bandar Buy%, Range kalem, Support dekat
  Power: Vol ratio, Chg%, RSI momentum, Above EMA20, Bandar net gede

*🆕 ORDER BLOCK & FVG - SMC (Point D):*
/ob bullish 5 = saham dekat Order Block bullish ±5%
  OB: bearish candle sebelum impulse bullish >=4% + vol 1.2x
  Pullback ke OB = entry ideal
/ob bearish / ob both
/fvg bullish 5 = dekat Fair Value Gap bullish
  FVG: low[i] > high[i-2] = gap 0.2-8%
/fvg both / fvg bearish

*SUPER MIX:*
/supermix = EMA50 bounce + Breakout + Whale
/smartmix / smartfilter = Confluence + Safety/Power + OB
/conmix / superconfluence

*AUTOSCAN:*
/auto on/off
/status
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
                    # ==================== NEW V4.42 - EMA & PATTERN COMMANDS ====================
                    elif first in ["/ema50","/near50","/pullback50","/bounce50"]:
                        # /ema50 [threshold] [top_n] [mode]
                        # mode: near, above, below, bounce, pullback
                        threshold=3.0
                        top_n=30
                        mode="near"
                        if len(parts)>=2:
                            try:
                                threshold=float(parts[1])
                                if len(parts)>=3:
                                    try: top_n=int(parts[2])
                                    except:
                                        if parts[2].lower() in ["near","above","below","bounce","pullback"]:
                                            mode=parts[2].lower()
                                            if len(parts)>=4:
                                                try: top_n=int(parts[3])
                                                except: pass
                            except:
                                # parts[1] adalah mode
                                if parts[1].lower() in ["near","above","below","bounce","pullback","golden"]:
                                    mode=parts[1].lower()
                                    if len(parts)>=3:
                                        try: threshold=float(parts[2])
                                        except: pass
                                        if len(parts)>=4:
                                            try: top_n=int(parts[3])
                                            except: pass
                        send_reply(chat_id, f"📊 SCAN EMA50 {mode.upper()} {threshold}% - 400 BY VALUE...")
                        def run_ema50(tg=chat_id, th=threshold, tn=top_n, md=mode):
                            sigs=scan_near_ema50(threshold_pct=th, limit_candidates=400, top_n=tn, mode=md)
                            broadcast_ema_signals(sigs, ema_period=50, mode=md, dest_chat_id=tg)
                        threading.Thread(target=run_ema50,daemon=True).start()

                    elif first in ["/ema200","/near200","/pullback200","/bounce200"]:
                        threshold=5.0
                        top_n=30
                        mode="near"
                        if len(parts)>=2:
                            try:
                                threshold=float(parts[1])
                                if len(parts)>=3:
                                    try: top_n=int(parts[2])
                                    except:
                                        if parts[2].lower() in ["near","above","below","bounce","pullback"]:
                                            mode=parts[2].lower()
                            except:
                                if parts[1].lower() in ["near","above","below","bounce","pullback"]:
                                    mode=parts[1].lower()
                                    if len(parts)>=3:
                                        try: threshold=float(parts[2])
                                        except: pass
                        send_reply(chat_id, f"📊 SCAN EMA200 {mode.upper()} {threshold}% - 400 BY VALUE...")
                        def run_ema200(tg=chat_id, th=threshold, tn=top_n, md=mode):
                            sigs=scan_near_ema200(threshold_pct=th, limit_candidates=400, top_n=tn, mode=md)
                            broadcast_ema_signals(sigs, ema_period=200, mode=md, dest_chat_id=tg)
                        threading.Thread(target=run_ema200,daemon=True).start()

                    elif first in ["/ema","/near","/golden","/ema_golden","/goldenarea","/emagolden"]:
                        threshold=4.0
                        top_n=30
                        if len(parts)>=2:
                            try: threshold=float(parts[1])
                            except: pass
                            if len(parts)>=3:
                                try: top_n=int(parts[2])
                                except: pass
                        send_reply(chat_id, f"✨ SCAN GOLDEN AREA EMA50+EMA200 {threshold}% - 400 BY VALUE...")
                        def run_golden(tg=chat_id, th=threshold, tn=top_n):
                            sigs=scan_ema_golden_area(threshold_pct=th, limit_candidates=400, top_n=tn)
                            broadcast_golden_area(sigs, dest_chat_id=tg)
                        threading.Thread(target=run_golden,daemon=True).start()

                    elif first in ["/pattern","/chartpattern","/cp","/breakout","/doublebottom","/hammer","/flag","/squeeze"]:
                        # /pattern all / breakout / double_bottom / pullback_ema50 / pullback_ema200 / hammer / bull_engulfing / bull_flag / squeeze
                        pattern_name="all"
                        top_n=30
                        if first in ["/breakout"]:
                            pattern_name="breakout"
                        elif first in ["/doublebottom"]:
                            pattern_name="double_bottom"
                        elif first in ["/hammer"]:
                            pattern_name="hammer"
                        elif first in ["/flag"]:
                            pattern_name="bull_flag"
                        elif first in ["/squeeze"]:
                            pattern_name="squeeze"
                        else:
                            if len(parts)>=2:
                                pattern_name=parts[1].lower()
                                if len(parts)>=3:
                                    try: top_n=int(parts[2])
                                    except: top_n=30
                        # alias
                        alias_map={"db":"double_bottom","doublebottom":"double_bottom","double_bottom":"double_bottom","break":"breakout","bo":"breakout","pb50":"pullback_ema50","pb200":"pullback_ema200","ema50":"pullback_ema50","ema200":"pullback_ema200","engulf":"bull_engulfing","hammer":"hammer","flag":"bull_flag","squeeze":"squeeze","bullflag":"bull_flag"}
                        pattern_name=alias_map.get(pattern_name, pattern_name)
                        send_reply(chat_id, f"📈 SCAN PATTERN {pattern_name.upper()} - 400 BY VALUE...")
                        def run_pattern(tg=chat_id, pn=pattern_name, tn=top_n):
                            sigs=scan_chart_patterns(pattern_name=pn, limit_candidates=400, top_n=tn)
                            broadcast_patterns(sigs, pattern_name=pn, dest_chat_id=tg)
                        threading.Thread(target=run_pattern,daemon=True).start()

                    elif first in ["/screen","/supermix","/mix","/allscan"]:
                        send_reply(chat_id, "🔥 SUPER MIX SCAN - EMA50 bounce + Breakout + Whale - 400 BY VALUE...")
                        def run_supermix(tg=chat_id):
                            try:
                                ema50_sigs=scan_near_ema50(threshold_pct=3, limit_candidates=400, top_n=15, mode="bounce")
                                broadcast_ema_signals(ema50_sigs, ema_period=50, mode="bounce", dest_chat_id=tg)
                                time.sleep(1)
                                breakout_sigs=scan_chart_patterns(pattern_name="breakout", limit_candidates=400, top_n=15)
                                broadcast_patterns(breakout_sigs, pattern_name="breakout", dest_chat_id=tg)
                                time.sleep(1)
                                whale_sigs=scan_whale_accum(period="daily", limit_candidates=400, top_n=15)
                                broadcast_whale_accum(whale_sigs, period="daily", dest_chat_id=tg)
                            except Exception as e:
                                print(f"supermix err {e}")
                        threading.Thread(target=run_supermix,daemon=True).start()

                    # ==================== NEW V4.43 - CONFLUENCE, SAFETY/POWER, OB/FVG ====================
                    elif first in ["/confluence","/conf","/smart","/con","/score"]:
                        min_score=6
                        top_n=30
                        mode="all"
                        if len(parts)>=2:
                            try:
                                min_score=int(parts[1])
                                if len(parts)>=3:
                                    mode=parts[2].lower()
                                    if len(parts)>=4:
                                        try: top_n=int(parts[3])
                                        except: pass
                            except:
                                mode=parts[1].lower()
                                if len(parts)>=3:
                                    try: min_score=int(parts[2])
                                    except: pass
                        send_reply(chat_id, f"🧠 SCAN CONFLUENCE Score>={min_score} Mode:{mode} - 400 BY VALUE...")
                        def run_conf(tg=chat_id, ms=min_score, tn=top_n, md=mode):
                            sigs=scan_confluence(min_score=ms, limit_candidates=400, top_n=tn, mode=md)
                            broadcast_confluence(sigs, min_score=ms, dest_chat_id=tg)
                        threading.Thread(target=run_conf,daemon=True).start()

                    elif first in ["/safety","/safe","/power","/safepower","/safe_power","/sp"]:
                        min_safety=6
                        min_power=5
                        mode="both"
                        top_n=30
                        # /safety 6 5 both 30
                        if first in ["/power"]:
                            mode="power"
                            min_safety=5
                            min_power=6
                        if len(parts)>=2:
                            try:
                                min_safety=int(parts[1])
                                if len(parts)>=3:
                                    try:
                                        min_power=int(parts[2])
                                        if len(parts)>=4:
                                            mode=parts[3].lower()
                                            if len(parts)>=5:
                                                try: top_n=int(parts[4])
                                                except: pass
                                    except:
                                        mode=parts[2].lower()
                                        if len(parts)>=4:
                                            try: top_n=int(parts[3])
                                            except: pass
                            except:
                                mode=parts[1].lower()
                        send_reply(chat_id, f"🛡️ SCAN SAFETY {min_safety} POWER {min_power} Mode:{mode} - 400 BY VALUE...")
                        def run_sp(tg=chat_id, ms=min_safety, mp=min_power, md=mode, tn=top_n):
                            sigs=scan_safety_power(min_safety=ms, min_power=mp, limit_candidates=400, top_n=tn, mode=md)
                            broadcast_safety_power(sigs, min_safety=ms, min_power=mp, mode=md, dest_chat_id=tg)
                        threading.Thread(target=run_sp,daemon=True).start()

                    elif first in ["/ob","/orderblock","/order_block","/block"]:
                        mode="bullish"
                        threshold=5.0
                        top_n=30
                        if len(parts)>=2:
                            if parts[1].lower() in ["bullish","bearish","both","bull","bear"]:
                                mode=parts[1].lower()
                                if mode=="bull": mode="bullish"
                                if mode=="bear": mode="bearish"
                                if len(parts)>=3:
                                    try: threshold=float(parts[2])
                                    except: pass
                                    if len(parts)>=4:
                                        try: top_n=int(parts[3])
                                        except: pass
                            else:
                                try:
                                    threshold=float(parts[1])
                                    if len(parts)>=3:
                                        try: top_n=int(parts[2])
                                        except: pass
                                except: pass
                        send_reply(chat_id, f"📦 SCAN ORDER BLOCK {mode.upper()} {threshold}% - 400 BY VALUE...")
                        def run_ob(tg=chat_id, md=mode, th=threshold, tn=top_n):
                            sigs=scan_order_block(limit_candidates=400, top_n=tn, mode=md, threshold_pct=th)
                            broadcast_ob(sigs, mode=md, dest_chat_id=tg)
                        threading.Thread(target=run_ob,daemon=True).start()

                    elif first in ["/fvg","/gap","/fairvalue","/fvgap"]:
                        mode="bullish"
                        threshold=5.0
                        top_n=30
                        if len(parts)>=2:
                            if parts[1].lower() in ["bullish","bearish","both","bull","bear"]:
                                mode=parts[1].lower()
                                if mode=="bull": mode="bullish"
                                if mode=="bear": mode="bearish"
                                if len(parts)>=3:
                                    try: threshold=float(parts[2])
                                    except: pass
                            else:
                                try:
                                    threshold=float(parts[1])
                                except: pass
                        send_reply(chat_id, f"📉 SCAN FVG {mode.upper()} {threshold}% - 400 BY VALUE...")
                        def run_fvg(tg=chat_id, md=mode, th=threshold, tn=top_n):
                            sigs=scan_fvg(limit_candidates=400, top_n=tn, mode=md, threshold_pct=th)
                            broadcast_fvg(sigs, mode=md, dest_chat_id=tg)
                        threading.Thread(target=run_fvg,daemon=True).start()

                    elif first in ["/smartmix","/smartfilter","/superconfluence","/conmix"]:
                        send_reply(chat_id, "🔥 SMART MIX - Confluence + Safety/Power + OB/FVG - 400 BY VALUE...")
                        def run_smartmix(tg=chat_id):
                            try:
                                conf_sigs=scan_confluence(min_score=7, limit_candidates=400, top_n=15, mode="bullish")
                                broadcast_confluence(conf_sigs, min_score=7, dest_chat_id=tg)
                                time.sleep(1.5)
                                sp_sigs=scan_safety_power(min_safety=6, min_power=5, limit_candidates=400, top_n=15, mode="safe_power")
                                broadcast_safety_power(sp_sigs, min_safety=6, min_power=5, mode="safe_power", dest_chat_id=tg)
                                time.sleep(1.5)
                                ob_sigs=scan_order_block(limit_candidates=400, top_n=10, mode="bullish", threshold_pct=4.0)
                                broadcast_ob(ob_sigs, mode="bullish", dest_chat_id=tg)
                            except Exception as e:
                                print(f"smartmix err {e}")
                        threading.Thread(target=run_smartmix,daemon=True).start()

                    # ==================== END NEW V4.43 ====================
                    # ==================== END NEW V4.42 ====================
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
    print(f"🔥 RAFANO V4.44 CLICK FINAL FIX + CONFLUENCE + OB/FVG - {len(IDX_FULL)} IDX")
    telegram_bot_listener()
