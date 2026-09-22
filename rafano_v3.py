"""RAFANO V4.31 - KIM + BSJP + SCANVOL 300 MOST LIQUID - INFORMATIF PRO CHART"""
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

FCA_EXCLUDE={"FUTR","FITT","HOTEL","ITIC","PUDP","COIN","SHID","RELI","ASPI","MEJA","MINA","ESTA","ASLI","VKTR","IMJS","GTSI","IRSX","ATAP","RONY","BCIC","DEFI","ROCK","YPAS","NIRO","BBHA","BKSW","NAGA","BEEF","BPTR","CBMF","CPRI","CRAB","DAAZ","DEAL","DGNS","DMND","DUCK","ENRG","ENVY","ESTI","ETWA","FIRE","FORU","GAMA","GOLL","HAIS","HATM","HITS","HOMI","IATA","INPS","IPOL","JGLE","KAYU","KBAG","KIOS","KPAL","KPAS","LCGP","LPLI","LPLR","MAGP","MAMI","MARI","SINI","SKYB","SMKM","SOCI","SONA","SOSS","SUGI","TALF","TDPM","TEBE","TOPS","TRAM","TRIL","TRIO","TRUS","UFOE","WIFI-W","WOWS","YELO","ZATA","ZONE","ZINC"}

def load_785():
    for p in ['data/daytrade-observe-tickers.txt','./data/daytrade-observe-tickers.txt','/content/rafano-v3/data/daytrade-observe-tickers.txt','/content/rafano-v3/rafano-v3/data/daytrade-observe-tickers.txt','/content/auto-cuan/data/daytrade-observe-tickers.txt']:
        if os.path.exists(p):
            try:
                t=[x.strip().upper() for x in open(p, encoding='utf-8').read().splitlines() if len(x.strip())==4 and x.strip().isalpha()]
                if len(t)>=100:
                    print(f"Loaded {len(t)} tickers from {p}")
                    return t
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
ALERTED_TODAY=set(); ALERTED_DATE=None
STOCKBIT_CACHE={}
AUTO_KIM_ENABLED=False

def get_cached(k,cache,ttl):
    if k in cache:
        ts,d=cache[k]
        if time.time()-ts<ttl: return d
    return None
def set_cached(k,d,cache, maxsize=250):
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
    return {"rows": [{"stock_code": c, "close": 100, "volume": 1000000} for c in IDX_FULL[:300]]}

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
            chg=float(m_chg.group(1)) if m_chg else 0
            STOCKBIT_CACHE[sym]=(now,price,chg)
            return price,chg
    except: pass
    return 0,0

def get_realtime_from_screener(sym):
    p,c=get_realtime_stockbit(sym)
    if p>0: return p,c
    try:
        sc=get_screener_latest(force_today=True)
        for row in sc.get('rows',[]):
            if (row.get('stock_code') or "").upper()==sym.upper():
                return float(row.get('close') or 0), float(row.get('change_pct') or 0)
    except: pass
    return 0,0

def get_itick_quotes_batch(symbols, max_batch=15):
    all_q={}
    for s in symbols:
        p,c=get_realtime_from_screener(s)
        if p>0:
            all_q[s.upper()]={'price':p,'changepct':c,'source':'STOCKBIT_RT'}
    return all_q

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
                    set_cached(hk,df,HISTORY_CACHE); return df
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

# ==================== IHSG FRACTION ROUNDING ====================
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

def rma(series, period):
    # Wilder's RMA = EMA with alpha 1/period
    return series.ewm(alpha=1/period, adjust=False, min_periods=period).mean()

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
        prev_close=close.shift(1)
        prev_high=high.shift(1)
        prev_low=low.shift(1)
        tr1=high-low
        tr2=(high-prev_close).abs()
        tr3=(low-prev_close).abs()
        tr=pd.concat([tr1,tr2,tr3],axis=1).max(axis=1)
        plus_dm_raw=high-prev_high
        minus_dm_raw=prev_low-low
        plus_dm=np.where((plus_dm_raw>minus_dm_raw) & (plus_dm_raw>0), plus_dm_raw, 0.0)
        minus_dm=np.where((minus_dm_raw>plus_dm_raw) & (minus_dm_raw>0), minus_dm_raw, 0.0)
        plus_dm_series=pd.Series(plus_dm, index=df.index)
        minus_dm_series=pd.Series(minus_dm, index=df.index)
        tr_smooth=rma(tr, period)
        plus_dm_smooth=rma(plus_dm_series, period)
        minus_dm_smooth=rma(minus_dm_series, period)
        plus_di=100*plus_dm_smooth/tr_smooth.replace(0,1)
        minus_di=100*minus_dm_smooth/tr_smooth.replace(0,1)
        dx=100*(plus_di-minus_di).abs()/(plus_di+minus_di).replace(0,1)
        adx=rma(dx, period)
        return plus_di.fillna(0), minus_di.fillna(0), adx.fillna(0)
    except Exception as e:
        print(f"DMI err {e}")
        zeros=pd.Series([0]*len(df), index=df.index)
        return zeros, zeros, zeros

def calc_atr(df, period=14):
    try:
        high=df['High']; low=df['Low']; close=df['Close']
        prev_close=close.shift(1)
        tr=pd.concat([high-low, (high-prev_close).abs(), (low-prev_close).abs()],axis=1).max(axis=1)
        atr=rma(tr, period)
        return atr
    except:
        return pd.Series([0]*len(df), index=df.index)

# ==================== KIM SIGNAL LOGIC ====================
def detect_kim_signal(df, len_8=8, len_fast=20, len_slow=200, lookback_vol=20, spike_mult=1.8, lookback_avg=60, lookback_sl=20, atr_mult=2.0, adx_min=20, atr_period=14, rsi_len=14, use_stretch_guard=True, max_stretch_pct=40.0):
    try:
        if df is None or len(df)<80:
            return {"buy":False, "reason":"len<80", "b_modal":0, "tp1":0, "tp2":0, "sl":0, "rsi":50, "adx":0, "stretch_pct":0, "raw_b":False, "re_entry":False, "markers":[]}
        df=df.copy()
        close=df['Close']; open_=df['Open']; high=df['High']; low=df['Low']; volume=df['Volume']
        ema_8=close.ewm(span=len_8, adjust=False).mean()
        ema_f=close.ewm(span=len_fast, adjust=False).mean()
        ema_slow=close.ewm(span=len_slow, adjust=False).mean()
        rsi_series=close.copy()
        # RSI series for chart
        delta=close.diff()
        gain=delta.where(delta>0,0).ewm(alpha=1/rsi_len, adjust=False).mean()
        loss=(-delta.where(delta<0,0)).ewm(alpha=1/rsi_len, adjust=False).mean()
        rs=gain/loss.replace(0,0.001)
        rsi_ser=100-(100/(1+rs))
        rsi_val=float(rsi_ser.iloc[-1]) if not pd.isna(rsi_ser.iloc[-1]) else 50.0

        plus_di, minus_di, adx_ser=calc_dmi_adx(df, 14)
        adx_val=float(adx_ser.iloc[-1]) if len(adx_ser)>0 else 0
        is_trending_ser=adx_ser>adx_min

        atr_ser=calc_atr(df, atr_period)
        atr_val=float(atr_ser.iloc[-1]) if len(atr_ser)>0 and not pd.isna(atr_ser.iloc[-1]) else 0
        atr_sma=atr_ser.rolling(20).mean()
        has_volatility_ser=atr_ser>atr_sma
        has_volatility=bool(has_volatility_ser.iloc[-1]) if len(has_volatility_ser)>0 else False

        v_sma=volume.rolling(lookback_vol).mean()
        is_v_akum=(close>open_) & (volume>v_sma)
        is_v_spike=volume>(v_sma*spike_mult)

        hlc3=(high+low+close)/3
        vp_cond=hlc3*volume
        vp_cond=vp_cond.where(is_v_akum, 0)
        v_cond=volume.where(is_v_akum, 0)
        sum_vp=vp_cond.rolling(lookback_avg, min_periods=1).sum()
        sum_v=v_cond.rolling(lookback_avg, min_periods=1).sum()
        b_modal_raw=np.where(sum_v>0, sum_vp/sum_v, close)
        b_modal_series=pd.Series([f_round_ihsg(x) for x in b_modal_raw], index=df.index)
        b_modal=float(b_modal_series.iloc[-1]) if len(b_modal_series)>0 else float(close.iloc[-1])

        stretch_pct=float(abs((close.iloc[-1]-b_modal)/b_modal*100)) if b_modal!=0 else 0
        stretch_ok=stretch_pct<=max_stretch_pct if use_stretch_guard else True

        is_trending=bool(is_trending_ser.iloc[-1]) if len(is_trending_ser)>0 else False
        common_ok_series=(is_v_akum | is_v_spike) & is_trending_ser & has_volatility_ser & (close>ema_slow)
        if use_stretch_guard:
            stretch_ok_series=(abs((close-b_modal_series)/b_modal_series*100)<=max_stretch_pct)
            common_ok_series=common_ok_series & stretch_ok_series

        common_ok=bool(common_ok_series.iloc[-1]) if len(common_ok_series)>0 else False

        # crossover close vs b_modal
        prev_close=close.shift(1)
        prev_b_modal=b_modal_series.shift(1)
        crossover=(prev_close<=prev_b_modal) & (close>b_modal_series)
        raw_b_series=crossover & (close>ema_f) & common_ok_series
        raw_b=bool(raw_b_series.iloc[-1]) if len(raw_b_series)>0 else False

        # re-entry: close > highest(high[1],10)
        highest_10=high.shift(1).rolling(10).max()
        re_entry_series=(close>highest_10) & (close>b_modal_series) & common_ok_series
        re_entry=bool(re_entry_series.iloc[-1]) if len(re_entry_series)>0 else False

        buy_condition=raw_b or re_entry

        # TP/SL calc
        val_sl=float(low.rolling(lookback_sl).min().iloc[-1]) if len(low)>=lookback_sl else float(low.min())
        ideal_buy=f_round_ihsg(float(close.iloc[-1]))
        t_stop_raw=min(float(close.iloc[-1]) - (atr_val*atr_mult), val_sl)
        t_stop=f_round_ihsg(t_stop_raw)
        p_range=abs(ideal_buy-val_sl)
        tp1=f_round_ihsg(ideal_buy + p_range*1.0)
        tp2=f_round_ihsg(ideal_buy + p_range*2.0)

        # markers for all bars
        markers=[]
        for i in range(len(df)):
            if i<len(df) and raw_b_series.iloc[i]:
                markers.append({"idx":i, "type":"KIM BUY", "price":float(close.iloc[i])})
            elif i<len(df) and re_entry_series.iloc[i]:
                markers.append({"idx":i, "type":"KIM RE-ENTRY", "price":float(close.iloc[i])})

        return {"buy":bool(buy_condition), "reason":"KIM", "b_modal":b_modal, "b_modal_series":b_modal_series, "tp1":tp1, "tp2":tp2, "sl":t_stop, "ideal_buy":ideal_buy, "rsi":rsi_val, "adx":adx_val, "stretch_pct":stretch_pct, "raw_b":raw_b, "re_entry":re_entry, "is_trending":is_trending, "has_volatility":has_volatility, "common_ok":common_ok, "markers":markers, "ema_8":ema_8, "ema_f":ema_f, "ema_slow":ema_slow, "atr":atr_val, "is_v_akum":bool(is_v_akum.iloc[-1]), "is_v_spike":bool(is_v_spike.iloc[-1])}
    except Exception as e:
        import traceback; traceback.print_exc()
        return {"buy":False, "reason":f"err {e}", "b_modal":0, "tp1":0, "tp2":0, "sl":0, "markers":[]}

# ==================== BSJP PRO MOMENTUM LOGIC ====================
def detect_bsjp_signal(df, timeframe="1d", volMultiplier=1.8, maVolLength=20, minPriceChange=0.5, minDailyVol=2000000, ema8Len=8, ema20Len=20, ema200Len=200, tp1Pct=3.0, tp2Pct=6.0, tp3Pct=9.0, slPct=3.0, startHour=14, startMinute=30, endHour=15, endMinute=45):
    try:
        if df is None or len(df)<60:
            return {"buy":False, "reason":"len<60", "markers":[]}
        df=df.copy()
        close=df['Close']; open_=df['Open']; high=df['High']; low=df['Low']; volume=df['Volume']
        ema8=close.ewm(span=ema8Len, adjust=False).mean()
        ema20=close.ewm(span=ema20Len, adjust=False).mean()
        ema200=close.ewm(span=ema200Len, adjust=False).mean()

        timeframe_norm=normalize_timeframe(timeframe)
        isTf15=timeframe_norm in ["15m","30m","1h"]
        isTf5=timeframe_norm=="5m"
        lookbackBreak=4 if isTf15 else 10 if isTf5 else 8

        # Time filter - if intraday, check WIB hour, else True
        def is_time_sore(idx):
            try:
                # idx is Timestamp
                if isinstance(idx, pd.Timestamp):
                    # convert to WIB if needed - assume index already WIB or UTC, we check hour
                    # For simplicity, if timeframe is daily, return True
                    if timeframe_norm=="1d" or timeframe_norm=="1w":
                        return True
                    # For intraday, check hour
                    # Convert to WIB (Asia/Jakarta) - if naive, assume WIB
                    h=idx.hour
                    m=idx.minute
                    # WIB 14:30-15:45
                    start_total=startHour*60+startMinute
                    end_total=endHour*60+endMinute
                    cur_total=h*60+m
                    return start_total<=cur_total<=end_total
                return True
            except:
                return True

        is_time_sore_series=pd.Series([is_time_sore(i) for i in df.index], index=df.index)
        # For daily, all True
        if timeframe_norm=="1d" or timeframe_norm=="1w":
            is_time_sore_series=pd.Series([True]*len(df), index=df.index)

        avgVolume=volume.rolling(maVolLength).mean()
        isVolumeSurge=volume>(avgVolume*volMultiplier)
        priceChangePct=(close-open_)/open_.replace(0,1)*100
        isPriceUp=priceChangePct>=minPriceChange
        isStrongCandle=close>open_
        isLiquidStock=volume>=minDailyVol
        isAboveMajorTrend=close>ema200

        highestHigh=high.shift(1).rolling(lookbackBreak).max()
        isBreakout=close>highestHigh

        # signalTriggeredToday logic - only one per day
        # We'll track dates
        signal_dates=set()
        bsjp_series=pd.Series([False]*len(df), index=df.index)
        markers=[]
        for i in range(len(df)):
            if not is_time_sore_series.iloc[i]: continue
            if not isVolumeSurge.iloc[i]: continue
            if not isPriceUp.iloc[i]: continue
            if not isStrongCandle.iloc[i]: continue
            if not isBreakout.iloc[i]: continue
            if not isLiquidStock.iloc[i]: continue
            if not isAboveMajorTrend.iloc[i]: continue
            # check if already triggered today
            try:
                cur_date=df.index[i].date()
                if cur_date in signal_dates:
                    continue
                signal_dates.add(cur_date)
            except: pass
            bsjp_series.iloc[i]=True
            markers.append({"idx":i, "type":"BSJP BUY SORE", "price":float(close.iloc[i]), "tp1":f_round_ihsg(float(high.iloc[i]*(1+tp1Pct/100))), "tp2":f_round_ihsg(float(high.iloc[i]*(1+tp2Pct/100))), "tp3":f_round_ihsg(float(high.iloc[i]*(1+tp3Pct/100))), "sl":f_round_ihsg(float(low.iloc[i]*(1-slPct/100)))})

        buy_last=bool(bsjp_series.iloc[-1]) if len(bsjp_series)>0 else False

        # calc TP/SL for last signal if buy
        if buy_last:
            last_high=float(high.iloc[-1]); last_low=float(low.iloc[-1])
            tp1=f_round_ihsg(last_high*(1+tp1Pct/100))
            tp2=f_round_ihsg(last_high*(1+tp2Pct/100))
            tp3=f_round_ihsg(last_high*(1+tp3Pct/100))
            sl=f_round_ihsg(last_low*(1-slPct/100))
        else:
            tp1=tp2=tp3=sl=0

        return {"buy":buy_last, "reason":"BSJP", "markers":markers, "series":bsjp_series, "ema8":ema8, "ema20":ema20, "ema200":ema200, "tp1":tp1, "tp2":tp2, "tp3":tp3, "sl":sl, "isVolumeSurge":bool(isVolumeSurge.iloc[-1]), "isPriceUp":bool(isPriceUp.iloc[-1]), "isBreakout":bool(isBreakout.iloc[-1]), "isLiquid":bool(isLiquidStock.iloc[-1]), "isAboveMajor":bool(isAboveMajorTrend.iloc[-1])}
    except Exception as e:
        import traceback; traceback.print_exc()
        return {"buy":False, "reason":f"err {e}", "markers":[]}

def check_bo_ema50_bob200(sym, hd, realtime_price=None):
    try:
        if hd is None or len(hd)<55: return None
        c=float(realtime_price if realtime_price and realtime_price>0 else hd['Close'].iloc[-1])
        pc=float(hd['Close'].iloc[-2])
        if c<50: return None
        ema50_s=hd['Close'].ewm(span=50,adjust=False).mean()
        ema200_s=hd['Close'].ewm(span=200,adjust=False).mean()
        ema50=float(ema50_s.iloc[-1]); ema200=float(ema200_s.iloc[-1])
        pe50=float(ema50_s.iloc[-2]); pe200=float(ema200_s.iloc[-2])
        if pd.isna(ema50) or pd.isna(ema200): return None
        if pc<=pe50 and c>ema50 and c>pe50: return {"type":"BO EMA50","ema50":ema50,"ema200":ema200}
        if pc<=pe200 and c>ema200 and c>pe200: return {"type":"BOB EMA200","ema50":ema50,"ema200":ema200}
        return None
    except: return None

def detect_bo_bos_markers(df):
    signals=[]
    if len(df)<55: return signals
    df=df.copy()
    df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
    df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
    for i in range(50,len(df)):
        c=df['Close'].iloc[i]; pc=df['Close'].iloc[i-1]
        ema50=df['EMA50'].iloc[i]; pe50=df['EMA50'].iloc[i-1]
        ema200=df['EMA200'].iloc[i]; pe200=df['EMA200'].iloc[i-1]
        if pd.isna(ema50) or pd.isna(ema200): continue
        if pc<=pe50 and c>ema50 and c>pe50: signals.append({"idx":i,"type":"BO EMA50"})
        elif pc<=pe200 and c>ema200 and c>pe200: signals.append({"idx":i,"type":"BOB EMA200"})
    return signals

def generate_caption_pro(symbol, df, realtime_price=None, tf_norm="1d", kim=None, bsjp=None):
    try:
        df=df.copy()
        if realtime_price and realtime_price>0 and len(df)>0:
            df.iloc[-1, df.columns.get_loc('Close')] = realtime_price
        last_close=df['Close'].iloc[-1]
        prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close
        chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
        rsi=calculate_rsi(df['Close'],14)
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        df_vsa,buy_ratios=calculate_vsa_metrics(df)
        br_last = buy_ratios.iloc[-1] if hasattr(buy_ratios, 'iloc') else buy_ratios[-1]
        buy_pct=int(br_last*100)
        buy_pct=max(0,min(100,buy_pct))
        v1=df['V1'].iloc[-1]
        vol_spike=df['Volume'].iloc[-1]/v1 if v1>0 else 1.0

        kim_txt="❌"
        if kim:
            kim_txt=f"{'✅ BUY' if kim.get('buy') else '⏳ WAIT'} | Modal:{kim.get('b_modal',0):.0f} TP1:{kim.get('tp1',0):.0f} SL:{kim.get('sl',0):.0f} | ADX:{kim.get('adx',0):.0f} RSI:{kim.get('rsi',0):.0f}"
        bsjp_txt="❌"
        if bsjp:
            bsjp_txt=f"{'✅ BUY SORE' if bsjp.get('buy') else '⏳ WAIT'} | VolSurge:{bsjp.get('isVolumeSurge')} Breakout:{bsjp.get('isBreakout')} Liquid:{bsjp.get('isLiquid')}"

        caption=f"{symbol} — {int(last_close)} ({chg_pct:+.2f}%) | {format_timeframe_label(tf_norm)}\n├ RSI:{rsi:.1f} Vol:{vol_spike:.1f}x Buy%:{buy_pct}%\n├ KIM: {kim_txt}\n└ BSJP: {bsjp_txt}"
        return caption, {}
    except Exception as e:
        print(f"Caption err {e}")
        return f"{symbol} — {int(df['Close'].iloc[-1]) if len(df)>0 else 0}", {}

def generate_pro_chart(df,symbol="BBCA",timeframe="5m",sector_info="IHSG",output_filename="chart.png",extra_info=None,realtime_price=None):
    try:
        extra_info=extra_info or {}
        timeframe_norm=normalize_timeframe(timeframe)
        tf_label_disp=extra_info.get('tf_label') or format_timeframe_label(timeframe_norm)
        df=df.copy().ffill().bfill()
        if not isinstance(df.index,pd.DatetimeIndex): df.index=pd.to_datetime(df.index)
        else: df=df.sort_index()
        df=df.dropna(subset=['Open','High','Low','Close'])
        if len(df)<20: return None, []
        if realtime_price and realtime_price>0 and len(df)>0:
            df.iloc[-1, df.columns.get_loc('Close')] = realtime_price
            if realtime_price>df['High'].iloc[-1]: df.iloc[-1, df.columns.get_loc('High')] = realtime_price
            if realtime_price<df['Low'].iloc[-1]: df.iloc[-1, df.columns.get_loc('Low')] = realtime_price

        # Core indicators
        df['EMA8']=df['Close'].ewm(span=8,adjust=False).mean()
        df['EMA13']=df['Close'].ewm(span=13,adjust=False).mean()
        df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
        df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
        df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        sma20,upper_bb,lower_bb=calculate_bollinger_bands(df,20,2)
        df['BB_UP']=upper_bb; df['BB_LOW']=lower_bb
        df,buy_ratios=calculate_vsa_metrics(df)

        # KIM & BSJP signals
        kim=detect_kim_signal(df)
        bsjp=detect_bsjp_signal(df, timeframe=timeframe_norm)

        bo_markers=detect_bo_bos_markers(df)

        last_close=df['Close'].iloc[-1]; prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close
        chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
        avg_price=df['Close'].tail(20).mean()
        vchg1=(df['Volume'].iloc[-1]/df['Volume'].iloc[-2]) if len(df)>1 and df['Volume'].iloc[-2]>0 else 0
        avg5=df['Volume'].tail(5).mean(); vchg5=(df['Volume'].iloc[-1]/avg5) if avg5>0 else 0
        br_last = buy_ratios.iloc[-1] if hasattr(buy_ratios, 'iloc') else buy_ratios[-1]
        buy_pct_temp=int(br_last*100)
        power="TURBO" if buy_pct_temp>=85 and vchg1>=1.2 else "STRONG" if buy_pct_temp>=70 or vchg1>=1.5 else "NORMAL" if buy_pct_temp>=60 else "WEAK"
        speed="FAST" if vchg1>2.0 else "SLOW" if vchg1<0.8 else "NORMAL"
        ema8=df['EMA8'].iloc[-1]; ema13=df['EMA13'].iloc[-1]; ema20=df['EMA20'].iloc[-1]; ema50=df['EMA50'].iloc[-1]; ema200=df['EMA200'].iloc[-1]
        if pd.isna(ema200): ema200=ema50
        safety="GOOD" if pd.notna(df['EMA200'].iloc[-1]) and last_close>df['EMA200'].iloc[-1] else "NEUTRAL"
        buy_pct=buy_pct_temp
        net_vol=df['Net_Vol_VSA'].iloc[-1]; net_vol_5d=df['Net_Vol_VSA'].tail(5).sum()

        plt.style.use('dark_background')
        fig=plt.figure(figsize=(15,9),dpi=150,facecolor='#000000')
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

        # EMA lines - KIM & BSJP
        ax_main.plot(x,df['EMA8'],color='#ffeb3b',linewidth=1.0, label='EMA8')
        ax_main.plot(x,df['EMA20'],color='#00e6ff',linewidth=1.0, label='EMA20')
        ax_main.plot(x,df['EMA50'],color='#ffffff',linewidth=0.9, alpha=0.8)
        if pd.notna(df['EMA200'].iloc[-1]): ax_main.plot(x,df['EMA200'],color='#a020f0',linewidth=1.4, label='EMA200')
        ax_main.plot(x,df['BB_UP'],color='#444488',linewidth=0.8,linestyle='--',alpha=0.5); ax_main.plot(x,df['BB_LOW'],color='#444488',linewidth=0.8,linestyle='--',alpha=0.5)

        # KIM Modal Bandar
        if 'b_modal_series' in kim and kim['b_modal_series'] is not None and len(kim['b_modal_series'])==len(df):
            ax_main.plot(x, kim['b_modal_series'], color='#ffffff', linewidth=1.2, linestyle='-', alpha=0.7, label='Modal Bandar')

        # KIM TP1/TP2/SL for last signal if buy
        if kim.get('buy'):
            ax_main.axhline(kim.get('tp1'), color='#00ff00', linewidth=1, linestyle='--', alpha=0.5)
            ax_main.axhline(kim.get('tp2'), color='#00ffff', linewidth=1, linestyle='--', alpha=0.5)
            ax_main.axhline(kim.get('sl'), color='#ff5500', linewidth=1.5, linestyle='-', alpha=0.8)

        # BSJP TP levels
        if bsjp.get('buy'):
            if bsjp.get('tp1'): ax_main.axhline(bsjp.get('tp1'), color='#00ff00', linewidth=0.8, linestyle=':', alpha=0.5)
            if bsjp.get('sl'): ax_main.axhline(bsjp.get('sl'), color='#ff0000', linewidth=1.2, linestyle='-', alpha=0.7)

        # Markers
        for sig in bo_markers:
            idx=sig['idx']
            if idx>=len(df): continue
            low=df['Low'].iloc[idx]
            ax_main.plot(idx, low*0.985, marker='^', color='#00ff00', markersize=8, alpha=0.7)

        # KIM markers
        for m in kim.get('markers',[]):
            idx=m['idx']
            if idx>=len(df): continue
            low=df['Low'].iloc[idx]
            ax_main.plot(idx, low*0.97, marker='^', color='#00ff00', markersize=10)
            ax_main.text(idx, low*0.95, f"KIM\n{m['type']}", color='#00ff00', fontsize=6, ha='center', fontweight='bold')

        # BSJP markers
        for m in bsjp.get('markers',[]):
            idx=m['idx']
            if idx>=len(df): continue
            low=df['Low'].iloc[idx]
            ax_main.plot(idx, low*0.96, marker='*', color='#0080ff', markersize=12)
            ax_main.text(idx, low*0.92, "BSJP\nBUY SORE", color='#0080ff', fontsize=6, ha='center', fontweight='bold')

        ax_main.set_xlim(-1,len(df)-1+10); ax_main.set_ylim(df['Low'].min()*0.96, df['High'].max()*1.06)

        # Left panel - enhanced with KIM/BSJP
        left_text=f"Avg Price : {avg_price:.1f}\nVchg 1 Bar: {vchg1:.1f} x\nVchg 5 Bar: {vchg5:.1f} x\nSpeed : {speed}\nPower : {power}\nSafety : {safety}\n\nEMA 8 : {ema8:.1f}\nEMA 20 : {ema20:.1f}\nEMA 50 : {ema50:.1f}\nEMA 200: {ema200:.1f}\n\n--- KIM SIGNAL ---\nModal : {kim.get('b_modal',0):.0f}\nTP1 : {kim.get('tp1',0):.0f}\nTP2 : {kim.get('tp2',0):.0f}\nSL : {kim.get('sl',0):.0f}\nRSI : {kim.get('rsi',0):.0f}\nADX : {kim.get('adx',0):.0f}\nStretch: {kim.get('stretch_pct',0):.1f}%\nStatus: {'BUY' if kim.get('buy') else 'WAIT'}\n\n--- BSJP ---\nTP1 : {bsjp.get('tp1',0):.0f}\nSL : {bsjp.get('sl',0):.0f}\nVolSurge: {bsjp.get('isVolumeSurge')}\nBreakout: {bsjp.get('isBreakout')}\nStatus: {'BUY SORE' if bsjp.get('buy') else 'WAIT'}"

        ax_main.text(0.005,0.98,left_text,transform=ax_main.transAxes,va='top',ha='left',fontsize=6,family='monospace',color='#e0e0e0',bbox=dict(facecolor='black',alpha=0.75,edgecolor='#333333'))

        fig.text(0.005,0.96,f"{symbol} : {last_close:.0f} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=14,fontweight='bold',ha='left', family='monospace')
        title_status=""
        if kim.get('buy') and bsjp.get('buy'): title_status="🔥 KIM+BSJP BUY"
        elif kim.get('buy'): title_status="🚀 KIM BUY"
        elif bsjp.get('buy'): title_status="💥 BSJP BUY SORE"
        else: title_status="RAFANO V4.31 KIM+BSJP"
        fig.text(0.5,0.96,title_status,color='white',fontsize=14,fontweight='bold',ha='center')
        ds=df.index[-1].strftime('%d %b %Y %H:%M')
        fig.text(0.99,0.96,f"{tf_label_disp} | {ds}",color='#ffcc00',fontsize=10,ha='right',fontweight='bold')
        ax_main.text(len(df)+1, last_close, f" {last_close:.0f}", color='black', fontsize=8, va='center', fontweight='bold', bbox=dict(facecolor='white', edgecolor='none', boxstyle='square,pad=0.2'))

        vol_info=f"Buy % = {buy_pct}% Sell % = {100-buy_pct}% Net Vol = {net_vol:,.0f} 5D = {net_vol_5d:,.0f} | KIM Modal {kim.get('b_modal',0):.0f} Stretch {kim.get('stretch_pct',0):.1f}%"
        ax_vol.text(0.005,0.88,vol_info,transform=ax_vol.transAxes,color='#ffffff',fontsize=7,va='top')
        ax_vol.bar(x,df['Vol_Sell'],color='#cc0000',width=0.8,alpha=0.8); ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00cc00',width=0.8,alpha=0.9)
        ax_vol.plot(x,df['V1'],color='white',linewidth=0.8,alpha=0.9)
        ax_vol.set_ylim(0,df['Volume'].max()*2.2); plt.setp(ax_vol.get_xticklabels(),visible=False)

        ax_nbsa.text(0.005,0.85,f"NBSA Rp. {abs(net_vol*last_close)/1e6:.2f} M | ADX {kim.get('adx',0):.0f} {'STRONG' if kim.get('adx',0)>20 else 'WEAK'} RSI {kim.get('rsi',0):.0f}",transform=ax_nbsa.transAxes,color='#ffffff',fontsize=7,va='top')
        nbsa_vals=df['Net_Vol_VSA'].tail(120)/(df['Net_Vol_VSA'].abs().max() or 1)*50
        xn=np.arange(len(df)-len(nbsa_vals),len(df))
        for i,v in zip(xn,nbsa_vals): ax_nbsa.bar(i,v,color='#00ffff' if v>=0 else '#ff4444',width=0.6)
        ax_nbsa.axhline(0,color='#444444',linewidth=0.5); ax_nbsa.set_ylim(-60,60)

        ax_mm.text(0.005,0.85,"Market Maker | KIM TP1/TP2 & BSJP TP",transform=ax_mm.transAxes,color='#ffffff',fontsize=7,va='top')
        if 'MM' not in df.columns: df['MM']=(df['Close']-df['EMA50'])/df['EMA50']*1000
        mm_vals=df['MM'].tail(120); xm=np.arange(len(df)-len(mm_vals),len(df))
        for i,v in zip(xm,mm_vals): ax_mm.bar(i,v,color='#cccccc' if v>=0 else '#888888',width=0.5,alpha=0.8)
        ax_mm.set_ylim(-30,30)
        step=max(1,len(df)//10); ax_mm.set_xticks(x[::step])
        labels=[df.index[i].strftime('%H:%M') if 'm' in timeframe_norm or 'h' in timeframe_norm else df.index[i].strftime('%d/%b') for i in range(0,len(df),step)]
        ax_mm.set_xticklabels(labels,fontsize=7)

        plt.savefig(output_filename,dpi=150,bbox_inches='tight',facecolor='#000000')
        plt.close('all')
        return output_filename, kim.get('markers',[])+bsjp.get('markers',[])
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

# ==================== TOP 300 MOST LIQUID ====================
def get_top_liquid_tickers(n=300):
    # Try screener sorted by value
    try:
        screener=get_screener_latest(force_today=False)
        rows=screener.get('rows',[])
        if len(rows)>=n:
            # Try sort by volume*close if volume exists, else keep order (assumed liquid first)
            try:
                rows_with_vol=[r for r in rows if r.get('volume') is not None]
                if len(rows_with_vol)>=n:
                    rows_sorted=sorted(rows_with_vol, key=lambda r: float(r.get('volume',0))*float(r.get('close',0)), reverse=True)
                    tickers=[(r.get('stock_code') or "").replace(".JK","").upper() for r in rows_sorted[:n]]
                    filtered=[t for t in tickers if t and "-W" not in t and t not in FCA_EXCLUDE]
                    if len(filtered)>=n*0.8:
                        return filtered[:n]
            except: pass
            # fallback: take first n rows as they are (usually sorted by liquidity in Arjum)
            tickers=[(r.get('stock_code') or "").replace(".JK","").upper() for r in rows[:n*2]]
            filtered=[t for t in tickers if t and "-W" not in t and t not in FCA_EXCLUDE]
            return filtered[:n]
    except: pass
    # Fallback: IDX_FULL is already sorted by liquidity from file
    filtered=[c for c in IDX_FULL if c not in FCA_EXCLUDE and "-W" not in c]
    return filtered[:n]

def scan_volume_spike(threshold=1.5, limit_candidates=300):
    # UPDATED: default 300 most liquid
    tickers=get_top_liquid_tickers(limit_candidates)
    detected=[]
    for sym in tickers:
        try:
            hd=get_history_pro(sym,60,"daily")
            if hd is None or len(hd)<20: continue
            v_last=hd['Volume'].iloc[-1]; v_avg=hd['Volume'].tail(20).mean()
            if v_avg==0: continue
            ratio=v_last/v_avg
            if ratio < threshold: continue
            close=hd['Close'].iloc[-1]; prev=hd['Close'].iloc[-2] if len(hd)>=2 else close
            chg=(close/prev-1)*100 if prev else 0
            detected.append({"symbol":sym,"close":int(close),"change_pct":chg,"vol_ratio":ratio,"vol_rp":v_last*close})
        except: continue
    detected.sort(key=lambda x: x['vol_ratio'], reverse=True)
    return detected

def scan_v417_final(top_arjum=60, top_itick=30, vol_thr=1.5, min_value=1_000_000_000, only_new=False):
    global ALERTED_TODAY, ALERTED_DATE
    today=get_now_wib().date()
    if ALERTED_DATE!=today: ALERTED_TODAY=set(); ALERTED_DATE=today
    screener=get_screener_latest(force_today=True)
    rows=screener.get('rows',[]) if isinstance(screener,dict) else []
    if not rows: return []
    candidates=[]
    for r in rows:
        code=(r.get('stock_code') or "").replace(".JK","").upper()
        if not code or code in FCA_EXCLUDE or "-W" in code: continue
        if float(r.get('close') or 100)<50: continue
        candidates.append(code)
        if len(candidates)>=top_arjum: break
    all_quotes=get_itick_quotes_batch(candidates,15)
    if not all_quotes:
        all_quotes={}
        for r in rows:
            code=(r.get('stock_code') or "").replace(".JK","").upper()
            if code in candidates:
                all_quotes[code]={'price':float(r.get('close') or 0),'changepct':float(r.get('change_pct') or 0)}
    sorted_q=sorted(all_quotes.items(), key=lambda x: x[1].get('changepct',-999), reverse=True)
    top_codes=[c for c,_ in sorted_q[:top_itick]] if sorted_q else candidates[:top_itick]
    def check_one(sym):
        try:
            if only_new and sym in ALERTED_TODAY: return None
            q=all_quotes.get(sym,{})
            realtime_price=q.get('price',0)
            hd=get_history_pro(sym, limit=120, frame="daily")
            if hd is None or len(hd)<55: return None
            c=float(realtime_price if realtime_price and realtime_price>0 else hd['Close'].iloc[-1])
            v_last=float(hd['Volume'].iloc[-1]); v_avg=float(hd['Volume'].iloc[-21:-1].mean())
            if v_avg==0 or v_last==0: return None
            if v_last/v_avg < vol_thr: return None
            if v_last*c < min_value: return None
            bo_info=check_bo_ema50_bob200(sym, hd, realtime_price)
            if not bo_info: return None
            return {"symbol":sym,"type":bo_info['type'],"close":int(c),"change_pct":q.get('changepct',0),"vol_ratio":v_last/v_avg,"vol_rp":v_last*c,"realtime":realtime_price}
        except: return None
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs={ex.submit(check_one,s):s for s in top_codes}
        detected=[]
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)
    detected.sort(key=lambda x: x['vol_ratio'], reverse=True)
    return detected

def scan_kim_bsjp_300():
    tickers=get_top_liquid_tickers(300)
    print(f"🔍 Scan KIM+BSJP 300 liquid: {len(tickers)} saham")
    detected=[]
    def check_one(sym):
        try:
            df=get_history_pro(sym, 150, "1d")
            if df is None or len(df)<80: return None
            kim=detect_kim_signal(df)
            bsjp=detect_bsjp_signal(df, timeframe="1d")
            if kim['buy'] or bsjp['buy']:
                last_close=df['Close'].iloc[-1]
                prev=df['Close'].iloc[-2] if len(df)>=2 else last_close
                chg=(last_close/prev-1)*100 if prev else 0
                v_last=df['Volume'].iloc[-1]
                v_avg=df['Volume'].tail(20).mean()
                vol_ratio=v_last/v_avg if v_avg else 1
                return {"symbol":sym,"close":int(last_close),"change_pct":chg,"vol_ratio":vol_ratio,"kim_buy":kim['buy'],"bsjp_buy":bsjp['buy'],"kim":kim,"bsjp":bsjp,"vol_rp":v_last*last_close}
        except Exception as e:
            print(f"scan_kim err {sym}: {e}")
            return None
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs={ex.submit(check_one,s):s for s in tickers}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)
    detected.sort(key=lambda x: (x['kim_buy'] and x['bsjp_buy'], x['vol_ratio']), reverse=True)
    return detected

def send_reply(cid, txt, rm=None):
    if not TELEGRAM_BOT_TOKEN or not cid: return False
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    pl={"chat_id":cid,"text":txt,"parse_mode":"Markdown"}
    if rm: pl["reply_markup"]=rm
    try:
        r=requests.post(url,json=pl,timeout=20)
        j=r.json()
        if not j.get('ok'):
            pl.pop('parse_mode',None)
            r=requests.post(url,json=pl,timeout=20)
            return r.json().get('ok',False)
        return True
    except Exception as e:
        print(f"send_reply err {e}")
        return False

def send_photo_reply(cid, path, caption=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(path,'rb') as ph:
            requests.post(url,data={'chat_id':cid,'caption':caption,'parse_mode':'Markdown'},files={'photo':ph},timeout=40)
    except Exception as e: print(f"Photo err {e}")

def process_chart_request(cid, code, tf_input="1d"):
    try:
        print(f"CHART REQ {code} {tf_input} -> {cid}")
        tf_norm=normalize_timeframe(tf_input)
        tf_label=format_timeframe_label(tf_norm)
        send_reply(cid, f"📊 *{code.upper()} ({tf_label}) chart pro KIM+BSJP...*")
        df=get_history_pro(code, 150, frame=tf_norm)
        if df is None or len(df)<20:
            send_reply(cid, f"⚠ Data {code} TF {tf_norm} kosong"); return
        quotes=get_itick_quotes_batch([code],1)
        realtime=quotes.get(code.upper(),{}).get('price',0)
        chart_file=f"/tmp/chart_{code.upper()}_{tf_norm}_{int(time.time())}.png"
        fp, _ = generate_pro_chart(df, symbol=code.upper(), timeframe=tf_norm, output_filename=chart_file, extra_info={'tf_label':tf_label}, realtime_price=realtime)
        if not fp or not os.path.exists(fp):
            send_reply(cid, f"❌ Gagal render {code} {tf_norm}"); return
        kim=detect_kim_signal(df)
        bsjp=detect_bsjp_signal(df, timeframe=tf_norm)
        caption,_=generate_caption_pro(code.upper(), df, realtime_price=realtime, tf_norm=tf_norm, kim=kim, bsjp=bsjp)
        send_photo_reply(cid, fp, caption=caption)
        if os.path.exists(fp): os.remove(fp)
        print(f"CHART DONE {code}")
    except Exception as e:
        print(f"CHART ERR {e}")
        import traceback; traceback.print_exc()
        try: send_reply(cid, f"❌ Err chart {code}: {e}")
        except: pass

def broadcast_vol_spike(signals, threshold=1.5, dest_chat_id=None):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target: return
    if not signals:
        send_reply(target, f"Vol Spike >{threshold}x (300 liquid): Tidak ada"); return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header=f"*VOL SPIKE >{threshold}x* 🔥 {now} - {len(signals)} saham (TOP 300 LIQUID)\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Vol {it['vol_ratio']:.1f}x\n"
        kb.append([{"text": f"{it['symbol']} {it['vol_ratio']:.1f}x", "callback_data": f"chart_{it['symbol']}"}])
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
        else: msg+=line
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

def broadcast_v417(signals, vol_thr=1.5, dest_chat_id=None, is_auto=False):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target: return
    if not signals:
        if not is_auto: send_reply(target, f"📉 VOL>{vol_thr}x - Tidak ada BO"); return
    now=get_now_wib().strftime('%d %b %Y %H:%M:%S WIB')
    tag="⚡ REALTIME" if is_auto else "🚀"
    header=f"{tag} *TOP {len(signals)} BO/BOB* 🔥\n{now}\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        line=f"{idx}. *{it['symbol']}* {it['type']} {it['close']} ({it['change_pct']:+.1f}%) Vol {it['vol_ratio']:.1f}x\n"
        kb.append([{"text": f"{it['symbol']} {it['type']}", "callback_data": f"chart_{it['symbol']}"}])
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
        else: msg+=line
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

def broadcast_kim_bsjp(signals, dest_chat_id=None, is_auto=False):
    target=dest_chat_id or TARGET_CHAT_ID
    if not target: return
    if not signals:
        if not is_auto: send_reply(target, f"🔍 KIM+BSJP (300 liquid): Tidak ada sinyal BUY hari ini"); return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    tag="⚡ AUTO" if is_auto else "🚀"
    header=f"{tag} *KIM + BSJP BUY* 🔥 {now}\nTOP 300 Most Liquid - {len(signals)} sinyal\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        kim_icon="✅" if it['kim_buy'] else "⏳"
        bsjp_icon="✅" if it['bsjp_buy'] else "⏳"
        both="🔥 KIM+BSJP" if it['kim_buy'] and it['bsjp_buy'] else "KIM BUY" if it['kim_buy'] else "BSJP BUY SORE"
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) {both} Vol {it['vol_ratio']:.1f}x\n  KIM {kim_icon} Modal {it['kim'].get('b_modal',0):.0f} | BSJP {bsjp_icon}\n"
        kb.append([{"text": f"{it['symbol']} {both}", "callback_data": f"chart_{it['symbol']}"}])
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
        else: msg+=line
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

def run_interactive_scanner(limit=300, threshold=1.5):
    try:
        import ipywidgets as widgets
        from IPython.display import display, Image, clear_output
    except:
        print("ipywidgets belum install, run !pip install ipywidgets")
        return
    print(f"🚀 Scan {limit} saham paling liquid threshold {threshold}x...")
    sigs=scan_volume_spike(threshold=threshold, limit_candidates=limit)
    print(f"✅ Ditemukan {len(sigs)} saham")
    if not sigs: return
    output_area=widgets.Output()
    def on_click(b):
        sym=b.description.split(" ")[0]
        with output_area:
            clear_output(wait=True)
            print(f"⏳ Render {sym} chart pro KIM+BSJP...")
            df=get_history_pro(sym,150,"1d")
            if df is not None:
                q=get_realtime_from_screener(sym)
                f=f"/tmp/{sym}_{int(time.time())}.png"
                fp,_=generate_pro_chart(df,symbol=sym,timeframe="1d",output_filename=f,extra_info={'tf_label':'Daily'},realtime_price=q[0])
                if fp and os.path.exists(fp):
                    display(Image(filename=fp))
    buttons=[]
    for it in sigs[:36]:
        btn=widgets.Button(description=f"{it['symbol']} {it['vol_ratio']:.1f}x", button_style='info', layout=widgets.Layout(width='140px',margin='2px'))
        btn.on_click(on_click)
        buttons.append(btn)
    grid=widgets.GridBox(buttons, layout=widgets.Layout(grid_template_columns='repeat(6, 150px)'))
    display(widgets.VBox([widgets.HTML("<h3>💡 Klik saham - Chart Informatif KIM+BSJP (300 liquid):</h3>"), grid, output_area]))

def run_kim_bsjp_scanner_interactive(limit=300):
    try:
        import ipywidgets as widgets
        from IPython.display import display, Image, clear_output
    except:
        print("ipywidgets belum install")
        return
    print(f"🚀 Scan KIM+BSJP {limit} most liquid...")
    sigs=scan_kim_bsjp_300()
    print(f"✅ Ditemukan {len(sigs)} sinyal")
    if not sigs: print("Tidak ada KIM/BSJP buy hari ini"); return
    output_area=widgets.Output()
    def on_click(b):
        sym=b.description.split(" ")[0]
        with output_area:
            clear_output(wait=True)
            print(f"⏳ Render {sym}...")
            df=get_history_pro(sym,150,"1d")
            if df is not None:
                q=get_realtime_from_screener(sym)
                f=f"/tmp/{sym}_{int(time.time())}.png"
                fp,_=generate_pro_chart(df,symbol=sym,timeframe="1d",output_filename=f,extra_info={'tf_label':'Daily'},realtime_price=q[0])
                if fp and os.path.exists(fp):
                    display(Image(filename=fp))
    buttons=[]
    for it in sigs[:36]:
        label=f"{it['symbol']} {'KIM' if it['kim_buy'] else ''}{'+' if it['kim_buy'] and it['bsjp_buy'] else ''}{'BSJP' if it['bsjp_buy'] else ''}"
        btn=widgets.Button(description=label, button_style='success' if it['kim_buy'] and it['bsjp_buy'] else 'warning', layout=widgets.Layout(width='160px',margin='2px'))
        btn.on_click(on_click)
        buttons.append(btn)
    grid=widgets.GridBox(buttons, layout=widgets.Layout(grid_template_columns='repeat(4, 170px)'))
    display(widgets.VBox([widgets.HTML(f"<h3>🔥 KIM+BSJP - {len(sigs)} sinyal dari 300 liquid:</h3>"), grid, output_area]))

def auto_broadcast_loop():
    global AUTO_KIM_ENABLED
    print("🔄 Auto broadcaster KIM+BSJP started - cek tiap 15 menit jam market 09:00-15:00 WIB")
    while True:
        try:
            if not AUTO_KIM_ENABLED:
                time.sleep(60); continue
            now=get_now_wib()
            # hanya jam market 09:00-15:30 WIB Senin-Jumat
            if now.weekday()>=5:
                time.sleep(300); continue
            if not (9 <= now.hour < 16):
                time.sleep(300); continue
            print(f"⏰ Auto scan KIM+BSJP 300 @ {now}")
            sigs=scan_kim_bsjp_300()
            if sigs:
                broadcast_kim_bsjp(sigs, is_auto=True)
            else:
                print("Auto scan: no signal")
            time.sleep(900)  # 15 menit
        except Exception as e:
            print(f"Auto broadcast err {e}")
            import traceback; traceback.print_exc()
            time.sleep(300)

def telegram_bot_listener():
    global AUTO_KIM_ENABLED
    offset=0
    print(f"🤖 RAFANO V4.31 KIM+BSJP - {len(IDX_FULL)} IDX - 300 LIQUID - CLICK OK")
    try: requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true",timeout=10)
    except: pass

    # start auto broadcaster thread
    threading.Thread(target=auto_broadcast_loop, daemon=True).start()

    while True:
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res=requests.get(url,timeout=25)
            if res.status_code!=200: time.sleep(3); continue
            data=res.json()
            if not data.get('ok'): time.sleep(3); continue
            for update in data.get("result",[]):
                offset=update["update_id"]+1
                if "callback_query" in update:
                    cb=update["callback_query"]; qid=cb.get("id"); cdata=cb.get("data","")
                    chat_id=cb.get("message",{}).get("chat",{}).get("id") or cb.get("from",{}).get("id") or TARGET_CHAT_ID
                    print(f"🔘 CLICK {cdata} -> {chat_id}")
                    try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",json={"callback_query_id":qid,"text":f"Loading {cdata}..."},timeout=5)
                    except: pass
                    if cdata.startswith("chart_"):
                        sym=cdata.replace("chart_","").strip().upper()
                        send_reply(chat_id, f"🔘 Klik {sym} diterima, generate chart pro KIM+BSJP...")
                        threading.Thread(target=process_chart_request,args=(chat_id,sym,"1d"),daemon=True).start()
                    continue
                if "message" in update and "text" in update["message"]:
                    txt=update["message"].get("text","").strip()
                    chat_id=update["message"]["chat"]["id"]
                    first=txt.split()[0].lower() if txt else ""
                    parts=txt.split()
                    if first in ["/start","/help","/menu"]:
                        help_msg=f"""🔥 *RAFANO V4.31 KIM+BSJP*
TOP 300 Most Liquid | {len(IDX_FULL)} IDX Loaded

*CHART:*
/c KODE 5 = chart 5m pro KIM+BSJP
/c ERAA = daily chart

*SCANNER 300 LIQUID:*
/scanvol 1.5 = Vol Spike 300 liquid (default)
/scanvol 300 1.5 = custom 300

*KIM + BSJP SCREENER:*
/kim = Scan KIM BUY (300 liquid)
/bsjp = Scan BSJP BUY SORE (300)
/kimbsjp = Scan KIM+BSJP combo (300)
/scan300 = KIM+BSJP auto broadcaster test

*AUTO BROADCAST:*
/auto on = aktifkan auto broadcast 15 menit
/auto off = matikan
/status = cek status auto

*LAIN:*
/quota = cek quota & idx count
"""
                        send_reply(chat_id, help_msg)
                    elif first in ["/quota","/status"]:
                        send_reply(chat_id, f"QUOTA: {'HABIS' if QUOTA_HIT else 'OK'}\nIDX:{len(IDX_FULL)}\nTOP 300 liquid ready\nAUTO KIM+BSJP: {'ON' if AUTO_KIM_ENABLED else 'OFF'}")
                    elif first in ["/c","/chart"]:
                        if len(parts)>=2:
                            sym=parts[1].upper(); tf_input=parts[2] if len(parts)>=3 else "1d"
                            threading.Thread(target=process_chart_request,args=(chat_id,sym,tf_input),daemon=True).start()
                        else: send_reply(chat_id, "Pakai: /c ERAA 5 atau /c BBCA")
                    elif first in ["/scanvol","/vol","/vol300"]:
                        try:
                            if len(parts)>=3:
                                lim=int(parts[1]); thr=float(parts[2])
                            elif len(parts)>=2:
                                # /scanvol 1.5 atau /scanvol 300
                                try:
                                    thr=float(parts[1]); lim=300
                                    if thr>10: # user input 300 as first arg
                                        lim=int(thr); thr=1.5
                                except:
                                    lim=300; thr=1.5
                            else:
                                lim=300; thr=1.5
                        except: lim=300; thr=1.5
                        send_reply(chat_id, f"🔥 SCAN VOL >{thr}x - {lim} most liquid...")
                        def run_vol(tg=chat_id, th=thr, li=lim):
                            sigs=scan_volume_spike(threshold=th, limit_candidates=li)
                            broadcast_vol_spike(sigs, threshold=th, dest_chat_id=tg)
                        threading.Thread(target=run_vol,daemon=True).start()
                    elif first in ["/kim","/kimbsjp","/scan300","/scan_kim","/kbs"]:
                        send_reply(chat_id, f"🚀 SCAN KIM+BSJP - 300 most liquid...")
                        def run_kim(tg=chat_id):
                            sigs=scan_kim_bsjp_300()
                            broadcast_kim_bsjp(sigs, dest_chat_id=tg)
                        threading.Thread(target=run_kim,daemon=True).start()
                    elif first in ["/bsjp"]:
                        send_reply(chat_id, f"💥 SCAN BSJP BUY SORE - 300 liquid...")
                        def run_bsjp(tg=chat_id):
                            sigs=scan_kim_bsjp_300()
                            # filter only bsjp buy
                            bsjp_only=[s for s in sigs if s['bsjp_buy']]
                            broadcast_kim_bsjp(bsjp_only, dest_chat_id=tg)
                        threading.Thread(target=run_bsjp,daemon=True).start()
                    elif first in ["/auto"]:
                        if len(parts)>=2 and parts[1].lower()=="on":
                            AUTO_KIM_ENABLED=True
                            send_reply(chat_id, "✅ Auto broadcast KIM+BSJP ON - scan tiap 15 menit jam 09:00-15:30 WIB (300 liquid)")
                        elif len(parts)>=2 and parts[1].lower()=="off":
                            AUTO_KIM_ENABLED=False
                            send_reply(chat_id, "❌ Auto broadcast OFF")
                        else:
                            send_reply(chat_id, f"AUTO KIM+BSJP: {'ON' if AUTO_KIM_ENABLED else 'OFF'}\nPakai /auto on atau /auto off")
                    elif first.startswith("/scanbo") or first.startswith("/scan"):
                        if first in ["/scanvol","/scanvol300","/scan300","/kim","/bsjp","/kimbsjp","/scan_kim","/kbs"]: pass
                        else:
                            vol_thr=1.5
                            try:
                                if len(parts)>=2: vol_thr=float(parts[1])
                            except: pass
                            send_reply(chat_id, f"🚀 SCAN BO {len(IDX_FULL)} VOL>{vol_thr}x...")
                            def run_scan(tg=chat_id, vt=vol_thr):
                                sigs=scan_v417_final(top_arjum=60, top_itick=30, vol_thr=vt)
                                broadcast_v417(sigs, vol_thr=vt, dest_chat_id=tg)
                            threading.Thread(target=run_scan,daemon=True).start()
        except Exception as e:
            print(f"Listener err {e}")
            import traceback; traceback.print_exc()
            time.sleep(3)

if __name__=="__main__":
    print(f"🔥 RAFANO V4.31 KIM+BSJP - {len(IDX_FULL)} IDX - TOP 300 LIQUID")
    telegram_bot_listener()
