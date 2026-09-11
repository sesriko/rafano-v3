"""
RAFANO V4.13 CAPTION PRO - Caption baru kayak BUMI, chart tetap BMTR hitam pro
BUMI — Harga 206 (+7.29%)
    ├  Buy Strength Score: 95% (VERY STRONG)
    ├  RSI (14): 66.31
    ├ Vol Spike: 2.1x V1 | Buy Vol: 89%
    └ Bandar 1W: +1014.67B (ACCUM
"""
import os, time, datetime, threading, requests, pytz
import numpy as np, pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.gridspec as gridspec
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

for p in ['/content/rafano-v3/.env','./.env','.env','/content/.env']:
    if os.path.exists(p):
        load_dotenv(p, override=True)
        print(f"✅ Loaded .env from {p}")
        break
else:
    load_dotenv()

TIMEZONE_WIB=pytz.timezone('Asia/Jakarta')
TELEGRAM_BOT_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN") or ""
TARGET_CHAT_ID=os.getenv("TARGET_CHAT_ID") or ""
ARJUM_API_KEY=os.getenv("ARJUM_API_KEY") or ""
ARJUM_BASE="https://stock.arjum.com/api"
ITICK_TOKEN=os.getenv("ITICK_TOKEN") or os.getenv("ITICK_API_KEY") or "7a470a83276242309fb940684046d35a88e450fdb95b46c383670e1e0c5e96f5"
ITICK_BASE="https://api.itick.org"
ITICK_ENABLED=bool(ITICK_TOKEN)

def get_now_wib(): return datetime.datetime.now(TIMEZONE_WIB)

FCA_EXCLUDE={"FUTR","FITT","HOTEL","ITIC","PUDP","COIN","SHID","RELI","ASPI","MEJA","MINA","ESTA","ASLI","VKTR","IMJS","GTSI","IRSX","ATAP","RONY","BCIC","DEFI","ROCK","YPAS","NIRO","BBHA","BKSW","NAGA","BEEF","BPTR","CBMF","CPRI","CRAB","DAAZ","DEAL","DGNS","DMND","DUCK","ELSA","ENRG","ENVY","ERAA","ESTI","ETWA","FIRE","FORU","GAMA","GOLL","HAIS","HATM","HITS","HOMI","IATA","INPS","IPOL","JGLE","KAYU","KBAG","KIOS","KPAL","KPAS","LCGP","LPLI","LPLR","MAGP","MAMI","MARI","SINI","SKYB","SMKM","SOCI","SONA","SOSS","SUGI","TALF","TDPM","TEBE","TOPS","TRAM","TRIL","TRIO","TRUS","UFOE","WIFI-W","WOWS","YELO","ZATA","ZONE","ZINC","TINS-W","BIPI-W","BULL-W","DEWA-W"}
IDX_600_LIQUID=["BBCA","BBRI","BMRI","BBNI","TLKM","ASII","BMTR","BIPI","GOTO","BUKA","BBKP","BRIS","ANTM","INCO","MDKA","ADRO","PTBA","PGAS","EXCL","ISAT","BREN","CUAN","WIFI","DEWA","BULL","NCKL","AMRT","TOWR","TBIG","ELSA","BEKS","BNGA","AALI","ACES","ADMR","AKRA","AMMN","BRMS","BRPT","BSDE","CPIN","CTRA","EMTK","ICBP","INDF","INKP","INTP","ITMG","JPFA","KLBF","MEDC","SMGR","SMRA","TPIA","UNTR","UNVR"]

HISTORY_CACHE={}; SCREENER_CACHE={}; BROKER_CACHE={}
HISTORY_CACHE_TTL=300; SCREENER_CACHE_TTL=120; BROKER_CACHE_TTL=300
QUOTA_HIT=False; LAST_429_TIME=0
ALERTED_TODAY=set()
ALERTED_DATE=None

def get_cached(k, cache, ttl):
    if k in cache:
        ts,d=cache[k]
        if time.time()-ts<ttl: return d
    return None
def set_cached(k,d,cache):
    cache[k]=(time.time(),d)

def arjum_get(path, params=None):
    global QUOTA_HIT, LAST_429_TIME
    if QUOTA_HIT and time.time()-LAST_429_TIME<300: return None
    url=f"{ARJUM_BASE}{path}"
    try:
        headers={"X-API-Key": ARJUM_API_KEY.strip(),"Accept":"application/json","User-Agent":"Mozilla/5.0"}
        r=requests.get(url,headers=headers,params=params,timeout=12)
        if r.status_code==200: return r.json()
        elif r.status_code==429:
            QUOTA_HIT=True; LAST_429_TIME=time.time(); return None
    except: pass
    return None

def get_screener_latest(force_today=False):
    if not force_today:
        c=get_cached('latest', SCREENER_CACHE, SCREENER_CACHE_TTL)
        if c and isinstance(c, dict) and 'rows' in c and len(c['rows'])>0: return c
    data=arjum_get("/screener/latest")
    if data and isinstance(data, dict) and 'rows' in data and len(data['rows'])>0:
        set_cached('latest', data, SCREENER_CACHE); return data
    return {"rows": [{"stock_code": c, "close": 100} for c in IDX_600_LIQUID[:150]]}

def get_itick_quotes_batch(symbols, max_batch=15):
    if not ITICK_ENABLED or not symbols: return {}
    all_quotes={}
    try:
        for i in range(0, len(symbols), max_batch):
            batch=symbols[i:i+max_batch]
            codes=",".join(batch)
            url = f"{ITICK_BASE}/stock/quotes?region=ID&codes={codes}"
            headers = {"accept": "application/json", "token": ITICK_TOKEN}
            r = requests.get(url, headers=headers, timeout=8)
            if r.status_code==200:
                j=r.json()
                if j.get('code')==0 and j.get('data'):
                    for item in j['data']:
                        code=item.get('s') or item.get('code')
                        if not code: continue
                        code=str(code).upper().replace(".JK","")
                        all_quotes[code]={'price': float(item.get('ld') or item.get('c') or 0),'changepct': float(item.get('chp') or 0),'high': float(item.get('h') or 0),'low': float(item.get('l') or 0),'volume': float(item.get('v') or 0),'source': 'ITICK_REALTIME'}
            time.sleep(0.2)
    except: pass
    return all_quotes

def normalize_timeframe(tf_input):
    if not tf_input or str(tf_input).strip()=="":
        return "1d"
    tf = str(tf_input).lower().strip()
    mapping={
        "5":"5m","5m":"5m","5min":"5m","5menit":"5m","m5":"5m",
        "15":"15m","15m":"15m","15min":"15m","15menit":"15m","m15":"15m",
        "30":"30m","30m":"30m","30min":"30m","m30":"30m",
        "1h":"1h","60":"1h","60m":"1h","1j":"1h","1jam":"1h","h1":"1h",
        "4h":"4h","240":"4h","4jam":"4h",
        "1d":"1d","d":"1d","daily":"1d","harian":"1d","day":"1d","1":"1d",
        "1w":"1w","w":"1w","weekly":"1w","minggu":"1w"
    }
    return mapping.get(tf, tf)

def get_history_pro(sym, limit=150, frame="daily"):
    frame = normalize_timeframe(frame)
    hk=f"{sym}_{frame}_{limit}"
    cached=get_cached(hk, HISTORY_CACHE, HISTORY_CACHE_TTL)
    if cached is not None: return cached
    if frame=="1d" and not QUOTA_HIT:
        data=arjum_get(f"/history/{sym}",params={"limit":limit,"frame":"daily"})
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
                    set_cached(hk,df,HISTORY_CACHE); return df
            except: pass
    try:
        import yfinance as yf
        yf_interval_map={"5m":"5m","15m":"15m","30m":"30m","1h":"1h","4h":"1h","1d":"1d","1w":"1wk"}
        interval=yf_interval_map.get(frame, "1d")
        if frame in ["5m","15m"]: period="5d"
        elif frame in ["30m","1h"]: period="1mo"
        elif frame=="4h": period="3mo"
        elif frame=="1d": period="6mo"
        else: period="1y"
        ticker=yf.Ticker(f"{sym}.JK")
        hist=ticker.history(period=period,interval=interval,timeout=15, auto_adjust=False)
        if hist is not None and len(hist)>20:
            if frame=="4h" and interval=="1h":
                hist=hist.resample('4H').agg({'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'}).dropna()
            set_cached(hk,hist.tail(limit),HISTORY_CACHE)
            return hist.tail(limit)
    except Exception as e:
        print(f"YF err {sym} TF {frame}: {e}")
    return None

def get_bandar_info(sym):
    cache_key=f"bandar_{sym}"
    cached=get_cached(cache_key, BROKER_CACHE, BROKER_CACHE_TTL)
    if cached is not None: return cached
    result={"foreign_net":0,"foreign_net_1w":0,"is_foreign_buy":False,"foreign_str":"N/A","foreign_str_1w":"N/A","score_bonus":0,"accum_type":"NEUTRAL"}
    try:
        fdata=arjum_get(f"/foreign/{sym}", params={"limit": 5, "frame": "daily"})
        if fdata:
            rows=fdata.get('data') if isinstance(fdata, dict) else fdata
            if isinstance(rows, list) and len(rows)>0:
                # 1 hari
                last=rows[0]
                fn=float(last.get('foreign_net') or last.get('net_foreign') or 0)
                if fn!=0:
                    result["foreign_net"]=fn
                    result["is_foreign_buy"]=fn>0
                    result["foreign_str"]=f"{'FB' if fn>0 else 'FS'} {fn/1_000_000_000:+.1f}B" if abs(fn)>=1e9 else f"{'FB' if fn>0 else 'FS'} {fn/1_000_000:+.0f}M"
                    result["score_bonus"]+=15 if fn>0 else 0
                # 1 minggu (5 hari)
                fn_1w=0
                for r in rows[:5]:
                    fn_1w+=float(r.get('foreign_net') or r.get('net_foreign') or 0)
                result["foreign_net_1w"]=fn_1w
                if fn_1w!=0:
                    result["foreign_str_1w"]=f"{'+' if fn_1w>0 else ''}{fn_1w/1_000_000_000:.2f}B" if abs(fn_1w)>=1e9 else f"{'+' if fn_1w>0 else ''}{fn_1w/1_000_000:.0f}M"
                    if fn_1w>1_000_000_000: result["accum_type"]="ACCUM"
                    elif fn_1w<-1_000_000_000: result["accum_type"]="DIST"
                    else: result["accum_type"]="NEUTRAL"
    except: pass
    set_cached(cache_key, result, BROKER_CACHE)
    return result

def format_large_number(val,show_sign=False):
    if pd.isna(val) or val==0: return "0"
    a=abs(val); s="+" if (show_sign and val>0) else ("-" if val<0 else "")
    if a>=1_000_000_000: return f"{s}{a/1_000_000_000:.2f}B"
    elif a>=1_000_000: return f"{s}{a/1_000_000:,.0f}M"
    elif a>=1_000: return f"{s}{a/1_000:,.0f}K"
    else: return f"{s}{val:,.0f}"

def format_timeframe_label(tf):
    tf=normalize_timeframe(tf)
    m={"5m":"5 Menit","15m":"15 Menit","30m":"30 Menit","1h":"1 Jam","4h":"4 Jam","1d":"Daily","1w":"Weekly"}
    return m.get(tf, tf.upper())

def calculate_rsi(prices, period=14):
    """RSI 14"""
    try:
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss.replace(0, 0.001)
        rsi = 100 - (100 / (1 + rs))
        return float(rsi.iloc[-1]) if len(rsi)>0 and not pd.isna(rsi.iloc[-1]) else 50.0
    except:
        return 50.0

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
    df['Net_Vol_VSA']=df['Vol_Buy']-df['Vol_Sell']; df['Buy_Pct']=br*100
    return df,br

def calculate_bollinger_bands(df,p=20,s=2):
    sma=df['Close'].rolling(p).mean(); std=df['Close'].rolling(p).std()
    return sma,sma+(std*s),sma-(std*s)

def check_bo_ema50_bob200(sym, hd, realtime_price=None):
    try:
        if hd is None or len(hd) < 55: return None
        c = float(realtime_price if realtime_price and realtime_price>0 else hd['Close'].iloc[-1])
        pc = float(hd['Close'].iloc[-2])
        if c < 50: return None
        ema50_s = hd['Close'].ewm(span=50, adjust=False).mean()
        ema200_s = hd['Close'].ewm(span=200, adjust=False).mean()
        ema50 = float(ema50_s.iloc[-1]); ema200 = float(ema200_s.iloc[-1])
        pe50 = float(ema50_s.iloc[-2]); pe200 = float(ema200_s.iloc[-2])
        is_bo=False; stype=""
        if pc <= pe50 and c > ema50 and c > pe50: is_bo=True; stype="BO EMA50"
        elif pc <= pe200 and c > ema200 and c > pe200: is_bo=True; stype="BOB EMA200"
        if not is_bo: return None
        return {"type": stype, "ema50": ema50, "ema200": ema200}
    except: return None

def detect_bo_bos_markers(df):
    signals=[]
    if len(df)<55: return signals
    df=df.copy()
    df['EMA13']=df['Close'].ewm(span=13,adjust=False).mean()
    df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
    df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
    df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
    for i in range(50, len(df)):
        c=df['Close'].iloc[i]; pc=df['Close'].iloc[i-1]
        ema50=df['EMA50'].iloc[i]; pe50=df['EMA50'].iloc[i-1]
        ema200=df['EMA200'].iloc[i]; pe200=df['EMA200'].iloc[i-1]
        if pc <= pe50 and c > ema50 and c > pe50:
            signals.append({"idx":i,"type":"BO EMA50","color":"green"})
        elif pc <= pe200 and c > ema200 and c > pe200:
            signals.append({"idx":i,"type":"BOB EMA200","color":"green"})
        elif i>=2:
            prev_high=max(df['High'].iloc[i-1], df['High'].iloc[i-2])
            if c > prev_high and c > ema50 and pc <= ema50*1.01:
                if not any(s['idx']==i and 'BO' in s['type'] for s in signals):
                    signals.append({"idx":i,"type":"BOS EMA","color":"yellow"})
    return signals

# ===== CAPTION PRO BARU - Kayak BUMI yang lu mau =====
def generate_caption_pro(symbol, df, realtime_price=None, tf_norm="1d"):
    """
    BUMI — Harga 206 (+7.29%)
        ├  Buy Strength Score: 95% (VERY STRONG)
        ├  RSI (14): 66.31
        ├ Vol Spike: 2.1x V1 | Buy Vol: 89%
        └ Bandar 1W: +1014.67B (ACCUM
    """
    try:
        df=df.copy()
        if realtime_price and realtime_price>0 and len(df)>0:
            df.iloc[-1, df.columns.get_loc('Close')] = realtime_price
        
        last_close=df['Close'].iloc[-1]
        prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close
        chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
        
        # RSI
        rsi = calculate_rsi(df['Close'], 14)
        
        # VSA Metrics
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        df_vsa, buy_ratios = calculate_vsa_metrics(df)
        buy_pct = int(buy_ratios.iloc[-1]*100) if hasattr(buy_ratios,'iloc') else int(buy_ratios[-1]*100)
        buy_pct = max(0, min(100, buy_pct))
        
        # Buy Strength Score = kombinasi Buy Vol + close pos + RSI
        vchg = df['Volume'].iloc[-1] / df['V1'].iloc[-1] if df['V1'].iloc[-1]>0 else 1.0
        high_today=df['High'].iloc[-1]; low_today=df['Low'].iloc[-1]
        close_pos = (last_close - low_today) / (high_today - low_today) if high_today!=low_today else 1.0
        # Score: buy_pct 40% + close_pos 30% + rsi normalized 30%
        rsi_score = min(100, max(0, (rsi-30)/40*100))  # RSI 30=0%, 70=100%
        buy_strength = int(buy_pct*0.4 + close_pos*100*0.3 + rsi_score*0.3)
        buy_strength = max(0, min(100, buy_strength))
        
        if buy_strength>=90: strength_label="VERY STRONG"
        elif buy_strength>=75: strength_label="STRONG"
        elif buy_strength>=60: strength_label="MODERATE"
        elif buy_strength>=40: strength_label="WEAK"
        else: strength_label="VERY WEAK"
        
        # Vol Spike vs V1
        v1=df['V1'].iloc[-1]
        vol_spike = df['Volume'].iloc[-1] / v1 if v1>0 else 1.0
        
        # Bandar 1W
        bandar=get_bandar_info(symbol)
        bandar_1w_val=bandar.get('foreign_net_1w',0)
        bandar_1w_str=bandar.get('foreign_str_1w','N/A')
        accum_type=bandar.get('accum_type','NEUTRAL')
        
        # Format harga
        harga_str = f"{int(last_close)}"
        chg_str = f"{chg_pct:+.2f}%"
        
        # TF label
        tf_label = format_timeframe_label(tf_norm)
        
        caption = f"""{symbol} — Harga {harga_str} ({chg_str}) | {tf_label}
    ├  Buy Strength Score: {buy_strength}% ({strength_label})
    ├  RSI (14): {rsi:.2f}
    ├  Vol Spike: {vol_spike:.1f}x V1 | Buy Vol: {buy_pct}%
    └  Bandar 1W: {bandar_1w_str} ({accum_type})"""
        
        return caption, {
            "buy_strength": buy_strength,
            "strength_label": strength_label,
            "rsi": rsi,
            "vol_spike": vol_spike,
            "buy_vol": buy_pct,
            "bandar_1w": bandar_1w_str,
            "accum": accum_type,
            "close": last_close,
            "chg_pct": chg_pct
        }
    except Exception as e:
        print(f"Caption err {e}")
        import traceback; traceback.print_exc()
        return f"{symbol} — Harga {int(df['Close'].iloc[-1]) if len(df)>0 else 0}", {}

# CHART TETAP SAMA - BMTR HITAM PRO
def generate_pro_chart(df,symbol="BBCA",timeframe="5m",sector_info="IHSG",output_filename="chart.png",extra_info=None,realtime_price=None):
    try:
        extra_info=extra_info or {}
        timeframe_norm = normalize_timeframe(timeframe)
        tf_label_disp=extra_info.get('tf_label') or format_timeframe_label(timeframe_norm)
        df=df.copy().ffill().bfill()
        if not isinstance(df.index,pd.DatetimeIndex): df.index=pd.to_datetime(df.index)
        else: df=df.sort_index()
        if realtime_price and realtime_price>0 and len(df)>0:
            df.iloc[-1, df.columns.get_loc('Close')] = realtime_price
            if realtime_price > df['High'].iloc[-1]: df.iloc[-1, df.columns.get_loc('High')] = realtime_price
            if realtime_price < df['Low'].iloc[-1]: df.iloc[-1, df.columns.get_loc('Low')] = realtime_price
        df['EMA13']=df['Close'].ewm(span=13,adjust=False).mean()
        df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
        df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
        df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        sma20, upper_bb, lower_bb = calculate_bollinger_bands(df,20,2)
        df['BB_UP']=upper_bb; df['BB_LOW']=lower_bb
        df,buy_ratios=calculate_vsa_metrics(df)
        bo_markers = detect_bo_bos_markers(df)
        last_close=df['Close'].iloc[-1]; last_open=df['Open'].iloc[-1]; last_high=df['High'].iloc[-1]; last_low=df['Low'].iloc[-1]; last_vol=df['Volume'].iloc[-1]
        prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close
        chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
        avg_price=df['Close'].tail(20).mean()
        vchg1=(last_vol/df['Volume'].iloc[-2]) if len(df)>1 and df['Volume'].iloc[-2]>0 else 0
        avg5=df['Volume'].tail(5).mean(); vchg5=(last_vol/avg5) if avg5>0 else 0
        speed="FAST" if vchg1>2.0 else "SLOW" if vchg1<0.8 else "NORMAL"
        buy_pct_temp=int(buy_ratios.iloc[-1]*100) if hasattr(buy_ratios,'iloc') else int(buy_ratios[-1]*100)
        power="TURBO" if buy_pct_temp>=85 and vchg1>=1.2 else "STRONG" if buy_pct_temp>=70 or vchg1>=1.5 else "NORMAL" if buy_pct_temp>=60 else "WEAK"
        safety="GOOD" if last_close>df['EMA200'].iloc[-1] else "BAD"
        ema13=df['EMA13'].iloc[-1]; ema20=df['EMA20'].iloc[-1]; ema50=df['EMA50'].iloc[-1]; ema200=df['EMA200'].iloc[-1]
        buy_pct=buy_pct_temp; sell_pct=100-buy_pct
        net_vol=df['Net_Vol_VSA'].iloc[-1]; net_vol_5d=df['Net_Vol_VSA'].tail(5).sum()
        plt.style.use('dark_background')
        fig=plt.figure(figsize=(18,10),dpi=180,facecolor='#000000')
        gs=gridspec.GridSpec(4,1,height_ratios=[4.5,1.1,0.9,0.8],hspace=0.05)
        ax_main=fig.add_subplot(gs[0]); ax_vol=fig.add_subplot(gs[1],sharex=ax_main); ax_nbsa=fig.add_subplot(gs[2],sharex=ax_main); ax_mm=fig.add_subplot(gs[3],sharex=ax_main)
        fig.subplots_adjust(left=0.06,right=0.94,top=0.90,bottom=0.05)
        for ax in [ax_main,ax_vol,ax_nbsa,ax_mm]:
            ax.set_facecolor('#000000')
            ax.tick_params(colors='#aaaaaa',labelsize=8)
            ax.yaxis.tick_right()
            ax.grid(False)
            for spine in ax.spines.values(): spine.set_color('#333333')
        x=np.arange(len(df))
        for i in range(len(df)):
            o,h,l,c=df['Open'].iloc[i],df['High'].iloc[i],df['Low'].iloc[i],df['Close'].iloc[i]
            ax_main.plot([i,i],[l,h],color='#00ff00' if c>=o else '#ff4444',linewidth=0.7,alpha=0.9)
            body_low=min(o,c); body_h=max(0.3,abs(c-o))
            if c>=o:
                rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor='none',edgecolor='#00ff00',linewidth=0.7)
            else:
                rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor='#ff3333',edgecolor='#ff3333',linewidth=0.7)
            ax_main.add_patch(rect)
        ax_main.plot(x,df['EMA13'],color='#ffff00',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA20'],color='#ff0000',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA50'],color='#ffffff',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA200'],color='#a020f0',linewidth=1.3,alpha=0.9)
        ax_main.plot(x,df['BB_UP'],color='#444488',linewidth=0.8,linestyle='--',alpha=0.6)
        ax_main.plot(x,df['BB_LOW'],color='#444488',linewidth=0.8,linestyle='--',alpha=0.6)
        for sig in bo_markers:
            idx=sig['idx']
            if idx >= len(df): continue
            low=df['Low'].iloc[idx]
            if sig['type']=='BO EMA50' or sig['type']=='BOB EMA200':
                ax_main.plot(idx, low*0.985, marker='^', color='#00ff00', markersize=10, markeredgecolor='black', markeredgewidth=0.5)
                ax_main.text(idx, low*0.97, sig['type'], fontsize=7, color='black', ha='center', va='top', fontweight='bold', bbox=dict(facecolor='#00ff00', edgecolor='black', boxstyle='round,pad=0.2'))
            elif 'BOS' in sig['type']:
                ax_main.plot(idx, low*0.985, marker='^', color='#00ff00', markersize=8)
                ax_main.text(idx, low*0.97, sig['type'], fontsize=6, color='black', ha='center', va='top', fontweight='bold', bbox=dict(facecolor='#ffff00', edgecolor='black', boxstyle='round,pad=0.2'))
        if len(df)>20:
            ax_main.add_patch(patches.Rectangle((len(df)-20, df['Low'].min()), 20, df['High'].max()-df['Low'].min(), fill=False, edgecolor='white', linestyle='--', linewidth=0.5, alpha=0.5))
        ax_main.set_xlim(-1,len(df)-1+15)
        ax_main.set_ylim(df['Low'].min()*0.97, df['High'].max()*1.05)
        left_text=f"Avg Price : {avg_price:.1f}\nVchg 1 Bar: {vchg1:.1f} x\nVchg 5 Bar: {vchg5:.1f} x\nSpeed : {speed}\nPower : {power}\nSafety : {safety}\n\nEMA 13 : {ema13:.1f}\nEMA 20 : {ema20:.1f}\nEMA 50 : {ema50:.1f}\nEMA 200: {ema200:.1f}"
        ax_main.text(0.005,0.98,left_text,transform=ax_main.transAxes,va='top',ha='left',fontsize=7,family='monospace',color='#e0e0e0',bbox=dict(facecolor='black',alpha=0.7,edgecolor='#333333'))
        fig.text(0.005,0.96,f"{symbol} : {last_close:.0f} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=14,fontweight='bold',ha='left',va='center', family='monospace')
        fig.text(0.005,0.93,f"{symbol} | IHSG",color='#ffaa00',fontsize=8,ha='left')
        fig.text(0.005,0.905,f"● {'WAIT' if abs(chg_pct)<0.5 else 'GO'}",color='#aaaaaa',fontsize=9,ha='left', fontweight='bold')
        fig.text(0.005,0.885,f"High:{last_high:.0f} Low:{last_low:.0f} Open:{last_open:.0f} Vol:{last_vol:,.0f}",color='#00ffff',fontsize=7,ha='left')
        fig.text(0.5,0.96,"RAFANO TRADER V4.13 CAPTION PRO",color='white',fontsize=16,fontweight='bold',ha='center',va='center')
        ds=df.index[-1].strftime('%d %b %Y %H:%M') if hasattr(df.index[-1],'strftime') else get_now_wib().strftime('%d %b %Y %H:%M')
        fig.text(0.99,0.96,f"{tf_label_disp} | {ds}",color='#ffcc00',fontsize=11,ha='right',va='center', fontweight='bold')
        fig.text(0.99,0.93,f"Command BOT /C {symbol} {timeframe_norm}",color='#cccccc',fontsize=8,ha='right')
        ax_main.text(len(df)+1, last_close, f" {last_close:.0f}", color='black', fontsize=8, va='center', fontweight='bold', bbox=dict(facecolor='white', edgecolor='none', boxstyle='square,pad=0.2'))
        ax_main.text(len(df)+1, ema200, f" EMA 200", color='white', fontsize=7, va='center', bbox=dict(facecolor='#a020f0', edgecolor='none', boxstyle='square,pad=0.1'))
        vol_info=f"Buy % = {buy_pct}% Sell % = {sell_pct}% Net Vol = {net_vol:,.0f} 5D = {net_vol_5d:,.0f}"
        ax_vol.text(0.005,0.88,vol_info,transform=ax_vol.transAxes,color='#ffffff',fontsize=8,va='top')
        ax_vol.bar(x,df['Vol_Sell'],color='#cc0000',width=0.8,alpha=0.8)
        ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00cc00',width=0.8,alpha=0.9)
        ax_vol.plot(x,df['V1'],color='white',linewidth=0.8,alpha=0.9)
        ax_vol.set_ylim(0,df['Volume'].max()*2)
        plt.setp(ax_vol.get_xticklabels(),visible=False)
        ax_nbsa.text(0.005,0.85,f"NBSA Rp. {abs(net_vol*last_close)/1e6:.2f} M",transform=ax_nbsa.transAxes,color='#ffffff',fontsize=8,va='top')
        nbsa_vals=df['Net_Vol_VSA'].tail(150)/(df['Net_Vol_VSA'].abs().max() or 1)*50
        xn=np.arange(len(df)-len(nbsa_vals),len(df))
        for i,v in zip(xn,nbsa_vals): ax_nbsa.bar(i,v,color='#00ffff' if v>=0 else '#ff4444',width=0.6)
        ax_nbsa.axhline(0,color='#444444',linewidth=0.5); ax_nbsa.set_ylim(-60,60)
        ax_mm.text(0.005,0.85,"Market Maker",transform=ax_mm.transAxes,color='#ffffff',fontsize=8,va='top')
        if 'MM' not in df.columns: df['MM']=(df['Close']-df['EMA50'])/df['EMA50']*1000
        mm_vals=df['MM'].tail(150); xm=np.arange(len(df)-len(mm_vals),len(df))
        for i,v in zip(xm,mm_vals): ax_mm.bar(i,v,color='#cccccc' if v>=0 else '#888888',width=0.5,alpha=0.8)
        mm_last = df['MM'].iloc[-1] if len(df)>0 else 0
        ax_mm.text(len(df)+1, mm_last, f" {mm_last:.4f}", color='black', fontsize=7, va='center', fontweight='bold', bbox=dict(facecolor='#ffff00', edgecolor='none'))
        ax_mm.set_ylim(-30,30)
        step=max(1,len(df)//10); ax_mm.set_xticks(x[::step])
        if timeframe_norm in ["5m","15m","30m","1h","4h"]:
            labels=[df.index[i].strftime('%H:%M') if hasattr(df.index[i],'strftime') else str(i) for i in range(0,len(df),step)]
        else:
            labels=[df.index[i].strftime('%d/%b') if hasattr(df.index[i],'strftime') else str(i) for i in range(0,len(df),step)]
        ax_mm.set_xticklabels(labels,fontsize=7)
        plt.savefig(output_filename,dpi=180,bbox_inches='tight',facecolor='#000000')
        plt.close('all')
        return output_filename, bo_markers
    except Exception as e:
        print(f"Chart error {e}"); import traceback; traceback.print_exc(); return None, []
    finally:
        try: plt.clf(); plt.close('all')
        except: pass

def scan_v413_final(top_arjum=60, top_itick=30, vol_thr=1.5, min_value=1_000_000_000, min_closepos=0.6, only_new=False):
    global ALERTED_TODAY, ALERTED_DATE
    today = get_now_wib().date()
    if ALERTED_DATE != today:
        ALERTED_TODAY=set()
        ALERTED_DATE=today
    screener = get_screener_latest(force_today=True)
    rows = screener.get('rows', []) if isinstance(screener, dict) else []
    if not rows: return []
    candidates_arjum=[]
    for r in rows:
        code = (r.get('stock_code') or r.get('symbol') or "").replace(".JK","").upper()
        if not code or code in FCA_EXCLUDE or "-W" in code: continue
        if float(r.get('close') or 100) < 50: continue
        candidates_arjum.append(code)
        if len(candidates_arjum) >= top_arjum: break
    all_quotes = get_itick_quotes_batch(candidates_arjum, 15)
    if not all_quotes:
        all_quotes={}
        for r in rows:
            code=(r.get('stock_code') or "").replace(".JK","").upper()
            if code in candidates_arjum:
                all_quotes[code]={'price': float(r.get('close') or 0), 'changepct': float(r.get('change_pct') or 0), 'high': float(r.get('high') or 0), 'low': float(r.get('low') or 0), 'source': 'ARJUM_FALLBACK'}
    sorted_itick = sorted(all_quotes.items(), key=lambda x: x[1].get('changepct', -999), reverse=True)
    top_codes = [c for c,_ in sorted_itick[:top_itick]]
    def check_one(sym):
        try:
            if only_new and sym in ALERTED_TODAY: return None
            q=all_quotes.get(sym, {})
            realtime_price=q.get('price',0)
            hd=get_history_pro(sym, limit=120, frame="daily")
            if hd is None or len(hd)<55: return None
            c = float(realtime_price if realtime_price and realtime_price>0 else hd['Close'].iloc[-1])
            v_last=float(hd['Volume'].iloc[-1]); v_avg=float(hd['Volume'].iloc[-21:-1].mean())
            if v_avg==0 or v_last==0: return None
            if v_last/v_avg < vol_thr: return None
            if v_last * c < min_value: return None
            bo_info = check_bo_ema50_bob200(sym, hd, realtime_price)
            if not bo_info: return None
            bandar=get_bandar_info(sym)
            return {"symbol": sym, "type": bo_info['type'], "close": int(c), "change_pct": q.get('changepct',0), "vol_ratio": v_last/v_avg, "vol_rp": v_last * c, "bandar": bandar, "foreign_str": bandar.get('foreign_str',''), "source": q.get('source',''), "realtime": realtime_price}
        except: return None
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs={ex.submit(check_one, s): s for s in top_codes}
        detected=[]
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)
    detected.sort(key=lambda x: x['vol_ratio'], reverse=True)
    return detected

def scan_volume_spike(threshold=2.0, limit_candidates=60, sort_by_rp=False):
    sd=get_screener_latest(force_today=False)
    if isinstance(sd, dict) and 'rows' in sd: cands=[(r.get('stock_code') or "").replace(".JK","").upper() for r in sd['rows']]
    else: cands=IDX_600_LIQUID
    seen=set(); uniq=[]
    for c in cands:
        cu=c.upper().strip()
        if not cu or "-W" in cu or cu in FCA_EXCLUDE: continue
        if cu not in seen: seen.add(cu); uniq.append(cu)
    for c in IDX_600_LIQUID:
        if len(uniq)>=limit_candidates: break
        if "-W" in c or c in FCA_EXCLUDE or c in seen: continue
        uniq.append(c); seen.add(c)
    cands=uniq[:limit_candidates]
    detected=[]
    for sym in cands:
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
    if sort_by_rp: detected.sort(key=lambda x: x['vol_rp'], reverse=True)
    else: detected.sort(key=lambda x: x['vol_ratio'], reverse=True)
    return detected

def send_reply(cid, txt, rm=None):
    if not TELEGRAM_BOT_TOKEN or not cid: return False
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    pl={"chat_id":cid,"text":txt,"parse_mode":"Markdown"}
    if rm: pl["reply_markup"]=rm
    try:
        r=requests.post(url,json=pl,timeout=15)
        j=r.json()
        if not j.get('ok'):
            pl.pop('parse_mode',None)
            r=requests.post(url,json=pl,timeout=15)
            return r.json().get('ok',False)
        return True
    except: return False

def send_photo_reply(cid, path, caption=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(path,'rb') as ph:
            requests.post(url,data={'chat_id':cid,'caption':caption,'parse_mode':'Markdown'},files={'photo':ph},timeout=30)
    except Exception as e: print(f"Photo err {e}")

def process_chart_request(cid, code, tf_input="1d"):
    tf_norm = normalize_timeframe(tf_input)
    tf_label = format_timeframe_label(tf_norm)
    send_reply(cid, f"📊 *{code.upper()} ({tf_label}) chart pro + ITICK...*")
    df=get_history_pro(code, 150, frame=tf_norm)
    if df is None or len(df)<20:
        send_reply(cid, f"⚠ Data {code} TF {tf_norm} tidak ada"); return
    quotes=get_itick_quotes_batch([code],1)
    realtime=quotes.get(code.upper(),{}).get('price',0)
    chart_file=f"/tmp/chart_{code.upper()}_{tf_norm}_{int(time.time())}.png"
    fp, markers = generate_pro_chart(df, symbol=code.upper(), timeframe=tf_norm, sector_info=f"{code.upper()} | IHSG", output_filename=chart_file, extra_info={'tf_label':tf_label}, realtime_price=realtime)
    if not fp or not os.path.exists(fp):
        send_reply(cid, "❌ Gagal render chart"); return
    
    # CAPTION PRO BARU - kayak BUMI
    caption_pro, metrics = generate_caption_pro(code.upper(), df, realtime_price=realtime, tf_norm=tf_norm)
    # Tambah marker info
    sig_text = ", ".join([f"{s['type']}" for s in markers[-3:]]) if markers else "No BO signal"
    full_caption = caption_pro + f"\n\n🔍 {sig_text} | Command: /C {code.upper()} {tf_norm}"
    
    send_photo_reply(cid, fp, caption=full_caption)
    if os.path.exists(fp): os.remove(fp)

def process_broker_request(cid, sym):
    bandar=get_bandar_info(sym)
    send_reply(cid, f"🏦 *{sym} BANDAR*\n{bandar.get('foreign_str','N/A')} | 1W: {bandar.get('foreign_str_1w','N/A')} ({bandar.get('accum_type','')})")

def broadcast_v413(signals, vol_thr=1.5, dest_chat_id=None, is_auto=False):
    target = dest_chat_id or TARGET_CHAT_ID
    if not target: return
    if not signals:
        if not is_auto: send_reply(target, f"📉 V4.13 VOL>{vol_thr}x - Tidak ada BO valid"); 
        return
    now=get_now_wib().strftime('%d %b %Y %H:%M:%S WIB')
    auto_tag = "⚡ REALTIME ALERT" if is_auto else "🚀"
    header=f"{auto_tag} *V4.13 TOP {len(signals)} BO/BOB + ITICK* 🔥\n{now}\nArjum 60 -> ITICK 30 -> Vol>{vol_thr}x -> BO EMA50/BOB EMA200\n{'='*50}\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        rt_str = f" RT{it['realtime']:.0f}" if it.get('realtime',0)>0 else ""
        fb = it.get('foreign_str','')
        line=f"{idx}. *{it['symbol']}* {it['type']}{rt_str} {it['close']} ({it['change_pct']:+.1f}%) Vol {it['vol_ratio']:.1f}x Rp {format_large_number(it['vol_rp'])} {fb}\n\n"
        kb.append([{"text": f"{it['symbol']} {it['type']}", "callback_data": f"chart_{it['symbol']}"}])
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
        else: msg+=line
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

def broadcast_vol_spike(signals, threshold=2.0, sort_by_rp=False, dest_chat_id=None):
    target = dest_chat_id or TARGET_CHAT_ID
    if not target: return
    if not signals:
        send_reply(target, f"Vol Spike >{threshold}x: Tidak ada"); return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header=f"*VOL SPIKE >{threshold}x* 🔥 {now} | {len(signals)} saham\n{'='*30}\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        rp=format_large_number(it.get('vol_rp',0))
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Vol {it['vol_ratio']:.1f}x Rp {rp}\n\n"
        kb.append([{"text": f"{it['symbol']} {it['vol_ratio']:.1f}x", "callback_data": f"chart_{it['symbol']}"}])
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
        else: msg+=line
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

AUTO_ALERT_ENABLED=True
AUTO_ALERT_INTERVAL=60
LAST_AUTO_ALERT_TIME=0

def realtime_auto_alert_loop():
    global LAST_AUTO_ALERT_TIME, ALERTED_TODAY, ALERTED_DATE
    while True:
        try:
            if not AUTO_ALERT_ENABLED: time.sleep(10); continue
            now=get_now_wib()
            today=now.date()
            global ALERTED_DATE
            if ALERTED_DATE != today:
                ALERTED_TODAY=set()
                ALERTED_DATE=today
            if now.weekday()>=5: time.sleep(300); continue
            market_start=now.replace(hour=9, minute=0, second=0, microsecond=0)
            market_end=now.replace(hour=15, minute=30, second=0, microsecond=0)
            if not (market_start <= now <= market_end): time.sleep(300); continue
            if time.time() - LAST_AUTO_ALERT_TIME < AUTO_ALERT_INTERVAL: time.sleep(5); continue
            signals = scan_v413_final(top_arjum=60, top_itick=30, vol_thr=1.5, min_value=1_000_000_000, min_closepos=0.6, only_new=True)
            LAST_AUTO_ALERT_TIME=time.time()
            if signals:
                for s in signals: ALERTED_TODAY.add(s['symbol'])
                broadcast_v413(signals, vol_thr=1.5, dest_chat_id=TARGET_CHAT_ID, is_auto=True)
            time.sleep(AUTO_ALERT_INTERVAL)
        except Exception as e:
            print(f"REALTIME ALERT err {e}")
            time.sleep(30)

def telegram_bot_listener():
    offset=0
    print("🤖 RAFANO V4.13 CAPTION PRO")
    try: requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true",timeout=10)
    except: pass
    if AUTO_ALERT_ENABLED:
        threading.Thread(target=realtime_auto_alert_loop, daemon=True).start()
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
                    cb=update["callback_query"]; qid=cb.get("id"); cdata=cb.get("data",""); chat_id=cb["message"]["chat"]["id"]
                    try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",json={"callback_query_id":qid},timeout=5)
                    except: pass
                    if cdata.startswith("chart_"):
                        sym=cdata.split("_")[1]
                        threading.Thread(target=process_chart_request,args=(chat_id,sym,"1d")).start()
                elif "message" in update and "text" in update["message"]:
                    txt=update["message"].get("text","").strip()
                    chat_id=update["message"]["chat"]["id"]
                    from_user=update["message"]["from"].get("username","?")
                    print(f"💬 {from_user} ({chat_id}): {txt}")
                    first=txt.split()[0].lower() if txt else ""
                    parts=txt.split()
                    if first in ["/start","/help","/menu"]:
                        help_text="""🔥 *RAFANO V4.13 CAPTION PRO*
⚡ Realtime 60 detik: Arjum 60 -> ITICK 30 -> BO

📈 *CHART MULTITF + CAPTION PRO*
/c KODE = Daily
/c KODE 5 = 5 menit
/c KODE 15 = 15 menit
/c KODE 1h = 1 jam
/c KODE 4h = 4 jam
Contoh: /c BUMI 5 , /c BBCA 15
Caption: BUMI — Harga 206 (+7.29%)
    ├  Buy Strength 95% (VERY STRONG)
    ├  RSI 66.31
    ├  Vol Spike 2.1x | Buy Vol 89%
    └  Bandar 1W +1014B (ACCUM)

🏦 /b KODE - Bandar 1W

🚀 *SCAN REALTIME*
/scanbo [vol] - Arjum 60 -> ITICK 30 -> BO/BOB

🔥 /scanvol /vol [thr] [limit]
/volall [thr]

🤖 /autoalert on/off/status/reset
/quota
"""
                        send_reply(chat_id, help_text)
                    elif first in ["/quota"]:
                        send_reply(chat_id, f"QUOTA: {'HABIS' if QUOTA_HIT else 'OK'}\nAUTO: {'ON' if AUTO_ALERT_ENABLED else 'OFF'} {AUTO_ALERT_INTERVAL}s\nAlerted: {len(ALERTED_TODAY)}")
                    elif first in ["/autoalert","/auto"]:
                        if len(parts)>=2:
                            cmd=parts[1].lower()
                            if cmd=="on":
                                globals()['AUTO_ALERT_ENABLED']=True
                                send_reply(chat_id, f"⚡ REALTIME AUTO ON {AUTO_ALERT_INTERVAL}s")
                            elif cmd=="off":
                                globals()['AUTO_ALERT_ENABLED']=False
                                send_reply(chat_id, "AUTO OFF")
                            elif cmd=="reset":
                                ALERTED_TODAY.clear()
                                send_reply(chat_id, f"🔄 Reset anti-spam")
                            elif cmd=="status":
                                now=get_now_wib()
                                last=time.strftime('%H:%M:%S', time.localtime(LAST_AUTO_ALERT_TIME)) if LAST_AUTO_ALERT_TIME else "Belum"
                                send_reply(chat_id, f"AUTO: {'ON' if AUTO_ALERT_ENABLED else 'OFF'}\nInterval: {AUTO_ALERT_INTERVAL}s\nLast: {last}\nAlerted: {len(ALERTED_TODAY)} - {list(ALERTED_TODAY)[:10]}")
                        else:
                            send_reply(chat_id, f"AUTO: {'ON' if AUTO_ALERT_ENABLED else 'OFF'} {AUTO_ALERT_INTERVAL}s")
                    elif first in ["/c","/chart"]:
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            tf_input=parts[2] if len(parts)>=3 else "1d"
                            threading.Thread(target=process_chart_request,args=(chat_id,sym,tf_input)).start()
                        else:
                            send_reply(chat_id, "Pakai: /c BUMI 5 atau /c BBCA\n/c KODE [5/15/30/1h/4h/1d/1w]")
                    elif first in ["/b","/broker","/bandar"]:
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            threading.Thread(target=process_broker_request,args=(chat_id,sym)).start()
                        else:
                            send_reply(chat_id, "Pakai: /b BUMI")
                    elif first in ["/scanvol","/vol"]:
                        try: thr=float(parts[1]) if len(parts)>=2 else 2.0; lim=int(parts[2]) if len(parts)>=3 else 60
                        except: thr=2.0; lim=60
                        send_reply(chat_id, f"🔥 SCAN VOL >{thr}x ({lim})...")
                        def run_vol(tg=chat_id, th=thr, l=lim):
                            sigs=scan_volume_spike(threshold=th, limit_candidates=l, sort_by_rp=False)
                            broadcast_vol_spike(sigs, threshold=th, sort_by_rp=False, dest_chat_id=tg)
                        threading.Thread(target=run_vol).start()
                    elif first in ["/scanvolall","/volall"]:
                        try: thr=float(parts[1]) if len(parts)>=2 else 2.0
                        except: thr=2.0
                        send_reply(chat_id, f"🔥🔥 SCAN VOL ALL 300 >{thr}x...")
                        def run_volall(tg=chat_id, th=thr):
                            sigs=scan_volume_spike(threshold=th, limit_candidates=300, sort_by_rp=True)
                            broadcast_vol_spike(sigs, threshold=th, sort_by_rp=True, dest_chat_id=tg)
                        threading.Thread(target=run_volall).start()
                    elif first.startswith("/scanbo") or first.startswith("/topbo") or first.startswith("/scan"):
                        vol_thr=1.5; top_final=30; top_arjum=60; min_val=1_000_000_000
                        try:
                            if len(parts)>=2: vol_thr=float(parts[1])
                            if len(parts)>=3: min_val=float(parts[2])
                        except: pass
                        send_reply(chat_id, f"🚀 V4.13 SCAN Arjum {top_arjum} -> ITICK {top_final} VOL>{vol_thr}x...")
                        def run_scan(tg=chat_id, vt=vol_thr, tf=top_final, ta=top_arjum, mv=min_val):
                            sigs=scan_v413_final(top_arjum=ta, top_itick=tf, vol_thr=vt, min_value=mv, only_new=False)
                            broadcast_v413(sigs, vol_thr=vt, dest_chat_id=tg, is_auto=False)
                        threading.Thread(target=run_scan).start()
        except Exception as e:
            print(f"Listener err {e}"); import traceback; traceback.print_exc(); time.sleep(3)

if __name__=="__main__":
    print("==========================================")
    print("🔥 RAFANO V4.13 CAPTION PRO")
    print("BUMI — Harga 206 (+7.29%) Buy Strength RSI Vol Bandar 1W")
    print("Chart BMTR hitam pro tetap")
    print("==========================================")
    telegram_bot_listener()
