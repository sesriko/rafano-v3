"""
RAFANO V4.18 COLAB - Baca dari Colab Secrets
Secrets yang harus ada di Colab:
- TELEGRAM_BOT_TOKEN
- TARGET_CHAT_ID
- ITICK_TOKEN
- ARJUM_API_KEY
"""
import os, sys

# ===== COLAB SECRETS LOADER =====
try:
    from google.colab import userdata
    IS_COLAB = True
    print("✅ Running di Colab, load secrets dari userdata...")
    for key in ["TELEGRAM_BOT_TOKEN","TARGET_CHAT_ID","ITICK_TOKEN","ITICK_API_KEY","ARJUM_API_KEY","DB_PATH"]:
        try:
            val = userdata.get(key)
            if val:
                os.environ[key] = val
                print(f"  Loaded secret: {key}")
        except Exception as e:
            pass
    # Default DB di Drive biar gak hilang
    if not os.getenv("DB_PATH"):
        # coba cek drive
        if os.path.exists("/content/drive"):
            os.environ["DB_PATH"] = "/content/drive/MyDrive/rafano_vol.db"
            print(f"  DB_PATH -> {os.environ['DB_PATH']} (Drive)")
        else:
            os.environ["DB_PATH"] = "/content/rafano_vol.db"
            print(f"  DB_PATH -> {os.environ['DB_PATH']} (local)")
except:
    IS_COLAB = False
    print("Running local, pakai .env")

# ===== IMPORT SETELAH ENV DI-SET =====
import time, datetime, threading, requests, pytz, sqlite3
import numpy as np, pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.gridspec as gridspec
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

load_dotenv(override=True)

TIMEZONE_WIB=pytz.timezone('Asia/Jakarta')
TELEGRAM_BOT_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN") or ""
TARGET_CHAT_ID=os.getenv("TARGET_CHAT_ID") or ""
ARJUM_API_KEY=os.getenv("ARJUM_API_KEY") or ""
ARJUM_BASE="https://stock.arjum.com/api"
ITICK_TOKEN=os.getenv("ITICK_TOKEN") or os.getenv("ITICK_API_KEY") or ""
ITICK_BASE="https://api.itick.org"
ITICK_ENABLED=bool(ITICK_TOKEN)
DB_PATH = os.getenv("DB_PATH") or "/content/rafano_vol.db"

SESSION = requests.Session()
ARJUM_LOCK = threading.Lock()

def get_now_wib(): return datetime.datetime.now(TIMEZONE_WIB)

IDX_600_LIQUID=["BBCA","BBRI","BMRI","BBNI","TLKM","ASII","BMTR","BIPI","GOTO","BUKA","BBKP","BRIS","ANTM","INCO","MDKA","ADRO","PTBA","PGAS","EXCL","ISAT","BREN","CUAN","WIFI","DEWA","BULL","NCKL","AMRT","TOWR","TBIG","ELSA","BEKS","BNGA","AALI","ACES","ADMR","AKRA","AMMN","BRMS","BRPT","BSDE","CPIN","CTRA","EMTK","ICBP","INDF","INKP","INTP","ITMG","JPFA","KLBF","MEDC","SMGR","SMRA","TPIA","UNTR","UNVR"]

HISTORY_CACHE={}; SCREENER_CACHE={}
HISTORY_CACHE_TTL=600; SCREENER_CACHE_TTL=300
QUOTA_HIT=False; LAST_429_TIME=0; LAST_ARJUM_REQUEST=0
ARJUM_MIN_INTERVAL=1.2

def get_cached(k, cache, ttl):
    if k in cache:
        ts,d=cache[k]
        if time.time()-ts<ttl: return d
    return None
def set_cached(k,d,cache):
    cache[k]=(time.time(),d)

def arjum_get(path, params=None, bypass_quota=False, retries=1):
    global QUOTA_HIT, LAST_429_TIME, LAST_ARJUM_REQUEST
    with ARJUM_LOCK:
        elapsed = time.time() - LAST_ARJUM_REQUEST
        if elapsed < ARJUM_MIN_INTERVAL:
            time.sleep(ARJUM_MIN_INTERVAL - elapsed)
        LAST_ARJUM_REQUEST = time.time()
    if QUOTA_HIT and not bypass_quota and time.time()-LAST_429_TIME<180:
        return None
    url=f"{ARJUM_BASE}{path}"
    for attempt in range(retries+1):
        try:
            headers={"X-API-Key": ARJUM_API_KEY.strip(),"Accept":"application/json","User-Agent":"Mozilla/5.0"}
            r=SESSION.get(url,headers=headers,params=params,timeout=15)
            if r.status_code==200:
                if QUOTA_HIT and time.time()-LAST_429_TIME>60: QUOTA_HIT=False
                return r.json()
            elif r.status_code==429:
                if not bypass_quota:
                    QUOTA_HIT=True; LAST_429_TIME=time.time()
                if attempt < retries:
                    time.sleep(5 + attempt*2); continue
                return None
            else:
                return None
        except:
            if attempt < retries:
                time.sleep(2); continue
            return None
    return None

# ===== DB YF =====
def init_db():
    os.makedirs(os.path.dirname(DB_PATH) if os.path.dirname(DB_PATH) else ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS yf_baseline (symbol TEXT PRIMARY KEY, avg_vol_20 REAL, last_close REAL, updated_at TEXT)")
    conn.commit(); conn.close()
    print(f"✅ DB ready: {DB_PATH}")

def update_yf_baseline(symbols=IDX_600_LIQUID, batch_size=10):
    print(f"🔄 Update Baseline YF {len(symbols)} saham ke {DB_PATH}...")
    import yfinance as yf
    conn = sqlite3.connect(DB_PATH)
    def fetch_one(sym):
        try:
            ticker = yf.Ticker(f"{sym}.JK")
            hist = ticker.history(period="2mo", timeout=10, auto_adjust=False)
            if hist is None or len(hist) < 25: return None
            avg20 = hist['Volume'].tail(20).mean()
            last_close = hist['Close'].iloc[-1]
            return (sym, float(avg20), float(last_close), datetime.datetime.now().isoformat())
        except Exception as e:
            print(f"YF err {sym}: {e}"); return None
    with ThreadPoolExecutor(max_workers=5) as ex:
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            results = list(ex.map(fetch_one, batch))
            for r in results:
                if r:
                    conn.execute("REPLACE INTO yf_baseline VALUES (?,?,?,?)", r)
                    print(f"✅ {r[0]} Avg20: {r[1]:,.0f}")
            conn.commit(); time.sleep(1)
    conn.close(); print("✅ Baseline YF selesai")

def get_yf_baseline_dict():
    if not os.path.exists(DB_PATH): return {}
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("SELECT symbol, avg_vol_20, last_close FROM yf_baseline")
        rows = cur.fetchall()
    except: rows=[]
    conn.close()
    return {r[0]: {"avg20": r[1], "close": r[2]} for r in rows}

def get_screener_latest():
    c=get_cached('latest', SCREENER_CACHE, SCREENER_CACHE_TTL)
    if c: return c
    data=arjum_get("/screener/latest", bypass_quota=True, retries=1)
    if data and isinstance(data, dict) and 'rows' in data and len(data['rows'])>0:
        set_cached('latest', data, SCREENER_CACHE); return data
    return {"rows": [{"stock_code": c, "close": 100} for c in IDX_600_LIQUID[:150]]}

def get_itick_quotes_batch(symbols, max_batch=20):
    if not ITICK_ENABLED or not symbols: return {}
    all_quotes={}
    try:
        for i in range(0, len(symbols), max_batch):
            batch=symbols[i:i+max_batch]
            codes=",".join(batch)
            url = f"{ITICK_BASE}/stock/quotes?region=ID&codes={codes}"
            headers = {"accept": "application/json", "token": ITICK_TOKEN}
            r = SESSION.get(url, headers=headers, timeout=8)
            if r.status_code==200:
                j=r.json()
                if j.get('code')==0 and j.get('data'):
                    for item in j['data']:
                        code=item.get('s') or item.get('code')
                        if not code: continue
                        code=str(code).upper().replace(".JK","")
                        all_quotes[code]={
                            'price': float(item.get('ld') or item.get('c') or 0),
                            'changepct': float(item.get('chp') or 0),
                            'volume': float(item.get('v') or 0),
                        }
            time.sleep(0.3)
    except Exception as e:
        print(f"ITICK err {e}")
    return all_quotes

def normalize_timeframe(tf_input):
    if not tf_input: return "1d"
    tf = str(tf_input).lower().strip()
    mapping={"5":"5m","5m":"5m","15":"15m","15m":"15m","30":"30m","30m":"30m","1h":"1h","60":"1h","60m":"1h","4h":"4h","1d":"1d","d":"1d","daily":"1d","1":"1d","1w":"1w"}
    return mapping.get(tf, tf)

def get_history_pro(sym, limit=150, frame="daily"):
    frame = normalize_timeframe(frame)
    hk=f"{sym}_{frame}_{limit}"
    cached=get_cached(hk, HISTORY_CACHE, HISTORY_CACHE_TTL)
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
        ticker=yf.Ticker(f"{sym}.JK")
        hist=ticker.history(period="6mo",interval="1d",timeout=10, auto_adjust=False)
        if hist is not None and len(hist)>20:
            set_cached(hk,hist.tail(limit),HISTORY_CACHE)
            return hist.tail(limit)
    except: pass
    return None

def format_large_number(val):
    if pd.isna(val) or val==0: return "0"
    a=abs(val)
    if a>=1_000_000_000: return f"{a/1_000_000_000:.2f}B"
    elif a>=1_000_000: return f"{a/1_000_000:,.0f}M"
    elif a>=1_000: return f"{a/1_000:,.0f}K"
    else: return f"{val:,.0f}"

def format_timeframe_label(tf):
    tf=normalize_timeframe(tf)
    m={"5m":"5 Menit","15m":"15 Menit","30m":"30 Menit","1h":"1 Jam","4h":"4 Jam","1d":"Daily","1w":"Weekly"}
    return m.get(tf, tf.upper())

def calculate_rsi(prices, period=14):
    try:
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss.replace(0, 0.001)
        rsi = 100 - (100 / (1 + rs))
        return float(rsi.iloc[-1]) if len(rsi)>0 and not pd.isna(rsi.iloc[-1]) else 50.0
    except: return 50.0

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

def generate_caption_pro(symbol, df, realtime_price=None, tf_norm="1d"):
    df=df.copy()
    if realtime_price and realtime_price>0 and len(df)>0:
        df.iloc[-1, df.columns.get_loc('Close')] = realtime_price
    last_close=df['Close'].iloc[-1]
    prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close
    chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
    rsi = calculate_rsi(df['Close'], 14)
    df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
    df_vsa, buy_ratios = calculate_vsa_metrics(df)
    buy_pct = int(buy_ratios.iloc[-1]*100) if hasattr(buy_ratios,'iloc') else int(buy_ratios[-1]*100)
    buy_pct = max(0, min(100, buy_pct))
    v1=df['V1'].iloc[-1]
    vol_spike = df['Volume'].iloc[-1] / v1 if v1>0 else 1.0
    tf_label = format_timeframe_label(tf_norm)
    caption = f"""{symbol} — Harga {int(last_close)} ({chg_pct:+.2f}%) | {tf_label}
├  Buy Strength Score: {buy_pct}% 
├  RSI (14): {rsi:.2f}
└  Vol Spike: {vol_spike:.1f}x V1 | Buy Vol: {buy_pct}%"""
    return caption, {}

def generate_pro_chart(df,symbol="BBCA",timeframe="5m",output_filename="chart.png",extra_info=None,realtime_price=None):
    try:
        extra_info=extra_info or {}
        timeframe_norm = normalize_timeframe(timeframe)
        tf_label_disp=extra_info.get('tf_label') or format_timeframe_label(timeframe_norm)
        df=df.copy().ffill().bfill()
        if not isinstance(df.index,pd.DatetimeIndex): df.index=pd.to_datetime(df.index)
        else: df=df.sort_index()
        if realtime_price and realtime_price>0 and len(df)>0:
            df.iloc[-1, df.columns.get_loc('Close')] = realtime_price
        df['EMA13']=df['Close'].ewm(span=13,adjust=False).mean()
        df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
        df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
        df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        sma20, upper_bb, lower_bb = calculate_bollinger_bands(df,20,2)
        df['BB_UP']=upper_bb; df['BB_LOW']=lower_bb
        df,buy_ratios=calculate_vsa_metrics(df)
        last_close=df['Close'].iloc[-1]; last_high=df['High'].iloc[-1]; last_low=df['Low'].iloc[-1]; last_vol=df['Volume'].iloc[-1]
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
        buy_pct=buy_pct_temp; net_vol=df['Net_Vol_VSA'].iloc[-1]; net_vol_5d=df['Net_Vol_VSA'].tail(5).sum()
        plt.style.use('dark_background')
        fig=plt.figure(figsize=(18,10),dpi=180,facecolor='#000000')
        gs=gridspec.GridSpec(4,1,height_ratios=[4.5,1.1,0.9,0.8],hspace=0.05)
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
        ax_main.plot(x,df['EMA13'],color='#ffff00',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA20'],color='#ff0000',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA50'],color='#ffffff',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA200'],color='#a020f0',linewidth=1.3,alpha=0.9)
        ax_main.plot(x,df['BB_UP'],color='#444488',linewidth=0.8,linestyle='--',alpha=0.6)
        ax_main.plot(x,df['BB_LOW'],color='#444488',linewidth=0.8,linestyle='--',alpha=0.6)
        ax_main.set_xlim(-1,len(df)-1+15); ax_main.set_ylim(df['Low'].min()*0.97, df['High'].max()*1.05)
        left_text=f"Avg Price : {avg_price:.1f}\nVchg 1 Bar: {vchg1:.1f} x\nVchg 5 Bar: {vchg5:.1f} x\nSpeed : {speed}\nPower : {power}\nSafety : {safety}\n\nEMA 13 : {ema13:.1f}\nEMA 20 : {ema20:.1f}\nEMA 50 : {ema50:.1f}\nEMA 200: {ema200:.1f}"
        ax_main.text(0.005,0.98,left_text,transform=ax_main.transAxes,va='top',ha='left',fontsize=7,family='monospace',color='#e0e0e0',bbox=dict(facecolor='black',alpha=0.7,edgecolor='#333333'))
        fig.text(0.005,0.96,f"{symbol} : {last_close:.0f} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=14,fontweight='bold',ha='left',va='center', family='monospace')
        fig.text(0.5,0.96,"RAFANO V4.18 COLAB",color='white',fontsize=16,fontweight='bold',ha='center',va='center')
        ds=df.index[-1].strftime('%d %b %Y %H:%M') if hasattr(df.index[-1],'strftime') else get_now_wib().strftime('%d %b %Y %H:%M')
        fig.text(0.99,0.96,f"{tf_label_disp} | {ds}",color='#ffcc00',fontsize=11,ha='right',va='center', fontweight='bold')
        vol_info=f"Buy % = {buy_pct}% Sell % = {100-buy_pct}% Net Vol = {net_vol:,.0f} 5D = {net_vol_5d:,.0f}"
        ax_vol.text(0.005,0.88,vol_info,transform=ax_vol.transAxes,color='#ffffff',fontsize=8,va='top')
        ax_vol.bar(x,df['Vol_Sell'],color='#cc0000',width=0.8,alpha=0.8)
        ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00cc00',width=0.8,alpha=0.9)
        ax_vol.plot(x,df['V1'],color='white',linewidth=0.8,alpha=0.9)
        ax_vol.set_ylim(0,df['Volume'].max()*2); plt.setp(ax_vol.get_xticklabels(),visible=False)
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
        ax_mm.set_ylim(-30,30); step=max(1,len(df)//10); ax_mm.set_xticks(x[::step])
        labels=[df.index[i].strftime('%d/%b') if hasattr(df.index[i],'strftime') else str(i) for i in range(0,len(df),step)]
        ax_mm.set_xticklabels(labels,fontsize=7)
        plt.savefig(output_filename,dpi=180,bbox_inches='tight',facecolor='#000000')
        plt.close('all')
        return output_filename, []
    except Exception as e:
        print(f"Chart error {e}"); import traceback; traceback.print_exc(); return None, []
    finally:
        try: plt.clf(); plt.close('all')
        except: pass

def scan_vol_2x_itick_vs_yf(threshold=2.0, min_avg_vol=500000):
    baseline = get_yf_baseline_dict()
    if not baseline: return [], "DB kosong, jalankan /updatedb dulu"
    symbols = [s for s in baseline.keys() if baseline[s]['avg20'] >= min_avg_vol]
    quotes = get_itick_quotes_batch(symbols, max_batch=20)
    detected = []
    for sym, q in quotes.items():
        if sym not in baseline: continue
        avg20 = baseline[sym]['avg20']
        vol_rt = q.get('volume',0)
        if avg20==0 or vol_rt==0: continue
        ratio = vol_rt / avg20
        if ratio >= threshold:
            detected.append({"symbol": sym, "price": q['price'], "change_pct": q.get('changepct',0), "vol_rt": vol_rt, "avg20": avg20, "ratio": ratio})
    detected.sort(key=lambda x: x['ratio'], reverse=True)
    return detected, None

def send_reply(cid, txt, rm=None):
    if not TELEGRAM_BOT_TOKEN or not cid: return False
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    pl={"chat_id":cid,"text":txt,"parse_mode":"Markdown"}
    if rm: pl["reply_markup"]=rm
    try:
        r=SESSION.post(url,json=pl,timeout=15)
        j=r.json()
        if not j.get('ok'):
            pl.pop('parse_mode',None)
            r=SESSION.post(url,json=pl,timeout=15)
            return r.json().get('ok',False)
        return True
    except: return False

def send_photo_reply(cid, path, caption=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(path,'rb') as ph:
            SESSION.post(url,data={'chat_id':cid,'caption':caption,'parse_mode':'Markdown'},files={'photo':ph},timeout=30)
    except Exception as e: print(f"Photo err {e}")

def process_chart_request(cid, code, tf_input="1d"):
    tf_norm = normalize_timeframe(tf_input)
    tf_label = format_timeframe_label(tf_norm)
    send_reply(cid, f"📊 *{code.upper()} ({tf_label}) chart pro...*")
    df=get_history_pro(code, 150, frame=tf_norm)
    if df is None or len(df)<20:
        send_reply(cid, f"⚠ Data {code} TF {tf_norm} tidak ada"); return
    quotes=get_itick_quotes_batch([code],1)
    realtime=quotes.get(code.upper(),{}).get('price',0)
    chart_file=f"/tmp/chart_{code.upper()}_{tf_norm}_{int(time.time())}.png"
    fp, _ = generate_pro_chart(df, symbol=code.upper(), timeframe=tf_norm, output_filename=chart_file, extra_info={'tf_label':tf_label}, realtime_price=realtime)
    if not fp or not os.path.exists(fp):
        send_reply(cid, "❌ Gagal render chart"); return
    caption_pro, _ = generate_caption_pro(code.upper(), df, realtime_price=realtime, tf_norm=tf_norm)
    send_photo_reply(cid, fp, caption=caption_pro)
    if os.path.exists(fp):
        try: os.remove(fp)
        except: pass

def broadcast_vol2x(signals, threshold=2.0, dest_chat_id=None):
    target = dest_chat_id or TARGET_CHAT_ID
    if not target: return
    if not signals:
        send_reply(target, f"Vol 2x: Tidak ada lonjakan >{threshold}x"); return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header=f"*VOL 2x SCANNER* 🔥 {now} | {len(signals)} saham | ITICK vs YF Avg20\n{'='*30}\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals[:30],1):
        rp=format_large_number(it.get('vol_rt',0))
        avg=format_large_number(it.get('avg20',0))
        line=f"{idx}. *{it['symbol']}* {it['price']:.0f} ({it['change_pct']:+.1f}%) Vol {rp} / Avg {avg} = *{it['ratio']:.1f}x*\n\n"
        kb.append([{"text": f"{it['symbol']} {it['ratio']:.1f}x", "callback_data": f"chart_{it['symbol']}"}])
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
        else: msg+=line
    if msg: send_reply(target, msg, rm={"inline_keyboard": kb})

def telegram_bot_listener():
    offset=0
    print("🤖 RAFANO V4.18 COLAB - Secrets OK")
    try: SESSION.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true",timeout=10)
    except: pass
    init_db()
    while True:
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res=SESSION.get(url,timeout=25)
            if res.status_code!=200: time.sleep(3); continue
            data=res.json()
            if not data.get('ok'): time.sleep(3); continue
            for update in data.get("result",[]):
                offset=update["update_id"]+1
                if "callback_query" in update:
                    cb=update["callback_query"]; qid=cb.get("id"); cdata=cb.get("data",""); chat_id=cb["message"]["chat"]["id"]
                    try: SESSION.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",json={"callback_query_id":qid},timeout=5)
                    except: pass
                    if cdata.startswith("chart_"):
                        sym=cdata.split("_")[1]
                        threading.Thread(target=process_chart_request,args=(chat_id,sym,"1d")).start()
                elif "message" in update and "text" in update["message"]:
                    txt=update["message"].get("text","").strip()
                    chat_id=update["message"]["chat"]["id"]
                    print(f"💬 {txt}")
                    first=txt.split()[0].lower() if txt else ""
                    parts=txt.split()
                    if first in ["/start","/help","/menu"]:
                        help_text="""🔥 *RAFANO V4.18 COLAB*
✅ Baca dari Colab Secrets

📈 /c KODE = chart (tanpa bandar, kenceng)
🔥 /vol2x 2 = scan vol ITICK vs YF Avg20
🗄️ /updatedb = update DB YF
/dbstatus = cek DB
"""
                        send_reply(chat_id, help_text)
                    elif first in ["/c","/chart"]:
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            tf_input=parts[2] if len(parts)>=3 else "1d"
                            threading.Thread(target=process_chart_request,args=(chat_id,sym,tf_input)).start()
                    elif first in ["/vol2x","/vol"]:
                        try: thr=float(parts[1]) if len(parts)>=2 else 2.0
                        except: thr=2.0
                        send_reply(chat_id, f"🔍 SCAN VOL 2x >{thr}x...")
                        def run_vol2x(tg=chat_id, th=thr):
                            sigs, err = scan_vol_2x_itick_vs_yf(threshold=th)
                            if err: send_reply(tg, f"⚠ {err}")
                            else: broadcast_vol2x(sigs, threshold=th, dest_chat_id=tg)
                        threading.Thread(target=run_vol2x).start()
                    elif first in ["/updatedb"]:
                        send_reply(chat_id, "🔄 Update DB YF...")
                        def run_upd(tg=chat_id):
                            update_yf_baseline()
                            send_reply(tg, "✅ DB YF selesai")
                        threading.Thread(target=run_upd).start()
                    elif first in ["/dbstatus"]:
                        base = get_yf_baseline_dict()
                        send_reply(chat_id, f"📊 DB: {len(base)} saham\nPath: {DB_PATH}")
        except Exception as e:
            print(f"Listener err {e}"); time.sleep(3)

if __name__=="__main__":
    print("==========================================")
    print("🔥 RAFANO V4.18 COLAB SECRETS")
    print(f"DB: {DB_PATH}")
    print(f"ITICK: {'OK' if ITICK_ENABLED else 'MISSING'}")
    print(f"TELEGRAM: {'OK' if TELEGRAM_BOT_TOKEN else 'MISSING'}")
    print("==========================================")
    init_db()
    telegram_bot_listener()
