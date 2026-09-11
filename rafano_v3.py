"""
RAFANO V4.8 ULTIMATE PRO - FINAL FIX
- Fix NameError format_timeframe_label
- Chart hitam pro + all commands + broadcast fix
"""
import os, time, datetime, threading, requests, pytz, json
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
ITICK_TOKEN=os.getenv("ITICK_TOKEN") or "7a470a83276242309fb940684046d35a88e450fdb95b46c383670e1e0c5e96f5"
ITICK_BASE="https://api.itick.org"

def get_now_wib(): return datetime.datetime.now(TIMEZONE_WIB)

FCA_EXCLUDE={"FUTR","FITT","HOTEL","ITIC","PUDP","COIN","SHID","RELI","ASPI","MEJA","MINA","ESTA","ASLI","VKTR","IMJS","GTSI","IRSX","ATAP","RONY","BCIC","DEFI","ROCK","YPAS","NIRO","BBHA","BKSW","NAGA","BEEF","BPTR","CBMF","CPRI","CRAB","DAAZ","DEAL","DGNS","DMND","DUCK","ELSA","ENRG","ENVY","ERAA","ESTI","ETWA","FIRE","FORU","GAMA","GOLL","HAIS","HATM","HITS","HOMI","IATA","INPS","IPOL","JGLE","KAYU","KBAG","KIOS","KPAL","KPAS","LCGP","LPLI","LPLR","MAGP","MAMI","MARI","SINI","SKYB","SMKM","SOCI","SONA","SOSS","SUGI","TALF","TDPM","TEBE","TOPS","TRAM","TRIL","TRIO","TRUS","UFOE","WIFI-W","WOWS","YELO","ZATA","ZONE","ZINC","TINS-W","BIPI-W","BULL-W","DEWA-W"}
IDX_600_LIQUID=["BBCA","BBRI","BMRI","BBNI","TLKM","ASII","BMTR","BIPI","GOTO","BUKA","BBKP","BRIS","ANTM","INCO","MDKA","ADRO","PTBA","PGAS","EXCL","ISAT","BREN","CUAN","WIFI","DEWA","BULL","NCKL","AMRT","TOWR","TBIG","ELSA","BEKS","BNGA","AALI","ACES","ADMR","AKRA","AMMN","BRMS","BRPT","BSDE","CPIN","CTRA","EMTK","ICBP","INDF","INKP","INTP","ITMG","JPFA","KLBF","MEDC","SMGR","SMRA","TPIA","UNTR","UNVR"]

HISTORY_CACHE={}; SCREENER_CACHE={}; BROKER_CACHE={}
HISTORY_CACHE_TTL=300; SCREENER_CACHE_TTL=120; BROKER_CACHE_TTL=300
QUOTA_HIT=False; LAST_429_TIME=0

def get_cached(k, cache, ttl):
    import time
    if k in cache:
        ts,d=cache[k]
        if time.time()-ts<ttl: return d
    return None
def set_cached(k,d,cache):
    import time; cache[k]=(time.time(),d)

def arjum_get(path, params=None):
    global QUOTA_HIT, LAST_429_TIME
    import time as _time
    if QUOTA_HIT and _time.time()-LAST_429_TIME<300: return None
    url=f"{ARJUM_BASE}{path}"
    try:
        headers={"X-API-Key": ARJUM_API_KEY.strip(),"Accept":"application/json"}
        r=requests.get(url,headers=headers,params=params,timeout=12)
        if r.status_code==200: return r.json()
        elif r.status_code==429: QUOTA_HIT=True; LAST_429_TIME=_time.time(); print("🚨 429 QUOTA"); return None
        elif r.status_code==401: print("🚨 401 KEY SALAH"); return None
    except: pass
    return None

def get_screener_latest(force_today=False):
    if not force_today:
        c=get_cached('latest', SCREENER_CACHE, SCREENER_CACHE_TTL)
        if c and isinstance(c, dict) and 'rows' in c: return c
    data=arjum_get("/screener/latest")
    if data and isinstance(data, dict) and 'rows' in data:
        set_cached('latest', data, SCREENER_CACHE); return data
    print("⚠ Arjum kosong, fallback IDX")
    return {"rows": [{"stock_code": c, "close": 100, "change_pct": 0} for c in IDX_600_LIQUID[:150]]}

def get_history_pro(sym, limit=120):
    hk=f"{sym}_1d_{limit}"
    cached=get_cached(hk, HISTORY_CACHE, HISTORY_CACHE_TTL)
    if cached is not None: return cached
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
        hist=yf.Ticker(f"{sym}.JK").history(period="1y",interval="1d",timeout=10)
        if hist is not None and len(hist)>10:
            set_cached(hk,hist.tail(limit),HISTORY_CACHE); return hist.tail(limit)
    except: pass
    return None

def get_bandar_info(sym):
    return {"foreign_net":0,"is_foreign_buy":False,"akum_ratio":0,"is_akum":False,"foreign_str":"N/A","bandar_str":"N/A","score_bonus":0}

def format_large_number(val,show_sign=False):
    if pd.isna(val) or val==0: return "0"
    a=abs(val); s="+" if (show_sign and val>0) else ("-" if val<0 else "")
    if a>=1_000_000_000: return f"{s}{a/1_000_000_000:.2f}B"
    elif a>=1_000_000: return f"{s}{a/1_000_000:,.0f}M"
    elif a>=1_000: return f"{s}{a/1_000:,.0f}K"
    else: return f"{s}{val:,.0f}"

def format_timeframe_label(tf):
    m={"1m":"1 Menit","5m":"5 Menit","15m":"15 Menit","30m":"30 Menit","1h":"1 Jam","4h":"4 Jam","1d":"Daily","1w":"Weekly","1mo":"Monthly","1M":"Monthly"}
    return m.get((tf or "1d").lower().strip(),(tf or "1d").upper())

def safe_int(v,d=0):
    try:
        if pd.isna(v): return d
        return int(v)
    except: return d

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

# ===== CHART PRO ASLI V4.3.3 FIXED =====
def generate_pro_chart(df,symbol="BBCA",timeframe="1d",sector_info="IHSG",output_filename="chart.png",extra_info=None):
    try:
        extra_info=extra_info or {}
        tf_label_disp=extra_info.get('tf_label') or format_timeframe_label(timeframe)
        df=df.copy().ffill().bfill()
        if not isinstance(df.index,pd.DatetimeIndex): df.index=pd.to_datetime(df.index)
        else: df=df.sort_index()
        df['EMA13']=df['Close'].ewm(span=13,adjust=False).mean()
        df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
        df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
        df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        df['V2']=df['Volume'].rolling(50,min_periods=1).mean()
        df,buy_ratios=calculate_vsa_metrics(df)
        last_close=df['Close'].iloc[-1]; last_open=df['Open'].iloc[-1]; last_high=df['High'].iloc[-1]; last_low=df['Low'].iloc[-1]; last_vol=df['Volume'].iloc[-1]
        prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close
        chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
        avg_price=df['Close'].tail(20).mean()
        vchg1=(last_vol/df['Volume'].iloc[-2]) if len(df)>1 and df['Volume'].iloc[-2]>0 else 1
        avg5=df['Volume'].tail(5).mean()
        vchg5=(last_vol/avg5) if avg5>0 else 1
        speed="FAST" if vchg1>2.0 else "SLOW" if vchg1<0.8 else "NORMAL"
        buy_pct_temp=int(buy_ratios.iloc[-1]*100) if hasattr(buy_ratios,'iloc') else int(buy_ratios[-1]*100)
        power="TURBO" if buy_pct_temp>=85 and vchg1>=1.2 else "STRONG" if buy_pct_temp>=70 or vchg1>=1.5 else "NORMAL" if buy_pct_temp>=60 else "WEAK"
        safety="GOOD" if last_close>df['EMA200'].iloc[-1] else "BAD"
        ema13=df['EMA13'].iloc[-1]; ema20=df['EMA20'].iloc[-1]; ema50=df['EMA50'].iloc[-1]; ema200=df['EMA200'].iloc[-1]
        buy_pct=buy_pct_temp; sell_pct=100-buy_pct
        net_vol=df['Net_Vol_VSA'].iloc[-1]; net_vol_5d=df['Net_Vol_VSA'].tail(5).sum()

        plt.style.use('dark_background')
        fig=plt.figure(figsize=(16,9),dpi=150,facecolor='#000000')
        gs=gridspec.GridSpec(4,1,height_ratios=[4.5,1.1,0.9,0.8],hspace=0.05)
        ax_main=fig.add_subplot(gs[0]); ax_vol=fig.add_subplot(gs[1],sharex=ax_main); ax_nbsa=fig.add_subplot(gs[2],sharex=ax_main); ax_mm=fig.add_subplot(gs[3],sharex=ax_main)
        fig.subplots_adjust(left=0.08,right=0.92,top=0.88,bottom=0.06)
        for ax in [ax_main,ax_vol,ax_nbsa,ax_mm]:
            ax.set_facecolor('#000000')
            ax.tick_params(colors='#aaaaaa',labelsize=8)
            ax.yaxis.tick_right()
            ax.grid(False)

        x=np.arange(len(df))
        for i in range(len(df)):
            o,h,l,c=df['Open'].iloc[i],df['High'].iloc[i],df['Low'].iloc[i],df['Close'].iloc[i]
            ax_main.plot([i,i],[l,h],color='#00ff00' if c>=o else '#ff0000',linewidth=0.8,alpha=0.8)
            body_low=min(o,c); body_h=max(0.5,abs(c-o))
            if c>=o:
                rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor='none',edgecolor='#00ff00',linewidth=0.8)
            else:
                rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor='#ff3333',edgecolor='#ff3333',linewidth=0.8)
            ax_main.add_patch(rect)

        ax_main.plot(x,df['EMA13'],color='#ffff00',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA20'],color='#ff0000',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA50'],color='#ffffff',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA200'],color='#a020f0',linewidth=1.2,alpha=0.9)
        ax_main.set_xlim(-1,len(df)-1+10)
        ax_main.set_ylim(df['Low'].min()*0.95,df['High'].max()*1.08)

        left_text=f"Avg Price : {avg_price:,.1f}\nVchg 1 Bar: {vchg1:.1f} x\nVchg 5 Bar: {vchg5:.1f} x\nSpeed : {speed}\nPower : {power}\nSafety : {safety}\n\nEMA 13 : {ema13:,.1f}\nEMA 20 : {ema20:,.1f}\nEMA 50 : {ema50:,.1f}\nEMA 200: {ema200:,.1f}"
        ax_main.text(0.01,0.98,left_text,transform=ax_main.transAxes,va='top',ha='left',fontsize=8,family='monospace',color='#e0e0e0',bbox=dict(facecolor='black',alpha=0.6,edgecolor='none'))

        fig.text(0.01,0.96,f"{symbol} : {last_close:.0f} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=13,fontweight='bold',ha='left',va='center')
        fig.text(0.01,0.93,f"{sector_info}",color='#ffaa00',fontsize=8,ha='left')
        fig.text(0.5,0.96,"RAFANO TRADER",color='white',fontsize=14,fontweight='bold',ha='center',va='center')
        ds=df.index[-1].strftime('%d %b %Y') if hasattr(df.index[-1],'strftime') else get_now_wib().strftime('%d %b %Y')
        fig.text(0.99,0.96,f"{tf_label_disp} | {ds}",color='#ffcc00',fontsize=10,ha='right',va='center')
        fig.text(0.99,0.93,f"Command BOT /C {symbol}",color='white',fontsize=8,ha='right')
        fig.text(0.01,0.885,f"High:{last_high:.0f} Low:{last_low:.0f} Open:{last_open:.0f} Vol:{last_vol:,.0f}",color='#00ffff',fontsize=8,ha='left')

        vol_info=f"Buy % = {buy_pct}% Sell % = {sell_pct}% Net Vol = {net_vol:,.0f} 5D = {net_vol_5d:,.0f}"
        ax_vol.text(0.005,0.88,vol_info,transform=ax_vol.transAxes,color='#ffffff',fontsize=8,va='top')
        ax_vol.bar(x,df['Vol_Sell'],color='#cc0000',width=0.8,alpha=0.8)
        ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00cc00',width=0.8,alpha=0.9)
        ax_vol.plot(x,df['V1'],color='white',linewidth=0.8,alpha=0.9)
        ax_vol.set_ylim(0,df['Volume'].max()*1.8)
        plt.setp(ax_vol.get_xticklabels(),visible=False)

        nbsa_label=f"NBSA Rp. {abs(net_vol*last_close)/1e9:.2f} B"
        ax_nbsa.text(0.005,0.85,nbsa_label,transform=ax_nbsa.transAxes,color='#ffffff',fontsize=8,va='top')
        nbsa_vals=df['Net_Vol_VSA'].tail(80)/(df['Net_Vol_VSA'].abs().max() or 1)*50
        xn=np.arange(len(df)-len(nbsa_vals),len(df))
        for i,v in zip(xn,nbsa_vals):
            ax_nbsa.bar(i,v,color='#00ffff' if v>=0 else '#ff4444',width=0.6)
        ax_nbsa.axhline(0,color='#444444',linewidth=0.5)
        ax_nbsa.set_ylim(-60,60)

        ax_mm.text(0.005,0.85,"Market Maker",transform=ax_mm.transAxes,color='#ffffff',fontsize=8,va='top')
        if 'MM' not in df.columns: df['MM']=(df['Close']-df['EMA50'])/df['EMA50']*1000
        mm_vals=df['MM'].tail(80)
        xm=np.arange(len(df)-len(mm_vals),len(df))
        ax_mm.bar(xm,mm_vals,color='#cccccc',width=0.5,alpha=0.8)
        step=max(1,len(df)//8)
        ax_mm.set_xticks(x[::step])
        ax_mm.set_xticklabels([df.index[i].strftime('%b') if hasattr(df.index[i],'strftime') else str(i) for i in range(0,len(df),step)],fontsize=7)

        plt.savefig(output_filename,dpi=150,bbox_inches='tight',facecolor='#000000')
        plt.close('all')
        return output_filename
    except Exception as e:
        print(f"Chart error {e}")
        import traceback; traceback.print_exc()
        return None
    finally:
        try: plt.clf(); plt.close('all')
        except: pass

def _check_strict_bo_today(sym, hd, today_date):
    try:
        if hd is None or len(hd) < 55: return None
        c = float(hd['Close'].iloc[-1]); pc = float(hd['Close'].iloc[-2])
        if c < 50: return None
        ema50_s = hd['Close'].ewm(span=50, adjust=False).mean()
        ema200_s = hd['Close'].ewm(span=200, adjust=False).mean()
        ema50 = float(ema50_s.iloc[-1]); ema200 = float(ema200_s.iloc[-1])
        pe50 = float(ema50_s.iloc[-2]); pe200 = float(ema200_s.iloc[-2])
        is_bo=False; stype=""
        if pc <= pe50 and c > ema50 and c > pe50: is_bo=True; stype="BO EMA50"
        elif pc <= pe200 and c > ema200 and c > pe200: is_bo=True; stype="BOB EMA200"
        if not is_bo: return None
        vol_today = float(hd['Volume'].iloc[-1])
        avg_vol_20 = float(hd['Volume'].iloc[-21:-1].mean()) if len(hd)>=21 else float(hd['Volume'].iloc[:-1].mean())
        if avg_vol_20 <= 0: return None
        vol_ratio = vol_today / avg_vol_20
        if vol_ratio < 1.5: return None
        chg = (c/pc-1)*100 if pc>0 else 0
        return {"symbol":sym,"type":stype,"close":c,"change":chg,"ema50":ema50,"ema200":ema200,"vol_ratio":vol_ratio}
    except: return None

def scan_v48(top_gainer_arjum=60, top_itick=30, vol_thr=1.5, min_value=1_000_000_000):
    print(f"[{get_now_wib()}] 🚀 V4.8: ARJUM {top_gainer_arjum} VOL>{vol_thr}x")
    screener = get_screener_latest(force_today=True)
    rows = screener.get('rows', []) if isinstance(screener, dict) else []
    if not rows: return []
    cands=[]
    for r in rows:
        code = (r.get('stock_code') or "").replace(".JK","").upper()
        if not code or code in FCA_EXCLUDE or "-W" in code: continue
        if float(r.get('close') or 100) < 50: continue
        cands.append(code)
        if len(cands) >= 150: break
    today=get_now_wib().date()
    def check_one(sym):
        try:
            hd=get_history_pro(sym,120)
            bo=_check_strict_bo_today(sym,hd,today)
            if not bo: return None
            if hd['Volume'].iloc[-1]*hd['Close'].iloc[-1] < min_value: return None
            return bo
        except: return None
    with ThreadPoolExecutor(max_workers=15) as ex:
        futs={ex.submit(check_one,s):s for s in cands[:top_gainer_arjum]}
        res=[]
        for f in as_completed(futs):
            r=f.result()
            if r:
                res.append(r)
                print(f"🔥 {r['symbol']} {r['type']} Vol {r['vol_ratio']:.1f}x")
    res.sort(key=lambda x: x['vol_ratio'], reverse=True)
    return res

def scan_volume_spike(threshold=2.0, limit_candidates=60, sort_by_rp=False):
    print(f"[{get_now_wib()}] 🚀 VOL SPIKE >{threshold}x")
    sd=get_screener_latest(force_today=False)
    if isinstance(sd, dict) and 'rows' in sd:
        cands=[(r.get('stock_code') or "").replace(".JK","").upper() for r in sd['rows']]
    else:
        cands=IDX_600_LIQUID
    seen=set(); uniq=[]
    for c in cands:
        cu=c.upper().strip()
        if not cu or "-W" in cu or cu in FCA_EXCLUDE: continue
        if cu not in seen:
            seen.add(cu); uniq.append(cu)
    for c in IDX_600_LIQUID:
        if len(uniq)>=limit_candidates: break
        if "-W" in c or c in FCA_EXCLUDE or c in seen: continue
        uniq.append(c); seen.add(c)
    cands=uniq[:limit_candidates]
    detected=[]
    for sym in cands:
        try:
            hd=get_history_pro(sym,60)
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
        print(f"✅ Sent to {cid}")
        return True
    except: return False

def send_photo_reply(cid, path, caption=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(path,'rb') as ph:
            requests.post(url,data={'chat_id':cid,'caption':caption,'parse_mode':'Markdown'},files={'photo':ph},timeout=30)
    except Exception as e: print(f"Photo err {e}")

def process_chart_request(cid, code, tf="1d"):
    send_reply(cid, f"📊 *{code.upper()} ({tf})...*")
    df=get_history_pro(code,150)
    if df is None or len(df)<20:
        send_reply(cid, f"⚠ Data {code} tidak ada"); return
    chart_file=f"/tmp/chart_{code.upper()}_{int(time.time())}.png"
    fp=generate_pro_chart(df, symbol=code.upper(), timeframe=tf, sector_info=f"{code.upper()} | IHSG", output_filename=chart_file, extra_info={'tf_label':format_timeframe_label(tf)})
    if not fp or not os.path.exists(fp):
        send_reply(cid, "❌ Gagal render chart"); return
    cap=f"*{code.upper()}* {int(df['Close'].iloc[-1])} ({((df['Close'].iloc[-1]/df['Close'].iloc[-2]-1)*100):+.2f}%) | Vol {format_large_number(df['Volume'].iloc[-1])}"
    send_photo_reply(cid, fp, caption=cap)
    if os.path.exists(fp): os.remove(fp)

def broadcast_v48(signals, vol_thr=1.5, dest_chat_id=None):
    target = dest_chat_id or TARGET_CHAT_ID
    if not target: return
    if not signals:
        send_reply(target, f"📉 V4.8 VOL>{vol_thr}x - Tidak ada BO valid hari ini\nCoba turunin threshold: /scanbo 1"); return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header=f"*🚀 V4.8 TOP {len(signals)} BO* {now}\n{'='*30}\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        line=f"{idx}. *{it['symbol']}* {it['type']} S{int(it.get('vol_ratio',0)*10)} {it['close']:.0f} ({it.get('change',0):+.1f}%) Vol {it.get('vol_ratio',0):.1f}x\n\n"
        kb.append([{"text":f"{it['symbol']} {it['vol_ratio']:.1f}x", "callback_data":f"chart_{it['symbol']}"}])
        if len(msg)+len(line)>3500:
            send_reply(target,msg,rm={"inline_keyboard":kb}); msg=line; kb=[]
        else: msg+=line
    if msg: send_reply(target,msg,rm={"inline_keyboard":kb})

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
        kb.append([{"text":f"{it['symbol']} {it['vol_ratio']:.1f}x", "callback_data":f"chart_{it['symbol']}"}])
        if len(msg)+len(line)>3500:
            send_reply(target,msg,rm={"inline_keyboard":kb}); msg=line; kb=[]
        else: msg+=line
    if msg: send_reply(target,msg,rm={"inline_keyboard":kb})

def telegram_bot_listener():
    offset=0
    print("🤖 RAFANO V4.8 ULTIMATE PRO - All commands + chart pro fixed")
    print(f"🔑 BOT: {'ADA' if TELEGRAM_BOT_TOKEN else 'KOSONG'} | CHAT: {TARGET_CHAT_ID} | ARJUM: {'ADA' if ARJUM_API_KEY else 'KOSONG'}")
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
                    cb=update["callback_query"]; qid=cb.get("id"); cdata=cb.get("data",""); chat_id=cb["message"]["chat"]["id"]
                    try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",json={"callback_query_id":qid},timeout=5)
                    except: pass
                    if cdata.startswith("chart_"):
                        sym=cdata.split("_")[1]
                        threading.Thread(target=process_chart_request,args=(chat_id,sym,"1d")).start()

                elif "message" in update and "text" in update["message"]:
                    txt=update["message"].get("text","").strip()
                    chat_id=update["message"]["chat"]["id"]
                    user=update["message"]["from"].get("username","?")
                    print(f"💬 {user} ({chat_id}): {txt}")
                    first=txt.split()[0].lower() if txt else ""
                    parts=txt.split()

                    if first in ["/start","/help","/menu"]:
                        help_msg="""🔥 *RAFANO V4.8 ULTIMATE PRO* - Chart hitam pro fix
📈 *CHART & BROKER*
/c KODE - Chart (hitam pro)
/c KODE 5m - Chart 5 menit
/b KODE - Bandar

🚀 *SCAN*
/scanbo [vol] - BO + Bandar (final)
/topbo [vol] - Sama
/scan [vol] - BO

🔥 *VOLUME*
/scanvol /vol [thr] [limit]
/scanvolall /volall [thr]

⚡ /quota
"""
                        send_reply(chat_id, help_msg)

                    elif first in ["/c","/chart"]:
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            tf=parts[2] if len(parts)>=3 else "1d"
                            threading.Thread(target=process_chart_request,args=(chat_id,sym,tf)).start()
                        else:
                            send_reply(chat_id, "Pakai: /c BBCA atau /c BBCA 5m")

                    elif first in ["/b","/broker","/bandar"]:
                        send_reply(chat_id, f"Bandar {parts[1].upper() if len(parts)>=2 else ''} - coming soon")

                    elif first in ["/quota"]:
                        send_reply(chat_id, f"QUOTA: {'HABIS' if QUOTA_HIT else 'OK'}\nARJUM: {'ADA' if ARJUM_API_KEY else 'KOSONG'}")

                    elif first in ["/scanvol","/vol","/vspike","/volspike","/spike"]:
                        try: thr=float(parts[1]) if len(parts)>=2 else 2.0; lim=int(parts[2]) if len(parts)>=3 else 60
                        except: thr=2.0; lim=60
                        send_reply(chat_id, f"🔥 SCAN VOL >{thr}x ({lim}) ke {chat_id}...")
                        def run_vol(tg=chat_id, th=thr, l=lim):
                            sigs=scan_volume_spike(threshold=th, limit_candidates=l)
                            broadcast_vol_spike(sigs, threshold=th, dest_chat_id=tg)
                        threading.Thread(target=run_vol).start()

                    elif first in ["/scanvolall","/volall","/vall","/scanallvol"]:
                        try: thr=float(parts[1]) if len(parts)>=2 else 2.0
                        except: thr=2.0
                        send_reply(chat_id, f"🔥🔥 SCAN VOL ALL 300 >{thr}x ke {chat_id}...")
                        def run_volall(tg=chat_id, th=thr):
                            sigs=scan_volume_spike(threshold=th, limit_candidates=300, sort_by_rp=True)
                            broadcast_vol_spike(sigs, threshold=th, sort_by_rp=True, dest_chat_id=tg)
                        threading.Thread(target=run_volall).start()

                    elif first.startswith("/scanbo") or first.startswith("/topbo") or first.startswith("/scan"):
                        vol_thr=1.5
                        try:
                            if len(parts)>=2: vol_thr=float(parts[1])
                        except: pass
                        if vol_thr>=10: vol_thr=1.5
                        send_reply(chat_id, f"🚀 SCAN BO VOL>{vol_thr}x ke {chat_id}...")
                        def run_scan(tg=chat_id, vt=vol_thr):
                            sigs=scan_v48(vol_thr=vt)
                            broadcast_v48(sigs, vol_thr=vt, dest_chat_id=tg)
                        threading.Thread(target=run_scan).start()

        except Exception as e:
            print(f"Listener err {e}")
            import traceback; traceback.print_exc()
            time.sleep(3)

if __name__=="__main__":
    print("==========================================")
    print("🔥 RAFANO V4.8 ULTIMATE PRO - FIXED")
    print("Chart hitam pro + sinyal BO + /c /b /scanvol")
    print("==========================================")
    telegram_bot_listener()
