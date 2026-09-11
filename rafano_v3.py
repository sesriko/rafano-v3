"""
RAFANO V4.8 ULTIMATE - V4.3.3 FULL + BANDAR SCORING V4.8
- Semua fungsi V4.3.3: /c, /chart, /b, /broker, /scan, /scanfast, /scanvol, /volall
- Ditambah V4.8: Bandar scoring (FB + AKUM) + fix broadcast ke requester
- Token tetap .env pertama
"""
import os, time, datetime, threading, requests, pytz, json, sys
import numpy as np, pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.gridspec as gridspec
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load .env pertama - tetap pakai yang pertama
for p in ['/content/rafano-v3/.env','./.env','.env','/content/.env']:
    if os.path.exists(p):
        load_dotenv(p, override=True)
        print(f"✅ Loaded .env from {p}")
        break
else:
    load_dotenv()
    print("⚠️ .env not found in common paths")

TIMEZONE_WIB=pytz.timezone('Asia/Jakarta')
TELEGRAM_BOT_TOKEN=os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN") or ""
TARGET_CHAT_ID=os.getenv("TARGET_CHAT_ID") or os.getenv("CHAT_ID") or ""
ARJUM_API_KEY=os.getenv("ARJUM_API_KEY") or ""
ARJUM_BASE="https://stock.arjum.com/api"
ITICK_TOKEN=os.getenv("ITICK_TOKEN") or os.getenv("ITICK_API_KEY") or "7a470a83276242309fb940684046d35a88e450fdb95b46c383670e1e0c5e96f5"
ITICK_BASE="https://api.itick.org"
ITICK_ENABLED=bool(ITICK_TOKEN)

print(f"🔑 BOT: {'ADA' if TELEGRAM_BOT_TOKEN else 'KOSONG'} {TELEGRAM_BOT_TOKEN[:12]+'...' if TELEGRAM_BOT_TOKEN else ''}")
print(f"💬 CHAT: {TARGET_CHAT_ID or 'KOSONG'}")
print(f"📊 ARJUM: {'ADA' if ARJUM_API_KEY else 'KOSONG'}")
print(f"⚡ ITICK: {'ON' if ITICK_ENABLED else 'OFF'}")

def get_now_wib(): return datetime.datetime.now(TIMEZONE_WIB)

def safe_get_env(k):
    v=os.getenv(k)
    if v: return str(v).strip().strip('"').strip("'")
    return None

FCA_EXCLUDE={"FUTR","FITT","HOTEL","ITIC","PUDP","COIN","SHID","RELI","ASPI","MEJA","MINA","ESTA","ASLI","VKTR","IMJS","GTSI","IRSX","ATAP","RONY","BCIC","DEFI","ROCK","YPAS","NIRO","BBHA","BKSW","NAGA","BEEF","BPTR","CBMF","CPRI","CRAB","DAAZ","DEAL","DGNS","DMND","DUCK","ELSA","ENRG","ENVY","ERAA","ESTI","ETWA","FIRE","FORU","GAMA","GOLL","HAIS","HATM","HITS","HOMI","IATA","INPS","IPOL","JGLE","KAYU","KBAG","KIOS","KPAL","KPAS","LCGP","LPLI","LPLR","MAGP","MAMI","MARI","SINI","SKYB","SMKM","SOCI","SONA","SOSS","SUGI","TALF","TDPM","TEBE","TOPS","TRAM","TRIL","TRIO","TRUS","UFOE","WIFI-W","WOWS","YELO","ZATA","ZONE","ZINC","TINS-W","BIPI-W","BULL-W","DEWA-W"}

IDX_600_LIQUID=["BBCA","BBRI","BMRI","BBNI","TLKM","ASII","BMTR","BIPI","GOTO","BUKA","BBKP","BRIS","ANTM","INCO","MDKA","ADRO","PTBA","PGAS","EXCL","ISAT","BREN","CUAN","WIFI","DEWA","BULL","NCKL","AMRT","TOWR","TBIG","ELSA","BEKS","BNGA","AALI","ACES","ADMR","AKRA","AMMN","BRMS","BRPT","BSDE","CPIN","CTRA","EMTK","ICBP","INDF","INKP","INTP","ITMG","JPFA","KLBF","MEDC","SMGR","SMRA","TPIA","UNTR","UNVR","ADHI","ARTO","ASRI","AUTO","BBHI","BIRD","BKSL","BUKK","BYAN","CMRY","DOID","ESSA","FILM","HRUM","ISAT","MAPA","MAPI","MBMA","MIKA","MNCN","PTMP","PTPP","PWON","RAJA","SCMA","SIDO","SRTG","TOWR","ACES","AKPI","AMRT","ARTO","BBNI","BIRD","BIPI","BKSL","BMRI","BREN","BSDE","BTPS","BUKA","BUMI","CMRY","CPIN","CUAN","ELSA","EMTK","GOTO","INCO","INDF","INTP","KLBF","MAPA","MAPI","MBMA","MDKA","MEDC","PGAS","PTBA","SMGR","SMRA","TLKM","TPIA","UNTR","WIFI","ADRO","AMMN","BRIS","BRMS","BSDE","CTRA","EMTK","ERAA","GOTO","ICBP","INCO","INDY","ITMG","MDKA","PGAS","PTBA","SMGR","TLKM","TPIA","UNVR","BBCA","BBRI","BMRI","BBNI","BREN","BUKA","NCKL","TOWR","DEWA","ELSA","BULL","BBKP"]

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
    if QUOTA_HIT and _time.time()-LAST_429_TIME<300:
        return None
    url=f"{ARJUM_BASE}{path}"
    try:
        headers={"X-API-Key": ARJUM_API_KEY.strip(),"Accept":"application/json","User-Agent":"Mozilla/5.0"}
        r=requests.get(url,headers=headers,params=params,timeout=12)
        if r.status_code==200: return r.json()
        elif r.status_code==429:
            QUOTA_HIT=True; LAST_429_TIME=_time.time(); print("🚨 429 QUOTA"); return None
        elif r.status_code==401:
            print("🚨 401 KEY SALAH"); return None
    except Exception as e:
        print(f"Arjum err {path}: {e}")
    return None

def get_screener_latest(force_today=False):
    if not force_today:
        c=get_cached('latest', SCREENER_CACHE, SCREENER_CACHE_TTL)
        if c and isinstance(c, dict) and 'rows' in c: return c
    data=arjum_get("/screener/latest")
    if data and isinstance(data, dict) and 'rows' in data:
        set_cached('latest', data, SCREENER_CACHE); return data
    # Fallback
    print("⚠️ Arjum kosong, fallback IDX")
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

# ===== BANDAR INFO V4.8 =====
def get_bandar_info(sym):
    cache_key=f"bandar_{sym}"
    cached=get_cached(cache_key, BROKER_CACHE, BROKER_CACHE_TTL)
    if cached is not None: return cached
    foreign_net=0; foreign_str="N/A"; is_fb=False; akum_ratio=0; bandar_str="N/A"; is_akum=False
    try:
        fdata=arjum_get(f"/foreign/{sym}", params={"limit": 5, "frame": "daily"})
        if fdata:
            rows=fdata.get('data') if isinstance(fdata, dict) else fdata
            if isinstance(rows, list) and len(rows)>0:
                last=rows[0] if isinstance(rows[0], dict) else {}
                for k in ['foreign_net','net_foreign','foreign_flow','net_buy','net']:
                    if k in last: foreign_net=float(last[k] or 0); break
            elif isinstance(rows, dict):
                foreign_net=float(rows.get('foreign_net') or 0)
        if foreign_net!=0:
            is_fb = foreign_net > 0
            if abs(foreign_net)>=1_000_000_000: foreign_str=f"{'FB' if is_fb else 'FS'} {foreign_net/1_000_000_000:+.1f}B"
            else: foreign_str=f"{'FB' if is_fb else 'FS'} {foreign_net/1_000_000:+.0f}M"
    except: pass
    result={"foreign_net": foreign_net,"is_foreign_buy": is_fb,"akum_ratio": akum_ratio,"is_akum": is_akum,"foreign_str": foreign_str,"bandar_str": bandar_str,"score_bonus": (15 if is_fb else 0) + (15 if is_akum else 0)}
    set_cached(cache_key, result, BROKER_CACHE)
    return result

def get_itick_quotes_batch(symbols, max_batch=15):
    if not ITICK_ENABLED or not symbols: return {}
    try:
        codes=",".join(symbols[:max_batch])
        url = f"{ITICK_BASE}/stock/quotes?region=ID&codes={codes}"
        headers = {"accept": "application/json", "token": ITICK_TOKEN}
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code==200:
            j=r.json()
            if j.get('code')==0 and j.get('data'):
                result={}
                for item in j['data']:
                    code=item.get('s') or item.get('code')
                    if not code: continue
                    code=str(code).upper().replace(".JK","")
                    result[code]={'price': float(item.get('ld') or item.get('c') or 0),'changepct': float(item.get('chp') or 0),'high': float(item.get('h') or 0),'low': float(item.get('l') or 0),'source': 'ITICK_REALTIME'}
                return result
    except: pass
    return {}

def format_large_number(val,show_sign=False):
    if pd.isna(val) or val==0: return "0"
    a=abs(val); s="+" if (show_sign and val>0) else ("-" if val<0 else "")
    if a>=1_000_000_000: return f"{s}{a/1_000_000_000:.2f}B"
    elif a>=1_000_000: return f"{s}{a/1_000_000:,.0f}M"
    elif a>=1_000: return f"{s}{a/1_000:,.0f}K"
    else: return f"{s}{val:,.0f}"

def safe_int(v,d=0):
    try:
        if pd.isna(v): return d
        return int(v)
    except: return d

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

def calculate_score_v48(item):
    score=0
    vol=item.get('vol_ratio',0)
    if vol>=3.0: score+=30
    elif vol>=2.5: score+=25
    elif vol>=2.0: score+=20
    elif vol>=1.5: score+=10
    c=item.get('close',0); ema=item.get('ema50') if item.get('type')=='BO EMA50' else item.get('ema200',0)
    if ema>0:
        dist=(c-ema)/ema*100
        if dist>=5: score+=20
        elif dist>=3: score+=15
        elif dist>=1: score+=10
        else: score+=5
    val=item.get('vol_rp',0)
    if val>=10_000_000_000: score+=20
    elif val>=5_000_000_000: score+=15
    elif val>=2_000_000_000: score+=10
    elif val>=1_000_000_000: score+=5
    cp=item.get('close_pos',0)
    if cp>=0.8: score+=15
    elif cp>=0.7: score+=10
    elif cp>=0.6: score+=5
    if item.get('type')=='BOB EMA200': score+=15
    else: score+=10
    base=min(score,100)
    bonus=item.get('bandar_bonus',0)
    total=min(base+bonus,100)
    return base, bonus, total


def _check_strict_bo_today(sym, hd, today_date, signal_type_filter="BO_BOB"):
    try:
        if hd is None or len(hd) < 55: return None
        try:
            last_dt = pd.to_datetime(hd.index[-1])
            last_candle_date = last_dt.date()
        except:
            return None
        delta = (today_date - last_candle_date).days
        if delta != 0: return None
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
        if avg_vol_20 <= 0 or vol_today <= 0: return None
        vol_ratio = vol_today / avg_vol_20
        if vol_ratio < 1.5: return None
        chg = (c/pc-1)*100 if pc>0 else 0
        return {"symbol":sym,"type":stype,"close":c,"change":chg,"ema50":ema50,"ema200":ema200,"last_date":str(last_candle_date),"vol_today":vol_today,"avg_vol_20":avg_vol_20,"vol_ratio":vol_ratio,"source":"STRICT_TODAY_VOL","score":vol_ratio*10}
    except:
        return None

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


def scan_v48(top_gainer_arjum=60, top_itick=30, vol_thr=1.5, min_value=1_000_000_000, min_closepos=0.6):
    print(f"[{get_now_wib()}] 🚀 V4.8: ARJUM {top_gainer_arjum}->ITICK {top_itick} VOL>{vol_thr}x")
    screener = get_screener_latest(force_today=True)
    rows = screener.get('rows', []) if isinstance(screener, dict) else []
    if not rows: return []
    candidates=[]
    for r in rows:
        code = (r.get('stock_code') or r.get('symbol') or "").replace(".JK","").upper()
        if not code or code in FCA_EXCLUDE or "-W" in code: continue
        if float(r.get('close') or 100) < 50: continue
        candidates.append(code)
        if len(candidates) >= 150: break
    final=[]
    for slice_size in [top_gainer_arjum, 100, 150]:
        pool = candidates[:slice_size]
        all_quotes={}
        for i in range(0, len(pool), 15):
            q=get_itick_quotes_batch(pool[i:i+15],15)
            all_quotes.update(q)
            time.sleep(0.2)
        if not all_quotes:
            for r in rows:
                code=(r.get('stock_code') or "").replace(".JK","").upper()
                if code in pool:
                    all_quotes[code]={'price': float(r.get('close') or 0), 'changepct': float(r.get('change_pct') or 0), 'high': float(r.get('high') or 0), 'low': float(r.get('low') or 0)}
        sorted_q = sorted(all_quotes.items(), key=lambda x: x[1].get('changepct', -999), reverse=True)
        top_codes = [c for c,_ in sorted_q[:top_itick]]
        def check_one(sym):
            try:
                hd=get_history_pro(sym,120)
                if hd is None or len(hd)<55: return None
                q=all_quotes.get(sym,{})
                c = float(q.get('price') or hd['Close'].iloc[-1])
                v_last=float(hd['Volume'].iloc[-1]); v_avg=float(hd['Volume'].iloc[-21:-1].mean())
                if v_avg==0 or v_last==0: return None
                ratio=v_last/v_avg
                if ratio < vol_thr: return None
                if v_last*c < min_value: return None
                hi=q.get('high') or float(hd['High'].iloc[-1]); lo=q.get('low') or float(hd['Low'].iloc[-1])
                cp=(c-lo)/(hi-lo) if hi!=lo else 1.0
                if cp < min_closepos: return None
                bo=check_bo_ema50_bob200(sym,hd,q.get('price'))
                if not bo: return None
                bandar=get_bandar_info(sym)
                item={"symbol":sym,"type":bo['type'],"close":int(c),"change_pct":q.get('changepct',0),"vol_ratio":ratio,"vol_rp":v_last*c,"close_pos":cp,"ema50":bo['ema50'],"ema200":bo['ema200'],"bandar_bonus":bandar.get('score_bonus',0),"foreign_str":bandar.get('foreign_str',''),"bandar_str":bandar.get('bandar_str','')}
                base,bonus,total=calculate_score_v48(item)
                item['base_score']=base; item['bandar_bonus']=bonus; item['score']=total
                return item
            except: return None
        with ThreadPoolExecutor(max_workers=15) as ex:
            futs={ex.submit(check_one,s):s for s in top_codes}
            batch=[]
            for f in as_completed(futs):
                r=f.result()
                if r:
                    batch.append(r)
                    print(f"🔥 {r['symbol']} {r['type']} S{r['score']} {r['foreign_str']}")
        batch.sort(key=lambda x: x['score'], reverse=True)
        final=batch
        if len(final)>=3: break
    final.sort(key=lambda x: x['score'], reverse=True)
    return final

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
            print(f"🔥 VOL {sym} {ratio:.1f}x")
        except: continue
    if sort_by_rp: detected.sort(key=lambda x: x['vol_rp'], reverse=True)
    else: detected.sort(key=lambda x: x['vol_ratio'], reverse=True)
    return detected

# ===== TELEGRAM SEND =====
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
    except Exception as e:
        print(f"send err {e}"); return False

def send_photo_reply(cid, path, caption=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(path,'rb') as ph:
            requests.post(url,data={'chat_id':cid,'caption':caption,'parse_mode':'Markdown'},files={'photo':ph},timeout=30)
    except Exception as e:
        print(f"Photo err {e}")

# ===== CHART (dari V4.3.3) =====
def calculate_atr(df,p=14):
    tr1=df['High']-df['Low']; tr2=(df['High']-df['Close'].shift(1)).abs(); tr3=(df['Low']-df['Close'].shift(1)).abs()
    tr=pd.concat([tr1,tr2,tr3],axis=1).max(axis=1); return tr.rolling(window=p,min_periods=1).mean()

# OLD simple chart replaced by PRO CHART
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
        buy_pct_temp=int(buy_ratios[-1]*100)
        power="TURBO" if buy_pct_temp>=85 and vchg1>=1.2 else "STRONG" if buy_pct_temp>=70 or vchg1>=1.5 else "NORMAL" if buy_pct_temp>=60 else "WEAK"
        safety="GOOD" if last_close>df['EMA200'].iloc[-1] else "BAD"
        ema13=df['EMA13'].iloc[-1]; ema20=df['EMA20'].iloc[-1]; ema50=df['EMA50'].iloc[-1]; ema200=df['EMA200'].iloc[-1]
        buy_pct=int(buy_ratios[-1]*100); sell_pct=100-buy_pct
        net_vol=df['Net_Vol_VSA'].iloc[-1]; net_vol_5d=df['Net_Vol_VSA'].tail(5).sum()

        plt.style.use('dark_background')
        fig=plt.figure(figsize=(16,9),dpi=200,facecolor='#000000')
        gs=gridspec.GridSpec(4,1,height_ratios=[4.5,1.1,0.9,0.8],hspace=0.05)
        ax_main=fig.add_subplot(gs[0]); ax_vol=fig.add_subplot(gs[1],sharex=ax_main); ax_nbsa=fig.add_subplot(gs[2],sharex=ax_main); ax_mm=fig.add_subplot(gs[3],sharex=ax_main)
        fig.subplots_adjust(left=0.08,right=0.92,top=0.88,bottom=0.06)
        for ax in [ax_main,ax_vol,ax_nbsa,ax_mm]:
            ax.set_facecolor('#000000')
            ax.tick_params(colors='#aaaaaa',labelsize=8)
            ax.yaxis.tick_right()
            ax.grid(False)

        x=np.arange(len(df))
        # Candle
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

        fig.text(0.01,0.96,f"{symbol} :    {last_close:.0f} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=13,fontweight='bold',ha='left',va='center')
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

        # NBSA
        nbsa_label=f"NBSA Rp. {abs(net_vol*last_close)/1e9:.2f} B"
        ax_nbsa.text(0.005,0.85,nbsa_label,transform=ax_nbsa.transAxes,color='#ffffff',fontsize=8,va='top')
        nbsa_vals=df['Net_Vol_VSA'].tail(80)/(df['Net_Vol_VSA'].abs().max() or 1)*50
        xn=np.arange(len(df)-len(nbsa_vals),len(df))
        for i,v in zip(xn,nbsa_vals):
            ax_nbsa.bar(i,v,color='#00ffff' if v>=0 else '#ff4444',width=0.6)
        ax_nbsa.axhline(0,color='#444444',linewidth=0.5)
        ax_nbsa.set_ylim(-60,60)

        # MM
        ax_mm.text(0.005,0.85,"Market Maker",transform=ax_mm.transAxes,color='#ffffff',fontsize=8,va='top')
        if 'MM' not in df.columns: df['MM']=(df['Close']-df['EMA50'])/df['EMA50']*1000
        mm_vals=df['MM'].tail(80)
        xm=np.arange(len(df)-len(mm_vals),len(df))
        ax_mm.bar(xm,mm_vals,color='#cccccc',width=0.5,alpha=0.8)
        step=max(1,len(df)//8)
        ax_mm.set_xticks(x[::step])
        ax_mm.set_xticklabels([df.index[i].strftime('%b') if hasattr(df.index[i],'strftime') else str(i) for i in range(0,len(df),step)],fontsize=7)

        plt.savefig(output_filename,dpi=200,bbox_inches='tight',facecolor='#000000')
        plt.close('all')
        return output_filename
    except Exception as e:
        print(f"Chart error {e}")
        import traceback; traceback.print_exc()
        return None
    finally:
        try: plt.clf(); plt.close('all')
        except: pass


def generate_simple_chart_BACKUP(df, symbol, output_filename="chart.png"):
    try:
        plt.style.use('dark_background')
        fig, (ax1, ax2) = plt.subplots(2,1, figsize=(12,8), gridspec_kw={'height_ratios':[3,1]}, facecolor='black')
        x=np.arange(len(df))
        for i in range(len(df)):
            o,h,l,c=df['Open'].iloc[i],df['High'].iloc[i],df['Low'].iloc[i],df['Close'].iloc[i]
            color='#00ff00' if c>=o else '#ff0000'
            ax1.plot([i,i],[l,h],color=color,linewidth=0.8)
            body_low=min(o,c); body_h=max(0.5,abs(c-o))
            rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor=color if c<o else 'none',edgecolor=color,linewidth=0.8)
            ax1.add_patch(rect)
        ema50=df['Close'].ewm(span=50).mean()
        ema200=df['Close'].ewm(span=200).mean()
        ax1.plot(x,ema50,color='white',linewidth=1); ax1.plot(x,ema200,color='purple',linewidth=1.2)
        ax1.set_title(f"{symbol} - {df['Close'].iloc[-1]:.0f} ({((df['Close'].iloc[-1]/df['Close'].iloc[-2]-1)*100):+.2f}%)", color='yellow')
        ax1.set_facecolor('black')
        ax2.bar(x,df['Volume'],color='#00cc00',width=0.8,alpha=0.7)
        ax2.set_facecolor('black')
        plt.tight_layout()
        plt.savefig(output_filename,dpi=150,bbox_inches='tight',facecolor='black')
        plt.close('all')
        return output_filename
    except Exception as e:
        print(f"Chart err {e}"); return None

def process_chart_request(cid, code, tf="1d", cache=None):
    send_reply(cid, f"📊 *{code.upper()} ({tf})...*")
    df=get_history_pro(code,150)
    if df is None or len(df)<20:
        send_reply(cid, f"⚠ Data {code} tidak ada"); return
    chart_file=f"chart_{code.upper()}_{int(time.time())}.png"
    fp=generate_pro_chart(df, symbol=code.upper(), timeframe="1d", sector_info=f"{code.upper()} | IHSG", output_filename=chart_file, extra_info={})
    if not fp or not os.path.exists(fp):
        send_reply(cid, "❌ Gagal render chart"); return
    caption=f"*{code.upper()}* {int(df['Close'].iloc[-1])} | Vol {format_large_number(df['Volume'].iloc[-1])} | EMA50 {int(df['Close'].ewm(span=50).mean().iloc[-1])}"
    send_photo_reply(cid, fp, caption=caption)
    if os.path.exists(fp): os.remove(fp)

def process_broker_request(cid, sym):
    try:
        bandar=get_bandar_info(sym)
        hd=get_history_pro(sym,30)
        close=int(hd['Close'].iloc[-1]) if hd is not None else 0
        msg=f"🏦 *{sym} BANDAR*\nClose: {close}\n{bandar.get('foreign_str','N/A')} {bandar.get('bandar_str','N/A')}\nBonus: +{bandar.get('score_bonus',0)}"
        send_reply(cid, msg)
    except Exception as e:
        send_reply(cid, f"❌ {e}")

# ===== BROADCAST FIX =====
def broadcast_v48(signals, vol_thr=1.5, dest_chat_id=None):
    target = dest_chat_id or TARGET_CHAT_ID
    if not target: return
    if not signals:
        send_reply(target, f"📉 V4.8 VOL>{vol_thr}x - Tidak ada BO valid"); return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header=f"*🚀 V4.8 TOP {len(signals)} BO + BANDAR* {now}\n{'='*30}\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        tot=it.get('score',0); fb=it.get('foreign_str','')
        line=f"{idx}. *{it['symbol']}* {it['type']} S{tot} {it['close']} ({it['change_pct']:+.1f}%) Vol {it['vol_ratio']:.1f}x {fb}\n\n"
        kb.append([{"text":f"{it['symbol']} S{tot}", "callback_data":f"chart_{it['symbol']}"}])
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

# ===== LISTENER - SEMUA COMMAND BALIK =====
def telegram_bot_listener():
    offset=0
    print("🤖 RAFANO V4.8 ULTIMATE - All commands restored + fix broadcast")
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
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery",json={"callback_query_id":qid})
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
                        help_msg="""🔥 *RAFANO V4.8 ULTIMATE* - Semua fitur balik
==========================
📈 *CHART & BROKER (YANG HILANG TADI)*
/c KODE - Chart KODE (contoh /c BBCA)
/c KODE 5m - Chart 5 menit
/chart KODE - Sama kayak /c
/b KODE /broker KODE - Bandar info
/bandar KODE

🚀 *SCAN BO + BANDAR*
/scanbo [vol] [minval] - BO + Bandar scoring
/scanbo 2 - Vol>2x
/topbo 3 - Top 3 BO

🔥 *VOLUME SPIKE*
/scanvol /vol [thr] [limit] - Vol spike
/scanvolall /volall [thr] - 300 saham sort Rp
/volakum - Vol + Akum

⚡ *LAIN*
/quota - Cek quota Arjum
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
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            threading.Thread(target=process_broker_request,args=(chat_id,sym)).start()
                        else:
                            send_reply(chat_id, "Pakai: /b BBCA")

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
                        send_reply(chat_id, f"🔥🔥 SCAN VOL ALL 300 >{thr}x SORT Rp ke {chat_id}...")
                        def run_volall(tg=chat_id, th=thr):
                            sigs=scan_volume_spike(threshold=th, limit_candidates=300, sort_by_rp=True)
                            broadcast_vol_spike(sigs, threshold=th, sort_by_rp=True, dest_chat_id=tg)
                        threading.Thread(target=run_volall).start()

                    elif first.startswith("/scanbo") or first.startswith("/topbo"):
                        vol_thr=1.5; top_final=30; top_arjum=60; min_val=1_000_000_000
                        try:
                            if len(parts)>=2: vol_thr=float(parts[1])
                            if len(parts)>=3: min_val=float(parts[2])
                        except: pass
                        if vol_thr>=10: top_final=int(vol_thr); vol_thr=1.5
                        send_reply(chat_id, f"🚀 SCAN BO ARJUM{top_arjum}->ITICK{top_final} VOL>{vol_thr}x ke {chat_id}...")
                        def run_scan(tg=chat_id, vt=vol_thr, tf=top_final, ta=top_arjum, mv=min_val):
                            sigs=scan_v48(top_gainer_arjum=ta, top_itick=tf, vol_thr=vt, min_value=mv)
                            broadcast_v48(sigs, vol_thr=vt, dest_chat_id=tg)
                        threading.Thread(target=run_scan).start()

                    elif first.startswith("/scan"):
                        # /scan, /scanfast, /scanfull fallback ke scanbo
                        vol_thr=1.5
                        try:
                            if len(parts)>=2: vol_thr=float(parts[1])
                        except: pass
                        send_reply(chat_id, f"🔍 SCAN BO VOL>{vol_thr}x ke {chat_id}...")
                        def run_scan2(tg=chat_id, vt=vol_thr):
                            sigs=scan_v48(vol_thr=vt)
                            broadcast_v48(sigs, vol_thr=vt, dest_chat_id=tg)
                        threading.Thread(target=run_scan2).start()

        except Exception as e:
            print(f"Listener err {e}")
            import traceback; traceback.print_exc()
            time.sleep(3)

if __name__=="__main__":
    print("==========================================")
    print("🔥 RAFANO V4.8 ULTIMATE - /c /b /scanvol balik + bandar scoring")
    print("==========================================")
    telegram_bot_listener()
