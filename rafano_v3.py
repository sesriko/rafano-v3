"""
RAFANO V4.3.3 + SCAN VOLUME SPIKE >2x - FINAL FIX BROADCAST
- Script ASLI lu yang jalan normal, 100% sama
- Fix cuma: broadcast ke chat yang request, bukan ke TARGET_CHAT_ID terus
- Token, Chat ID, API Key tetap pakai .env file yang pertama (load_dotenv)
"""
import os, time, datetime, threading, requests, pytz, numpy as np, pandas as pd, matplotlib.patches as patches, matplotlib.gridspec as gridspec
from dotenv import load_dotenv
load_dotenv()

# ===== ITICK.ORG INTEGRATION - V4.6.0 3-API HYBRID =====
ITICK_TOKEN = os.getenv("ITICK_TOKEN") or os.getenv("ITICK_API_KEY") or "7a470a83276242309fb940684046d35a88e450fdb95b46c383670e1e0c5e96f5"
ITICK_BASE = "https://api.itick.org"
ITICK_ENABLED = bool(ITICK_TOKEN)

def get_itick_quote(sym):
    if not ITICK_ENABLED:
        return None
    try:
        url = f"{ITICK_BASE}/stock/quote?region=ID&code={sym}"
        headers = {"accept": "application/json", "token": ITICK_TOKEN}
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            j = r.json()
            if j.get('code') == 0 and j.get('data'):
                d = j['data']
                return {
                    'price': float(d.get('ld') or d.get('c') or 0),
                    'open': float(d.get('o') or 0),
                    'high': float(d.get('h') or 0),
                    'low': float(d.get('l') or 0),
                    'volume': float(d.get('v') or 0),
                    'change': float(d.get('ch') or 0),
                    'changepct': float(d.get('chp') or 0),
                    'source': 'ITICK_REALTIME'
                }
    except Exception as e:
        print(f"itick quote error {sym}: {e}")
    return None

def get_itick_quotes_batch(symbols, max_batch=15):
    if not ITICK_ENABLED or not symbols:
        return {}
    try:
        codes = ",".join(symbols[:max_batch])
        url = f"{ITICK_BASE}/stock/quotes?region=ID&codes={codes}"
        headers = {"accept": "application/json", "token": ITICK_TOKEN}
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code == 200:
            j = r.json()
            if j.get('code') == 0 and j.get('data'):
                result = {}
                for item in j['data']:
                    code = item.get('s') or item.get('code')
                    if code:
                        result[code] = {
                            'price': float(item.get('ld') or 0),
                            'change': float(item.get('ch') or 0),
                            'changepct': float(item.get('chp') or 0),
                            'source': 'ITICK_REALTIME'
                        }
                return result
    except Exception as e:
        print(f"itick batch error: {e}")
    return {}

def safe_get_env(k):
    v=os.getenv(k)
    if v: return str(v).strip().strip('"').strip("'")
    return None

FCA_EXCLUDE = {"FUTR","FITT","HOTEL","ITIC","PUDP","PUDJIADI","COIN","SHID","RELI","ASPI","MEJA","MINA","ESTA","ASLI","VKTR","IMJS","GTSI","IRSX","ATAP","RONY","BCIC","DEFI","ROCK","YPAS","NIRO","BBHA","BKSW","NAGA","BEEF","BPTR","CBMF","CPRI","CRAB","DAAZ","DEAL","DGNS","DMND","DUCK","ELSA","ENRG","ENVY","ERAA","ESTI","ETWA","FIRE","FORU","GAMA","GOLL","HAIS","HATM","HITS","HOMI","IATA","INPS","IPOL","JGLE","KAYU","KBAG","KIOS","KPAL","KPAS","LCGP","LPLI","LPLR","MAGP","MAMI","MARI","MABA","MABA-W","MBMA-W","NINE","NUSA","PALM","PADI","PDPP","PEVE","PGLI","PGJO","PICO","POWR","PSAB","PTDU","PURA","RAJA-W","RIGS","RODA","SATI","SINI","SKYB","SMKM","SOCI","SONA","SOSS","SUGI","TALF","TDPM","TEBE","TOPS","TRAM","TRIL","TRIO","TRUS","UFOE","WIFI-W","WOWS","YELO","ZATA","ZONE","ZINC","TINS-W","BIPI-W","BULL-W","DEWA-W","ARKA","ARTO-W","BBYB-W","KPAS","LPCK","MMLP","MTFN","NELY","NOBU","PNBN","PNBS","BNGA-W","BTPS-W"}

IDX_600_LIQUID = [
    "AALI","ACES","ACRO","ACST","ADCP","ADES","ADHI","ADMG","ADMR","ADRO","AGAR","AGII","AIMS","AKKU","AKPI","AKRA","AKSI","ALDO","ALKA","AMAN","AMFG","AMIN","AMMN","ANDI","ANJT","ANTM","APII","APLN","ARCI","AREA","ARGO","ARII","ARNA","ARTA","ASGR","ASHA","ASII","ASLC","ASLI","ASPI","ASRI","ATAP","ATLA","AUTO","AVIA","AWAN","AXIO","AYAM","AYLS","BABY","BAIK","BALI","BANK","BAPA","BATA","BATR","BAYU","BBRM","BCIP","BDKR","BEBS","BELI","BELL","BESS","BEST","BIKE","BIMA","BINO","BIRD","BISI","BKDP","BKSL","BLES","BLTA","BLTZ","BLUE","BMHS","BMSR","BMTR","BOBA","BOGA","BOLT","BRAM","BRIS","BRMS","BRPT","BSBK","BSDE","BSML","BSSR","BTON","BTPS","BUAH","BUDI","BUKK","BYAN","CAKK","CAMP","CANI","CARE","CASS","CBPE","CBRE","CCSI","CEKA","CGAS","CHEM","CINT","CITA","CITY","CLEO","CLPI","CMNP","CMPP","CMRY","CNMA","CPIN","CPRO","CRAB","CRSN","CSAP","CSIS","CSMI","CSRA","CTBN","CTRA","CUAN","CYBR","DAAZ","DADA","DATA","DAYA","DCII","DEPO","DEWI","DGIK","DGNS","DILD","DIVA","DKFT","DMAS","DMMX","DMND","DOOH","DOSS","DPNS","DRMA","DSFI","DSNG","DSSA","DUTI","DVLA","DWGL","DYAN","EAST","ECII","EDGE","EKAD","ELIT","ELPI","ELTY","EMDE","EMTK","ENAK","EPAC","EPMT","ERAA","ERAL","ESIP","ESSA","ESTA","EXCL","FAPA","FAST","FILM","FIRE","FISH","FMII","FOLK","FOOD","FORU","FPNI","FREN","FUTR","FWCT","GDST","GDYR","GEMA","GEMS","GGRP","GHON","GIAA","GJTL","GLVA","GMTD","GOLD","GOLF","GOOD","GOTO","GPRA","GPSO","GRIA","GRPH","GTBO","GTSI","GULA","GUNA","GWSA","GZCO","HAIS","HALO","HATM","HDIT","HEAL","HERO","HEXA","HITS","HOKI","HOMI","HOPE","HRME","HRUM","HUMI","HYGN","IATA","IBST","ICBP","ICON","IDPR","IFII","IFSH","IGAR","IKAN","IKBI","IKPM","IMPC","INCI","INCO","INDF","INDR","INDS","INDX","INDY","INET","INKP","INTD","INTP","IPCC","IPCM","IPOL","IPPE","IPTV","IRRA","IRSX","ISAT","ISSP","ITMA","ITMG","JAST","JATI","JAYA","JECC","JGLE","JIHD","JKON","JMAS","JPFA","JRPT","JSPT","JTPE","KARW","KBLI","KBLM","KBLV","KDSI","KDTN","KEEN","KEJU","KIAS","KICI","KIJA","KINO","KIOS","KJEN","KKES","KKGI","KLAS","KLBF","KMDS","KOBX","KOCI","KOKA","KONI","KOPI","KOTA","KPIG","KREN","KRYA","KUAS","LABA","LABS","LAJU","LAND","LCKM","LION","LIVE","LMPI","LMSH","LPCK","LPIN","LPKR","LPLI","LPPF","LRNA","LSIP","LTLS","LUCK","MAHA","MAIN","MAPA","MAPB","MAPI","MARK","MAXI","MBAP","MBMA","MBSS","MBTO","MCAS","MCOL","MDKA","MDKI","MEDC","MEDS","MERK","META","MFMI","MGNA","MHKI","MICE","MIDI","MIKA","MINA","MIRA","MITI","MKAP","MKPI","MKTR","MLIA","MLPL","MLPT","MMIX","MMLP","MNCN","MORA","MPIX","MPMX","MPOW","MPPA","MPRO","MPXL","MSJA","MSKY","MSTI","MTDL","MTEL","MTLA","MTMH","MTSM","MUTU","MYOH","MYOR","NAIK","NASA","NASI","NELY","NEST","NFCX","NICE","NICL","NIKL","NPGF","NRCA","NTBK","NZIA","OBMD","OILS","OKAS","OMED","OMRE","OPMS","PADA","PALM","PAMG","PANI","PANR","PBID","PBSA","PCAR","PDPP","PEHA","PEVE","PGAS","PGEO","PGLI","PGUN","PIPA","PJAA","PKPK","PLIN","PMJS","PNBS","PNGO","PNSE","POLI","PORT","POWR","PPRE","PPRI","PRAY","PRDA","PSAB","PSDN","PSGO","PSKT","PSSI","PTBA","PTIS","PTMP","PTMR","PTPP","PTPS","PTPW","PTRO","PTSN","PTSP","PURA","PURI","PWON","PZZA","RAAM","RAFI","RAJA","RALS","RANC","RBMS","RDTX","REAL","RGAS","RIGS","RISE","RMKE","ROCK","RODA","RONY","ROTI","RSCH","RSGK","RUIS","SAGE","SAME","SAMF","SAPX","SATU","SBMA","SCCO","SCMA","SCNP","SCPI","SDPC","SEMA","SGER","SGRO","SHID","SICO","SIDO","SILO","SIMP","SIPD","SKBM","SKLT","SKRN","SLIS","SMAR","SMBR","SMCB","SMDM","SMDR","SMGA","SMGR","SMIL","SMKL","SMLE","SMMT","SMRA","SMSM","SNLK","SOCI","SOHO","SOLA","SONA","SOSS","SOTS","SPMA","SPTO","SRAJ","SRTG","SSIA","SSTM","STAA","STTP","SUNI","SUPR","SURI","SWID","TAMA","TAMU","TAPG","TAYS","TBMS","TCID","TCPI","TEBE","TFAS","TFCO","TGKA","TGUK","TINS","TIRA","TKIM","TLDN","TLKM","TMAS","TMPO","TNCA","TOOL","TOSK","TOTL","TOTO","TOYS","TPIA","TPMA","TRIS","TRON","TRST","TRUE","TRUK","TSPC","TYRE","UANG","UCID","UFOE","ULTJ","UNIC","UNIQ","UNTR","UNVR","UVCR","VAST","VERN","VICI","VISI","VKTR","VOKS","WAPO","WEGE","WEHA","WIFI","WINR","WINS","WIRG","WMUU","WOOD","WOWS","WTON","YPAS","ZATA","ZONE","ZYRX","BBCA","BBRI","BMRI","BBNI","BREN","BIPI","BUKA","NCKL","AMRT","TOWR","TBIG","DEWA","ELSA","BULL","BEKS","BBKP","BNGA","BDMN","BJBR","BJTM","CDIA",
]

def get_liquid_candidates():
    return IDX_600_LIQUID

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
    if s=="VSA_ESTIMATE": return "⚠ VSA"
    if s=="QUOTA": return "♻ Cache"
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
BROKER_CACHE_TTL=1800; HISTORY_CACHE_TTL=300; SCREENER_CACHE_TTL=120
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
    a5,d5,n5,s5,b5,sr5=get_broker_summary(sym,df_from=d5,df_to=today)
    _t.sleep(0.4)
    a20,d20,n20,s20,b20,sr20=get_broker_summary(sym,df_from=d20,df_to=today)
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

def get_screener_latest(force_today=False):
    if not force_today:
        cached=get_cached_screener()
        if cached:
            if isinstance(cached,dict) and 'rows' in cached:
                norm=[]
                for r in cached['rows']:
                    code=r.get('stock_code') or r.get('symbol') or r.get('code')
                    if code: norm.append({'symbol':code.replace(".JK","").upper(),'raw':r})
                return norm
            elif not force_today:
                return cached
    data=arjum_get("/screener/latest",use_cache=False if force_today else True)
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

def scan_volume_spike(threshold=2.0, limit_candidates=60, akum_only=False, sort_by_rp=False):
    print(f"[{get_now_wib()}] 🚀 SCAN VOLUME SPIKE >{threshold}x - QUOTA {'HABIS' if QUOTA_HIT else 'OK'}")
    sd=get_screener_latest(force_today=False)
    if sd:
        cands=[(it.get('symbol') or it.get('code') or "").replace(".JK","").upper() for it in sd]
        cands=[c for c in cands if c]
    else:
        cands=IDX_600_LIQUID
    seen=set(); uniq=[]
    for c in cands:
        cu=c.upper().strip()
        if not cu: continue
        if "-W" in cu: continue
        if cu and cu not in seen and cu not in FCA_EXCLUDE:
            seen.add(cu); uniq.append(cu)
    for c in IDX_600_LIQUID:
        if len(uniq)>=limit_candidates: break
        if "-W" in c: continue
        if c not in seen and c not in FCA_EXCLUDE:
            uniq.append(c); seen.add(c)
    cands=uniq[:limit_candidates]
    detected=[]
    def process_vol(sym):
        try:
            hd=get_history_pro(sym, limit=60, timeframe="1d")
            if hd is None or len(hd)<20: return None
            v_last=hd['Volume'].iloc[-1]
            v_avg=hd['Volume'].tail(20).mean()
            if v_avg==0: return None
            ratio=v_last/v_avg
            if ratio < threshold: return None
            close=hd['Close'].iloc[-1]
            prev=hd['Close'].iloc[-2] if len(hd)>=2 else close
            chg_pct=(close/prev-1)*100 if prev else 0
            # VSA buy%
            df_v,_=calculate_vsa_metrics(hd.copy())
            buy_pct=df_v['Buy_Pct'].iloc[-1] if 'Buy_Pct' in df_v.columns else 50
            if akum_only and buy_pct < 60: return None
            vol_rp = v_last*close
            return {"symbol":sym,"close":int(close),"change_pct":chg_pct,"vol_ratio":ratio,"vol_last":v_last,"vol_avg":v_avg,"vol_rp":vol_rp,"buy_pct":buy_pct,"history_df":hd}
        except: return None
    for sym in cands:
        r=process_vol(sym)
        if r:
            detected.append(r)
            print(f"🔥 VOL {r['symbol']} {r['vol_ratio']:.1f}x Rp {format_large_number(r['vol_rp'])} Buy {r['buy_pct']:.0f}%")
    seen={}
    for d in detected:
        if d['symbol'] not in seen or d['vol_ratio']>seen[d['symbol']]['vol_ratio']:
            seen[d['symbol']]=d
    detected=list(seen.values())
    if sort_by_rp:
        detected.sort(key=lambda x: x.get('vol_rp',0), reverse=True)
    else:
        detected.sort(key=lambda x: x['vol_ratio'], reverse=True)
    return detected

# FIX: tambah dest_chat_id
def broadcast_vol_spike(signals, threshold=2.0, akum_only=False, sort_by_rp=False, dest_chat_id=None):
    target = dest_chat_id or TARGET_CHAT_ID
    if not target:
        print("❌ No target chat"); return
    if not signals:
        msg = f"Vol Spike + AKUM REAL >{threshold}x: Tidak ada yang valid hari ini." if akum_only else f"Vol Spike >{threshold}x: Tidak ada yang spike hari ini."
        send_reply(target, msg)
        return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    tag = f" + AKUM (REAL+VSA)" if akum_only else ""
    if sort_by_rp: tag += " [SORT Rp]"
    header=f"*VOL SPIKE{tag} >{threshold}x* 🔥\n{now} | {len(signals)} saham\n{'='*30}\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        rp_str=format_large_number(it.get('vol_rp',0), False)
        line=f"{idx}. *{it['symbol']}* {it['close']} ({it['change_pct']:+.1f}%) Vol {it['vol_ratio']:.1f}x Rp {rp_str} Buy {it['buy_pct']:.0f}%\n\n"
        kb.append([{"text": f"{it['symbol']} {it['vol_ratio']:.1f}x", "callback_data": f"chart_{it['symbol']}_1d"}])
        if len(msg)+len(line)>3500:
            send_reply(target, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
        else:
            msg+=line
    if msg:
        send_reply(target, msg, rm={"inline_keyboard": kb})

def send_reply(cid,txt,rm=None):
    if not TELEGRAM_BOT_TOKEN or not cid: return
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    pl={"chat_id":cid,"text":txt,"parse_mode":"Markdown"}
    if rm: pl["reply_markup"]=rm
    try:
        r=requests.post(url,json=pl,timeout=15)
        j=r.json()
        if not j.get('ok'):
            print(f"TG fail to {cid}: {j}")
            pl.pop('parse_mode',None)
            requests.post(url,json=pl,timeout=15)
        else:
            print(f"✅ Sent to {cid}")
    except Exception as e:
        print(f"send_reply err {e}")

def send_photo_reply(cid,pp,cap=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(pp,'rb') as ph:
            requests.post(url,data={'chat_id':cid,'caption':cap,'parse_mode':'Markdown'},files={'photo':ph},timeout=30)
    except Exception as e: print(f"Photo err {e}")

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
        if pc <= pe50 and c > ema50 and c > pe50:
            is_bo=True; stype="BO EMA50"
        elif pc <= pe200 and c > ema200 and c > pe200:
            is_bo=True; stype="BOB EMA200"
        if not is_bo: return None
        if signal_type_filter=="BO_BOB" and stype not in ["BO EMA50","BOB EMA200"]: return None
        if len(hd)>=4:
            c2=float(hd['Close'].iloc[-3]); c3=float(hd['Close'].iloc[-4])
            pe50_2=float(ema50_s.iloc[-3]); pe50_3=float(ema50_s.iloc[-4])
            if stype=="BO EMA50":
                if c2 > pe50_2 and c3 > pe50_3: return None
                if c2 > pe50_2 and pc > pe50: return None
        vol_today = float(hd['Volume'].iloc[-1])
        avg_vol_20 = float(hd['Volume'].iloc[-21:-1].mean()) if len(hd)>=21 else float(hd['Volume'].iloc[:-1].mean())
        if avg_vol_20 <= 0 or vol_today <= 0: return None
        vol_ratio = vol_today / avg_vol_20
        if vol_ratio < 1.5: return None
        chg = (c/pc-1)*100 if pc>0 else 0
        return {"symbol":sym,"type":stype,"close":c,"change":chg,"ema50":ema50,"ema200":ema200,"last_date":str(last_candle_date),"vol_today":vol_today,"avg_vol_20":avg_vol_20,"vol_ratio":vol_ratio,"source":"STRICT_TODAY_VOL","score":vol_ratio*10}
    except:
        return None

def scan_v3_full_fast(force_today=False, limit_candidates=60, signal_type_filter="BO_BOB"):
    today_date=get_now_wib().date()
    print(f"[{get_now_wib()}] ⚡ FAST SCAN limit={limit_candidates}")
    candidates = get_liquid_candidates()[:limit_candidates]
    candidates = [c for c in candidates if "-W" not in c]
    signals=[]
    def proc(sym):
        try:
            hd=get_history_pro(sym,limit=100,timeframe="1d")
            return _check_strict_bo_today(sym, hd, today_date, signal_type_filter)
        except:
            return None
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=20) as ex:
        futs={ex.submit(proc,s):s for s in candidates}
        for f in as_completed(futs):
            r=f.result()
            if r:
                signals.append(r)
    return signals

def scan_v3_full(force_today=False, limit_candidates=150):
    return scan_v3_full_fast(force_today=True, limit_candidates=limit_candidates, signal_type_filter="BO_BOB")

# FIX: broadcast_v3 pakai dest_chat_id
def broadcast_v3(signals, filter_label="BO_BOB", dest_chat_id=None):
    target = dest_chat_id or TARGET_CHAT_ID
    if not target:
        print("❌ No target chat"); return
    if not signals:
        msg="Scan: Tidak ada BUY / Quota habis."+("\n♻ Cache" if QUOTA_HIT else "")
        send_reply(target,msg); return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header=f"*RAFANO V4.3.3 FIX*\n{now} | {len(signals)} BUY\n{'='*30}\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        item=f"{idx}. *{it['symbol']}* {it['close']:.0f} ({it.get('change',0):+.1f}%) Vol {it.get('vol_ratio',0):.1f}x\n"
        kb.append([{"text":f"{it['symbol']}", "callback_data":f"chart_{it['symbol']}_1d"}])
        if len(msg)+len(item)>3500:
            send_reply(target,msg,rm={"inline_keyboard":kb}); msg=item; kb=[]
        else: msg+=item
    if msg: send_reply(target,msg,rm={"inline_keyboard":kb})

def telegram_bot_listener():
    offset=0
    print("🤖 V4.3.3 FINAL FIX - Listening... Reply ke chat yang request")
    print(f"🔑 BOT_TOKEN: {'ADA' if TELEGRAM_BOT_TOKEN else 'KOSONG'} {TELEGRAM_BOT_TOKEN[:10] if TELEGRAM_BOT_TOKEN else ''}...")
    print(f"💬 TARGET_CHAT_ID: {TARGET_CHAT_ID}")
    try:
        r=requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true",timeout=10)
        print(f"deleteWebhook: {r.json()}")
    except Exception as e:
        print(f"deleteWebhook err: {e}")

    while True:
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res=requests.get(url,timeout=25)
            if res.status_code!=200:
                print(f"getUpdates {res.status_code} {res.text[:100]}")
                time.sleep(3); continue
            data=res.json()
            if not data.get('ok'):
                print(f"getUpdates not ok {data}"); time.sleep(3); continue

            results=data.get("result",[])
            if results:
                print(f"📥 Got {len(results)} updates")

            for update in results:
                offset=update["update_id"]+1
                print(f"🔔 Update {update['update_id']}: {list(update.keys())}")

                if "message" in update and "text" in update["message"]:
                    txt=update["message"].get("text","").strip()
                    chat_id=update["message"]["chat"]["id"]
                    from_user=update["message"]["from"].get("username") or update["message"]["from"].get("first_name","?")
                    chat_type=update["message"]["chat"].get("type","private")
                    print(f"💬 {from_user} ({chat_id}/{chat_type}): {txt}")

                    first=txt.split()[0].lower() if txt else ""

                    if first in ["/start","/help","/menu"]:
                        send_reply(chat_id, "🔥 *RAFANO V4.3.3 FIX*\n/scan /scanfast /scanvol 2 /volall /c KODE\nBot reply ke chat yang request")

                    elif first.startswith("/scanvol") or first.startswith("/vol") or first.startswith("/vspike") or first.startswith("/spike") or first.startswith("/vall"):
                        parts=txt.split()
                        try:
                            thr=float(parts[1]) if len(parts)>=2 else 2.0
                            lim=int(parts[2]) if len(parts)>=3 else 60
                        except:
                            thr=2.0; lim=60
                        if thr<1.0: thr=1.0
                        if lim<20: lim=20
                        if lim>300: lim=300
                        sort_rp = "all" in first
                        send_reply(chat_id, f"🔥 SCAN VOL >{thr}x ({lim} saham) ke {chat_id}...")
                        def run_vol(tg=chat_id, th=thr, l=lim, sr=sort_rp):
                            sigs=scan_volume_spike(threshold=th, limit_candidates=l, sort_by_rp=sr)
                            broadcast_vol_spike(sigs, threshold=th, sort_by_rp=sr, dest_chat_id=tg)
                        threading.Thread(target=run_vol, args=(chat_id, thr, lim, sort_rp)).start()

                    elif first.startswith("/scan"):
                        send_reply(chat_id, f"🔍 SCAN BO 300 saham ke {chat_id}...")
                        def run_scan(tg=chat_id):
                            sigs=scan_v3_full(force_today=True, limit_candidates=300)
                            broadcast_v3(sigs, dest_chat_id=tg)
                        threading.Thread(target=run_scan, args=(chat_id,)).start()

        except Exception as e:
            print(f"Listener err {e}")
            import traceback; traceback.print_exc()
            time.sleep(3)

if __name__=="__main__":
    print("==========================================")
    print("🔥 RAFANO V4.3.3 FINAL FIX BROADCAST")
    print("Pakai .env yang pertama, token asli lu")
    print("==========================================")
    telegram_bot_listener()
