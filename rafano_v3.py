"""
RAFANO V3.2 - AKUM/DIST FIXED
Perbaikan utama: logic akumulasi & distribusi REAL
"""
import os, time, logging, datetime, threading, requests, pytz, json
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.gridspec as gridspec
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')

def safe_get_env(key: str):
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
    return {"X-API-Key": k.strip(), "Accept": "application/json", "User-Agent": "RAFANO/3.2"}

def get_now_wib(): return datetime.datetime.now(TIMEZONE_WIB)

# ===== CACHE THREAD-SAFE =====
CACHE_LOCK = threading.RLock()
BROKER_CACHE, HISTORY_CACHE, SCREENER_CACHE = {}, {}, {}
CACHE_FILE = Path("/tmp/rafano_cache.json")
BROKER_CACHE_TTL, HISTORY_CACHE_TTL, SCREENER_CACHE_TTL = 300, 600, 180

def make_cache_key(path, params):
    if not params: return path
    try: return f"{path}?{'&'.join([f'{k}={v}' for k,v in sorted(params.items())])}"
    except: return path

def get_cached_broker(k):
    with CACHE_LOCK:
        if k in BROKER_CACHE:
            ts,d = BROKER_CACHE[k]
            if time.time()-ts < BROKER_CACHE_TTL: return d
            del BROKER_CACHE[k]
    return None
def set_cached_broker(k,d):
    with CACHE_LOCK: BROKER_CACHE[k]=(time.time(),d)
    try:
        with open(CACHE_FILE,'w') as f: json.dump({'broker':{k:[v[0],v[1]] for k,v in BROKER_CACHE.items()}},f)
    except: pass
def get_cached_history(k):
    with CACHE_LOCK:
        if k in HISTORY_CACHE:
            ts,d=HISTORY_CACHE[k]
            if time.time()-ts < HISTORY_CACHE_TTL: return d
            del HISTORY_CACHE[k]
    return None
def set_cached_history(k,d):
    with CACHE_LOCK: HISTORY_CACHE[k]=(time.time(),d)
def get_cached_screener():
    with CACHE_LOCK:
        if 'latest' in SCREENER_CACHE:
            ts,d=SCREENER_CACHE['latest']
            if time.time()-ts < SCREENER_CACHE_TTL: return d
            del SCREENER_CACHE['latest']
    return None
def set_cached_screener(d):
    with CACHE_LOCK: SCREENER_CACHE['latest']=(time.time(),d)

# ===== HELPERS =====
def safe_int(v,d=0):
    try:
        if pd.isna(v) or np.isinf(v): return d
        return int(v)
    except: return d
def format_large_number(val, show_sign=False):
    if pd.isna(val) or val==0: return "0"
    av=abs(val); s="+" if show_sign and val>0 else "-" if val<0 else ""
    if av>=1e9: return f"{s}{av/1e9:.2f}B"
    if av>=1e6: return f"{s}{av/1e6:.0f}M"
    if av>=1e3: return f"{s}{av/1e3:.0f}K"
    return f"{s}{val:,.0f}"
def round_to_ihsg_fraction(p):
    if pd.isna(p) or p<=0: return 0
    p=float(p)
    tick=1 if p<200 else 2 if p<500 else 5 if p<2000 else 10 if p<5000 else 25
    return int(round(p/tick)*tick)
def calculate_atr(df,period=14):
    tr1=df['High']-df['Low']
    tr2=(df['High']-df['Close'].shift(1)).abs()
    tr3=(df['Low']-df['Close'].shift(1)).abs()
    tr=pd.concat([tr1,tr2,tr3],axis=1).max(axis=1)
    return tr.rolling(period,min_periods=1).mean()

# ===== VSA FIXED - REAL BUY/SELL =====
def calculate_vsa_metrics_fixed(df):
    """Buy% = posisi close di range 0-100% murni, bukan 30-90%"""
    df=df.copy()
    price_range=(df['High']-df['Low']).replace(0,0.1)
    close_pos=(df['Close']-df['Low'])/price_range
    close_pos=np.clip(close_pos,0.02,0.98)
    buy_ratio=close_pos.copy()  # 0 = full sell, 1 = full buy
    # Power boost hanya jika volume besar + close di atas + hijau
    if 'V1' not in df.columns:
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
    vol_ratio=df['Volume']/df['V1'].replace(0,1)
    is_green=df['Close']>=df['Open']
    is_high_close=close_pos>=0.70
    # TURBO: vol >2.5x + high close + green
    turbo_mask=(vol_ratio>2.5)&is_green&is_high_close
    strong_mask=(vol_ratio>1.5)&is_green&(close_pos>=0.6)
    buy_ratio=np.where(turbo_mask, np.minimum(0.97, buy_ratio+0.15), buy_ratio)
    buy_ratio=np.where(strong_mask&~turbo_mask, np.minimum(0.95, buy_ratio+0.08), buy_ratio)
    buy_ratio=np.clip(buy_ratio,0.05,0.97)
    df['Vol_Buy']=df['Volume']*buy_ratio
    df['Vol_Sell']=df['Volume']-df['Vol_Buy']
    df['Net_Vol_VSA']=df['Vol_Buy']-df['Vol_Sell']
    df['Net_Val_VSA']=df['Net_Vol_VSA']*df['Close']
    df['Buy_Pct']=buy_ratio*100
    return df, buy_ratio

# ===== AKUM/DIST LOGIC FIXED =====
MIN_TRX_THRESHOLD = 500_000_000  # 500jt minimal baru dianggap akum/dist

def determine_status(net_value, buy_value, sell_value, min_trx=MIN_TRX_THRESHOLD):
    """Status REAL dengan threshold"""
    if abs(net_value) < min_trx:
        # cek buy vs sell juga
        if max(buy_value, sell_value) < min_trx:
            return "NEUTRAL"
        # jika buy dan sell sama besar -> netral
        if abs(buy_value-sell_value) < min_trx*0.5:
            return "NEUTRAL"
    if net_value > 0:
        return "AKUM"
    elif net_value < 0:
        return "DIST"
    else:
        if buy_value > sell_value * 1.2: return "AKUM"
        if sell_value > buy_value * 1.2: return "DIST"
        return "NEUTRAL"

def calc_from_brokers_fixed(brokers_list):
    """Hitung buy/sell/net dari list broker TANPA fake 35%"""
    if not brokers_list: return 0,0,0,"NEUTRAL"
    buy_sum=sell_sum=net_sum=0
    for b in brokers_list:
        if not isinstance(b,dict): continue
        buy=float(b.get('buy_value',0) or b.get('bval',0) or 0)
        sell=float(b.get('sell_value',0) or b.get('sval',0) or 0)
        net=float(b.get('net_value',0) or b.get('nval',0) or 0)
        # Jika hanya net yang ada, jangan karang buy/sell
        if buy==0 and sell==0 and net!=0:
            if net>0:
                buy=net
                sell=0
            else:
                sell=abs(net)
                buy=0
        # Jika net 0 tapi buy/sell ada
        if net==0 and (buy!=0 or sell!=0):
            net=buy-sell
        buy_sum+=buy; sell_sum+=sell; net_sum+=net
    status=determine_status(net_sum, buy_sum, sell_sum)
    return buy_sum, sell_sum, net_sum, status

def format_top_brokers_fixed(brokers, top=3, status="AKUM"):
    if not brokers or not isinstance(brokers, list): return "-"
    valid=[b for b in brokers if isinstance(b,dict) and (b.get('broker_code') or b.get('broker'))]
    if not valid: return "-"
    try:
        if status in ["DIST","DISTRIB"]:
            # DIST: urutkan net paling negatif dulu
            sorted_b=sorted(valid, key=lambda x: float(x.get('net_value',0) or 0))
        else:
            sorted_b=sorted(valid, key=lambda x: float(x.get('net_value',0) or x.get('buy_value',0) or 0), reverse=True)
    except:
        sorted_b=valid
    parts=[]
    for b in sorted_b[:top]:
        code=b.get('broker_code') or b.get('broker') or "??"
        net=float(b.get('net_value',0) or 0)
        buy=float(b.get('buy_value',0) or 0)
        sell=float(b.get('sell_value',0) or 0)
        # val yang ditampilkan = net, kalau net 0 pakai buy/sell
        if net!=0:
            val=abs(net)
        else:
            val=buy if status=="AKUM" else sell
        if val==0: continue
        if abs(val)>=1e9: s=f"{val/1e9:.1f}B"
        elif abs(val)>=1e6: s=f"{val/1e6:.0f}M"
        else: s=f"{val:.0f}"
        parts.append(f"{code} {s}")
    return ", ".join(parts) if parts else "-"

# ===== ARJUM WRAPPER =====
def arjum_get(path, params=None, use_cache=True):
    ck=make_cache_key(path,params) if use_cache else None
    if use_cache and ck:
        if 'broker' in path:
            c=get_cached_broker(ck)
            if c is not None: return c
        elif 'screener' in path:
            c=get_cached_screener()
            if c is not None: return c
    url=f"{ARJUM_BASE}{path}"
    try:
        headers=get_arjum_headers()
        if not headers.get("X-API-Key"): return None
        r=requests.get(url,headers=headers,params=params,timeout=12)
        if r.status_code==200:
            j=r.json()
            if use_cache and ck:
                if 'broker' in path: set_cached_broker(ck,j)
                elif 'screener' in path: set_cached_screener(j)
            return j
        logging.warning(f"arjum {path} {params} -> {r.status_code}")
        return None
    except Exception as e:
        logging.error(f"arjum_get {path}: {e}")
        return None

def get_screener_latest():
    data=arjum_get("/screener/latest")
    if not data: return []
    if isinstance(data,dict) and 'rows' in data and isinstance(data['rows'],list):
        out=[]
        for r in data['rows']:
            code=r.get('stock_code') or r.get('symbol')
            if code: out.append({'symbol':code.replace(".JK","").upper(),'raw':r})
        return out
    if isinstance(data,dict):
        for k in ['data','results','stocks']:
            if k in data and isinstance(data[k],list): return data[k]
    return data if isinstance(data,list) else []

def get_broker_accumulation_fixed(symbol, top=10, days=None):
    """FIXED: pakai timeline accum_val REAL, bukan sum abs"""
    params={"top":top}
    if days: params["days"]=days; params["period"]=days
    data=arjum_get(f"/broker-accumulation/{symbol}", params=params, use_cache=False)
    if not data: return 0.0, [], 0,0
    raw_brokers=[]; net_period=0; buy_sum=sell_sum=0
    if isinstance(data,dict):
        top_buyers=data.get('top_buyers') or []
        series=data.get('series') or []
        # Case 1: timeline format [{date, accum_val}]
        is_timeline=series and isinstance(series[0],dict) and 'accum_val' in series[0]
        if is_timeline:
            # REAL NET = last - first N days
            if len(series)>=2:
                last=float(series[-1].get('accum_val',0) or 0)
                if days and len(series)>=int(days):
                    n=int(days)
                    first=float(series[-n].get('accum_val',0) or series[0].get('accum_val',0) or 0)
                    # untuk hari pertama, first adalah hari sebelumnya, jadi net = last-first
                    # kalau data series hanya accum hari itu saja, last adalah net hari itu
                    if len(series)>=n+1:
                        # ambil selisih
                        net_period=last-first
                    else:
                        # kalau series pendek, last adalah total kumulatif, pakai selisih dengan 0 atau first
                        net_period=last-first if n>1 else last
                else:
                    net_period=last
                # untuk buy/sell total period, kita tidak punya, pakai net saja
                buy_sum=net_period if net_period>0 else 0
                sell_sum=abs(net_period) if net_period<0 else 0
            # top_buyers tetap dipakai untuk top broker list
            for b in top_buyers[:20]:
                if not isinstance(b,dict): continue
                code=b.get('broker_code') or '??'
                nval=float(b.get('nval',0) or b.get('net_val',0) or 0)
                bval=float(b.get('bval',0) or (nval if nval>0 else 0))
                sval=float(b.get('sval',0) or (abs(nval) if nval<0 else 0))
                raw_brokers.append({"broker_code":str(code).upper(),"broker":str(code).upper(),"buy_value":bval,"sell_value":sval,"net_value":nval,"buy_volume":float(b.get('bvol',0) or 0),"sell_volume":float(b.get('svol',0) or 0),"avg_price":float(b.get('bavg',0) or 0)})
            # Jika top_buyers kosong tapi timeline ada, buat 1 broker ALL
            if not raw_brokers and net_period!=0:
                raw_brokers=[{"broker_code":"ALL","broker":"ALL","buy_value":buy_sum,"sell_value":sell_sum,"net_value":net_period,"buy_volume":0,"sell_volume":0,"avg_price":0}]
            return float(net_period), raw_brokers, float(buy_sum), float(sell_sum)
        # Case 2: per-broker points
        if series and isinstance(series[0],dict) and 'broker_code' in series[0]:
            for ser in series[:20]:
                code=ser.get('broker_code') or '??'
                points=ser.get('points') or []
                if not points: continue
                pts=points[-int(days):] if days and len(points)>=int(days) else points
                sb=sum([float(p.get('bval',0) or 0) for p in pts])
                ss=sum([float(p.get('sval',0) or 0) for p in pts])
                sn=sum([float(p.get('nval',0) or 0) for p in pts])
                if sb==0 and ss==0 and sn==0: continue
                raw_brokers.append({"broker_code":str(code).upper(),"broker":str(code).upper(),"buy_value":float(sb),"sell_value":float(ss),"net_value":float(sn),"buy_volume":float(sum([float(p.get('bvol',0) or 0) for p in pts])),"sell_volume":float(sum([float(p.get('svol',0) or 0) for p in pts])),"avg_price":float(pts[-1].get('bavg',0) or 0) if pts else 0})
                buy_sum+=sb; sell_sum+=ss; net_period+=sn
            return float(net_period), raw_brokers, float(buy_sum), float(sell_sum)
    return 0.0, [], 0,0

def get_broker_summary_fixed(symbol, days=None):
    """FIXED: tidak karang buy/sell 35%, pakai data apa adanya"""
    base_params=[]
    if days:
        try:
            now=get_now_wib()
            end=now
            start=end-datetime.timedelta(days=int(days)*2) if int(days)>1 else end
            base_params.append({"start_date":start.strftime('%Y-%m-%d'),"end_date":end.strftime('%Y-%m-%d'),"broker_limit":30,"flow":"all"})
        except: pass
    base_params.extend([{"broker_limit":30,"flow":"all"},{}])
    data=None; used=None
    for p in base_params:
        d=arjum_get(f"/broker-summary/{symbol}", params=p, use_cache=False)
        if d and isinstance(d,dict):
            has=(d.get('brokers') and len(d.get('brokers'))>0) or d.get('net_value') or d.get('bval') or d.get('nval')
            if has:
                data=d; used=p; break
            if data is None: data=d; used=p
    brokers=[]; net_value=0; buy_total=sell_total=0
    if data and isinstance(data,dict):
        raw=data.get('brokers') or data.get('data') or []
        if not raw and (data.get('buy_value') or data.get('net_value') or data.get('bval') or data.get('nval')):
            bval=float(data.get('buy_value') or data.get('bval') or 0)
            sval=float(data.get('sell_value') or data.get('sval') or 0)
            nval=float(data.get('net_value') or data.get('nval') or (bval-sval))
            # FIX: jangan karang 0.35
            if bval==0 and sval==0 and nval!=0:
                if nval>0: bval=nval; sval=0
                else: sval=abs(nval); bval=0
            brokers=[{"broker_code":"ALL","broker":"ALL","buy_value":bval,"sell_value":sval,"net_value":nval,"buy_volume":float(data.get('buy_volume',0) or 0),"sell_volume":float(data.get('sell_volume',0) or 0),"avg_price":float(data.get('avg_price',0) or 0)}]
            net_value=nval; buy_total=bval; sell_total=sval
        elif raw:
            for b in raw[:30]:
                if not isinstance(b,dict): continue
                code=b.get('broker_code') or b.get('code') or '??'
                bval=float(b.get('bval') or b.get('buy_value') or 0)
                sval=float(b.get('sval') or b.get('sell_value') or 0)
                nval=float(b.get('nval') or b.get('net_value') or 0)
                if bval==0 and sval==0 and nval!=0:
                    if nval>0: bval=nval; sval=0
                    else: sval=abs(nval); bval=0
                if nval==0 and (bval!=0 or sval!=0):
                    nval=bval-sval
                brokers.append({"broker_code":str(code).upper(),"broker":str(code).upper(),"buy_value":bval,"sell_value":sval,"net_value":nval,"buy_volume":float(b.get('bvol',0) or 0),"sell_volume":float(b.get('svol',0) or 0),"avg_price":float(b.get('bavg',0) or 0)})
            net_value=sum([x['net_value'] for x in brokers])
            buy_total=sum([x['buy_value'] for x in brokers])
            sell_total=sum([x['sell_value'] for x in brokers])
    # fallback ke accumulation jika summary kosong
    if net_value==0 and not brokers:
        acc_net, acc_brokers, acc_buy, acc_sell = get_broker_accumulation_fixed(symbol, top=10, days=days)
        if acc_net!=0:
            net_value=acc_net; brokers=acc_brokers; buy_total=acc_buy; sell_total=acc_sell
    status=determine_status(net_value, buy_total, sell_total)
    if used and data:
        set_cached_broker(make_cache_key(f"/broker-summary/{symbol}",used), data)
    return float(net_value), status, brokers, float(buy_total), float(sell_total)

def calculate_bandars_avg(brokers, hist_df=None, period_days=None):
    try:
        if brokers:
            vals=[float(b.get('avg_price',0)) for b in brokers if b.get('avg_price') and float(b.get('avg_price'))!=0 and float(b.get('net_value',0))>0]
            if vals: return float(np.mean(vals))
    except: pass
    try:
        if hist_df is not None and len(hist_df)>=1:
            df_slice=hist_df.tail(period_days) if period_days else hist_df.tail(1)
            if len(df_slice)>0 and df_slice['Volume'].sum()>0:
                return float((df_slice['Close']*df_slice['Volume']).sum()/df_slice['Volume'].sum())
            return float(df_slice['Close'].iloc[-1])
    except: pass
    return 0

def get_broker_multi_tf_fixed(symbol, hist_df=None):
    """MTF FIXED REAL"""
    cache_key=f"multi_{symbol}"
    cached=get_cached_broker(cache_key)
    if cached and hist_df is None:
        # cek fake same net: jika D=5D=20D persis sama -> re-fetch
        nd=cached.get('net_d',0); n5=cached.get('net_5d',0); n20=cached.get('net_20d',0)
        if not (nd!=0 and abs(nd-n5)<1e6 and abs(nd-n20)<1e6):
            return cached

    # 1D - pakai accumulation timeline untuk net REAL
    net_d_acc, brokers_acc_d, buy_d_acc, sell_d_acc = get_broker_accumulation_fixed(symbol, top=20, days=1)
    net_d_sum, status_d_sum, brokers_sum_d, buy_d_sum, sell_d_sum = get_broker_summary_fixed(symbol, days=1)
    # pilih yang net paling besar absolute (lebih lengkap)
    if abs(net_d_acc) >= abs(net_d_sum) and net_d_acc!=0:
        net_d=net_d_acc; buy_d=buy_d_acc; sell_d=sell_d_acc; brokers_d=brokers_acc_d
    else:
        net_d=net_d_sum; buy_d=buy_d_sum; sell_d=sell_d_sum; brokers_d=brokers_sum_d
    status_d=determine_status(net_d, buy_d, sell_d)

    # 5D
    net_5_acc, brokers_acc_5, buy_5_acc, sell_5_acc = get_broker_accumulation_fixed(symbol, top=20, days=5)
    net_5_sum, status_5_sum, brokers_sum_5, buy_5_sum, sell_5_sum = get_broker_summary_fixed(symbol, days=5)
    if abs(net_5_acc) >= abs(net_5_sum) and net_5_acc!=0:
        net_5d=net_5_acc; buy_5d=buy_5_acc; sell_5d=sell_5_acc; brokers_5d=brokers_acc_5
    else:
        net_5d=net_5_sum; buy_5d=buy_5_sum; sell_5d=sell_5_sum; brokers_5d=brokers_sum_5
    status_5d=determine_status(net_5d, buy_5d, sell_5d)

    # 20D
    net_20_acc, brokers_acc_20, buy_20_acc, sell_20_acc = get_broker_accumulation_fixed(symbol, top=20, days=20)
    net_20_sum, status_20_sum, brokers_sum_20, buy_20_sum, sell_20_sum = get_broker_summary_fixed(symbol, days=20)
    if abs(net_20_acc) >= abs(net_20_sum) and net_20_acc!=0:
        net_20d=net_20_acc; buy_20d=buy_20_acc; sell_20d=sell_20_acc; brokers_20d=brokers_acc_20
    else:
        net_20d=net_20_sum; buy_20d=buy_20_sum; sell_20d=sell_20_sum; brokers_20d=brokers_sum_20
    status_20d=determine_status(net_20d, buy_20d, sell_20d)

    # VSA fallback jika broker net 0 (weekend)
    vsa_1d=vsa_5d=vsa_20d=0
    if hist_df is not None and len(hist_df)>=5:
        try:
            if 'Net_Val_VSA' not in hist_df.columns:
                hist_df,_=calculate_vsa_metrics_fixed(hist_df)
            vsa_1d=float(hist_df['Net_Val_VSA'].iloc[-1])
            vsa_5d=float(hist_df['Net_Val_VSA'].tail(5).sum())
            vsa_20d=float(hist_df['Net_Val_VSA'].tail(20).sum())
            # jika broker net 0, pakai VSA sebagai proxy
            if net_d==0 and abs(vsa_1d)>1e8:
                net_d=vsa_1d
                status_d=determine_status(net_d, buy_d, sell_d, min_trx=100_000_000)
            if net_5d==0 and abs(vsa_5d)>5e8:
                net_5d=vsa_5d
                status_5d=determine_status(net_5d, buy_5d, sell_5d, min_trx=100_000_000)
        except: pass

    avg_d=calculate_bandars_avg(brokers_d, hist_df, 1)
    avg_5d=calculate_bandars_avg(brokers_5d, hist_df, 5)
    avg_20d=calculate_bandars_avg(brokers_20d, hist_df, 20)

    result={
        "accum_d": float(abs(net_d)), "accum_5d": float(abs(net_5d)), "accum_20d": float(abs(net_20d)),
        "buy_d": float(buy_d), "sell_d": float(sell_d), "buy_5d": float(buy_5d), "sell_5d": float(sell_5d),
        "buy_20d": float(buy_20d), "sell_20d": float(sell_20d),
        "net_d": float(net_d), "net_5d": float(net_5d), "net_20d": float(net_20d),
        "avg_d": float(avg_d), "avg_5d": float(avg_5d), "avg_20d": float(avg_20d),
        "brokers": brokers_d, "brokers_5d": brokers_5d, "brokers_20d": brokers_20d,
        "status": status_d, "status_d": status_d, "status_5d": status_5d, "status_20d": status_20d,
        "vsa_1d": vsa_1d, "vsa_5d": vsa_5d, "vsa_20d": vsa_20d
    }
    if not (buy_d==0 and sell_d==0 and net_d==0 and len(brokers_d)==0):
        set_cached_broker(cache_key, result)
    print(f"✅ REAL MTF {symbol}: D={status_d} Net {net_d/1e9:.2f}B (B{buy_d/1e9:.1f} S{sell_d/1e9:.1f}) | 5D={status_5d} {net_5d/1e9:.2f}B | 20D={status_20d} {net_20d/1e9:.2f}B")
    return result

# ===== SISANYA SAMA - SCAN, CHART, TELEGRAM =====

# Aliases for compatibility with original listener (minimal fix - keep original names pointing to fixed logic)
def get_broker_accumulation(symbol, top=10, days=None):
    net, brokers, buy, sell = get_broker_accumulation_fixed(symbol, top=top, days=days)
    return net, brokers

def get_broker_summary(symbol, days=None):
    net, status, brokers, buy, sell = get_broker_summary_fixed(symbol, days=days)
    return net, status, brokers, buy, sell

def get_broker_multi_tf(symbol, hist_df=None):
    return get_broker_multi_tf_fixed(symbol, hist_df=hist_df)

def format_top_brokers(brokers, top=3, status="AKUM"):
    return format_top_brokers_fixed(brokers, top=top, status=status)

def calculate_vsa_metrics(df):
    return calculate_vsa_metrics_fixed(df)


def is_market_open():
    now=get_now_wib(); wd=now.weekday()
    if wd>=5: return False
    ct=now.time()
    if wd==4:
        return (datetime.time(9,0)<=ct<=datetime.time(11,30)) or (datetime.time(14,0)<=ct<=datetime.time(15,50))
    else:
        return (datetime.time(9,0)<=ct<=datetime.time(12,0)) or (datetime.time(13,30)<=ct<=datetime.time(15,50))

def get_history_pro(symbol, limit=150, timeframe="1d"):
    hk=f"{symbol}_{timeframe}_{limit}"
    ch=get_cached_history(hk)
    if ch is not None: return ch
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
            yf_map={"1m":("7d","1m"),"5m":("5d","5m"),"15m":("5d","15m"),"30m":("1mo","30m"),"1h":("1mo","60m"),"4h":("3mo","90m"),"1d":("6mo","1d"),"1w":("1y","1wk")}
            period,interval=yf_map.get(tf,("6mo","1d"))
            hist=yf.Ticker(f"{symbol}.JK").history(period=period,interval=interval,timeout=10)
            if hist is not None and len(hist)>10:
                set_cached_history(hk, hist.tail(limit))
                return hist.tail(limit)
        except Exception as e:
            logging.warning(f"yfinance {symbol} {tf}: {e}")
            return None
    try:
        df=pd.DataFrame(rows)
        rm={}
        for c in df.columns:
            cl=str(c).lower()
            if cl in ['o','open']: rm[c]='Open'
            elif cl in ['h','high']: rm[c]='High'
            elif cl in ['l','low']: rm[c]='Low'
            elif cl in ['c','close','close_price']: rm[c]='Close'
            elif cl in ['v','volume','vol']: rm[c]='Volume'
            elif cl in ['date','time','t','datetime','timestamp']: rm[c]='Date'
        df.rename(columns=rm,inplace=True)
        if 'Date' in df.columns:
            df['Date']=pd.to_datetime(df['Date']); df.set_index('Date',inplace=True)
        df=df.sort_index()
        for col in ['Open','High','Low','Close','Volume']:
            if col in df.columns: df[col]=pd.to_numeric(df[col],errors='coerce')
        df=df.dropna(subset=['Close'])
        if len(df)<10: return None
        set_cached_history(hk, df)
        return df
    except Exception as e:
        logging.error(f"History parse {symbol}: {e}")
        return None

def detect_buy_signals(df,multi_tf=None):
    signals=[]
    if df is None or len(df)<30: return signals,df
    df=df.copy()
    df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
    df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
    df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
    df['ATR']=calculate_atr(df,14)
    df,_=calculate_vsa_metrics_fixed(df)
    net_5d=multi_tf.get('net_5d',0) if multi_tf else df['Net_Val_VSA'].tail(5).sum()
    for i in range(20,len(df)):
        close=df['Close'].iloc[i]; ema50=df['EMA50'].iloc[i]; ema20=df['EMA20'].iloc[i]
        vol=df['Volume'].iloc[i]; v1=df['V1'].iloc[i]
        prev_close=df['Close'].iloc[i-1]; prev_ema50=df['EMA50'].iloc[i-1]
        is_bo=prev_close<=prev_ema50 and close>ema50 and close>ema20
        vol_spike=vol>v1*1.5 if v1>0 else False
        if is_bo and vol_spike and close>=df['Open'].iloc[i] and net_5d>0:
            signals.append({'index':i,'date':df.index[i],'type':'BO EMA50','side':'BUY','entry':float(close),'sl':float(min(df['Low'].iloc[max(0,i-5):i+1].min(), close-df['ATR'].iloc[i]*1.2)),'reason':f'BO EMA50 Vol {vol/v1:.1f}x Net 5D AKUM','strength':90})
    return signals,df

def detect_sell_signals(df,multi_tf=None):
    return [],df

def calculate_trading_plan(df,signals=None,multi_tf=None):
    try:
        if df is None or len(df)<20: return None
        last_close=df['Close'].iloc[-1]
        atr=calculate_atr(df,14).iloc[-1]
        if pd.isna(atr) or atr==0: atr=last_close*0.03
        ema20=df['Close'].ewm(span=20).mean().iloc[-1]
        ema50=df['Close'].ewm(span=50).mean().iloc[-1]
        ema200=df['Close'].ewm(span=200).mean().iloc[-1]
        buy_sigs,_=detect_buy_signals(df,multi_tf)
        signals=buy_sigs
        if buy_sigs and buy_sigs[-1]['index']>=len(df)-10:
            last=buy_sigs[-1]
            entry=last['entry']; sl=last['sl']; signal_type=last['type']; reason=last['reason']; strength=last['strength']; side="BUY"
        else:
            entry=round_to_ihsg_fraction(last_close)
            sl=round_to_ihsg_fraction(max(df['Low'].tail(5).min(), last_close-atr*1.5))
            signal_type="NO SIGNAL"; reason="Tunggu BO EMA50"; strength=0; side="WAIT"
        min_sl=last_close*0.92; max_sl=last_close*0.98
        sl=max(min(sl,max_sl),min_sl); sl=round_to_ihsg_fraction(sl)
        tp1=round_to_ihsg_fraction(entry+atr*1.5); tp2=round_to_ihsg_fraction(entry+atr*3.0)
        risk=abs(entry-sl); rr1=abs(tp1-entry)/risk if risk>0 else 0; rr2=abs(tp2-entry)/risk if risk>0 else 0
        if last_close>ema20 and last_close>ema50 and last_close>ema200: trend="STRONG UPTREND"
        elif last_close>ema20 and last_close>ema50: trend="UPTREND"
        elif last_close>ema20: trend="WEAK UPTREND"
        elif last_close<ema20 and last_close<ema50 and last_close<ema200: trend="STRONG DOWNTREND"
        else: trend="DOWNTREND"
        mtf_confirm="NEUTRAL"
        if multi_tf:
            s5=multi_tf.get('status_5d','NEUTRAL'); s20=multi_tf.get('status_20d','NEUTRAL')
            if s5=="AKUM" and s20=="AKUM": mtf_confirm="STRONG BULLISH MTF"
            elif s5=="AKUM" or s20=="AKUM": mtf_confirm="BULLISH MTF"
            elif s5=="DIST" and s20=="DIST": mtf_confirm="BEARISH MTF"
        trend_mtf=f"{trend} + {mtf_confirm}" if mtf_confirm!="NEUTRAL" else trend
        if "DOWNTREND" in trend and last_close<ema20 and side=="BUY":
            side="WAIT"; signal_type="NO SIGNAL"; reason=f"WAIT - {trend} Close {last_close:.0f} < EMA20 {ema20:.0f}"; strength=0
        return {"entry":int(entry),"sl":int(sl),"tp1":int(tp1),"tp2":int(tp2),"atr":float(atr),"risk_pct":round((risk/entry)*100,2) if entry else 0,"rr1":round(rr1,2),"rr2":round(rr2,2),"trend":trend_mtf,"support":int(df['Low'].tail(10).min()),"resistance":int(df['High'].tail(10).max()),"signal_type":signal_type,"signal_reason":reason,"signal_strength":strength,"side":side,"is_buy_signal":side=="BUY" and strength>=70,"buy_signals":buy_sigs,"mtf_confirm":mtf_confirm}
    except Exception as e:
        logging.error(f"TP error: {e}")
        return None

def generate_pro_chart(df,symbol="BBCA",timeframe="1d",sector_info="IHSG",output_filename="chart.png",extra_info=None):
    try:
        extra_info=extra_info or {}
        df=df.copy().ffill().bfill()
        if not isinstance(df.index,pd.DatetimeIndex): df.index=pd.to_datetime(df.index)
        df=df.sort_index()
        df['EMA13']=df['Close'].ewm(span=13,adjust=False).mean()
        df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
        df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
        df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        df,_=calculate_vsa_metrics_fixed(df)
        last_close=df['Close'].iloc[-1]; prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close
        chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
        buy_pct=int(df['Buy_Pct'].iloc[-1]); net_vol=df['Net_Vol_VSA'].iloc[-1]
        vchg1=df['Volume'].iloc[-1]/df['Volume'].iloc[-2] if len(df)>1 and df['Volume'].iloc[-2]>0 else 1
        power="TURBO" if buy_pct>=85 and vchg1>=1.2 else "STRONG" if buy_pct>=70 or vchg1>=1.5 else "NORMAL" if buy_pct>=60 else "WEAK"
        plt.style.use('dark_background')
        fig=plt.figure(figsize=(16,9),dpi=180,facecolor='#000000')
        gs=gridspec.GridSpec(4,1,height_ratios=[4.5,1.1,0.9,0.8],hspace=0.05)
        ax_main=fig.add_subplot(gs[0]); ax_vol=fig.add_subplot(gs[1],sharex=ax_main)
        ax_nbsa=fig.add_subplot(gs[2],sharex=ax_main); ax_mm=fig.add_subplot(gs[3],sharex=ax_main)
        fig.subplots_adjust(left=0.08,right=0.92,top=0.88,bottom=0.06)
        for ax in [ax_main,ax_vol,ax_nbsa,ax_mm]:
            ax.set_facecolor('#000000'); ax.tick_params(colors='#aaaaaa',labelsize=8); ax.yaxis.tick_right(); ax.grid(False)
        x=np.arange(len(df))
        multi=extra_info.get('multi_tf')
        buy_sigs,_=detect_buy_signals(df,multi)
        extra_info['_chart_buy_signals']=buy_sigs
        for i in range(len(df)):
            o,h,l,c=df['Open'].iloc[i],df['High'].iloc[i],df['Low'].iloc[i],df['Close'].iloc[i]
            ax_main.plot([i,i],[l,h],color='#00ff00' if c>=o else '#ff3333',linewidth=0.8,alpha=0.8)
            body_low=min(o,c); body_h=max(0.5,abs(c-o))
            rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor='none' if c>=o else '#ff3333',edgecolor='#00ff00' if c>=o else '#ff3333',linewidth=0.8)
            ax_main.add_patch(rect)
        ax_main.plot(x,df['EMA13'],color='#ffff00',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA20'],color='#ff0000',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA50'],color='#ffffff',linewidth=1.0,alpha=0.9)
        ax_main.plot(x,df['EMA200'],color='#a020f0',linewidth=1.2,alpha=0.9)
        for sig in buy_sigs:
            idx=sig['index']
            if idx<len(df):
                low=df['Low'].iloc[idx]; atr=df['Close'].iloc[idx]*0.02
                ax_main.annotate('▲',xy=(idx,low-atr*0.6),fontsize=14,color='#00ff00',fontweight='bold',ha='center')
        ax_main.set_xlim(-1,len(df)); ax_main.set_ylim(df['Low'].min()*0.95,df['High'].max()*1.08)
        left_text=f"Avg:{df['Close'].tail(20).mean():,.0f}\nVchg1D:{vchg1:.1f}x\nPower:{power}\nBuy%:{buy_pct}%\nEMA13:{df['EMA13'].iloc[-1]:,.0f}\nEMA20:{df['EMA20'].iloc[-1]:,.0f}\nEMA50:{df['EMA50'].iloc[-1]:,.0f}"
        ax_main.text(0.01,0.98,left_text,transform=ax_main.transAxes,va='top',fontsize=8,family='monospace',color='#e0e0e0',bbox=dict(facecolor='black',alpha=0.6,edgecolor='none'))
        fig.text(0.01,0.96,f"{symbol} : {last_close:.0f} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=13,fontweight='bold',ha='left')
        fig.text(0.5,0.96,"RAFANO TRADER V3.2 FIXED",color='white',fontsize=14,fontweight='bold',ha='center')
        fig.text(0.99,0.96,f"{timeframe.upper()} {df.index[-1].strftime('%d %b %Y')}",color='#ffcc00',fontsize=10,ha='right')
        vol_info=f"Buy%={buy_pct}% NetVol={net_vol:,.0f} Power={power}"
        ax_vol.text(0.005,0.88,vol_info,transform=ax_vol.transAxes,color='#ffffff',fontsize=8,va='top')
        ax_vol.bar(x,df['Vol_Sell'],color='#cc0000',width=0.8,alpha=0.8)
        ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00cc00',width=0.8,alpha=0.9)
        ax_vol.plot(x,df['V1'],color='white',linewidth=0.8,alpha=0.9)
        ax_vol.set_ylim(0,df['Volume'].max()*1.8); plt.setp(ax_vol.get_xticklabels(),visible=False)
        real_net=extra_info.get('broker_net',0)
        ax_nbsa.text(0.005,0.85,f"NBSA Rp. {abs(real_net)/1e9:.2f}B | {extra_info.get('broker_status','')}",transform=ax_nbsa.transAxes,color='#ffffff',fontsize=8,va='top')
        nbsa_vals=df['Net_Vol_VSA'].tail(80)/(df['Net_Vol_VSA'].abs().max() or 1)*50
        x_nbsa=np.arange(len(df)-len(nbsa_vals),len(df))
        for i,v in zip(x_nbsa,nbsa_vals):
            ax_nbsa.bar(i,v,color='#00ffff' if v>=0 else '#ff4444',width=0.6)
        ax_nbsa.axhline(0,color='#444444',linewidth=0.5)
        if 'MM' not in df.columns: df['MM']=(df['Close']-df['EMA50'])/df['EMA50']*1000
        ax_mm.bar(np.arange(len(df)-80,len(df)), df['MM'].tail(80), color='#cccccc', width=0.5, alpha=0.8)
        ax_mm.text(0.005,0.85,"Market Maker",transform=ax_mm.transAxes,color='#ffffff',fontsize=8,va='top')
        step=max(1,len(df)//8)
        ax_mm.set_xticks(x[::step]); ax_mm.set_xticklabels([df.index[i].strftime('%d %b') for i in range(0,len(df),step)],fontsize=7)
        plt.savefig(output_filename,dpi=180,bbox_inches='tight',facecolor='#000000')
        plt.close(fig)
        return output_filename
    except Exception as e:
        logging.error(f"Chart error {e}",exc_info=True)
        try: plt.close('all')
        except: pass
        return None

# ===== TELEGRAM =====
LAST_SENT_SIGNALS={}; COOLDOWN_SECONDS=3600; LAST_RESET_DATE=""; LAST_SIGNALS_CACHE={}
def filter_signals_with_cooldown(signals):
    global LAST_RESET_DATE, LAST_SENT_SIGNALS
    ct=time.time(); today=get_now_wib().strftime('%Y-%m-%d')
    if LAST_RESET_DATE!=today: LAST_SENT_SIGNALS.clear(); LAST_RESET_DATE=today
    filt=[]
    for sig in signals:
        if (ct-LAST_SENT_SIGNALS.get(sig['symbol'],0))>=COOLDOWN_SECONDS:
            filt.append(sig); LAST_SENT_SIGNALS[sig['symbol']]=ct
    return filt

def get_analysis(symbol):
    d=arjum_get(f"/analysis/{symbol}")
    return d if isinstance(d,dict) else {}

def calculate_score_v2(symbol,hist_df,accum_value,broker_net,analysis_data):
    score=30; reasons=["Screener"]
    if accum_value>20e9: score+=30; reasons.append(f"Akum {accum_value/1e9:.1f}B")
    elif accum_value>5e9: score+=20; reasons.append(f"Akum {accum_value/1e9:.1f}B")
    elif accum_value>0: score+=10
    if broker_net>10e9: score+=20; reasons.append(f"Net {broker_net/1e9:.1f}B")
    elif broker_net>0: score+=10
    try:
        if analysis_data.get('trend')=='BULLISH': score+=20; reasons.append("BULLISH")
        elif hist_df is not None and len(hist_df)>50:
            ema50=hist_df['Close'].ewm(span=50).mean().iloc[-1]
            if hist_df['Close'].iloc[-1]>ema50: score+=15; reasons.append(">EMA50")
    except: pass
    label="VERY STRONG" if score>=85 else "STRONG BUY" if score>=70 else "WEAK BUY" if score>=50 else "NO SIGNAL"
    return score,label,reasons

def scan_v3():
    print(f"[{get_now_wib()}] 🚀 Scan V3.2 REAL...")
    screener=get_screener_latest()
    if not screener:
        candidates=["BBCA","BBRI","BMRI","TLKM","ASII","GOTO","AMMN","BBNI","BRIS","ANTM","MDKA","ADRO","UNTR","ICBP","INDF"]
        is_fallback=True
    else:
        candidates=[x.get('symbol') for x in screener if x.get('symbol')][:25]
        is_fallback=False
    print(f"  Kandidat: {candidates[:10]}")
    detected=[]
    def process(sym):
        try:
            hist=get_history_pro(sym,limit=120,timeframe="1d")
            multi=get_broker_multi_tf_fixed(sym,hist)
            score,label,reasons=calculate_score_v2(sym,hist,multi['accum_d'],multi['net_d'],get_analysis(sym))
            thresh=20 if is_fallback else 40
            if get_now_wib().weekday()>=5: thresh=max(15,thresh-20)
            if score>=thresh:
                last_close=int(hist['Close'].iloc[-1]) if hist is not None else 0
                chg=((hist['Close'].iloc[-1]/hist['Close'].iloc[-2]-1)*100) if hist is not None and len(hist)>=2 else 0
                tp=calculate_trading_plan(hist,multi_tf=multi) if hist is not None else None
                return {"symbol":sym,"close":last_close,"change_pct":chg,"score":score,"score_label":label,"accum_value":multi['accum_d'],"broker_net":multi['net_d'],"broker_status":multi['status'],"reasons":reasons,"history_df":hist,"trading_plan":tp,"brokers":multi['brokers'],"multi_tf":multi}
        except Exception as e:
            logging.error(f"{sym}: {e}")
        return None
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs={ex.submit(process,s):s for s in candidates}
        for f in as_completed(futs):
            r=f.result()
            if r: detected.append(r)
    detected.sort(key=lambda x:x['score'],reverse=True)
    print(f"✅ Scan: {len(detected)} sinyal")
    return detected

def send_reply(chat_id,text,reply_markup=None):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload={"chat_id":chat_id,"text":text,"parse_mode":"Markdown"}
    if reply_markup: payload["reply_markup"]=reply_markup
    try: requests.post(url,json=payload,timeout=10)
    except Exception as e: print(f"TG {e}")

def send_photo_reply(chat_id,photo_path,caption=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path,'rb') as photo:
            requests.post(url,data={'chat_id':chat_id,'caption':caption,'parse_mode':'Markdown'},files={'photo':photo},timeout=30)
    except Exception as e: print(f"Photo {e}")

def broadcast_v3(signals):
    if not signals:
        send_reply(TARGET_CHAT_ID,"V3.2: Tidak ada sinyal REAL AKUM hari ini.")
        return
    now_str=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header=f"*RAFANO V3.2 REAL AKUM/DIST*\n{now_str}\nTotal: {len(signals)}\n============================\n\n"
    msg=header; kb=[]
    for idx,item in enumerate(signals,1):
        multi=item.get('multi_tf') or {}
        daily=f"{multi.get('status_d','')} Net {format_large_number(multi.get('net_d',0),True)} B{format_large_number(multi.get('buy_d',0),True)} S{format_large_number(multi.get('sell_d',0),True)}"
        weekly=f"{multi.get('status_5d','')} Net {format_large_number(multi.get('net_5d',0),True)}"
        top_d=format_top_brokers_fixed(multi.get('brokers',[]),3,multi.get('status_d','AKUM'))
        item_str=f"{idx}. *{item['symbol']}* -- {item['close']} ({item['change_pct']:+.2f}%)\n   |- Score {item['score']}% | {daily}\n   |  └ Top: {top_d}\n   |- {weekly}\n\n"
        kb.append([{"text":f"Chart {item['symbol']}","callback_data":f"chart_{item['symbol']}_1d"}])
        if len(msg)+len(item_str)>3500:
            send_reply(TARGET_CHAT_ID,msg,reply_markup={"inline_keyboard":kb}); msg=item_str; kb=[]
        else: msg+=item_str
    if msg: send_reply(TARGET_CHAT_ID,msg,reply_markup={"inline_keyboard":kb})

def process_chart_request(chat_id,stock_code,timeframe="1d",extra_cache=None):
    send_reply(chat_id,f"📊 *Generating {stock_code.upper()} {timeframe.upper()} REAL...*")
    df=get_history_pro(stock_code,limit=150,timeframe=timeframe)
    if df is None or len(df)<20:
        send_reply(chat_id,f"⚠ Data {stock_code} tidak ketemu TF {timeframe}"); return
    if extra_cache and stock_code in extra_cache:
        extra=extra_cache[stock_code]
    else:
        multi=get_broker_multi_tf_fixed(stock_code,df)
        extra={"accum_value":multi.get('accum_d',0),"broker_net":multi.get('net_d',0),"broker_status":multi.get('status','NEUTRAL'),"brokers":multi.get('brokers',[]),"multi_tf":multi}
    chart_file=f"/tmp/chart_{stock_code.upper()}_{timeframe}_{int(time.time())}.png"
    try:
        fp=generate_pro_chart(df=df,symbol=stock_code.upper(),timeframe=timeframe,sector_info=f"{stock_code.upper()} | IHSG",output_filename=chart_file,extra_info=extra)
        if not fp:
            send_reply(chat_id,f"❌ Gagal render {stock_code}"); return
        multi=extra.get('multi_tf') or {}
        top_d=format_top_brokers_fixed(multi.get('brokers',[]),3,multi.get('status_d','AKUM'))
        top_5d=format_top_brokers_fixed(multi.get('brokers_5d',[]) or multi.get('brokers',[]),3,multi.get('status_5d','AKUM'))
        tp=calculate_trading_plan(df,multi_tf=multi)
        if tp:
            caption=f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])} | {tp['trend']}\nDaily: {multi.get('status_d')} Net {format_large_number(multi.get('net_d',0),True)} B{format_large_number(multi.get('buy_d',0),True)} S{format_large_number(multi.get('sell_d',0),True)}\n  └ {top_d}\nWeekly: {multi.get('status_5d')} Net {format_large_number(multi.get('net_5d',0),True)} | {top_5d}\n{tp['signal_type']} {tp['side']} | Entry {tp['entry']} SL {tp['sl']} TP1 {tp['tp1']} RR {tp['rr1']}"
        else:
            caption=f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])}\nDaily {multi.get('status_d')} Net {format_large_number(multi.get('net_d',0),True)} | {top_d}"
        send_photo_reply(chat_id,fp,caption=caption)
        if os.path.exists(fp): os.remove(fp)
    except Exception as e:
        logging.error(f"Chart req {e}",exc_info=True)
        send_reply(chat_id,f"❌ Error: {e}")


def telegram_bot_listener():
    global LAST_SIGNALS_CACHE
    offset = 0
    print("🤖 Telegram Listener V3 Running...")
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
        print("✅ Webhook deleted, polling mode active")
    except Exception as e:
        print(f"Webhook delete fail: {e}")
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe", timeout=10)
        print(f"✅ Bot Info: {r.json().get('result',{})}")
    except Exception as e:
        print(f"❌ Bot token error: {e}")
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res = requests.get(url, timeout=25)
            if res.status_code != 200:
                print(f"⚠ getUpdates {res.status_code}: {res.text[:200]}")
                time.sleep(3)
                continue
            data = res.json()
            for update in data.get("result", []):
                offset = update["update_id"] + 1
                if "callback_query" in update:
                    cb = update["callback_query"]
                    cb_id = cb.get("id")
                    cb_data = cb.get("data","")
                    chat_id = cb["message"]["chat"]["id"]
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery", json={"callback_query_id": cb_id})
                    if cb_data.startswith("chart_"):
                        parts = cb_data.split("_")
                        if len(parts) >=3:
                            sym = parts[1]
                            tf = parts[2]
                            threading.Thread(target=process_chart_request, args=(chat_id, sym, tf, LAST_SIGNALS_CACHE), daemon=True).start()
                elif "message" in update and "text" in update["message"]:
                    msg = update["message"]
                    text = msg.get("text","").strip()
                    chat_id = msg["chat"]["id"]
                    first_word = text.split()[0].lower() if text else ""
                    print(f"📩 Pesan masuk: {text} dari chat_id={chat_id}")
                    if first_word in ["/start","/help"]:
                        help_msg = (
                            "🤖 *RAFANO V3 PRO FINAL*
"
                            "============================
"
                            "📈 *CHART & ANALISA*
"
                            "`/c <KODE> [TF]` - Chart Pro + Real Akum
"
                            "   `/c BBCA` `/c ANTM 15m` `/c BBCA 1h`
"
                            "`/b <KODE>` - Detail Bandar / Broker
"
                            "`/info <KODE>` - Info lengkap saham
"
                            "`/trend <KODE>` - Analisa trend MTF
"
                            "
"
                            "🔍 *SCREENER*
"
                            "`/scan` - Scan V3 Real Accumulation
"
                            "`/scanpro` - Scan + chart top 3
"
                            "`/top [N] [akum/dist]` - Top akumulasi
"
                            "   `/top 10` `/top 5 dist`
"
                            "`/compare <KODE1> <KODE2>` - Bandingkan 2 saham
"
                            "
"
                            "⭐ *WATCHLIST*
"
                            "`/wl` - Lihat watchlist
"
                            "`/wl add <KODE>` - Tambah watchlist
"
                            "`/wl del <KODE>` - Hapus
"
                            "`/wl scan` - Scan hanya watchlist
"
                            "
"
                            "🛠 *TOOLS*
"
                            "`/clearcache` atau `/cc` - Hapus cache Buy 0
"
                            "`/help` - Menu ini
"
                        )
                        send_reply(chat_id, help_msg)
                    elif first_word in ["/c","/chart","!chart"]:
                        parts = text.split()
                        if len(parts) >=2:
                            sym = parts[1].upper()
                            tf = parts[2] if len(parts)>=3 else "1d"
                            threading.Thread(target=process_chart_request, args=(chat_id, sym, tf, LAST_SIGNALS_CACHE), daemon=True).start()
                        else:
                            send_reply(chat_id, "⚠ Format: `/c <KODE> [TF]`")
                    elif first_word in ["/b","/broker","/bandar"]:
                        parts = text.split()
                        if len(parts) >=2:
                            sym = parts[1].upper()
                            def broker_detail(target_chat, symbol):
                                try:
                                    multi = get_broker_multi_tf(symbol)
                                    net_d, status_d, brokers, buy_d, sell_d = get_broker_summary(symbol)
                                    acc, brokers_acc, b_buy, b_sell = get_broker_accumulation(symbol, top=10)
                                    msg = f"🏦 *BROKER DETAIL {symbol}* -- {get_now_wib().strftime('%d %b %H:%M')}
"
                                    msg += f"Status: {status_d} | Net: {format_large_number(net_d, True)}
"
                                    msg += f"Accum: {format_large_number(acc, True)}

"
                                    if multi:
                                        msg += f"Daily: {multi.get('status_d')} | Buy {format_large_number(multi.get('buy_d',0),True)} Sell {format_large_number(multi.get('sell_d',0),True)} Net {format_large_number(multi.get('net_d',0),True)} Avg {multi.get('avg_d',0):.0f}
"
                                        msg += f"  └ Top: {format_top_brokers(multi.get('brokers',[]),3,multi.get('status_d'))}
"
                                        msg += f"Weekly: {multi.get('status_5d')} | Buy {format_large_number(multi.get('buy_5d',0),True)} Sell {format_large_number(multi.get('sell_5d',0),True)} Net {format_large_number(multi.get('net_5d',0),True)} Avg {multi.get('avg_5d',0):.0f}
"
                                        msg += f"  └ Top: {format_top_brokers(multi.get('brokers_5d',[]) or multi.get('brokers',[]),3,multi.get('status_5d'))}
"
                                        msg += f"Monthly: {multi.get('status_20d')} | Buy {format_large_number(multi.get('buy_20d',0),True)} Sell {format_large_number(multi.get('sell_20d',0),True)} Net {format_large_number(multi.get('net_20d',0),True)} Avg {multi.get('avg_20d',0):.0f}
"
                                        msg += f"  └ Top: {format_top_brokers(multi.get('brokers_20d',[]) or multi.get('brokers',[]),3,multi.get('status_20d'))}

"
                                    msg += "*TOP BROKERS:*
"
                                    for idx, b in enumerate(brokers[:10],1):
                                        code = b.get('broker_code','??')
                                        buy = format_large_number(b.get('buy_value',0), True)
                                        sell = format_large_number(b.get('sell_value',0), True)
                                        net = format_large_number(b.get('net_value',0), True)
                                        emoji = "🟢" if b.get('net_value',0)>0 else "🔴" if b.get('net_value',0)<0 else "⚪"
                                        msg += f"{idx}. {emoji} {code} Buy {buy} Sell {sell} Net {net}
"
                                    send_reply(target_chat, msg)
                                except Exception as e:
                                    send_reply(target_chat, f"❌ Error broker {symbol}: {e}")
                            threading.Thread(target=broker_detail, args=(chat_id, sym), daemon=True).start()
                        else:
                            send_reply(chat_id, "⚠ Format: `/b <KODE>` contoh `/b BBCA`")
                    elif first_word in ["/info","/i"]:
                        parts = text.split()
                        if len(parts) >=2:
                            sym = parts[1].upper()
                            def info_detail(target_chat, symbol):
                                try:
                                    df = get_history_pro(symbol, limit=50, timeframe="1d")
                                    multi = get_broker_multi_tf(symbol, df)
                                    last_close = df['Close'].iloc[-1] if df is not None and len(df)>0 else 0
                                    msg = f"📊 *INFO {symbol}* -- {safe_int(last_close)}
"
                                    msg += f"Time: {get_now_wib().strftime('%d %b %Y %H:%M')}

"
                                    if multi:
                                        msg += f"🏦 Bandar: {multi.get('status_d')} | {multi.get('status_5d')} | {multi.get('status_20d')}
"
                                        msg += f"Daily Net: {format_large_number(multi.get('net_d',0),True)} Avg {multi.get('avg_d',0):.0f}
"
                                        msg += f"Top: {format_top_brokers(multi.get('brokers',[]),3, multi.get('status_d','AKUM'))}

"
                                    if df is not None and len(df)>=20:
                                        df['EMA50'] = df['Close'].ewm(span=50).mean()
                                        ema50 = df['EMA50'].iloc[-1]
                                        trend = "UPTREND" if last_close>ema50 else "DOWNTREND"
                                        msg += f"📈 Trend: {trend} | EMA50: {ema50:.0f}
"
                                        msg += f"High 20D: {df['High'].tail(20).max():.0f} Low 20D: {df['Low'].tail(20).min():.0f}

"
                                    msg += f"Gunakan `/c {symbol}` untuk chart, `/b {symbol}` untuk broker detail"
                                    send_reply(target_chat, msg)
                                except Exception as e:
                                    import traceback; traceback.print_exc()
                                    send_reply(target_chat, f"❌ Error info {symbol}: {e}")
                            threading.Thread(target=info_detail, args=(chat_id, sym), daemon=True).start()
                        else:
                            send_reply(chat_id, "⚠ Format: `/info <KODE>`")
                    elif first_word in ["/trend","/t"]:
                        parts = text.split()
                        if len(parts) >=2:
                            sym = parts[1].upper()
                            def trend_detail(target_chat, symbol):
                                try:
                                    df = get_history_pro(symbol, limit=150, timeframe="1d")
                                    multi = get_broker_multi_tf(symbol, df)
                                    buy_sigs, _ = detect_buy_signals(df, multi)
                                    sell_sigs, _ = detect_sell_signals(df, multi)
                                    tp = calculate_trading_plan(df, signals=buy_sigs+sell_sigs, multi_tf=multi)
                                    msg = f"📈 *TREND MTF {symbol}*

"
                                    if multi:
                                        msg += f"Daily: {multi.get('status_d')} Net {format_large_number(multi.get('net_d',0),True)}
"
                                        msg += f"Weekly: {multi.get('status_5d')} Net {format_large_number(multi.get('net_5d',0),True)}
"
                                        msg += f"Monthly: {multi.get('status_20d')} Net {format_large_number(multi.get('net_20d',0),True)}

"
                                    if tp:
                                        msg += f"Signal: {tp.get('signal_type')} | {tp.get('side')} ({tp.get('signal_strength')}%)
"
                                        msg += f"Trend: {tp.get('trend')}
"
                                        msg += f"MTF Confirm: {tp.get('mtf_confirm')}
"
                                    send_reply(target_chat, msg)
                                except Exception as e:
                                    send_reply(target_chat, f"❌ Error trend {symbol}: {e}")
                            threading.Thread(target=trend_detail, args=(chat_id, sym), daemon=True).start()
                        else:
                            send_reply(chat_id, "⚠ Format: `/trend <KODE>`")
                    elif first_word in ["/top"]:
                        parts = text.split()
                        n = 10
                        filter_status = None
                        if len(parts)>=2:
                            try:
                                n = int(parts[1])
                                if len(parts)>=3:
                                    filter_status = parts[2].upper()
                            except:
                                filter_status = parts[1].upper()
                        def top_accum(target_chat, limit, status_filter):
                            try:
                                sigs = list(LAST_SIGNALS_CACHE.values()) if LAST_SIGNALS_CACHE else scan_v3()
                                def get_net(x):
                                    multi = x.get('multi_tf') or {}
                                    return abs(multi.get('net_d',0) or x.get('broker_net',0) or 0)
                                sorted_sigs = sorted(sigs, key=get_net, reverse=True)
                                if status_filter:
                                    def match_status(s):
                                        st = (s.get('multi_tf',{}).get('status_d','') or s.get('broker_status','') or '').upper()
                                        if status_filter in ["DIST", "DISTRIB", "DISTRIBUSI"]:
                                            return st in ["DIST", "DISTRIB", "DISTRIBUSI"]
                                        elif status_filter in ["AKUM", "ACCUM"]:
                                            return st in ["AKUM", "ACCUM", "AKUMULASI"]
                                        else:
                                            return st == status_filter
                                    sorted_sigs = [s for s in sorted_sigs if match_status(s)]
                                msg = f"🏆 *TOP {limit} {status_filter or 'AKUMULASI'}*

"
                                for idx, item in enumerate(sorted_sigs[:limit],1):
                                    multi = item.get('multi_tf') or {}
                                    sym = item.get('symbol','??')
                                    net = multi.get('net_d',0) or item.get('broker_net',0)
                                    status = multi.get('status_d','') or item.get('broker_status','')
                                    emoji = "🟢" if status=="AKUM" else "🔴" if status=="DIST" else "⚪"
                                    msg += f"{idx}. {emoji} *{sym}* {status} Net {format_large_number(net,True)} | {format_top_brokers(multi.get('brokers',[]) or item.get('brokers',[]),2,status)}
"
                                send_reply(target_chat, msg)
                            except Exception as e:
                                import traceback; traceback.print_exc()
                                send_reply(target_chat, f"❌ Error top: {e}")
                        threading.Thread(target=top_accum, args=(chat_id, n, filter_status), daemon=True).start()
                    elif first_word in ["/compare","/comp"]:
                        parts = text.split()
                        if len(parts)>=3:
                            sym1 = parts[1].upper()
                            sym2 = parts[2].upper()
                            def compare_stocks(target_chat, s1, s2):
                                try:
                                    m1 = get_broker_multi_tf(s1)
                                    m2 = get_broker_multi_tf(s2)
                                    df1 = get_history_pro(s1, limit=20)
                                    df2 = get_history_pro(s2, limit=20)
                                    close1 = df1['Close'].iloc[-1] if df1 is not None else 0
                                    close2 = df2['Close'].iloc[-1] if df2 is not None else 0
                                    msg = f"⚖ *COMPARE {s1} vs {s2}*

"
                                    msg += f"*{s1}* {safe_int(close1)} | {m1.get('status_d')} Net {format_large_number(m1.get('net_d',0),True)}
"
                                    msg += f"  Top: {format_top_brokers(m1.get('brokers',[]),2,m1.get('status_d'))}

"
                                    msg += f"*{s2}* {safe_int(close2)} | {m2.get('status_d')} Net {format_large_number(m2.get('net_d',0),True)}
"
                                    msg += f"  Top: {format_top_brokers(m2.get('brokers',[]),2,m2.get('status_d'))}

"
                                    winner = s1 if abs(m1.get('net_d',0))>abs(m2.get('net_d',0)) else s2
                                    msg += f"🏆 Lebih kuat: *{winner}*"
                                    send_reply(target_chat, msg)
                                except Exception as e:
                                    send_reply(target_chat, f"❌ Error compare: {e}")
                            threading.Thread(target=compare_stocks, args=(chat_id, sym1, sym2), daemon=True).start()
                        else:
                            send_reply(chat_id, "⚠ Format: `/compare BBCA BBRI`")
                    elif first_word in ["/wl","/watchlist"]:
                        parts = text.split()
                        WATCHLIST_FILE = "/tmp/rafano_watchlist.json"
                        def load_wl():
                            try:
                                import json, os
                                if os.path.exists(WATCHLIST_FILE):
                                    with open(WATCHLIST_FILE,'r') as f:
                                        return json.load(f)
                            except: pass
                            return []
                        def save_wl(wl):
                            try:
                                import json
                                with open(WATCHLIST_FILE,'w') as f:
                                    json.dump(wl,f)
                            except: pass
                        if len(parts)==1 or parts[1].lower() in ["list","show"]:
                            wl = load_wl()
                            if not wl:
                                send_reply(chat_id, "⭐ Watchlist kosong. Tambah dengan `/wl add BBCA`")
                            else:
                                msg = f"⭐ *WATCHLIST* ({len(wl)} saham)

"
                                for s in wl: msg += f"• {s}
"
                                send_reply(chat_id, msg)
                        elif parts[1].lower()=="add" and len(parts)>=3:
                            sym = parts[2].upper()
                            wl = load_wl()
                            if sym not in wl:
                                wl.append(sym); save_wl(wl)
                                send_reply(chat_id, f"✅ {sym} ditambah ke watchlist")
                            else:
                                send_reply(chat_id, f"⚠ {sym} sudah ada")
                        elif parts[1].lower() in ["del","remove","rm"] and len(parts)>=3:
                            sym = parts[2].upper()
                            wl = load_wl()
                            if sym in wl:
                                wl.remove(sym); save_wl(wl)
                                send_reply(chat_id, f"🗑 {sym} dihapus")
                            else:
                                send_reply(chat_id, f"⚠ {sym} tidak ada")
                        elif parts[1].lower()=="scan":
                            wl = load_wl()
                            if not wl:
                                send_reply(chat_id, "⭐ Watchlist kosong")
                            else:
                                def scan_wl(target_chat, symbols):
                                    try:
                                        results = []
                                        for sym in symbols:
                                            try:
                                                df = get_history_pro(sym, limit=50)
                                                multi = get_broker_multi_tf(sym, df)
                                                results.append({"symbol":sym, "multi_tf":multi, "close": df['Close'].iloc[-1] if df is not None else 0})
                                            except: pass
                                        results = sorted(results, key=lambda x: abs(x.get('multi_tf',{}).get('net_d',0)), reverse=True)
                                        msg = f"⭐ *WATCHLIST SCAN* ({len(results)})

"
                                        for idx, item in enumerate(results,1):
                                            multi = item.get('multi_tf',{})
                                            msg += f"{idx}. *{item['symbol']}* -- {safe_int(item.get('close',0))} | {multi.get('status_d')} Net {format_large_number(multi.get('net_d',0),True)}
"
                                        send_reply(target_chat, msg)
                                    except Exception as e:
                                        send_reply(target_chat, f"❌ Error wl scan: {e}")
                                threading.Thread(target=scan_wl, args=(chat_id, wl), daemon=True).start()
                    elif first_word in ["/clearcache","/cc","/clear"]:
                        try:
                            BROKER_CACHE.clear(); HISTORY_CACHE.clear(); SCREENER_CACHE.clear(); LAST_SIGNALS_CACHE.clear()
                            send_reply(chat_id, "🧹 Cache cleared, coba `/scan` lagi")
                        except Exception as e:
                            send_reply(chat_id, f"❌ Error clear: {e}")
                    elif first_word in ["/scan","!scan","/scanpro"]:
                        send_reply(chat_id, "🔍 *V3 Scanning Real Accumulation...*")
                        def manual_scan(is_pro=False, target_chat=chat_id):
                            global LAST_SIGNALS_CACHE
                            sigs = scan_v3()
                            LAST_SIGNALS_CACHE = {s['symbol']: s for s in sigs}
                            # FILTER: hanya AKUM untuk scan, biar gak campur DIST
                            filt_akum = [s for s in sigs if (s.get('multi_tf',{}).get('status_d')=='AKUM' or s.get('broker_status')=='AKUM')]
                            # kalau mau lihat DIST, pakai /top dist
                            filt = filt_akum if filt_akum else sigs  # fallback jika tidak ada akum
                            now_str = get_now_wib().strftime('%d %b %Y %H:%M WIB')
                            if not filt:
                                send_reply(target_chat, f"*RAFANO V3* {now_str}
0 sinyal akumulasi")
                                return
                            header = f"*RAFANO V3 PRO - {now_str}*
Total: {len(filt)} (AKUM only)

"
                            msg = header; kb=[]
                            for idx, item in enumerate(filt,1):
                                def fmt(v): return format_large_number(v, True)
                                multi = item.get('multi_tf') or {}
                                daily_str = f"Daily: {multi.get('status_d','')} Net {fmt(multi.get('net_d',0))}"
                                weekly_str = f"Weekly 5D: {multi.get('status_5d','')} Net {fmt(multi.get('net_5d',0))}"
                                top_d = format_top_brokers(multi.get('brokers',[]) or item.get('brokers',[]),2,multi.get('status_d','AKUM'))
                                tp = item.get('trading_plan')
                                tp_line = f"Entry {tp['entry']} TP1 {tp['tp1']} SL {tp['sl']}" if tp else ""
                                item_str = f"{idx}. *{item['symbol']}* -- {item.get('close',0)} ({item.get('change_pct',0):+.2f}%)
   |- {daily_str} | {top_d}
   |- {weekly_str}
   +- {tp_line}

"
                                kb.append([{"text": f"Pro Chart {item['symbol']}", "callback_data": f"chart_{item['symbol']}_1d"}])
                                if len(msg) + len(item_str) > 3500:
                                    send_reply(target_chat, msg, reply_markup={"inline_keyboard": kb})
                                    msg = item_str; kb = []
                                else:
                                    msg += item_str
                            send_reply(target_chat, msg, reply_markup={"inline_keyboard": kb})
                            if is_pro:
                                for top in filt[:3]:
                                    process_chart_request(target_chat, top['symbol'], "1d", LAST_SIGNALS_CACHE)
                                    time.sleep(1)
                        is_pro_flag = (first_word == "/scanpro")
                        threading.Thread(target=manual_scan, args=(is_pro_flag, chat_id), daemon=True).start()
        except Exception as e:
            print(f"Listener error: {e}")
            time.sleep(3)


def auto_screener_loop():
    global LAST_SIGNALS_CACHE
    print("🚀 Auto Screener V3.2...")
    while True:
        try:
            if not is_market_open(): time.sleep(300); continue
            sigs=scan_v3(); LAST_SIGNALS_CACHE={s['symbol']:s for s in sigs}
            filt=filter_signals_with_cooldown(sigs)
            if filt: broadcast_v3(filt)
            time.sleep(600)
        except Exception as e:
            logging.error(f"Auto {e}"); time.sleep(10)

if __name__=="__main__":
    print("==========================================")
    print("🔥 RAFANO V3.2 AKUM/DIST FIXED")
    print("==========================================")
    threading.Thread(target=auto_screener_loop,daemon=True).start()
    telegram_bot_listener()
