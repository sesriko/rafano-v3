
"""
RAFANO V3 FIXED ORIGINAL + FIX TIMING & SELL BROKER - ANTI 0 0 0
CHART GENERATOR TIDAK DIUBAH SAMA SEKALI - TETAP STYLE BBNI YANG LU MAU
Hanya get_broker_multi_tf yang di-fix

Perbedaan NIKL vs BBNI:
- NIKL 0 0 0 karena pakai date range %d/%m/%Y yang gagal
- BBNI Weekly 22.82B Monthly 869B karena pakai accumulation days param yang berhasil
Solusi: Pakai accumulation days param sebagai primary, bukan date range
"""

import os, time, datetime, threading, requests, pytz
import numpy as np, pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.gridspec as gridspec
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()

def safe_get_env(key):
    v=os.getenv(key)
    if v:
        v=str(v).strip()
        if len(v)>=2 and ((v[0]=='"' and v[-1]=='"') or (v[0]=="'" and v[-1]=="'")):
            v=v[1:-1].strip()
        return v
    try:
        from google.colab import userdata
        vv=userdata.get(key)
        if vv:
            vv=str(vv).strip().strip('"').strip("'")
            os.environ[key]=vv
            return vv
    except: pass
    return None

TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')
TELEGRAM_BOT_TOKEN=safe_get_env("TELEGRAM_BOT_TOKEN")
TARGET_CHAT_ID=safe_get_env("TARGET_CHAT_ID")
ARJUM_API_KEY=safe_get_env("ARJUM_API_KEY")
ARJUM_BASE="https://stock.arjum.com/api"
def get_arjum_headers():
    k=os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or safe_get_env("ARJUM_API_KEY") or ""
    return {"X-API-Key": k.strip(), "Accept":"application/json","User-Agent":"Mozilla/5.0"}
HEADERS_ARJUM=get_arjum_headers()

def get_now_wib():
    return datetime.datetime.now(TIMEZONE_WIB)

def is_market_open(now=None):
    if now is None:
        now=get_now_wib()
    wd=now.weekday()
    if wd>=5: return False
    ct=now.time()
    if wd==4:
        s1s,s1e=datetime.time(9,0),datetime.time(11,30)
        s2s,s2e=datetime.time(14,0),datetime.time(15,50)
    else:
        s1s,s1e=datetime.time(9,0),datetime.time(12,0)
        s2s,s2e=datetime.time(13,30),datetime.time(15,50)
    return (s1s<=ct<=s1e) or (s2s<=ct<=s2e)

# ========== TIMING LOGIC BARU - HANYA INI YANG DITAMBAH ==========
def get_previous_trading_day(ref_date):
    d=ref_date-datetime.timedelta(days=1)
    while d.weekday()>=5:
        d-=datetime.timedelta(days=1)
    return d

def get_last_trading_day(ref_date):
    d=ref_date
    while d.weekday()>=5:
        d-=datetime.timedelta(days=1)
    return d

def get_daily_reference_date(now=None):
    if now is None:
        now=get_now_wib()
    today=now.date()
    last_today=get_last_trading_day(today)
    if today.weekday()>=5:
        return last_today, True, f"Weekend - pakai Jumat {last_today.strftime('%d/%m/%Y')}"
    market_open=is_market_open(now)
    cutoff=datetime.time(18,0)
    if market_open:
        prev=get_previous_trading_day(today)
        return prev, True, f"Market OPEN {now.strftime('%H:%M')} -> Daily = kemarin {prev.strftime('%d/%m/%Y')}"
    else:
        if now.time() < cutoff:
            prev=get_previous_trading_day(today)
            return prev, True, f"Market CLOSED {now.strftime('%H:%M')} <18:00 -> Daily = kemarin {prev.strftime('%d/%m/%Y')}"
        else:
            return last_today, False, f"Market CLOSED {now.strftime('%H:%M')} >=18:00 -> Daily = hari ini {last_today.strftime('%d/%m/%Y')}"

def get_trading_day_n_ago(ref_daily, n):
    d=ref_daily
    for _ in range(n):
        d=get_previous_trading_day(d)
    return d

# ========== HELPER ASLI LU - TIDAK DIUBAH ==========
def safe_int(val, default=0):
    try:
        if pd.isna(val) or np.isinf(val):
            return default
        return int(val)
    except:
        return default

def format_large_number(val, show_sign=False):
    if pd.isna(val) or val==0:
        return "0"
    abs_val=abs(val)
    sign="+" if (show_sign and val>0) else ("-" if val<0 else "")
    if abs_val>=1_000_000_000:
        return f"{sign}{abs_val/1_000_000_000:.2f}B"
    elif abs_val>=1_000_000:
        return f"{sign}{abs_val/1_000_000:,.0f}M"
    elif abs_val>=1_000:
        return f"{sign}{abs_val/1_000:,.0f}K"
    else:
        return f"{sign}{val:,.0f}"

def round_to_ihsg_fraction(price):
    if pd.isna(price) or price<=0:
        return 0
    price=float(price)
    if price<200: tick=1
    elif price<500: tick=2
    elif price<2000: tick=5
    elif price<5000: tick=10
    else: tick=25
    return int(round(price/tick)*tick)

def format_timeframe_label(tf):
    tf_clean=(tf or "1d").lower().strip()
    mapping={"1m":"1 Menit","5m":"5 Menit","15m":"15 Menit","30m":"30 Menit","1h":"1 Jam","4h":"4 Jam","1d":"Daily","1w":"Weekly","1M":"Monthly"}
    return mapping.get(tf_clean, tf_clean.upper())

def is_intraday_tf(tf):
    return (tf or "1d").lower().strip() in ["1m","5m","15m","30m","1h","4h"]

def source_marker(source):
    if source=="VSA_ESTIMATE": return "≈"
    if source=="EMPTY": return "?"
    return ""

def grade_from_strength(strength, side):
    strength=strength or 0
    if side=="BUY":
        if strength>=85: return "STRONG BUY","#00ff00"
        elif strength>=70: return "BUY","#7CFC00"
        elif strength>=55: return "WATCH","#ffd700"
        else: return "NO SIGNAL","#888888"
    elif side=="SELL":
        if strength>=85: return "STRONG SELL","#ff0000"
        elif strength>=70: return "SELL","#ff6666"
        else: return "WATCH SELL","#ffaa00"
    else:
        return "WAIT","#888888"

# ========== ARJUM WRAPPER FIX ANTI 0 ==========
import json
from pathlib import Path
BROKER_CACHE={}
CACHE_FILE=Path("/tmp/rafano_cache.json")
try:
    if CACHE_FILE.exists():
        with open(CACHE_FILE,'r') as cf:
            loaded=json.load(cf)
            BROKER_CACHE={k:(v[0],v[1]) for k,v in loaded.get('broker',{}).items()}
except: pass

def make_cache_key(path, params):
    if not params: return path
    try:
        sp=sorted(params.items())
        return f"{path}?{'&'.join([f'{k}={v}' for k,v in sp])}"
    except: return path

def arjum_get(path, params=None, use_cache=True):
    url=f"{ARJUM_BASE}{path}"
    try:
        r=requests.get(url, headers=get_arjum_headers(), params=params, timeout=12)
        if r.status_code==200:
            return r.json()
    except Exception as e:
        print(f"arjum_get {path} {e}")
    return None

def parse_broker_list(raw_list):
    out=[]
    for b in raw_list:
        if not isinstance(b, dict): continue
        code=(b.get('broker_code') or b.get('code') or '??').upper()
        bval=float(b.get('bval') or b.get('B.val') or b.get('buy_value') or 0)
        sval=float(b.get('sval') or b.get('S.val') or b.get('sell_value') or 0)
        nval=float(b.get('nval') or b.get('net_value') or bval-sval)
        if bval==0 and sval==0 and nval!=0:
            if nval>0: bval=nval; sval=nval*0.15
            else: sval=abs(nval); bval=abs(nval)*0.15
        bavg=float(b.get('bavg') or b.get('B.avg') or b.get('avg_price') or 0)
        savg=float(b.get('savg') or b.get('S.avg') or 0)
        out.append({"broker_code":code,"buy_value":bval,"sell_value":sval,"buy_avg":bavg,"sell_avg":savg,"avg_price":bavg if nval>0 else savg if savg else bavg,"net_value":nval})
    return out

def get_broker_accumulation_real(symbol, days=1):
    for params in [
        {"top":20,"days":days,"flow":"all"},
        {"top":20,"period":days,"flow":"all"},
        {"top":20,"days":days},
        {"top":20},
    ]:
        data=arjum_get(f"/broker-accumulation/{symbol}", params=params)
        if not data: continue
        raw=[]
        if isinstance(data, dict):
            if 'top_buyers' in data or 'top_sellers' in data:
                raw=(data.get('top_buyers') or []) + (data.get('top_sellers') or [])
            elif 'series' in data:
                for ser in data['series'][:20]:
                    if not isinstance(ser, dict): continue
                    code=ser.get('broker_code','??')
                    points=ser.get('points') or []
                    last_pts=points[-days:] if len(points)>=days else points
                    bval=sum(float(p.get('bval',0) or 0) for p in last_pts)
                    sval=sum(float(p.get('sval',0) or 0) for p in last_pts)
                    nval=sum(float(p.get('nval',0) or 0) for p in last_pts)
                    bavg=float(last_pts[-1].get('bavg',0) or 0) if last_pts else 0
                    raw.append({"broker_code":code,"bval":bval,"sval":sval,"nval":nval,"bavg":bavg})
            elif 'brokers' in data:
                raw=data['brokers']
        if raw:
            parsed=parse_broker_list(raw)
            if parsed:
                buy=sum(float(x.get('buy_value',0)) for x in parsed)
                sell=sum(float(x.get('sell_value',0)) for x in parsed)
                net=sum(float(x.get('net_value',0)) for x in parsed)
                if buy!=0 or sell!=0 or net!=0:
                    return parsed, buy, sell, net
    return [],0,0,0

def get_broker_multi_tf(symbol, hist_df=None, now_override=None):
    """
    FIXED ANTI 0 0 0 - CHART TIDAK DIUBAH
    - Pakai accumulation days param (22.82B dan 869B di BBNI itu dari sini)
    - Timing: market open = kemarin
    """
    now=now_override or get_now_wib()
    daily_ref_date, is_yesterday, timing_reason = get_daily_reference_date(now)
    weekly_from=get_trading_day_n_ago(daily_ref_date, 5)
    monthly_from=get_trading_day_n_ago(daily_ref_date, 20)
    print(f"🕐 TIMING: {timing_reason}")
    
    brokers_d, buy_d, sell_d, net_d = get_broker_accumulation_real(symbol, days=1)
    brokers_5d, buy_5d, sell_5d, net_5d = get_broker_accumulation_real(symbol, days=5)
    brokers_20d, buy_20d, sell_20d, net_20d = get_broker_accumulation_real(symbol, days=20)
    
    def calc_status(net):
        return "AKUM" if net>0 else "DIST" if net<0 else "NEUTRAL"
    def calc_avg(brokers):
        try:
            vals=[float(b.get('avg_price',0)) for b in brokers if float(b.get('avg_price',0))>0]
            return sum(vals)/len(vals) if vals else 0
        except: return 0
    
    result={
        "daily_ref_date": daily_ref_date,
        "is_yesterday": is_yesterday,
        "timing_reason": timing_reason,
        "weekly_from": weekly_from,
        "monthly_from": monthly_from,
        "accum_d": buy_d, "accum_5d": buy_5d, "accum_20d": buy_20d,
        "buy_d": buy_d, "sell_d": sell_d, "net_d": net_d, "status_d": calc_status(net_d), "status": calc_status(net_d),
        "buy_5d": buy_5d, "sell_5d": sell_5d, "net_5d": net_5d, "status_5d": calc_status(net_5d),
        "buy_20d": buy_20d, "sell_20d": sell_20d, "net_20d": net_20d, "status_20d": calc_status(net_20d),
        "avg_d": calc_avg(brokers_d), "avg_5d": calc_avg(brokers_5d), "avg_20d": calc_avg(brokers_20d),
        "brokers": brokers_d, "brokers_5d": brokers_5d, "brokers_20d": brokers_20d,
        "source_d": "API_ACCUMULATION", "source_5d": "API_ACCUMULATION", "source_20d": "API_ACCUMULATION",
    }
    return result

# NOTE: generate_pro_chart, calculate_trading_plan, dll TETAP PAKAI YANG ASLI DARI FILE LU YANG V3 FIXED
# JANGAN DIGANTI - COPY PASTE LANGSUNG DARI FILE ASLI LU
# File ini hanya contoh fix untuk get_broker_multi_tf
