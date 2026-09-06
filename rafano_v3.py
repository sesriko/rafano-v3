"""
RAFANO V3.7 - BROKER CODE DITAMPILKAN FULL
- /b BBCA -> tampil 15 broker dengan B S Net REAL
- /scan -> tiap saham ada Top 3 broker code + Net
- Chart caption -> ada Top broker code
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
    if v: return str(v).strip().strip('"').strip("'")
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
    return {"X-API-Key": k.strip(), "Accept": "application/json", "User-Agent": "RAFANO/3.7"}
def get_now_wib(): return datetime.datetime.now(TIMEZONE_WIB)

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

def safe_int(val, default=0):
    try:
        if pd.isna(val) or np.isinf(val): return default
        return int(val)
    except: return default
def format_large_number(val, show_sign=False):
    if pd.isna(val) or val==0: return "0"
    abs_val=abs(val); sign="+" if show_sign and val>0 else "-" if val<0 else ""
    if abs_val>=1e9: return f"{sign}{abs_val/1e9:.2f}B"
    if abs_val>=1e6: return f"{sign}{abs_val/1e6:.0f}M"
    if abs_val>=1e3: return f"{sign}{abs_val/1e3:.0f}K"
    return f"{sign}{val:,.0f}"
def round_to_ihsg_fraction(price):
    if pd.isna(price) or price<=0: return 0
    price=float(price)
    if price<200: tick=1
    elif price<500: tick=2
    elif price<2000: tick=5
    elif price<5000: tick=10
    else: tick=25
    return int(round(price/tick)*tick)
def calculate_atr(df, period=14):
    tr1=df['High']-df['Low']; tr2=(df['High']-df['Close'].shift(1)).abs(); tr3=(df['Low']-df['Close'].shift(1)).abs()
    tr=pd.concat([tr1,tr2,tr3],axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=1).mean()

MIN_TRX_THRESHOLD = 500_000_000
def determine_status(net_value, buy_value, sell_value, min_trx=MIN_TRX_THRESHOLD):
    if abs(net_value) < min_trx:
        if max(buy_value, sell_value) < min_trx: return "NEUTRAL"
        if abs(buy_value-sell_value) < min_trx*0.5: return "NEUTRAL"
    if net_value>0: return "AKUM"
    if net_value<0: return "DIST"
    if buy_value > sell_value*1.2: return "AKUM"
    if sell_value > buy_value*1.2: return "DIST"
    return "NEUTRAL"

def calculate_vsa_metrics(df):
    df=df.copy()
    price_range=(df['High']-df['Low']).replace(0,0.1)
    close_pos=(df['Close']-df['Low'])/price_range
    close_pos=np.clip(close_pos,0.02,0.98)
    buy_ratio=close_pos.copy()
    if 'V1' not in df.columns: df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
    vol_ratio=df['Volume']/df['V1'].replace(0,1)
    is_green=df['Close']>=df['Open']
    turbo_mask=(vol_ratio>2.5)&is_green&(close_pos>=0.70)
    strong_mask=(vol_ratio>1.5)&is_green&(close_pos>=0.60)
    buy_ratio=np.where(turbo_mask, np.minimum(0.97, buy_ratio+0.15), buy_ratio)
    buy_ratio=np.where(strong_mask&~turbo_mask, np.minimum(0.95, buy_ratio+0.08), buy_ratio)
    buy_ratio=np.clip(buy_ratio,0.05,0.97)
    df['Vol_Buy']=df['Volume']*buy_ratio
    df['Vol_Sell']=df['Volume']-df['Vol_Buy']
    df['Net_Vol_VSA']=df['Vol_Buy']-df['Vol_Sell']
    df['Net_Val_VSA']=df['Net_Vol_VSA']*df['Close']
    df['Buy_Pct']=buy_ratio*100
    return df, buy_ratio

def detect_buy_signals(df, multi_tf=None):
    signals=[]
    if df is None or len(df)<30: return signals,df
    try:
        df=df.copy()
        df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
        df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        df['ATR']=calculate_atr(df,14)
        df,_=calculate_vsa_metrics(df)
        net_5d=multi_tf.get('net_5d',0) if multi_tf else df['Net_Val_VSA'].tail(5).sum()
        for i in range(20,len(df)):
            close=df['Close'].iloc[i]; open_=df['Open'].iloc[i]
            vol=df['Volume'].iloc[i]; v1=df['V1'].iloc[i]
            ema50=df['EMA50'].iloc[i]; ema20=df['EMA20'].iloc[i]
            prev_close=df['Close'].iloc[i-1]; prev_ema50=df['EMA50'].iloc[i-1]
            is_bo=prev_close<=prev_ema50 and close>ema50 and close>ema20
            vol_spike=vol>v1*1.5 if v1>0 else False
            if is_bo and vol_spike and close>=open_ and net_5d>0:
                signals.append({'index':i,'date':df.index[i],'type':'BO EMA50','side':'BUY','entry':float(close),'sl':float(min(df['Low'].iloc[max(0,i-5):i+1].min(), close-df['ATR'].iloc[i]*1.2)),'reason':f'BO EMA50 Vol {vol/v1:.1f}x','strength':90})
        filtered=[]; last_idx=-20
        for sig in sorted(signals, key=lambda x: x['index']):
            if sig['index']-last_idx>=5:
                filtered.append(sig); last_idx=sig['index']
        return filtered,df
    except: return [],df
def detect_sell_signals(df, multi_tf=None): return [],df
def calculate_trading_plan(df, signals=None, multi_tf=None):
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
            last=buy_sigs[-1]; entry=last['entry']; sl=last['sl']; signal_type=last['type']; reason=last['reason']; strength=last['strength']; side="BUY"
        else:
            entry=round_to_ihsg_fraction(last_close); sl=round_to_ihsg_fraction(max(df['Low'].tail(5).min(), last_close-atr*1.5))
            signal_type="NO SIGNAL"; reason="Tunggu BO EMA50"; strength=0; side="WAIT"
        min_sl=last_close*0.92; max_sl=last_close*0.98
        sl=max(min(sl,max_sl),min_sl); sl=round_to_ihsg_fraction(sl)
        if entry<=sl: entry=round_to_ihsg_fraction(sl*1.03)
        tp1=round_to_ihsg_fraction(entry+atr*1.5); tp2=round_to_ihsg_fraction(entry+atr*3.0)
        risk=abs(entry-sl)
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
            side="WAIT"; signal_type="NO SIGNAL"; reason=f"WAIT - {trend}"; strength=0
        return {"entry":int(entry),"sl":int(sl),"tp1":int(tp1),"tp2":int(tp2),"atr":float(atr),"risk_pct":round((risk/entry)*100,2) if entry else 0,"trend":trend_mtf,"signal_type":signal_type,"signal_reason":reason,"signal_strength":strength,"side":side,"is_buy_signal":side=="BUY" and strength>=70,"buy_signals":buy_sigs,"mtf_confirm":mtf_confirm}
    except: return None

def is_market_open():
    now=get_now_wib(); weekday=now.weekday()
    if weekday>=5: return False
    current_time=now.time()
    if weekday==4:
        s1_start,s1_end=datetime.time(9,0),datetime.time(11,30)
        s2_start,s2_end=datetime.time(14,0),datetime.time(15,50)
    else:
        s1_start,s1_end=datetime.time(9,0),datetime.time(12,0)
        s2_start,s2_end=datetime.time(13,30),datetime.time(15,50)
    return (s1_start<=current_time<=s1_end) or (s2_start<=current_time<=s2_end)

def arjum_get(path, params=None, use_cache=True):
    cache_key=make_cache_key(path,params) if use_cache else None
    if use_cache and cache_key:
        if 'broker' in path:
            cached=get_cached_broker(cache_key)
            if cached is not None: return cached
    url=f"{ARJUM_BASE}{path}"
    try:
        headers=get_arjum_headers()
        if not headers.get("X-API-Key"): return None
        r=requests.get(url,headers=headers,params=params,timeout=15)
        if r.status_code==200:
            j=r.json()
            if use_cache and cache_key and 'broker' in path: set_cached_broker(cache_key,j)
            return j
        else:
            print(f"Arjum {path} {params} -> {r.status_code}")
        return None
    except Exception as e:
        print(f"arjum_get {path}: {e}"); return None

def get_screener_latest():
    data=arjum_get("/screener/latest")
    candidates=[]
    if data:
        if isinstance(data,dict):
            if 'rows' in data and isinstance(data['rows'],list):
                for r in data['rows']:
                    code=r.get('stock_code') or r.get('symbol')
                    if code: candidates.append({'symbol':code.replace(".JK","").upper(),'raw':r})
            else:
                for k in ['data','results','stocks']:
                    if k in data and isinstance(data[k],list):
                        for r in data[k]:
                            code=r.get('stock_code') or r.get('symbol') or r.get('code') if isinstance(r,dict) else r
                            if code and isinstance(code,str): candidates.append({'symbol':code.replace(".JK","").upper(),'raw':r})
                        break
        elif isinstance(data,list):
            for r in data:
                code=r.get('stock_code') or r.get('symbol') if isinstance(r,dict) else r
                if code: candidates.append({'symbol':str(code).replace(".JK","").upper(),'raw':r})
    if len(candidates) < 15:
        fallback=["BBCA","BBRI","BMRI","TLKM","ASII","GOTO","AMMN","ADRO","ANTM","MDKA","BBNI","BRIS","UNTR","ICBP","INDF","BRPT","TPIA","CUAN","PTRO","BREN","AMRT","KLBF","CPIN","INCO","MBMA","ESSA","FILM","WIFI","DEWA","BULL","RAJA","CDIA","COIN"]
        for sym in fallback:
            if sym not in [c['symbol'] for c in candidates]:
                candidates.append({'symbol':sym,'raw':{}})
    seen=set(); uniq=[]
    for c in candidates:
        s=c['symbol']
        if s not in seen:
            seen.add(s); uniq.append(c)
    return uniq[:35]

# BROKER-SUMMARY REAL
def get_broker_summary_real(symbol, days=1):
    now=get_now_wib()
    if days==1:
        start_date = now.strftime('%Y-%m-%d')
        end_date = now.strftime('%Y-%m-%d')
    else:
        delta = int(days*1.5)+2
        start_date = (now - datetime.timedelta(days=delta)).strftime('%Y-%m-%d')
        end_date = now.strftime('%Y-%m-%d')
    params = {"net": False, "broker_limit": 30, "level_limit": 25, "flow": "all", "start_date": start_date, "end_date": end_date}
    data = arjum_get(f"/broker-summary/{symbol}", params=params, use_cache=False)
    if not data or not isinstance(data,dict) or not data.get('brokers'):
        params2 = {"net": False, "broker_limit": 30, "flow": "all"}
        data = arjum_get(f"/broker-summary/{symbol}", params=params2, use_cache=False)
    brokers=[]; buy_total=sell_total=net_total=0
    if data and isinstance(data,dict):
        raw = data.get('brokers') or []
        for b in raw[:30]:
            if not isinstance(b,dict): continue
            code = b.get('broker_code') or b.get('code') or '??'
            bval = float(b.get('bval',0) or b.get('buy_value',0) or 0)
            sval = float(b.get('sval',0) or b.get('sell_value',0) or 0)
            nval = float(b.get('nval',0) or b.get('net_value',0) or (bval - sval))
            if bval==0 and sval==0 and nval!=0:
                if nval>0: bval=nval
                else: sval=abs(nval)
            brokers.append({
                "broker_code": str(code).upper(),
                "broker_name": b.get('broker_name',''),
                "buy_value": bval,
                "sell_value": sval,
                "net_value": nval,
                "buy_volume": float(b.get('bvol',0) or 0),
                "sell_volume": float(b.get('svol',0) or 0),
                "nvol": float(b.get('nvol',0) or 0),
                "bfrq": int(b.get('bfrq',0) or 0),
                "sfrq": int(b.get('sfrq',0) or 0),
            })
            buy_total+=bval; sell_total+=sval; net_total+=nval
    if buy_total==0 and sell_total==0 and net_total!=0:
        if net_total>0: buy_total=net_total
        else: sell_total=abs(net_total)
    status = determine_status(net_total, buy_total, sell_total)
    return float(net_total), status, brokers, float(buy_total), float(sell_total)

def get_broker_summary(symbol, days=None):
    d = days if days else 1
    return get_broker_summary_real(symbol, days=d)

def get_broker_multi_tf(symbol, hist_df=None):
    net_d, status_d, brokers_d, buy_d, sell_d = get_broker_summary_real(symbol, days=1)
    net_5d, status_5d, brokers_5d, buy_5d, sell_5d = get_broker_summary_real(symbol, days=5)
    net_20d, status_20d, brokers_20d, buy_20d, sell_20d = get_broker_summary_real(symbol, days=20)
    vsa_1d=vsa_5d=vsa_20d=0
    if hist_df is not None and len(hist_df)>=5:
        try:
            if 'Net_Val_VSA' not in hist_df.columns: hist_df,_=calculate_vsa_metrics(hist_df)
            vsa_1d=float(hist_df['Net_Val_VSA'].iloc[-1])
        except: pass
    result={
        "net_d": float(net_d), "net_5d": float(net_5d), "net_20d": float(net_20d),
        "buy_d": float(buy_d), "sell_d": float(sell_d),
        "buy_5d": float(buy_5d), "sell_5d": float(sell_5d),
        "buy_20d": float(buy_20d), "sell_20d": float(sell_20d),
        "accum_d": float(abs(net_d)), "accum_5d": float(abs(net_5d)), "accum_20d": float(abs(net_20d)),
        "brokers": brokers_d, "brokers_5d": brokers_5d, "brokers_20d": brokers_20d,
        "status": status_d, "status_d": status_d, "status_5d": status_5d, "status_20d": status_20d,
        "vsa_1d": vsa_1d, "vsa_5d": vsa_5d, "vsa_20d": vsa_20d
    }
    print(f"MTF {symbol}: D={status_d} Net {net_d/1e9:.2f}B B{buy_d/1e9:.1f} S{sell_d/1e9:.1f} | 5D={status_5d} {net_5d/1e9:.2f}B | 20D={status_20d} {net_20d/1e9:.2f}B")
    return result

def format_top_brokers(brokers, top=3, status="AKUM"):
    if not brokers or not isinstance(brokers, list): return "-"
    valid=[b for b in brokers if isinstance(b,dict) and (b.get('broker_code') or b.get('broker'))]
    if not valid: return "-"
    try:
        if status in ["DIST","DISTRIB"]: sorted_b=sorted(valid, key=lambda x: float(x.get('net_value',0) or 0))
        else: sorted_b=sorted(valid, key=lambda x: float(x.get('net_value',0) or 0), reverse=True)
    except: sorted_b=valid
    parts=[]
    for b in sorted_b[:top]:
        code=b.get('broker_code') or b.get('broker') or "??"
        net=float(b.get('net_value',0) or 0)
        val=abs(net) if net!=0 else float(b.get('buy_value',0) or b.get('sell_value',0) or 0)
        if val==0: continue
        if abs(val)>=1e9: s=f"{val/1e9:.1f}B"
        elif abs(val)>=1e6: s=f"{val/1e6:.0f}M"
        else: s=f"{val:.0f}"
        parts.append(f"{code} {s}")
    return ", ".join(parts) if parts else "-"

def format_broker_detailed(brokers, top=15):
    """Tampilkan code broker full dengan B S Net REAL"""
    if not brokers or not isinstance(brokers, list): return "No broker data"
    # sort by net absolute terbesar
    try:
        sorted_b=sorted(brokers, key=lambda x: abs(float(x.get('net_value',0) or 0)), reverse=True)
    except:
        sorted_b=brokers
    lines=[]
    for idx,b in enumerate(sorted_b[:top],1):
        code=b.get('broker_code','??')
        name=b.get('broker_name','')[:25]
        bval=b.get('buy_value',0)
        sval=b.get('sell_value',0)
        nval=b.get('net_value',0)
        # format
        b_str=format_large_number(bval,False)
        s_str=format_large_number(sval,False)
        n_str=format_large_number(nval,True)
        emoji="🟢" if nval>0 else "🔴" if nval<0 else "⚪"
        # nvol dan freq kalau ada
        nvol=b.get('nvol',0)
        bfrq=b.get('bfrq',0)
        lines.append(f"{idx}. {emoji} *{code}* {n_str} | B:{b_str} S:{s_str} | {name}")
    return "\n".join(lines)

def get_analysis(symbol):
    return {"trend":"NEUTRAL"}
def get_history_pro(symbol, limit=150, timeframe="1d"):
    try:
        import yfinance as yf
        hist=yf.Ticker(f"{symbol}.JK").history(period="6mo",interval="1d",timeout=10)
        if hist is not None and len(hist)>10:
            return hist.tail(limit)
    except: pass
    return None

def generate_pro_chart(df, symbol="BBCA", timeframe="1d", sector_info="IHSG", output_filename="chart.png", extra_info=None):
    try:
        extra_info=extra_info or {}
        df=df.copy().ffill().bfill()
        df['EMA13']=df['Close'].ewm(span=13,adjust=False).mean()
        df['EMA20']=df['Close'].ewm(span=20,adjust=False).mean()
        df['EMA50']=df['Close'].ewm(span=50,adjust=False).mean()
        df['EMA200']=df['Close'].ewm(span=200,adjust=False).mean()
        df['V1']=df['Volume'].rolling(20,min_periods=1).mean()
        df, buy_ratios=calculate_vsa_metrics(df)
        last_close=df['Close'].iloc[-1]
        prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close
        chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
        plt.style.use('dark_background')
        fig=plt.figure(figsize=(16,9),dpi=180,facecolor='#000000')
        gs=gridspec.GridSpec(4,1,height_ratios=[4.5,1.1,0.9,0.8],hspace=0.05)
        ax_main=fig.add_subplot(gs[0]); ax_vol=fig.add_subplot(gs[1],sharex=ax_main); ax_nbsa=fig.add_subplot(gs[2],sharex=ax_main); ax_mm=fig.add_subplot(gs[3],sharex=ax_main)
        fig.subplots_adjust(left=0.08,right=0.92,top=0.88,bottom=0.06)
        for ax in [ax_main,ax_vol,ax_nbsa,ax_mm]:
            ax.set_facecolor('#000000'); ax.tick_params(colors='#aaaaaa',labelsize=8); ax.yaxis.tick_right(); ax.grid(False)
        x=np.arange(len(df))
        for i in range(len(df)):
            o,h,l,c=df['Open'].iloc[i],df['High'].iloc[i],df['Low'].iloc[i],df['Close'].iloc[i]
            ax_main.plot([i,i],[l,h],color='#00ff00' if c>=o else '#ff0000',linewidth=0.8,alpha=0.8)
            body_low=min(o,c); body_h=max(0.5,abs(c-o))
            rect=patches.Rectangle((i-0.35,body_low),0.7,body_h,facecolor='none' if c>=o else '#ff3333',edgecolor='#00ff00' if c>=o else '#ff3333',linewidth=0.8)
            ax_main.add_patch(rect)
        ax_main.plot(x,df['EMA13'],color='#ffff00',linewidth=1.0); ax_main.plot(x,df['EMA20'],color='#ff0000',linewidth=1.0)
        ax_main.plot(x,df['EMA50'],color='#ffffff',linewidth=1.0); ax_main.plot(x,df['EMA200'],color='#a020f0',linewidth=1.2)
        ax_main.set_xlim(-1,len(df)); ax_main.set_ylim(df['Low'].min()*0.95,df['High'].max()*1.08)
        fig.text(0.01,0.96,f"{symbol} : {last_close:.0f} ({chg_pct:+.2f}%)",color='#ffff00',fontsize=13,fontweight='bold',ha='left')
        fig.text(0.5,0.96,"RAFANO V3.7 BROKER CODE REAL",color='white',fontsize=14,fontweight='bold',ha='center')
        ax_vol.bar(x,df['Vol_Sell'],color='#cc0000',width=0.8,alpha=0.8); ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00cc00',width=0.8,alpha=0.9)
        plt.savefig(output_filename,dpi=180,bbox_inches='tight',facecolor='#000000')
        plt.close(fig); return output_filename
    except Exception as e:
        print(f"Chart error {e}"); import traceback; traceback.print_exc()
        try: plt.close('all')
        except: pass
        return None

LAST_SENT_SIGNALS={}; COOLDOWN_SECONDS=3600; LAST_RESET_DATE=""; LAST_SIGNALS_CACHE={}
def filter_signals_with_cooldown(signals):
    global LAST_RESET_DATE, LAST_SENT_SIGNALS
    current_time=time.time(); today_str=get_now_wib().strftime('%Y-%m-%d')
    if LAST_RESET_DATE!=today_str: LAST_SENT_SIGNALS.clear(); LAST_RESET_DATE=today_str
    filtered=[]
    for sig in signals:
        sym=sig['symbol']; last_sent=LAST_SENT_SIGNALS.get(sym,0)
        if (current_time-last_sent)>=COOLDOWN_SECONDS:
            filtered.append(sig); LAST_SENT_SIGNALS[sym]=current_time
    return filtered

def calculate_score_v2(symbol, history_df, accum_value, broker_net, analysis_data):
    score=30
    if accum_value>20_000_000_000: score+=30
    elif accum_value>5_000_000_000: score+=20
    elif accum_value>0: score+=10
    if broker_net>10_000_000_000: score+=20
    elif broker_net>0: score+=10
    if score>=85: label="VERY STRONG"
    elif score>=70: label="STRONG BUY"
    elif score>=50: label="WEAK BUY"
    else: label="NO SIGNAL"
    return score,label,[]

def scan_v3():
    print(f"[{get_now_wib()}] Scan V3.7 BROKER CODE REAL...")
    screener_data=get_screener_latest()
    candidates=[item['symbol'] for item in screener_data]
    print(f"  Kandidat: {candidates[:15]} (total {len(candidates)})")
    detected=[]
    def process_symbol(sym):
        try:
            hist_df=get_history_pro(sym,limit=120,timeframe="1d")
            multi=get_broker_multi_tf(sym,hist_df)
            accum_val=multi['accum_d']; broker_net=multi['net_d']; brokers_combined=multi['brokers']
            analysis={}
            score,label,reasons=calculate_score_v2(sym,hist_df,accum_val,broker_net,analysis)
            threshold=20 if get_now_wib().weekday()>=5 else 40
            if score>=threshold:
                last_close=int(hist_df['Close'].iloc[-1]) if hist_df is not None and len(hist_df)>=2 else 0
                prev=hist_df['Close'].iloc[-2] if hist_df is not None and len(hist_df)>=2 else 0
                change_pct=((last_close/prev)-1)*100 if prev else 0
                tp=calculate_trading_plan(hist_df,multi_tf=multi) if hist_df is not None else None
                return {"symbol":sym,"close":last_close,"change_pct":change_pct,"score":score,"score_label":label,"accum_value":accum_val,"broker_net":broker_net,"broker_status":multi['status'],"history_df":hist_df,"trading_plan":tp,"brokers":brokers_combined,"multi_tf":multi}
        except Exception as e:
            print(f"Error {sym}: {e}")
        return None
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures=[executor.submit(process_symbol,s) for s in candidates]
        for f in as_completed(futures):
            res=f.result()
            if res: detected.append(res)
    detected.sort(key=lambda x: x['score'], reverse=True)
    print(f"Scan: {len(detected)} sinyal")
    return detected

def send_reply(chat_id, text, reply_markup=None):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload={"chat_id":chat_id,"text":text,"parse_mode":"Markdown"}
    if reply_markup: payload["reply_markup"]=reply_markup
    try: requests.post(url,json=payload,timeout=10)
    except Exception as e: print(f"TG Error: {e}")
def send_photo_reply(chat_id, photo_path, caption=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path,'rb') as photo:
            requests.post(url,data={'chat_id':chat_id,'caption':caption,'parse_mode':'Markdown'},files={'photo':photo},timeout=30)
    except Exception as e: print(f"Photo Error: {e}")

def broadcast_v3(signals):
    if not signals:
        send_reply(TARGET_CHAT_ID,"V3 Scan: Tidak ada sinyal")
        return
    now_str=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    akum_only=[s for s in signals if s.get('multi_tf',{}).get('status_d')=='AKUM']
    use_signals=akum_only if akum_only else signals
    header=f"*RAFANO V3.7 BROKER CODE REAL*\n{now_str}\nTotal: {len(use_signals)}\n============================\n\n"
    msg=header; keyboard=[]
    for idx,item in enumerate(use_signals,1):
        multi=item.get('multi_tf') or {}
        top_d=format_top_brokers(multi.get('brokers',[]) or item.get('brokers',[]),3,multi.get('status_d','AKUM'))
        # tampilkan code broker detail top 3
        top_detailed=format_broker_detailed(multi.get('brokers',[]), top=3)
        daily_str=f"Daily: {multi.get('status_d','')} Net {format_large_number(multi.get('net_d',0),True)} B{format_large_number(multi.get('buy_d',0),True)} S{format_large_number(multi.get('sell_d',0),True)}"
        item_str=f"{idx}. *{item['symbol']}* -- {item.get('close',0)} ({item.get('change_pct',0):+.2f}%)\n   {daily_str}\n   Top: {top_d}\n\n"
        keyboard.append([{"text":f"Chart {item['symbol']}","callback_data":f"chart_{item['symbol']}_1d"}, {"text":f"Broker {item['symbol']}","callback_data":f"broker_{item['symbol']}"}])
        if len(msg)+len(item_str)>3500:
            send_reply(TARGET_CHAT_ID,msg,reply_markup={"inline_keyboard":keyboard}); msg=item_str; keyboard=[]
        else: msg+=item_str
    if msg: send_reply(TARGET_CHAT_ID,msg,reply_markup={"inline_keyboard":keyboard})

def process_chart_request(chat_id, stock_code, timeframe="1d", extra_info_cache=None):
    send_reply(chat_id,f"Generating {stock_code.upper()} ({timeframe.upper()}) REAL B S...")
    df=get_history_pro(stock_code,limit=150,timeframe=timeframe)
    if df is None or len(df)<20:
        send_reply(chat_id,f"Data {stock_code} tidak ketemu TF {timeframe}"); return
    if extra_info_cache and stock_code in extra_info_cache:
        extra=extra_info_cache[stock_code]
    else:
        multi=get_broker_multi_tf(stock_code,df)
        extra={"broker_net":multi.get('net_d',0),"brokers":multi.get('brokers',[]),"multi_tf":multi}
    chart_file=f"/tmp/chart_{stock_code.upper()}_{timeframe}_{int(time.time())}.png"
    try:
        file_path=generate_pro_chart(df=df,symbol=stock_code.upper(),timeframe=timeframe,output_filename=chart_file,extra_info=extra)
        multi=extra.get('multi_tf') or {}
        top_d=format_top_brokers(multi.get('brokers',[]) or extra.get('brokers',[]),3,multi.get('status_d','AKUM'))
        detailed=format_broker_detailed(multi.get('brokers',[]), top=5)
        tp=calculate_trading_plan(df,multi_tf=multi)
        if tp:
            caption=f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])} | {tp['trend']}\nDaily: {multi.get('status_d')} Net {format_large_number(multi.get('net_d',0),True)} B{format_large_number(multi.get('buy_d',0),True)} S{format_large_number(multi.get('sell_d',0),True)}\nTop: {top_d}\n{tp['signal_type']} {tp['side']} Entry {tp['entry']} SL {tp['sl']}"
        else:
            caption=f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])}\nTop: {top_d}"
        send_photo_reply(chat_id,file_path,caption=caption)
        # kirim detail broker sebagai teks terpisah
        send_reply(chat_id, f"🏦 *BROKER CODE {stock_code.upper()} DETAIL*\n{detailed}")
        if os.path.exists(file_path): os.remove(file_path)
    except Exception as e:
        import traceback; traceback.print_exc(); send_reply(chat_id,f"Gagal render: {e}")

def telegram_bot_listener():
    global LAST_SIGNALS_CACHE
    offset=0
    print("Telegram Listener V3.7 Running...")
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
    except: pass
    while True:
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res=requests.get(url,timeout=25)
            if res.status_code!=200:
                time.sleep(3); continue
            data=res.json()
            for update in data.get("result",[]):
                offset=update["update_id"]+1
                if "callback_query" in update:
                    cb=update["callback_query"]
                    cb_id=cb.get("id"); cb_data=cb.get("data",""); chat_id=cb["message"]["chat"]["id"]
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery", json={"callback_query_id":cb_id})
                    if cb_data.startswith("chart_"):
                        parts=cb_data.split("_")
                        if len(parts)>=3:
                            sym=parts[1]; tf=parts[2]
                            threading.Thread(target=process_chart_request, args=(chat_id,sym,tf,LAST_SIGNALS_CACHE), daemon=True).start()
                    elif cb_data.startswith("broker_"):
                        sym=cb_data.split("_")[1]
                        def broker_detail_cb(target_chat, symbol):
                            try:
                                net_d,status_d,brokers,buy_d,sell_d=get_broker_summary_real(symbol, days=1)
                                multi=get_broker_multi_tf(symbol)
                                detailed=format_broker_detailed(brokers, top=15)
                                msg=f"🏦 *BROKER CODE {symbol} REAL*\nStatus: {status_d} Net {format_large_number(net_d,True)} B{format_large_number(buy_d,True)} S{format_large_number(sell_d,True)}\n\n{detailed}\n\n5D: {multi.get('status_5d')} Net {format_large_number(multi.get('net_5d',0),True)}\n20D: {multi.get('status_20d')} Net {format_large_number(multi.get('net_20d',0),True)}"
                                send_reply(target_chat, msg)
                            except Exception as e:
                                send_reply(target_chat, f"Error broker {symbol}: {e}")
                        threading.Thread(target=broker_detail_cb, args=(chat_id,sym), daemon=True).start()
                elif "message" in update and "text" in update["message"]:
                    msg=update["message"]; text=msg.get("text","").strip(); chat_id=msg["chat"]["id"]
                    first_word=text.split()[0].lower() if text else ""
                    print(f"Pesan: {text} dari {chat_id}")
                    if first_word in ["/start","/help"]:
                        help_msg="🤖 *RAFANO V3.7 BROKER CODE REAL*\n`/c KODE [TF]` Chart + Broker Code\n`/b KODE` Broker Detail 15 broker B S Net\n`/scan` Scan + Top broker code\n`/top` Top akum\n`/clearcache` Clear\n"
                        send_reply(chat_id, help_msg)
                    elif first_word in ["/c","/chart"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper(); tf=parts[2] if len(parts)>=3 else "1d"
                            threading.Thread(target=process_chart_request, args=(chat_id,sym,tf,LAST_SIGNALS_CACHE), daemon=True).start()
                    elif first_word in ["/b","/broker"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            def broker_detail(target_chat, symbol):
                                try:
                                    net_d,status_d,brokers,buy_d,sell_d=get_broker_summary_real(symbol, days=1)
                                    multi=get_broker_multi_tf(symbol)
                                    detailed=format_broker_detailed(brokers, top=15)
                                    msg=f"🏦 *BROKER CODE {symbol} REAL B S Net*\n"
                                    msg+=f"Status: {status_d} | Net: {format_large_number(net_d, True)} | Buy: {format_large_number(buy_d,True)} Sell: {format_large_number(sell_d,True)}\n"
                                    msg+=f"Date: {get_now_wib().strftime('%d %b %H:%M')}\n\n"
                                    msg+=f"{detailed}\n\n"
                                    msg+=f"5D: {multi.get('status_5d')} Net {format_large_number(multi.get('net_5d',0),True)} B{format_large_number(multi.get('buy_5d',0),True)} S{format_large_number(multi.get('sell_5d',0),True)}\n"
                                    msg+=f"20D: {multi.get('status_20d')} Net {format_large_number(multi.get('net_20d',0),True)} B{format_large_number(multi.get('buy_20d',0),True)} S{format_large_number(multi.get('sell_20d',0),True)}\n"
                                    send_reply(target_chat, msg)
                                except Exception as e:
                                    import traceback; traceback.print_exc()
                                    send_reply(target_chat, f"Error broker {symbol}: {e}")
                            threading.Thread(target=broker_detail, args=(chat_id,sym), daemon=True).start()
                    elif first_word in ["/clearcache","/cc"]:
                        BROKER_CACHE.clear(); HISTORY_CACHE.clear(); SCREENER_CACHE.clear(); LAST_SIGNALS_CACHE.clear()
                        send_reply(chat_id, "🧹 Cache cleared")
                    elif first_word in ["/scan","/scanpro","/top"]:
                        send_reply(chat_id, "🔍 *Scanning REAL BROKER CODE...*")
                        def manual_scan(is_pro=False, target_chat=chat_id):
                            global LAST_SIGNALS_CACHE
                            sigs=scan_v3()
                            LAST_SIGNALS_CACHE={s['symbol']: s for s in sigs}
                            akum_only=[s for s in sigs if s.get('multi_tf',{}).get('status_d')=='AKUM']
                            filt=akum_only if akum_only else sigs
                            now_str=get_now_wib().strftime('%d %b %Y %H:%M WIB')
                            if not filt:
                                send_reply(target_chat, f"*RAFANO V3.7* {now_str}\n0 sinyal AKUM"); return
                            header=f"*RAFANO V3.7 BROKER CODE REAL - {now_str}*\nTotal: {len(filt)} (AKUM only)\n\n"
                            msg=header; kb=[]
                            for idx,item in enumerate(filt,1):
                                multi=item.get('multi_tf') or {}
                                top_d=format_top_brokers(multi.get('brokers',[]) or item.get('brokers',[]),3,multi.get('status_d','AKUM'))
                                daily_str=f"Daily: {multi.get('status_d','')} Net {format_large_number(multi.get('net_d',0),True)} B{format_large_number(multi.get('buy_d',0),True)} S{format_large_number(multi.get('sell_d',0),True)} | Top {top_d}"
                                item_str=f"{idx}. *{item['symbol']}* -- {item.get('close',0)} ({item.get('change_pct',0):+.2f}%)\n   {daily_str}\n\n"
                                kb.append([{"text":f"Chart {item['symbol']}","callback_data":f"chart_{item['symbol']}_1d"}, {"text":f"Broker {item['symbol']}","callback_data":f"broker_{item['symbol']}"}])
                                if len(msg)+len(item_str)>3500:
                                    send_reply(target_chat,msg,reply_markup={"inline_keyboard":kb}); msg=item_str; kb=[]
                                else: msg+=item_str
                            send_reply(target_chat,msg,reply_markup={"inline_keyboard":kb})
                        threading.Thread(target=manual_scan, args=(False,chat_id), daemon=True).start()
        except Exception as e:
            print(f"Listener error: {e}"); time.sleep(3)

def auto_screener_loop():
    global LAST_SIGNALS_CACHE
    print("Auto Screener V3.7...")
    while True:
        try:
            if not is_market_open(): time.sleep(300); continue
            sigs=scan_v3(); LAST_SIGNALS_CACHE={s['symbol']: s for s in sigs}
            filt=filter_signals_with_cooldown(sigs)
            if filt: broadcast_v3(filt)
            time.sleep(600)
        except Exception as e:
            print(f"Auto loop error: {e}"); time.sleep(10)

if __name__=="__main__":
    print("==========================================")
    print("RAFANO V3.7 - BROKER CODE DITAMPILKAN FULL")
    print("==========================================")
    threading.Thread(target=auto_screener_loop, daemon=True).start()
    telegram_bot_listener()
