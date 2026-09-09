
"""
RAFANO V4 FINAL - GABUNGAN LENGKAP
Fix dari V3:
- Akum & Distribusi REAL (bukan abs)
- Daily Weekly Monthly REAL via API days param (bukan *1.8)
- Timeframe label di chart menitan
- Chart lebih informatif + Sinyal BUY/STRONG BUY/WATCH/SELL
- TP1 TP2 tidak kembar lagi
- Professional refactor + tetap kompatibel V3
"""
import os, time, datetime, threading, requests, logging, json
import pytz, numpy as np, pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.gridspec as gridspec
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List, Dict
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

# ========== ENV FIX ANTI COLAB JSON ERROR ==========
def safe_get_env(key):
    v = os.getenv(key)
    if v:
        v = str(v).strip()
        if len(v)>=2 and ((v[0]=='"' and v[-1]=='"') or (v[0]=="'" and v[-1]=="'")):
            v = v[1:-1].strip()
        return v
    try:
        from google.colab import userdata
        vv = userdata.get(key)
        if vv:
            vv = str(vv).strip().strip('"').strip("'")
            os.environ[key] = vv
            return vv
    except: pass
    return None

TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')
TELEGRAM_BOT_TOKEN = safe_get_env("TELEGRAM_BOT_TOKEN")
TARGET_CHAT_ID = safe_get_env("TARGET_CHAT_ID")
ARJUM_API_KEY = safe_get_env("ARJUM_API_KEY")
ARJUM_BASE = "https://stock.arjum.com/api"

print(f"🔑 ENV - TOKEN={bool(TELEGRAM_BOT_TOKEN)} CHAT={TARGET_CHAT_ID} ARJUM={len(ARJUM_API_KEY) if ARJUM_API_KEY else 0}")

def get_now_wib():
    return datetime.datetime.now(TIMEZONE_WIB)

def get_arjum_headers():
    k = os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or ""
    return {"X-API-Key": k.strip(), "Accept": "application/json", "User-Agent": "Mozilla/5.0"}

# ========== HELPERS ==========
def safe_int(val, default=0):
    try:
        if pd.isna(val): return default
        return int(val)
    except: return default

def format_large_number(val, show_sign=False):
    if pd.isna(val) or val==0: return "0"
    abs_val = abs(val)
    sign = "+" if (show_sign and val>0) else ("-" if val<0 else "")
    if abs_val >= 1_000_000_000_000: return f"{sign}{abs_val/1e12:.2f}T"
    if abs_val >= 1_000_000_000: return f"{sign}{abs_val/1e9:.2f}B"
    if abs_val >= 1_000_000: return f"{sign}{abs_val/1e6:.0f}M"
    if abs_val >= 1_000: return f"{sign}{abs_val/1e3:.0f}K"
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

def calculate_rsi(series, period=14):
    delta=series.diff()
    gain=delta.where(delta>0,0.0)
    loss=-delta.where(delta<0,0.0)
    avg_gain=gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss=loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs=avg_gain/avg_loss.replace(0,0.00001)
    rsi=100.0-(100.0/(1.0+rs))
    return rsi.fillna(50)

def calculate_atr(df, period=14):
    high,low,close=df['High'],df['Low'],df['Close']
    tr1=high-low
    tr2=(high-close.shift(1)).abs()
    tr3=(low-close.shift(1)).abs()
    tr=pd.concat([tr1,tr2,tr3], axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=1).mean()

def calculate_bollinger_bands(df, period=20, std=2):
    sma=df['Close'].rolling(period).mean()
    stddev=df['Close'].rolling(period).std()
    return sma, sma+(stddev*std), sma-(stddev*std)

def calculate_vsa_metrics(df):
    price_range=(df['High']-df['Low']).replace(0,0.1)
    close_pos=(df['Close']-df['Low'])/price_range
    close_pos=np.clip(close_pos,0.05,0.95)
    buy_ratio=0.30+close_pos*0.60
    if 'V1' in df.columns:
        vol_ratio=df['Volume']/df['V1'].replace(0,1)
        is_green=df['Close']>=df['Open']
        boost=np.where((vol_ratio>1.5)&is_green,0.10,0)
        boost+=np.where((vol_ratio>2.5)&is_green,0.10,0)
        buy_ratio=buy_ratio+boost
    buy_ratio=np.clip(buy_ratio,0.05,0.95)
    df['Vol_Buy']=df['Volume']*buy_ratio
    df['Vol_Sell']=df['Volume']-df['Vol_Buy']
    df['Net_Vol_VSA']=df['Vol_Buy']-df['Vol_Sell']
    df['Net_Val_VSA']=df['Net_Vol_VSA']*df['Close']
    df['Buy_Pct']=buy_ratio*100
    return df, buy_ratio

# ========== MODEL BARU V4 - AKUM & DIST REAL ==========
@dataclass
class BrokerFlow:
    code: str
    buy_val: float
    sell_val: float
    net_val: float
    buy_vol: float=0
    sell_vol: float=0
    avg_price: float=0
    @property
    def is_accum(self): return self.net_val>0
    @property
    def is_dist(self): return self.net_val<0

@dataclass
class MarketFlow:
    total_buy: float
    total_sell: float
    total_net: float
    accum_val: float
    distrib_val: float
    accum_ratio: float
    top_accum: List[BrokerFlow]
    top_dist: List[BrokerFlow]
    all_brokers: List[BrokerFlow]
    status: str

def parse_brokers(raw_list: List[Dict]) -> List[BrokerFlow]:
    out=[]
    for b in raw_list:
        if not isinstance(b, dict): continue
        code=(b.get('broker_code') or b.get('code') or b.get('broker') or '??').upper()
        bval=float(b.get('bval') or b.get('buy_value') or b.get('buy_val') or 0)
        sval=float(b.get('sval') or b.get('sell_value') or b.get('sell_val') or 0)
        nval=float(b.get('nval') or b.get('net_value') or b.get('net_val') or bval-sval)
        if bval==0 and sval==0 and nval!=0:
            if nval>0: bval=nval; sval=nval*0.15
            else: sval=abs(nval); bval=abs(nval)*0.15
        out.append(BrokerFlow(code,bval,sval,nval,float(b.get('bvol') or 0),float(b.get('svol') or 0),float(b.get('bavg') or b.get('avg_price') or 0)))
    return out

def calculate_flow(brokers: List[BrokerFlow]) -> MarketFlow:
    if not brokers:
        return MarketFlow(0,0,0,0,0,50,[],[],[],"NEUTRAL")
    total_buy=sum(b.buy_val for b in brokers)
    total_sell=sum(b.sell_val for b in brokers)
    total_net=sum(b.net_val for b in brokers)
    accum_b=[b for b in brokers if b.is_accum]
    distrib_b=[b for b in brokers if b.is_dist]
    accum_val=sum(b.net_val for b in accum_b)
    distrib_val=abs(sum(b.net_val for b in distrib_b))
    total_abs=accum_val+distrib_val
    ratio=(accum_val/total_abs*100) if total_abs>0 else 50
    if total_net>0 and ratio>55: status="AKUM"
    elif total_net<0 and ratio<45: status="DIST"
    else: status="NEUTRAL"
    top_accum=sorted(accum_b, key=lambda x: x.net_val, reverse=True)[:5]
    top_dist=sorted(distrib_b, key=lambda x: x.net_val)[:5]
    return MarketFlow(total_buy,total_sell,total_net,accum_val,distrib_val,ratio,top_accum,top_dist,brokers,status)

# ========== ARJUM REAL FETCH ==========
BROKER_CACHE={}
HISTORY_CACHE={}
SCREENER_CACHE={}
CACHE_FILE=Path("/tmp/rafano_cache.json")
BROKER_CACHE_TTL=300

def arjum_get(path, params=None, use_cache=True):
    url=f"{ARJUM_BASE}{path}"
    try:
        key=os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or safe_get_env("ARJUM_API_KEY") or ""
        headers={"X-API-Key": key.strip(), "Accept": "application/json", "User-Agent": "Mozilla/5.0"}
        r=requests.get(url, headers=headers, params=params, timeout=12)
        if r.status_code==200:
            return r.json()
    except Exception as e:
        print(f"arjum_get {path} {e}")
    return None

def get_broker_real(symbol: str, days: int) -> MarketFlow:
    for p in [{"days": days, "top": 20, "flow": "all"}, {"period": days, "top": 20}, {"days": days}]:
        data=arjum_get(f"/broker-accumulation/{symbol}", params=p)
        if not data:
            data=arjum_get(f"/broker-summary/{symbol}", params=p)
        if not data: continue
        raw=[]
        if isinstance(data, dict):
            if 'top_buyers' in data or 'top_sellers' in data:
                raw=(data.get('top_buyers') or [])+(data.get('top_sellers') or [])
            elif 'brokers' in data: raw=data['brokers']
            elif 'data' in data: raw=data['data']
            elif 'series' in data:
                for ser in data['series'][:20]:
                    code=ser.get('broker_code','??')
                    points=ser.get('points') or []
                    last=points[-days:] if len(points)>=days else points
                    bval=sum(float(x.get('bval',0) or 0) for x in last)
                    sval=sum(float(x.get('sval',0) or 0) for x in last)
                    nval=sum(float(x.get('nval',0) or 0) for x in last)
                    bavg=float(last[-1].get('bavg',0) or 0) if last else 0
                    raw.append({"broker_code": code, "bval": bval, "sval": sval, "nval": nval, "bavg": bavg})
        if raw:
            return calculate_flow(parse_brokers(raw))
    return calculate_flow([])

def get_multi_tf_real(symbol: str, hist_df=None):
    flow_d=get_broker_real(symbol, days=1)
    flow_w=get_broker_real(symbol, days=5)
    flow_m=get_broker_real(symbol, days=20)
    vsa_1d=vsa_5d=vsa_20d=0
    if hist_df is not None and len(hist_df)>=20:
        if 'Net_Val_VSA' not in hist_df.columns:
            hist_df,_=calculate_vsa_metrics(hist_df)
        vsa_1d=float(hist_df['Net_Val_VSA'].iloc[-1])
        vsa_5d=float(hist_df['Net_Val_VSA'].tail(5).sum())
        vsa_20d=float(hist_df['Net_Val_VSA'].tail(20).sum())
    return {
        "daily": flow_d, "weekly": flow_w, "monthly": flow_m,
        "vsa_1d": vsa_1d, "vsa_5d": vsa_5d, "vsa_20d": vsa_20d,
        "buy_d": flow_d.total_buy, "sell_d": flow_d.total_sell, "net_d": flow_d.total_net, "status_d": flow_d.status, "accum_d": flow_d.accum_val,
        "buy_5d": flow_w.total_buy, "sell_5d": flow_w.total_sell, "net_5d": flow_w.total_net, "status_5d": flow_w.status, "accum_5d": flow_w.accum_val,
        "buy_20d": flow_m.total_buy, "sell_20d": flow_m.total_sell, "net_20d": flow_m.total_net, "status_20d": flow_m.status, "accum_20d": flow_m.accum_val,
        "brokers": flow_d.all_brokers, "brokers_5d": flow_w.all_brokers, "brokers_20d": flow_m.all_brokers,
    }

# ========== SIGNAL & TF LABEL ==========
def get_tf_label(tf: str) -> str:
    tf=tf.lower()
    return {
        "1m": "1 Menit (Scalping - Sangat Agresif)",
        "5m": "5 Menit (Intraday Scalping)",
        "15m": "15 Menit (Intraday)",
        "30m": "30 Menit (Intraday Swing)",
        "1h": "1 Jam (Intraday Swing)",
        "4h": "4 Jam (Swing Pendek)",
        "1d": "Daily / Harian (Swing Trading - REKOMENDASI)",
        "1w": "Weekly / Mingguan (Trend Menengah)",
        "1mo": "Monthly / Bulanan",
        "1M": "Monthly / Bulanan",
    }.get(tf, f"{tf.upper()}")

def classify_signal(score, flow_d: MarketFlow, trend: str):
    if score>=80 and flow_d.status=="AKUM" and "UPTREND" in trend and flow_d.accum_ratio>60:
        return "STRONG BUY", "🟢🟢"
    elif score>=65 and flow_d.status=="AKUM" and flow_d.total_net>0:
        return "BUY", "🟢"
    elif score>=50 and flow_d.status=="AKUM":
        return "WATCH - AKUM TIPIS", "🟡"
    elif flow_d.status=="DIST" and flow_d.distrib_val>flow_d.accum_val*1.2:
        return "SELL - DISTRIBUSI", "🔴"
    elif flow_d.status=="DIST":
        return "WATCH - DISTRIBUSI", "🟠"
    else:
        return "NEUTRAL - WAIT", "⚪"

def format_top_brokers(brokers, top=3, status="AKUM"):
    if not brokers: return "-"
    valid=[b for b in brokers if isinstance(b, (BrokerFlow, dict))]
    parts=[]
    for b in valid[:top]:
        if isinstance(b, BrokerFlow):
            code=b.code; val=b.net_val
        else:
            code=(b.get('broker_code') or b.get('broker') or '??').upper()
            val=float(b.get('net_value') or b.get('nval') or 0)
        if abs(val)>=1e9: s=f"{val/1e9:.1f}B"
        elif abs(val)>=1e6: s=f"{val/1e6:.0f}M"
        else: s=f"{val:.0f}"
        parts.append(f"{code} {s}")
    return ", ".join(parts) if parts else "-"

# ========== TRADING PLAN FIX TP1 TP2 KEMBAR ==========
def calculate_trading_plan(df, signals=None, multi_tf=None):
    try:
        if df is None or len(df)<20: return None
        last_close=df['Close'].iloc[-1]
        atr=calculate_atr(df,14).iloc[-1]
        if pd.isna(atr) or atr==0: atr=last_close*0.03
        ema20=df['Close'].ewm(span=20).mean().iloc[-1]
        ema50=df['Close'].ewm(span=50).mean().iloc[-1]
        ema200=df['Close'].ewm(span=200).mean().iloc[-1]
        # deteksi sinyal simple
        if last_close>ema20 and last_close>ema50: trend="STRONG UPTREND" if last_close>ema200 else "UPTREND"
        elif last_close>ema20: trend="WEAK UPTREND"
        else: trend="DOWNTREND"
        
        entry=round_to_ihsg_fraction(last_close)
        sl=round_to_ihsg_fraction(max(df['Low'].tail(5).min(), last_close-atr*1.5))
        min_sl=last_close*0.92; max_sl=last_close*0.98
        sl=max(min(sl, max_sl), min_sl); sl=round_to_ihsg_fraction(sl)
        
        # FIX: TP1 dan TP2 beda
        tp1=round_to_ihsg_fraction(entry+atr*1.5)
        tp2=round_to_ihsg_fraction(entry+atr*3.0)
        if tp1==tp2: tp2=round_to_ihsg_fraction(entry*1.08)
        
        risk=entry-sl; reward1=tp1-entry; reward2=tp2-entry
        rr1=reward1/risk if risk>0 else 0
        rr2=reward2/risk if risk>0 else 0
        
        mtf_confirm="NEUTRAL"
        if multi_tf:
            sd=multi_tf.get('status_d',''); s5=multi_tf.get('status_5d',''); s20=multi_tf.get('status_20d','')
            if sd=="AKUM" and s5=="AKUM": mtf_confirm="STRONG BULLISH MTF"
            elif sd=="AKUM" or s5=="AKUM": mtf_confirm="BULLISH MTF"
            elif sd=="DIST" and s20=="DIST": mtf_confirm="BEARISH MTF"
        
        return {
            "entry": int(entry), "sl": int(sl), "tp1": int(tp1), "tp2": int(tp2),
            "atr": float(atr), "risk_pct": round((risk/entry)*100,2) if entry else 0,
            "rr1": round(rr1,2), "rr2": round(rr2,2), "trend": f"{trend} + {mtf_confirm}" if mtf_confirm!="NEUTRAL" else trend,
            "support": int(df['Low'].tail(10).min()), "resistance": int(df['High'].tail(10).max()),
            "signal_type": "BOS EMA", "signal_reason": f"Close > EMA20/50 + {mtf_confirm}", "signal_strength": 70,
            "side": "BUY" if last_close>ema50 else "WAIT", "mtf_confirm": mtf_confirm
        }
    except Exception as e:
        print(f"Trading plan error {e}"); return None

# ========== HISTORY REAL ==========
def get_history_pro(symbol, limit=150, timeframe="1d"):
    tf=timeframe.lower().strip()
    arjum_frame_map={"1m":"1min","5m":"5min","15m":"15min","30m":"30min","1h":"1hour","4h":"4hour","1d":"daily","1w":"weekly","1M":"monthly"}
    arjum_frame=arjum_frame_map.get(tf,"daily")
    data=arjum_get(f"/history/{symbol}", params={"limit": limit, "frame": arjum_frame})
    rows=[]
    if data:
        if isinstance(data, dict): rows=data.get('data') or data.get('history') or []
        elif isinstance(data, list): rows=data
    if not rows:
        try:
            import yfinance as yf
            yf_map={"1m":("7d","1m"),"5m":("5d","5m"),"15m":("5d","15m"),"30m":("1mo","30m"),"1h":("1mo","60m"),"4h":("3mo","90m"),"1d":("6mo","1d"),"1w":("1y","1wk")}
            period, interval=yf_map.get(tf,("6mo","1d"))
            hist=yf.Ticker(f"{symbol}.JK").history(period=period, interval=interval, timeout=10)
            if hist is not None and len(hist)>10: return hist.tail(limit)
        except: pass
        return None
    try:
        df=pd.DataFrame(rows)
        rename={}
        for c in df.columns:
            cl=str(c).lower()
            if cl in ['o','open']: rename[c]='Open'
            elif cl in ['h','high']: rename[c]='High'
            elif cl in ['l','low']: rename[c]='Low'
            elif cl in ['c','close']: rename[c]='Close'
            elif cl in ['v','volume']: rename[c]='Volume'
            elif cl in ['date','time']: rename[c]='Date'
        df.rename(columns=rename, inplace=True)
        if 'Date' in df.columns: df['Date']=pd.to_datetime(df['Date']); df.set_index('Date', inplace=True)
        df=df.sort_index()
        for col in ['Open','High','Low','Close','Volume']: df[col]=pd.to_numeric(df[col], errors='coerce')
        df=df.dropna(subset=['Close'])
        return df if len(df)>=10 else None
    except: return None

# ========== CHART GENERATOR V4 - LEBIH INFORMATIF ==========
def generate_pro_chart(df, symbol="BBCA", timeframe="1d", sector_info="IHSG", output_filename="chart.png", extra_info=None):
    try:
        extra_info=extra_info or {}
        tf_desc=get_tf_label(timeframe)
        df=df.copy().ffill().bfill()
        df['EMA13']=df['Close'].ewm(span=13).mean()
        df['EMA20']=df['Close'].ewm(span=20).mean()
        df['EMA50']=df['Close'].ewm(span=50).mean()
        df['EMA200']=df['Close'].ewm(span=200).mean()
        df['V1']=df['Volume'].rolling(20, min_periods=1).mean()
        df['V2']=df['Volume'].rolling(50, min_periods=1).mean()
        df,_=calculate_vsa_metrics(df)
        last_close=df['Close'].iloc[-1]; last_open=df['Open'].iloc[-1]; last_high=df['High'].iloc[-1]; last_low=df['Low'].iloc[-1]; last_vol=df['Volume'].iloc[-1]
        prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close
        chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
        
        plt.style.use('dark_background')
        fig=plt.figure(figsize=(16,9), dpi=200, facecolor='#000000')
        gs=gridspec.GridSpec(4,1,height_ratios=[4.5,1.1,0.9,0.8], hspace=0.05)
        ax_main=fig.add_subplot(gs[0]); ax_vol=fig.add_subplot(gs[1], sharex=ax_main); ax_nbsa=fig.add_subplot(gs[2], sharex=ax_main); ax_mm=fig.add_subplot(gs[3], sharex=ax_main)
        fig.subplots_adjust(left=0.08, right=0.92, top=0.88, bottom=0.06)
        for ax in [ax_main, ax_vol, ax_nbsa, ax_mm]:
            ax.set_facecolor('#000000'); ax.tick_params(colors='#aaaaaa', labelsize=8); ax.yaxis.tick_right(); ax.grid(False)
        x=np.arange(len(df))
        # Candles
        for i in range(len(df)):
            o,h,l,c=df['Open'].iloc[i], df['High'].iloc[i], df['Low'].iloc[i], df['Close'].iloc[i]
            ax_main.plot([i,i],[l,h], color='#00ff00' if c>=o else '#ff0000', linewidth=0.8, alpha=0.8)
            body_low=min(o,c); body_h=max(0.5, abs(c-o))
            if c>=o:
                rect=patches.Rectangle((i-0.35, body_low),0.7,body_h, facecolor='none', edgecolor='#00ff00', linewidth=0.8)
            else:
                rect=patches.Rectangle((i-0.35, body_low),0.7,body_h, facecolor='#ff3333', edgecolor='#ff3333', linewidth=0.8)
            ax_main.add_patch(rect)
        ax_main.plot(x, df['EMA13'], color='#ffff00', linewidth=1.0, alpha=0.9)
        ax_main.plot(x, df['EMA20'], color='#ff0000', linewidth=1.0, alpha=0.9)
        ax_main.plot(x, df['EMA50'], color='#ffffff', linewidth=1.0, alpha=0.9)
        ax_main.plot(x, df['EMA200'], color='#a020f0', linewidth=1.2, alpha=0.9)
        ax_main.set_xlim(-1,len(df)); ax_main.set_ylim(df['Low'].min()*0.95, df['High'].max()*1.08)
        # Header
        fig.text(0.01,0.96,f"{symbol} : {last_close:.0f} ({chg_pct:+.2f}%)", color='#ffff00', fontsize=13, fontweight='bold', ha='left')
        fig.text(0.5,0.96,"RAFANO V4 PRO", color='white', fontsize=14, fontweight='bold', ha='center')
        fig.text(0.99,0.96,f"{timeframe.upper()} - {tf_desc}", color='#ffcc00', fontsize=9, ha='right')
        fig.text(0.99,0.93,f"Daily {df.index[-1].strftime('%d %b %Y') if hasattr(df.index[-1],'strftime') else ''}", color='#ffcc00', fontsize=8, ha='right')
        fig.text(0.01,0.905,f"High:{last_high:.0f} Low:{last_low:.0f} Open:{last_open:.0f} Vol:{last_vol:,.0f} V1:{df['V1'].iloc[-1]:,.0f}", color='#00ffff', fontsize=8, ha='left')
        # Volume - stacked Buy/Sell
        ax_vol.bar(x, df['Vol_Sell'], color='#cc0000', width=0.8, alpha=0.8)
        ax_vol.bar(x, df['Vol_Buy'], bottom=df['Vol_Sell'], color='#00cc00', width=0.8, alpha=0.9)
        ax_vol.plot(x, df['V1'], color='white', linewidth=0.8, alpha=0.9)
        ax_vol.set_ylim(0, df['Volume'].max()*1.8)
        plt.setp(ax_vol.get_xticklabels(), visible=False)
        # NBSA
        nbsa_vals=df['Net_Vol_VSA'].tail(80)/(df['Net_Vol_VSA'].abs().max() or 1)*50
        x_nbsa=np.arange(len(df)-len(nbsa_vals), len(df))
        for i,v in zip(x_nbsa, nbsa_vals):
            col='#00ffff' if v>=0 else '#ff4444'
            ax_nbsa.bar(i,v,color=col,width=0.6)
        ax_nbsa.axhline(0,color='#444444',linewidth=0.5)
        ax_nbsa.set_ylim(-60,60)
        # MM
        if 'MM' not in df.columns: df['MM']=(df['Close']-df['EMA50'])/df['EMA50']*1000
        mm_vals=df['MM'].tail(80); x_mm=np.arange(len(df)-len(mm_vals), len(df))
        ax_mm.bar(x_mm, mm_vals, color='#cccccc', width=0.5, alpha=0.8)
        ax_mm.set_ylim(df['MM'].min()*1.2-10, df['MM'].max()*1.2+10)
        plt.savefig(output_filename, dpi=200, bbox_inches='tight', facecolor='#000000')
        return output_filename
    except Exception as e:
        print(f"Chart error {e}"); import traceback; traceback.print_exc(); return None
    finally:
        try: plt.clf(); plt.close('all')
        except: pass

# ========== CAPTION PROFESIONAL BARU ==========
def build_professional_caption(symbol, last_close, flow_multi, trading_plan, timeframe):
    flow_d=flow_multi['daily']; flow_w=flow_multi['weekly']; flow_m=flow_multi['monthly']
    def fmt(v):
        if abs(v)>=1e12: return f"{v/1e12:.2f}T"
        if abs(v)>=1e9: return f"{v/1e9:.2f}B"
        if abs(v)>=1e6: return f"{v/1e6:.0f}M"
        if abs(v)>=1e3: return f"{v/1e3:.0f}K"
        return f"{v:.0f}"
    signal_label, emoji=classify_signal(trading_plan.get('signal_strength',0) if trading_plan else 0, flow_d, trading_plan.get('trend','') if trading_plan else '')
    tf_desc=get_tf_label(timeframe)
    top_accum_str=", ".join([f"{b.code} {fmt(b.net_val)}" for b in flow_d.top_accum[:3]]) or "-"
    top_dist_str=", ".join([f"{b.code} {fmt(abs(b.net_val))}" for b in flow_d.top_dist[:3]]) or "-"
    
    caption=f"*{symbol} -- {last_close:.0f} | {trading_plan.get('trend','NO TREND') if trading_plan else 'NO TREND'}*\n"
    caption+=f"{emoji} *{signal_label}* | Score: {trading_plan.get('signal_strength',0) if trading_plan else 0}% | {flow_d.status} Ratio {flow_d.accum_ratio:.0f}%\n"
    caption+=f"Timeframe: *{timeframe.upper()}* - {tf_desc}\n"
    caption+=f"────────────────────────────────\n"
    caption+=f"📊 *FLOW REAL (Bukan Estimasi)*\n"
    caption+=f"Daily: Akum {fmt(flow_d.accum_val)} | Dist {fmt(flow_d.distrib_val)} | Net {fmt(flow_d.total_net)}\n"
    caption+=f"  └ Top Akum: {top_accum_str}\n"
    caption+=f"  └ Top Dist: {top_dist_str}\n"
    caption+=f"Weekly: {flow_w.status} Net {fmt(flow_w.total_net)} Ratio {flow_w.accum_ratio:.0f}% | {', '.join([f'{b.code} {fmt(b.net_val)}' for b in flow_w.top_accum[:2]])}\n"
    caption+=f"Monthly: {flow_m.status} Net {fmt(flow_m.total_net)} Ratio {flow_m.accum_ratio:.0f}%\n"
    caption+=f"VSA: 1D {fmt(flow_multi['vsa_1d'])} | 5D {fmt(flow_multi['vsa_5d'])} | 20D {fmt(flow_multi['vsa_20d'])}\n"
    caption+=f"────────────────────────────────\n"
    if trading_plan and trading_plan.get('side')!='WAIT':
        caption+=f"📈 *TRADING PLAN - {trading_plan.get('signal_type')}*\n"
        caption+=f"Entry: {trading_plan['entry']} | SL: {trading_plan['sl']} ({trading_plan['risk_pct']}%)\n"
        caption+=f"TP1: {trading_plan['tp1']} (RR {trading_plan['rr1']}) | TP2: {trading_plan['tp2']} (RR {trading_plan['rr2']})\n"
        caption+=f"Sup: {trading_plan['support']} | Res: {trading_plan['resistance']} | ATR: {trading_plan['atr']:.1f}\n"
    else:
        caption+=f"⏸ *WAIT* - Tidak ada trigger valid\n"
        if trading_plan: caption+=f"Sup: {trading_plan['support']} | Res: {trading_plan['resistance']}\n"
    return caption

# ========== SCREENER & TELEGRAM ==========
def get_screener_latest():
    data=arjum_get("/screener/latest")
    if not data: return []
    if isinstance(data, dict) and 'rows' in data:
        out=[]
        for r in data['rows']:
            code=r.get('stock_code') or r.get('symbol')
            if code: out.append({"symbol": code.replace(".JK","").upper(), "raw": r})
        return out
    return data if isinstance(data, list) else []

def scan_v4():
    print(f"[{get_now_wib()}] 🚀 V4 Scan REAL...")
    screener=get_screener_latest()
    candidates=[x['symbol'] for x in screener[:30]] if screener else ["BBCA","BBRI","BMRI","TLKM","ASII","GOTO","AMMN","ADRO","ANTM","MDKA","BBNI","BRIS"]
    detected=[]
    def process(sym):
        try:
            hist=get_history_pro(sym, limit=120, timeframe="1d")
            multi=get_multi_tf_real(sym, hist)
            score=0
            if multi['daily'].accum_val>5e9: score+=30
            if multi['daily'].total_net>0: score+=20
            if hist is not None and len(hist)>50 and hist['Close'].iloc[-1]>hist['Close'].ewm(50).mean().iloc[-1]: score+=20
            score+=30 # screener
            if score>=40:
                return {"symbol": sym, "close": int(hist['Close'].iloc[-1]) if hist is not None else 0, "score": score, "multi_tf": multi, "history": hist, "flow": multi['daily']}
        except Exception as e: print(f"{sym} {e}")
        return None
    with ThreadPoolExecutor(max_workers=12) as ex:
        for f in ex.map(process, candidates):
            if f: detected.append(f)
    detected.sort(key=lambda x: x['score'], reverse=True)
    print(f"✅ V4 Scan: {len(detected)} sinyal REAL")
    return detected

def send_reply(chat_id, text, reply_markup=None):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup: payload["reply_markup"]=reply_markup
    try: requests.post(url, json=payload, timeout=10)
    except Exception as e: print(f"TG Error {e}")

def send_photo_reply(chat_id, photo_path, caption=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path, 'rb') as p:
            requests.post(url, data={'chat_id': chat_id, 'caption': caption, 'parse_mode': 'Markdown'}, files={'photo': p}, timeout=30)
    except Exception as e: print(f"Photo Error {e}")

LAST_SIGNALS_CACHE={}

def process_chart_request(chat_id, stock_code, timeframe="1d", extra_cache=None):
    send_reply(chat_id, f"📊 *Generating V4 Pro Chart {stock_code.upper()} ({timeframe.upper()}) + REAL FLOW...*")
    df=get_history_pro(stock_code, limit=150, timeframe=timeframe)
    if df is None or len(df)<20:
        send_reply(chat_id, f"⚠ Data {stock_code} TF {timeframe} tidak ketemu")
        return
    multi=get_multi_tf_real(stock_code, df)
    tp=calculate_trading_plan(df, multi_tf=multi)
    chart_file=f"chart_{stock_code.upper()}_{timeframe}_{int(time.time())}.png"
    file_path=generate_pro_chart(df, symbol=stock_code.upper(), timeframe=timeframe, sector_info=f"{stock_code.upper()} | IHSG", output_filename=chart_file, extra_info={"multi_tf": multi})
    if file_path:
        caption=build_professional_caption(stock_code.upper(), df['Close'].iloc[-1], multi, tp, timeframe)
        send_photo_reply(chat_id, file_path, caption=caption)
        try: os.remove(file_path)
        except: pass

def telegram_bot_listener():
    global LAST_SIGNALS_CACHE
    offset=0
    print("🤖 Telegram V4 Listener Running...")
    try: requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
    except: pass
    while True:
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res=requests.get(url, timeout=25)
            if res.status_code!=200: time.sleep(3); continue
            for update in res.json().get("result", []):
                offset=update["update_id"]+1
                if "callback_query" in update:
                    cb=update["callback_query"]; cb_id=cb.get("id"); cb_data=cb.get("data",""); chat_id=cb["message"]["chat"]["id"]
                    requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery", json={"callback_query_id": cb_id})
                    if cb_data.startswith("chart_"):
                        parts=cb_data.split("_"); sym=parts[1]; tf=parts[2]
                        threading.Thread(target=process_chart_request, args=(chat_id, sym, tf, LAST_SIGNALS_CACHE)).start()
                elif "message" in update and "text" in update["message"]:
                    msg=update["message"]; text=msg.get("text","").strip(); chat_id=msg["chat"]["id"]
                    first=text.split()[0].lower() if text else ""
                    if first in ["/start","/help"]:
                        help_msg="🤖 *RAFANO V4 PRO - REAL FLOW*\n\n📈 `/c <KODE> [TF]` - Chart Pro + Real Akum/Dist\n`/b <KODE>` - Detail Bandar Real\n`/scan` - Scan V4 REAL Accumulation\n`/top` - Top Akum\n`/clearcache` - Hapus cache"
                        send_reply(chat_id, help_msg)
                    elif first in ["/c","/chart"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper(); tf=parts[2] if len(parts)>=3 else "1d"
                            threading.Thread(target=process_chart_request, args=(chat_id, sym, tf, LAST_SIGNALS_CACHE)).start()
                    elif first in ["/scan"]:
                        send_reply(chat_id, "🔍 *V4 Scanning REAL...*")
                        def manual_scan(target_chat=chat_id):
                            sigs=scan_v4()
                            global LAST_SIGNALS_CACHE
                            LAST_SIGNALS_CACHE={s['symbol']: s for s in sigs}
                            if not sigs: send_reply(target_chat, "0 sinyal REAL"); return
                            msg=f"*V4 PRO REAL - {get_now_wib().strftime('%d %b %H:%M')}* Total {len(sigs)}\n\n"
                            kb=[]
                            for idx, item in enumerate(sigs[:10],1):
                                flow=item['multi_tf']['daily']
                                msg+=f"{idx}. *{item['symbol']}* -- {item['close']} Score {item['score']}% {flow.status} Net {format_large_number(flow.total_net,True)} Akum {format_large_number(flow.accum_val)} Dist {format_large_number(flow.distrib_val)} Ratio {flow.accum_ratio:.0f}%\n"
                                kb.append([{"text": f"Chart {item['symbol']}", "callback_data": f"chart_{item['symbol']}_1d"}])
                            send_reply(target_chat, msg, reply_markup={"inline_keyboard": kb})
                        threading.Thread(target=manual_scan).start()
        except Exception as e:
            print(f"Listener error {e}"); time.sleep(3)

if __name__=="__main__":
    print("🔥 RAFANO V4 PRO STARTING...")
    threading.Thread(target=telegram_bot_listener, daemon=True).start()
    while True: time.sleep(60)
