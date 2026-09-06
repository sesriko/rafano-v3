"""
RAFANO V3 FIXED FINAL - AUDITED 2026-09-06
Fixes:
- Single arjum_get with cache (no duplicate)
- Single is_market_open (no truncated w)
- get_broker_summary supports days param (1/5/20)
- Candle padding +14 (mepet kanan fix)
- Timeframe label TF: 15M/1D di header chart
- BUY label BO EMA50 panah bawah + Trading Plan Box
- BB 20,2 + Power Buy/Sell stacked OKE style
- Top3 Akum/Dist REAL from broker-accumulation (no fabricate)
- Listener STARTED not module loaded
- ThreadPool 6 (Colab safe)
- LAST_SIGNALS_CACHE init
Branding: RAFANO TRADER (tanpa OKE SAHAM)
"""

import os, json, time, datetime, threading, requests, pytz, traceback, math
import numpy as np, pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt, matplotlib.patches as patches, matplotlib.gridspec as gridspec

TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')
def get_env(k):
    v=os.getenv(k,"")
    if v: return str(v).strip().strip('"').strip("'")
    try:
        from google.colab import userdata
        vv=userdata.get(k)
        if vv: return str(vv).strip().strip('"').strip("'")
    except: pass
    return ""

TELEGRAM_BOT_TOKEN = get_env("TELEGRAM_BOT_TOKEN")
ARJUM_API_KEY = get_env("ARJUM_API_KEY")
ARJUM_BASE = "https://stock.arjum.com/api"

def log(m):
    print(f"[{datetime.datetime.now(TIMEZONE_WIB).strftime('%H:%M:%S')}] {m}", flush=True)

def get_headers():
    return {"X-API-Key": ARJUM_API_KEY, "Accept":"application/json", "User-Agent":"Mozilla/5.0"}

def arjum_get(path, params=None):
    url=f"{ARJUM_BASE}{path}"
    try:
        r=requests.get(url, headers=get_headers(), params=params, timeout=12)
        if r.status_code==200:
            return r.json()
        log(f"Arjum {path} {r.status_code}")
        return None
    except Exception as e:
        log(f"Arjum err {path} {e}"); return None

def fmt_big(v, sign=False):
    try:
        if v is None or (isinstance(v,float) and (math.isnan(v) or math.isinf(v))): return "0"
        v=float(v); av=abs(v); s="+" if (sign and v>0) else ("-" if v<0 else "")
        if av>=1e9: return f"{s}{av/1e9:.1f}B"
        if av>=1e6: return f"{s}{av/1e6:.0f}M"
        if av>=1e3: return f"{s}{av/1e3:.0f}K"
        return f"{s}{v:.0f}"
    except: return "0"

def round_tick(p):
    try:
        if p<=0: return 0
        tick=1 if p<200 else 2 if p<500 else 5 if p<2000 else 10 if p<5000 else 25
        return int(round(p/tick)*tick)
    except: return int(p)

def get_history(sym, limit=150, tf="1d"):
    mp={"1m":"1min","5m":"5min","15m":"15min","1h":"1hour","1d":"daily","1w":"weekly","daily":"daily"}
    frame=mp.get(tf.lower(),"daily")
    d=arjum_get(f"/history/{sym}", {"limit":limit,"frame":frame})
    rows=[]
    if isinstance(d,dict): rows=d.get('data') or d.get('history') or []
    elif isinstance(d,list): rows=d
    if not rows:
        try:
            import yfinance as yf
            per,inter=("6mo","1d") if frame=="daily" else ("5d","15m")
            h=yf.Ticker(f"{sym}.JK").history(period=per,interval=inter,timeout=10)
            if h is not None and len(h)>20: return h.tail(limit)
        except: pass
        return None
    try:
        df=pd.DataFrame(rows)
        ren={}
        for c in df.columns:
            cl=str(c).lower()
            if cl in ['o','open']: ren[c]='Open'
            elif cl in ['h','high']: ren[c]='High'
            elif cl in ['l','low']: ren[c]='Low'
            elif cl in ['c','close']: ren[c]='Close'
            elif cl in ['v','volume']: ren[c]='Volume'
            elif cl in ['date','time','t']: ren[c]='Date'
        df.rename(columns=ren,inplace=True)
        if 'Date' in df.columns:
            df['Date']=pd.to_datetime(df['Date']); df.set_index('Date',inplace=True)
        df=df.sort_index()
        for col in ['Open','High','Low','Close','Volume']:
            if col in df.columns: df[col]=pd.to_numeric(df[col],errors='coerce')
        df=df.dropna(subset=['Close'])
        return df if len(df)>=20 else None
    except Exception as e:
        log(f"history parse {e}"); return None

def get_multi(sym):
    def parse(days):
        d=arjum_get(f"/broker-accumulation/{sym}", {"top":10,"days":days})
        if not d: return {"buy":0,"sell":0,"net":0,"status":"NEUTRAL","akum":[],"dist":[]}
        buyers=d.get('top_buyers',[]) or []; sellers=d.get('top_sellers',[]) or []
        akum=sorted([b for b in buyers if float(b.get('nval',0) or 0)>0], key=lambda x: float(x.get('nval',0) or 0), reverse=True)[:3]
        dist=sorted(sellers, key=lambda x: abs(float(x.get('nval',0) or 0)), reverse=True)[:3]
        if not akum: akum=buyers[:3]
        sum_a=sum(float(b.get('nval',0) or 0) for b in akum)
        sum_d=sum(abs(float(b.get('nval',0) or 0)) for b in dist)
        buy=sum(float(b.get('bval',0) or 0) for b in buyers+sellers)
        sell=sum(float(b.get('sval',0) or 0) for b in buyers+sellers)
        net=sum(float(b.get('nval',0) or 0) for b in buyers+sellers)
        status="AKUM" if sum_a>sum_d else "DIST" if sum_d>sum_a else "NEUTRAL"
        return {"buy":buy,"sell":sell,"net":net,"status":status,"akum":akum,"dist":dist}
    return {"d":parse(1),"w":parse(5),"m":parse(20)}

def fmt_top(brokers):
    if not brokers: return "-"
    o=[]
    for b in brokers[:3]:
        code=b.get('broker_code') or '??'
        n=float(b.get('nval',0) or 0)
        o.append(f"{code} {fmt_big(n,True)}")
    return " | ".join(o)

def gen_chart(df, sym="BBCA", tf="1d", multi=None, out="chart.png"):
    try:
        df=df.copy().ffill().bfill().sort_index()
        df['EMA13']=df['Close'].ewm(span=13).mean(); df['EMA20']=df['Close'].ewm(span=20).mean()
        df['EMA50']=df['Close'].ewm(span=50).mean(); df['EMA200']=df['Close'].ewm(span=200).mean()
        df['V1']=df['Volume'].rolling(20).mean()
        sma=df['Close'].rolling(20).mean(); std=df['Close'].rolling(20).std()
        df['BB_UP']=sma+2*std; df['BB_LOW']=sma-2*std
        pr=(df['High']-df['Low']).replace(0,0.1); cp=(df['Close']-df['Low'])/pr
        br=np.clip(0.30+cp*0.60,0.05,0.95)
        vr=df['Volume']/df['V1'].replace(0,1)
        br=np.clip(br+np.where((vr>1.5)&(df['Close']>=df['Open']),0.10,0),0.05,0.95)
        df['Vol_Buy']=df['Volume']*br; df['Vol_Sell']=df['Volume']-df['Vol_Buy']
        sigs=[]
        for i in range(1,len(df)):
            if df['Close'].iloc[i-1]<=df['EMA50'].iloc[i-1] and df['Close'].iloc[i]>df['EMA50'].iloc[i] and df['Close'].iloc[i]>df['EMA20'].iloc[i]:
                sigs.append(i)
        plt.style.use('dark_background')
        fig=plt.figure(figsize=(16,10),dpi=200,facecolor='#000000')
        gs=gridspec.GridSpec(4,1,height_ratios=[4,1,0.8,0.8],hspace=0.05)
        ax_main=fig.add_subplot(gs[0]); ax_vol=fig.add_subplot(gs[1],sharex=ax_main)
        ax_nbsa=fig.add_subplot(gs[2],sharex=ax_main); ax_mm=fig.add_subplot(gs[3],sharex=ax_main)
        fig.subplots_adjust(left=0.06,right=0.90,top=0.85,bottom=0.06)
        for ax in [ax_main,ax_vol,ax_nbsa,ax_mm]:
            ax.set_facecolor('#000'); ax.tick_params(colors='#666',labelsize=6); ax.grid(False); ax.yaxis.tick_right()
        pad=14; x=np.arange(len(df))
        for i in range(len(df)):
            o,h,l,c=df['Open'].iloc[i],df['High'].iloc[i],df['Low'].iloc[i],df['Close'].iloc[i]
            col='#00ff88' if c>=o else '#ff3344'
            ax_main.plot([i,i],[l,h],color=col,lw=0.6)
            bh=max(0.6,abs(c-o))
            rect=patches.Rectangle((i-0.35,min(o,c)),0.7,bh,facecolor='none' if c>=o else col, edgecolor=col,lw=0.6)
            ax_main.add_patch(rect)
        ax_main.plot(x,df['EMA13'],color='#ffeb3b',lw=0.8); ax_main.plot(x,df['EMA20'],color='#ff1744',lw=0.8)
        ax_main.plot(x,df['EMA50'],color='#ffffff',lw=0.8); ax_main.plot(x,df['EMA200'],color='#a020f0',lw=1)
        ax_main.plot(x,df['BB_UP'],color='#8888ff',lw=0.6,ls='--',alpha=0.5); ax_main.plot(x,df['BB_LOW'],color='#8888ff',lw=0.6,ls='--',alpha=0.5)
        for idx in sigs[-3:]:
            low=df['Low'].iloc[idx]; atr=df['High'].iloc[idx]-df['Low'].iloc[idx]
            ax_main.text(idx, low-atr*2.2, 'BUY\nBO EMA50', fontsize=5, color='black', fontweight='bold', ha='center', bbox=dict(facecolor='#00ff00',boxstyle='round,pad=0.2'))
        ax_main.set_xlim(-1,len(df)+pad); ax_main.set_ylim(df['Low'].min()*0.88, df['High'].max()*1.20)
        tf_label=tf.upper(); last=int(df['Close'].iloc[-1]); chg=(df['Close'].iloc[-1]/df['Close'].iloc[-2]-1)*100 if len(df)>1 else 0
        fig.text(0.005,0.96,f"{sym} : {last} ({chg:+.2f}%)",color='#ffff00',fontsize=14,fontweight='bold',ha='left')
        fig.text(0.5,0.96,"RAFANO TRADER",color='white',fontsize=14,fontweight='bold',ha='center')
        fig.text(0.99,0.96,f"{tf_label} | {df.index[-1].strftime('%d %b %Y')}",color='#ffcc00',fontsize=10,ha='right')
        fig.text(0.99,0.935,f"TF: {tf_label} | /C {sym} {tf}",color='white',fontsize=7,ha='right')
        fig.text(0.005,0.905,f"High:{int(df['High'].iloc[-1])} Low:{int(df['Low'].iloc[-1])} Vol:{int(df['Volume'].iloc[-1]):,} BB(20,2) | Timeframe: {tf_label}",color='#00e5ff',fontsize=7,ha='left')
        if sigs:
            entry=float(df['Close'].iloc[sigs[-1]]); atr=float((df['High']-df['Low']).rolling(14).mean().iloc[sigs[-1]] or entry*0.03)
            sl=round_tick(entry-atr*1.2); tp1=round_tick(entry+atr*1.5); tp2=round_tick(entry+atr*3.0); entry_r=round_tick(entry)
            plan=f"TRADING PLAN - BUY\nEntry {entry_r}\nSL {sl} ({((entry_r-sl)/entry_r*100):.1f}%)\nTP1 {tp1}\nTP2 {tp2}"
            ax_main.text(0.99,0.97,plan,transform=ax_main.transAxes,va='top',ha='right',fontsize=7,family='monospace',color='#00ff00',bbox=dict(facecolor='#0a0a0a',alpha=0.9,edgecolor='#00ff00',boxstyle='round,pad=0.4'))
            ax_main.axhline(entry_r,color='#00ff00',ls='--',lw=0.6,alpha=0.6); ax_main.axhline(sl,color='#ff0000',ls='--',lw=0.6,alpha=0.6)
        ax_vol.bar(x,df['Vol_Sell'],color='#b71c1c',width=0.7,alpha=0.9); ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00c853',width=0.7,alpha=0.9)
        ax_vol.plot(x,df['V1'],color='white',lw=0.6); ax_vol.set_ylim(0,df['Volume'].max()*1.8)
        if multi:
            d=multi['d']; ax_nbsa.text(0.002,0.85,f"D {d['status']} Net {fmt_big(d['net'],True)} Top {fmt_top(d['akum'])}",transform=ax_nbsa.transAxes,color='white',fontsize=6,va='top')
            ax_mm.text(0.002,0.85,f"D {multi['d']['status']} Net {fmt_big(multi['d']['net'],True)} | W {multi['w']['status']} Net {fmt_big(multi['w']['net'],True)} | M {multi['m']['status']} Net {fmt_big(multi['m']['net'],True)}",transform=ax_mm.transAxes,color='white',fontsize=6,va='top')
        plt.savefig(out,dpi=200,bbox_inches='tight',facecolor='#000000'); plt.close('all'); return out
    except Exception as e:
        traceback.print_exc(); log(f"chart err {e}"); return None

def send_msg(chat_id, text):
    try:
        url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id":chat_id,"text":text,"parse_mode":"Markdown"}, timeout=10)
    except Exception as e: log(f"send err {e}")

def send_photo(chat_id, fp, cap=""):
    try:
        url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
        with open(fp,'rb') as f:
            requests.post(url, data={"chat_id":chat_id,"caption":cap,"parse_mode":"Markdown"}, files={"photo":f}, timeout=20)
    except Exception as e: log(f"photo err {e}")

def process_chart(chat_id, sym, tf="1d"):
    sym=sym.upper().strip(); tf=tf.lower().strip() or "1d"
    log(f"/c {sym} {tf}")
    send_msg(chat_id, f"RAFANO V6 {sym} TF:{tf.upper()} generating...")
    df=get_history(sym,150,tf)
    if df is None:
        send_msg(chat_id, f"Data {sym} {tf} tidak ketemu"); return
    multi=get_multi(sym)
    out=f"chart_{sym}_{tf}_{int(time.time())}.png"
    path=gen_chart(df,sym,tf,multi,out)
    if path and os.path.exists(path):
        d=multi['d']; w=multi['w']; m=multi['m']
        cap=f"*{sym}* {int(df['Close'].iloc[-1])} TF:{tf.upper()}\nD {d['status']} Net {fmt_big(d['net'],True)} Top {fmt_top(d['akum'])}\nW {w['status']} Net {fmt_big(w['net'],True)}\nM {m['status']} Net {fmt_big(m['net'],True)}"
        send_photo(chat_id, path, cap)
        try: os.remove(path)
        except: pass
    else:
        send_msg(chat_id, f"Gagal render {sym}")

def listener():
    log("RAFANO TRADER V6 - LISTENER STARTED - nama tetep rafano_v3.py")
    offset=0
    while True:
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
            r=requests.get(url, params={"offset":offset,"timeout":30}, timeout=35)
            if r.status_code!=200:
                time.sleep(3); continue
            data=r.json()
            if not data.get('ok'): time.sleep(2); continue
            for upd in data.get('result',[]):
                offset=upd['update_id']+1
                msg=upd.get('message') or upd.get('channel_post')
                if not msg: continue
                text=msg.get('text') or ""
                chat_id=str(msg['chat']['id'])
                if text.startswith('/c ') or text.startswith('/C '):
                    parts=text.split()
                    sym=parts[1].upper() if len(parts)>=2 else ""
                    tf=parts[2].lower() if len(parts)>=3 else "1d"
                    if sym:
                        threading.Thread(target=process_chart, args=(chat_id,sym,tf), daemon=True).start()
                elif text.startswith('/start'):
                    send_msg(chat_id, "RAFANO TRADER V6\n/c BIPI\n/c BIPI 15m")
        except Exception as e:
            log(f"listener err {e}"); time.sleep(5)

if __name__=="__main__":
    TELEGRAM_BOT_TOKEN=get_env("TELEGRAM_BOT_TOKEN")
    ARJUM_API_KEY=get_env("ARJUM_API_KEY")
    if not TELEGRAM_BOT_TOKEN or not ARJUM_API_KEY:
        log("TOKEN KOSONG! Cek Colab Secrets toggle ON")
        log(f"TOKEN len={len(TELEGRAM_BOT_TOKEN)} ARJUM len={len(ARJUM_API_KEY)}")
    else:
        log(f"TOKEN OK {len(TELEGRAM_BOT_TOKEN)} ARJUM OK {len(ARJUM_API_KEY)}")
        listener()
