
import os, json, time, datetime, threading, requests, pytz, traceback, math, re
import numpy as np, pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt, matplotlib.patches as patches, matplotlib.gridspec as gridspec
from concurrent.futures import ThreadPoolExecutor

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
TARGET_CHAT_ID = get_env("TARGET_CHAT_ID")
ARJUM_API_KEY = get_env("ARJUM_API_KEY")
ARJUM_BASE = "https://stock.arjum.com/api"

def log(m): print(f"[{datetime.datetime.now(TIMEZONE_WIB).strftime('%H:%M:%S')}] {m}", flush=True)
def sanitize_symbol(s):
    if not s: return ""
    s=str(s).upper().strip()
    s=s.replace(".JK","")
    s=s.replace("'","").replace('"',"").replace("`","")
    s=re.sub(r'[^A-Z0-9]', '', s)
    return s[:12]

# CACHE
BROKER_CACHE={}; HISTORY_CACHE={}; SCREENER_CACHE={}
import pathlib; CACHE_FILE=pathlib.Path("/tmp/rafano_cache.json")
def get_cached(key, store, ttl):
    if key in store:
        ts,data=store[key]
        if time.time()-ts < ttl: return data
        else: del store[key]
    return None
def set_cached(key, data, store):
    store[key]=(time.time(), data)

def arjum_get(path, params=None):
    url=f"{ARJUM_BASE}{path}"
    try:
        headers={"X-API-Key": ARJUM_API_KEY, "Accept":"application/json", "User-Agent":"Mozilla/5.0"}
        r=requests.get(url, headers=headers, params=params, timeout=12)
        if r.status_code==200:
            try: return r.json()
            except: return None
        if r.status_code in (400,403,404,422):
            log(f"Arjum {path} {r.status_code} - fallback")
            return None
        log(f"Arjum {path} {r.status_code}")
        return None
    except Exception as e:
        log(f"Arjum err {path} {e}"); return None

def fmt_big(v, sign=False):
    try:
        v=float(v); av=abs(v); s="+" if (sign and v>0) else ("-" if v<0 else "")
        if av>=1e12: return f"{s}{av/1e12:.2f}T"
        if av>=1e9: return f"{s}{av/1e9:.2f}B"
        if av>=1e6: return f"{s}{av/1e6:.0f}M"
        if av>=1e3: return f"{s}{av/1e3:.0f}K"
        return f"{s}{v:.0f}"
    except: return "0"
def round_tick(p):
    if p<=0: return 0
    tick=1 if p<200 else 2 if p<500 else 5 if p<2000 else 10 if p<5000 else 25
    return int(round(p/tick)*tick)

def get_history_pro(symbol, limit=150, timeframe="1d"):
    symbol=sanitize_symbol(symbol)
    if not symbol: return None
    key=f"{symbol}_{timeframe}_{limit}"
    cached=get_cached(key, HISTORY_CACHE, 600)
    if cached is not None: return cached
    mp={"1m":"1min","5m":"5min","15m":"15min","30m":"30min","1h":"1hour","4h":"4hour","1d":"daily","1w":"weekly"}
    frame=mp.get(timeframe.lower(),"daily")
    # Arjum max 200
    data=arjum_get(f"/history/{symbol}", {"limit": min(limit,200), "frame": frame})
    rows=[]
    if isinstance(data,dict): rows=data.get('data') or data.get('history') or []
    elif isinstance(data,list): rows=data
    if rows:
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
            for col in ['Open','High','Low','Close','Volume']: df[col]=pd.to_numeric(df[col],errors='coerce')
            df=df.dropna(subset=['Close'])
            if len(df)>=15:
                set_cached(key, df.tail(limit), HISTORY_CACHE)
                return df.tail(limit)
        except: pass
    # Fallback yfinance
    try:
        import yfinance as yf
        yf_map={"1m":("7d","1m"),"5m":("5d","5m"),"15m":("5d","15m"),"30m":("1mo","30m"),"1h":("1mo","60m"),"4h":("3mo","90m"),"1d":("6mo","1d"),"1w":("1y","1wk")}
        per,inter=yf_map.get(timeframe.lower(),("6mo","1d"))
        hist=yf.Ticker(f"{symbol}.JK").history(period=per, interval=inter, timeout=10)
        if hist is not None and len(hist)>15:
            log(f"yfinance {symbol} {timeframe} {len(hist)}")
            set_cached(key, hist.tail(limit), HISTORY_CACHE)
            return hist.tail(limit)
    except Exception as e:
        log(f"yf err {symbol} {e}")
    return None

def get_broker_accumulation(symbol, top=10, days=None):
    symbol=sanitize_symbol(symbol)
    params={"top": top}
    if days: params["days"]=days
    data=arjum_get(f"/broker-accumulation/{symbol}", params)
    if not data: return 0, []
    top_buyers=data.get('top_buyers',[]) or []; top_sellers=data.get('top_sellers',[]) or []; series=data.get('series',[]) or []
    # timeline format
    if series and isinstance(series[0], dict) and 'accum_val' in series[0]:
        if days and len(series)>=days:
            last=float(series[-1].get('accum_val',0) or 0)
            first=float(series[-days].get('accum_val',0) or 0)
            net=last-first if last!=0 else last
        else:
            net=float(series[-1].get('accum_val',0) or 0) if series else 0
        return net, top_buyers+top_sellers
    # per broker
    brokers=[]
    acc_sum=0
    for b in top_buyers[:10]:
        try:
            code=b.get('broker_code') or '??'
            n=float(b.get('nval',0) or 0); bv=float(b.get('bval',0) or 0); sv=float(b.get('sval',0) or 0)
            bvol=float(b.get('bvol',0) or 0); svol=float(b.get('svol',0) or 0); avg=float(b.get('bavg',0) or 0)
            brokers.append({"broker_code":code.upper(),"buy_value":bv,"sell_value":sv,"net_value":n,"buy_volume":bvol,"sell_volume":svol,"avg_price":avg})
            acc_sum+=abs(n)
        except: pass
    return acc_sum, brokers

def get_broker_summary(symbol, days=None):
    symbol=sanitize_symbol(symbol)
    # Try days param first
    params={}
    if days: params["days"]=days
    params.update({"broker_limit":20,"flow":"all","net":"false"})
    data=arjum_get(f"/broker-summary/{symbol}", params)
    if not data:
        data=arjum_get(f"/broker-summary/{symbol}", {"broker_limit":20})
    net=0; brokers=[]
    if data:
        blist=data.get('brokers') or data.get('data') or []
        if blist:
            for b in blist[:20]:
                code=b.get('broker_code') or b.get('code') or '??'
                bv=float(b.get('bval',0) or b.get('buy_value',0) or 0)
                sv=float(b.get('sval',0) or b.get('sell_value',0) or 0)
                nv=float(b.get('nval',0) or b.get('net_value',0) or (bv-sv))
                brokers.append({"broker_code":str(code).upper(),"buy_value":bv,"sell_value":sv,"net_value":nv,"avg_price":float(b.get('bavg',0) or 0)})
                net+=nv
        else:
            bv=float(data.get('bval',0) or data.get('buy_value',0) or 0)
            sv=float(data.get('sval',0) or data.get('sell_value',0) or 0)
            nv=float(data.get('nval',0) or data.get('net_value',0) or (bv-sv))
            if nv!=0:
                brokers=[{"broker_code":"ALL","buy_value":bv,"sell_value":sv,"net_value":nv,"avg_price":0}]
                net=nv
    if net==0 and not brokers:
        # fallback to accumulation
        acc, acc_b= get_broker_accumulation(symbol, days=days)
        if acc!=0:
            net=acc; brokers=acc_b
    status="AKUM" if net>0 else "DIST" if net<0 else "NEUTRAL"
    return net, status, brokers

def get_broker_multi_tf(symbol, hist_df=None):
    symbol=sanitize_symbol(symbol)
    def flow_for(days):
        net,status,brokers=get_broker_summary(symbol, days=days)
        # also get top accum
        _, acc_brokers = get_broker_accumulation(symbol, top=10, days=days)
        if not brokers: brokers=acc_brokers
        # Top3 calc
        accum=[b for b in brokers if b['net_value']>0]
        distrib=[b for b in brokers if b['net_value']<0]
        accum.sort(key=lambda x: x['net_value'], reverse=True)
        distrib.sort(key=lambda x: abs(x['net_value']), reverse=True)
        top_a=accum[:3]; top_d=distrib[:3]
        top_a_val=sum(b['net_value'] for b in top_a)
        top_d_val=sum(abs(b['net_value']) for b in top_d)
        buy=sum(b['buy_value'] for b in brokers)
        sell=sum(b['sell_value'] for b in brokers)
        # Status by Top3
        if top_a_val > top_d_val: st="AKUM"
        elif top_d_val > top_a_val: st="DIST"
        else: st=status
        return {"buy":buy,"sell":sell,"net":net,"status":st,"brokers":brokers,"top_a":top_a,"top_d":top_d,"top_a_val":top_a_val,"top_d_val":top_d_val}
    d=flow_for(1); w=flow_for(5); m=flow_for(20)
    log(f"FLOW FINAL {symbol} 1D: {d['status']} {fmt_big(d['top_a_val'])} { [b['broker_code'] for b in d['top_a']] } vs {fmt_big(d['top_d_val'])} {[b['broker_code'] for b in d['top_d']]} => {d['status']}")
    return {"d":d,"w":w,"m":m,"buy_d":d['buy'],"sell_d":d['sell'],"net_d":d['net'],"status_d":d['status'],"brokers":d['brokers'],"top_a_d":d['top_a'],"top_d_d":d['top_d'],
            "buy_5d":w['buy'],"sell_5d":w['sell'],"net_5d":w['net'],"status_5d":w['status'],"brokers_5d":w['brokers'],"top_a_5d":w['top_a'],"top_d_5d":w['top_d'],
            "buy_20d":m['buy'],"sell_20d":m['sell'],"net_20d":m['net'],"status_20d":m['status'],"brokers_20d":m['brokers'],"top_a_20d":m['top_a'],"top_d_20d":m['top_d']}

def fmt_top(brokers):
    if not brokers: return "-"
    return ", ".join([f"{b['broker_code']} {fmt_big(b['net_value'],True)}" for b in brokers[:3]])

def calculate_vsa(df):
    pr=(df['High']-df['Low']).replace(0,0.1); cp=(df['Close']-df['Low'])/pr
    br=np.clip(0.30+cp*0.60,0.05,0.95)
    df['V1']=df['Volume'].rolling(20).mean()
    vr=df['Volume']/df['V1'].replace(0,1)
    br=np.clip(br+np.where((vr>1.5)&(df['Close']>=df['Open']),0.10,0),0.05,0.95)
    df['Vol_Buy']=df['Volume']*br; df['Vol_Sell']=df['Volume']-df['Vol_Buy']
    df['BuyPct']=br*100; return df

def detect_signals(df):
    df=df.copy(); df['EMA20']=df['Close'].ewm(span=20).mean(); df['EMA50']=df['Close'].ewm(span=50).mean()
    sigs=[]
    for i in range(1,len(df)):
        if df['Close'].iloc[i-1]<=df['EMA50'].iloc[i-1] and df['Close'].iloc[i]>df['EMA50'].iloc[i] and df['Close'].iloc[i]>df['EMA20'].iloc[i]:
            sigs.append(i)
    return sigs

def gen_chart(df, symbol, timeframe, multi, out="chart.png"):
    df=df.copy().ffill().bfill().sort_index()
    df['EMA13']=df['Close'].ewm(span=13).mean(); df['EMA20']=df['Close'].ewm(span=20).mean()
    df['EMA50']=df['Close'].ewm(span=50).mean(); df['EMA200']=df['Close'].ewm(span=200).mean()
    df['V1']=df['Volume'].rolling(20).mean(); df['V2']=df['Volume'].rolling(50).mean()
    sma=df['Close'].rolling(20).mean(); std=df['Close'].rolling(20).std()
    df['BB_UP']=sma+2*std; df['BB_LOW']=sma-2*std
    df=calculate_vsa(df)
    sigs=detect_signals(df)
    # Metrics dashboard kiri
    last=df['Close'].iloc[-1]; prev=df['Close'].iloc[-2] if len(df)>1 else last
    chg=(last/prev-1)*100 if prev else 0
    avg_price=df['Close'].tail(20).mean()
    vchg1=(df['Volume'].iloc[-1]/df['Volume'].iloc[-2]) if len(df)>1 and df['Volume'].iloc[-2]>0 else 1
    vchg5=(df['Volume'].iloc[-1]/df['Volume'].tail(5).mean()) if df['Volume'].tail(5).mean()>0 else 1
    buy_pct=int(df['BuyPct'].iloc[-1]); speed="FAST" if vchg1>2 else "SLOW" if vchg1<0.8 else "NORMAL"
    power="TURBO" if buy_pct>=85 and vchg1>=1.2 else "STRONG" if buy_pct>=70 or vchg1>=1.5 else "NORMAL" if buy_pct>=60 else "WEAK"
    safety="GOOD" if last>df['EMA200'].iloc[-1] else "BAD"
    plt.style.use('dark_background')
    fig=plt.figure(figsize=(16,10),dpi=200,facecolor='#000000')
    gs=gridspec.GridSpec(4,1,height_ratios=[4.5,1.1,0.9,0.8],hspace=0.05)
    ax_main=fig.add_subplot(gs[0]); ax_vol=fig.add_subplot(gs[1],sharex=ax_main); ax_nbsa=fig.add_subplot(gs[2],sharex=ax_main); ax_mm=fig.add_subplot(gs[3],sharex=ax_main)
    fig.subplots_adjust(left=0.08,right=0.92,top=0.88,bottom=0.06)
    for ax in [ax_main,ax_vol,ax_nbsa,ax_mm]: ax.set_facecolor('#000'); ax.tick_params(colors='#666',labelsize=6); ax.grid(False); ax.yaxis.tick_right()
    pad=14  # JARAK CANDLE TERAKHIR
    x=np.arange(len(df))
    for i in range(len(df)):
        o,h,l,c=df['Open'].iloc[i],df['High'].iloc[i],df['Low'].iloc[i],df['Close'].iloc[i]
        col='#00ff88' if c>=o else '#ff3344'
        ax_main.plot([i,i],[l,h],color=col,lw=0.6)
        bh=max(0.6,abs(c-o)); rect=patches.Rectangle((i-0.35,min(o,c)),0.7,bh,facecolor='none' if c>=o else col, edgecolor=col,lw=0.6); ax_main.add_patch(rect)
    ax_main.plot(x,df['EMA13'],color='#ffeb3b',lw=0.8); ax_main.plot(x,df['EMA20'],color='#ff1744',lw=0.8); ax_main.plot(x,df['EMA50'],color='#ffffff',lw=0.8); ax_main.plot(x,df['EMA200'],color='#a020f0',lw=1)
    ax_main.plot(x,df['BB_UP'],color='#8888ff',lw=0.6,ls='--',alpha=0.5); ax_main.plot(x,df['BB_LOW'],color='#8888ff',lw=0.6,ls='--',alpha=0.5)
    for idx in sigs[-3:]:
        low=df['Low'].iloc[idx]; atr=df['High'].iloc[idx]-df['Low'].iloc[idx]
        ax_main.text(idx, low-atr*2.2, 'BUY\nBO EMA50', fontsize=5, color='black', fontweight='bold', ha='center', bbox=dict(facecolor='#00ff00',boxstyle='round,pad=0.2'))
    ax_main.set_xlim(-1, len(df)+pad)  # FIX MEPET KANAN
    ax_main.set_ylim(df['Low'].min()*0.88, df['High'].max()*1.20)
    tf_label=timeframe.upper(); last_i=int(df['Close'].iloc[-1])
    fig.text(0.01,0.96,f"{symbol} : {last_i} ({chg:+.2f}%)",color='#ffff00',fontsize=13,fontweight='bold',ha='left')
    fig.text(0.5,0.96,"RAFANO TRADER",color='white',fontsize=14,fontweight='bold',ha='center')
    fig.text(0.99,0.96,f"{tf_label} | {df.index[-1].strftime('%d %b %Y')}",color='#ffcc00',fontsize=10,ha='right')
    fig.text(0.99,0.935,f"TF: {tf_label} | /C {symbol} {timeframe}",color='white',fontsize=7,ha='right')
    fig.text(0.01,0.905,f"High:{int(df['High'].iloc[-1])} Low:{int(df['Low'].iloc[-1])} Open:{int(df['Open'].iloc[-1])} Vol:{int(df['Volume'].iloc[-1]):,} V1:{int(df['V1'].iloc[-1]):,} V2:{int(df['V2'].iloc[-1]):,} BB(20,2)",color='#00e5ff',fontsize=7,ha='left')
    # LEFT DASHBOARD FULL
    left_text=(f"Avg Price : {avg_price:,.1f}\nVchg 1 Day: {vchg1:.1f} x\nVchg 5 Days: {vchg5:.1f} x\nSpeed : {speed}\nPower : {power}\nSafety : {safety}\n\nEMA 13 : {df['EMA13'].iloc[-1]:,.1f}\nEMA 20 : {df['EMA20'].iloc[-1]:,.1f}\nEMA 50 : {df['EMA50'].iloc[-1]:,.1f}\nEMA 200: {df['EMA200'].iloc[-1]:,.1f}")
    ax_main.text(0.01,0.98,left_text,transform=ax_main.transAxes,va='top',ha='left',fontsize=7,family='monospace',color='#e0e0e0',bbox=dict(facecolor='black',alpha=0.6,edgecolor='none'))
    # TRADING PLAN
    if sigs:
        entry=float(df['Close'].iloc[sigs[-1]]); atr=float((df['High']-df['Low']).rolling(14).mean().iloc[sigs[-1]] or entry*0.03)
        sl=round_tick(entry-atr*1.2); tp1=round_tick(entry+atr*1.5); tp2=round_tick(entry+atr*3.0); entry_r=round_tick(entry)
        plan=f"TRADING PLAN - BUY\nEntry {entry_r}\nSL {sl} ({((entry_r-sl)/entry_r*100):.1f}%)\nTP1 {tp1}\nTP2 {tp2}"
        ax_main.text(0.99,0.97,plan,transform=ax_main.transAxes,va='top',ha='right',fontsize=7,family='monospace',color='#00ff00',bbox=dict(facecolor='#0a0a0a',alpha=0.9,edgecolor='#00ff00',boxstyle='round,pad=0.4'))
        ax_main.axhline(entry_r,color='#00ff00',ls='--',lw=0.6,alpha=0.6); ax_main.axhline(sl,color='#ff0000',ls='--',lw=0.6,alpha=0.6)
    # VOLUME PANEL
    ax_vol.bar(x,df['Vol_Sell'],color='#b71c1c',width=0.7,alpha=0.9); ax_vol.bar(x,df['Vol_Buy'],bottom=df['Vol_Sell'],color='#00c853',width=0.7,alpha=0.9)
    ax_vol.plot(x,df['V1'],color='white',lw=0.6); ax_vol.set_ylim(0,df['Volume'].max()*1.8)
    ax_vol.text(0.005,0.88,f"Buy%={buy_pct}% Sell%={100-buy_pct}% Net Vol={int(df['Vol_Buy'].iloc[-1]-df['Vol_Sell'].iloc[-1]):,}",transform=ax_vol.transAxes,color='white',fontsize=7)
    # NBSA & MM
    if multi:
        d=multi['d']; w=multi['w']; m=multi['m']
        ax_nbsa.text(0.005,0.85,f"D {d['status']} Net {fmt_big(d['net'],True)} Top {fmt_top(d['top_a'])} | DIST {fmt_top(d['top_d'])}",transform=ax_nbsa.transAxes,color='white',fontsize=6,va='top')
        ax_nbsa.text(0.005,0.55,f"W {w['status']} Net {fmt_big(w['net'],True)} Top {fmt_top(w['top_a'])}",transform=ax_nbsa.transAxes,color='#aaa',fontsize=5,va='top')
        ax_mm.text(0.005,0.85,f"D {d['status']} | W {w['status']} Net {fmt_big(w['net'],True)} | M {m['status']} Net {fmt_big(m['net'],True)} | TF {tf_label}",transform=ax_mm.transAxes,color='white',fontsize=6,va='top')
    plt.savefig(out,dpi=200,bbox_inches='tight',facecolor='#000000'); plt.close('all'); return out

# TELEGRAM
def send_msg(chat_id, text):
    try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", data={"chat_id":chat_id,"text":text,"parse_mode":"Markdown"}, timeout=10)
    except: pass
def send_photo(chat_id, fp, cap=""):
    try:
        with open(fp,'rb') as f: requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto", data={"chat_id":chat_id,"caption":cap,"parse_mode":"Markdown"}, files={"photo":f}, timeout=20)
    except Exception as e: log(f"photo err {e}")

def process_chart(chat_id, sym, tf="1d"):
    sym=sanitize_symbol(sym); tf=tf.lower() or "1d"
    log(f"/c {sym} {tf}"); send_msg(chat_id, f"RAFANO V6 {sym} TF:{tf.upper()} generating...")
    df=get_history_pro(sym,150,tf)
    if df is None: send_msg(chat_id, f"Data {sym} {tf} tidak ketemu (mungkin delisted)"); return
    multi=get_broker_multi_tf(sym)
    out=f"chart_{sym}_{tf}_{int(time.time())}.png"
    path=gen_chart(df,sym,tf,multi,out)
    if path and os.path.exists(path):
        d=multi['d']; cap=f"*{sym}* {int(df['Close'].iloc[-1])} TF:{tf.upper()}\nD {d['status']} Net {fmt_big(d['net'],True)} Top {fmt_top(d['top_a'])}\nW {multi['w']['status']} Net {fmt_big(multi['w']['net'],True)}\nM {multi['m']['status']} Net {fmt_big(multi['m']['net'],True)}"
        send_photo(chat_id, path, cap)
        try: os.remove(path)
        except: pass
    else: send_msg(chat_id, f"Gagal render {sym}")

def listener():
    log("RAFANO TRADER V6.1 FULL DASHBOARD - LISTENER STARTED - pad 14 - dashboard lengkap")
    try: requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
    except: pass
    offset=0
    while True:
        try:
            r=requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates", params={"offset":offset,"timeout":30}, timeout=35)
            if r.status_code!=200: time.sleep(3); continue
            data=r.json()
            if not data.get('ok'): time.sleep(2); continue
            for upd in data.get('result',[]):
                offset=upd['update_id']+1
                msg=upd.get('message') or upd.get('channel_post')
                if not msg: continue
                text=msg.get('text') or ""; chat_id=str(msg['chat']['id'])
                if text.startswith('/c ') or text.startswith('/C '):
                    parts=text.split(); sym=parts[1] if len(parts)>=2 else ""; tf=parts[2].lower() if len(parts)>=3 else "1d"
                    if sym:
                        import threading; threading.Thread(target=process_chart, args=(chat_id,sym,tf), daemon=True).start()
                elif text.startswith('/start'): send_msg(chat_id, "RAFANO TRADER V6.1\n/c BIPI\n/c BIPI 15m\n/c FILM (fix petik & 422)")
        except Exception as e:
            log(f"listener err {e}"); time.sleep(5)

if __name__=="__main__":
    if not TELEGRAM_BOT_TOKEN or not ARJUM_API_KEY: log("TOKEN KOSONG!")
    else: listener()
