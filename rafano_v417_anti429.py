
"""
RAFANO V4.17 ITICK REALTIME + CHART VISI + TELEGRAM - FINAL
"""
import os, time, sqlite3, datetime, requests, threading, pytz, glob
import pandas as pd, numpy as np
from dotenv import load_dotenv
for p in ['/content/rafano-v3/.env','./.env','.env','/content/.env']:
    if os.path.exists(p):
        load_dotenv(p, override=True)
        break
else:
    load_dotenv()
TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')
def now_wib(): return datetime.datetime.now(TIMEZONE_WIB)
DB_PATH = os.getenv("RAFANO_DB_PATH") or "rafano.db"
CHART_DIR = "charts_v417"
os.makedirs(CHART_DIR, exist_ok=True)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or ""
TARGET_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("TARGET_CHAT_ID") or ""
ARJUM_API_KEY = os.getenv("ARJUM_API_KEY") or ""
ARJUM_BASE = "https://stock.arjum.com/api"
ITICK_TOKEN = os.getenv("ITICK_TOKEN") or "7a470a83276242309fb940684046d35a88e450fdb95b46c383670e1e0c5e96f5"
ITICK_BASE = "https://api.itick.org"
ITICK_ENABLED = bool(ITICK_TOKEN)

def get_itick_realtime(symbols, batch_size=50):
    if not ITICK_ENABLED or not symbols: return {}
    out={}
    for i in range(0, len(symbols), batch_size):
        batch=symbols[i:i+batch_size]
        try:
            codes=",".join(batch)
            url=f"{ITICK_BASE}/stock/quotes?region=ID&codes={codes}"
            r=requests.get(url, headers={"accept":"application/json","token":ITICK_TOKEN}, timeout=12)
            if r.status_code==200:
                for item in r.json().get('data',[]):
                    code=str(item.get('s') or item.get('code') or "").upper().replace(".JK","")
                    close=float(item.get('c') or 0)
                    if close>0:
                        out[code]={"close":close,"change_pct":float(item.get('ch') or 0),"volume":float(item.get('v') or 0),"high":float(item.get('h') or close),"low":float(item.get('l') or close),"open":float(item.get('o') or close)}
        except: pass
        time.sleep(0.4)
    return out

def get_df(symbol, limit=200, use_itick=True):
    conn=sqlite3.connect(DB_PATH)
    df=pd.read_sql_query(f"SELECT date,open,high,low,close,volume FROM ohlcv WHERE symbol='{symbol}' ORDER BY date ASC", conn)
    conn.close()
    if df.empty or len(df)<30: return None, pd.DataFrame(), pd.DataFrame()
    df['date']=pd.to_datetime(df['date']); df.set_index('date', inplace=True); df=df.tail(limit).copy()
    if use_itick and ITICK_ENABLED and 9 <= now_wib().hour < 16 and now_wib().weekday()<5:
        rt=get_itick_realtime([symbol],1)
        if symbol in rt:
            rp=rt[symbol]
            if df.index[-1].strftime("%Y-%m-%d")==now_wib().strftime("%Y-%m-%d"):
                df.iloc[-1, df.columns.get_loc('close')]=rp['close']
                df.iloc[-1, df.columns.get_loc('volume')]=max(df.iloc[-1]['volume'], rp['volume'])
    df['EMA13']=df['close'].ewm(span=13, adjust=False, min_periods=13).mean()
    df['EMA20']=df['close'].ewm(span=20, adjust=False, min_periods=20).mean()
    df['EMA50']=df['close'].ewm(span=50, adjust=False, min_periods=50).mean()
    df['EMA200']=df['close'].ewm(span=200, adjust=False, min_periods=30).mean()
    df['BB_MA']=df['close'].rolling(20).mean(); df['BB_STD']=df['close'].rolling(20).std()
    df['BB_UP']=df['BB_MA']+2*df['BB_STD']; df['BB_LOW']=df['BB_MA']-2*df['BB_STD']
    df['VOL_MA20']=df['volume'].rolling(20, min_periods=10).mean()
    df['VCHG_1']=df['volume']/df['volume'].shift(1).replace(0,np.nan); df['VCHG_1']=df['VCHG_1'].fillna(1.0)
    df['VCHG_5']=df['volume']/df['volume'].rolling(5, min_periods=3).mean()
    df['buy_vol']=np.where(df['close']>=df['open'], df['volume']*0.6, df['volume']*0.4)
    df['sell_vol']=df['volume']-df['buy_vol']
    return df, pd.DataFrame(), pd.DataFrame()

def scan_top10(use_itick=True, vol_thr=1.3, near_pct=0.95):
    conn=sqlite3.connect(DB_PATH)
    symbols=[r[0] for r in conn.execute("SELECT DISTINCT symbol FROM ohlcv").fetchall()]
    conn.close()
    realtime_map=get_itick_realtime(symbols,50) if use_itick else {}
    cands=[]
    for sym in symbols:
        try:
            df,_,_=get_df(sym,200,False)
            if df is None: continue
            last=df.iloc[-1]; prev=df.iloc[-2]
            rt_close=realtime_map.get(sym,{}).get('close',last['close'])
            rt_vol=realtime_map.get(sym,{}).get('volume',last['volume'])
            rt_chg=realtime_map.get(sym,{}).get('change_pct',(rt_close/prev['close']-1)*100)
            if pd.isna(last['EMA20']): continue
            if rt_close>=last['EMA20']*near_pct and rt_vol>=last['VOL_MA20']*0.9 and rt_vol/prev['volume']>=vol_thr:
                df_plot=df.copy()
                df_plot.iloc[-1, df_plot.columns.get_loc('close')]=rt_close
                cands.append({"symbol":sym,"close":rt_close,"chg":rt_chg,"vchg_1":rt_vol/prev['volume'],"score":rt_vol/prev['volume'],"df":df_plot,"foreign_str":f"+{rt_vol/prev['volume']:.1f}xVol","accum_type":"LIVE BO ITICK" if rt_close>last['EMA20'] else "VOL SURGE","bandar":pd.DataFrame(),"is_live":sym in realtime_map})
        except: continue
    cands=sorted(cands, key=lambda x: x['score'], reverse=True)[:10]
    print(f"TOP {len(cands)}")
    for c in cands: print(f"{c['symbol']} {c['close']:.0f} {c['chg']:+.1f}% Vol {c['vchg_1']:.1f}x")
    return cands

def plot_v417(c):
    import matplotlib; matplotlib.use('Agg')
    import matplotlib.pyplot as plt, matplotlib.gridspec as gridspec
    from matplotlib.patches import Rectangle
    sym=c['symbol']; df=c['df']
    fig=plt.figure(figsize=(16,9), facecolor='black')
    gs=gridspec.GridSpec(4,1,height_ratios=[3.5,0.8,0.8,0.8], hspace=0.05)
    ax1=plt.subplot(gs[0], facecolor='black'); ax2=plt.subplot(gs[1], facecolor='black', sharex=ax1)
    ax3=plt.subplot(gs[2], facecolor='black', sharex=ax1); ax4=plt.subplot(gs[3], facecolor='black', sharex=ax1)
    x=np.arange(len(df))
    for i in range(len(df)):
        o,h,l,cl=df['open'].iloc[i],df['high'].iloc[i],df['low'].iloc[i],df['close'].iloc[i]
        color='#00ff00' if cl>=o else '#ff3333'
        ax1.plot([i,i],[l,h],color=color,linewidth=0.8)
        ax1.add_patch(Rectangle((i-0.3,min(o,cl)),0.6,max(abs(cl-o),0.5),facecolor=color,edgecolor=color))
    ax1.plot(x,df['EMA13'],color='white',linewidth=0.8); ax1.plot(x,df['EMA20'],color='yellow',linewidth=1.0)
    ax1.plot(x,df['EMA50'],color='red',linewidth=1.0); ax1.plot(x,df['EMA200'],color='#aa00ff',linewidth=1.2)
    fig.suptitle("RAFANO V4.17 ITICK", color='white', fontsize=14, fontweight='bold', y=0.98)
    fig.text(0.005,0.96,f"{sym} : {df['close'].iloc[-1]:.0f} ({c['chg']:+.2f}%) {'[LIVE]' if c.get('is_live') else ''}", color='yellow', fontsize=14, fontweight='bold')
    fig.text(0.995,0.96,f"{now_wib().strftime('%d %b %Y %H:%M')} | {c['accum_type']} {c['foreign_str']}", color='#ffcc00', fontsize=9, ha='right')
    ax2.bar(x,df['volume']/1e6,color='#00aa00',width=0.8); ax2.plot(x,df['VOL_MA20']/1e6,color='white',linewidth=0.7)
    plt.tight_layout(rect=[0,0,1,0.95])
    save_path=os.path.join(CHART_DIR,f"{sym}_V417_ITICK.png")
    plt.savefig(save_path,dpi=180,facecolor='black',bbox_inches='tight'); plt.close()
    return save_path

def send_telegram(chat_id,text,photo_path=None):
    if not TELEGRAM_BOT_TOKEN: return
    try:
        if photo_path and os.path.exists(photo_path):
            with open(photo_path,'rb') as f:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto", data={"chat_id":chat_id,"caption":text,"parse_mode":"Markdown"}, files={"photo":f}, timeout=20)
        else:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json={"chat_id":chat_id,"text":text,"parse_mode":"Markdown"}, timeout=15)
    except: pass

def process_chart_request(chat_id,symbol):
    symbol=symbol.upper().replace(".JK","")
    df,_,_=get_df(symbol,200,True)
    if df is None:
        send_telegram(chat_id,f"❌ {symbol} gak ada di DB")
        return
    last=df.iloc[-1]; prev=df.iloc[-2]; chg=(last['close']/prev['close']-1)*100
    c={"symbol":symbol,"close":last['close'],"chg":chg,"foreign_str":f"+{last['VCHG_1']:.1f}xVol","accum_type":"VOL SURGE","df":df,"bandar":pd.DataFrame(),"is_live":True}
    path=plot_v417(c)
    send_telegram(chat_id,f"📈 *{symbol}* {last['close']:.0f} ({chg:+.2f}%) Vol {last['VCHG_1']:.1f}x", photo_path=path)

def telegram_listener():
    offset=0
    print(f"🤖 BOT START {now_wib()}")
    try: requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
    except: pass
    while True:
        try:
            res=requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=30", timeout=35)
            if res.status_code!=200: time.sleep(3); continue
            for upd in res.json().get("result",[]):
                offset=upd["update_id"]+1
                if "message" in upd and "text" in upd["message"]:
                    txt=upd["message"]["text"].strip(); chat_id=upd["message"]["chat"]["id"]
                    parts=txt.split(); first=parts[0].lower() if parts else ""
                    if first in ["/start","/help"]: send_telegram(chat_id,"/c VISI - Chart LIVE itick\n/scanbo - TOP 10 LIVE\n/db - cek DB")
                    elif first in ["/c","/chart"] and len(parts)>=2:
                        threading.Thread(target=process_chart_request, args=(chat_id,parts[1].upper())).start()
                    elif first in ["/scanbo","/topbo","/vol","/db"]:
                        vol_thr=float(parts[1]) if len(parts)>=2 else 1.3
                        send_telegram(chat_id,f"🚀 SCAN ITICK Vol>{vol_thr}x ~20 detik")
                        def run_scan(tg=chat_id, vt=vol_thr):
                            top10=scan_top10(True, vt)
                            msg=f"🔥 TOP {len(top10)} ITICK {now_wib().strftime('%H:%M WIB')}\n\n"
                            for i,c in enumerate(top10,1):
                                msg+=f"{i}. {'🔴' if c.get('is_live') else '⚪'} *{c['symbol']}* {c['close']:.0f} ({c['chg']:+.1f}%) Vol {c['vchg_1']:.1f}x\n"
                            send_telegram(tg,msg)
                            for c in top10:
                                try:
                                    p=plot_v417(c)
                                    send_telegram(tg,f"*{c['symbol']}*", photo_path=p)
                                    time.sleep(1)
                                except: pass
                        threading.Thread(target=run_scan).start()
        except Exception as e:
            print(e); time.sleep(5)

if __name__=="__main__":
    if not TELEGRAM_BOT_TOKEN:
        top10=scan_top10(True,1.3)
        for c in top10: plot_v417(c)
    else:
        telegram_listener()
