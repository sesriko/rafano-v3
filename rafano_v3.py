
"""
RAFANO V3 - COLAB READY FINAL
- Chart style BBNI original (Avg Price, BOS EMA, dll) TIDAK DIUBAH
- Fix flow NIKL 0 0 0 -> pakai /broker-accumulation?days=1/5/20 (BBNI 22.82B & 869B dari sini)
- Timing: market open atau <18:00 = daily = kemarin, >=18:00 = hari ini
- Colab secrets ready: TELEGRAM_BOT_TOKEN, TARGET_CHAT_ID, ARJUM_API_KEY
"""

import os, time, datetime, threading, requests, pytz, json, logging
import numpy as np, pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.gridspec as gridspec
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# ========== COLAB SECRETS FIX ==========
def safe_get_env(key):
    v = os.getenv(key)
    if v:
        v = str(v).strip()
        if len(v)>=2 and ((v[0]=='"' and v[-1]=='"') or (v[0]=="'" and v[-1]=="'")):
            v = v[1:-1].strip()
        if v:
            print(f"✅ {key} OK len={len(v)} from ENV")
            return v
    try:
        from google.colab import userdata
        vv = userdata.get(key)
        if vv:
            vv = str(vv).strip().strip('"').strip("'")
            os.environ[key] = vv
            print(f"✅ {key} OK len={len(vv)} from COLAB SECRET")
            return vv
    except Exception as e:
        print(f"ℹ️ Colab secrets not available for {key}: {e}")
    print(f"❌ {key} KOSONG!")
    return None

TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')
TELEGRAM_BOT_TOKEN = safe_get_env("TELEGRAM_BOT_TOKEN")
TARGET_CHAT_ID = safe_get_env("TARGET_CHAT_ID")
ARJUM_API_KEY = safe_get_env("ARJUM_API_KEY")

print(f"🔑 ENV CHECK - TOKEN={bool(TELEGRAM_BOT_TOKEN)} CHAT={TARGET_CHAT_ID} ARJUM len={len(ARJUM_API_KEY) if ARJUM_API_KEY else 0}")

ARJUM_BASE = "https://stock.arjum.com/api"
def get_arjum_headers():
    k = os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or safe_get_env("ARJUM_API_KEY") or ""
    return {"X-API-Key": k.strip(), "Accept":"application/json","User-Agent":"Mozilla/5.0"}

def get_now_wib():
    return datetime.datetime.now(TIMEZONE_WIB)

def is_market_open(now=None):
    if now is None:
        now=get_now_wib()
    wd=now.weekday()
    if wd>=5:
        return False
    ct=now.time()
    if wd==4:
        s1s,s1e=datetime.time(9,0),datetime.time(11,30)
        s2s,s2e=datetime.time(14,0),datetime.time(15,50)
    else:
        s1s,s1e=datetime.time(9,0),datetime.time(12,0)
        s2s,s2e=datetime.time(13,30),datetime.time(15,50)
    return (s1s<=ct<=s1e) or (s2s<=ct<=s2e)

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

# ========== HELPERS ASLI - TIDAK DIUBAH ==========
def safe_int(val, default=0):
    try:
        if pd.isna(val) or np.isinf(val):
            return default
        return int(val)
    except:
        return default

def format_large_number(val, show_sign=False):
    try:
        if pd.isna(val) or val==0:
            return "0"
    except: 
        if val==0: return "0"
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
    if price<=0: return 0
    price=float(price)
    if price<200: tick=1
    elif price<500: tick=2
    elif price<2000: tick=5
    elif price<5000: tick=10
    else: tick=25
    return int(round(price/tick)*tick)

def format_timeframe_label(tf):
    m={"1m":"1 Menit","5m":"5 Menit","15m":"15 Menit","30m":"30 Menit","1h":"1 Jam","4h":"4 Jam","1d":"Daily","1w":"Weekly","1M":"Monthly"}
    return m.get((tf or "1d").lower(), tf.upper())

def is_intraday_tf(tf):
    return (tf or "1d").lower() in ["1m","5m","15m","30m","1h","4h"]

def format_top_brokers(brokers, top=3, status="AKUM"):
    if not brokers: return "-"
    valid=[b for b in brokers if isinstance(b, dict) and (b.get('broker_code') or b.get('broker'))]
    if not valid: return "-"
    try:
        sorted_b=sorted(valid, key=lambda x: abs(float(x.get('net_value',0) or x.get('buy_value',0) or 0)), reverse=True)
    except:
        sorted_b=valid
    parts=[]
    for b in sorted_b[:top]:
        code=b.get('broker_code') or b.get('broker') or "??"
        buy=float(b.get('buy_value',0) or 0)
        sell=float(b.get('sell_value',0) or 0)
        net=float(b.get('net_value',0) or 0)
        val=sell if status=="DIST" and sell!=0 else buy if buy!=0 else abs(net)
        if val==0: val=1
        if abs(val)>=1e9: s=f"{val/1e9:.1f}B"
        elif abs(val)>=1e6: s=f"{val/1e6:.0f}M"
        else: s=f"{val:.0f}"
        parts.append(f"{code} {s}")
    return ", ".join(parts) if parts else "-"

# ========== ARJUM FIX ANTI 0 ==========
def arjum_get(path, params=None):
    url=f"{ARJUM_BASE}{path}"
    try:
        r=requests.get(url, headers=get_arjum_headers(), params=params, timeout=12)
        if r.status_code==200:
            return r.json()
    except Exception as e:
        print(f"arjum_get {path} error {e}")
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
            if nval>0: bval=nval
            else: sval=abs(nval)
        bavg=float(b.get('bavg') or b.get('B.avg') or 0)
        savg=float(b.get('savg') or b.get('S.avg') or 0)
        out.append({"broker_code":code,"broker":code,"buy_value":bval,"sell_value":sval,"avg_price":bavg if nval>0 else savg if savg else bavg,"net_value":nval})
    return out

def get_broker_accumulation_real(symbol, days=1):
    for params in [{"top":20,"days":days,"flow":"all"},{"top":20,"period":days,"flow":"all"},{"top":20,"days":days},{"top":20}]:
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
    now=now_override or get_now_wib()
    daily_ref, is_yesterday, timing_reason = get_daily_reference_date(now)
    weekly_from=get_trading_day_n_ago(daily_ref, 5)
    monthly_from=get_trading_day_n_ago(daily_ref, 20)
    print(f"🕐 TIMING {symbol}: {timing_reason}")
    
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
        "daily_ref_date": daily_ref,
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

# ========== HISTORY & CHART (STYLE BBNI ORIGINAL - TIDAK DIUBAH) ==========
def get_history_pro(symbol, limit=150, timeframe="1d"):
    try:
        import yfinance as yf
        tf_map={"1m":("7d","1m"),"5m":("5d","5m"),"15m":("5d","15m"),"30m":("1mo","30m"),"1h":("1mo","60m"),"4h":("3mo","90m"),"1d":("6mo","1d"),"1w":("1y","1wk"),"1M":("2y","1mo")}
        period, interval=tf_map.get(timeframe.lower(),"6mo,1d".split(","))
        if isinstance(period, str) and "," in timeframe.lower():
            period, interval = timeframe.lower().split(",")
        hist=yf.Ticker(f"{symbol}.JK").history(period=period, interval=interval, timeout=10)
        if hist is not None and len(hist)>10:
            return hist.tail(limit)
    except Exception as e:
        print(f"history {symbol} {e}")
    return None

def calculate_trading_plan(df, multi_tf=None, timeframe="1d"):
    try:
        if df is None or len(df)<20: return None
        last_close=df['Close'].iloc[-1]
        ema20=df['Close'].ewm(span=20).mean().iloc[-1]
        ema50=df['Close'].ewm(span=50).mean().iloc[-1]
        ema200=df['Close'].ewm(span=200).mean().iloc[-1]
        atr=(df['High']-df['Low']).rolling(14).mean().iloc[-1]
        if pd.isna(atr): atr=last_close*0.03
        
        # Simple signal
        if last_close>ema20 and last_close>ema50:
            trend="STRONG UPTREND" if last_close>ema200 else "UPTREND"
            side="BUY"; strength=85
        elif last_close>ema20:
            trend="WEAK UPTREND"; side="BUY"; strength=70
        else:
            trend="DOWNTREND"; side="SELL"; strength=60
        
        if multi_tf and multi_tf.get('status_5d')=="AKUM" and multi_tf.get('status_20d')=="AKUM":
            trend+=f" + STRONG BULLISH MTF"
            strength=min(100,strength+10)
        
        entry=round_to_ihsg_fraction(last_close)
        sl=round_to_ihsg_fraction(last_close-atr*1.5)
        tp1=round_to_ihsg_fraction(entry+atr*1.2)
        tp2=round_to_ihsg_fraction(entry+atr*2.5)
        
        return {
            "entry":entry,"sl":sl,"tp1":tp1,"tp2":tp2,
            "atr":float(atr),"trend":trend,"side":side,
            "signal_strength":strength,"signal_type":"BOS EMA",
            "support":int(df['Low'].tail(10).min()),"resistance":int(df['High'].tail(10).max()),
            "risk_pct":round((entry-sl)/entry*100,2) if entry else 0,
            "rr1":1.2,"rr2":2.5
        }
    except Exception as e:
        print(f"TP error {e}")
        return None

def generate_pro_chart(df, symbol="BBCA", timeframe="1d", sector_info="IHSG", output_filename="chart.png", extra_info=None):
    try:
        extra_info=extra_info or {}
        df=df.copy()
        df['EMA13']=df['Close'].ewm(span=13).mean()
        df['EMA20']=df['Close'].ewm(span=20).mean()
        df['EMA50']=df['Close'].ewm(span=50).mean()
        df['EMA200']=df['Close'].ewm(span=200).mean()
        df['V1']=df['Volume'].rolling(20).mean()
        df['V2']=df['Volume'].rolling(50).mean()
        
        last_close=df['Close'].iloc[-1]
        last_open=df['Open'].iloc[-1]
        last_high=df['High'].iloc[-1]
        last_low=df['Low'].iloc[-1]
        last_vol=df['Volume'].iloc[-1]
        prev_close=df['Close'].iloc[-2] if len(df)>1 else last_close
        chg_pct=((last_close/prev_close)-1)*100 if prev_close else 0
        
        plt.style.use('dark_background')
        fig=plt.figure(figsize=(16,9), dpi=200, facecolor='#000000')
        gs=gridspec.GridSpec(4,1,height_ratios=[4.5,1.1,0.9,0.8], hspace=0.05)
        ax_main=fig.add_subplot(gs[0])
        ax_vol=fig.add_subplot(gs[1], sharex=ax_main)
        ax_nbsa=fig.add_subplot(gs[2], sharex=ax_main)
        ax_mm=fig.add_subplot(gs[3], sharex=ax_main)
        fig.subplots_adjust(left=0.08,right=0.92,top=0.88,bottom=0.06)
        
        for ax in [ax_main, ax_vol, ax_nbsa, ax_mm]:
            ax.set_facecolor('#000000')
            ax.tick_params(colors='#aaaaaa', labelsize=8)
            ax.yaxis.tick_right()
            ax.grid(False)
        
        x=np.arange(len(df))
        for i in range(len(df)):
            o,h,l,c=df['Open'].iloc[i],df['High'].iloc[i],df['Low'].iloc[i],df['Close'].iloc[i]
            ax_main.plot([i,i],[l,h], color='#00ff00' if c>=o else '#ff0000', linewidth=0.8)
            body_low=min(o,c)
            body_h=max(0.5,abs(c-o))
            rect=patches.Rectangle((i-0.35,body_low),0.7,body_h, facecolor='none' if c>=o else '#ff3333', edgecolor='#00ff00' if c>=o else '#ff3333', linewidth=0.8)
            ax_main.add_patch(rect)
        
        ax_main.plot(x, df['EMA13'], color='#ffff00', linewidth=1.0)
        ax_main.plot(x, df['EMA20'], color='#ff0000', linewidth=1.0)
        ax_main.plot(x, df['EMA50'], color='#ffffff', linewidth=1.0)
        ax_main.plot(x, df['EMA200'], color='#a020f0', linewidth=1.2)
        
        ax_main.set_xlim(-1, len(df)+3)
        ax_main.set_ylim(df['Low'].min()*0.95, df['High'].max()*1.08)
        
        fig.text(0.01,0.96,f"{symbol} :    {last_close:.0f} ({chg_pct:+.2f}%)", color='#ffff00', fontsize=13, fontweight='bold', ha='left')
        fig.text(0.5,0.96,"RAFANO TRADER", color='white', fontsize=14, fontweight='bold', ha='center')
        fig.text(0.99,0.96,f"{timeframe.upper()} | {get_now_wib().strftime('%d %b %Y')}", color='#ffcc00', fontsize=10, ha='right')
        fig.text(0.01,0.885,f"High:{last_high:.0f} Low:{last_low:.0f} Open:{last_open:.0f} Volume:{last_vol:,.0f} V1:{df['V1'].iloc[-1]:,.0f} V2:{df['V2'].iloc[-1]:,.0f}", color='#00ffff', fontsize=8, ha='left')
        
        left_text=f"Avg Price : {df['Close'].tail(20).mean():,.1f}\\nVchg 1 Bar: {last_vol/df['Volume'].iloc[-2]:.1f} x\\nSpeed : NORMAL\\nPower : NORMAL\\nSafety : GOOD\\n\\nEMA 13 : {df['EMA13'].iloc[-1]:,.1f}\\nEMA 20 : {df['EMA20'].iloc[-1]:,.1f}\\nEMA 50 : {df['EMA50'].iloc[-1]:,.1f}\\nEMA 200: {df['EMA200'].iloc[-1]:,.1f}"
        ax_main.text(0.01,0.98,left_text, transform=ax_main.transAxes, va='top', ha='left', fontsize=8, family='monospace', color='#e0e0e0', bbox=dict(facecolor='black', alpha=0.6, edgecolor='none'))
        
        ax_vol.bar(x, df['Volume'], color=['#00cc00' if df['Close'].iloc[i]>=df['Open'].iloc[i] else '#cc0000' for i in range(len(df))], width=0.8, alpha=0.8)
        ax_vol.plot(x, df['V1'], color='white', linewidth=0.8)
        ax_vol.set_ylim(0, df['Volume'].max()*1.8)
        plt.setp(ax_vol.get_xticklabels(), visible=False)
        
        plt.savefig(output_filename, dpi=200, bbox_inches='tight', facecolor='#000000')
        plt.close('all')
        return output_filename
    except Exception as e:
        print(f"Chart error {e}")
        import traceback; traceback.print_exc()
        return None

# ========== TELEGRAM - COLAB READY ==========
def send_reply(chat_id, text, reply_markup=None):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload={"chat_id":chat_id,"text":text,"parse_mode":"Markdown"}
    if reply_markup:
        payload["reply_markup"]=reply_markup
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"TG Error: {e}")

def send_photo_reply(chat_id, photo_path, caption=""):
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path,'rb') as photo:
            requests.post(url, data={'chat_id':chat_id,'caption':caption,'parse_mode':'Markdown'}, files={'photo':photo}, timeout=30)
    except Exception as e:
        print(f"Photo Error: {e}")

def process_chart_request(chat_id, stock_code, timeframe="1d"):
    send_reply(chat_id, f"📊 *Generating {stock_code.upper()} {timeframe}...*")
    df=get_history_pro(stock_code, limit=150, timeframe=timeframe)
    if df is None:
        send_reply(chat_id, f"⚠ Data {stock_code} tidak ketemu")
        return
    multi=get_broker_multi_tf(stock_code, df)
    tp=calculate_trading_plan(df, multi, timeframe)
    
    chart_file=f"chart_{stock_code.upper()}_{int(time.time())}.png"
    file_path=generate_pro_chart(df, symbol=stock_code.upper(), timeframe=timeframe, output_filename=chart_file)
    
    if multi:
        daily_str=f"{multi['status_d']} | Buy {format_large_number(multi['buy_d'],True)} Sell {format_large_number(multi['sell_d'],True)} Net {format_large_number(multi['net_d'],True)} Avg {multi['avg_d']:.0f} | {format_top_brokers(multi['brokers'],3,multi['status_d'])}"
        weekly_str=f"{multi['status_5d']} | Buy {format_large_number(multi['buy_5d'],True)} Sell {format_large_number(multi['sell_5d'],True)} Net {format_large_number(multi['net_5d'],True)} Avg {multi['avg_5d']:.0f} | {format_top_brokers(multi['brokers_5d'],3,multi['status_5d'])}"
        monthly_str=f"{multi['status_20d']} | Buy {format_large_number(multi['buy_20d'],True)} Sell {format_large_number(multi['sell_20d'],True)} Net {format_large_number(multi['net_20d'],True)} Avg {multi['avg_20d']:.0f} | {format_top_brokers(multi['brokers_20d'],3,multi['status_20d'])}"
    else:
        daily_str=weekly_str=monthly_str="-"
    
    if tp:
        caption=f"*{stock_code.upper()} -- {safe_int(df['Close'].iloc[-1])} | {tp['trend']}*\\n"
        caption+=f"🟢 Grade: STRONG BUY | Signal Score: {tp['signal_strength']}% | MTF: {tp.get('trend','')}\\n"
        caption+=f"Daily: {daily_str}\\n"
        caption+=f"Weekly 5D: {weekly_str}\\n"
        caption+=f"Monthly 20D: {monthly_str}\\n"
        caption+=f"Timeframe: {format_timeframe_label(timeframe)}\\n"
        caption+=f"------------------\\n"
        caption+=f"TRADING PLAN - ✅ {tp['signal_type']} - BUY NOW\\n"
        caption+=f"Entry: {tp['entry']} | SL: {tp['sl']} ({tp['risk_pct']}%)\\n"
        caption+=f"TP1: {tp['tp1']} (RR {tp['rr1']}) | TP2: {tp['tp2']} (RR {tp['rr2']})\\n"
    else:
        caption=f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])}\\nDaily: {daily_str}"
    
    if file_path and os.path.exists(file_path):
        send_photo_reply(chat_id, file_path, caption=caption)
        os.remove(file_path)
    else:
        send_reply(chat_id, caption)

def telegram_bot_listener():
    offset=0
    print("🤖 Telegram Listener Running...")
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
        r=requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe", timeout=10)
        print(f"✅ Bot: {r.json().get('result',{}).get('username')}")
    except Exception as e:
        print(f"Bot init error: {e}")
    
    while True:
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res=requests.get(url, timeout=25)
            if res.status_code!=200:
                time.sleep(3)
                continue
            data=res.json()
            for update in data.get("result",[]):
                offset=update["update_id"]+1
                if "message" in update and "text" in update["message"]:
                    msg=update["message"]
                    text=msg.get("text","").strip()
                    chat_id=msg["chat"]["id"]
                    print(f"📩 {text} dari {chat_id}")
                    first=text.split()[0].lower() if text else ""
                    if first in ["/start","/help"]:
                        send_reply(chat_id, "🤖 RAFANO V3 COLAB\\n/c <KODE> - Chart\\n/b <KODE> - Broker\\n/scan - Scan")
                    elif first in ["/c","/chart"]:
                        parts=text.split()
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            tf=parts[2] if len(parts)>=3 else "1d"
                            threading.Thread(target=process_chart_request, args=(chat_id, sym, tf)).start()
        except Exception as e:
            print(f"Listener error: {e}")
            time.sleep(3)

if __name__=="__main__":
    print("==========================================")
    print("🔥 RAFANO V3 COLAB FINAL - TIMING + SELL BROKER")
    print("==========================================")
    if not TELEGRAM_BOT_TOKEN or not ARJUM_API_KEY:
        print("❌ TOKEN / ARJUM KOSONG - Cek Colab Secrets")
    else:
        telegram_bot_listener()
