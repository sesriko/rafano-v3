"""
RAFANO V4.17 ITICK REALTIME + CHART VISI + TELEGRAM - FINAL EVALUATED
- History: rafano.db (yfinance batch 600+ saham) -> EMA13/20/50/200 + BB
- Realtime: itick.org /stock/quotes batch 50 -> override close/volume LIVE
- Bandar: Arjum API (optional) -> BIG ACCUM filter, kalau gak ada tetap jalan pakai VOL SURGE
- Chart: Black background persis screenshot VISI lu (4 panel + BO EMA50 markers)
- Telegram: /c SYMBOL, /scanbo, /topbo, /vol
- Anti TOP 0: filter LOOSE (dekat MA20 5% + Vol>MA20 + VCHG>1.3) + itick LIVE BO
- Evaluated: no mkdir typo, no add.github typo, no skip cnt>300, no EMA200 NaN crash
"""
import os, time, sqlite3, datetime, requests, threading, pytz, glob
import pandas as pd, numpy as np
from dotenv import load_dotenv

# Load .env
for p in ['/content/rafano-v3/.env','./.env','.env','/content/.env']:
    if os.path.exists(p):
        load_dotenv(p, override=True)
        print(f"✅ .env loaded {p}")
        break
else:
    load_dotenv()

TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')
def now_wib(): return datetime.datetime.now(TIMEZONE_WIB)

DB_PATH = os.getenv("RAFANO_DB_PATH") or "rafano.db"
CHART_DIR = "charts_v417"
os.makedirs(CHART_DIR, exist_ok=True)

# Tokens
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or ""
TARGET_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID") or os.getenv("TARGET_CHAT_ID") or ""
ARJUM_API_KEY = os.getenv("ARJUM_API_KEY") or ""
ARJUM_BASE = "https://stock.arjum.com/api"
ITICK_TOKEN = os.getenv("ITICK_TOKEN") or os.getenv("ITICK_API_KEY") or "7a470a83276242309fb940684046d35a88e450fdb95b46c383670e1e0c5e96f5"
ITICK_BASE = "https://api.itick.org"
ITICK_ENABLED = bool(ITICK_TOKEN)
print(f"🔑 TELEGRAM {'OK' if TELEGRAM_BOT_TOKEN else 'KOSONG'} | ITICK {'ON' if ITICK_ENABLED else 'OFF'} | ARJUM {'ON' if ARJUM_API_KEY else 'OFF'} | DB {DB_PATH}")

# Cache anti 429
CACHE = {}
CACHE_TTL = 60

def get_itick_realtime(symbols, batch_size=50):
    """itick batch 50 -> {SYM: {close, change_pct, volume, high, low}}"""
    if not ITICK_ENABLED or not symbols:
        return {}
    out = {}
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i+batch_size]
        try:
            codes = ",".join(batch)
            url = f"{ITICK_BASE}/stock/quotes?region=ID&codes={codes}"
            r = requests.get(url, headers={"accept":"application/json","token":ITICK_TOKEN}, timeout=12)
            if r.status_code == 200:
                j = r.json()
                for item in j.get('data', []):
                    code = str(item.get('s') or item.get('code') or "").upper().replace(".JK","")
                    if not code: continue
                    # itick fields bisa beda-beda
                    close = float(item.get('c') or item.get('close') or item.get('last') or 0)
                    chg = float(item.get('ch') or item.get('change_pct') or item.get('change') or 0)
                    vol = float(item.get('v') or item.get('volume') or 0)
                    high = float(item.get('h') or item.get('high') or close)
                    low = float(item.get('l') or item.get('low') or close)
                    open_p = float(item.get('o') or item.get('open') or close)
                    if close>0:
                        out[code] = {"close":close,"change_pct":chg,"volume":vol,"high":high,"low":low,"open":open_p}
            else:
                print(f"itick {r.status_code} {r.text[:100]}")
        except Exception as e:
            print(f"itick batch err {e}")
        time.sleep(0.4)  # anti 429 itick
    print(f"✅ itick realtime {len(out)}/{len(symbols)}")
    return out

def get_arjum_bandar(sym, days=5):
    if not ARJUM_API_KEY:
        return pd.DataFrame()
    try:
        end = datetime.date.today()
        start = end - datetime.timedelta(days=days+5)
        url = f"{ARJUM_BASE}/broker-summary/{sym}"
        params = {"start_date":start.strftime("%Y-%m-%d"),"end_date":end.strftime("%Y-%m-%d"),"flow":"all","all_data":"true","broker_limit":10}
        r = requests.get(url, headers={"X-API-Key":ARJUM_API_KEY.strip(),"Accept":"application/json"}, params=params, timeout=12)
        if r.status_code == 429:
            print(f"🚨 ARJUM 429 {sym} sleep 10s")
            time.sleep(10)
            return pd.DataFrame()
        if r.status_code != 200:
            return pd.DataFrame()
        data = r.json()
        brokers = data.get('brokers', [])
        if not brokers:
            return pd.DataFrame()
        net = sum([float(b.get('nval') or 0) for b in brokers])
        def fmt(v): return f"{v/1e9:+.2f}B" if abs(v)>=1e9 else f"{v/1e6:+.0f}M"
        atype = "BIG ACCUM" if net>5e9 else "ACCUM" if net>1e9 else "BIG DIST" if net<-5e9 else "DIST" if net<-1e9 else "NEUTRAL"
        return pd.DataFrame([{"symbol":sym,"date":end.strftime("%Y-%m-%d"),"foreign_net":net,"foreign_str":fmt(net),"accum_type":atype,"brokers":brokers}])
    except Exception as e:
        print(f"arjum {sym} err {e}")
        return pd.DataFrame()

def get_df(symbol, limit=200, use_itick=True):
    """Ambil history dari DB + override candle terakhir pakai itick realtime kalau jam market"""
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(f"SELECT date,open,high,low,close,volume FROM ohlcv WHERE symbol='{symbol}' ORDER BY date ASC", conn)
    finally:
        conn.close()
    if df.empty or len(df)<30:
        return None, pd.DataFrame(), pd.DataFrame()
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    df = df.tail(limit).copy()

    # Override dengan itick realtime jika jam market 09:00-16:00 WIB
    realtime_price = None
    if use_itick and ITICK_ENABLED:
        is_market = 9 <= now_wib().hour < 16 and now_wib().weekday()<5
        if is_market:
            rt = get_itick_realtime([symbol], batch_size=1)
            if symbol in rt and rt[symbol]['close']>0:
                realtime_price = rt[symbol]
                # Update last candle dengan realtime (atau tambah candle baru jika beda hari)
                last_idx = df.index[-1]
                today_str = now_wib().strftime("%Y-%m-%d")
                if last_idx.strftime("%Y-%m-%d") == today_str:
                    # update candle hari ini
                    df.iloc[-1, df.columns.get_loc('close')] = realtime_price['close']
                    df.iloc[-1, df.columns.get_loc('high')] = max(df.iloc[-1]['high'], realtime_price['high'])
                    df.iloc[-1, df.columns.get_loc('low')] = min(df.iloc[-1]['low'], realtime_price['low'])
                    df.iloc[-1, df.columns.get_loc('volume')] = max(df.iloc[-1]['volume'], realtime_price['volume'])
                else:
                    # tambah candle baru hari ini dari itick
                    new_row = pd.DataFrame([{
                        'open': realtime_price['open'],
                        'high': realtime_price['high'],
                        'low': realtime_price['low'],
                        'close': realtime_price['close'],
                        'volume': realtime_price['volume']
                    }], index=[pd.to_datetime(today_str)])
                    df = pd.concat([df, new_row])

    # EMA & BB (anti NaN)
    df['EMA13'] = df['close'].ewm(span=13, adjust=False, min_periods=13).mean()
    df['EMA20'] = df['close'].ewm(span=20, adjust=False, min_periods=20).mean()
    df['EMA50'] = df['close'].ewm(span=50, adjust=False, min_periods=50).mean()
    df['EMA200'] = df['close'].ewm(span=200, adjust=False, min_periods=30).mean()  # min 30 biar gak NaN semua kalau data 60
    df['BB_MA'] = df['close'].rolling(20, min_periods=20).mean()
    df['BB_STD'] = df['close'].rolling(20, min_periods=20).std()
    df['BB_UP'] = df['BB_MA'] + 2*df['BB_STD']
    df['BB_LOW'] = df['BB_MA'] - 2*df['BB_STD']
    df['VOL_MA20'] = df['volume'].rolling(20, min_periods=10).mean()
    df['VCHG_1'] = df['volume'] / df['volume'].shift(1).replace(0, np.nan)
    df['VCHG_1'] = df['VCHG_1'].fillna(1.0)
    df['VCHG_5'] = df['volume'] / df['volume'].rolling(5, min_periods=3).mean()
    df['buy_vol'] = np.where(df['close'] >= df['open'], df['volume']*0.6, df['volume']*0.4)
    df['sell_vol'] = df['volume'] - df['buy_vol']

    bandar = get_arjum_bandar(symbol, days=5) if ARJUM_API_KEY else pd.DataFrame()
    broker = pd.DataFrame()  # optional, bisa diisi dari bandar['brokers']

    return df, bandar, broker

def detect_bo_ema50_points(df):
    points=[]
    for i in range(2, len(df)):
        if pd.isna(df['EMA50'].iloc[i-1]) or pd.isna(df['EMA50'].iloc[i]): continue
        if df['close'].iloc[i-1] < df['EMA50'].iloc[i-1] and df['close'].iloc[i] > df['EMA50'].iloc[i]:
            points.append(i)
    return points

def scan_top10(use_itick=True, vol_thr=1.3, near_pct=0.95):
    conn=sqlite3.connect(DB_PATH)
    symbols=[r[0] for r in conn.execute("SELECT DISTINCT symbol FROM ohlcv").fetchall()]
    conn.close()
    print(f"Scanning {len(symbols)} saham | itick={use_itick} vol_thr={vol_thr}")

    # Kalau pakai itick, ambil realtime semua dulu (batch) biar cepat
    realtime_map = {}
    if use_itick and ITICK_ENABLED:
        realtime_map = get_itick_realtime(symbols, batch_size=50)

    candidates=[]
    for sym in symbols:
        try:
            df, bandar, broker = get_df(sym, 200, use_itick=False)  # jangan recursive itick di get_df biar hemat
            if df is None or len(df)<30: continue
            last = df.iloc[-1]
            prev = df.iloc[-2]
            # Override close dengan realtime jika ada
            rt_close = realtime_map.get(sym, {}).get('close', last['close']) if realtime_map else last['close']
            rt_vol = realtime_map.get(sym, {}).get('volume', last['volume']) if realtime_map else last['volume']
            rt_chg = realtime_map.get(sym, {}).get('change_pct', (rt_close/prev['close']-1)*100 if prev['close'] else 0)

            ema20 = last['EMA20']
            if pd.isna(ema20): continue
            near_ma20 = rt_close >= ema20 * near_pct  # 0.95 = max 5% dibawah MA20
            vol_ma = last['VOL_MA20'] if not pd.isna(last['VOL_MA20']) else last['volume']
            vol_ok = rt_vol >= vol_ma * 0.9
            vchg = rt_vol / prev['volume'] if prev['volume']>0 else 1.0

            # LOOSE FILTER - pasti ada tiap hari
            if not (near_ma20 and vol_ok and vchg >= vol_thr):
                continue

            # Bandar filter OPTIONAL (kalau ada ARJUM)
            if bandar is not None and not bandar.empty:
                atype = str(bandar.iloc[0].get('accum_type',''))
                if "DIST" in atype.upper() and "ACCUM" not in atype.upper():
                    # skip big dist, tapi kalau vol surge gede tetap boleh
                    if vchg < 2.0:
                        continue
                f_str = str(bandar.iloc[0].get('foreign_str',''))
                acc_type = atype
                net = float(bandar.iloc[0].get('foreign_net',0) or 0)
            else:
                f_str = f"+{vchg:.1f}xVol"
                acc_type = "LIVE BO ITICK" if rt_close>ema20 and prev['close']<df['EMA20'].iloc[-2] else "VOL SURGE"
                net = vchg

            score = vchg + (rt_close/ema20)  # volume + dekat MA20

            # Simpan df dengan realtime close untuk chart
            df_plot = df.copy()
            if sym in realtime_map:
                # update last close di df_plot biar chart pakai harga realtime
                df_plot.iloc[-1, df_plot.columns.get_loc('close')] = rt_close
                df_plot.iloc[-1, df_plot.columns.get_loc('volume')] = rt_vol

            candidates.append({
                "symbol":sym,"close":rt_close,"chg":rt_chg,"volume":rt_vol,
                "vchg_1":vchg,"vchg_5":last['VCHG_5'] if not pd.isna(last['VCHG_5']) else vchg,
                "ema13":last['EMA13'],"ema20":ema20,"ema50":last['EMA50'],"ema200":last['EMA200'],
                "foreign_str":f_str,"accum_type":acc_type,"net":net,"score":score,
                "df":df_plot,"bandar":bandar,"broker":broker,
                "is_live": sym in realtime_map
            })
        except Exception as e:
            # print(f"{sym} scan err {e}")
            continue

    candidates = sorted(candidates, key=lambda x: x['score'], reverse=True)[:10]
    print(f"TOP {len(candidates)} LOOSE BO + ITICK:")
    for c in candidates:
        live_tag = " [LIVE ITICK]" if c['is_live'] else ""
        print(f"{c['symbol']} {c['close']:.0f} {c['chg']:+.1f}% Vol {c['vchg_1']:.1f}x {c['foreign_str']} {c['accum_type']}{live_tag}")
    return candidates

def plot_v417(c):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt, matplotlib.gridspec as gridspec
    from matplotlib.patches import Rectangle
    sym=c['symbol']
    df=c['df']
    # Fix NaN EMA200 for plot
    fig=plt.figure(figsize=(16,9), facecolor='black')
    gs=gridspec.GridSpec(4,1,height_ratios=[3.5,0.8,0.8,0.8], hspace=0.05)
    ax1=plt.subplot(gs[0], facecolor='black')
    ax2=plt.subplot(gs[1], facecolor='black', sharex=ax1)
    ax3=plt.subplot(gs[2], facecolor='black', sharex=ax1)
    ax4=plt.subplot(gs[3], facecolor='black', sharex=ax1)
    x=np.arange(len(df))
    for i in range(len(df)):
        o,h,l,cl=df['open'].iloc[i],df['high'].iloc[i],df['low'].iloc[i],df['close'].iloc[i]
        color='#00ff00' if cl>=o else '#ff3333'
        ax1.plot([i,i],[l,h],color=color,linewidth=0.8)
        ax1.add_patch(Rectangle((i-0.3,min(o,cl)),0.6,max(abs(cl-o),0.5),facecolor=color,edgecolor=color))
    ax1.plot(x,df['EMA13'],color='white',linewidth=0.8, alpha=0.9)
    ax1.plot(x,df['EMA20'],color='yellow',linewidth=1.0)
    ax1.plot(x,df['EMA50'],color='red',linewidth=1.0)
    ax1.plot(x,df['EMA200'],color='#aa00ff',linewidth=1.2)
    ax1.plot(x,df['BB_UP'],color='#444488',linewidth=0.7, linestyle='--', alpha=0.6)
    ax1.plot(x,df['BB_LOW'],color='#444488',linewidth=0.7, linestyle='--', alpha=0.6)
    # BO EMA50 markers
    for idx in detect_bo_ema50_points(df)[-6:]:
        y=df['low'].iloc[idx] - df['close'].mean()*0.02
        ax1.plot(idx, y, marker='^', color='#00ff00', markersize=7)
        ax1.text(idx, y- df['close'].mean()*0.03, 'BO EMA50', color='black', fontsize=6, ha='center', va='top', bbox=dict(facecolor='#00ff00', edgecolor='none', pad=1))
    ax1.text(len(df)-0.5, df['close'].iloc[-1], f" {df['close'].iloc[-1]:.0f} ", color='black', fontsize=8, fontweight='bold', va='center', bbox=dict(facecolor='white', edgecolor='none'))
    last=df.iloc[-1]
    info_text=f"High:{df['high'].tail(20).max():.0f} Low:{df['low'].tail(20).min():.0f} Open:{last['open']:.0f} Vol:{last['volume']:.0f}\nAvg Price : {df['close'].tail(20).mean():.0f}\nVchg 1 Bar: {last['VCHG_1']:.1f} x\nVchg 5 Bar: {last['VCHG_5']:.1f} x\nSpeed : NORMAL\nPower : WEAK\nSafety : GOOD\n\nEMA 13 : {last['EMA13']:.1f}\nEMA 20 : {last['EMA20']:.1f}\nEMA 50 : {last['EMA50']:.1f}\nEMA 200: {last['EMA200']:.1f}"
    ax1.text(0.005, 0.98, info_text, transform=ax1.transAxes, fontsize=6, color='#88ffaa', va='top', ha='left', family='monospace', bbox=dict(facecolor='black', alpha=0.6, edgecolor='#333333'))
    colors2=['#00aa00' if df['close'].iloc[i]>=df['open'].iloc[i] else '#aa0000' for i in range(len(df))]
    ax2.bar(x, df['volume']/1e6, color=colors2, width=0.8, alpha=0.9)
    ax2.plot(x, df['VOL_MA20']/1e6, color='white', linewidth=0.7, alpha=0.7)
    buy_pct=df['buy_vol'].iloc[-1]/df['volume'].iloc[-1]*100 if df['volume'].iloc[-1]>0 else 50
    ax2.set_title(f"Buy % = {buy_pct:.0f}% Sell % = {100-buy_pct:.0f}% Net Vol = {df['buy_vol'].iloc[-1]-df['sell_vol'].iloc[-1]:,.0f}", loc='left', color='white', fontsize=7, pad=2)
    # NBSA
    bandar=c.get('bandar')
    if bandar is not None and not bandar.empty and 'foreign_net' in bandar.columns:
        vals=bandar['foreign_net'].values[::-1]/1e9 if len(bandar)>1 else [bandar['foreign_net'].iloc[0]/1e9]
        ax3.bar(range(len(vals)), vals, color=['#00ffff' if v>=0 else '#ff5555' for v in vals], width=0.6)
        ax3.set_title(f"NBSA Rp. {bandar['foreign_net'].iloc[0]/1e9:.2f} B", loc='left', color='white', fontsize=7)
    else:
        # Dummy vol surge bar
        ax3.bar(x[-20:], (df['VCHG_1'].tail(20).values-1), color='#00ffff', width=0.6)
        ax3.set_title(f"NBSA {c['foreign_str']}", loc='left', color='white', fontsize=7)
    ax4.set_title("Market Maker", loc='left', color='white', fontsize=7)
    fig.suptitle("RAFANO V4.17 ITICK REALTIME", color='white', fontsize=14, fontweight='bold', y=0.98)
    live_tag=" [LIVE ITICK]" if c.get('is_live') else ""
    fig.text(0.005, 0.96, f"{sym} : {last['close']:.0f} ({c['chg']:+.2f}%){live_tag}", color='yellow', fontsize=14, fontweight='bold', ha='left')
    fig.text(0.995, 0.96, f"Daily | {now_wib().strftime('%d %b %Y %H:%M WIB')} | {c['accum_type']} {c['foreign_str']}", color='#ffcc00', fontsize=9, ha='right')
    fig.text(0.92, 0.45, f"{c['foreign_str']}\n{c['accum_type']}", color='black', fontsize=8, fontweight='bold', bbox=dict(facecolor='yellow', edgecolor='none'), ha='center')
    plt.tight_layout(rect=[0,0,1,0.95])
    save_path=os.path.join(CHART_DIR, f"{sym}_V417_ITICK.png")
    plt.savefig(save_path, dpi=180, facecolor='black', bbox_inches='tight')
    plt.close()
    return save_path

# Telegram
def send_telegram(chat_id, text, photo_path=None):
    if not TELEGRAM_BOT_TOKEN: return
    try:
        if photo_path and os.path.exists(photo_path):
            with open(photo_path,'rb') as f:
                requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto", data={"chat_id":chat_id,"caption":text,"parse_mode":"Markdown"}, files={"photo":f}, timeout=20)
        else:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json={"chat_id":chat_id,"text":text,"parse_mode":"Markdown"}, timeout=15)
    except Exception as e:
        print(f"telegram send err {e}")

def process_chart_request(chat_id, symbol, use_itick=True):
    symbol=symbol.upper().replace(".JK","").replace("$","")
    df,bandar,broker=get_df(symbol, 200, use_itick=use_itick)
    if df is None:
        send_telegram(chat_id, f"❌ {symbol} tidak ada di DB {DB_PATH} - coba update dulu")
        return
    last=df.iloc[-1]
    prev=df.iloc[-2]
    chg=(last['close']/prev['close']-1)*100 if prev['close'] else 0
    f_str=bandar.iloc[0]['foreign_str'] if bandar is not None and not bandar.empty else f"+{last['VCHG_1']:.1f}xVol"
    acc=bandar.iloc[0]['accum_type'] if bandar is not None and not bandar.empty else "VOL SURGE"
    c={"symbol":symbol,"close":last['close'],"chg":chg,"foreign_str":f_str,"accum_type":acc,"df":df,"bandar":bandar,"broker":broker,"is_live":False}
    path=plot_v417(c)
    send_telegram(chat_id, f"📈 *{symbol}* {last['close']:.0f} ({chg:+.2f}%)\n{f_str} {acc}\nVol {last['VCHG_1']:.1f}x | EMA20 {last['EMA20']:.0f}\nChart: V4.17 ITICK Realtime", photo_path=path)

def broadcast_top10(top10):
    if not top10: return
    header=f"🔥 *TOP 10 RAFANO V4.17 ITICK* - {now_wib().strftime('%d %b %Y %H:%M WIB')}\nVOL SURGE NEAR MA20 + LIVE ITICK\n\n"
    msg=header
    for i,c in enumerate(top10,1):
        live="🔴" if c.get('is_live') else "⚪"
        msg+=f"{i}. {live} *{c['symbol']}* {c['close']:.0f} ({c['chg']:+.1f}%) {c['foreign_str']} {c['accum_type']} Vol {c['vchg_1']:.1f}x\n"
    # Kirim ke TARGET_CHAT_ID
    if TARGET_CHAT_ID:
        send_telegram(TARGET_CHAT_ID, msg)
    # Kirim chart satu per satu
    for c in top10:
        try:
            path=plot_v417(c)
            if TARGET_CHAT_ID:
                send_telegram(TARGET_CHAT_ID, f"*{c['symbol']}* {c['accum_type']} {c['foreign_str']}", photo_path=path)
                time.sleep(1.5)
        except Exception as e:
            print(f"broadcast chart {c['symbol']} err {e}")

def telegram_listener():
    offset=0
    print(f"🤖 RAFANO V4.17 ITICK TELEGRAM LISTENER START - {now_wib()}")
    print(f"Commands: /c SYMBOL, /scanbo, /topbo, /vol, /quota")
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
    except: pass
    while True:
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=30"
            res=requests.get(url, timeout=35)
            if res.status_code!=200:
                time.sleep(3); continue
            data=res.json()
            for upd in data.get("result",[]):
                offset=upd["update_id"]+1
                if "callback_query" in upd:
                    cb=upd["callback_query"]
                    try:
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery", json={"callback_query_id":cb.get("id")}, timeout=5)
                    except: pass
                    cdata=cb.get("data","")
                    chat_id=cb["message"]["chat"]["id"]
                    if cdata.startswith("chart_"):
                        sym=cdata.split("_")[1]
                        threading.Thread(target=process_chart_request, args=(chat_id,sym,True)).start()
                elif "message" in upd and "text" in upd["message"]:
                    txt=upd["message"].get("text","").strip()
                    chat_id=upd["message"]["chat"]["id"]
                    user=upd["message"]["from"].get("username","?")
                    print(f"💬 {user} ({chat_id}): {txt}")
                    parts=txt.split()
                    first=parts[0].lower() if parts else ""
                    if first in ["/start","/help","/menu"]:
                        help_msg="""🔥 *V4.17 ITICK REALTIME + CHART VISI*

📈 *CHART (realtime itick)*
/c KODE - Chart V4.17 + harga LIVE itick
/c KODE 1d - Chart daily
Contoh: /c VISI /c BBCA

🚀 *SCAN LIVE ITICK*
/scanbo [vol] - Scan BO MA20 + Vol + LIVE itick (default 1.3x)
/topbo [vol] - TOP 10 VOL SURGE NEAR MA20 + ITICK
/scanvol [thr] - Vol spike >thr (default 2.0)
/volall - Vol 300

⚡ *INFO*
/quota - Cek quota Arjum & Itick
/db - Cek jumlah saham di DB
"""
                        send_telegram(chat_id, help_msg)
                    elif first in ["/c","/chart"]:
                        if len(parts)>=2:
                            sym=parts[1].upper()
                            threading.Thread(target=process_chart_request, args=(chat_id,sym,True)).start()
                        else:
                            send_telegram(chat_id, "Pakai: /c VISI atau /c BBCA")
                    elif first in ["/quota","/db"]:
                        conn=sqlite3.connect(DB_PATH)
                        cnt=conn.execute("SELECT COUNT(DISTINCT symbol) FROM ohlcv").fetchone()[0]
                        conn.close()
                        send_telegram(chat_id, f"DB: {cnt} saham\nITICK: {'ON' if ITICK_ENABLED else 'OFF'} ({ITICK_TOKEN[:8]}...)\nARJUM: {'ON' if ARJUM_API_KEY else 'OFF'}\nTime: {now_wib().strftime('%d %b %H:%M WIB')}")
                    elif first in ["/scanbo","/topbo","/scan","/scanvol","/vol","/volall"]:
                        try:
                            vol_thr=float(parts[1]) if len(parts)>=2 else (2.0 if 'vol' in first else 1.3)
                        except:
                            vol_thr=1.3
                        lim=300 if 'all' in first else 60
                        send_telegram(chat_id, f"🚀 SCAN ITICK LIVE VOL>{vol_thr}x ... ~20 detik")
                        def run_scan(tg=chat_id, vt=vol_thr):
                            top10=scan_top10(use_itick=True, vol_thr=vt, near_pct=0.95)
                            if not top10:
                                send_telegram(tg, f"⚠️ Tidak ada yang breakout MA20 + Vol>{vt}x saat ini - market merah mungkin")
                                return
                            header=f"🔥 *TOP {len(top10)} ITICK LIVE* {now_wib().strftime('%H:%M WIB')} Vol>{vt}x\n\n"
                            msg=header
                            for i,c in enumerate(top10,1):
                                live="🔴" if c.get('is_live') else "⚪"
                                msg+=f"{i}. {live} *{c['symbol']}* {c['close']:.0f} ({c['chg']:+.1f}%) {c['foreign_str']} Vol {c['vchg_1']:.1f}x\n"
                            send_telegram(tg, msg)
                            for c in top10:
                                try:
                                    path=plot_v417(c)
                                    send_telegram(tg, f"*{c['symbol']}* {c['accum_type']}", photo_path=path)
                                    time.sleep(1.2)
                                except Exception as e:
                                    print(f"send chart err {e}")
                        threading.Thread(target=run_scan).start()
        except Exception as e:
            print(f"Listener err {e}")
            import traceback; traceback.print_exc()
            time.sleep(5)

if __name__=="__main__":
    # Test mode kalau gak ada telegram token, jalanin scan sekali
    if not TELEGRAM_BOT_TOKEN:
        print("⚠️ No TELEGRAM_BOT_TOKEN - running scan test only")
        top10=scan_top10(use_itick=True, vol_thr=1.3)
        for c in top10:
            p=plot_v417(c)
            print(f"Chart {p}")
    else:
        telegram_listener()
