"""
RAFANO V4.7 FIXED - ARJUM80 -> ITICK30 REALTIME -> VOL(ITICK realtime vs ARJUM avg20) 1.2x -> BO EMA50/BOB EMA200

Flow (sesuai spesifikasi terbaru):
1. ARJUM screener/latest -> semua baris di-SORT eksplisit by change% -> TOP 80 GAINER
2. ITICK quotes batch -> dari 80 itu, realtime -> TOP 30 REALTIME (sorted by change% realtime)
3. History harian dari ARJUM -> rata-rata volume 20 hari (hari ini DIKECUALIKAN dari rata-rata)
4. Volume HARI INI pakai data realtime ITICK kalau tersedia (fallback ke Arjum kalau tidak ada)
5. Filter FINAL: volume >= 1.2x rata-rata 20 hari, DAN close BO EMA50 atau BOB EMA200
   - Deteksi BO memakai harga realtime sebagai titik "hari ini", dengan penyesuaian otomatis
     tergantung apakah bar terakhir Arjum sudah termasuk hari ini atau belum (fix alignment tanggal)
6. Broadcast Telegram, tiap sinyal ditandai sumber data (ARJUM / YFINANCE_FALLBACK) &
   sumber volume (ITICK_REALTIME / ARJUM_DELAYED) supaya bisa diaudit

Perubahan dari V4.6:
- Default top_gainer_arjum: 60 -> 80
- Default vol_threshold: 1.5 -> 1.2
- Screener Arjum di-sort eksplisit berdasarkan change% sebelum diambil top-N (sebelumnya asumsi
  urutan API = top gainer, tidak diverifikasi)
- Volume "hari ini" (v_last) sekarang pakai data realtime iTick kalau ada, bukan cuma volume
  Arjum yang delayed
- Perhitungan EMA50/EMA200 & previous-close sekarang memvalidasi apakah bar terakhir Arjum sudah
  mencakup hari ini atau belum, lalu menyesuaikan index/menambah bar sintetis dari harga realtime
  supaya tidak salah geser index -2 (bug lama: bisa kebaca "2 hari lalu" padahal maksudnya kemarin)
- Field mapping response iTick dibuat lebih toleran (coba beberapa nama field umum) + debug print
  sekali di awal supaya gampang diverifikasi field mana yang benar-benar dipakai iTick kamu
- Cache history diperpanjang (300s -> 1800s) karena bar historis (selain hari ini) praktis statis;
  volume/harga hari ini tetap disegarkan lewat quote realtime iTick tiap scan
- except: pass diganti print pesan error seperlunya biar gampang didiagnosis
- Setiap sinyal ditandai data_source & vol_source untuk audit
"""

import os, time, datetime, threading, requests, pytz, json
import numpy as np, pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
load_dotenv()

TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN") or ""
TARGET_CHAT_ID = os.getenv("TARGET_CHAT_ID") or os.getenv("CHAT_ID") or ""
ARJUM_API_KEY = os.getenv("ARJUM_API_KEY") or ""
ARJUM_BASE = "https://stock.arjum.com/api"

ITICK_TOKEN = os.getenv("ITICK_TOKEN") or os.getenv("ITICK_API_KEY") or ""
ITICK_BASE = "https://api.itick.org"
ITICK_ENABLED = bool(ITICK_TOKEN)
ITICK_DEBUG_PRINTED = False  # supaya raw response iTick dicetak sekali saja utk verifikasi field

def get_now_wib(): return datetime.datetime.now(TIMEZONE_WIB)

FCA_EXCLUDE = {
    "FUTR","FITT","HOTEL","ITIC","PUDP","COIN","SHID","RELI","ASPI","MEJA","MINA",
    "ESTA","ASLI","VKTR","IMJS","GTSI","IRSX","ATAP","RONY","BCIC","DEFI","ROCK","YPAS","NIRO",
    "BBHA","BKSW","NAGA","BEEF","BPTR","CBMF","CPRI","CRAB","DAAZ","DEAL","DGNS","DMND","DUCK",
    "ELSA","ENRG","ENVY","ERAA","ESTI","ETWA","FIRE","FORU","GAMA","GOLL","HAIS","HATM","HITS",
    "HOMI","IATA","INPS","IPOL","JGLE","KAYU","KBAG","KIOS","KPAL","KPAS","LCGP","LPLI","LPLR",
    "MAGP","MAMI","MARI","SINI","SKYB","SMKM","SOCI","SONA","SOSS","SUGI","TALF","TDPM","TEBE","TOPS",
    "TRAM","TRIL","TRIO","TRUS","UFOE","WIFI-W","WOWS","YELO","ZATA","ZONE","ZINC","TINS-W","BIPI-W","BULL-W","DEWA-W"
}

HISTORY_CACHE={}; SCREENER_CACHE={}
HISTORY_CACHE_TTL=1800   # diperpanjang: bar historis (selain hari ini) praktis statis sepanjang hari
SCREENER_CACHE_TTL=120
QUOTA_HIT=False; LAST_429_TIME=0

def get_cached_history(k):
    if k in HISTORY_CACHE:
        ts,d=HISTORY_CACHE[k]
        if time.time()-ts<HISTORY_CACHE_TTL: return d
    return None
def set_cached_history(k,d):
    HISTORY_CACHE[k]=(time.time(),d)
def get_cached_screener():
    if 'latest' in SCREENER_CACHE:
        ts,d=SCREENER_CACHE['latest']
        if time.time()-ts<SCREENER_CACHE_TTL: return d
    return None
def set_cached_screener(d):
    SCREENER_CACHE['latest']=(time.time(),d)

def arjum_get(path, params=None):
    global QUOTA_HIT, LAST_429_TIME
    if QUOTA_HIT and time.time()-LAST_429_TIME<300:
        return None
    url=f"{ARJUM_BASE}{path}"
    try:
        headers={"X-API-Key": ARJUM_API_KEY.strip(),"Accept":"application/json","User-Agent":"Mozilla/5.0"}
        r=requests.get(url,headers=headers,params=params,timeout=15)
        if r.status_code==200:
            return r.json()
        elif r.status_code==429:
            QUOTA_HIT=True; LAST_429_TIME=time.time()
            print(f"⚠️ Arjum 429 quota hit di {path}, pause 5 menit")
            return None
        else:
            print(f"⚠️ Arjum {path} status {r.status_code}")
    except Exception as e:
        print(f"⚠️ Arjum {path} err: {e}")
    return None

def get_screener_latest(force_today=False):
    if not force_today:
        c=get_cached_screener()
        if c and isinstance(c, dict) and 'rows' in c:
            return c
    data=arjum_get("/screener/latest")
    if data and isinstance(data, dict) and 'rows' in data:
        set_cached_screener(data)
        return data
    return data if data else {}

def get_history_pro(sym, limit=100):
    hk=f"{sym}_1d_{limit}"
    cached=get_cached_history(hk)
    if cached is not None: return cached
    data=arjum_get(f"/history/{sym}",params={"limit":limit,"frame":"daily"})
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
                df.attrs['source']='ARJUM'
                set_cached_history(hk,df); return df
        except Exception as e:
            print(f"⚠️ parse history Arjum {sym} err: {e}")
    try:
        import yfinance as yf
        hist=yf.Ticker(f"{sym}.JK").history(period="1y",interval="1d",timeout=10)
        if hist is not None and len(hist)>10:
            hist=hist.tail(limit)
            hist.attrs['source']='YFINANCE_FALLBACK'
            set_cached_history(hk,hist); return hist
    except Exception as e:
        print(f"⚠️ yfinance fallback {sym} err: {e}")
    return None

def get_itick_quotes_batch(symbols, max_batch=15):
    global ITICK_DEBUG_PRINTED
    if not ITICK_ENABLED or not symbols: return {}
    try:
        codes=",".join(symbols[:max_batch])
        url = f"{ITICK_BASE}/stock/quotes?region=ID&codes={codes}"
        headers = {"accept": "application/json", "token": ITICK_TOKEN}
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code!=200:
            print(f"⚠️ iTick status {r.status_code}: {r.text[:200]}")
            return {}
        j=r.json()
        if not ITICK_DEBUG_PRINTED:
            print(f"[DEBUG ITICK] contoh raw response (cek nama field!): {json.dumps(j)[:800]}")
            ITICK_DEBUG_PRINTED = True
        if j.get('code')!=0 or not j.get('data'):
            print(f"⚠️ iTick tidak ada data valid: code={j.get('code')} msg={j.get('msg')}")
            return {}
        result={}
        for item in j['data']:
            code=item.get('s') or item.get('code') or item.get('symbol')
            if not code: continue
            code=str(code).upper().replace(".JK","")
            # toleran ke beberapa kemungkinan nama field iTick - VERIFIKASI ke dokumentasi resmi
            price = item.get('ld') or item.get('c') or item.get('close') or item.get('last') or 0
            chg_pct = item.get('chp') or item.get('chgPer') or item.get('changePercent') or item.get('pctChg') or 0
            vol = item.get('v') or item.get('volume') or item.get('vol') or 0
            result[code]={
                'price': float(price or 0),
                'changepct': float(chg_pct or 0),
                'volume': float(vol or 0),
                'source': 'ITICK_REALTIME'
            }
        return result
    except Exception as e:
        print(f"⚠️ itick batch err: {e}")
    return {}

def format_large_number(val,show_sign=False):
    if pd.isna(val) or val==0: return "0"
    a=abs(val); s="+" if (show_sign and val>0) else ("-" if val<0 else "")
    if a>=1_000_000_000: return f"{s}{a/1_000_000_000:.2f}B"
    elif a>=1_000_000: return f"{s}{a/1_000_000:,.0f}M"
    elif a>=1_000: return f"{s}{a/1_000:,.0f}K"
    else: return f"{s}{val:,.0f}"

# ============ FINAL FILTER: BO EMA50 / BOB EMA200 (dengan fix alignment tanggal) ============
def check_bo_ema50_bob200(sym, hd, realtime_price=None):
    """
    - Menentukan apakah bar terakhir Arjum sudah termasuk hari ini atau belum, lalu:
        * kalau sudah: timpa close hari ini dengan harga realtime (biar tidak stale)
        * kalau belum: tambahkan bar sintetis hari ini dari harga realtime
      supaya "previous close" & EMA tidak salah geser index (bug lama).
    - Cross: kemarin close <= EMA, hari ini close (realtime) > EMA -> valid BO
    - Tolak kalau sudah BO 2 hari lalu (anti label BO berturut-turut)
    """
    try:
        if hd is None or len(hd) < 55:
            return None

        today_wib = get_now_wib().date()
        last_idx_date = hd.index[-1].date() if hasattr(hd.index[-1], "date") else None

        closes = hd['Close'].copy()
        c = float(realtime_price) if realtime_price and realtime_price > 0 else float(closes.iloc[-1])
        if c < 50: return None

        if last_idx_date == today_wib:
            # bar terakhir Arjum = hari ini (kemungkinan masih delayed) -> timpa dgn harga realtime
            closes.iloc[-1] = c
        else:
            # bar terakhir Arjum belum termasuk hari ini -> tambahkan bar sintetis hari ini
            closes = pd.concat([closes, pd.Series([c], index=[pd.Timestamp(today_wib)])])

        pc = float(closes.iloc[-2])

        ema50_s = closes.ewm(span=50, adjust=False).mean()
        ema200_s = closes.ewm(span=200, adjust=False).mean()
        ema50 = float(ema50_s.iloc[-1]); pe50 = float(ema50_s.iloc[-2])
        ema200 = float(ema200_s.iloc[-1]); pe200 = float(ema200_s.iloc[-2])

        is_bo=False; stype=""
        if pc <= pe50 and c > ema50 and c > pe50:
            is_bo=True; stype="BO EMA50"
        elif pc <= pe200 and c > ema200 and c > pe200:
            is_bo=True; stype="BOB EMA200"

        if not is_bo:
            return None

        if len(closes)>=4:
            c2=float(closes.iloc[-3]); c3=float(closes.iloc[-4])
            pe50_2=float(ema50_s.iloc[-3]); pe50_3=float(ema50_s.iloc[-4])
            pe200_2=float(ema200_s.iloc[-3]); pe200_3=float(ema200_s.iloc[-4])
            if stype=="BO EMA50":
                if c2 > pe50_2 and c3 > pe50_3: return None
                if c2 > pe50_2 and pc > pe50: return None
            if stype=="BOB EMA200":
                if c2 > pe200_2 and c3 > pe200_3: return None
                if c2 > pe200_2 and pc > pe200: return None

        return {"type": stype, "ema50": ema50, "ema200": ema200, "pe50": pe50, "pe200": pe200}
    except Exception as e:
        print(f"⚠️ BO check {sym} err: {e}")
        return None

# ============ CORE V4.7 FIXED ============
def scan_final_v47(top_gainer_arjum=80, top_itick=30, vol_threshold=1.2):
    """
    FLOW: ARJUM 80 (sorted by gainer) -> ITICK 30 realtime -> VOL (itick realtime vs arjum avg20)
          -> BO EMA50/BOB EMA200
    """
    print(f"[{get_now_wib()}] 🚀 V4.7 FIXED: ARJUM {top_gainer_arjum} -> ITICK {top_itick} + VOL {vol_threshold}x + BO/BOB")

    # Step 1: ARJUM TOP N, di-sort eksplisit berdasarkan change% (bukan asumsi urutan API)
    screener = get_screener_latest(force_today=True)
    rows = screener.get('rows', []) if isinstance(screener, dict) else []
    if not rows:
        print("❌ Arjum screener kosong"); return []

    def _gain(r):
        for key in ('change_pct','changePercent','pct_change','gain_pct'):
            v=r.get(key)
            if v is not None:
                try: return float(v)
                except (TypeError,ValueError): pass
        return -999.0

    rows_sorted = sorted(rows, key=_gain, reverse=True)

    candidates_arjum=[]
    for r in rows_sorted:
        code = (r.get('stock_code') or r.get('symbol') or "").replace(".JK","").upper()
        if not code or code in FCA_EXCLUDE or "-W" in code: continue
        try:
            close=float(r.get('close') or 0)
            if close < 50: continue
        except (TypeError,ValueError): continue
        candidates_arjum.append(code)
        if len(candidates_arjum) >= top_gainer_arjum: break

    print(f"📊 Step1 ARJUM TOP {top_gainer_arjum} (sorted by gainer%): {candidates_arjum[:10]}...")

    # Step 2: ITICK REALTIME -> TOP N (sorted by change% realtime)
    all_quotes={}
    for i in range(0, len(candidates_arjum), 15):
        batch=candidates_arjum[i:i+15]
        q=get_itick_quotes_batch(batch, 15)
        all_quotes.update(q)
        time.sleep(0.2)

    coverage = len(all_quotes)
    print(f"📡 iTick coverage: {coverage}/{len(candidates_arjum)} simbol dapat quote")

    if not all_quotes:  # fallback total ke data Arjum (delayed)
        for r in rows_sorted:
            code=(r.get('stock_code') or "").replace(".JK","").upper()
            if code in candidates_arjum:
                all_quotes[code]={'price': float(r.get('close') or 0), 'changepct': _gain(r), 'volume': 0, 'source': 'ARJUM_DELAY'}

    sorted_itick = sorted(all_quotes.items(), key=lambda x: x[1].get('changepct', -999), reverse=True)
    top30_codes = [c for c,_ in sorted_itick[:top_itick]]
    print(f"⚡ Step2 ITICK TOP {top_itick}: {top30_codes}")

    # Step 3 & 4: VOL (realtime iTick vs avg20 Arjum) + BO/BOB
    detected=[]
    def check_one(sym):
        try:
            hd=get_history_pro(sym, limit=100)
            if hd is None or len(hd)<55: return None

            q=all_quotes.get(sym, {})
            realtime_price=q.get('price', 0)
            changepct=q.get('changepct', 0)
            realtime_volume=q.get('volume', 0)

            today_wib = get_now_wib().date()
            last_idx_date = hd.index[-1].date() if hasattr(hd.index[-1], "date") else None
            hist_vol = hd['Volume'].iloc[:-1] if last_idx_date == today_wib else hd['Volume']
            if len(hist_vol) < 20: return None
            v_avg=float(hist_vol.iloc[-20:].mean())
            if v_avg==0: return None

            if realtime_volume and realtime_volume>0:
                v_last=realtime_volume; vol_source="ITICK_REALTIME"
            else:
                v_last=float(hd['Volume'].iloc[-1]); vol_source="ARJUM_DELAYED"
            if v_last==0: return None

            ratio=v_last/v_avg
            if ratio < vol_threshold:
                print(f"   ⏭ {sym} VOL {ratio:.2f}x < {vol_threshold}x SKIP"); return None

            bo_info = check_bo_ema50_bob200(sym, hd, realtime_price)
            if not bo_info:
                return None

            c = realtime_price if realtime_price>0 else float(hd['Close'].iloc[-1])

            return {
                "symbol": sym,
                "type": bo_info['type'],
                "close": int(c),
                "change_pct": changepct,
                "vol_ratio": ratio,
                "vol_rp": v_last * c,
                "vol_source": vol_source,
                "ema50": bo_info['ema50'],
                "ema200": bo_info['ema200'],
                "data_source": hd.attrs.get('source','ARJUM'),
            }
        except Exception as e:
            print(f"⚠️ check {sym} err: {e}"); return None

    with ThreadPoolExecutor(max_workers=15) as ex:
        futs={ex.submit(check_one, s): s for s in top30_codes}
        for f in as_completed(futs):
            r=f.result()
            if r:
                detected.append(r)
                print(f"🔥 VALID BO {r['symbol']} {r['type']} {r['change_pct']:+.1f}% VOL {r['vol_ratio']:.2f}x ({r['vol_source']}, {r['data_source']})")

    detected.sort(key=lambda x: x['vol_ratio'], reverse=True)
    return detected

# ============ TELEGRAM ============
def send_reply(cid, txt, rm=None):
    if not TELEGRAM_BOT_TOKEN: return
    url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    pl={"chat_id":cid,"text":txt,"parse_mode":"Markdown"}
    if rm: pl["reply_markup"]=rm
    try:
        requests.post(url,json=pl,timeout=15)
    except Exception as e:
        print(f"⚠️ send_reply err: {e}")

def broadcast_final(signals, vol_thr=1.2):
    if not signals:
        send_reply(TARGET_CHAT_ID, f"📉 *ARJUM80->ITICK30 + VOL>{vol_thr}x + BO/BOB*\nTidak ada yang valid hari ini. Market sepi BO.")
        return
    now=get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header=f"*🚀 TOP BO/BOB {len(signals)} SAHAM* 🔥\n{now}\nARJUM 80 -> ITICK 30 REALTIME + VOL >{vol_thr}x + BO EMA50/BOB EMA200\n{'='*35}\n\n"
    msg=header; kb=[]
    for idx,it in enumerate(signals,1):
        src_flag = "" if it.get('data_source')=='ARJUM' and it.get('vol_source')=='ITICK_REALTIME' else f" [{it.get('data_source')}/{it.get('vol_source')}]"
        line=f"{idx}. *{it['symbol']}* {it['type']} {it['close']} ({it['change_pct']:+.1f}%)\n   Vol {it['vol_ratio']:.1f}x Rp {format_large_number(it['vol_rp'])} EMA50 {it['ema50']:.0f}{src_flag}\n\n"
        kb.append([{"text": f"{it['symbol']} {it['type']}", "callback_data": f"chart_{it['symbol']}_1d"}])
        if len(msg)+len(line)>3500:
            send_reply(TARGET_CHAT_ID, msg, rm={"inline_keyboard": kb}); msg=line; kb=[]
        else:
            msg+=line
    if msg:
        send_reply(TARGET_CHAT_ID, msg, rm={"inline_keyboard": kb})

# ============ AUTO NOTIFY ============
AUTO_NOTIFY_ENABLED = os.getenv("AUTO_NOTIFY", "true").lower() == "true"
AUTO_INTERVAL = 180
COOLDOWN_MIN = 60
LAST_ALERT_MAP = {}

def is_market_hours_v2():
    now=get_now_wib()
    if now.weekday()>=5: return False, "WEEKEND"
    h=now.hour+now.minute/60
    if 8.9 <= h < 9.0: return True, "PRE_OPEN"
    if 9.0 <= h < 12.0: return True, "SESI_1"
    if 12.0 <= h < 13.5: return False, "BREAK"
    if 13.5 <= h < 15.85: return True, "SESI_2"
    return False, "CLOSED"

def should_alert(sym, vol_ratio):
    now=time.time()
    if sym not in LAST_ALERT_MAP: return True
    last_t, last_v = LAST_ALERT_MAP[sym]
    if now-last_t < COOLDOWN_MIN*60:
        if vol_ratio > last_v*1.5: return True
        return False
    return True

def auto_scan_loop():
    print(f"🤖 AUTO V4.7 FIXED: ARJUM80->ITICK30->VOL1.2x->BO tiap {AUTO_INTERVAL}s")
    while True:
        try:
            is_open, sesi = is_market_hours_v2()
            if not is_open:
                print(f"[{get_now_wib()}] Market {sesi}, sleep 5m")
                time.sleep(300); continue

            print(f"[{get_now_wib()}] 🔍 AUTO SCAN {sesi} FIXED...")
            sigs = scan_final_v47(top_gainer_arjum=80, top_itick=30, vol_threshold=1.2)

            to_broadcast=[]
            for sig in sigs:
                if should_alert(sig['symbol'], sig['vol_ratio']):
                    to_broadcast.append(sig)
                    LAST_ALERT_MAP[sig['symbol']]=(time.time(), sig['vol_ratio'])

            if to_broadcast:
                broadcast_final(to_broadcast, vol_thr=1.2)
            else:
                print("😴 Tidak ada BO baru (cooldown)")

            time.sleep(AUTO_INTERVAL)
        except Exception as e:
            print(f"⚠️ AUTO err: {e}"); time.sleep(60)

def telegram_bot_listener():
    offset=0
    print("🤖 RAFANO V4.7 FIXED - Listening...")
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true",timeout=10)
    except Exception as e:
        print(f"⚠️ deleteWebhook err: {e}")
    while True:
        try:
            url=f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res=requests.get(url,timeout=25)
            if res.status_code!=200: time.sleep(3); continue
            data=res.json()
            for update in data.get("result",[]):
                offset=update["update_id"]+1
                if "message" in update and "text" in update["message"]:
                    txt=update["message"].get("text","").strip(); chat_id=update["message"]["chat"]["id"]; first=txt.split()[0].lower() if txt else ""
                    if first in ["/start","/help","/menu"]:
                        send_reply(chat_id, "🔥 *RAFANO V4.7 FIXED*\nARJUM80(sorted)->ITICK30 REALTIME->VOL1.2x(itick realtime)->BO50/BOB200\n\n/scanbo - Scan BO/BOB final (~15 detik)\n/scanbo 2 - Vol >2x\n/topbo 30 1.2 80 - custom (top_itick vol_thr top_arjum)\n/quota")
                    elif first in ["/scanbo","/topbo","/bobot","/bo","/bob","/topgainer","/scanvol","/scan","/top80"]:
                        parts=txt.split()
                        vol_thr=1.2; top_final=30; top_arjum=80
                        try:
                            if len(parts)>=2: vol_thr=float(parts[1])
                            if len(parts)>=3: top_final=int(float(parts[2]))
                            if len(parts)>=4: top_arjum=int(float(parts[3]))
                        except Exception:
                            pass
                        if vol_thr>=10:  # user ketik /scanbo 30 -> itu maksudnya top_final
                            top_final=int(vol_thr); vol_thr=1.2
                        if vol_thr<1.0: vol_thr=1.0
                        if top_final>80: top_final=80
                        send_reply(chat_id, f"🚀 *SCAN FINAL BO/BOB*\nARJUM {top_arjum} -> ITICK {top_final} + VOL >{vol_thr}x + BO50/BOB200\n~15 detik...")
                        def run_scan(tg=chat_id, vt=vol_thr, tf=top_final, ta=top_arjum):
                            sigs=scan_final_v47(top_gainer_arjum=ta, top_itick=tf, vol_threshold=vt)
                            broadcast_final(sigs, vol_thr=vt)
                        threading.Thread(target=run_scan, args=(chat_id, vol_thr, top_final, top_arjum)).start()
        except Exception as e:
            print(f"⚠️ Listener err {e}"); time.sleep(3)

if __name__=="__main__":
    print("==========================================")
    print("🔥 RAFANO V4.7 FIXED")
    print("ARJUM80(sorted) -> ITICK30 REALTIME -> VOL1.2x(itick realtime/arjum avg20) -> BO50/BOB200")
    print("==========================================")
    if AUTO_NOTIFY_ENABLED:
        threading.Thread(target=auto_scan_loop, daemon=True).start()
    telegram_bot_listener()
