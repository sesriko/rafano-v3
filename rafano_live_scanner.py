"""
RAFANO LIVE CONTINUOUS SCANNER - JALAN JAM MARKET 09:00-16:00 WIB
- Scan 940 saham tiap 5 menit
- Filter: BIG ACCUM + BO MA20 LIVE (intraday)
- Kirim Telegram otomatis + Chart V4.17
- Anti spam: saham yang udah alert hari ini gak di-alert lagi
"""
import os, time, sqlite3, requests, json
from datetime import datetime, time as dtime
import pytz

# CONFIG
DB_PATH = "rafano.db"
CHART_DIR = "charts_v417"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "") # id grup / channel lu
SCAN_INTERVAL = 300 # 5 menit
MARKET_START = dtime(9, 0) # 09:00 WIB
MARKET_END = dtime(16, 0) # 16:00 WIB

WIB = pytz.timezone('Asia/Jakarta')

# Cache biar gak spam
ALERTED_FILE = "alerted_today.json"

def is_market_hours():
    now_wib = datetime.now(WIB)
    if now_wib.weekday() >= 5: # Sabtu Minggu
        return False
    t = now_wib.time()
    return MARKET_START <= t <= MARKET_END

def load_alerted():
    if not os.path.exists(ALERTED_FILE):
        return {}
    try:
        with open(ALERTED_FILE, 'r') as f:
            data = json.load(f)
            # reset kalau ganti hari
            if data.get('date')!= datetime.now(WIB).strftime('%Y-%m-%d'):
                return {'date': datetime.now(WIB).strftime('%Y-%m-%d'), 'symbols': []}
            return data
    except:
        return {'date': datetime.now(WIB).strftime('%Y-%m-%d'), 'symbols': []}

def save_alerted(symbol):
    data = load_alerted()
    if symbol not in data['symbols']:
        data['symbols'].append(symbol)
        data['date'] = datetime.now(WIB).strftime('%Y-%m-%d')
        with open(ALERTED_FILE, 'w') as f:
            json.dump(data, f)

def send_telegram(text, photo_path=None):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[TELEGRAM SKIP] {text[:100]}")
        return

    try:
        if photo_path and os.path.exists(photo_path):
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
            with open(photo_path, 'rb') as f:
                r = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'caption': text, 'parse_mode': 'HTML'}, files={'photo': f}, timeout=20)
        else:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            r = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': text, 'parse_mode': 'HTML'}, timeout=15)
        print(f"Telegram sent: {r.status_code}")
    except Exception as e:
        print(f"Telegram error: {e}")

def scan_live():
    # import scanner logic dari file sebelumnya
    try:
        import rafano_scanner_v417 as scanner
    except:
        print("rafano_scanner_v417.py belum ada!")
        return []

    print(f"[{datetime.now(WIB).strftime('%H:%M:%S WIB')}] Scanning 940 saham...")

    # Update DB dulu dengan data terbaru hari ini (live price)
    # Pakai yfinance 1d latest
    try:
        import yfinance as yf
        import pandas as pd
        conn = sqlite3.connect(DB_PATH)
        symbols = [r[0] for r in conn.execute("SELECT DISTINCT symbol FROM ohlcv").fetchall()]
        # ambil live price 10 saham tercepat dulu biar gak berat - scan bertahap
        # untuk full 940, loop batch 100
        live_prices = {}
        for i in range(0, len(symbols), 100):
            batch = symbols[i:i+100]
            tickers = " ".join([f"{s}.JK" for s in batch])
            try:
                data = yf.download(tickers, period="1d", interval="1m", group_by='ticker', progress=False, threads=True)
                # simplified: ambil close terakhir
                for s in batch:
                    try:
                        if len(batch)==1:
                            last_close = float(data['Close'].dropna().iloc[-1])
                        else:
                            last_close = float(data[s+'.JK']['Close'].dropna().iloc[-1])
                        live_prices[s] = last_close
                    except:
                        pass
            except:
                pass
            time.sleep(1)

        # Update df dengan live price buat cek breakout intraday
        top10 = scanner.scan_top10() # scan pakai DB daily + bandar

        # Filter tambahan live price > MA20
        filtered = []
        for c in top10:
            sym = c['symbol']
            live = live_prices.get(sym, c['close'])
            # kalau live price breakout MA20 padahal daily close belum
            if live > c['ema20'] and c['close'] <= c['ema20']*1.02: # masih dekat MA20
                c['live_price'] = live
                c['chg_live'] = (live - c['close'])/c['close']*100
                filtered.append(c)
            elif c['close'] > c['ema20']: # udah breakout daily
                c['live_price'] = live
                filtered.append(c)

        conn.close()
        return filtered
    except Exception as e:
        print(f"scan_live error {e}")
        import traceback
        traceback.print_exc()
        return []

def main_loop():
    print("=== RAFANO LIVE CONTINUOUS SCANNER START ===")
    print(f"Market hours: {MARKET_START} - {MARKET_END} WIB, Mon-Fri")
    print(f"Scan interval: {SCAN_INTERVAL}s")
    send_telegram(f"🚀 <b>RAFANO LIVE SCANNER ON</b>\nJam market {MARKET_START}-{MARKET_END} WIB\nScan tiap {SCAN_INTERVAL//60} menit - BIG ACCUM + BO MA20")

    while True:
        try:
            if not is_market_hours():
                now = datetime.now(WIB)
                print(f"[{now.strftime('%H:%M:%S')}] Market tutup, sleep 10 menit...")
                time.sleep(600)
                continue

            alerted = load_alerted()
            candidates = scan_live()

            new_alerts = [c for c in candidates if c['symbol'] not in alerted.get('symbols', [])]

            if new_alerts:
                print(f"🔥 {len(new_alerts)} NEW breakout!")
                for c in new_alerts[:10]: # max 10 per scan
                    try:
                        import rafano_scanner_v417 as scanner
                        chart_path = scanner.plot_v417(c)
                        txt = f"🔥 <b>BREAKOUT MA20 LIVE</b> 🔥\n<b>{c['symbol']}</b> {c.get('live_price', c['close']):.0f} ({c['chg']:+.2f}%) Live {c.get('chg_live',0):+.2f}%\n{c['foreign_str']} {c['accum_type']}\nVol {c['vchg_1']:.1f}x | MA20 {c['ema20']:.0f}\n{datetime.now(WIB).strftime('%d %b %H:%M WIB')}"
                        send_telegram(txt, chart_path)
                        save_alerted(c['symbol'])
                        time.sleep(2)
                    except Exception as e:
                        print(f"Error send {c['symbol']} {e}")
            else:
                print("Tidak ada breakout baru")

            print(f"Sleep {SCAN_INTERVAL}s...")
            time.sleep(SCAN_INTERVAL)

        except KeyboardInterrupt:
            print("Stop scanner")
            break
        except Exception as e:
            print(f"Loop error {e}")
            time.sleep(60)

if __name__ == "__main__":
    main_loop()
