
import os, time, json, requests, sqlite3
from datetime import datetime, time as dtime
import pytz
DB_PATH="rafano.db"
WIB=pytz.timezone('Asia/Jakarta')
MARKET_START=dtime(9,0)
MARKET_END=dtime(16,0)
SCAN_INTERVAL=300
ALERTED_FILE="alerted_today.json"

def is_market_hours():
    now=datetime.now(WIB)
    if now.weekday()>=5: return False
    return MARKET_START <= now.time() <= MARKET_END

def load_alerted():
    if not os.path.exists(ALERTED_FILE):
        return {'date': datetime.now(WIB).strftime('%Y-%m-%d'), 'symbols': []}
    try:
        import json
        with open(ALERTED_FILE,'r') as f:
            data=json.load(f)
            if data.get('date')!=datetime.now(WIB).strftime('%Y-%m-%d'):
                return {'date': datetime.now(WIB).strftime('%Y-%m-%d'), 'symbols': []}
            return data
    except:
        return {'date': datetime.now(WIB).strftime('%Y-%m-%d'), 'symbols': []}

def save_alerted(sym):
    data=load_alerted()
    if sym not in data['symbols']:
        data['symbols'].append(sym)
        data['date']=datetime.now(WIB).strftime('%Y-%m-%d')
        with open(ALERTED_FILE,'w') as f:
            json.dump(data,f)

def send_telegram(text, photo=None):
    token=os.getenv("TELEGRAM_BOT_TOKEN")
    chat=os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat:
        print(f"[SKIP TELE] {text[:80]}")
        return
    try:
        if photo and os.path.exists(photo):
            url=f"https://api.telegram.org/bot{token}/sendPhoto"
            with open(photo,'rb') as f:
                r=requests.post(url, data={'chat_id':chat,'caption':text,'parse_mode':'HTML'}, files={'photo':f}, timeout=20)
        else:
            url=f"https://api.telegram.org/bot{token}/sendMessage"
            r=requests.post(url, data={'chat_id':chat,'text':text,'parse_mode':'HTML'}, timeout=15)
        print(f"Telegram {r.status_code}")
    except Exception as e:
        print(f"Tele err {e}")

def main():
    print("=== RAFANO LIVE CONTINUOUS SCANNER ===")
    import rafano_scanner_v417 as scanner
    while True:
        if not is_market_hours():
            print(f"[{datetime.now(WIB).strftime('%H:%M')}] Market tutup, sleep 10m")
            time.sleep(600)
            continue
        try:
            top10=scanner.scan_top10()
            alerted=load_alerted()
            new=[c for c in top10 if c['symbol'] not in alerted.get('symbols',[])]
            if new:
                for c in new[:5]:
                    path=scanner.plot_v417(c)
                    txt=f"🔥 <b>{c['symbol']}</b> {c['close']:.0f} ({c['chg']:+.2f}%) {c['foreign_str']} {c['accum_type']} Vol {c['vchg_1']:.1f}x"
                    send_telegram(txt, path)
                    save_alerted(c['symbol'])
                    time.sleep(2)
            else:
                print(f"[{datetime.now(WIB).strftime('%H:%M:%S')}] No new")
            time.sleep(SCAN_INTERVAL)
        except Exception as e:
            print(f"Loop err {e}")
            time.sleep(60)

if __name__=="__main__":
    main()
