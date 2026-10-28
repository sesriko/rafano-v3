"""RAFANO V5.0 - refactor & bugfix dari V4.46
Perbaikan utama:
- EMA/RSI/ATR/ADX dihitung benar (seed SMA seperti TradingView) dan histori cukup (>=400 bar) untuk EMA200
- Bandarmology: data broker (jika tersedia) dihitung via top-N net buy/sell; fallback proxy OHLCV diberi label jelas
- Perbaikan crash: as_completed, global AUTO, KeyError kim_buy, matplotlib tidak thread-safe, cache tanpa lock
- Pattern/OB/FVG diperbaiki (mitigasi, pivot, breakout sungguhan)
"""
import os, sys, time, json, re, datetime, threading, multiprocessing
import html as _html
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests, pytz
import numpy as np, pandas as pd
import matplotlib
matplotlib.use('Agg')
from matplotlib.figure import Figure
from matplotlib.patches import Rectangle
from dotenv import load_dotenv

# ==================== ENV / COLAB ====================
IS_COLAB = False
userdata = None
try:
    from google.colab import userdata as _ud
    userdata = _ud
    IS_COLAB = True
except Exception:
    pass

for _p in ['/content/rafano-v3/.env', './.env', '.env', '/content/.env', '/content/rafano-v3/rafano-v3/.env']:
    if os.path.exists(_p):
        load_dotenv(_p, override=True)
        break

if IS_COLAB and userdata:
    for _k in ["TELEGRAM_BOT_TOKEN", "TARGET_CHAT_ID", "ARJUM_API_KEY", "ALLOWED_CHAT_IDS", "ARJUM_BROKER_PATH",
               "ARJUM_BROKER_START_PARAM", "ARJUM_BROKER_END_PARAM", "ARJUM_BROKER_FLOW_PARAM", "ARJUM_BROKER_FLOW",
               "BROKER_SCAN_N", "BROKER_MIN_NET_PCT"]:
        try:
            _v = userdata.get(_k)
            if _v:
                os.environ[_k] = _v
        except Exception:
            pass

TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')
TELEGRAM_BOT_TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
TARGET_CHAT_ID = (os.getenv("TARGET_CHAT_ID") or "").strip()
ARJUM_API_KEY = (os.getenv("ARJUM_API_KEY") or "").strip()
ARJUM_BASE = "https://stock.arjum.com/api"
# Isi jika endpoint broker summary Arjum sudah kamu verifikasi, contoh: /broker/{symbol}
ARJUM_BROKER_PATH = (os.getenv("ARJUM_BROKER_PATH") or "").strip()   # wajib memuat {symbol}
# Nama parameter query. Jalankan /brokerprobe untuk mendeteksi nilai yang benar di akunmu.
ARJUM_BROKER_START_PARAM = (os.getenv("ARJUM_BROKER_START_PARAM") or "start_date").strip()
ARJUM_BROKER_END_PARAM = (os.getenv("ARJUM_BROKER_END_PARAM") or "end_date").strip()
ARJUM_BROKER_FLOW_PARAM = os.getenv("ARJUM_BROKER_FLOW_PARAM", "flow").strip()   # kosongkan untuk tidak mengirim
ARJUM_BROKER_FLOW = (os.getenv("ARJUM_BROKER_FLOW") or "all").strip()
BROKER_SCAN_N = int(os.getenv("BROKER_SCAN_N") or 150)             # scan bandar hanya top-N by value (hemat kuota)
BROKER_MIN_NET_PCT = float(os.getenv("BROKER_MIN_NET_PCT") or 2.0)  # net top-3 minimal (% dari total transaksi)
ALLOWED = set(x.strip() for x in (os.getenv("ALLOWED_CHAT_IDS") or "").split(",") if x.strip())
if TARGET_CHAT_ID:
    ALLOWED.add(str(TARGET_CHAT_ID))

LOG_FILE = "/content/rafano_debug.log" if os.path.exists("/content") else "/tmp/rafano_debug.log"
SCAN_WORKERS = min(16, (multiprocessing.cpu_count() or 4) * 4)
CHART_WORKERS = 2
MIN_PRICE = 50
CHART_LOCK = threading.Lock()
SCAN_SEM = threading.Semaphore(1)
CHART_POOL = ThreadPoolExecutor(max_workers=CHART_WORKERS)
AUTO_ENABLED = False


def clog(msg):
    try:
        print(msg, flush=True)
    except Exception:
        pass
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as lf:
            lf.write(f"{datetime.datetime.now().strftime('%H:%M:%S')} {msg}\n")
    except Exception:
        pass


def now_wib():
    return datetime.datetime.now(TIMEZONE_WIB)


def esc(s):
    return _html.escape(str(s))


def market_open(n=None):
    n = n or now_wib()
    if n.weekday() >= 5:
        return False
    t = n.hour * 60 + n.minute
    if n.weekday() == 4:
        return (9 * 60 <= t < 11 * 60 + 30) or (14 * 60 <= t < 15 * 60 + 50)
    return (9 * 60 <= t < 12 * 60) or (13 * 60 + 30 <= t < 15 * 60 + 50)


# ==================== DAFTAR SAHAM ====================
FCA_EXCLUDE = {"FUTR","FITT","HOTEL","ITIC","PUDP","COIN","SHID","RELI","ASPI","MEJA","MINA","ESTA","ASLI","VKTR","IMJS","GTSI","IRSX","ATAP","RONY","BCIC","DEFI","ROCK","YPAS","NIRO","BBHA","BKSW","NAGA","BEEF","BPTR","CBMF","CPRI","CRAB","DAAZ","DEAL","DGNS","DMND","DUCK","ENRG","ENVY","ESTI","ETWA","FIRE","FORU","GAMA","GOLL","HAIS","HATM","HITS","HOMI","IATA","INPS","IPOL","JGLE","KAYU","KBAG","KIOS","KPAL","KPAS","LCGP","LPLI","LPLR","MAGP","MAMI","MARI","SINI","SKYB","SMKM","SOCI","SONA","SOSS","SUGI","TALF","TDPM","TEBE","TOPS","TRAM","TRIL","TRIO","TRUS","UFOE","WIFI-W","WOWS","YELO","ZATA","ZONE","ZINC","MPRO","BAPI","MMLP","NICE","BLES","HOPE","ALMI","RMBA","MTRA","KRAH","INCF","FPNI","CASA","KBLI","RIMO","JARR-W","COAL-W","BBYB-W","BTPS-W"}


def load_universe():
    for p in ['data/daytrade-observe-tickers.txt', './data/daytrade-observe-tickers.txt', '/content/rafano-v3/data/daytrade-observe-tickers.txt']:
        if os.path.exists(p):
            try:
                t = [x.strip().upper() for x in open(p, encoding='utf-8').read().splitlines() if len(x.strip()) == 4 and x.strip().isalpha()]
                if len(t) >= 100:
                    return t
            except Exception:
                pass
    try:
        r = requests.get("https://raw.githubusercontent.com/budikuatno2-ship-it/auto-cuan/main/data/daytrade-observe-tickers.txt", timeout=10)
        if r.status_code == 200:
            t = [x.strip().upper() for x in r.text.splitlines() if len(x.strip()) == 4 and x.strip().isalpha()]
            if len(t) >= 100:
                return t
    except Exception:
        pass
    return ["BBCA","BBRI","BMRI","BBNI","TLKM","ASII","GOTO","BUKA","BREN","CUAN","WIFI","DEWA","BULL","ERAA","BAIK","BBKP","BRIS","ANTM","INCO","MDKA","ADRO","PTBA","PGAS","EXCL","ISAT","AMRT","TOWR","BBYB","BRMS","BRPT"]


IDX_FULL = load_universe()

# Daftar broker: VERIFIKASI dengan daftar resmi IDX/Stockbit sebelum dipakai serius
FOREIGN_BROKERS = {"AK", "BK", "KZ", "ZP", "RX", "CS", "MS", "DB", "ML", "GW", "CG"}


# ==================== CACHE / RATE LIMIT (THREAD SAFE) ====================
class TTLCache:
    def __init__(self, maxsize=500):
        self.d = OrderedDict()
        self.lock = threading.Lock()
        self.maxsize = maxsize

    def get(self, k, ttl):
        with self.lock:
            v = self.d.get(k)
            if v is not None and time.time() - v[0] < ttl:
                self.d.move_to_end(k)
                return v[1]
        return None

    def set(self, k, val):
        with self.lock:
            self.d[k] = (time.time(), val)
            self.d.move_to_end(k)
            while len(self.d) > self.maxsize:
                self.d.popitem(last=False)

    def pop_matching(self, sub):
        with self.lock:
            for k in [k for k in self.d if sub in k]:
                self.d.pop(k, None)

    def __len__(self):
        return len(self.d)


class RateLimiter:
    def __init__(self, min_interval):
        self.min = min_interval
        self.lock = threading.Lock()
        self.next = 0.0

    def wait(self):
        with self.lock:
            now = time.time()
            w = max(0.0, self.next - now)
            self.next = max(now, self.next) + self.min
        if w > 0:
            time.sleep(w)


HISTORY = TTLCache(800)
BANDAR = TTLCache(900)
SCREENER = TTLCache(5)
STOCKBIT = TTLCache(300)
ARJUM_LIMITER = RateLimiter(0.8)
QUOTA = {"hit": False, "ts": 0.0}
UNIVERSE_SRC = {"v": "fallback"}
_SUSPEND = {}
_SUSPEND_LOCK = threading.Lock()


def is_suspended(sym):
    with _SUSPEND_LOCK:
        exp = _SUSPEND.get(sym)
        if exp and exp > time.time():
            return True
        _SUSPEND.pop(sym, None)
        return False


def mark_suspended(sym, ttl=6 * 3600):
    with _SUSPEND_LOCK:
        _SUSPEND[sym] = time.time() + ttl


def unsuspend(sym):
    with _SUSPEND_LOCK:
        _SUSPEND.pop(sym, None)


# ==================== INDIKATOR (SEED SMA, SAMA SEPERTI TRADINGVIEW) ====================
def _seeded_ma(s, n, alpha):
    s = pd.Series(s, dtype=float)
    v = s.to_numpy()
    out = np.full(len(v), np.nan)
    valid = np.flatnonzero(~np.isnan(v))
    if len(valid) == 0:
        return pd.Series(out, index=s.index)
    i0 = int(valid[0])
    if len(v) - i0 < n:
        return pd.Series(out, index=s.index)
    out[i0 + n - 1] = np.nanmean(v[i0:i0 + n])
    for i in range(i0 + n, len(v)):
        x = v[i]
        out[i] = out[i - 1] if np.isnan(x) else out[i - 1] * (1 - alpha) + x * alpha
    return pd.Series(out, index=s.index)


def ema(s, n):
    return _seeded_ma(s, n, 2.0 / (n + 1))


def rma(s, n):
    return _seeded_ma(s, n, 1.0 / n)


def rsi_series(close, n=14):
    d = close.diff()
    au = rma(d.clip(lower=0), n)
    ad = rma((-d).clip(lower=0), n)
    rs = au / ad.replace(0, np.nan)
    r = 100 - 100 / (1 + rs)
    r = r.where(ad != 0, 100.0)
    r = r.where(~((au == 0) & (ad == 0)), 50.0)
    return r


def calculate_rsi(prices, period=14):
    try:
        r = rsi_series(pd.Series(prices), period)
        v = r.iloc[-1]
        return float(v) if pd.notna(v) else 50.0
    except Exception:
        return 50.0


def calc_atr(df, period=14):
    pc = df['Close'].shift(1)
    tr = pd.concat([df['High'] - df['Low'], (df['High'] - pc).abs(), (df['Low'] - pc).abs()], axis=1).max(axis=1)
    return rma(tr, period)


def calc_dmi_adx(df, period=14):
    try:
        h, l, c = df['High'], df['Low'], df['Close']
        pc, ph, pl = c.shift(1), h.shift(1), l.shift(1)
        tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
        up, dn = h - ph, pl - l
        plus_dm = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=df.index)
        minus_dm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=df.index)
        trs, ps, ms = rma(tr, period), rma(plus_dm, period), rma(minus_dm, period)
        pdi = 100 * ps / trs.replace(0, np.nan)
        mdi = 100 * ms / trs.replace(0, np.nan)
        dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
        adx = rma(dx, period)
        return pdi.fillna(0), mdi.fillna(0), adx.fillna(0)
    except Exception:
        z = pd.Series(0.0, index=df.index)
        return z, z, z


def calculate_bollinger_bands(df, p=20, s=2):
    sma = df['Close'].rolling(p).mean()
    std = df['Close'].rolling(p).std(ddof=0)
    return sma, sma + std * s, sma - std * s


def clv_series(df):
    rng = (df['High'] - df['Low']).replace(0, np.nan)
    return (((df['Close'] - df['Low']) - (df['High'] - df['Close'])) / rng).fillna(0.0)


def cmf(df, n=20):
    mfv = clv_series(df) * df['Volume']
    den = df['Volume'].rolling(n).sum().replace(0, np.nan)
    return (mfv.rolling(n).sum() / den).fillna(0)


def obv(df):
    return (np.sign(df['Close'].diff()).fillna(0) * df['Volume']).cumsum()


def vol_ratio(df, n=20):
    v = df['Volume']
    if len(v) < n + 1:
        return 1.0
    base = v.iloc[-n - 1:-1].mean()
    return float(v.iloc[-1] / base) if base > 0 else 1.0


def last_ema(close, n):
    if len(close) < n:
        return float('nan')
    return float(ema(close, n).iloc[-1])


def f_round_ihsg(price):
    try:
        p = float(price)
        tick = 1.0 if p < 200 else 2.0 if p < 500 else 5.0 if p < 2000 else 10.0 if p < 5000 else 25.0
        return round(p / tick) * tick
    except Exception:
        return price


def normalize_timeframe(tf_input):
    if not tf_input:
        return "1d"
    tf = str(tf_input).lower().strip()
    m = {"5": "5m", "5m": "5m", "15": "15m", "15m": "15m", "30": "30m", "30m": "30m", "1h": "1h", "60": "1h", "60m": "1h",
         "4h": "4h", "1d": "1d", "d": "1d", "daily": "1d", "1w": "1w", "w": "1w"}
    return m.get(tf, "1d")


def format_timeframe_label(tf):
    return {"5m": "5 Menit", "15m": "15 Menit", "30m": "30 Menit", "1h": "1 Jam", "4h": "4 Jam", "1d": "Daily", "1w": "Weekly"}.get(normalize_timeframe(tf), str(tf).upper())


def fmt_rp(val, signed=True):
    a = abs(val)
    sign = ("-" if val < 0 else "+") if signed else ""
    if a >= 1e9:
        return f"{sign}{a / 1e9:.2f}B"
    if a >= 1e6:
        return f"{sign}{a / 1e6:.0f}M"
    return f"{sign}{a / 1e3:.0f}K"


# ==================== DATA ====================
def arjum_get(path, params=None, bypass_quota=False, retries=1):
    if not ARJUM_API_KEY:
        return None
    if QUOTA["hit"] and not bypass_quota and time.time() - QUOTA["ts"] < 180:
        return None
    for attempt in range(retries + 1):
        try:
            ARJUM_LIMITER.wait()
            r = requests.get(f"{ARJUM_BASE}{path}", headers={"X-API-Key": ARJUM_API_KEY, "Accept": "application/json"}, params=params, timeout=15)
            if r.status_code == 200:
                if QUOTA["hit"] and time.time() - QUOTA["ts"] > 60:
                    QUOTA["hit"] = False
                return r.json()
            if r.status_code == 429:
                QUOTA["hit"], QUOTA["ts"] = True, time.time()
                if attempt < retries:
                    time.sleep(5)
                    continue
            return None
        except Exception:
            if attempt < retries:
                time.sleep(2)
                continue
    return None


def get_screener_latest():
    c = SCREENER.get('latest', 300)
    if c:
        return c
    data = arjum_get("/screener/latest", bypass_quota=True, retries=1)
    if data and isinstance(data, dict) and data.get('rows'):
        SCREENER.set('latest', data)
        return data
    return {"rows": []}


def get_top_liquid_tickers(n=400):
    """Urut berdasarkan value (volume*close) dari screener. Jika screener gagal, pakai daftar universe (bukan urut value)."""
    try:
        rows = get_screener_latest().get('rows', [])
        flt = []
        for r in rows:
            code = (r.get('stock_code') or "").replace(".JK", "").upper()
            if len(code) != 4 or not code.isalpha() or code in FCA_EXCLUDE or is_suspended(code):
                continue
            close_v = float(r.get('close') or 0)
            vol_v = float(r.get('volume') or 0)
            if close_v < MIN_PRICE or vol_v == 0:
                continue
            flt.append((vol_v * close_v, code))
        if len(flt) >= max(20, int(n * 0.3)):
            flt.sort(reverse=True)
            UNIVERSE_SRC["v"] = "screener"
            return [c for _, c in flt[:n]]
    except Exception as e:
        clog(f"get_top_liquid err {e}")
    UNIVERSE_SRC["v"] = "fallback"
    return [c for c in IDX_FULL if c not in FCA_EXCLUDE and not is_suspended(c)][:n]


def universe_label(n=400):
    return f"TOP {n} BY VALUE" if UNIVERSE_SRC["v"] == "screener" else f"UNIVERSE FALLBACK (BUKAN urut value) max {n}"


def _clean_ohlcv(df):
    if df is None or len(df) == 0:
        return None
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df.rename(columns={c: str(c).capitalize() for c in df.columns})
    need = ['Open', 'High', 'Low', 'Close', 'Volume']
    if not all(c in df.columns for c in need):
        return None
    df = df[need].apply(pd.to_numeric, errors='coerce')
    df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])
    df['Volume'] = df['Volume'].fillna(0)
    df = df[~df.index.duplicated(keep='last')].sort_index()
    return df if len(df) else None


def _arjum_history(sym, limit=400):
    data = arjum_get(f"/history/{sym}", params={"limit": limit, "frame": "daily"}, bypass_quota=True, retries=1)
    rows = []
    if isinstance(data, dict):
        rows = data.get('data') or data.get('history') or []
    elif isinstance(data, list):
        rows = data
    if not rows:
        return None
    df = pd.DataFrame(rows)
    rn, has_date = {}, False
    for c in df.columns:
        cl = str(c).lower()
        if cl in ('o', 'open'):
            rn[c] = 'Open'
        elif cl in ('h', 'high'):
            rn[c] = 'High'
        elif cl in ('l', 'low'):
            rn[c] = 'Low'
        elif cl in ('c', 'close'):
            rn[c] = 'Close'
        elif cl in ('v', 'volume'):
            rn[c] = 'Volume'
        elif cl == 'date':
            rn[c] = 'Date'
            has_date = True
    if not has_date:
        for c in df.columns:
            if str(c).lower() == 'time':
                rn[c] = 'Date'
                break
    df = df.rename(columns=rn)
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.dropna(subset=['Date']).set_index('Date')
    return _clean_ohlcv(df)


_YF_INTERVAL = {"5m": "5m", "15m": "15m", "30m": "30m", "1h": "60m", "4h": "60m", "1d": "1d", "1w": "1wk"}
_YF_PERIODS = {"5m": ["5d"], "15m": ["30d"], "30m": ["60d"], "1h": ["60d"], "4h": ["60d"], "1d": ["2y", "1y", "6mo"], "1w": ["5y", "2y"]}


def _yf_history(sym, frame):
    try:
        import yfinance as yf
    except Exception:
        return None
    for per in _YF_PERIODS.get(frame, ["1y"]):
        try:
            h = yf.Ticker(f"{sym}.JK").history(period=per, interval=_YF_INTERVAL.get(frame, "1d"), auto_adjust=False)
            h = _clean_ohlcv(h)
            if h is None or len(h) < 10:
                continue
            if frame == "4h":
                h = h.resample('4h').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'}).dropna()
            return h
        except Exception as e:
            clog(f"YF err {sym} {frame} {per}: {e}")
    return None


def get_history(sym, frame="1d"):
    """Mengembalikan histori PENUH (daily: s.d. ~400-500 bar). Potong sendiri untuk tampilan, jangan untuk indikator."""
    sym = sym.upper().strip()
    frame = normalize_timeframe(frame)
    key = f"{sym}_{frame}"
    c = HISTORY.get(key, 600 if frame in ("1d", "1w") else 120)
    if c is not None:
        return c
    df = None
    if frame == "1d":
        df = _arjum_history(sym, 400)
        if df is None or len(df) < 250:
            y = _yf_history(sym, frame)
            if y is not None and (df is None or len(y) > len(df)):
                df = y
    else:
        df = _yf_history(sym, frame)
    if df is None or len(df) < 10:
        clog(f"get_history FAIL {sym} {frame}")
        return None
    HISTORY.set(key, df)
    return df


def tradable(df):
    try:
        if float(df['Close'].iloc[-1]) < MIN_PRICE:
            return False
        if float(df['Volume'].tail(5).sum()) == 0:
            return False
        if df['Close'].tail(10).nunique() == 1 and int(df['Close'].iloc[-1]) == 100:
            return False
        return True
    except Exception:
        return False


def load_daily(sym, min_len=60):
    if sym in FCA_EXCLUDE or is_suspended(sym):
        return None
    df = get_history(sym, "1d")
    if df is None or len(df) < min_len:
        return None
    if not tradable(df):
        mark_suspended(sym)
        return None
    return df


def get_realtime_price(sym):
    """Scraping Stockbit rapuh (bisa berubah/butuh login). Hanya dipakai untuk chart, gagal = diabaikan."""
    c = STOCKBIT.get(sym, 5)
    if c is not None:
        return c
    try:
        r = requests.get(f"https://stockbit.com/symbol/{sym}", headers={"User-Agent": "Mozilla/5.0", "Referer": "https://stockbit.com/"}, timeout=8)
        if r.status_code == 200:
            m = re.search(r'"lastPrice"\s*:\s*(\d+(?:\.\d+)?)', r.text)
            if m:
                p = float(m.group(1))
                if p >= MIN_PRICE:
                    STOCKBIT.set(sym, p)
                    return p
    except Exception:
        pass
    return 0.0


def apply_realtime(df, price):
    """Update candle terakhir hanya jika candle itu candle hari ini dan harga wajar."""
    try:
        if not price or price < MIN_PRICE:
            return df
        if df.index[-1].date() != now_wib().date():
            return df
        lc = float(df['Close'].iloc[-1])
        if lc <= 0 or abs(price - lc) / lc > 0.35:
            return df
        d = df.copy()
        i = len(d) - 1
        d.iloc[i, d.columns.get_loc('Close')] = price
        d.iloc[i, d.columns.get_loc('High')] = max(float(d['High'].iloc[i]), price)
        d.iloc[i, d.columns.get_loc('Low')] = min(float(d['Low'].iloc[i]), price)
        return d
    except Exception:
        return df


def snap(df):
    c = float(df['Close'].iloc[-1])
    p = float(df['Close'].iloc[-2]) if len(df) > 1 else c
    return {"close": int(round(c)), "chg": (c / p - 1) * 100 if p else 0.0, "vr": vol_ratio(df), "rsi": calculate_rsi(df['Close'])}


def base_row(sym, df, **extra):
    r = snap(df)
    r["symbol"] = sym
    r.update(extra)
    return r


def run_parallel(items, fn, workers=SCAN_WORKERS):
    def safe(x):
        try:
            return fn(x)
        except Exception as e:
            clog(f"scan item err {x}: {e}")
            return None
    out = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for f in as_completed([ex.submit(safe, it) for it in items]):
            r = f.result()
            if r:
                out.append(r)
    return out


# ==================== BANDARMOLOGY ====================
BROKER_STATE = {"date_max": None, "weekly_ok": None, "fails": 0, "disabled_until": 0.0}


def broker_enabled():
    if not (ARJUM_BROKER_PATH and ARJUM_API_KEY and "{symbol}" in ARJUM_BROKER_PATH):
        return False
    if time.time() < BROKER_STATE["disabled_until"]:
        return False
    return not (QUOTA["hit"] and time.time() - QUOTA["ts"] < 180)


def arjum_request(path, params=None, timeout=15):
    """Mengembalikan (status_http, body). Dipakai untuk broker agar error 404/429 terlihat, bukan ditelan."""
    if not ARJUM_API_KEY:
        return 0, None
    try:
        ARJUM_LIMITER.wait()
        r = requests.get(f"{ARJUM_BASE}{path}", headers={"X-API-Key": ARJUM_API_KEY, "Accept": "application/json"}, params=params, timeout=timeout)
        if r.status_code == 429:
            QUOTA["hit"], QUOTA["ts"] = True, time.time()
        try:
            body = r.json()
        except Exception:
            body = r.text[:300]
        return r.status_code, body
    except Exception as e:
        return -1, str(e)


def _f(d, keys, default=0.0):
    for k in keys:
        if isinstance(d, dict) and d.get(k) is not None:
            try:
                return float(d[k])
            except Exception:
                continue
    return default


def _extract_rows(raw):
    if isinstance(raw, list):
        return raw
    if isinstance(raw, dict):
        for k in ('brokers', 'data', 'broker_summary', 'summary', 'result', 'broker_data'):
            if isinstance(raw.get(k), list):
                return raw[k]
    return []


def _date10(v):
    return str(v)[:10] if v else None


def parse_broker_data(raw, sym=""):
    """Skema Arjum: raw['brokers'] = [{broker_code, bval, sval, nval, bvol, svol, nvol, bfrq, sfrq}],
    raw['broker_net'] = total pasar (bval = sval, nval = 0).
    Net seluruh broker selalu 0 (zero-sum), jadi akum/dist diukur dari top-3 net buyer vs top-3 net seller."""
    rows = _extract_rows(raw)
    brokers = []
    for it in rows:
        if not isinstance(it, dict):
            continue
        code = str(it.get('broker_code') or it.get('broker') or it.get('code') or "").upper().strip()
        if not code or len(code) > 4:
            continue
        buy = _f(it, ['bval', 'buy_value', 'buy', 'total_buy_value'])
        sell = _f(it, ['sval', 'sell_value', 'sell', 'total_sell_value'])
        net = _f(it, ['nval', 'net_value', 'net'], None)
        if net is None:
            net = buy - sell
        brokers.append({"broker": code, "name": str(it.get('broker_name') or ""), "buy_value": buy, "sell_value": sell, "net_value": net,
                        "buy_vol": _f(it, ['bvol', 'buy_volume']), "sell_vol": _f(it, ['svol', 'sell_volume']),
                        "bfrq": _f(it, ['bfrq']), "sfrq": _f(it, ['sfrq'])})
    if len(brokers) < 3:
        return None
    bn = raw.get("broker_net") if isinstance(raw, dict) and isinstance(raw.get("broker_net"), dict) else {}
    total_value = _f(bn, ['bval']) or sum(b['buy_value'] for b in brokers)
    if total_value <= 0:
        return None
    buyers = sorted([b for b in brokers if b['net_value'] > 0], key=lambda b: b['net_value'], reverse=True)
    sellers = sorted([b for b in brokers if b['net_value'] < 0], key=lambda b: b['net_value'])
    top_b, top_s = buyers[:3], sellers[:3]
    tb = sum(b['net_value'] for b in top_b)
    ts = sum(b['net_value'] for b in top_s)
    acc = tb / (tb + abs(ts)) if (tb + abs(ts)) > 0 else 0.5
    bv, sv = sum(b['buy_vol'] for b in top_b), sum(b['sell_vol'] for b in top_s)
    bf = sum(b['bfrq'] for b in top_b)
    return {"source": "BROKER", "net_value": tb + ts, "top3_buy": tb, "top3_sell": ts, "acc_ratio": acc, "buy_pct": acc * 100,
            "intensity": (tb + ts) / total_value, "net_pct": (tb + ts) / total_value * 100, "total_value": total_value,
            "top_buyers": top_b, "top_sellers": top_s,
            "avg_buy": sum(b['buy_value'] for b in top_b) / bv if bv > 0 else 0.0,
            "avg_sell": sum(b['sell_value'] for b in top_s) / sv if sv > 0 else 0.0,
            "lot_per_trx": bv / bf / 100.0 if bf > 0 else 0.0,
            "coverage": sum(b['buy_value'] for b in brokers) / total_value, "n_brokers": len(brokers),
            "foreign_net": sum(b['net_value'] for b in brokers if b['broker'] in FOREIGN_BROKERS),
            "range_start": _date10(raw.get('broker_start_date')) if isinstance(raw, dict) else None,
            "range_end": _date10(raw.get('broker_end_date')) if isinstance(raw, dict) else None,
            "date_max": _date10(raw.get('broker_date_max')) if isinstance(raw, dict) else None}


def proxy_flow(df, period="daily"):
    """ESTIMASI aliran dana dari OHLCV (Close Location Value x Volume). BUKAN data broker."""
    n = 1 if period == "daily" else 5
    d = df.tail(n)
    tp = (d['High'] + d['Low'] + d['Close']) / 3
    fv = clv_series(d) * d['Volume']
    net_vol, net_val, tot = float(fv.sum()), float((fv * tp).sum()), float(d['Volume'].sum())
    buy_pct = 50 + 50 * net_vol / tot if tot > 0 else 50.0
    adv = float((df['Close'] * df['Volume']).tail(20).mean())
    ob = obv(df)
    return {"source": "PROXY", "net_value": net_val, "net_volume": net_vol, "buy_pct": buy_pct,
            "intensity": net_val / (adv * n) if adv > 0 else 0.0, "cmf20": float(cmf(df, 20).iloc[-1]),
            "obv_rising": bool(len(ob) > 11 and ob.iloc[-1] > ob.iloc[-11]),
            "top_buyers": [], "top_sellers": [], "foreign_net": 0.0}


def classify_flow(a):
    if a['source'] == 'BROKER':
        a['is_akum'] = a['acc_ratio'] >= 0.55 and a['net_pct'] >= BROKER_MIN_NET_PCT
        a['is_dist'] = a['acc_ratio'] <= 0.45 and a['net_pct'] <= -BROKER_MIN_NET_PCT
    else:
        a['is_akum'] = a['intensity'] >= 0.10 and a['buy_pct'] >= 55 and a['net_value'] > 0
        a['is_dist'] = a['intensity'] <= -0.10 and a['buy_pct'] <= 45 and a['net_value'] < 0
    return a


def strong_flow(a):
    """Skala intensitas berbeda: broker = % net top-3 dari total transaksi, proxy = flow relatif terhadap rata-rata nilai harian."""
    return abs(a['intensity']) >= (0.08 if a['source'] == 'BROKER' else 0.25)


def fetch_broker_analysis(sym, period="daily"):
    if period == "weekly" and BROKER_STATE["weekly_ok"] is False:
        return None
    params = {}
    if ARJUM_BROKER_FLOW_PARAM:
        params[ARJUM_BROKER_FLOW_PARAM] = ARJUM_BROKER_FLOW
    if period == "weekly":
        end = BROKER_STATE["date_max"] or now_wib().date()
        params[ARJUM_BROKER_START_PARAM] = (end - datetime.timedelta(days=6)).isoformat()
        params[ARJUM_BROKER_END_PARAM] = end.isoformat()
    status, body = arjum_request(ARJUM_BROKER_PATH.format(symbol=sym), params or None)
    if status != 200 or not isinstance(body, dict):
        if status != 429:
            BROKER_STATE["fails"] += 1
            if BROKER_STATE["fails"] >= 5:
                BROKER_STATE["disabled_until"] = time.time() + 900
                BROKER_STATE["fails"] = 0
                clog(f"broker API gagal berulang (HTTP {status}): dinonaktifkan 15 menit. Jalankan /brokerprobe")
        return None
    BROKER_STATE["fails"] = 0
    res = parse_broker_data(body, sym)
    if res is None:
        return None
    if res.get("date_max"):
        try:
            BROKER_STATE["date_max"] = datetime.date.fromisoformat(res["date_max"])
        except Exception:
            pass
    if period == "weekly":
        if res["range_start"] and res["range_start"] == res["range_end"]:
            BROKER_STATE["weekly_ok"] = False
            clog("broker weekly: parameter tanggal tidak berpengaruh (rentang 1 hari). Weekly memakai PROXY. Jalankan /brokerprobe")
            return None
        BROKER_STATE["weekly_ok"] = True
    return res


def get_bandar_analysis(sym, period="daily", df=None, allow_broker=True):
    use_b = allow_broker and broker_enabled()
    key = f"{sym}_{period}_{'b' if use_b else 'p'}"
    c = BANDAR.get(key, 900 if (use_b and not market_open()) else 300)
    if c is not None:
        return c
    res = fetch_broker_analysis(sym, period) if use_b else None
    if res is None:
        df = df if df is not None else get_history(sym, "1d")
        if df is None or len(df) < 30:
            return None
        res = proxy_flow(df, period)
    res.setdefault("cmf20", 0.0)
    res["symbol"], res["period"] = sym, period
    classify_flow(res)
    BANDAR.set(key, res)
    return res


def bandar_universe(n):
    t = get_top_liquid_tickers(n)
    return t[:BROKER_SCAN_N] if broker_enabled() else t


def broker_detail_text(sym, period="daily", df=None):
    a = get_bandar_analysis(sym, period, df)
    if a is None:
        return f"Data {esc(sym)} tidak tersedia"
    verdict = "AKUMULASI" if a['is_akum'] else "DISTRIBUSI" if a['is_dist'] else "NETRAL"
    t = f"<b>BANDAR {esc(sym)} {period.upper()}</b> [{a['source']}]\n"
    if a['source'] == 'BROKER':
        last = float(df['Close'].iloc[-1]) if df is not None and len(df) else 0.0
        t += f"Data: {esc(a['range_start'])} s.d. {esc(a['range_end'])} | {a['n_brokers']} broker | cakupan {a['coverage'] * 100:.0f}%\n"
        t += (f"Net top-3: {fmt_rp(a['net_value'])} ({a['net_pct']:+.1f}% dari total {fmt_rp(a['total_value'], False)}) | "
              f"rasio akum {a['acc_ratio'] * 100:.0f}%\n")
        t += "\n<b>Top net buyer</b>\n"
        for b in a['top_buyers']:
            avg = b['buy_value'] / b['buy_vol'] if b['buy_vol'] > 0 else 0
            t += f"  {esc(b['broker'])} net {fmt_rp(b['net_value'])} | avg beli {avg:.0f} | {b['bfrq']:.0f} trx\n"
        t += "<b>Top net seller</b>\n"
        for b in a['top_sellers']:
            avg = b['sell_value'] / b['sell_vol'] if b['sell_vol'] > 0 else 0
            t += f"  {esc(b['broker'])} net {fmt_rp(b['net_value'])} | avg jual {avg:.0f} | {b['sfrq']:.0f} trx\n"
        if a['avg_buy'] > 0 and last > 0:
            t += f"\nAvg beli top-3: {a['avg_buy']:.0f} | harga {last:.0f} ({(last / a['avg_buy'] - 1) * 100:+.1f}%)\n"
        t += f"Rata-rata lot per transaksi top buyer: {a['lot_per_trx']:.0f}\n"
        t += f"Net broker asing (daftar lokal, belum terverifikasi): {fmt_rp(a['foreign_net'])}\n"
    else:
        t += (f"Net est {fmt_rp(a['net_value'])} | Flow/ADV {a['intensity'] * 100:+.0f}% | Buy% est {a['buy_pct']:.0f}\n"
              f"CMF20 {a.get('cmf20', 0):+.2f} | OBV naik: {'ya' if a.get('obv_rising') else 'tidak'}\n{PROXY_NOTE}\n")
    return t + f"\n<b>{verdict}</b>"


def probe_broker(cid, sym="BBCA"):
    """Deteksi path, parameter tanggal, dan parameter flow endpoint broker. Maks ~12 request."""
    out = [f"<b>BROKER PROBE {esc(sym)}</b>"]
    if not ARJUM_API_KEY:
        send_reply(cid, "ARJUM_API_KEY kosong.")
        return
    cands = [ARJUM_BROKER_PATH] if ARJUM_BROKER_PATH else []
    cands += [p for p in ("/broker/{symbol}", "/broker-summary/{symbol}", "/broker_summary/{symbol}", "/brokers/{symbol}", "/bandarmology/{symbol}") if p not in cands]
    found, base = None, None
    for p in cands:
        if "{symbol}" not in p:
            continue
        st, body = arjum_request(p.format(symbol=sym))
        keys = list(body.keys())[:8] if isinstance(body, dict) else type(body).__name__
        out.append(f"{esc(p)} -> HTTP {st} keys={esc(keys)}")
        if st == 200 and isinstance(body, dict) and isinstance(body.get("brokers"), list):
            found, base = p, body
            break
        if st == 429:
            out.append("Quota habis, probe dihentikan.")
            break
    if not found:
        out.append("\n❌ Endpoint broker tidak ditemukan. Cek dokumentasi Arjum lalu isi ARJUM_BROKER_PATH (contoh /broker/{symbol}).")
        send_reply(cid, "\n".join(out))
        return
    out.append(f"\n✅ Path: <code>{esc(found)}</code>")
    out.append(f"Broker: {len(base['brokers'])} | rentang default {esc(base.get('broker_start_date'))}..{esc(base.get('broker_end_date'))} | tersedia {esc(base.get('broker_date_min'))}..{esc(base.get('broker_date_max'))}")
    parsed = parse_broker_data(base, sym)
    if parsed:
        out.append(f"Parse OK: net top-3 {fmt_rp(parsed['net_value'])} ({parsed['net_pct']:+.1f}%), cakupan {parsed['coverage'] * 100:.0f}%")
    else:
        out.append("⚠ Parse gagal: struktur respons tidak dikenali")
    base_total = _f(base.get("broker_net", {}), ['bval'])
    dm = base.get("broker_date_max")
    path = found.format(symbol=sym)
    rec = [f"ARJUM_BROKER_PATH={found}"]
    if dm:
        end = datetime.date.fromisoformat(str(dm)[:10])
        start = end - datetime.timedelta(days=6)
        ok_params = False
        for sp, ep in (("start_date", "end_date"), ("from", "to"), ("start", "end"), ("date_from", "date_to")):
            st, body = arjum_request(path, {sp: start.isoformat(), ep: end.isoformat()})
            b0, b1 = (_date10(body.get('broker_start_date')), _date10(body.get('broker_end_date'))) if isinstance(body, dict) else (None, None)
            ok = st == 200 and b0 and b1 and b0 != b1
            out.append(f"param {sp}/{ep} -> HTTP {st} rentang {esc(b0)}..{esc(b1)}{' ✅' if ok else ''}")
            if ok:
                rec += [f"ARJUM_BROKER_START_PARAM={sp}", f"ARJUM_BROKER_END_PARAM={ep}"]
                ok_params = True
                break
            if st == 429:
                break
        if not ok_params:
            out.append("⚠ Parameter tanggal belum ketemu: /topakum weekly akan memakai PROXY.")
    for fv in ("foreign", "domestic"):
        st, body = arjum_request(path, {"flow": fv})
        if st == 200 and isinstance(body, dict):
            tot = _f(body.get("broker_net", {}), ['bval'])
            out.append(f"flow={fv} -> echo '{esc(body.get('flow'))}', total {fmt_rp(tot, False)} (all: {fmt_rp(base_total, False)}){' ✅ berpengaruh' if tot and abs(tot - base_total) > 1 else ''}")
        elif st == 429:
            break
    out.append("\n<b>Set di .env / Colab Secrets:</b>\n<code>" + esc("\n".join(rec)) + "</code>")
    send_reply(cid, "\n".join(out))


def scan_bandar(kind="akum", period="daily", n=400, top_n=30):
    def chk(sym):
        df = load_daily(sym, 40)
        if df is None:
            return None
        a = get_bandar_analysis(sym, period, df)
        if a is None or not (a['is_akum'] if kind == "akum" else a['is_dist']):
            return None
        return base_row(sym, df, a=a, net=a['net_value'], buy_pct=a['buy_pct'], intensity=a['intensity'], src=a['source'])
    res = run_parallel(bandar_universe(n), chk)
    res.sort(key=lambda x: x['intensity'], reverse=(kind == "akum"))
    return res[:top_n]


def scan_bandar_both(period="daily", n=400, top_n=20):
    def chk(sym):
        df = load_daily(sym, 40)
        if df is None:
            return None
        a = get_bandar_analysis(sym, period, df)
        if a is None or not (a['is_akum'] or a['is_dist']):
            return None
        return base_row(sym, df, a=a, net=a['net_value'], buy_pct=a['buy_pct'], intensity=a['intensity'], src=a['source'], kind="akum" if a['is_akum'] else "dist")
    res = run_parallel(bandar_universe(n), chk)
    ak = sorted([r for r in res if r['kind'] == "akum"], key=lambda x: x['intensity'], reverse=True)[:top_n]
    di = sorted([r for r in res if r['kind'] == "dist"], key=lambda x: x['intensity'])[:top_n]
    return ak, di


def scan_whale(period="daily", n=400, top_n=30):
    min_net = 1e9 if period == "daily" else 5e9

    def chk(sym):
        df = load_daily(sym, 60)
        if df is None:
            return None
        a = get_bandar_analysis(sym, period, df)
        if a is None or not a['is_akum'] or a['net_value'] < min_net:
            return None
        e200 = last_ema(df['Close'], 200)
        last = float(df['Close'].iloc[-1])
        if not np.isnan(e200) and last < e200 * 0.95:
            return None
        s = snap(df)
        score = (2 if strong_flow(a) else 1) + (2 if a['net_value'] >= min_net * 3 else 1)
        score += 1 if a.get('cmf20', 0) > 0.1 else 0
        score += 1 if s['vr'] >= 1.2 else 0
        score += 1 if (a['source'] == 'BROKER' and a.get('foreign_net', 0) > 0) else 0
        score += 1 if 40 <= s['rsi'] <= 75 else 0
        score += 1 if not np.isnan(e200) else 0
        return base_row(sym, df, a=a, net=a['net_value'], buy_pct=a['buy_pct'], intensity=a['intensity'], src=a['source'], score=score)
    res = run_parallel(bandar_universe(n), chk)
    res.sort(key=lambda x: (x['score'], x['net']), reverse=True)
    return res[:top_n]


def scan_insider(period="daily", n=400, top_n=30):
    def chk(sym):
        df = load_daily(sym, 60)
        if df is None:
            return None
        a = get_bandar_analysis(sym, period, df)
        if a is None or a['net_value'] <= 0 or not (a['is_akum'] if a['source'] == 'BROKER' else a['intensity'] >= 0.05):
            return None
        last = float(df['Close'].iloc[-1])
        lo20, hi20 = float(df['Low'].tail(20).min()), float(df['High'].tail(20).max())
        pos = (last - lo20) / (hi20 - lo20) if hi20 > lo20 else 0.5
        s = snap(df)
        e50 = last_ema(df['Close'], 50)
        score = sum([pos <= 0.45, s['vr'] <= 2.0, abs(s['chg']) <= 3.0, s['rsi'] <= 60,
                     (not np.isnan(e50) and last <= e50 * 1.08), a.get('cmf20', 0) > 0 or a['source'] == 'BROKER'])
        if score < 4:
            return None
        return base_row(sym, df, a=a, net=a['net_value'], buy_pct=a['buy_pct'], src=a['source'], score=int(score), pos=pos * 100)
    res = run_parallel(bandar_universe(n), chk)
    res.sort(key=lambda x: (x['score'], x['net']), reverse=True)
    return res[:top_n]


# ==================== SINYAL KIM & BSJP ====================
def detect_kim_signal(df, timeframe="1d"):
    tf = normalize_timeframe(timeframe)
    allowed = tf in ("1d", "1h")
    empty = {"buy": False, "b_modal": 0, "tp1": 0, "tp2": 0, "sl": 0, "markers": [], "allowed_tf": allowed, "b_modal_series": None}
    if df is None or len(df) < 260:     # butuh EMA200 valid
        return empty
    try:
        c, o, h, l, v = df['Close'], df['Open'], df['High'], df['Low'], df['Volume']
        ema_f, ema_s = ema(c, 20), ema(c, 200)
        _, _, adx = calc_dmi_adx(df, 14)
        atr = calc_atr(df, 14)
        v_sma = v.rolling(20).mean()
        v_akum = (c > o) & (v > v_sma)
        v_spike = v > v_sma * 1.8
        hlc3 = (h + l + c) / 3
        sum_vp = (hlc3 * v).where(v_akum, 0).rolling(60, min_periods=1).sum()
        sum_v = v.where(v_akum, 0).rolling(60, min_periods=1).sum()
        bm = (sum_vp / sum_v.replace(0, np.nan)).fillna(c).apply(f_round_ihsg)
        common = (v_akum | v_spike) & (adx > 20) & (atr > atr.rolling(20).mean()) & (c > ema_s) & (((c - bm).abs() / bm * 100) <= 40)
        cross = (c.shift(1) <= bm.shift(1)) & (c > bm)
        raw_b = cross & (c > ema_f) & common
        re_entry = (c > h.shift(1).rolling(10).max()) & (c > bm) & common
        buy = bool(raw_b.iloc[-1] or re_entry.iloc[-1]) and allowed
        last = float(c.iloc[-1])
        atr_v = float(atr.iloc[-1]) if pd.notna(atr.iloc[-1]) else 0.0
        entry = f_round_ihsg(last)
        sl = f_round_ihsg(min(last - 2 * atr_v, float(l.tail(20).min())))
        risk = entry - sl if entry > sl else max(atr_v, 1.0)
        markers = []
        if allowed:
            for i in np.flatnonzero((raw_b | re_entry).to_numpy()):
                markers.append({"idx": int(i), "type": "KIM BUY" if raw_b.iloc[i] else "KIM RE-ENTRY"})
        return {"buy": buy, "b_modal": float(bm.iloc[-1]), "b_modal_series": bm, "sl": sl,
                "tp1": f_round_ihsg(entry + risk), "tp2": f_round_ihsg(entry + 2 * risk), "markers": markers, "allowed_tf": allowed}
    except Exception as e:
        clog(f"KIM err {e}")
        return empty


def detect_bsjp_signal(df, timeframe="15m", min_vol=2_000_000):
    tf = normalize_timeframe(timeframe)
    allowed = tf in ("5m", "15m")
    empty = {"buy": False, "markers": [], "allowed_tf": allowed, "tp1": 0, "sl": 0}
    if df is None or len(df) < 210 or not allowed:
        return empty
    try:
        c, o, h, l, v = df['Close'], df['Open'], df['High'], df['Low'], df['Volume']
        idx = df.index
        if getattr(idx, 'tz', None) is not None:
            idx = idx.tz_convert(TIMEZONE_WIB)
        minutes = pd.Series(idx.hour * 60 + idx.minute, index=df.index)
        sore = (minutes >= 14 * 60 + 30) & (minutes <= 15 * 60 + 45)
        lookback = 4 if tf == "15m" else 10
        cond = (sore & (v > v.rolling(20).mean() * 1.8) & (((c - o) / o.replace(0, np.nan) * 100) >= 0.5) & (c > o)
                & (c > h.shift(1).rolling(lookback).max()) & (v >= min_vol) & (c > ema(c, 200)))
        dates = pd.Series(idx.date, index=df.index)
        first = cond & (cond.astype(int).groupby(dates).cumsum() == 1)
        buy = bool(first.iloc[-1])
        markers = [{"idx": int(i), "type": "BSJP BUY SORE"} for i in np.flatnonzero(first.to_numpy())]
        tp1 = sl = 0
        if buy:
            tp1, sl = f_round_ihsg(float(h.iloc[-1]) * 1.03), f_round_ihsg(float(l.iloc[-1]) * 0.97)
        return {"buy": buy, "markers": markers, "tp1": tp1, "sl": sl, "allowed_tf": allowed}
    except Exception as e:
        clog(f"BSJP err {e}")
        return empty


def scan_volume_spike(threshold=1.5, n=400, min_value=5e8):
    def chk(sym):
        df = load_daily(sym, 30)
        if df is None:
            return None
        vr = vol_ratio(df)
        value = float(df['Volume'].iloc[-1] * df['Close'].iloc[-1])
        if vr < threshold or value < min_value:
            return None
        return base_row(sym, df, value=value)
    res = run_parallel(get_top_liquid_tickers(n), chk)
    res.sort(key=lambda x: x['vr'], reverse=True)
    return res


def scan_kim_bsjp(n=400):
    t = now_wib()
    check_bsjp = (t.hour * 60 + t.minute >= 14 * 60 + 30) or t.hour < 9   # BSJP hanya relevan sore s.d. besok pagi

    def chk(sym):
        df = load_daily(sym, 260)
        if df is None:
            return None
        kim = detect_kim_signal(df, "1d")
        bsjp = {"buy": False}
        if check_bsjp:
            d15 = get_history(sym, "15m")
            if d15 is not None:
                bsjp = detect_bsjp_signal(d15, "15m")
        if not (kim['buy'] or bsjp['buy']):
            return None
        return base_row(sym, df, kim_buy=kim['buy'], bsjp_buy=bsjp['buy'], kim=kim, bsjp=bsjp)
    res = run_parallel(get_top_liquid_tickers(n), chk)
    res.sort(key=lambda x: (x['kim_buy'] and x['bsjp_buy'], x['vr']), reverse=True)
    return res


# ==================== PATTERN ====================
def _swing_lows(low, k=3):
    v = low.values
    return [i for i in range(k, len(v) - k) if v[i] == v[i - k:i + k + 1].min()]


def detect_patterns(df):
    out = {}
    if df is None or len(df) < 60:
        return out
    try:
        c, o, h, l, v = df['Close'], df['Open'], df['High'], df['Low'], df['Volume']
        last, vr, rsi = float(c.iloc[-1]), vol_ratio(df), calculate_rsi(c)
        green = c.iloc[-1] > o.iloc[-1]
        e20, e50, e200 = ema(c, 20), ema(c, 50), ema(c, 200)
        e20l, e50l = float(e20.iloc[-1]), float(e50.iloc[-1])
        e200l = float(e200.iloc[-1]) if pd.notna(e200.iloc[-1]) else float('nan')
        has200 = not np.isnan(e200l)
        uptrend = has200 and last > e200l and e50l > e200l

        prior_high = float(h.iloc[-21:-1].max())
        if last > prior_high and vr >= 1.3 and last > e20l:
            out['breakout'] = {"score": 8, "desc": f"Close>{prior_high:.0f} (High20) Vol{vr:.1f}x"}
        if uptrend and float(l.tail(3).min()) <= e50l * 1.01 and last >= e50l and green and 40 <= rsi <= 65:
            out['pullback_ema50'] = {"score": 7, "desc": f"Pullback EMA50 {(last / e50l - 1) * 100:+.1f}% uptrend"}
        if has200 and -3 <= (last / e200l - 1) * 100 <= 4 and green and 35 <= rsi <= 60:
            out['pullback_ema200'] = {"score": 6, "desc": f"Dekat EMA200 {(last / e200l - 1) * 100:+.1f}%"}

        w = df.tail(70)
        sl_idx = _swing_lows(w['Low'], 3)
        if len(sl_idx) >= 2:
            i2 = sl_idx[-1]
            cand = [i for i in sl_idx[:-1] if i2 - i >= 7]
            if cand:
                i1 = cand[-1]
                lo1, lo2 = float(w['Low'].iloc[i1]), float(w['Low'].iloc[i2])
                diff = abs(lo1 - lo2) / ((lo1 + lo2) / 2) * 100
                neck = float(w['High'].iloc[i1:i2 + 1].max())
                depth = (neck - max(lo1, lo2)) / max(lo1, lo2) * 100
                if diff <= 3.5 and depth >= 5 and (len(w) - 1 - i2) <= 25 and neck * 0.98 <= last <= neck * 1.07:
                    out['double_bottom'] = {"score": 7, "desc": f"DB {lo1:.0f}/{lo2:.0f} neckline {neck:.0f}"}

        pole = (float(c.iloc[-6]) / float(c.iloc[-16]) - 1) * 100
        pole_top = float(h.iloc[-15:-5].max())
        base_p = float(c.iloc[-16])
        flag_rng = (float(h.tail(5).max()) - float(l.tail(5).min())) / float(l.tail(5).min()) * 100
        if pole >= 15 and pole_top > base_p:
            retr = (pole_top - last) / (pole_top - base_p)
            if flag_rng <= 8 and 0 <= retr <= 0.5 and v.tail(5).mean() < v.iloc[-15:-5].mean():
                out['bull_flag'] = {"score": 6, "desc": f"Pole +{pole:.0f}% flag {flag_rng:.1f}% retr {retr * 100:.0f}%"}

        body = abs(float(c.iloc[-1]) - float(o.iloc[-1]))
        lower = min(float(o.iloc[-1]), last) - float(l.iloc[-1])
        upper = float(h.iloc[-1]) - max(float(o.iloc[-1]), last)
        if body > 0 and lower >= 2 * body and upper <= 0.5 * body and last <= e20l and float(l.iloc[-1]) <= float(l.tail(20).min()) * 1.02:
            out['hammer'] = {"score": 6, "desc": "Hammer di area low 20 hari"}
        o1, c1 = float(o.iloc[-2]), float(c.iloc[-2])
        if green and c1 < o1 and last > o1 and float(o.iloc[-1]) < c1 and body > (o1 - c1) and c1 < float(e20.iloc[-2]):
            out['bull_engulfing'] = {"score": 7, "desc": "Bullish engulfing setelah tekanan turun"}

        _, up_bb, lo_bb = calculate_bollinger_bands(df, 20, 2)
        sma = df['Close'].rolling(20).mean()
        bbw = ((up_bb - lo_bb) / sma * 100).dropna()
        if len(bbw) >= 60 and bbw.iloc[-1] <= bbw.tail(120).quantile(0.2) and vr < 1.3 and last > e50l * 0.97:
            out['squeeze'] = {"score": 5, "desc": f"BB squeeze {bbw.iloc[-1]:.1f}% (persentil<=20)"}
    except Exception as e:
        clog(f"detect_patterns err {e}")
    return out


def scan_chart_patterns(pattern_name="all", n=400, top_n=30):
    def chk(sym):
        df = load_daily(sym, 100)
        if df is None:
            return None
        pats = detect_patterns(df)
        if not pats or (pattern_name != "all" and pattern_name not in pats):
            return None
        key = max(pats, key=lambda k: pats[k]['score']) if pattern_name == "all" else pattern_name
        return base_row(sym, df, pattern=key, desc=pats[key]['desc'], score=pats[key]['score'])
    res = run_parallel(get_top_liquid_tickers(n), chk)
    res.sort(key=lambda x: x['score'], reverse=True)
    return res[:top_n]


# ==================== EMA PROXIMITY ====================
def scan_near_ema(period=50, thr=3.0, mode="near", n=400, top_n=30):
    need = period + 60

    def chk(sym):
        df = load_daily(sym, need)
        if df is None:
            return None
        c = df['Close']
        e = ema(c, period)
        el = float(e.iloc[-1])
        if np.isnan(el) or el == 0:
            return None
        last = float(c.iloc[-1])
        dist = (last / el - 1) * 100
        green = last > float(df['Open'].iloc[-1])
        if mode == "near":
            ok = abs(dist) <= thr
        elif mode == "above":
            ok = 0 <= dist <= thr
        elif mode == "below":
            ok = -thr <= dist <= 0
        elif mode == "bounce":
            ok = float(df['Low'].tail(3).min()) <= el * 1.005 and last >= el and green and dist <= thr
        elif mode == "pullback":
            e200 = last_ema(c, 200)
            e50 = last_ema(c, 50)
            ok = (not np.isnan(e200)) and last > e200 and e50 > e200 and 0 <= dist <= thr and green
        else:
            ok = abs(dist) <= thr
        if not ok:
            return None
        return base_row(sym, df, ema=int(el), dist=dist, abs_dist=abs(dist))
    res = run_parallel(get_top_liquid_tickers(n), chk)
    res.sort(key=lambda x: x['abs_dist'])
    return res[:top_n]


def scan_golden_area(thr=4.0, n=400, top_n=30):
    def chk(sym):
        df = load_daily(sym, 260)
        if df is None:
            return None
        c = df['Close']
        e50, e200 = last_ema(c, 50), last_ema(c, 200)
        if np.isnan(e200) or e200 == 0:
            return None
        last = float(c.iloc[-1])
        d50, d200, gap = (last / e50 - 1) * 100, (last / e200 - 1) * 100, (e50 / e200 - 1) * 100
        if abs(d50) <= thr and abs(d200) <= thr + 2 and abs(gap) <= 5:
            return base_row(sym, df, ema50=int(e50), ema200=int(e200), d50=d50, d200=d200, gap=gap, abs_dist=(abs(d50) + abs(d200)) / 2)
        return None
    res = run_parallel(get_top_liquid_tickers(n), chk)
    res.sort(key=lambda x: x['abs_dist'])
    return res[:top_n]


# ==================== ORDER BLOCK & FVG (DENGAN MITIGASI) ====================
def find_order_blocks(df, lookback=60, min_impulse_pct=4.0):
    o, c, h, l, v = (df[k].values for k in ('Open', 'Close', 'High', 'Low', 'Volume'))
    n = len(df)
    obs = []
    for i in range(max(10, n - lookback), n - 4):
        base_v = v[max(0, i - 10):i].mean()
        vr = v[i + 1:i + 4].mean() / base_v if base_v > 0 else 1.0
        if o[i] > c[i]:                       # candle turun -> calon demand
            top = c[i + 1:i + 4].max()
            imp = (top / c[i] - 1) * 100
            if imp >= min_impulse_pct and top > h[i] and (vr >= 1.2 or imp >= 6) and c[i + 1:].min() >= l[i]:
                obs.append({"type": "bullish", "high": float(h[i]), "low": float(l[i]), "idx": i, "date": str(df.index[i])[:10],
                            "impulse": float(imp), "strength": min(10, int(abs(imp) + vr)), "retested": bool(l[i + 4:].min() <= h[i])})
        elif o[i] < c[i]:                     # candle naik -> calon supply
            bot = c[i + 1:i + 4].min()
            imp = (bot / c[i] - 1) * 100
            if imp <= -min_impulse_pct and bot < l[i] and (vr >= 1.2 or imp <= -6) and c[i + 1:].max() <= h[i]:
                obs.append({"type": "bearish", "high": float(h[i]), "low": float(l[i]), "idx": i, "date": str(df.index[i])[:10],
                            "impulse": float(imp), "strength": min(10, int(abs(imp) + vr)), "retested": bool(h[i + 4:].max() >= l[i])})
    return sorted(obs, key=lambda x: (x['strength'], x['idx']), reverse=True)[:6]


def find_fvgs(df, lookback=60):
    o, c, h, l = (df[k].values for k in ('Open', 'Close', 'High', 'Low'))
    n = len(df)
    out = []
    for i in range(max(2, n - lookback), n):
        if l[i] > h[i - 2] and c[i - 1] > o[i - 1]:
            gap = (l[i] / h[i - 2] - 1) * 100
            if 0.2 <= gap <= 8 and not (i + 1 < n and c[i + 1:].min() < h[i - 2]):
                out.append({"type": "bullish", "low": float(h[i - 2]), "high": float(l[i]), "gap": float(gap), "idx": i,
                            "date": str(df.index[i])[:10], "strength": min(10, int(gap * 2 + 2))})
        if h[i] < l[i - 2] and c[i - 1] < o[i - 1]:
            gap = (l[i - 2] / h[i] - 1) * 100
            if 0.2 <= gap <= 8 and not (i + 1 < n and c[i + 1:].max() > l[i - 2]):
                out.append({"type": "bearish", "low": float(h[i]), "high": float(l[i - 2]), "gap": float(gap), "idx": i,
                            "date": str(df.index[i])[:10], "strength": min(10, int(gap * 2 + 2))})
    return sorted(out, key=lambda x: (x['strength'], x['idx']), reverse=True)[:6]


def zone_distance(price, low, high, ztype):
    """Jarak % harga ke zona. 0 jika di dalam zona. 999 jika sisi salah (zona sudah tertembus)."""
    if low <= price <= high:
        return 0.0
    if ztype == "bullish":
        return (price - high) / price * 100 if price > high else 999.0
    return (low - price) / price * 100 if price < low else 999.0


def _scan_zone(finder, mode, thr, n, top_n):
    def chk(sym):
        df = load_daily(sym, 80)
        if df is None:
            return None
        last = float(df['Close'].iloc[-1])
        best = None
        for z in finder(df):
            if mode != "both" and z['type'] != mode:
                continue
            d = zone_distance(last, z['low'], z['high'], z['type'])
            if d <= thr and (best is None or d < best[0]):
                best = (d, z)
        if best is None:
            return None
        return base_row(sym, df, zone=best[1], dist=best[0])
    res = run_parallel(get_top_liquid_tickers(n), chk)
    res.sort(key=lambda x: (x['zone']['strength'], -x['dist']), reverse=True)
    return res[:top_n]


def scan_order_block(mode="bullish", thr=5.0, n=400, top_n=30):
    return _scan_zone(find_order_blocks, mode, thr, n, top_n)


def scan_fvg(mode="bullish", thr=5.0, n=400, top_n=30):
    return _scan_zone(find_fvgs, mode, thr, n, top_n)


# ==================== CONFLUENCE / SAFETY / POWER ====================
def calc_confluence(df, a):
    """Skor 0-15 tanpa double counting: Tren 3 + Lokasi 3 + Trigger 4 + Volume 2 + Flow 3."""
    c = df['Close']
    last = float(c.iloc[-1])
    e20, e50, e200 = last_ema(c, 20), last_ema(c, 50), last_ema(c, 200)
    has200 = not np.isnan(e200)
    tr, trig_list = [], []
    trend = 0
    if has200 and last > e200:
        trend += 1; tr.append("Di atas EMA200")
    if has200 and e50 > e200:
        trend += 1; tr.append("EMA50>EMA200")
    if last > e20:
        trend += 1; tr.append("Di atas EMA20")
    loc = 0
    d50 = (last / e50 - 1) * 100
    if abs(d50) <= 3:
        loc += 1; tr.append(f"Dekat EMA50 {d50:+.1f}%")
    if has200 and abs((last / e200 - 1) * 100) <= 5:
        loc += 1; tr.append("Dekat EMA200")
    if has200 and abs(d50) <= 4 and abs((e50 / e200 - 1) * 100) <= 5:
        loc += 1; tr.append("Golden area")
    pats = detect_patterns(df)
    trig = 0
    for k in ('breakout', 'double_bottom', 'bull_engulfing', 'pullback_ema50', 'pullback_ema200', 'hammer', 'bull_flag'):
        if k in pats:
            trig += 2 if k in ('breakout', 'double_bottom', 'bull_engulfing', 'pullback_ema50') else 1
            trig_list.append(k)
    trig = min(trig, 4)
    vr = vol_ratio(df)
    volp = 2 if vr >= 2 else 1 if vr >= 1.5 else 0
    flow = 0
    if a and a.get('is_akum'):
        flow += 2 if strong_flow(a) else 1
        tr.append(f"Akum {a['source']} {fmt_rp(a['net_value'])}")
    if a and a.get('cmf20', 0) > 0.05:
        flow += 1
    flow = min(flow, 3)
    total = trend + loc + trig + volp + flow
    return total, tr + trig_list + ([f"Vol {vr:.1f}x"] if volp else [])


def calc_safety(df, a):
    c = df['Close']
    last = float(c.iloc[-1])
    e200 = last_ema(c, 200)
    rsi = calculate_rsi(c)
    s, reasons = 0, []
    if not np.isnan(e200):
        d = (last / e200 - 1) * 100
        s += 2 if -10 <= d <= 15 else 1 if -15 <= d <= 20 else 0
        reasons.append(f"EMA200 {d:+.0f}%")
    s += 2 if 35 <= rsi <= 70 else 1 if 30 <= rsi <= 75 else 0
    reasons.append(f"RSI {rsi:.0f}")
    if a:
        s += 2 if a['is_akum'] else 1 if a['buy_pct'] >= 48 else 0
        reasons.append(f"Flow {a['source']}")
    hi20, lo20 = float(df['High'].tail(20).max()), float(df['Low'].tail(20).min())
    rng = (hi20 - lo20) / ((hi20 + lo20) / 2) * 100
    s += 2 if rng <= 15 else 1 if rng <= 25 else 0
    reasons.append(f"Range20 {rng:.0f}%")
    dl = (last - lo20) / lo20 * 100 if lo20 else 0
    s += 2 if 0 <= dl <= 20 else 1 if dl <= 35 else 0
    return min(s, 10), reasons


def calc_power(df, a):
    c = df['Close']
    last = float(c.iloc[-1])
    prev = float(c.iloc[-2])
    chg = (last / prev - 1) * 100 if prev else 0
    vr = vol_ratio(df)
    rsi = calculate_rsi(c)
    rsi_prev = calculate_rsi(c.iloc[:-1])
    s, reasons = 0, []
    s += 3 if vr >= 2.5 else 2 if vr >= 1.8 else 1 if vr >= 1.3 else 0
    reasons.append(f"Vol {vr:.1f}x")
    s += 2 if chg >= 3 else 1 if chg >= 1 else 0
    reasons.append(f"Chg {chg:+.1f}%")
    s += 2 if (55 <= rsi <= 75 and rsi >= rsi_prev) else 1 if rsi >= 50 else 0
    s += 1 if last > last_ema(c, 20) else 0
    if a and a['is_akum']:
        s += 3 if strong_flow(a) else 2
        reasons.append(f"Akum {fmt_rp(a['net_value'])}")
    return min(s, 10), reasons


def scan_confluence(min_score=6, n=400, top_n=30, mode="all"):
    def chk(sym):
        df = load_daily(sym, 100)
        if df is None:
            return None
        a = get_bandar_analysis(sym, "daily", df)
        score, trig = calc_confluence(df, a)
        if score < min_score or (mode == "bullish" and score < 7) or (mode == "safe" and score < 8 and not any('Golden' in t or 'pullback' in t for t in trig)):
            return None
        return base_row(sym, df, score=score, triggers=trig)
    res = run_parallel(get_top_liquid_tickers(n), chk)
    res.sort(key=lambda x: x['score'], reverse=True)
    return res[:top_n]


def scan_safety_power(min_safety=6, min_power=5, mode="both", n=400, top_n=30):
    def chk(sym):
        df = load_daily(sym, 100)
        if df is None:
            return None
        a = get_bandar_analysis(sym, "daily", df)
        sf, sr = calc_safety(df, a)
        pw, pr = calc_power(df, a)
        if mode == "safe" and sf < min_safety:
            return None
        if mode == "power" and pw < min_power:
            return None
        if mode == "both" and (sf < min_safety or pw < min_power):
            return None
        if mode == "safe_power" and not (sf >= 7 and pw >= 6):
            return None
        return base_row(sym, df, safety=sf, power=pw, total=sf + pw, sr=sr, pr=pr)
    res = run_parallel(get_top_liquid_tickers(n), chk)
    key = 'safety' if mode == "safe" else 'power' if mode == "power" else 'total'
    res.sort(key=lambda x: x[key], reverse=True)
    return res[:top_n]


# ==================== TELEGRAM ====================
def _tg(method, payload=None, files=None, timeout=20):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
        r = requests.post(url, json=payload, timeout=timeout) if files is None else requests.post(url, data=payload, files=files, timeout=40)
        return r.json()
    except Exception as e:
        clog(f"tg {method} err {e}")
        return {"ok": False, "description": str(e)}


def send_reply(cid, txt, rm=None):
    if not TELEGRAM_BOT_TOKEN or not cid:
        return False
    try:
        cid = int(cid)
    except Exception:
        pass
    chunks = [txt[i:i + 4000] for i in range(0, len(txt), 4000)] or [""]
    ok = True
    for i, ch in enumerate(chunks):
        pl = {"chat_id": cid, "text": ch, "parse_mode": "HTML", "disable_web_page_preview": True}
        if rm and i == len(chunks) - 1:
            pl["reply_markup"] = rm
        j = _tg("sendMessage", pl)
        if not j.get("ok"):
            pl.pop("parse_mode")
            pl["text"] = _html.unescape(re.sub(r"<[^>]+>", "", ch))
            j = _tg("sendMessage", pl)
        ok = ok and j.get("ok", False)
    return ok


def send_photo_reply(cid, path, caption=""):
    try:
        cid = int(cid)
    except Exception:
        pass
    with open(path, 'rb') as ph:
        j = _tg("sendPhoto", {"chat_id": cid, "caption": caption[:1000]}, files={"photo": ph})
    if not j.get("ok"):
        clog(f"sendPhoto gagal: {j.get('description')}")
    return j.get("ok", False)


def broadcast_list(cid, header, items, line_fn, btn_fn, empty_msg, max_chars=3500, max_btn=60):
    if not items:
        send_reply(cid, esc(empty_msg))
        return
    msg, kb = header, []
    for i, it in enumerate(items, 1):
        line = line_fn(i, it) + "\n"
        if kb and (len(msg) + len(line) > max_chars or len(kb) >= max_btn):
            send_reply(cid, msg, rm={"inline_keyboard": kb})
            msg, kb = "", []
        msg += line
        kb.append([{"text": btn_fn(it)[:60], "callback_data": f"chart_{it['symbol']}"}])
    if kb:
        send_reply(cid, msg, rm={"inline_keyboard": kb})


def hdr(title, count, extra="", n=400):
    return f"<b>{esc(title)}</b> {now_wib().strftime('%d %b %Y %H:%M WIB')}\n{esc(universe_label(n))} | {count} saham{(' | ' + esc(extra)) if extra else ''}\n\n"


PROXY_NOTE = "⚠ Sumber PROXY = estimasi dari OHLCV, bukan broker summary."


def bc_volspike(cid, items, thr):
    broadcast_list(cid, hdr(f"VOL SPIKE >{thr}x", len(items), "vol dibanding rata-rata 20 hari SEBELUMNYA"), items,
                   lambda i, it: f"{i}. <b>{it['symbol']}</b> {it['close']} ({it['chg']:+.1f}%) Vol {it['vr']:.1f}x Val {fmt_rp(it['value'], False)}",
                   lambda it: f"{it['symbol']} {it['vr']:.1f}x", f"Vol spike >{thr}x: tidak ada")


def bc_kim(cid, items, auto=False):
    def line(i, it):
        tag = "KIM+BSJP" if it['kim_buy'] and it['bsjp_buy'] else "KIM" if it['kim_buy'] else "BSJP"
        extra = f" SL {int(it['kim']['sl'])} TP1 {int(it['kim']['tp1'])}" if it['kim_buy'] else ""
        return f"{i}. <b>{it['symbol']}</b> {it['close']} ({it['chg']:+.1f}%) {tag}{extra}"
    broadcast_list(cid, hdr("AUTO KIM+BSJP" if auto else "KIM+BSJP BUY", len(items)), items, line,
                   lambda it: f"{it['symbol']} {'KIM' if it['kim_buy'] else ''}{'+' if it['kim_buy'] and it['bsjp_buy'] else ''}{'BSJP' if it['bsjp_buy'] else ''}",
                   "KIM+BSJP: tidak ada sinyal BUY")


def _flow_note(items):
    nb = sum(1 for it in items if it.get('src') == 'BROKER')
    if items and nb == len(items):
        return f"Data broker s.d. {items[0]['a'].get('range_end') or '?'}"
    return (f"{nb} broker / {len(items) - nb} proxy. " if nb else "") + PROXY_NOTE


def _bandar_n():
    return BROKER_SCAN_N if broker_enabled() else 400


def bc_bandar(cid, items, kind, period):
    ttl = f"TOP {'AKUM' if kind == 'akum' else 'DISTRIBUSI'} {period.upper()}"

    def line(i, it):
        base = f"{i}. <b>{it['symbol']}</b> {it['close']} ({it['chg']:+.1f}%) Net {fmt_rp(it['net'])}"
        if it['src'] == 'BROKER':
            a, k = it['a'], it.get('kind', kind)
            who = a['top_buyers'] if k == 'akum' else a['top_sellers']
            avg = a.get('avg_buy', 0)
            avg_txt = f" AvgB {avg:.0f} ({(it['close'] / avg - 1) * 100:+.0f}%)" if avg > 0 else ""
            return base + f" ({a['net_pct']:+.1f}% total) Akum {a['acc_ratio'] * 100:.0f}%{avg_txt} {esc(','.join(b['broker'] for b in who))}"
        return base + f" Flow/ADV {it['intensity'] * 100:+.0f}% Buy% {it['buy_pct']:.0f} [PROXY]"
    broadcast_list(cid, hdr(ttl, len(items), _flow_note(items) if items else "", _bandar_n()), items, line,
                   lambda it: f"{it['symbol']} {fmt_rp(it['net'])}", f"{ttl}: tidak ada yang lolos")


def bc_whale(cid, items, period):
    broadcast_list(cid, hdr(f"WHALE ACCUM {period.upper()}", len(items), _flow_note(items) if items else "", _bandar_n()), items,
                   lambda i, it: f"{i}. <b>{it['symbol']}</b> {it['close']} ({it['chg']:+.1f}%) Skor {it['score']}/9 Net {fmt_rp(it['net'])} Vol {it['vr']:.1f}x [{it['src']}]",
                   lambda it: f"{it['symbol']} 🐋 {fmt_rp(it['net'])}", "Whale: tidak ada yang lolos")


def bc_insider(cid, items, period):
    broadcast_list(cid, hdr(f"INSIDER/STEALTH ACCUM {period.upper()}", len(items), _flow_note(items) if items else "", _bandar_n()), items,
                   lambda i, it: f"{i}. <b>{it['symbol']}</b> {it['close']} ({it['chg']:+.1f}%) Skor {it['score']}/6 Posisi range {it['pos']:.0f}% Net {fmt_rp(it['net'])} [{it['src']}]",
                   lambda it: f"{it['symbol']} 🕵 {it['pos']:.0f}%", "Insider: tidak ada yang lolos")


def bc_ema(cid, items, period, mode):
    broadcast_list(cid, hdr(f"EMA{period} {mode.upper()}", len(items)), items,
                   lambda i, it: f"{i}. <b>{it['symbol']}</b> {it['close']} EMA{period}:{it['ema']} ({it['dist']:+.1f}%) Chg {it['chg']:+.1f}% RSI {it['rsi']:.0f} Vol {it['vr']:.1f}x",
                   lambda it: f"{it['symbol']} {it['dist']:+.1f}%", f"EMA{period} {mode}: tidak ada (saham dengan histori kurang dari {period + 60} hari dilewati)")


def bc_golden(cid, items):
    broadcast_list(cid, hdr("GOLDEN AREA EMA50+200", len(items)), items,
                   lambda i, it: f"{i}. <b>{it['symbol']}</b> {it['close']} EMA50:{it['ema50']} ({it['d50']:+.1f}%) EMA200:{it['ema200']} ({it['d200']:+.1f}%) Gap {it['gap']:+.1f}%",
                   lambda it: f"{it['symbol']} gap {it['gap']:+.1f}%", "Golden area: tidak ada")


def bc_pattern(cid, items, name):
    broadcast_list(cid, hdr(f"PATTERN {name.upper()}", len(items)), items,
                   lambda i, it: f"{i}. <b>{it['symbol']}</b> {it['close']} ({it['chg']:+.1f}%) {esc(it['pattern'])} [{it['score']}] {esc(it['desc'])}",
                   lambda it: f"{it['symbol']} {it['pattern']}", f"Pattern {name}: tidak ada")


def bc_confluence(cid, items, min_score):
    broadcast_list(cid, hdr(f"CONFLUENCE >= {min_score}", len(items), "Tren3+Lokasi3+Trigger4+Volume2+Flow3"), items,
                   lambda i, it: f"{i}. <b>{it['symbol']}</b> {it['close']} ({it['chg']:+.1f}%) Skor {it['score']}/15 - {esc(', '.join(it['triggers'][:5]))}",
                   lambda it: f"{it['symbol']} {it['score']}/15", f"Confluence >= {min_score}: tidak ada")


def bc_safepower(cid, items, mode):
    broadcast_list(cid, hdr(f"SAFETY/POWER ({mode})", len(items)), items,
                   lambda i, it: f"{i}. <b>{it['symbol']}</b> {it['close']} ({it['chg']:+.1f}%) S:{it['safety']}/10 P:{it['power']}/10 | {esc(', '.join(it['sr'][:2]))} | {esc(', '.join(it['pr'][:2]))}",
                   lambda it: f"{it['symbol']} S{it['safety']} P{it['power']}", "Safety/Power: tidak ada")


def bc_zone(cid, items, label, mode):
    def line(i, it):
        z = it['zone']
        return f"{i}. <b>{it['symbol']}</b> {it['close']} ({it['chg']:+.1f}%) {label}:{int(z['low'])}-{int(z['high'])} Jarak {it['dist']:.1f}% Kuat {z['strength']} {z['date']}"
    broadcast_list(cid, hdr(f"{label} {mode.upper()}", len(items), "zona yang belum tertembus"), items, line,
                   lambda it: f"{it['symbol']} {label} {it['dist']:.1f}%", f"{label} {mode}: tidak ada")


# ==================== CHART ====================
def generate_pro_chart(df, symbol, timeframe="1d", out="chart.png", tf_label=None, show_bars=120):
    try:
        tf = normalize_timeframe(timeframe)
        tf_label = tf_label or format_timeframe_label(tf)
        d = df.dropna(subset=['Open', 'High', 'Low', 'Close']).copy()
        if len(d) < 30:
            return None
        # indikator dihitung di SELURUH histori, baru dipotong untuk tampilan
        for k in (8, 20, 50, 200):
            d[f'EMA{k}'] = ema(d['Close'], k)
        d['V20'] = d['Volume'].rolling(20, min_periods=1).mean()
        sma, bbu, bbl = calculate_bollinger_bands(d, 20, 2)
        d['BBU'], d['BBL'] = bbu, bbl
        clv = clv_series(d)
        d['BuyV'] = d['Volume'] * (0.5 + 0.5 * clv)
        d['SellV'] = d['Volume'] - d['BuyV']
        tp = (d['High'] + d['Low'] + d['Close']) / 3
        d['Flow'] = clv * d['Volume'] * tp
        d['Dev50'] = (d['Close'] / d['EMA50'] - 1) * 100
        kim, bsjp = detect_kim_signal(d, tf), detect_bsjp_signal(d, tf)
        rsi = calculate_rsi(d['Close'])
        _, _, adx = calc_dmi_adx(d)

        nb = min(show_bars, len(d))
        off = len(d) - nb
        p = d.iloc[-nb:]
        x = np.arange(nb)
        last, prev = float(p['Close'].iloc[-1]), float(p['Close'].iloc[-2])
        chg = (last / prev - 1) * 100 if prev else 0

        fig = Figure(figsize=(14, 8), dpi=150, facecolor='#000000')
        gs = fig.add_gridspec(4, 1, height_ratios=[4.8, 1.1, 0.9, 0.8], hspace=0.05)
        ax = fig.add_subplot(gs[0])
        axv = fig.add_subplot(gs[1], sharex=ax)
        axf = fig.add_subplot(gs[2], sharex=ax)
        axd = fig.add_subplot(gs[3], sharex=ax)
        fig.subplots_adjust(left=0.06, right=0.94, top=0.90, bottom=0.05)
        for a_ in (ax, axv, axf, axd):
            a_.set_facecolor('#000000')
            a_.tick_params(colors='#aaaaaa', labelsize=8)
            a_.yaxis.tick_right()
            for sp in a_.spines.values():
                sp.set_color('#333333')

        up = (p['Close'] >= p['Open']).to_numpy()
        ax.vlines(x, p['Low'].to_numpy(), p['High'].to_numpy(), colors=np.where(up, '#00ff00', '#ff4444'), linewidth=0.7)
        min_h = float(p['Close'].mean()) * 0.0004
        for xi, o_, c_, u_ in zip(x, p['Open'].to_numpy(), p['Close'].to_numpy(), up):
            ax.add_patch(Rectangle((xi - 0.35, min(o_, c_)), 0.7, max(abs(c_ - o_), min_h),
                                   facecolor='none' if u_ else '#ff3333', edgecolor='#00ff00' if u_ else '#ff3333', linewidth=0.7))
        ax.plot(x, p['EMA8'], color='#ffeb3b', linewidth=1.0)
        ax.plot(x, p['EMA20'], color='#00e6ff', linewidth=1.0)
        ax.plot(x, p['EMA50'], color='#ffffff', linewidth=0.9, alpha=0.8)
        ax.plot(x, p['EMA200'], color='#a020f0', linewidth=1.4)
        ax.plot(x, p['BBU'], color='#444488', linewidth=0.8, linestyle='--', alpha=0.5)
        ax.plot(x, p['BBL'], color='#444488', linewidth=0.8, linestyle='--', alpha=0.5)
        if kim.get('b_modal_series') is not None:
            ax.plot(x, kim['b_modal_series'].iloc[-nb:].to_numpy(), color='#ffffff', linewidth=1.1, alpha=0.6)
        if kim.get('buy'):
            ax.axhline(kim['tp1'], color='#00ff00', linewidth=1, linestyle='--', alpha=0.5)
            ax.axhline(kim['sl'], color='#ff5500', linewidth=1.2, alpha=0.7)
        if bsjp.get('buy'):
            ax.axhline(bsjp['tp1'], color='#00ff00', linewidth=0.8, linestyle=':', alpha=0.5)
            ax.axhline(bsjp['sl'], color='#ff0000', linewidth=1.2, alpha=0.7)
        for m in kim.get('markers', []):
            if m['idx'] >= off:
                ax.plot(m['idx'] - off, float(p['Low'].iloc[m['idx'] - off]) * 0.97, marker='^', color='#00ff00', markersize=9)
        for m in bsjp.get('markers', []):
            if m['idx'] >= off:
                ax.plot(m['idx'] - off, float(p['Low'].iloc[m['idx'] - off]) * 0.96, marker='*', color='#0080ff', markersize=11)
        ax.set_xlim(-1, nb + 9)
        ax.set_ylim(float(p['Low'].min()) * 0.96, float(p['High'].max()) * 1.06)

        e = lambda k: float(d[f'EMA{k}'].iloc[-1]) if pd.notna(d[f'EMA{k}'].iloc[-1]) else float('nan')
        vr1 = float(d['Volume'].iloc[-1] / d['Volume'].iloc[-2]) if d['Volume'].iloc[-2] > 0 else 0
        txt = (f"SMA20 : {float(d['Close'].tail(20).mean()):.1f}\nVol/Prev : {vr1:.1f}x\nVol/Avg20: {vol_ratio(d):.1f}x\n"
               f"RSI14 : {rsi:.1f}\nADX14 : {float(adx.iloc[-1]):.1f}\n\nEMA 8 : {e(8):.1f}\nEMA 20 : {e(20):.1f}\n"
               f"EMA 50 : {e(50):.1f}\nEMA 200: {e(200):.1f}")
        ax.text(0.005, 0.98, txt, transform=ax.transAxes, va='top', ha='left', fontsize=6, family='monospace', color='#e0e0e0',
                bbox=dict(facecolor='black', alpha=0.75, edgecolor='#333333'))
        fig.text(0.005, 0.96, f"{symbol} : {last:.0f} ({chg:+.2f}%)", color='#ffff00', fontsize=14, fontweight='bold', ha='left', family='monospace')
        fig.text(0.5, 0.96, "Rafano Trader", color='white', fontsize=14, fontweight='bold', ha='center')
        fig.text(0.99, 0.96, f"{tf_label} | {p.index[-1].strftime('%d %b %Y %H:%M')}", color='#ffcc00', fontsize=10, ha='right', fontweight='bold')
        ax.text(nb + 1, last, f" {last:.0f}", color='black', fontsize=8, va='center', fontweight='bold', bbox=dict(facecolor='white', edgecolor='none', boxstyle='square,pad=0.2'))

        tb, ts = float(p['BuyV'].iloc[-1]), float(p['SellV'].iloc[-1])
        axv.text(0.005, 0.88, f"Buy% est = {tb / (tb + ts) * 100 if tb + ts > 0 else 50:.0f}% (estimasi CLV, bukan data broker)", transform=axv.transAxes, color='#ffffff', fontsize=7, va='top')
        axv.bar(x, p['SellV'], color='#cc0000', width=0.8, alpha=0.8)
        axv.bar(x, p['BuyV'], bottom=p['SellV'], color='#00cc00', width=0.8, alpha=0.9)
        axv.plot(x, p['V20'], color='white', linewidth=0.8)
        axv.set_ylim(0, float(p['Volume'].max()) * 2.2)

        f5 = float(p['Flow'].tail(5).sum())
        axf.text(0.005, 0.85, f"Net flow est 5 bar: Rp {fmt_rp(f5)}", transform=axf.transAxes, color='#ffffff', fontsize=7, va='top')
        axf.bar(x, p['Flow'], color=np.where(p['Flow'] >= 0, '#00ffff', '#ff4444'), width=0.6)
        axf.axhline(0, color='#444444', linewidth=0.5)

        axd.text(0.005, 0.85, "Deviasi harga vs EMA50 (%)", transform=axd.transAxes, color='#ffffff', fontsize=7, va='top')
        axd.bar(x, p['Dev50'].fillna(0), color=np.where(p['Dev50'].fillna(0) >= 0, '#cccccc', '#888888'), width=0.5, alpha=0.8)

        step = max(1, nb // 10)
        axd.set_xticks(x[::step])
        fmt = '%H:%M' if tf in ('5m', '15m', '30m', '1h', '4h') else '%d/%b'
        axd.set_xticklabels([p.index[i].strftime(fmt) for i in range(0, nb, step)], fontsize=7)
        for a_ in (ax, axv, axf):
            for t in a_.get_xticklabels():
                t.set_visible(False)
        fig.savefig(out, dpi=150, bbox_inches='tight', facecolor='#000000')
        return out
    except Exception as e:
        clog(f"chart err {symbol} {timeframe}: {e}")
        import traceback
        traceback.print_exc()
        return None


def generate_caption(symbol, df, tf_norm="1d"):
    try:
        s = snap(df)
        a = get_bandar_analysis(symbol, "daily", df) if len(df) >= 30 and tf_norm == "1d" else None
        flow = f" | Flow {a['source']}: {'AKUM' if a['is_akum'] else 'DIST' if a['is_dist'] else 'netral'}" if a else ""
        return f"{symbol} - {s['close']} ({s['chg']:+.2f}%) | {format_timeframe_label(tf_norm)}\nRSI {s['rsi']:.1f} | Vol {s['vr']:.1f}x{flow}"
    except Exception:
        return f"{symbol}"


def process_chart_request(cid, code, tf_input="1d"):
    code = code.upper().strip()
    tf = normalize_timeframe(tf_input)
    label = format_timeframe_label(tf)
    try:
        send_reply(cid, f"⏳ {esc(code)} ({label}) menyiapkan chart...")
        HISTORY.pop_matching(code)
        unsuspend(code)
        df = get_history(code, tf)
        if (df is None or len(df) < 30) and tf == "1d":
            df5 = get_history(code, "5m")
            if df5 is not None and len(df5) >= 30:
                send_reply(cid, f"Data daily {esc(code)} gagal, memakai chart 5m")
                df, tf, label = df5, "5m", format_timeframe_label("5m")
        if df is None or len(df) < 30:
            send_reply(cid, f"❌ Data {esc(code)} TF {tf} kosong/kurang. Coba /c {esc(code)} 5")
            return
        rt = get_realtime_price(code) if market_open() else 0.0
        d = apply_realtime(df, rt)
        out = f"/tmp/chart_{code}_{tf}_{int(time.time() * 1000)}.png"
        with CHART_LOCK:
            fp = generate_pro_chart(d, code, tf, out, label)
        if not fp or not os.path.exists(fp):
            send_reply(cid, f"❌ Gagal render chart {esc(code)} {tf}. Cek log.")
            return
        send_photo_reply(cid, fp, generate_caption(code, d, tf))
        try:
            os.remove(fp)
        except Exception:
            pass
    except Exception as e:
        clog(f"CHART ERR {code}: {e}")
        send_reply(cid, f"❌ Error chart {esc(code)}: {esc(e)}")


# ==================== COMMAND HANDLER ====================
HELP = """<b>RAFANO V5.0</b>
Universe: top 400 by value (jika screener Arjum aktif), tanpa FCA/suspend, harga >= 50

<b>Chart:</b> /c KODE [5|15|60|1d]  (klik tombol hasil scan juga membuat chart)
<b>Scanner:</b> /scanvol 1.5 | /kimbsjp | /list400
<b>Bandar (broker summary Arjum bila dikonfigurasi, jika tidak PROXY OHLCV):</b>
/topakum [daily|weekly] [N] | /topdist | /akumdist | /bandar KODE [weekly] | /brokerprobe [KODE]
/whale [daily|weekly] | /insider | /whaleinsider
<b>EMA:</b> /ema50 [persen] [near|above|below|bounce|pullback] | /ema200 | /golden
<b>Pattern:</b> /pattern [all|breakout|db|pb50|pb200|hammer|flag|engulf|squeeze] | /breakout /doublebottom /hammer /flag /squeeze
<b>Skor:</b> /confluence [min] [bullish|safe] | /safety [S] [P] [both|safe|power|safe_power] | /power | /safepower
<b>SMC:</b> /ob [bullish|bearish|both] [persen] | /fvg [bullish|bearish|both] [persen]
<b>Mix:</b> /supermix | /smartmix
<b>Lain:</b> /auto on|off | /status | /id"""

PATTERN_ALIAS = {"db": "double_bottom", "doublebottom": "double_bottom", "double_bottom": "double_bottom", "break": "breakout", "bo": "breakout",
                 "pb50": "pullback_ema50", "pb200": "pullback_ema200", "engulf": "bull_engulfing", "flag": "bull_flag", "bullflag": "bull_flag"}


def parse_args(parts):
    nums = [float(p) for p in parts[1:] if re.fullmatch(r"\d+(\.\d+)?", p)]
    words = [p.lower() for p in parts[1:] if not re.fullmatch(r"\d+(\.\d+)?", p)]
    return nums, words


def get_period(words):
    for w in words:
        if w in ("weekly", "w", "1w"):
            return "weekly"
        if w in ("daily", "d", "1d"):
            return "daily"
    return "daily"


def spawn_scan(cid, title, fn):
    def job():
        if not SCAN_SEM.acquire(blocking=False):
            send_reply(cid, "⏳ Ada scan lain berjalan, permintaanmu antri...")
            SCAN_SEM.acquire()
        t0 = time.time()
        try:
            send_reply(cid, f"{esc(title)} ...")
            fn(cid)
            clog(f"scan selesai {title} {time.time() - t0:.0f}s")
        except Exception as e:
            clog(f"SCAN ERR {title}: {e}")
            import traceback
            traceback.print_exc()
            send_reply(cid, f"❌ Scan gagal: {esc(e)}")
        finally:
            SCAN_SEM.release()
    threading.Thread(target=job, daemon=True).start()


def status_text():
    if broker_enabled():
        wk = {None: "belum diuji", True: "OK", False: "tidak tersedia (proxy)"}[BROKER_STATE["weekly_ok"]]
        dm = BROKER_STATE["date_max"] or "?"
        broker_line = f"AKTIF {esc(ARJUM_BROKER_PATH)} | scan top {BROKER_SCAN_N} | weekly: {wk} | data terakhir: {dm}"
    else:
        broker_line = "NONAKTIF (path kosong/quota/gagal) -> PROXY OHLCV"
    quota = "HABIS" if QUOTA["hit"] else "OK"
    key_ok = "ada" if ARJUM_API_KEY else "KOSONG"
    pasar = "ya" if market_open() else "tidak"
    auto = "ON" if AUTO_ENABLED else "OFF"
    return (f"<b>STATUS</b>\nQuota Arjum: {quota} | API key: {key_ok}\n"
            f"Universe: {esc(universe_label())}\nBroker summary: {broker_line}\n"
            f"Cache history: {len(HISTORY)} | bandar: {len(BANDAR)} | suspend: {len(_SUSPEND)}\n"
            f"Pasar buka: {pasar} | Auto: {auto}\nWorkers scan: {SCAN_WORKERS}")


def handle_message(msg):
    global AUTO_ENABLED
    cid = msg["chat"]["id"]
    txt = (msg.get("text") or "").strip()
    if not txt.startswith("/"):
        return
    parts = txt.split()
    cmd = parts[0].lower().split("@")[0]
    if cmd == "/id":
        send_reply(cid, f"Chat ID: <code>{cid}</code>")
        return
    if ALLOWED and str(cid) not in ALLOWED:
        send_reply(cid, "⛔ Chat ini tidak diizinkan.")
        return
    nums, words = parse_args(parts)

    if cmd in ("/start", "/help", "/menu"):
        send_reply(cid, HELP)
    elif cmd in ("/status", "/quota"):
        send_reply(cid, status_text())
    elif cmd in ("/c", "/chart"):
        if len(parts) >= 2:
            CHART_POOL.submit(process_chart_request, cid, parts[1], parts[2] if len(parts) >= 3 else "1d")
        else:
            send_reply(cid, "Pakai: /c BBCA atau /c ERAA 5")
    elif cmd in ("/scanvol", "/vol", "/vol400"):
        thr = nums[0] if nums else 1.5
        spawn_scan(cid, f"🔥 Vol spike >{thr}x", lambda c: bc_volspike(c, scan_volume_spike(thr), thr))
    elif cmd in ("/kim", "/kimbsjp", "/scan400", "/kbs"):
        spawn_scan(cid, "🚀 KIM+BSJP", lambda c: bc_kim(c, scan_kim_bsjp()))
    elif cmd in ("/list400", "/400", "/list"):
        def lst(c):
            t = get_top_liquid_tickers(400)
            send_reply(c, f"<b>{esc(universe_label())}</b> total {len(t)}\n\n" + esc(", ".join(t[:150])))
            if len(t) > 150:
                send_reply(c, esc(", ".join(t[150:300])))
            if len(t) > 300:
                send_reply(c, esc(", ".join(t[300:])))
        spawn_scan(cid, "📋 List universe", lst)
    elif cmd in ("/topakum", "/akum", "/top_akum", "/akumulasi"):
        per, tn = get_period(words), int(nums[0]) if nums else 30
        spawn_scan(cid, f"🟢 Top akum {per}", lambda c: bc_bandar(c, scan_bandar("akum", per, 400, tn), "akum", per))
    elif cmd in ("/topdist", "/dist", "/top_dist", "/distribusi", "/topdistribusi"):
        per, tn = get_period(words), int(nums[0]) if nums else 30
        spawn_scan(cid, f"🔴 Top distribusi {per}", lambda c: bc_bandar(c, scan_bandar("dist", per, 400, tn), "dist", per))
    elif cmd in ("/bandar", "/bandarmology") and words and len(words[0]) == 4 and words[0].isalpha() and words[0] not in ("daily", "weekly"):
        sym, per = words[0].upper(), get_period(words[1:])

        def detail(c):
            df = get_history(sym, "1d")
            send_reply(c, broker_detail_text(sym, per, df))
            CHART_POOL.submit(process_chart_request, c, sym, "1d")
        spawn_scan(cid, f"🔍 Detail bandar {sym}", detail)
    elif cmd in ("/brokerprobe", "/probe"):
        sym = words[0].upper() if words and len(words[0]) == 4 else "BBCA"
        spawn_scan(cid, f"🔎 Probe broker {sym}", lambda c: probe_broker(c, sym))
    elif cmd in ("/akumdist", "/bandar", "/topbandar", "/bandarmology"):
        per = get_period(words)

        def both(c):
            ak, di = scan_bandar_both(per)
            bc_bandar(c, ak, "akum", per)
            bc_bandar(c, di, "dist", per)
        spawn_scan(cid, f"🔍 Akum+Distribusi {per}", both)
    elif cmd in ("/whale", "/whaleaccum", "/topwhale", "/institusional", "/akumwhale"):
        per, tn = get_period(words), int(nums[0]) if nums else 30
        spawn_scan(cid, f"🐋 Whale {per}", lambda c: bc_whale(c, scan_whale(per, 400, tn), per))
    elif cmd in ("/insider", "/insideraccum", "/topinsider", "/stealth"):
        per, tn = get_period(words), int(nums[0]) if nums else 30
        spawn_scan(cid, f"🕵️ Insider {per}", lambda c: bc_insider(c, scan_insider(per, 400, tn), per))
    elif cmd in ("/whaleinsider", "/insiderwhale"):
        per = get_period(words)

        def wi(c):
            bc_whale(c, scan_whale(per, 400, 20), per)
            bc_insider(c, scan_insider(per, 400, 20), per)
        spawn_scan(cid, f"🐋🕵️ Whale+Insider {per}", wi)
    elif cmd in ("/ema50", "/near50", "/pullback50", "/bounce50", "/ema200", "/near200"):
        period = 200 if "200" in cmd else 50
        thr = nums[0] if nums else (5.0 if period == 200 else 3.0)
        tn = int(nums[1]) if len(nums) > 1 else 30
        mode = next((w for w in words if w in ("near", "above", "below", "bounce", "pullback")), "bounce" if cmd == "/bounce50" else "pullback" if cmd == "/pullback50" else "near")
        spawn_scan(cid, f"📊 EMA{period} {mode} {thr}%", lambda c: bc_ema(c, scan_near_ema(period, thr, mode, 400, tn), period, mode))
    elif cmd in ("/ema", "/near", "/golden", "/goldenarea", "/emagolden"):
        thr = nums[0] if nums else 4.0
        spawn_scan(cid, f"✨ Golden area {thr}%", lambda c: bc_golden(c, scan_golden_area(thr)))
    elif cmd in ("/pattern", "/chartpattern", "/cp", "/breakout", "/doublebottom", "/hammer", "/flag", "/squeeze"):
        shortcut = {"/breakout": "breakout", "/doublebottom": "double_bottom", "/hammer": "hammer", "/flag": "bull_flag", "/squeeze": "squeeze"}
        name = shortcut.get(cmd) or (words[0] if words else "all")
        name = PATTERN_ALIAS.get(name, name)
        tn = int(nums[0]) if nums else 30
        spawn_scan(cid, f"📈 Pattern {name}", lambda c: bc_pattern(c, scan_chart_patterns(name, 400, tn), name))
    elif cmd in ("/confluence", "/conf", "/smart", "/con", "/score"):
        ms = int(nums[0]) if nums else 6
        mode = words[0] if words else "all"
        tn = int(nums[1]) if len(nums) > 1 else 30
        spawn_scan(cid, f"🧠 Confluence >= {ms}", lambda c: bc_confluence(c, scan_confluence(ms, 400, tn, mode), ms))
    elif cmd in ("/safety", "/safe", "/power", "/safepower", "/safe_power", "/sp"):
        mode = next((w for w in words if w in ("both", "safe", "power", "safe_power")), "power" if cmd == "/power" else "safe_power" if cmd in ("/safepower", "/safe_power") else "both")
        ms = int(nums[0]) if nums else 6
        mp = int(nums[1]) if len(nums) > 1 else 5
        tn = int(nums[2]) if len(nums) > 2 else 30
        spawn_scan(cid, f"🛡️ Safety {ms} Power {mp} ({mode})", lambda c: bc_safepower(c, scan_safety_power(ms, mp, mode, 400, tn), mode))
    elif cmd in ("/ob", "/orderblock", "/block", "/fvg", "/gap", "/fairvalue"):
        is_ob = cmd in ("/ob", "/orderblock", "/block")
        mode = next((w for w in words if w in ("bullish", "bearish", "both", "bull", "bear")), "bullish")
        mode = {"bull": "bullish", "bear": "bearish"}.get(mode, mode)
        thr = nums[0] if nums else 5.0
        tn = int(nums[1]) if len(nums) > 1 else 30
        if is_ob:
            spawn_scan(cid, f"📦 Order block {mode} {thr}%", lambda c: bc_zone(c, scan_order_block(mode, thr, 400, tn), "OB", mode))
        else:
            spawn_scan(cid, f"📉 FVG {mode} {thr}%", lambda c: bc_zone(c, scan_fvg(mode, thr, 400, tn), "FVG", mode))
    elif cmd in ("/supermix", "/mix", "/allscan", "/screen"):
        def sm(c):
            bc_ema(c, scan_near_ema(50, 3, "bounce", 400, 15), 50, "bounce")
            bc_pattern(c, scan_chart_patterns("breakout", 400, 15), "breakout")
            bc_whale(c, scan_whale("daily", 400, 15), "daily")
        spawn_scan(cid, "🔥 Supermix", sm)
    elif cmd in ("/smartmix", "/smartfilter", "/superconfluence", "/conmix"):
        def sm2(c):
            bc_confluence(c, scan_confluence(7, 400, 15, "bullish"), 7)
            bc_safepower(c, scan_safety_power(6, 5, "safe_power", 400, 15), "safe_power")
            bc_zone(c, scan_order_block("bullish", 4.0, 400, 10), "OB", "bullish")
        spawn_scan(cid, "🔥 Smartmix", sm2)
    elif cmd == "/auto":
        if words and words[0] == "on":
            AUTO_ENABLED = True
            send_reply(cid, "✅ Auto ON (scan KIM+BSJP tiap 15 menit saat pasar buka)")
        elif words and words[0] == "off":
            AUTO_ENABLED = False
            send_reply(cid, "❌ Auto OFF")
        else:
            send_reply(cid, f"AUTO: {'ON' if AUTO_ENABLED else 'OFF'}")


def handle_callback(cb):
    qid = cb.get("id")
    data = (cb.get("data") or "").strip()
    msg = cb.get("message") or {}
    cid = (msg.get("chat") or {}).get("id") or TARGET_CHAT_ID or (cb.get("from") or {}).get("id")
    _tg("answerCallbackQuery", {"callback_query_id": qid, "text": f"Loading {data[6:]}..." if data.startswith("chart_") else "OK"}, timeout=5)
    if ALLOWED and str(cid) not in ALLOWED:
        return
    if data.startswith("chart_"):
        sym = data[6:].strip().upper()
        if sym and len(sym) <= 6 and sym not in FCA_EXCLUDE:
            clog(f"CLICK chart {sym} -> {cid}")
            CHART_POOL.submit(process_chart_request, cid, sym, "1d")


AUTO_SEEN = set()


def auto_broadcast_loop():
    while True:
        try:
            if not AUTO_ENABLED or not market_open() or not TARGET_CHAT_ID:
                time.sleep(60)
                continue
            with SCAN_SEM:
                sigs = scan_kim_bsjp()
            day = now_wib().date()
            fresh = [s for s in sigs if (day, s['symbol']) not in AUTO_SEEN]
            for s in fresh:
                AUTO_SEEN.add((day, s['symbol']))
            if fresh:
                bc_kim(TARGET_CHAT_ID, fresh, auto=True)
            time.sleep(900)
        except Exception as e:
            clog(f"auto loop err {e}")
            time.sleep(300)


def telegram_bot_listener():
    if not TELEGRAM_BOT_TOKEN:
        print("TELEGRAM_BOT_TOKEN kosong, hentikan.")
        return
    if not ALLOWED:
        clog("⚠ ALLOWED_CHAT_IDS/TARGET_CHAT_ID kosong: bot terbuka untuk siapa saja. Isi salah satunya.")
    clog(f"RAFANO V5.0 start | IDX={len(IDX_FULL)} | workers={SCAN_WORKERS} | colab={IS_COLAB}")
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
    except Exception as e:
        clog(f"deleteWebhook err {e}")
    threading.Thread(target=auto_broadcast_loop, daemon=True).start()
    offset = 0
    while True:
        try:
            res = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                               params={"offset": offset, "timeout": 20, "allowed_updates": json.dumps(["message", "callback_query"])}, timeout=30)
            data = res.json() if res.status_code == 200 else {}
            if not data.get("ok"):
                time.sleep(3)
                continue
            for upd in data.get("result", []):
                offset = upd["update_id"] + 1
                try:
                    if "callback_query" in upd:
                        handle_callback(upd["callback_query"])
                    elif "message" in upd and "text" in upd["message"]:
                        handle_message(upd["message"])
                except Exception as e:
                    clog(f"handler err {e}")
                    import traceback
                    traceback.print_exc()
        except Exception as e:
            clog(f"listener err {e}")
            time.sleep(3)


if __name__ == "__main__":
    telegram_bot_listener()
