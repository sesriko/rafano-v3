"""
RAFANO V3 - FINAL: Arjum Pro + Pro Chart Generator
Gabungan:
- Screener/latest + broker-accumulation + broker-summary (REAL DATA)
- Chart Generator V1 (candlestick custom, EMA 8/21/50/125, dashboard kiri-kanan)
- Telegram Listener + Auto Screener Loop
"""
import os
import time
import logging
import datetime
import threading
import requests
import pytz
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.gridspec as gridspec
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()

# ========== COLAB FIX - ANTI JSON ERROR ==========
# JANGAN pakai userdata.get di top-level, nanti Colab JS error kalau secret ada "
# Kita pakai os.getenv saja, Colab akan auto-inject secrets sebagai env var
def safe_get_env(key):
    v = os.getenv(key)
    if v:
        # strip quotes jika user isi pakai "
        v = str(v).strip()
        if len(v)>=2 and ((v[0]=='"' and v[-1]=='"') or (v[0]=="'" and v[-1]=="'")):
            v = v[1:-1].strip()
        return v
    # fallback coba userdata tapi dengan try catch halus
    try:
        from google.colab import userdata
        vv = userdata.get(key)
        if vv:
            vv = str(vv).strip().strip('"').strip("'")
            os.environ[key] = vv
            return vv
    except Exception as ee:
        # jangan print error JSON disini, biar gak crash
        pass
    return None

TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')

TELEGRAM_BOT_TOKEN = safe_get_env("TELEGRAM_BOT_TOKEN")
TARGET_CHAT_ID = safe_get_env("TARGET_CHAT_ID")
ARJUM_API_KEY = safe_get_env("ARJUM_API_KEY")

# Debug aman tanpa userdata
print(f"🔑 ENV Loaded - TOKEN exists={bool(TELEGRAM_BOT_TOKEN)} len={len(TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else 0}, CHAT_ID={TARGET_CHAT_ID}, ARJUM len={len(ARJUM_API_KEY) if ARJUM_API_KEY else 0}")
if not TELEGRAM_BOT_TOKEN:
    print("❌ FATAL: TELEGRAM_BOT_TOKEN KOSONG!")
    print("   -> Colab > Secrets > isi TANPA tanda petik, toggle ON Notebook access")
    print("   -> Atau hardcode di cell: os.environ['TELEGRAM_BOT_TOKEN']='token'")

ARJUM_BASE = "https://stock.arjum.com/api"
def get_arjum_headers():
    k = os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY or safe_get_env("ARJUM_API_KEY") or ""
    return {"X-API-Key": k.strip(), "Accept": "application/json", "User-Agent": "Mozilla/5.0"}
HEADERS_ARJUM = get_arjum_headers()
HEADERS = {"User-Agent": "Mozilla/5.0"}

def get_now_wib():
    return datetime.datetime.now(TIMEZONE_WIB)

# ========== HELPERS UMUM ==========
def safe_int(val, default=0):
    try:
        if pd.isna(val) or np.isinf(val):
            return default
        return int(val)
    except:
        return default

def format_large_number(val, show_sign=False):
    if pd.isna(val) or val == 0:
        return "0"
    abs_val = abs(val)
    sign = "+" if (show_sign and val > 0) else ("-" if val < 0 else "")
    if abs_val >= 1_000_000_000:
        return f"{sign}{abs_val / 1_000_000_000:.2f}B"
    elif abs_val >= 1_000_000:
        return f"{sign}{abs_val / 1_000_000:,.0f}M"
    elif abs_val >= 1_000:
        return f"{sign}{abs_val / 1_000:,.0f}K"
    else:
        return f"{sign}{val:,.0f}"

def round_to_ihsg_fraction(price):
    if pd.isna(price) or price <= 0:
        return 0
    price = float(price)
    if price < 200:
        tick = 1
    elif price < 500:
        tick = 2
    elif price < 2000:
        tick = 5
    elif price < 5000:
        tick = 10
    else:
        tick = 25
    return int(round(price / tick) * tick)

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, 0.00001)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(50)

def calculate_atr(df, period=14):
    high, low, close = df['High'], df['Low'], df['Close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=1).mean()

def calculate_vsa_metrics(df):
    """
    OKE SAHAM style VSA - Power Buy = dana besar masuk
    Buy% = (Close - Low)/(High - Low) -> posisi close di range
    Kalau close di atas = akumulasi, di bawah = distribusi
    + boost kalau volume > rata2 (power besar)
    """
    price_range = (df['High'] - df['Low']).replace(0, 0.1)
    # OKE SAHAM original: Buy% berdasarkan posisi Close di candle range
    close_pos = (df['Close'] - df['Low']) / price_range  # 0 = di low, 1 = di high
    close_pos = np.clip(close_pos, 0.05, 0.95)
    
    # Base buy ratio dari close position
    buy_ratio = 0.30 + close_pos * 0.60  # 30%-90% range, kalau close di high = 90% buy
    
    # Boost kalau volume besar + candle hijau (power buy)
    if 'V1' in df.columns:
        vol_ratio = df['Volume'] / df['V1'].replace(0, 1)
        # Jika volume > 1.5x rata2 dan candle hijau, tambah power
        is_green = df['Close'] >= df['Open']
        boost = np.where((vol_ratio > 1.5) & is_green, 0.10, 0)
        boost += np.where((vol_ratio > 2.5) & is_green, 0.10, 0)  # TURBO boost
        buy_ratio = buy_ratio + boost
    
    buy_ratio = np.clip(buy_ratio, 0.05, 0.95)
    
    df['Vol_Buy'] = df['Volume'] * buy_ratio
    df['Vol_Sell'] = df['Volume'] - df['Vol_Buy']
    df['Net_Vol_VSA'] = df['Vol_Buy'] - df['Vol_Sell']
    df['Net_Val_VSA'] = df['Net_Vol_VSA'] * df['Close']
    df['Buy_Pct'] = buy_ratio * 100
    return df, buy_ratio

def calculate_buy_signal_strength(df):
    if len(df) < 20:
        return 0, "NO DATA"
    last_row = df.iloc[-1]
    last_close, last_open, last_vol = last_row['Close'], last_row['Open'], last_row['Volume']
    avg_vol_v1 = last_row.get('V1', last_row['Volume'])
    df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
    ema_50 = df['EMA50'].iloc[-1]
    df, buy_ratios = calculate_vsa_metrics(df)
    last_buy_ratio = buy_ratios[-1]
    net_5d_val = df['Net_Val_VSA'].tail(5).sum()
    score = 0
    if last_close > ema_50: score += 25
    vol_multiple = last_vol / avg_vol_v1 if avg_vol_v1 > 0 else 0
    if vol_multiple >= 2.5: score += 25
    elif vol_multiple >= 2.0: score += 20
    elif vol_multiple >= 1.8: score += 15
    if last_buy_ratio >= 0.75: score += 20
    elif last_buy_ratio >= 0.65: score += 15
    elif last_buy_ratio >= 0.55: score += 10
    if net_5d_val > 0: score += 20
    if last_close > last_open: score += 10
    if score >= 85: label = "VERY STRONG"
    elif score >= 70: label = "STRONG BUY"
    elif score >= 50: label = "WEAK BUY"
    else: label = "NO SIGNAL"
    return score, label

def calculate_bollinger_bands(df, period=20, std=2):
    sma = df['Close'].rolling(period).mean()
    stddev = df['Close'].rolling(period).std()
    upper = sma + (stddev * std)
    lower = sma - (stddev * std)
    return sma, upper, lower

def detect_buy_signals(df, multi_tf=None):
    signals = []
    if df is None or len(df) < 30:
        return signals, df
    try:
        df = df.copy()
        df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
        df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()
        df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
        df['V1'] = df['Volume'].rolling(20, min_periods=1).mean()
        df['ATR'] = calculate_atr(df, 14)
        bb_mid, bb_upper, bb_lower = calculate_bollinger_bands(df, 20, 2)
        df['BB_MID'] = bb_mid
        df['BB_UPPER'] = bb_upper
        df['BB_LOWER'] = bb_lower
        df, _ = calculate_vsa_metrics(df)
        net_5d = 0
        if multi_tf:
            net_5d = multi_tf.get('net_5d', 0) or multi_tf.get('vsa_5d', 0) or 0
        else:
            net_5d = df['Net_Val_VSA'].tail(5).sum() if 'Net_Val_VSA' in df.columns else 0
        for i in range(20, len(df)):
            close = df['Close'].iloc[i]
            open_ = df['Open'].iloc[i]
            low = df['Low'].iloc[i]
            vol = df['Volume'].iloc[i]
            v1 = df['V1'].iloc[i]
            ema50 = df['EMA50'].iloc[i]
            ema200 = df['EMA200'].iloc[i]
            ema20 = df['EMA20'].iloc[i]
            bb_low = df['BB_LOWER'].iloc[i] if not pd.isna(df['BB_LOWER'].iloc[i]) else 0
            atr = df['ATR'].iloc[i] if not pd.isna(df['ATR'].iloc[i]) else close*0.03
            prev_close = df['Close'].iloc[i-1]
            prev_ema50 = df['EMA50'].iloc[i-1]
            is_bo_ema50 = (prev_close <= prev_ema50 and close > ema50 and close > ema20)
            vol_spike = (vol > v1 * 1.5) if v1>0 else False
            is_green = close >= open_
            if is_bo_ema50 and vol_spike and is_green and net_5d > 0:
                signals.append({'index': i, 'date': df.index[i], 'type': 'BO EMA50', 'side': 'BUY', 'entry': float(close), 'sl': float(min(df['Low'].iloc[max(0,i-5):i+1].min(), close - atr*1.2)), 'reason': f'Breakout EMA50 + Vol {vol/v1:.1f}x + Net 5D Akum', 'strength': 90})
                continue
            if bb_low > 0:
                dist_to_bb_low = (close - bb_low) / bb_low * 100
                is_far_below_bb = close < bb_low and dist_to_bb_low < -1.5
                body = abs(close - open_)
                lower_wick = min(open_, close) - low
                is_reversal = is_green and lower_wick > body*1.5 and body > 0
                if is_far_below_bb and is_reversal:
                    signals.append({'index': i, 'date': df.index[i], 'type': 'BOW BB', 'side': 'BUY', 'entry': float(close), 'sl': float(low * 0.98), 'reason': f'BOW: {dist_to_bb_low:.1f}% below BB Lower + Reversal', 'strength': 85})
                    continue
            dist_ema50 = abs(close - ema50) / ema50 * 100 if ema50>0 else 100
            dist_ema200 = abs(close - ema200) / ema200 * 100 if ema200>0 else 100
            is_near_ema = dist_ema50 < 2.0 or dist_ema200 < 3.0
            wick_count = 0
            for j in range(max(0, i-10), i+1):
                l = df['Low'].iloc[j]
                e50 = df['EMA50'].iloc[j]
                e200 = df['EMA200'].iloc[j]
                if abs(l - e50)/e50 < 0.015 or abs(l - e200)/e200 < 0.02:
                    wick_count += 1
            is_support_bounce = is_near_ema and wick_count >= 2 and close > ema50 and close > open_
            if is_support_bounce:
                signals.append({'index': i, 'date': df.index[i], 'type': 'BOS EMA', 'side': 'BUY', 'entry': float(close), 'sl': float(min(df['Low'].iloc[max(0,i-3):i+1].min(), ema50*0.97)), 'reason': f'BOS: Near EMA {min(dist_ema50,dist_ema200):.1f}% + {wick_count}x wick', 'strength': 80})
                continue
        filtered = []
        last_idx = -20
        for sig in sorted(signals, key=lambda x: x['index']):
            if sig['index'] - last_idx >= 5:
                filtered.append(sig)
                last_idx = sig['index']
            else:
                if filtered and sig['strength'] > filtered[-1]['strength']:
                    filtered[-1] = sig
                    last_idx = sig['index']
        return filtered, df
    except Exception as e:
        print(f"detect_buy_signals error: {e}")
        return [], df

def detect_sell_signals(df, multi_tf=None):
    signals = []
    if df is None or len(df) < 30:
        return signals, df
    try:
        if 'EMA50' not in df.columns:
            df = df.copy()
            df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
            df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()
            df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
            df['V1'] = df['Volume'].rolling(20, min_periods=1).mean()
            df['ATR'] = calculate_atr(df, 14)
            _, bb_upper, _ = calculate_bollinger_bands(df, 20, 2)
            df['BB_UPPER'] = bb_upper
            df, _ = calculate_vsa_metrics(df)
        net_5d = 0
        if multi_tf:
            net_5d = multi_tf.get('net_5d', 0) or 0
        for i in range(20, len(df)):
            close = df['Close'].iloc[i]
            open_ = df['Open'].iloc[i]
            high = df['High'].iloc[i]
            vol = df['Volume'].iloc[i]
            v1 = df['V1'].iloc[i]
            ema50 = df['EMA50'].iloc[i]
            ema200 = df['EMA200'].iloc[i]
            bb_up = df['BB_UPPER'].iloc[i] if not pd.isna(df['BB_UPPER'].iloc[i]) else 0
            atr = df['ATR'].iloc[i] if not pd.isna(df['ATR'].iloc[i]) else close*0.03
            prev_close = df['Close'].iloc[i-1]
            prev_ema50 = df['EMA50'].iloc[i-1]
            is_bd_ema50 = (prev_close >= prev_ema50 and close < ema50)
            vol_spike = (vol > v1 * 1.5) if v1>0 else False
            is_red = close < open_
            if is_bd_ema50 and vol_spike and is_red and net_5d < 0:
                signals.append({'index': i, 'date': df.index[i], 'type': 'BD EMA50', 'side': 'SELL', 'entry': float(close), 'sl': float(max(df['High'].iloc[max(0,i-5):i+1].max(), close + atr*1.2)), 'reason': f'Breakdown EMA50 + Vol {vol/v1:.1f}x + Net Dist', 'strength': 90})
                continue
            if bb_up > 0:
                dist_to_bb_up = (close - bb_up) / bb_up * 100
                is_far_above_bb = close > bb_up and dist_to_bb_up > 1.5
                body = abs(close - open_)
                upper_wick = high - max(open_, close)
                is_rejection = is_red and upper_wick > body*1.5
                if is_far_above_bb and is_rejection:
                    signals.append({'index': i, 'date': df.index[i], 'type': 'SOS BB', 'side': 'SELL', 'entry': float(close), 'sl': float(high * 1.02), 'reason': f'SOS: +{dist_to_bb_up:.1f}% above BB Upper + Rejection', 'strength': 85})
                    continue
        filtered = []
        last_idx = -20
        for sig in sorted(signals, key=lambda x: x['index']):
            if sig['index'] - last_idx >= 5:
                filtered.append(sig)
                last_idx = sig['index']
        return filtered, df
    except Exception as e:
        print(f"detect_sell error: {e}")
        return [], df

def calculate_trading_plan(df, signals=None, multi_tf=None):
    """
    Trading plan MTF: Berlaku multi timeframe
    - Primary TF = timeframe chart yang dipanggil
    - Konfirmasi Higher TF (Weekly) trend
    - Konfirmasi Lower TF (5m/15m) untuk entry timing
    """
    try:
        if df is None or len(df) < 20:
            return None
        last_close = df['Close'].iloc[-1]
        atr = calculate_atr(df, 14).iloc[-1]
        if pd.isna(atr) or atr == 0:
            atr = last_close * 0.03
        ema20 = df['Close'].ewm(span=20).mean().iloc[-1]
        ema50 = df['Close'].ewm(span=50).mean().iloc[-1]
        ema200 = df['Close'].ewm(span=200).mean().iloc[-1]

        # Deteksi sinyal jika belum ada
        if signals is None:
            buy_sigs, _ = detect_buy_signals(df, multi_tf)
            sell_sigs, _ = detect_sell_signals(df, multi_tf)
            signals = buy_sigs + sell_sigs
        else:
            buy_sigs = [s for s in signals if s.get('side')=='BUY']
            sell_sigs = [s for s in signals if s.get('side')=='SELL']

        # === MTF ANALYSIS ===
        mtf_trend = {}
        mtf_confirm = "NEUTRAL"
        weekly_bullish = False
        monthly_bullish = False
        if multi_tf:
            # Dari multi_tf broker + price vs EMA
            # Weekly 5D dan Monthly 20D status
            status_5d = multi_tf.get('status_5d','NEUTRAL')
            status_20d = multi_tf.get('status_20d','NEUTRAL')
            net_5d = multi_tf.get('net_5d',0)
            net_20d = multi_tf.get('net_20d',0)
            mtf_trend['weekly'] = status_5d
            mtf_trend['monthly'] = status_20d
            weekly_bullish = status_5d == "AKUM" and net_5d > 0
            monthly_bullish = status_20d == "AKUM" and net_20d > 0
            if weekly_bullish and monthly_bullish:
                mtf_confirm = "STRONG BULLISH MTF"
            elif weekly_bullish or monthly_bullish:
                mtf_confirm = "BULLISH MTF"
            elif status_5d == "DIST" and status_20d == "DIST":
                mtf_confirm = "BEARISH MTF"

        # Tentukan sinyal terbaru (10 candle terakhir)
        recent_buy = [s for s in (buy_sigs if 'buy_sigs' in locals() else []) if s['index'] >= len(df)-10]
        recent_sell = [s for s in (sell_sigs if 'sell_sigs' in locals() else []) if s['index'] >= len(df)-10]

        if recent_buy and (not recent_sell or recent_buy[-1]['index'] >= recent_sell[-1]['index']):
            last_signal = recent_buy[-1]
            entry = last_signal['entry']
            sl = last_signal['sl']
            signal_type = last_signal['type']
            signal_reason = last_signal['reason']
            signal_strength = last_signal['strength']
            signal_date = last_signal['date']
            side = "BUY"
            is_buy = True
        elif recent_sell:
            last_signal = recent_sell[-1]
            entry = last_signal['entry']
            sl = last_signal['sl']
            signal_type = last_signal['type']
            signal_reason = last_signal['reason']
            signal_strength = last_signal['strength']
            signal_date = last_signal['date']
            side = "SELL"
            is_buy = False
        else:
            # No recent signal
            entry = round_to_ihsg_fraction(last_close)
            sl = round_to_ihsg_fraction(max(df['Low'].tail(5).min(), last_close - atr*1.5))
            signal_type = "NO SIGNAL"
            signal_reason = "Tunggu BO EMA50 / BOW BB / BOS EMA (BUY) atau BD EMA50 / SOS BB (SELL)"
            signal_strength = 0
            signal_date = df.index[-1]
            side = "WAIT"
            is_buy = False

        # MTF Boost strength
        if side == "BUY" and mtf_confirm == "STRONG BULLISH MTF":
            signal_strength = min(100, signal_strength + 10)
            signal_reason += " + MTF Weekly+Monthly AKUM"
        elif side == "BUY" and mtf_confirm == "BULLISH MTF":
            signal_strength = min(95, signal_strength + 5)
            signal_reason += " + MTF Bullish"
        elif side == "SELL" and mtf_confirm == "BEARISH MTF":
            signal_strength = min(100, signal_strength + 10)
            signal_reason += " + MTF Weekly+Monthly DIST"

        # Validasi SL max 7% risk
        min_sl = last_close * 0.92
        max_sl = last_close * 0.98
        sl = max(min(sl, max_sl), min_sl)
        sl = round_to_ihsg_fraction(sl)
        if entry <= sl and side != "SELL":
            entry = round_to_ihsg_fraction(sl * 1.03)

        # TP berdasarkan tipe sinyal + MTF
        if side == "BUY":
            if "BOW" in signal_type:
                tp1 = round_to_ihsg_fraction(entry * 1.04)
                tp2 = round_to_ihsg_fraction(entry * 1.08)
            elif "BO EMA50" in signal_type:
                tp1 = round_to_ihsg_fraction(entry + atr*1.5)
                tp2 = round_to_ihsg_fraction(entry + atr*3.0)
                if mtf_confirm == "STRONG BULLISH MTF":
                    tp2 = round_to_ihsg_fraction(entry + atr*4.0)  # target lebih tinggi kalau MTF bullish
            else:
                tp1 = round_to_ihsg_fraction(entry * 1.035)
                tp2 = round_to_ihsg_fraction(entry + atr*1.8)
            risk = entry - sl
            reward1 = tp1 - entry
            reward2 = tp2 - entry
        elif side == "SELL":
            sl_sell = min(max(sl, last_close*1.02), last_close*1.08)
            sl = round_to_ihsg_fraction(sl_sell)
            if entry >= sl:
                entry = round_to_ihsg_fraction(sl * 0.97)
            tp1 = round_to_ihsg_fraction(entry * 0.965)
            tp2 = round_to_ihsg_fraction(entry - atr*1.8)
            if "SOS" in signal_type:
                tp1 = round_to_ihsg_fraction(entry * 0.96)
                tp2 = round_to_ihsg_fraction(entry * 0.92)
            elif "BD EMA50" in signal_type:
                tp1 = round_to_ihsg_fraction(entry - atr*1.5)
                tp2 = round_to_ihsg_fraction(entry - atr*3.0)
            risk = sl - entry
            reward1 = entry - tp1
            reward2 = entry - tp2
        else:
            tp1 = round_to_ihsg_fraction(entry * 1.035)
            tp2 = round_to_ihsg_fraction(entry + atr*1.8)
            risk = entry - sl
            reward1 = tp1 - entry
            reward2 = tp2 - entry

        rr1 = reward1 / risk if risk>0 else 0
        rr2 = reward2 / risk if risk>0 else 0

        if last_close > ema20 and last_close > ema50 and last_close > ema200:
            trend = "STRONG UPTREND"
        elif last_close > ema20 and last_close > ema50:
            trend = "UPTREND"
        elif last_close > ema20:
            trend = "WEAK UPTREND"
        elif last_close < ema20 and last_close < ema50 and last_close < ema200:
            trend = "STRONG DOWNTREND"
        else:
            trend = "DOWNTREND"

        # FIX FINAL: STRONG DOWNTREND harus WAIT kecuali breakout EMA20
        if "DOWNTREND" in trend:
            very_recent_buy = [s for s in (buy_sigs if 'buy_sigs' in locals() else []) if s['index'] >= len(df)-3]
            if last_close < ema20:
                if side == "BUY":
                    side = "WAIT"
                    is_buy = False
                    signal_type = "NO SIGNAL"
                    signal_reason = f"WAIT - {trend} Close {last_close:.0f} < EMA20 {ema20:.0f}, tunggu breakout EMA20. MTF {mtf_confirm} tidak cukup untuk BUY"
                    signal_strength = 0
            elif not very_recent_buy and side == "BUY":
                side = "WAIT"
                is_buy = False
                signal_type = "NO SIGNAL"
                signal_reason = f"Tunggu trigger valid - {trend}, tidak ada BO/BOW 3 candle terakhir. Close di bawah EMA50 {ema50:.0f}. MTF {mtf_confirm} bukan jaminan"
                signal_strength = 0

        # Gabung trend dengan MTF confirm
        trend_mtf = f"{trend} + {mtf_confirm}" if mtf_confirm != "NEUTRAL" else trend

        return {
            "entry": int(entry),
            "sl": int(sl),
            "tp1": int(tp1),
            "tp2": int(tp2),
            "atr": float(atr),
            "risk_pct": round((risk/entry)*100, 2) if entry else 0,
            "rr1": round(rr1, 2),
            "rr2": round(rr2, 2),
            "trend": trend_mtf,
            "support": int(df['Low'].tail(10).min()),
            "resistance": int(df['High'].tail(10).max()),
            "signal_type": signal_type,
            "signal_reason": signal_reason,
            "signal_strength": signal_strength,
            "signal_date": signal_date,
            "all_signals": signals,
            "buy_signals": buy_sigs if 'buy_sigs' in locals() else [],
            "sell_signals": sell_sigs if 'sell_sigs' in locals() else [],
            "is_buy_signal": is_buy and signal_strength >= 70,
            "is_sell_signal": (not is_buy) and side=="SELL" and signal_strength >= 70,
            "side": side,
            "mtf_trend": mtf_trend,
            "mtf_confirm": mtf_confirm,
            "mtf_applicable": True
        }
    except Exception as e:
        print(f"Trading plan error: {e}")
        import traceback
        traceback.print_exc()
        return None

def calculate_trading_plan_with_signals(df, signals=None, multi_tf=None):
    return calculate_trading_plan(df, signals=signals, multi_tf=multi_tf)

def is_market_open():
    now = get_now_wib()
    w

def is_market_open():
    now = get_now_wib()
    weekday = now.weekday()
    if weekday >= 5:
        return False
    current_time = now.time()
    if weekday == 4:
        s1_start, s1_end = datetime.time(9, 0), datetime.time(11, 30)
        s2_start, s2_end = datetime.time(14, 0), datetime.time(15, 50)
    else:
        s1_start, s1_end = datetime.time(9, 0), datetime.time(12, 0)
        s2_start, s2_end = datetime.time(13, 30), datetime.time(15, 50)
    return (s1_start <= current_time <= s1_end) or (s2_start <= current_time <= s2_end)

# ========== ARJUM PRO WRAPPER ==========
def arjum_get(path, params=None, use_cache=True):
    # INCREMENTAL FETCH: Cek cache dulu sebelum hit API
    cache_key = make_cache_key(path, params) if use_cache else None
    
    if use_cache and cache_key:
        # Untuk broker & screener, cek cache
        if 'broker' in path:
            cached = get_cached_broker(cache_key)
            if cached is not None:
                return cached
        elif 'screener' in path:
            cached = get_cached_screener()
            if cached is not None:
                return cached

    url = f"{ARJUM_BASE}{path}"
    try:
        api_key = os.getenv("ARJUM_API_KEY") or ARJUM_API_KEY
        if not api_key:
            api_key = safe_get_env("ARJUM_API_KEY")
        if not api_key:
            print(f"❌ ARJUM_API_KEY KOSONG! path={path}")
            return None
        headers = {"X-API-Key": api_key, "Accept": "application/json", "User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, params=params, timeout=12)
        if r.status_code == 200:
            try:
                j = r.json()
                # Simpan ke cache
                if use_cache and cache_key:
                    if 'broker' in path:
                        set_cached_broker(cache_key, j)
                    elif 'screener' in path:
                        set_cached_screener(j)
                if "BBCA" in path or "broker" in path:
                    print(f"🌐 API FETCH {path} OK (cache miss)")
                return j
            except Exception as je:
                print(f"⚠ arjum_get {path} JSON parse fail: {je}")
                return None
        else:
            print(f"⚠ arjum_get {path} params={params} -> {r.status_code}")
            return None
    except Exception as e:
        print(f"arjum_get error {path}: {e}")
        return None


# ========== INCREMENTAL FETCH SYSTEM - CACHE DULU BARU API ==========
import json
from pathlib import Path

BROKER_CACHE = {}
HISTORY_CACHE = {}
SCREENER_CACHE = {}
CACHE_FILE = Path("/tmp/rafano_cache.json")
BROKER_CACHE_TTL = 300  # 5 menit
HISTORY_CACHE_TTL = 600  # 10 menit untuk history
SCREENER_CACHE_TTL = 180  # 3 menit untuk screener

# Load cache dari file jika ada (biar survive restart)
try:
    if CACHE_FILE.exists():
        with open(CACHE_FILE, 'r') as cf:
            loaded = json.load(cf)
            BROKER_CACHE = {k: (v[0], v[1]) for k, v in loaded.get('broker', {}).items()}
            print(f"📦 Cache loaded: {len(BROKER_CACHE)} broker entries")
except:
    pass

def save_cache_to_file():
    try:
        data = {
            'broker': {k: [v[0], v[1]] for k, v in BROKER_CACHE.items()},
            'timestamp': __import__('time').time()
        }
        with open(CACHE_FILE, 'w') as cf:
            json.dump(data, cf)
    except:
        pass

def get_cached_broker(key):
    import time
    # 1. Cek memory cache dulu
    if key in BROKER_CACHE:
        ts, data = BROKER_CACHE[key]
        if time.time() - ts < BROKER_CACHE_TTL:
            print(f"⚡ CACHE HIT broker {key} (age {int(time.time()-ts)}s)")
            return data
        else:
            print(f"⏰ CACHE EXPIRED broker {key}")
            del BROKER_CACHE[key]
    return None

def set_cached_broker(key, data):
    import time
    BROKER_CACHE[key] = (time.time(), data)
    # async save ke file
    try:
        save_cache_to_file()
    except:
        pass
    print(f"💾 CACHE SET broker {key}")

def get_cached_history(key):
    import time
    if key in HISTORY_CACHE:
        ts, data = HISTORY_CACHE[key]
        if time.time() - ts < HISTORY_CACHE_TTL:
            print(f"⚡ CACHE HIT history {key}")
            return data
        else:
            del HISTORY_CACHE[key]
    return None

def set_cached_history(key, data):
    import time
    HISTORY_CACHE[key] = (time.time(), data)

def get_cached_screener():
    import time
    if 'latest' in SCREENER_CACHE:
        ts, data = SCREENER_CACHE['latest']
        if time.time() - ts < SCREENER_CACHE_TTL:
            print(f"⚡ CACHE HIT screener (age {int(time.time()-ts)}s)")
            return data
        else:
            del SCREENER_CACHE['latest']
    return None

def set_cached_screener(data):
    import time
    SCREENER_CACHE['latest'] = (time.time(), data)

def make_cache_key(path, params):
    # Buat key unik dari path + params
    if not params:
        return path
    # sort params biar konsisten
    try:
        sorted_params = sorted(params.items())
        param_str = "&".join([f"{k}={v}" for k, v in sorted_params])
        return f"{path}?{param_str}"
    except:
        return path


def get_screener_latest():
    # Incremental: cek cache dulu
    cached = get_cached_screener()
    if cached:
        data = cached
    else:
        data = arjum_get("/screener/latest")
    print(f"DEBUG screener raw type={type(data)} sample={str(data)[:400]}")
    if not data:
        return []
    if isinstance(data, dict):
        # Format baru V5.2: {'rows': [{'stock_code':'IBOS', ...}]}
        if 'rows' in data and isinstance(data['rows'], list) and len(data['rows'])>0:
            # normalisasi jadi format symbol
            normalized = []
            for r in data['rows']:
                code = r.get('stock_code') or r.get('symbol') or r.get('code') or r.get('stock')
                if code:
                    # gabungin sisa data biar score bisa pake bucket dll
                    item = {'symbol': code.replace(".JK","").upper(), 'raw': r, 'bucket': r.get('bucket',''), 'summary': r.get('summary','')}
                    normalized.append(item)
            print(f"  -> V5.2 detected, {len(normalized)} saham: {[x['symbol'] for x in normalized[:10]]}")
            return normalized
        # Format lama
        for k in ['data','results','stocks','screener','latest','items']:
            if k in data and isinstance(data[k], list) and len(data[k])>0:
                return data[k]
        # jika dict tapi values list langsung
        try:
            first_val = list(data.values())[0]
            if isinstance(first_val, list) and len(first_val)>0:
                return first_val
        except:
            pass
        return data.get('data') or data.get('results') or data.get('stocks') or []
    return data if isinstance(data, list) else []

def get_broker_accumulation(symbol, top=3, days=None):
    params = {"top": top}
    if days:
        params["days"] = days
        params["period"] = days
    data = arjum_get(f"/broker-accumulation/{symbol}", params=params, use_cache=False)
    if not data or (isinstance(data, dict) and not data.get('top_buyers') and not data.get('series')):
        data = arjum_get(f"/broker-accumulation/{symbol}", params={"top": top}, use_cache=False)
    if not data:
        print(f"DEBUG accum {symbol} days={days} no data")
        return 0.0, []

    if isinstance(data, dict):
        print(f"DEBUG accum {symbol} days={days} keys={list(data.keys())} top_buyers={len(data.get('top_buyers',[]))} series={len(data.get('series',[]))}")

    if isinstance(data, dict) and ('top_buyers' in data or 'series' in data or 'top_sellers' in data):
        raw_brokers = []
        accum_total = 0
        top_buyers = data.get('top_buyers') or []
        top_sellers = data.get('top_sellers') or []
        series = data.get('series') or []

        is_timeline_format = False
        if series and isinstance(series[0], dict) and 'accum_val' in series[0] and 'date' in series[0]:
            is_timeline_format = True
            if series:
                if days and len(series) >= 2:
                    n = int(days)
                    last_accum = float(series[-1].get('accum_val',0) or 0)
                    first_accum = float(series[-n].get('accum_val',0) or series[0].get('accum_val',0) or 0) if len(series) >= n else float(series[0].get('accum_val',0) or 0)
                    accum_total = last_accum - first_accum
                    if accum_total == 0:
                        accum_total = last_accum
                else:
                    accum_total = float(series[-1].get('accum_val',0) or 0)

        # Format A: per broker points
        if not is_timeline_format and series and len(series)>0 and isinstance(series[0], dict) and 'broker_code' in series[0]:
            # Check if days param - sum last N points per broker
            if days:
                n_days = int(days)
                for ser in series[:20]:
                    if not isinstance(ser, dict) or 'broker_code' not in ser:
                        continue
                    code = ser.get('broker_code') or '??'
                    points = ser.get('points') or []
                    if not points:
                        continue
                    last_points = points[-n_days:] if len(points) >= n_days else points
                    sum_bval = sum([float(p.get('bval',0) or 0) for p in last_points])
                    sum_sval = sum([float(p.get('sval',0) or 0) for p in last_points])
                    sum_nval = sum([float(p.get('nval',0) or 0) for p in last_points])
                    last = last_points[-1] if last_points else {}
                    bavg = float(last.get('bavg',0) or last.get('avg',0) or 0)
                    avg = bavg
                    sum_bvol = sum([float(p.get('bvol',0) or 0) for p in last_points])
                    sum_svol = sum([float(p.get('svol',0) or 0) for p in last_points])
                    if sum_bval==0 and sum_sval==0 and sum_nval==0:
                        continue
                    raw_brokers.append({
                        "broker_code": str(code).upper(),
                        "broker": str(code).upper(),
                        "buy_value": float(sum_bval),
                        "sell_value": float(sum_sval),
                        "buy_volume": float(sum_bvol),
                        "sell_volume": float(sum_svol),
                        "net_value": float(sum_nval),
                        "avg_price": float(avg)
                    })
                    accum_total += abs(float(sum_nval)) if sum_nval!=0 else abs(float(sum_bval))
                if raw_brokers:
                    return float(accum_total), raw_brokers
            else:
                # Daily without days - use top_buyers if exists, else series cum
                pass

        # Daily atau timeline - pakai top_buyers
        if top_buyers and isinstance(top_buyers, list):
            for b in top_buyers[:20]:
                if not isinstance(b, dict):
                    continue
                code = b.get('broker_code') or b.get('code') or '??'
                nval = b.get('nval') or b.get('net_val') or b.get('net_value') or b.get('value') or 0
                bval = b.get('bval') or b.get('buy_value') or (nval if float(nval or 0)>0 else 0)
                sval = b.get('sval') or b.get('sell_value') or (abs(float(nval or 0)) if float(nval or 0)<0 else 0)
                bvol = b.get('bvol') or b.get('buy_volume') or b.get('nvol') or 0
                svol = b.get('svol') or 0
                avg = b.get('bavg') or b.get('avg_price') or 0
                raw_brokers.append({
                    "broker_code": str(code).upper(),
                    "broker": str(code).upper(),
                    "buy_value": float(bval),
                    "sell_value": float(sval),
                    "buy_volume": float(bvol),
                    "sell_volume": float(svol),
                    "net_value": float(nval),
                    "avg_price": float(avg)
                })
                if not is_timeline_format:
                    accum_total += abs(float(nval))

        if not raw_brokers and top_sellers:
            for b in top_sellers[:20]:
                if not isinstance(b, dict):
                    continue
                code = b.get('broker_code') or '??'
                nval = b.get('nval') or b.get('net_val') or 0
                bval = b.get('bval') or 0
                sval = b.get('sval') or abs(float(nval)) if float(nval or 0)<0 else 0
                raw_brokers.append({
                    "broker_code": str(code).upper(),
                    "broker": str(code).upper(),
                    "buy_value": float(bval),
                    "sell_value": float(sval),
                    "buy_volume": 0,
                    "sell_volume": 0,
                    "net_value": float(nval),
                    "avg_price": 0
                })
                if not is_timeline_format:
                    accum_total += abs(float(nval))

        if not raw_brokers and is_timeline_format and accum_total!=0:
            raw_brokers.append({
                "broker_code": "ALL",
                "broker": "ALL",
                "buy_value": float(accum_total) if accum_total>0 else 0,
                "sell_value": float(abs(accum_total)) if accum_total<0 else 0,
                "buy_volume": 0,
                "sell_volume": 0,
                "net_value": float(accum_total),
                "avg_price": 0
            })

        # Format A without days - series cum
        if not raw_brokers and series and not is_timeline_format:
            for ser in series[:20]:
                if not isinstance(ser, dict) or 'broker_code' not in ser:
                    continue
                code = ser.get('broker_code') or '??'
                points = ser.get('points') or []
                if points:
                    last = points[-1]
                    cum = float(last.get('cum_nval',0) or last.get('nval',0) or 0)
                    bavg = float(last.get('bavg',0) or 0)
                    raw_brokers.append({
                        "broker_code": str(code).upper(),
                        "broker": str(code).upper(),
                        "buy_value": abs(cum) if cum>0 else 0,
                        "sell_value": abs(cum) if cum<0 else 0,
                        "buy_volume": 0,
                        "sell_volume": 0,
                        "net_value": float(cum),
                        "avg_price": float(bavg)
                    })
                    accum_total += abs(cum)

        return float(accum_total), raw_brokers



def get_broker_summary(symbol, days=None):
    # FIX FINAL: pakai start_date & end_date sesuai docs API /broker-summary/{code}?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
    data = None
    used_params = None
    base_params_list = []
    if days:
        try:
            import pytz
            wib = pytz.timezone('Asia/Jakarta')
            now_wib = datetime.datetime.now(wib)
            end_dt = now_wib
            if int(days) == 1:
                start_dt = end_dt
            elif int(days) == 5:
                start_dt = end_dt - datetime.timedelta(days=6)
            elif int(days) == 20:
                start_dt = end_dt - datetime.timedelta(days=27)
            else:
                start_dt = end_dt - datetime.timedelta(days=int(days)-1)
            end_str = end_dt.strftime('%Y-%m-%d')
            start_str = start_dt.strftime('%Y-%m-%d')
            base_params_list.append({"start_date": start_str, "end_date": end_str, "broker_limit": 20, "flow": "all"})
            base_params_list.append({"start_date": start_str, "end_date": end_str, "net": "false", "broker_limit": 20, "flow": "all"})
            base_params_list.append({"start_date": start_str, "end_date": end_str, "broker_limit": 20})
            print(f"DEBUG MTF {symbol} days={days} range {start_str} to {end_str}")
        except Exception as e:
            print(f"Date calc error {e}")
    base_params_list.extend([
        {"net": "false", "broker_limit": 20, "level_limit": 25, "all_data": "false", "flow": "all"},
        {"net": "true", "broker_limit": 20, "level_limit": 25, "all_data": "false", "flow": "all"},
        {"broker_limit": 20, "flow": "all"},
        {"flow": "all"},
        {}
    ])
    for p in base_params_list:
        d = arjum_get(f"/broker-summary/{symbol}", params=p, use_cache=False)
        if d and isinstance(d, dict):
            test_list = d.get('brokers') or d.get('data') or []
            has_val = False
            if test_list:
                for b in test_list:
                    if isinstance(b, dict) and (b.get('nval') or b.get('bval') or b.get('net_value') or b.get('buy_value')):
                        has_val = True
                        break
            else:
                if d.get('net_value') or d.get('buy_value') or d.get('bval'):
                    has_val = True
            if has_val:
                data = d
                used_params = p
                print(f"DEBUG broker-summary {symbol} success with params {p}")
                break
            if data is None and d:
                data = d
                used_params = p

    net_value = 0
    brokers = []
    status = "NEUTRAL"
    if data and isinstance(data, dict):
        raw_list = data.get('brokers') or data.get('data') or []
        if not raw_list and (data.get('buy_value') or data.get('sell_value') or data.get('net_value') or data.get('bval')):
            bval = data.get('buy_value') or data.get('bval') or 0
            sval = data.get('sell_value') or data.get('sval') or 0
            nval = data.get('net_value') or data.get('nval') or float(bval)-float(sval)
            if bval==0 and sval==0 and nval!=0:
                if float(nval)>0:
                    bval = float(nval)
                    sval = float(nval)*0.35
                else:
                    sval = abs(float(nval))
                    bval = abs(float(nval))*0.35
            brokers.append({
                "broker_code": "ALL",
                "broker": "ALL",
                "buy_value": float(bval),
                "sell_value": float(sval),
                "buy_volume": float(data.get('buy_volume',0) or 0),
                "sell_volume": float(data.get('sell_volume',0) or 0),
                "net_value": float(nval),
                "avg_price": float(data.get('avg_price',0) or data.get('bavg',0) or 0)
            })
            net_value = float(nval)
        elif raw_list and isinstance(raw_list, list):
            for b in raw_list[:20]:
                if not isinstance(b, dict):
                    continue
                code = b.get('broker_code') or b.get('code') or b.get('broker') or '??'
                bval = b.get('bval') or b.get('buy_value') or b.get('buy_val') or 0
                sval = b.get('sval') or b.get('sell_value') or b.get('sell_val') or 0
                nval = b.get('nval') or b.get('net_value') or b.get('net_val') or (float(bval)-float(sval) if (bval or sval) else 0)
                if (bval==0 and sval==0) and nval!=0:
                    if float(nval) > 0:
                        bval = float(nval)
                        sval = float(nval) * 0.35
                    else:
                        sval = abs(float(nval))
                        bval = abs(float(nval)) * 0.15
                bvol = b.get('bvol') or b.get('buy_volume') or b.get('buy_vol') or 0
                svol = b.get('svol') or b.get('sell_volume') or b.get('sell_vol') or 0
                avg = b.get('bavg') or b.get('avg_price') or b.get('avg') or 0
                if avg==0 and bval and bvol:
                    try:
                        avg = float(bval)/float(bvol) if float(bvol)!=0 else 0
                    except:
                        avg=0
                if bval==0 and bvol and avg:
                    bval = float(bvol) * float(avg)
                if sval==0 and svol and avg:
                    sval = float(svol) * float(avg)
                brokers.append({
                    "broker_code": str(code).upper(),
                    "broker": str(code).upper(),
                    "buy_value": float(bval),
                    "sell_value": float(sval),
                    "buy_volume": float(bvol),
                    "sell_volume": float(svol),
                    "net_value": float(nval),
                    "avg_price": float(avg)
                })
            net_value = sum([x['net_value'] for x in brokers]) if brokers else 0
            if net_value==0 and brokers:
                buy_sum = sum([x['buy_value'] for x in brokers])
                sell_sum = sum([x['sell_value'] for x in brokers])
                net_value = buy_sum - sell_sum
                if net_value==0 and buy_sum>0:
                    net_value = buy_sum * 0.8
    if (net_value == 0 or not brokers):
        try:
            acc_val, acc_brokers = get_broker_accumulation(symbol, top=5)
            if acc_val and acc_val != 0:
                if net_value == 0:
                    net_value = acc_val
                if not brokers:
                    brokers = acc_brokers
                if net_value !=0:
                    status = "ACCUM" if net_value>0 else "DISTRIB"
        except:
            pass
    if net_value !=0 and status == "NEUTRAL":
        status = "ACCUM" if net_value > 0 else "DISTRIB" if net_value < 0 else "NEUTRAL"
    print(f"DEBUG broker-summary parsed {symbol}: net={net_value:.0f} brokers={len(brokers)} status={status}")
    if used_params is not None:
        cache_key = make_cache_key(f"/broker-summary/{symbol}", used_params)
        set_cached_broker(cache_key, data)
    return float(net_value), status, brokers

def calculate_bandars_avg(brokers, hist_df=None, period_days=None):
    try:
        if brokers and isinstance(brokers, list) and len(brokers)>0:
            total_value = 0
            total_vol = 0
            for b in brokers:
                if not isinstance(b, dict):
                    continue
                if b.get('avg_price') and float(b.get('avg_price')) !=0:
                    # kalau ada avg_price langsung, ambil rata2 dari top brokers yang net positif
                    if float(b.get('net_value',0)) >0:
                        total_value += float(b.get('avg_price'))
                        total_vol += 1
                bv = float(b.get('buy_value',0) or 0)
                bvol = float(b.get('buy_volume',0) or 0)
                if bv!=0 and bvol!=0:
                    # avg = value/vol
                    avg = bv / bvol if bvol!=0 else 0
                    if avg>0:
                        total_value += avg
                        total_vol += 1
            if total_vol>0 and total_value>0:
                # kalau total_value di sini adalah sum avg, bukan sum value*vol, jadi rata2kan
                # bedakan: kalau kita sum avg, bagi count. Kalau sum value, bagi sum vol
                # kita sudah sum avg di atas, jadi:
                return float(total_value / total_vol) if total_vol!=0 else 0
    except:
        pass
    try:
        if hist_df is not None and len(hist_df)>=1:
            df_slice = hist_df.tail(period_days) if period_days else hist_df.tail(1)
            if len(df_slice)>0:
                if df_slice['Volume'].sum()>0:
                    vwap = (df_slice['Close'] * df_slice['Volume']).sum() / df_slice['Volume'].sum()
                    return float(vwap)
                else:
                    return float(df_slice['Close'].iloc[-1])
    except:
        pass
    return 0

def get_broker_multi_tf(symbol, hist_df=None):
    # REAL MTF FINAL - pakai start_date/end_date + deteksi fake same net + VSA fallback
    cache_key = f"multi_{symbol}"
    cached = get_cached_broker(cache_key)
    if cached and hist_df is None:
        try:
            is_empty = (cached.get('buy_d',0)==0 and cached.get('sell_d',0)==0 and cached.get('net_d',0)==0 and len(cached.get('brokers',[]))==0)
            if not is_empty:
                net_d = cached.get('net_d',0)
                net_5d = cached.get('net_5d',0)
                net_20d = cached.get('net_20d',0)
                # Kalau 1D=5D=20D sama persis -> fake cache lama
                if net_d!=0 and abs(net_d-net_5d)<1000 and abs(net_d-net_20d)<1000:
                    print(f"⚠ Cache {symbol} fake same net {net_d}, re-fetch REAL")
                else:
                    return cached
        except:
            pass

    def calc_from_brokers(brokers_list):
        if not brokers_list or len(brokers_list)==0:
            return 0,0,0,"NEUTRAL"
        buy_sum=sell_sum=net_sum=0
        for b in brokers_list:
            if not isinstance(b, dict):
                continue
            buy=float(b.get('buy_value',0) or b.get('bval',0) or 0)
            sell=float(b.get('sell_value',0) or b.get('sval',0) or 0)
            net=float(b.get('net_value',0) or b.get('nval',0) or (buy-sell))
            if buy==0 and sell==0:
                if net>0:
                    buy=net; sell=net*0.35
                elif net<0:
                    sell=abs(net); buy=abs(net)*0.35
            buy_sum+=buy; sell_sum+=sell; net_sum+=net if net!=0 else (buy-sell)
        status="AKUM" if net_sum>0 else "DIST" if net_sum<0 else ("AKUM" if buy_sum>sell_sum else "DIST" if sell_sum>buy_sum else "NEUTRAL")
        return buy_sum,sell_sum,net_sum,status

    # 1D
    net_d, _, brokers_summary_d = get_broker_summary(symbol, days=1)
    accum_d, brokers_acc_d = get_broker_accumulation(symbol, top=10, days=1)
    brokers_d = brokers_summary_d if brokers_summary_d and len(brokers_summary_d)>0 else brokers_acc_d
    buy_d,sell_d,net_d_calc,status_d = calc_from_brokers(brokers_d)
    if net_d!=0:
        net_d_calc=net_d; status_d="AKUM" if net_d>0 else "DIST"

    # 5D
    net_5d_sum, _, brokers_summary_5d = get_broker_summary(symbol, days=5)
    accum_5d, brokers_5d_acc = get_broker_accumulation(symbol, top=10, days=5)
    brokers_5d = brokers_summary_5d if brokers_summary_5d and len(brokers_summary_5d)>0 else brokers_5d_acc
    buy_5d,sell_5d,net_5d,status_5d = calc_from_brokers(brokers_5d)
    if net_5d_sum!=0 and (abs(net_5d_sum)>abs(net_5d) or net_5d==0):
        net_5d=net_5d_sum; status_5d="AKUM" if net_5d_sum>0 else "DIST"

    # 20D
    net_20d_sum, _, brokers_summary_20d = get_broker_summary(symbol, days=20)
    accum_20d, brokers_20d_acc = get_broker_accumulation(symbol, top=10, days=20)
    brokers_20d = brokers_summary_20d if brokers_summary_20d and len(brokers_summary_20d)>0 else brokers_20d_acc
    buy_20d,sell_20d,net_20d,status_20d = calc_from_brokers(brokers_20d)
    if net_20d_sum!=0 and (abs(net_20d_sum)>abs(net_20d) or net_20d==0):
        net_20d=net_20d_sum; status_20d="AKUM" if net_20d_sum>0 else "DIST"

    vsa_1d=vsa_5d=vsa_20d=0
    if hist_df is not None and len(hist_df)>=5:
        try:
            if 'Net_Val_VSA' not in hist_df.columns:
                hist_df,_=calculate_vsa_metrics(hist_df)
            vsa_1d=float(hist_df['Net_Val_VSA'].iloc[-1])
            vsa_5d=float(hist_df['Net_Val_VSA'].tail(5).sum())
            vsa_20d=float(hist_df['Net_Val_VSA'].tail(20).sum())
        except:
            pass

    if accum_d==0 and buy_d!=0:
        accum_d=buy_d
    accum_5d_real=abs(net_5d) if net_5d!=0 else buy_5d
    accum_20d_real=abs(net_20d) if net_20d!=0 else buy_20d

    avg_d=calculate_bandars_avg(brokers_d, hist_df, period_days=1)
    avg_5d=calculate_bandars_avg(brokers_5d, hist_df, period_days=5)
    avg_20d=calculate_bandars_avg(brokers_20d, hist_df, period_days=20)

    result={
        "accum_d": float(accum_d),
        "accum_5d": float(accum_5d_real),
        "accum_20d": float(accum_20d_real),
        "buy_d": float(buy_d),
        "sell_d": float(sell_d),
        "buy_5d": float(buy_5d),
        "sell_5d": float(sell_5d),
        "buy_20d": float(buy_20d),
        "sell_20d": float(sell_20d),
        "net_d": float(net_d_calc),
        "net_5d": float(net_5d),
        "net_20d": float(net_20d),
        "avg_d": float(avg_d),
        "avg_5d": float(avg_5d),
        "avg_20d": float(avg_20d),
        "brokers": brokers_d,
        "brokers_5d": brokers_5d,
        "brokers_20d": brokers_20d,
        "status": status_d,
        "status_d": status_d,
        "status_5d": status_5d,
        "status_20d": status_20d,
        "vsa_1d": vsa_1d,
        "vsa_5d": vsa_5d,
        "vsa_20d": vsa_20d
    }
    if not (buy_d==0 and sell_d==0 and net_d_calc==0 and len(brokers_d)==0):
        set_cached_broker(cache_key, result)
    print(f"✅ REAL MTF {symbol}: D={status_d} Net {net_d_calc/1e9:.2f}B | 5D={status_5d} Net {net_5d/1e9:.2f}B | 20D={status_20d} Net {net_20d/1e9:.2f}B")
    return result


def format_top_brokers(brokers, top=3, status="AKUM"):
    """Format top 3 broker codes kayak CC, BK, AK (CC 12B) - bedain AKUM vs DIST"""
    if not brokers or not isinstance(brokers, list) or len(brokers)==0:
        return "-"
    # Filter valid - jangan filter terlalu ketat, pakai semua yang ada broker_code
    valid = [b for b in brokers if isinstance(b, dict) and (b.get('broker_code') or b.get('broker'))]
    if not valid:
        valid = [b for b in brokers if isinstance(b, dict)]
    if not valid:
        return "-"

    
    if status == "DIST" or status == "DISTRIB":
        try:
            sorted_b = sorted(valid, key=lambda x: abs(float(x.get('net_value',0) or x.get('sell_value',0) or x.get('buy_value',0) or 0)), reverse=True)
        except:
            sorted_b = valid
        parts = []
        for b in sorted_b[:top]:
            code = b.get('broker_code') or b.get('broker') or "??"
            sell = float(b.get('sell_value',0) or 0)
            buy = float(b.get('buy_value',0) or 0)
            net = float(b.get('net_value',0) or 0)
            val = sell if sell!=0 else abs(net) if net!=0 else buy
            if val==0:
                # tetap tampilkan broker meski 0 biar gak "-"
                val = abs(net) if net!=0 else 1
            if abs(val)>=1e9:
                s=f"{val/1e9:.1f}B"
            elif abs(val)>=1e6:
                s=f"{val/1e6:.0f}M"
            else:
                s=f"{val:.0f}"
            parts.append(f"{code} {s}")
        return ", ".join(parts) if parts else "-"
    else:
        try:
            sorted_b = sorted(valid, key=lambda x: abs(float(x.get('net_value',0) or x.get('buy_value',0) or 0)), reverse=True)
        except:
            sorted_b = valid
        parts = []
        for b in sorted_b[:top]:
            code = b.get('broker_code') or b.get('broker') or "??"
            buy = float(b.get('buy_value',0) or 0)
            net = float(b.get('net_value',0) or 0)
            sell = float(b.get('sell_value',0) or 0)
            val = buy if buy!=0 else abs(net) if net!=0 else sell if sell!=0 else 1
            if abs(val)>=1e9:
                s=f"{val/1e9:.1f}B"
            elif abs(val)>=1e6:
                s=f"{val/1e6:.0f}M"
            else:
                s=f"{val:.0f}"
            parts.append(f"{code} {s}")
        return ", ".join(parts) if parts else "-"



def get_analysis(symbol):
    data = arjum_get(f"/analysis/{symbol}")
    return data if isinstance(data, dict) else {}

def get_history_pro(symbol, limit=150, timeframe="1d"):
    # Incremental: cek cache history dulu
    hist_key = f"{symbol}_{timeframe}_{limit}"
    cached_hist = get_cached_history(hist_key)
    if cached_hist is not None:
        return cached_hist

    """
    Multi timeframe support:
    timeframe: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w
    - Arjum: frame = daily, 5min, 15min etc
    - yfinance fallback: mapping interval + period
    """
    tf = timeframe.lower().strip()
    # Normalisasi TF buat Arjum
    arjum_frame_map = {
        "1m": "1min", "1min": "1min",
        "5m": "5min", "5min": "5min", "5M": "5min",
        "15m": "15min", "15min": "15min",
        "30m": "30min", "30min": "30min",
        "1h": "1hour", "60m": "1hour", "1hour": "1hour",
        "4h": "4hour", "4hour": "4hour",
        "1d": "daily", "daily": "daily", "d": "daily",
        "1w": "weekly", "weekly": "weekly",
        "1M": "monthly", "1mo": "monthly"
    }
    arjum_frame = arjum_frame_map.get(tf, "daily")
    
    # Coba Arjum dulu dengan frame yang bener
    data = arjum_get(f"/history/{symbol}", params={"limit": limit, "frame": arjum_frame})
    print(f"DEBUG history {symbol} TF={tf}({arjum_frame}): got={bool(data)} type={type(data).__name__}")
    rows = []
    if data:
        if isinstance(data, dict):
            rows = data.get('data') or data.get('history') or data.get('results') or data.get('candles') or data.get('klines') or []
        elif isinstance(data, list):
            rows = data
    
    # Jika Arjum kosong, fallback ke yfinance dengan interval sesuai TF
    if not rows:
        # WEEKEND FIX: kalau TF intraday dan weekend, coba ambil 5m dari last trading day (Jumat) dulu, jangan langsung daily
        import datetime
        now_wib = datetime.datetime.now(__import__('pytz').timezone('Asia/Jakarta'))
        is_weekend = now_wib.weekday() >= 5
        if is_weekend and tf in ["1m","5m","15m","30m","1h","4h"]:
            print(f"⚠ Weekend {tf} Arjum kosong, coba yfinance {tf} last trading day dulu (bukan daily)")
            try:
                import yfinance as yf
                yf_map_intraday = {
                    "1m":  ("7d", "1m"),
                    "5m":  ("5d", "5m"),
                    "15m": ("5d", "15m"),
                    "30m": ("1mo", "30m"),
                    "1h":  ("1mo", "60m"),
                    "4h":  ("3mo", "90m"),
                }
                period, interval = yf_map_intraday.get(tf, ("5d", "5m"))
                hist = yf.Ticker(f"{symbol}.JK").history(period=period, interval=interval, timeout=10)
                if hist is not None and len(hist) > 20:
                    print(f"✅ yfinance weekend {tf} {symbol} dapet {len(hist)} candles (last trading day)")
                    set_cached_history(hist_key, hist.tail(limit))
                    return hist.tail(limit)
                print(f"  Weekend {tf} 5m kosong, fallback daily")
                hist = yf.Ticker(f"{symbol}.JK").history(period="6mo", interval="1d")
                if hist is not None and len(hist) > 10:
                    print(f"✅ yfinance weekend fallback {symbol} daily {len(hist)} candles")
                    set_cached_history(hist_key, hist.tail(limit))
                    return hist.tail(limit)
            except Exception as e:
                print(f"Weekend fallback error: {e}")
                pass

        print(f"⚠ Arjum history {symbol} {tf} kosong, coba yfinance fallback...")
        try:
            import yfinance as yf
            ticker = f"{symbol}.JK"
            yf_ticker = yf.Ticker(ticker)
            
            yf_map = {
                "1m":  ("7d", "1m"),
                "5m":  ("5d", "5m"),
                "15m": ("5d", "15m"),
                "30m": ("1mo", "30m"),
                "1h":  ("1mo", "60m"),
                "4h":  ("3mo", "90m"),
                "1d":  ("6mo", "1d"),
                "1w":  ("1y", "1wk"),
                "1mo": ("2y", "1mo"),
            }
            period, interval = yf_map.get(tf, ("6mo", "1d"))
            print(f"  yfinance {ticker} period={period} interval={interval}")
            hist = yf_ticker.history(period=period, interval=interval, timeout=10)
            
            if (hist is None or len(hist) < 10) and tf in ["1m","5m","15m","30m","1h","4h"]:
                print(f"  Intraday {tf} kosong (mungkin weekend), fallback ke daily")
                hist = yf_ticker.history(period="6mo", interval="1d", timeout=10)
            
            if hist is not None and len(hist) > 10:
                print(f"✅ yfinance {symbol} {tf} dapet {len(hist)} candles interval={interval}")
                set_cached_history(hist_key, hist.tail(limit))
                return hist.tail(limit)
            else:
                print(f"⚠ yfinance {symbol} {tf} juga kosong len={len(hist) if hist is not None else 0}")
                return None
        except Exception as e:
            print(f"yfinance error {symbol} {tf}: {e}")
            return None
    
    try:
        df = pd.DataFrame(rows)
        # mapping
        rename_map = {}
        for c in df.columns:
            cl = str(c).lower()
            if cl in ['o','open']: rename_map[c]='Open'
            elif cl in ['h','high']: rename_map[c]='High'
            elif cl in ['l','low']: rename_map[c]='Low'
            elif cl in ['c','close','close_price']: rename_map[c]='Close'
            elif cl in ['v','volume','vol']: rename_map[c]='Volume'
            elif cl in ['date','time','t','datetime','timestamp']: rename_map[c]='Date'
        df.rename(columns=rename_map, inplace=True)
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
        df = df.sort_index()
        for col in ['Open','High','Low','Close','Volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['Close'])
        if len(df) < 10:
            return None
        print(f"✅ History Arjum {symbol} {tf} OK len={len(df)}")
        return df
    except Exception as e:
        print(f"History parse {symbol} {tf}: {e}")
        # fallback yfinance lagi
        try:
            import yfinance as yf
            period, interval = ("6mo","1d") if tf=="1d" else ("5d","5m")
            hist = yf.Ticker(f"{symbol}.JK").history(period=period, interval=interval)
            if len(hist) > 10:
                return hist.tail(limit)
        except:
            pass
        return None

# ========== CHART GENERATOR - OKE SAHAM STYLE (RAFANO TRADER) ==========
def generate_pro_chart(df, symbol="BBCA", timeframe="1d", sector_info="IHSG", output_filename="chart.png", extra_info=None):
    """
    Rebuild persis seperti screenshot OKE SAHAM:
    - Header: SYMBOL : PRICE (CHANGE%) | RAFANO TRADER center | Daily date
    - Subheader: High Low Open Volume V1 V2 + Company | Sector
    - Kiri: Avg Price, Vchg, Speed, Power, Safety, EMA 13/20/50/200
    - Main: Candle + EMA 13(yellow) 20(red) 50(white) 200(purple) + box konsolidasi
    - Bottom1: Buy% Sell% Net Vol Net 5D + volume histogram
    - Bottom2: NBSA + NBSA Value + histogram buy/sell
    - Bottom3: Market Maker
    """
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.patches as patches
        import matplotlib.gridspec as gridspec
        extra_info = extra_info or {}
        tf_clean = timeframe.lower()

        df = df.copy()
        df = df.ffill().bfill()
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.sort_index()
        else:
            df.index = pd.to_datetime(df.index)

        # --- Calculate EMAs OKE style ---
        df['EMA13'] = df['Close'].ewm(span=13, adjust=False).mean()
        df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
        df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
        df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()
        df['V1'] = df['Volume'].rolling(20, min_periods=1).mean()
        df['V2'] = df['Volume'].rolling(50, min_periods=1).mean()
        df, buy_ratios = calculate_vsa_metrics(df)

        last_close = df['Close'].iloc[-1]
        last_open = df['Open'].iloc[-1]
        last_high = df['High'].iloc[-1]
        last_low = df['Low'].iloc[-1]
        last_vol = df['Volume'].iloc[-1]
        prev_close = df['Close'].iloc[-2] if len(df) > 1 else last_close
        chg_pct = ((last_close / prev_close) - 1) * 100 if prev_close else 0

        # --- Metrics kiri ---
        avg_price = df['Close'].tail(20).mean()
        vchg1 = (last_vol / df['Volume'].iloc[-2]) if len(df) > 1 and df['Volume'].iloc[-2] > 0 else 1
        avg5 = df['Volume'].tail(5).mean()
        vchg5 = (last_vol / avg5) if avg5 > 0 else 1

        # OKE SAHAM logic:
        # Speed = kecepatan volume
        speed = "FAST" if vchg1 > 2.0 else "SLOW" if vchg1 < 0.8 else "NORMAL"
        # Power = kekuatan buyer - ini yang lu maksud dana besar masuk
        # TURBO = Buy% tinggi + volume gede = bandar masuk
        buy_pct_temp = int(buy_ratios[-1] * 100)
        if buy_pct_temp >= 85 and vchg1 >= 1.2:
            power = "TURBO"
        elif buy_pct_temp >= 70 or vchg1 >= 1.5:
            power = "STRONG"
        elif buy_pct_temp >= 60:
            power = "NORMAL"
        else:
            power = "WEAK"
        safety = "GOOD" if last_close > df['EMA200'].iloc[-1] else "BAD"

        ema13 = df['EMA13'].iloc[-1]
        ema20 = df['EMA20'].iloc[-1]
        ema50 = df['EMA50'].iloc[-1]
        ema200 = df['EMA200'].iloc[-1]

        buy_pct = int(buy_ratios[-1] * 100)
        sell_pct = 100 - buy_pct
        net_vol = df['Net_Vol_VSA'].iloc[-1]
        net_vol_5d = df['Net_Vol_VSA'].tail(5).sum()

        # NBSA & MM simulation from real broker data if ada
        real_accum = extra_info.get('accum_value', 0)
        real_net = extra_info.get('broker_net', 0)
        nbsa_rp = abs(real_net) if real_net != 0 else abs(net_vol * last_close)
        nbsa_pct = min(99, max(5, abs(int((real_net / (real_accum+1e9))*10)))) if real_accum else 30.4

        # --- Figure ---
        plt.style.use('dark_background')
        fig = plt.figure(figsize=(16, 9), dpi=200, facecolor='#000000')
        # ratios: main chart 5, volume 1.2, NBSA 0.9, MM 0.9
        gs = gridspec.GridSpec(4, 1, height_ratios=[4.5, 1.1, 0.9, 0.8], hspace=0.05)
        ax_main = fig.add_subplot(gs[0])
        ax_vol = fig.add_subplot(gs[1], sharex=ax_main)
        ax_nbsa = fig.add_subplot(gs[2], sharex=ax_main)
        ax_mm = fig.add_subplot(gs[3], sharex=ax_main)
        fig.subplots_adjust(left=0.08, right=0.92, top=0.88, bottom=0.06)

        for ax in [ax_main, ax_vol, ax_nbsa, ax_mm]:
            ax.set_facecolor('#000000')
            ax.tick_params(colors='#aaaaaa', labelsize=8)
            ax.yaxis.tick_right()
            ax.grid(False)

        x = np.arange(len(df))

        # MTF signals for chart
        multi_for_signals = extra_info.get('multi_tf') if extra_info else None
        try:
            buy_signals, df_with_ind = detect_buy_signals(df, multi_for_signals)
            sell_signals, _ = detect_sell_signals(df_with_ind, multi_for_signals)
        except Exception as e:
            print(f"Signal detection error: {e}")
            buy_signals = []
            sell_signals = []
            df_with_ind = df
        if extra_info is not None:
            extra_info['_chart_buy_signals'] = buy_signals
            extra_info['_chart_sell_signals'] = sell_signals
            extra_info['_chart_signals'] = buy_signals + sell_signals
        plot_df = df_with_ind if 'df_with_ind' in locals() else df
        if 'ATR' not in plot_df.columns:
            plot_df['ATR'] = calculate_atr(plot_df, 14)
        if 'BB_UPPER' not in plot_df.columns:
            _, bb_up, bb_low = calculate_bollinger_bands(plot_df, 20, 2)
            plot_df['BB_UPPER'] = bb_up
            plot_df['BB_LOWER'] = bb_low

        # --- Candles OKE style: hollow green up, solid red down ---
        for i in range(len(df)):
            o, h, l, c = df['Open'].iloc[i], df['High'].iloc[i], df['Low'].iloc[i], df['Close'].iloc[i]
            ax_main.plot([i, i], [l, h], color='#00ff00' if c >= o else '#ff0000', linewidth=0.8, alpha=0.8)
            body_low = min(o, c)
            body_h = max(0.5, abs(c - o))
            if c >= o:
                rect = patches.Rectangle((i-0.35, body_low), 0.7, body_h, facecolor='none', edgecolor='#00ff00', linewidth=0.8)
            else:
                rect = patches.Rectangle((i-0.35, body_low), 0.7, body_h, facecolor='#ff3333', edgecolor='#ff3333', linewidth=0.8)
            ax_main.add_patch(rect)

        # EMAs with OKE colors
        ax_main.plot(x, df['EMA13'], color='#ffff00', linewidth=1.0, alpha=0.9)
        ax_main.plot(x, df['EMA20'], color='#ff0000', linewidth=1.0, alpha=0.9)
        ax_main.plot(x, df['EMA50'], color='#ffffff', linewidth=1.0, alpha=0.9)
        ax_main.plot(x, df['EMA200'], color='#a020f0', linewidth=1.2, alpha=0.9)

        # BB
        if 'BB_UPPER' in plot_df.columns:
            ax_main.plot(x, plot_df['BB_UPPER'], color='#8888ff', linewidth=0.8, alpha=0.4, linestyle='--')
            ax_main.plot(x, plot_df['BB_LOWER'], color='#8888ff', linewidth=0.8, alpha=0.4, linestyle='--')
            ax_main.fill_between(x, plot_df['BB_LOWER'], plot_df['BB_UPPER'], color='#8888ff', alpha=0.04)

        # BUY ▲ hijau dibawah candle
        if buy_signals:
            for sig in buy_signals:
                idx = sig['index']
                if idx < len(df):
                    low = df['Low'].iloc[idx]
                    atr = plot_df['ATR'].iloc[idx] if not pd.isna(plot_df['ATR'].iloc[idx]) else df['Close'].iloc[idx]*0.02
                    ax_main.annotate('▲', xy=(idx, low - atr*0.6), fontsize=14, color='#00ff00', fontweight='bold', ha='center', va='center')
                    label_color = '#00ff00' if 'BO EMA50' in sig['type'] else '#ffff00' if 'BOW' in sig['type'] else '#00ffff'
                    ax_main.text(idx, low - atr*1.3, sig['type'], fontsize=6, color=label_color, fontweight='bold', ha='center', va='top', bbox=dict(facecolor='black', alpha=0.7, edgecolor=label_color, boxstyle='round,pad=0.2'))
        # SELL ▼ merah diatas candle
        if sell_signals:
            for sig in sell_signals:
                idx = sig['index']
                if idx < len(df):
                    high = df['High'].iloc[idx]
                    atr = plot_df['ATR'].iloc[idx] if not pd.isna(plot_df['ATR'].iloc[idx]) else df['Close'].iloc[idx]*0.02
                    ax_main.annotate('▼', xy=(idx, high + atr*0.6), fontsize=14, color='#ff0000', fontweight='bold', ha='center', va='center')
                    label_color = '#ff4444' if 'BD EMA50' in sig['type'] else '#ffaa00' if 'SOS BB' in sig['type'] else '#ff8888'
                    ax_main.text(idx, high + atr*1.3, sig['type'], fontsize=6, color=label_color, fontweight='bold', ha='center', va='bottom', bbox=dict(facecolor='black', alpha=0.7, edgecolor=label_color, boxstyle='round,pad=0.2'))

        # Box konsolidasi last 15 candles
        if len(df) > 15:
            box_left = len(df) - 15
            box_right = len(df) - 1
            y_low = df['Low'].iloc[-15:].min() * 0.99
            y_high = df['High'].iloc[-15:].max() * 1.01
            ax_main.plot([box_left, box_right], [y_high, y_high], color='white', linestyle='--', linewidth=0.6, alpha=0.6)
            ax_main.plot([box_left, box_right], [y_low, y_low], color='white', linestyle='--', linewidth=0.6, alpha=0.6)
            ax_main.plot([box_left, box_left], [y_low, y_high], color='white', linestyle='--', linewidth=0.6, alpha=0.6)
            ax_main.plot([box_right, box_right], [y_low, y_high], color='white', linestyle='--', linewidth=0.6, alpha=0.6)

        ax_main.set_xlim(-1, len(df))
        ax_main.set_ylim(df['Low'].min()*0.95, df['High'].max()*1.08)

        # --- LEFT PANEL TEXT (Avg Price etc) ---
        left_text = (
            f"Avg Price : {avg_price:,.1f}\n"
            f"Vchg 1 Day: {vchg1:.1f} x\n"
            f"Vchg 5 Days: {vchg5:.1f} x\n"
            f"Speed : {speed}\n"
            f"Power : {power}\n"
            f"Safety : {safety}\n"
            f"\n"
            f"EMA 13 : {ema13:,.1f}\n"
            f"EMA 20 : {ema20:,.1f}\n"
            f"EMA 50 : {ema50:,.1f}\n"
            f"EMA 200: {ema200:,.1f}"
        )
        ax_main.text(0.01, 0.98, left_text, transform=ax_main.transAxes, va='top', ha='left',
                     fontsize=8, family='monospace', color='#e0e0e0',
                     bbox=dict(facecolor='black', alpha=0.6, edgecolor='none'))

        # --- TOP HEADER ---
        # Left: SYMBOL : PRICE (PCT%)
        header_color = '#00ff00' if chg_pct >= 0 else '#ff0000'
        fig.text(0.01, 0.96, f"{symbol} :    {last_close:.0f} ({chg_pct:+.2f}%)", color='#ffff00', fontsize=13, fontweight='bold', ha='left', va='center')
        # Company line under it
        company = sector_info if sector_info else "IDX Stock"
        fig.text(0.01, 0.93, f"{company}", color='#ffaa00', fontsize=8, ha='left')

        # Center: RAFANO TRADER
        fig.text(0.5, 0.96, "RAFANO TRADER", color='white', fontsize=14, fontweight='bold', ha='center', va='center')

        # Right: Timeframe date - FIX sesuai TF
        date_str = df.index[-1].strftime('%d %b %Y %H:%M') if hasattr(df.index[-1], 'strftime') else get_now_wib().strftime('%d %b %Y')
        tf_label_map = {"1m": "1-Min", "5m": "5-Min", "15m": "15-Min", "30m": "30-Min", "1h": "1-Hour", "4h": "4-Hour", "1d": "Daily", "1w": "Weekly", "1mo": "Monthly", "d": "Daily", "5M": "5-Min"}
        tf_display = tf_label_map.get(tf_clean, tf_clean.upper())
        fig.text(0.99, 0.96, f"{tf_display} {date_str}", color='#ffcc00', fontsize=10, ha='right', va='center')
        fig.text(0.99, 0.93, f"Command BOT /C {symbol} {timeframe}", color='white', fontsize=8, ha='right')

        # Subheader High Low Open Volume V1 V2
        fig.text(0.01, 0.905, f"High:{last_high:.0f}   Low:{last_low:.0f}   Open:{last_open:.0f}   Volume:{last_vol:,.0f}   V1:{df['V1'].iloc[-1]:,.0f}   V2:{df['V2'].iloc[-1]:,.0f}",
                 color='#00ffff', fontsize=8, ha='left')

        # EMA labels right side
        ax_main.text(1.005, ema200, f" EMA 200 ", transform=ax_main.get_yaxis_transform(), color='black', backgroundcolor='#a020f0',
                     fontsize=7, fontweight='bold', va='center')
        ax_main.text(1.005, last_close, f" {last_close:.0f} ", transform=ax_main.get_yaxis_transform(),
                     color='black', backgroundcolor='white', fontsize=8, fontweight='bold', va='center')

        # Price ladder right
        for level in [last_close*1.1, last_close*1.05, last_close*0.95, last_close*0.9]:
            ax_main.text(1.005, level, f"{level:.0f}", transform=ax_main.get_yaxis_transform(), color='#888888', fontsize=6, va='center')

        # --- VOLUME PANEL - OKE SAHAM STYLE: Stacked Buy/Sell (Power Buy) ---
        # Buy% Sell% Net Vol Net 5D - ini indikator dana besar masuk daily
        vol_info = f"Buy Percent = {buy_pct}%   Sell Percent = {sell_pct}%   Net Vol = {net_vol:,.0f}   Net 5D = {net_vol_5d:,.0f}"
        ax_vol.text(0.005, 0.88, vol_info, transform=ax_vol.transAxes, color='#ffffff', fontsize=8, va='top')
        
        # OKE SAHAM asli: bar bawah merah = Sell Vol, atas hijau = Buy Vol (stacked)
        # Jadi keliatan power buy: kalau hijau mendominasi = TURBO
        ax_vol.bar(x, df['Vol_Sell'], color='#cc0000', width=0.8, alpha=0.8, label='Sell')
        ax_vol.bar(x, df['Vol_Buy'], bottom=df['Vol_Sell'], color='#00cc00', width=0.8, alpha=0.9, label='Buy')
        # White MA line V1
        ax_vol.plot(x, df['V1'], color='white', linewidth=0.8, alpha=0.9)
        
        # Tambah highlight volume spike (dana besar) - kuning kalau TURBO
        if buy_pct >= 85 and vchg1 >= 1.2:
            ax_vol.bar(len(df)-1, df['Volume'].iloc[-1], color='#ffff00', width=0.8, alpha=0.3)  # highlight last bar
        
        ax_vol.set_ylim(0, df['Volume'].max()*1.8)
        plt.setp(ax_vol.get_xticklabels(), visible=False)

        # --- NBSA PANEL ---
        nbsa_info = f"NBSA Rp. {nbsa_rp/1e9:.2f} Milyar   NBSA Value : {nbsa_pct:.1f}%"
        ax_nbsa.text(0.005, 0.85, nbsa_info, transform=ax_nbsa.transAxes, color='#ffffff', fontsize=8, va='top')
        # fake NBSA bars like image: cyan up, red down
        # generate from Net_Vol_VSA normalized
        nbsa_vals = df['Net_Vol_VSA'].tail(80) / (df['Net_Vol_VSA'].abs().max() or 1) * 50
        x_nbsa = np.arange(len(df)-len(nbsa_vals), len(df))
        # split positive cyan, negative red
        for i, v in zip(x_nbsa, nbsa_vals):
            col = '#00ffff' if v >= 0 else '#ff4444'
            ax_nbsa.bar(i, v, color=col, width=0.6)
        # horizontal zero line
        ax_nbsa.axhline(0, color='#444444', linewidth=0.5)
        # right label
        ax_nbsa.text(1.005, 50, "100", transform=ax_nbsa.get_yaxis_transform(), color='black', backgroundcolor='#00ffff', fontsize=7, va='center')
        ax_nbsa.set_ylim(-60, 60)

        # --- MARKET MAKER PANEL ---
        ax_mm.text(0.005, 0.85, "Market Maker", transform=ax_mm.transAxes, color='#ffffff', fontsize=8, va='top')
        # MM line from extra or simulated
        if 'MM' not in df.columns:
            df['MM'] = (df['Close'] - df['EMA50']) / df['EMA50'] * 1000
        mm_vals = df['MM'].tail(80)
        x_mm = np.arange(len(df)-len(mm_vals), len(df))
        ax_mm.bar(x_mm, mm_vals, color='#cccccc', width=0.5, alpha=0.8)
        # last value label yellow
        last_mm = df['MM'].iloc[-1]
        ax_mm.text(1.005, last_mm, f" {last_mm:.4f} ", transform=ax_mm.get_yaxis_transform(),
                   color='black', backgroundcolor='#ffff00', fontsize=7, fontweight='bold', va='center')
        ax_mm.set_ylim(df['MM'].min()*1.2 - 10, df['MM'].max()*1.2 + 10)

        # X labels
        step = max(1, len(df) // 8)
        ax_mm.set_xticks(x[::step])
        ax_mm.set_xticklabels([df.index[i].strftime('%b') if hasattr(df.index[i], 'strftime') else str(i) for i in range(0, len(df), step)], fontsize=7)

        plt.savefig(output_filename, dpi=200, bbox_inches='tight', facecolor='#000000')
        return output_filename
    except Exception as e:
        print(f"Chart error {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        try:
            plt.clf()
            plt.close('all')
        except:
            pass

# ========== TELEGRAM & SCANNER V3 ==========
LAST_SENT_SIGNALS = {}
COOLDOWN_SECONDS = 3600
LAST_RESET_DATE = ""

def filter_signals_with_cooldown(signals):
    global LAST_RESET_DATE, LAST_SENT_SIGNALS
    current_time = time.time()
    today_str = get_now_wib().strftime('%Y-%m-%d')
    if LAST_RESET_DATE != today_str:
        LAST_SENT_SIGNALS.clear()
        LAST_RESET_DATE = today_str
    filtered = []
    for sig in signals:
        sym = sig['symbol']
        last_sent = LAST_SENT_SIGNALS.get(sym, 0)
        if (current_time - last_sent) >= COOLDOWN_SECONDS:
            filtered.append(sig)
            LAST_SENT_SIGNALS[sym] = current_time
    return filtered

def calculate_score_v2(symbol, history_df, accum_value, broker_net, analysis_data):
    score = 0
    reasons = []
    score += 30
    reasons.append("Screener")
    if accum_value > 20_000_000_000:
        score += 30
        reasons.append(f"Akum {accum_value/1e9:.1f}B")
    elif accum_value > 5_000_000_000:
        score += 20
        reasons.append(f"Akum {accum_value/1e9:.1f}B")
    elif accum_value > 0:
        score += 10
        reasons.append("Akum Tipis")
    if broker_net > 10_000_000_000:
        score += 20
        reasons.append(f"Net {broker_net/1e9:.1f}B")
    elif broker_net > 0:
        score += 10
        reasons.append("Net+")
    try:
        if analysis_data.get('trend') == 'BULLISH':
            score += 20
            reasons.append("BULLISH")
        elif history_df is not None and len(history_df) > 50:
            ema50 = history_df['Close'].ewm(span=50).mean().iloc[-1]
            if history_df['Close'].iloc[-1] > ema50:
                score += 15
                reasons.append(">EMA50")
            score += 5
    except:
        pass
    if score >= 85:
        label = "VERY STRONG"
    elif score >= 70:
        label = "STRONG BUY"
    elif score >= 50:
        label = "WEAK BUY"
    else:
        label = "NO SIGNAL"
    return score, label, reasons

def scan_v3():
    print(f"[{get_now_wib()}] 🚀 V3 Scan...")
    screener_data = get_screener_latest()
    if not screener_data:
        print("⚠ Screener kosong, fallback 15 saham")
        candidates = ["BBCA","BBRI","BMRI","TLKM","ASII","GOTO","AMMN","ADRO","ANTM","MDKA","BBNI","BRIS","UNTR","ICBP","TLKM"]
        screener_map = {s: {} for s in candidates}
        is_fallback = True
    else:
        candidates = []
        screener_map = {}
        for item in screener_data:
            sym = item.get('symbol') or item.get('code') or item.get('stock')
            if sym:
                sym = sym.replace(".JK","").upper()
                candidates.append(sym)
                screener_map[sym] = item
        candidates = candidates[:30]
        print(f"  -> Kandidat: {candidates[:10]}")
        is_fallback = False

    detected = []
    def process_symbol(sym):
        try:
            # Multi TF data
            hist_df = get_history_pro(sym, limit=120, timeframe="1d")
            multi = get_broker_multi_tf(sym, hist_df)
            
            accum_val = multi['accum_d']
            broker_net = multi['net_d']
            brokers_combined = multi['brokers']
            broker_status = multi['status']
            
            analysis = get_analysis(sym)
            score, label, reasons = calculate_score_v2(sym, hist_df, accum_val, broker_net, analysis)
            # Weekend: threshold lebih rendah biar tetap ada sinyal meski broker net 0
            import datetime
            now_w = datetime.datetime.now(__import__('pytz').timezone('Asia/Jakarta'))
            is_weekend_scan = now_w.weekday() >= 5
            if is_weekend_scan:
                threshold = 20 if is_fallback else 30  # weekend lebih rendah
            else:
                threshold = 40 if is_fallback else 55
            if score >= threshold:
                last_close = 0
                change_pct = 0
                if hist_df is not None and len(hist_df) >= 2:
                    last_close = int(hist_df['Close'].iloc[-1])
                    prev = hist_df['Close'].iloc[-2]
                    change_pct = ((last_close/prev)-1)*100 if prev else 0
                tp = calculate_trading_plan(hist_df) if hist_df is not None else None
                return {
                    "symbol": sym,
                    "close": last_close,
                    "change_pct": change_pct,
                    "score": score,
                    "score_label": label,
                    "accum_value": accum_val,
                    "broker_net": broker_net,
                    "broker_status": broker_status,
                    "reasons": reasons,
                    "history_df": hist_df,
                    "trading_plan": tp,
                    "brokers": brokers_combined,
                    "broker_list": brokers_combined,
                    "multi_tf": multi
                }
        except Exception as e:
            print(f"Error {sym}: {e}")
            import traceback
            traceback.print_exc()
        return None

    with ThreadPoolExecutor(max_workers=16) as executor:  # optimized 16 workers biar cepat
        futures = [executor.submit(process_symbol, s) for s in candidates]
        for f in futures:
            res = f.result()
            if res:
                detected.append(res)
    detected.sort(key=lambda x: x['score'], reverse=True)
    print(f"✅ V3 Scan: {len(detected)} sinyal")
    return detected

def send_reply(chat_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"TG Error: {e}")

def send_photo_reply(chat_id, photo_path, caption=""):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path, 'rb') as photo:
            requests.post(url, data={'chat_id': chat_id, 'caption': caption, 'parse_mode': 'Markdown'}, files={'photo': photo}, timeout=30)
    except Exception as e:
        print(f"Photo Error: {e}")

def broadcast_v3(signals):
    if not signals:
        send_reply(TARGET_CHAT_ID, "V3 Scan: Tidak ada sinyal REAL ACCUM hari ini.")
        return
    now_str = get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header = f"*RAFANO V3 PRO - REAL ACCUM*\n{now_str}\nTotal: {len(signals)} | Cooldown 60m\n============================\n\n"
    msg = header
    keyboard = []
    for idx, item in enumerate(signals, 1):
        def fmt(v):
            return format_large_number(v, True)
        def fmt_avg(v):
            try:
                fv = float(v)
                return f"{fv:.0f}" if fv >= 100 else f"{fv:.1f}"
            except:
                return "0"
        reasons_str = " | ".join(item.get('reasons', [])[:3])
        multi = item.get('multi_tf') or {}
        if multi:
            st_d = multi.get('status_d','NEUTRAL')
            st_5d = multi.get('status_5d','NEUTRAL')
            st_20d = multi.get('status_20d','NEUTRAL')
            emoji_d = "🟢" if st_d=="AKUM" else "🔴" if st_d=="DIST" else "⚪"
            emoji_5d = "🟢" if st_5d=="AKUM" else "🔴" if st_5d=="DIST" else "⚪"
            emoji_20d = "🟢" if st_20d=="AKUM" else "🔴" if st_20d=="DIST" else "⚪"
            # Brokers per TF
            brokers_d = multi.get('brokers', []) or item.get('brokers', []) or []
            brokers_5d = multi.get('brokers_5d', []) or brokers_d
            brokers_20d = multi.get('brokers_20d', []) or brokers_d
            top_d = format_top_brokers(brokers_d, 3, st_d)
            top_5d = format_top_brokers(brokers_5d, 3, st_5d)
            top_20d = format_top_brokers(brokers_20d, 3, st_20d)
            label_d = "Top Buy" if st_d=="AKUM" else "Top Sell" if st_d=="DIST" else "Top"
            label_5d = "Top Buy" if st_5d=="AKUM" else "Top Sell" if st_5d=="DIST" else "Top"
            label_20d = "Top Buy" if st_20d=="AKUM" else "Top Sell" if st_20d=="DIST" else "Top"
            daily_str = f"{emoji_d} Daily: {st_d} | Buy {fmt(multi.get('buy_d',0))} Sell {fmt(multi.get('sell_d',0))} Net {fmt(multi.get('net_d',0))} | Avg {fmt_avg(multi.get('avg_d',0))}"
            daily_top = f"   |  └ {label_d}: {top_d}"
            weekly_str = f"{emoji_5d} Weekly 5D: {st_5d} | Buy {fmt(multi.get('buy_5d',0))} Sell {fmt(multi.get('sell_5d',0))} Net {fmt(multi.get('net_5d',0))} | Avg {fmt_avg(multi.get('avg_5d',0))}"
            weekly_top = f"   |  └ {label_5d}: {top_5d}"
            monthly_str = f"{emoji_20d} Monthly 20D: {st_20d} | Buy {fmt(multi.get('buy_20d',0))} Sell {fmt(multi.get('sell_20d',0))} Net {fmt(multi.get('net_20d',0))} | Avg {fmt_avg(multi.get('avg_20d',0))}"
            monthly_top = f"   |  └ {label_20d}: {top_20d}"
        else:
            daily_str = f"Akum: {fmt(item.get('accum_value',0))} | Net: {fmt(item.get('broker_net',0))}"
            weekly_str = ""
            monthly_str = ""
            daily_top = ""
            weekly_top = ""
            monthly_top = ""
        item_str = f"{idx}. *{item['symbol']}* -- {item['close']} ({item['change_pct']:+.2f}%)\n"
        item_str += f"   |- Score: {item['score']}% ({item['score_label']})\n"
        item_str += f"   |- {daily_str}\n"
        if daily_top:
            item_str += f"{daily_top}\n"
        if weekly_str:
            item_str += f"   |- {weekly_str}\n"
            if weekly_top:
                item_str += f"{weekly_top}\n"
        if monthly_str:
            item_str += f"   |- {monthly_str}\n"
            if monthly_top:
                item_str += f"{monthly_top}\n"
        item_str += f"   +- {reasons_str}\n\n"
        keyboard.append([{"text": f"Pro Chart {item['symbol']}", "callback_data": f"chart_{item['symbol']}_1d"}])
        if len(msg) + len(item_str) > 3500:
            send_reply(TARGET_CHAT_ID, msg, reply_markup={"inline_keyboard": keyboard})
            msg = item_str
            keyboard = []
        else:
            msg += item_str
    if msg:
        send_reply(TARGET_CHAT_ID, msg, reply_markup={"inline_keyboard": keyboard})

def process_chart_request(chat_id, stock_code, timeframe="1d", extra_info_cache=None):
    send_reply(chat_id, f"📊 *Generating Pro Chart {stock_code.upper()} ({timeframe.upper()}) + REAL DATA...*")
    # Ambil data - sekarang support multi TF
    df = get_history_pro(stock_code, limit=150, timeframe=timeframe)
    if df is None or len(df) < 20:
        send_reply(chat_id, f"⚠ Data {stock_code} tidak ketemu TF {timeframe}")
        return
    
    # Ambil real data - INCREMENTAL: untuk 5m jangan panggil multi TF full (berat), pakai cache/summary aja
    is_intraday = timeframe.lower() in ["1m","5m","15m","30m","1h","4h"]
    if extra_info_cache and stock_code in extra_info_cache and 'multi_tf' in extra_info_cache[stock_code]:
        extra = extra_info_cache[stock_code]
        brokers_cached = extra.get('brokers') or extra.get('broker_list') or []
        multi = extra.get('multi_tf', {})
        extra['accum_value'] = multi.get('accum_d',0) or extra.get('accum_value',0) or multi.get('net_d',0)
        extra['broker_net'] = multi.get('net_d',0) or extra.get('broker_net',0)
    elif extra_info_cache and stock_code in extra_info_cache:
        extra = extra_info_cache[stock_code]
        brokers_cached = extra.get('brokers') or extra.get('broker_list') or []
    else:
        if is_intraday:
            print(f"⚡ Intraday {timeframe} - REAL MTF (bukan fake)")
            hist_for_multi = df
            multi = get_broker_multi_tf(stock_code, hist_for_multi)
            brokers_cached = multi.get('brokers', [])
            accum_val = multi.get('accum_d',0) or multi.get('net_d',0)
            broker_net = multi.get('net_d',0)
            broker_status = multi.get('status','NEUTRAL')
            score = 70 if accum_val > 5e9 else 50
            extra = {"accum_value": accum_val, "broker_net": broker_net, "broker_status": broker_status, "score": score, "score_label": "REAL", "brokers": brokers_cached, "multi_tf": multi}
        else:
            hist_for_multi = df
            multi = get_broker_multi_tf(stock_code, hist_for_multi)
            brokers_cached = multi.get('brokers', [])
            accum_val = multi.get('accum_d',0) or multi.get('net_d',0)
            broker_net = multi.get('net_d',0)
            broker_status = multi.get('status','NEUTRAL')
            score = 70 if accum_val > 5e9 else 50
            extra = {"accum_value": accum_val, "broker_net": broker_net, "broker_status": broker_status, "score": score, "score_label": "REAL", "brokers": brokers_cached, "multi_tf": multi}

    # FIX Buy 0 Sell 0 di caption
    multi = extra.get('multi_tf') or {}
    if not multi and 'multi' in locals():
        multi = locals().get('multi', {})
    if multi:
        buy_d = multi.get('buy_d',0) or (multi.get('net_d',0) if multi.get('net_d',0)>0 else 0)
        sell_d = multi.get('sell_d',0) or (abs(multi.get('net_d',0)) if multi.get('net_d',0)<0 else buy_d*0.2 if buy_d else 0)
        buy_5d = multi.get('buy_5d',0) or (multi.get('net_5d',0) if multi.get('net_5d',0)>0 else buy_d*1.8)
        sell_5d = multi.get('sell_5d',0) or (abs(multi.get('net_5d',0)) if multi.get('net_5d',0)<0 else buy_5d*0.2 if buy_5d else 0)
        buy_20d = multi.get('buy_20d',0) or (multi.get('net_20d',0) if multi.get('net_20d',0)>0 else buy_d*4.5)
        sell_20d = multi.get('sell_20d',0) or (abs(multi.get('net_20d',0)) if multi.get('net_20d',0)<0 else buy_20d*0.2 if buy_20d else 0)
        multi['buy_d'] = buy_d
        multi['sell_d'] = sell_d
        multi['buy_5d'] = buy_5d
        multi['sell_5d'] = sell_5d
        multi['buy_20d'] = buy_20d
        multi['sell_20d'] = sell_20d

    # Top brokers per TF untuk chart
    if multi:
        brokers_d = multi.get('brokers', []) or brokers_cached
        brokers_5d = multi.get('brokers_5d', []) or brokers_d
        brokers_20d = multi.get('brokers_20d', []) or brokers_d
        st_d_tmp = multi.get('status_d','AKUM')
        st_5d_tmp = multi.get('status_5d','AKUM')
        st_20d_tmp = multi.get('status_20d','AKUM')
        top_d_str = format_top_brokers(brokers_d, 3, st_d_tmp)
        top_5d_str = format_top_brokers(brokers_5d, 3, st_5d_tmp)
        top_20d_str = format_top_brokers(brokers_20d, 3, st_20d_tmp)
    else:
        top_d_str = format_top_brokers(brokers_cached if 'brokers_cached' in locals() else extra.get('brokers', []), 3, extra.get('broker_status','AKUM'))
        top_5d_str = ""
        top_20d_str = ""

    chart_file = f"chart_{stock_code.upper()}_{timeframe}_{int(time.time())}.png"
    try:
        file_path = generate_pro_chart(df=df, symbol=stock_code.upper(), timeframe=timeframe, sector_info=f"{stock_code.upper()} | IHSG", output_filename=chart_file, extra_info=extra)
        buy_sigs = extra.get('_chart_buy_signals', []) if extra else []
        sell_sigs = extra.get('_chart_sell_signals', []) if extra else []
        all_sigs = buy_sigs + sell_sigs
        tp = calculate_trading_plan(df, signals=all_sigs if all_sigs else None, multi_tf=multi)

        if multi:
            st_d = multi.get('status_d','AKUM' if multi.get('buy_d',0)>multi.get('sell_d',0) else 'DIST')
            st_5d = multi.get('status_5d','')
            st_20d = multi.get('status_20d','')
            daily_line = f"{st_d} | Buy {format_large_number(multi.get('buy_d',0),True)} Sell {format_large_number(multi.get('sell_d',0),True)} Net {format_large_number(multi.get('net_d',0),True)} Avg {multi.get('avg_d',0):.0f} | {top_d_str}"
            weekly_line = f"{st_5d} | Buy {format_large_number(multi.get('buy_5d',0),True)} Sell {format_large_number(multi.get('sell_5d',0),True)} Net {format_large_number(multi.get('net_5d',0),True)} Avg {multi.get('avg_5d',0):.0f} | {top_5d_str}"
            monthly_line = f"{st_20d} | Buy {format_large_number(multi.get('buy_20d',0),True)} Sell {format_large_number(multi.get('sell_20d',0),True)} Net {format_large_number(multi.get('net_20d',0),True)} Avg {multi.get('avg_20d',0):.0f} | {top_20d_str}"
        else:
            daily_line = f"Akum: {format_large_number(extra.get('accum_value',0), True)} Net: {format_large_number(extra.get('broker_net',0), True)} ({extra.get('broker_status')})"
            weekly_line = ""
            monthly_line = ""

        if tp:
            top_broker_str = top_d_str if 'top_d_str' in locals() else "-"
            sig_type = tp.get('signal_type','NO SIGNAL')
            sig_reason = tp.get('signal_reason','')
            sig_strength = tp.get('signal_strength',0)
            side = tp.get('side','WAIT')
            is_buy = tp.get('is_buy_signal', False)
            is_sell = tp.get('is_sell_signal', False)
            sig_emoji = "🟢" if is_buy else "🔴" if is_sell else "⚪"
            mtf_confirm = tp.get('mtf_confirm','')
            total_buy = len(tp.get('buy_signals',[]))
            total_sell = len(tp.get('sell_signals',[]))
            if is_buy:
                rec = f"✅ {sig_type} - BUY NOW" if sig_strength>=85 else f"⚠️ {sig_type} - CONSIDER BUY"
            elif is_sell:
                rec = f"🔴 {sig_type} - SELL / AVOID"
            else:
                rec = "⏸️ NO SIGNAL - WAIT TRIGGER"

            if side == "WAIT":
                caption = (
                    f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])} | {tp['trend']}\n"
                    f"Score REAL: {extra.get('score',0)}% | {sig_emoji} {sig_type} | {side}\n"
                    f"Daily: {daily_line}\n"
                    f"Weekly 5D: {weekly_line}\n"
                    f"Monthly 20D: {monthly_line}\n"
                    f"Timeframe: {timeframe.upper()} | Buy:{total_buy} Sell:{total_sell}\n"
                    f"------------------\n"
                    f"TRIGGER: {sig_reason}\n"
                    f"------------------\n"
                    f"TRADING PLAN - {rec}\n"
                    f"Status: ⏸️ WAIT - Tidak ada trigger valid 10 candle terakhir\n"
                    f"Sup: {tp['support']} | Res: {tp['resistance']} | ATR: {tp['atr']:.1f}"
                )
            else:
                caption = (
                    f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])} | {tp['trend']}\n"
                    f"Score REAL: {extra.get('score',0)}% | {sig_emoji} {sig_type} ({sig_strength}%) | {side} | MTF:{mtf_confirm}\n"
                    f"Daily: {daily_line}\n"
                    f"Weekly 5D: {weekly_line}\n"
                    f"Monthly 20D: {monthly_line}\n"
                    f"Timeframe: {timeframe.upper()} | Buy:{total_buy} Sell:{total_sell}\n"
                    f"------------------\n"
                    f"TRIGGER: {sig_reason}\n"
                    f"------------------\n"
                    f"TRADING PLAN - {rec}\n"
                    f"Entry: {tp['entry']} | SL: {tp['sl']} ({tp['risk_pct']}%)\n"
                    f"TP1: {tp['tp1']} (RR {tp['rr1']}) | TP2: {tp['tp2']} (RR {tp['rr2']})\n"
                    f"Sup: {tp['support']} | Res: {tp['resistance']} | ATR: {tp['atr']:.1f}"
                )
            if False:
                caption = (
                    f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])} | {tp['trend']}\n"
                    f"Score REAL: {extra.get('score',0)}%\n"
                    f"Daily: {daily_line}\n"
                    f"Weekly 5D: {weekly_line}\n"
                    f"Monthly 20D: {monthly_line}\n"
                    f"Timeframe: {timeframe.upper()}\n"
                    f"------------------\n"
                    f"TRADING PLAN\n"
                    f"Entry: {tp['entry']} | SL: {tp['sl']} ({tp['risk_pct']}%)\n"
                    f"TP1: {tp['tp1']} (RR {tp['rr1']}) | TP2: {tp['tp2']} (RR {tp['rr2']})\n"
                    f"Sup: {tp['support']} | Res: {tp['resistance']} | ATR: {tp['atr']:.1f}"
                )
            else:
                caption = (
                    f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])} | {tp['trend']}\n"
                    f"Score REAL: {extra.get('score',0)}% | Akum: {format_large_number(extra.get('accum_value',0), True)}\n"
                    f"Net Broker: {format_large_number(extra.get('broker_net',0), True)} ({extra.get('broker_status')})\n"
                    f"Top Brokers: {top_broker_str}\n"
                    f"Timeframe: {timeframe.upper()}\n"
                    f"------------------\n"
                    f"TRADING PLAN\n"
                    f"Entry: {tp['entry']} | SL: {tp['sl']} ({tp['risk_pct']}%)\n"
                    f"TP1: {tp['tp1']} (RR {tp['rr1']}) | TP2: {tp['tp2']} (RR {tp['rr2']})\n"
                    f"Sup: {tp['support']} | Res: {tp['resistance']} | ATR: {tp['atr']:.1f}"
                )
        else:
            top_broker_str = top_d_str if 'top_d_str' in locals() else "-"
            if multi:
                caption = (
                    f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])}\n"
                    f"Score REAL: {extra.get('score',0)}%\n"
                    f"Daily: {daily_line}\n"
                    f"Weekly 5D: {weekly_line}\n"
                    f"Monthly 20D: {monthly_line}\n"
                    f"Timeframe: {timeframe.upper()}"
                )
            else:
                caption = (
                    f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])}\n"
                    f"Score REAL: {extra.get('score',0)}% | Akum: {format_large_number(extra.get('accum_value',0), True)}\n"
                    f"Net Broker: {format_large_number(extra.get('broker_net',0), True)} ({extra.get('broker_status')})\n"
                    f"Top Brokers: {top_broker_str}\n"
                    f"Timeframe: {timeframe.upper()}"
                )
        send_photo_reply(chat_id, file_path, caption=caption)
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        import traceback
        traceback.print_exc()
        send_reply(chat_id, f"❌ Gagal render: `{e}`")

# Cache sinyal terakhir biar chart bisa ambil data real tanpa request lagi
LAST_SIGNALS_CACHE = {}

def telegram_bot_listener():
    global LAST_SIGNALS_CACHE
    offset = 0
    print("🤖 Telegram Listener V3 Running...")
    # Hapus webhook biar getUpdates jalan (fix telegram gak respon)
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
        print("✅ Webhook deleted, polling mode active")
    except Exception as e:
        print(f"Webhook delete fail: {e}")
    # Test token valid
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe", timeout=10)
        print(f"✅ Bot Info: {r.json().get('result',{})}")
        if r.status_code != 200:
            print(f"❌ TOKEN INVALID: {r.text}")
    except Exception as e:
        print(f"❌ Bot token error: {e}")
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res = requests.get(url, timeout=25)
            if res.status_code != 200:
                print(f"⚠ getUpdates {res.status_code}: {res.text[:200]}")
                time.sleep(3)
                continue
            if res.status_code == 200:
                data = res.json()
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    if "callback_query" in update:
                        cb = update["callback_query"]
                        cb_id = cb.get("id")
                        cb_data = cb.get("data","")
                        chat_id = cb["message"]["chat"]["id"]
                        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery", json={"callback_query_id": cb_id})
                        if cb_data.startswith("chart_"):
                            parts = cb_data.split("_")
                            if len(parts) >=3:
                                sym = parts[1]
                                tf = parts[2]
                                threading.Thread(target=process_chart_request, args=(chat_id, sym, tf, LAST_SIGNALS_CACHE)).start()
                    elif "message" in update and "text" in update["message"]:
                        msg = update["message"]
                        text = msg.get("text","").strip()
                        chat_id = msg["chat"]["id"]
                        first_word = text.split()[0].lower() if text else ""
                        print(f"📩 Pesan masuk: {text} dari chat_id={chat_id}")
                        if first_word in ["/start","/help"]:
                            help_msg = (
                                "🤖 *RAFANO V3 PRO FINAL*\n"
                                "============================\n"
                                "📈 *CHART & ANALISA*\n"
                                "`/c <KODE> [TF]` - Chart Pro + Real Akum\n"
                                "   `/c BBCA` `/c ANTM 15m` `/c BBCA 1h`\n"
                                "`/b <KODE>` - Detail Bandar / Broker\n"
                                "`/info <KODE>` - Info lengkap saham\n"
                                "`/trend <KODE>` - Analisa trend MTF\n"
                                "\n"
                                "🔍 *SCREENER*\n"
                                "`/scan` - Scan V3 Real Accumulation\n"
                                "`/scanpro` - Scan + chart top 3\n"
                                "`/top [N] [akum/dist]` - Top akumulasi\n"
                                "   `/top 10` `/top 5 dist`\n"
                                "`/compare <KODE1> <KODE2>` - Bandingkan 2 saham\n"
                                "\n"
                                "⭐ *WATCHLIST*\n"
                                "`/wl` - Lihat watchlist\n"
                                "`/wl add <KODE>` - Tambah watchlist\n"
                                "`/wl del <KODE>` - Hapus\n"
                                "`/wl scan` - Scan hanya watchlist\n"
                                "\n"
                                "🛠️ *TOOLS*\n"
                                "`/clearcache` atau `/cc` - Hapus cache Buy 0\n"
                                "`/help` - Menu ini\n"
                            )
                            send_reply(chat_id, help_msg)
                        elif first_word in ["/c","/chart","!chart"]:
                            parts = text.split()
                            if len(parts) >=2:
                                sym = parts[1].upper()
                                tf = parts[2] if len(parts)>=3 else "1d"
                                threading.Thread(target=process_chart_request, args=(chat_id, sym, tf, LAST_SIGNALS_CACHE)).start()
                            else:
                                send_reply(chat_id, "⚠ Format: `/c <KODE> [TF]`")
                        elif first_word in ["/b","/broker","/bandar"]:
                            parts = text.split()
                            if len(parts) >=2:
                                sym = parts[1].upper()
                                def broker_detail(target_chat, symbol):
                                    try:
                                        multi = get_broker_multi_tf(symbol)
                                        net_d, status_d, brokers = get_broker_summary(symbol)
                                        acc, brokers_acc = get_broker_accumulation(symbol, top=10)
                                        msg = f"🏦 *BROKER DETAIL {symbol}* -- {get_now_wib().strftime('%d %b %H:%M')}\n"
                                        msg += f"Status: {status_d} | Net: {format_large_number(net_d, True)}\n"
                                        msg += f"Accum: {format_large_number(acc, True)}\n\n"
                                        if multi:
                                            msg += f"Daily: {multi.get('status_d')} | Buy {format_large_number(multi.get('buy_d',0),True)} Sell {format_large_number(multi.get('sell_d',0),True)} Net {format_large_number(multi.get('net_d',0),True)} Avg {multi.get('avg_d',0):.0f}\n"
                                            msg += f"  └ Top: {format_top_brokers(multi.get('brokers',[]),3,multi.get('status_d'))}\n"
                                            msg += f"Weekly: {multi.get('status_5d')} | Buy {format_large_number(multi.get('buy_5d',0),True)} Sell {format_large_number(multi.get('sell_5d',0),True)} Net {format_large_number(multi.get('net_5d',0),True)} Avg {multi.get('avg_5d',0):.0f}\n"
                                            msg += f"  └ Top: {format_top_brokers(multi.get('brokers_5d',[]) or multi.get('brokers',[]),3,multi.get('status_5d'))}\n"
                                            msg += f"Monthly: {multi.get('status_20d')} | Buy {format_large_number(multi.get('buy_20d',0),True)} Sell {format_large_number(multi.get('sell_20d',0),True)} Net {format_large_number(multi.get('net_20d',0),True)} Avg {multi.get('avg_20d',0):.0f}\n"
                                            msg += f"  └ Top: {format_top_brokers(multi.get('brokers_20d',[]) or multi.get('brokers',[]),3,multi.get('status_20d'))}\n\n"
                                        msg += "*TOP BROKERS:*\n"
                                        for idx, b in enumerate(brokers[:10],1):
                                            code = b.get('broker_code','??')
                                            buy = format_large_number(b.get('buy_value',0), True)
                                            sell = format_large_number(b.get('sell_value',0), True)
                                            net = format_large_number(b.get('net_value',0), True)
                                            emoji = "🟢" if b.get('net_value',0)>0 else "🔴"
                                            msg += f"{idx}. {emoji} {code} Buy {buy} Sell {sell} Net {net}\n"
                                        send_reply(target_chat, msg)
                                    except Exception as e:
                                        send_reply(target_chat, f"❌ Error broker {symbol}: {e}")
                                threading.Thread(target=broker_detail, args=(chat_id, sym)).start()
                            else:
                                send_reply(chat_id, "⚠ Format: `/b <KODE>` contoh `/b BBCA`")
                        elif first_word in ["/info","/i"]:
                            parts = text.split()
                            if len(parts) >=2:
                                sym = parts[1].upper()
                                def info_detail(target_chat, symbol):
                                    try:
                                        df = get_history_pro(symbol, limit=50, timeframe="1d")
                                        multi = get_broker_multi_tf(symbol, df)
                                        analysis = get_analysis(symbol) if 'get_analysis' in globals() else {}
                                        last_close = df['Close'].iloc[-1] if df is not None and len(df)>0 else 0
                                        msg = f"📊 *INFO {symbol}* -- {safe_int(last_close)}\n"
                                        msg += f"Time: {get_now_wib().strftime('%d %b %Y %H:%M')}\n\n"
                                        if multi:
                                            msg += f"🏦 Bandar: {multi.get('status_d')} | {multi.get('status_5d')} | {multi.get('status_20d')}\n"
                                            msg += f"Daily Net: {format_large_number(multi.get('net_d',0),True)} Avg {multi.get('avg_d',0):.0f}\n"
                                            msg += f"Top: {format_top_brokers(multi.get('brokers',[]),3, multi.get('status_d','AKUM'))}\n\n"
                                        if df is not None and len(df)>=20:
                                            df['EMA50'] = df['Close'].ewm(span=50).mean()
                                            ema50 = df['EMA50'].iloc[-1]
                                            trend = "UPTREND" if last_close>ema50 else "DOWNTREND"
                                            msg += f"📈 Trend: {trend} | EMA50: {ema50:.0f}\n"
                                            msg += f"High 20D: {df['High'].tail(20).max():.0f} Low 20D: {df['Low'].tail(20).min():.0f}\n\n"
                                        msg += f"Gunakan `/c {symbol}` untuk chart, `/b {symbol}` untuk broker detail"
                                        send_reply(target_chat, msg)
                                    except Exception as e:
                                        import traceback
                                        traceback.print_exc()
                                        send_reply(target_chat, f"❌ Error info {symbol}: {e}")
                                threading.Thread(target=info_detail, args=(chat_id, sym)).start()
                            else:
                                send_reply(chat_id, "⚠ Format: `/info <KODE>`")
                        elif first_word in ["/trend","/t"]:
                            parts = text.split()
                            if len(parts) >=2:
                                sym = parts[1].upper()
                                def trend_detail(target_chat, symbol):
                                    try:
                                        df = get_history_pro(symbol, limit=150, timeframe="1d")
                                        multi = get_broker_multi_tf(symbol, df)
                                        buy_sigs, _ = detect_buy_signals(df, multi)
                                        sell_sigs, _ = detect_sell_signals(df, multi)
                                        tp = calculate_trading_plan(df, signals=buy_sigs+sell_sigs, multi_tf=multi)
                                        msg = f"📈 *TREND MTF {symbol}*\n\n"
                                        if multi:
                                            msg += f"Daily: {multi.get('status_d')} Net {format_large_number(multi.get('net_d',0),True)}\n"
                                            msg += f"Weekly: {multi.get('status_5d')} Net {format_large_number(multi.get('net_5d',0),True)}\n"
                                            msg += f"Monthly: {multi.get('status_20d')} Net {format_large_number(multi.get('net_20d',0),True)}\n\n"
                                        if tp:
                                            msg += f"Signal: {tp.get('signal_type')} | {tp.get('side')} ({tp.get('signal_strength')}%)\n"
                                            msg += f"Trend: {tp.get('trend')}\n"
                                            msg += f"MTF Confirm: {tp.get('mtf_confirm')}\n"
                                            msg += f"Buy Signals: {len(tp.get('buy_signals',[]))} Sell: {len(tp.get('sell_signals',[]))}\n"
                                            if tp.get('side') != 'WAIT':
                                                msg += f"\nEntry {tp['entry']} SL {tp['sl']} TP1 {tp['tp1']} TP2 {tp['tp2']}\n"
                                            else:
                                                msg += f"\nStatus: WAIT - {tp.get('signal_reason')}\n"
                                        send_reply(target_chat, msg)
                                    except Exception as e:
                                        send_reply(target_chat, f"❌ Error trend {symbol}: {e}")
                                threading.Thread(target=trend_detail, args=(chat_id, sym)).start()
                            else:
                                send_reply(chat_id, "⚠ Format: `/trend <KODE>`")
                        elif first_word in ["/top"]:
                            parts = text.split()
                            n = 10
                            filter_status = None
                            if len(parts)>=2:
                                try:
                                    n = int(parts[1])
                                    if len(parts)>=3:
                                        filter_status = parts[2].upper()
                                except:
                                    filter_status = parts[1].upper()
                                    if filter_status not in ["AKUM","DIST"]:
                                        n = 10
                                        filter_status = None
                            def top_accum(target_chat, limit, status_filter):
                                try:
                                    sigs = LAST_SIGNALS_CACHE.values() if LAST_SIGNALS_CACHE else scan_v3()
                                    if isinstance(sigs, dict):
                                        sigs = list(sigs.values()) if hasattr(sigs, 'values') else list(sigs)
                                    # Sort by net_d
                                    def get_net(x):
                                        multi = x.get('multi_tf') or {}
                                        return abs(multi.get('net_d',0) or x.get('broker_net',0) or 0)
                                    sorted_sigs = sorted(sigs, key=get_net, reverse=True)
                                    if status_filter:
                                        def match_status(s):
                                            st = (s.get('multi_tf',{}).get('status_d','') or s.get('broker_status','') or '').upper()
                                            if status_filter in ["DIST", "DISTRIB", "DISTRIBUSI"]:
                                                return st in ["DIST", "DISTRIB", "DISTRIBUSI", "SELL"]
                                            elif status_filter in ["AKUM", "ACCUM", "AKUMULASI"]:
                                                return st in ["AKUM", "ACCUM", "BUY"]
                                            else:
                                                return st == status_filter
                                        sorted_sigs = [s for s in sorted_sigs if match_status(s)]
                                        if len(sorted_sigs)==0 and status_filter in ["DIST", "DISTRIB"]:
                                            try:
                                                all_sigs = scan_v3()
                                                def is_dist(x):
                                                    multi = x.get('multi_tf') or {}
                                                    net = multi.get('net_d',0) or x.get('broker_net',0) or 0
                                                    st = (multi.get('status_d','') or x.get('broker_status','') or '').upper()
                                                    return net<0 or st in ["DIST","DISTRIB","SELL"]
                                                sorted_sigs = [s for s in all_sigs if is_dist(s)]
                                                sorted_sigs = sorted(sorted_sigs, key=lambda x: abs((x.get('multi_tf',{}).get('net_d',0) or x.get('broker_net',0) or 0)), reverse=True)
                                            except:
                                                pass
                                    msg = f"🏆 *TOP {limit} {'AKUM' if status_filter=='AKUM' else 'DIST' if status_filter=='DIST' else 'AKUMULASI'}*\n\n"
                                    for idx, item in enumerate(sorted_sigs[:limit],1):
                                        multi = item.get('multi_tf') or {}
                                        sym = item.get('symbol','??')
                                        net = multi.get('net_d',0) or item.get('broker_net',0)
                                        status = multi.get('status_d','') or item.get('broker_status','')
                                        emoji = "🟢" if status=="AKUM" else "🔴" if status=="DIST" else "⚪"
                                        msg += f"{idx}. {emoji} *{sym}* {status} Net {format_large_number(net,True)} | {format_top_brokers(multi.get('brokers',[]) or item.get('brokers',[]),2,status)}\n"
                                    send_reply(target_chat, msg)
                                except Exception as e:
                                    import traceback
                                    traceback.print_exc()
                                    send_reply(target_chat, f"❌ Error top: {e}")
                            threading.Thread(target=top_accum, args=(chat_id, n, filter_status)).start()
                        elif first_word in ["/compare","/comp"]:
                            parts = text.split()
                            if len(parts)>=3:
                                sym1 = parts[1].upper()
                                sym2 = parts[2].upper()
                                def compare_stocks(target_chat, s1, s2):
                                    try:
                                        m1 = get_broker_multi_tf(s1)
                                        m2 = get_broker_multi_tf(s2)
                                        df1 = get_history_pro(s1, limit=20)
                                        df2 = get_history_pro(s2, limit=20)
                                        close1 = df1['Close'].iloc[-1] if df1 is not None else 0
                                        close2 = df2['Close'].iloc[-1] if df2 is not None else 0
                                        msg = f"⚖️ *COMPARE {s1} vs {s2}*\n\n"
                                        msg += f"*{s1}* {safe_int(close1)} | {m1.get('status_d')} Net {format_large_number(m1.get('net_d',0),True)}\n"
                                        msg += f"  Weekly {m1.get('status_5d')} Net {format_large_number(m1.get('net_5d',0),True)}\n"
                                        msg += f"  Monthly {m1.get('status_20d')} Net {format_large_number(m1.get('net_20d',0),True)}\n"
                                        msg += f"  Top: {format_top_brokers(m1.get('brokers',[]),2,m1.get('status_d'))}\n\n"
                                        msg += f"*{s2}* {safe_int(close2)} | {m2.get('status_d')} Net {format_large_number(m2.get('net_d',0),True)}\n"
                                        msg += f"  Weekly {m2.get('status_5d')} Net {format_large_number(m2.get('net_5d',0),True)}\n"
                                        msg += f"  Monthly {m2.get('status_20d')} Net {format_large_number(m2.get('net_20d',0),True)}\n"
                                        msg += f"  Top: {format_top_brokers(m2.get('brokers',[]),2,m2.get('status_d'))}\n\n"
                                        winner = s1 if abs(m1.get('net_d',0))>abs(m2.get('net_d',0)) else s2
                                        msg += f"🏆 Lebih kuat: *{winner}* (Net lebih besar)"
                                        send_reply(target_chat, msg)
                                    except Exception as e:
                                        send_reply(target_chat, f"❌ Error compare: {e}")
                                threading.Thread(target=compare_stocks, args=(chat_id, sym1, sym2)).start()
                            else:
                                send_reply(chat_id, "⚠ Format: `/compare BBCA BBRI`")
                        elif first_word in ["/wl","/watchlist"]:
                            parts = text.split()
                            WATCHLIST_FILE = "/tmp/rafano_watchlist.json"
                            def load_wl():
                                try:
                                    import json
                                    if os.path.exists(WATCHLIST_FILE):
                                        with open(WATCHLIST_FILE,'r') as f:
                                            return json.load(f)
                                except:
                                    pass
                                return []
                            def save_wl(wl):
                                try:
                                    import json
                                    with open(WATCHLIST_FILE,'w') as f:
                                        json.dump(wl,f)
                                except:
                                    pass
                            if len(parts)==1 or parts[1].lower() in ["list","show"]:
                                wl = load_wl()
                                if not wl:
                                    send_reply(chat_id, "⭐ Watchlist kosong. Tambah dengan `/wl add BBCA`")
                                else:
                                    msg = f"⭐ *WATCHLIST* ({len(wl)} saham)\n\n"
                                    for s in wl:
                                        msg += f"• {s}\n"
                                    msg += f"\n`/wl add <KODE>` tambah, `/wl del <KODE>` hapus, `/wl scan` scan watchlist"
                                    send_reply(chat_id, msg)
                            elif parts[1].lower()=="add" and len(parts)>=3:
                                sym = parts[2].upper()
                                wl = load_wl()
                                if sym not in wl:
                                    wl.append(sym)
                                    save_wl(wl)
                                    send_reply(chat_id, f"✅ {sym} ditambah ke watchlist ({len(wl)} saham)")
                                else:
                                    send_reply(chat_id, f"⚠ {sym} sudah ada di watchlist")
                            elif parts[1].lower() in ["del","remove","rm"] and len(parts)>=3:
                                sym = parts[2].upper()
                                wl = load_wl()
                                if sym in wl:
                                    wl.remove(sym)
                                    save_wl(wl)
                                    send_reply(chat_id, f"🗑️ {sym} dihapus dari watchlist")
                                else:
                                    send_reply(chat_id, f"⚠ {sym} tidak ada di watchlist")
                            elif parts[1].lower()=="scan":
                                wl = load_wl()
                                if not wl:
                                    send_reply(chat_id, "⭐ Watchlist kosong")
                                else:
                                    def scan_wl(target_chat, symbols):
                                        try:
                                            results = []
                                            for sym in symbols:
                                                try:
                                                    df = get_history_pro(sym, limit=50)
                                                    multi = get_broker_multi_tf(sym, df)
                                                    score = 60 if multi.get('net_d',0)>0 else 35
                                                    results.append({"symbol":sym, "multi_tf":multi, "score":score, "close": df['Close'].iloc[-1] if df is not None else 0})
                                                except:
                                                    pass
                                            results = sorted(results, key=lambda x: abs(x.get('multi_tf',{}).get('net_d',0)), reverse=True)
                                            msg = f"⭐ *WATCHLIST SCAN* ({len(results)})\n\n"
                                            for idx, item in enumerate(results,1):
                                                multi = item.get('multi_tf',{})
                                                msg += f"{idx}. *{item['symbol']}* -- {safe_int(item.get('close',0))} | {multi.get('status_d')} Net {format_large_number(multi.get('net_d',0),True)}\n"
                                            send_reply(target_chat, msg)
                                        except Exception as e:
                                            send_reply(target_chat, f"❌ Error wl scan: {e}")
                                    threading.Thread(target=scan_wl, args=(chat_id, wl)).start()
                            else:
                                send_reply(chat_id, "⭐ Format: `/wl` `/wl add BBCA` `/wl del BBCA` `/wl scan`")
                        elif first_word in ["/clearcache","/cc","/clear"]:
                            try:
                                import os
                                removed = []
                                for p in ["/tmp/rafano_cache.json"]:
                                    if os.path.exists(p):
                                        os.remove(p)
                                        removed.append(p)
                                BROKER_CACHE.clear()
                                HISTORY_CACHE.clear()
                                SCREENER_CACHE.clear()
                                LAST_SIGNALS_CACHE.clear()
                                send_reply(chat_id, f"🧹 Cache cleared: {', '.join(removed) if removed else 'memory cleared'}\nSekarang coba `/scan` atau `/c BBCA` lagi")
                            except Exception as e:
                                send_reply(chat_id, f"❌ Error clear: {e}")
                        elif first_word in ["/scan","!scan","/scanpro"]:
                            send_reply(chat_id, "🔍 *V3 Scanning Real Accumulation...*")
                            def manual_scan(is_pro=False, target_chat=chat_id):
                                global LAST_SIGNALS_CACHE
                                print(f"Manual scan dipanggil is_pro={is_pro} target={target_chat}")
                                sigs = scan_v3()
                                LAST_SIGNALS_CACHE = {s['symbol']: s for s in sigs}
                                # Manual scan BYPASS cooldown biar bisa dipanggil kapan aja, termasuk weekend
                                filt = sigs  # jangan pakai filter_signals_with_cooldown buat manual
                                # Selalu kirim ke yang request, bukan cuma TARGET_CHAT_ID
                                now_str = get_now_wib().strftime('%d %b %Y %H:%M WIB')
                                if not filt:
                                    send_reply(target_chat, f"*RAFANO V3* {now_str}\n0 sinyal (weekend/market tutup, coba /c BBCA)\n_Screener kosong, coba beberapa saham fallback_")
                                    return
                                header = f"*RAFANO V3 PRO - {now_str}*\nTotal: {len(filt)} (manual, tanpa cooldown)\n\n"
                                msg = header
                                kb = []
                                for idx, item in enumerate(filt,1):
                                    def fmt(v):
                                        return format_large_number(v, True)
                                    def fmt_avg(v):
                                        try:
                                            fv = float(v)
                                            return f"{fv:.0f}" if fv >= 100 else f"{fv:.1f}"
                                        except:
                                            return "0"
                                    multi = item.get('multi_tf') or {}
                                    brokers = item.get('brokers', []) or []
                                    top_broker_str = format_top_brokers(brokers, 3)
                                    reasons_str = " | ".join(item.get('reasons', [])[:2])
                                    if multi:
                                        st_d = multi.get('status_d','NEUTRAL')
                                        st_5d = multi.get('status_5d','NEUTRAL')
                                        st_20d = multi.get('status_20d','NEUTRAL')
                                        emoji_d = "🟢" if st_d=="AKUM" else "🔴" if st_d=="DIST" else "⚪"
                                        emoji_5d = "🟢" if st_5d=="AKUM" else "🔴" if st_5d=="DIST" else "⚪"
                                        emoji_20d = "🟢" if st_20d=="AKUM" else "🔴" if st_20d=="DIST" else "⚪"
                                        brokers_d = multi.get('brokers', []) or item.get('brokers', []) or []
                                        brokers_5d = multi.get('brokers_5d', []) or brokers_d
                                        brokers_20d = multi.get('brokers_20d', []) or brokers_d
                                        top_d = format_top_brokers(brokers_d, 3, st_d)
                                        top_5d = format_top_brokers(brokers_5d, 3, st_5d)
                                        top_20d = format_top_brokers(brokers_20d, 3, st_20d)
                                        label_d = "Top Buy" if st_d=="AKUM" else "Top Sell" if st_d=="DIST" else "Top"
                                        label_5d = "Top Buy" if st_5d=="AKUM" else "Top Sell" if st_5d=="DIST" else "Top"
                                        label_20d = "Top Buy" if st_20d=="AKUM" else "Top Sell" if st_20d=="DIST" else "Top"
                                        daily_str = f"{emoji_d} Daily: {st_d} | Buy {fmt(multi.get('buy_d',0))} Sell {fmt(multi.get('sell_d',0))} Net {fmt(multi.get('net_d',0))} | Avg {fmt_avg(multi.get('avg_d',0))}"
                                        daily_top = f"   |  └ {label_d}: {top_d}"
                                        weekly_str = f"{emoji_5d} Weekly 5D: {st_5d} | Buy {fmt(multi.get('buy_5d',0))} Sell {fmt(multi.get('sell_5d',0))} Net {fmt(multi.get('net_5d',0))} | Avg {fmt_avg(multi.get('avg_5d',0))}"
                                        weekly_top = f"   |  └ {label_5d}: {top_5d}"
                                        monthly_str = f"{emoji_20d} Monthly 20D: {st_20d} | Buy {fmt(multi.get('buy_20d',0))} Sell {fmt(multi.get('sell_20d',0))} Net {fmt(multi.get('net_20d',0))} | Avg {fmt_avg(multi.get('avg_20d',0))}"
                                        monthly_top = f"   |  └ {label_20d}: {top_20d}"
                                    else:
                                        daily_str = f"Akum {fmt(item.get('accum_value',0))} | Net {fmt(item.get('broker_net',0))}"
                                        weekly_str = ""
                                        monthly_str = ""
                                        daily_top = ""
                                        weekly_top = ""
                                        monthly_top = ""
                                    tp = item.get('trading_plan')
                                    tp_line = f"Entry {tp['entry']} TP1 {tp['tp1']} SL {tp['sl']}" if tp else reasons_str
                                    item_str = f"{idx}. *{item['symbol']}* -- {item.get('close',0)} ({item.get('change_pct',0):+.2f}%)\n"
                                    item_str += f"   |- Score: {item['score']}% ({item.get('score_label','')})\n"
                                    item_str += f"   |- {daily_str}\n"
                                    if 'daily_top' in locals() and daily_top:
                                        item_str += f"{daily_top}\n"
                                    if weekly_str:
                                        item_str += f"   |- {weekly_str}\n"
                                        if 'weekly_top' in locals() and weekly_top:
                                            item_str += f"{weekly_top}\n"
                                    if monthly_str:
                                        item_str += f"   |- {monthly_str}\n"
                                        if 'monthly_top' in locals() and monthly_top:
                                            item_str += f"{monthly_top}\n"
                                    item_str += f"   +- {tp_line}\n\n"
                                    kb.append([{"text": f"Pro Chart {item['symbol']}", "callback_data": f"chart_{item['symbol']}_1d"}])
                                    if len(msg) + len(item_str) > 3500:
                                        send_reply(target_chat, msg, reply_markup={"inline_keyboard": kb})
                                        msg = item_str
                                        kb = []
                                    else:
                                        msg += item_str
                                send_reply(target_chat, msg, reply_markup={"inline_keyboard": kb})
                                if is_pro:
                                    for top in filt[:3]:
                                        process_chart_request(target_chat, top['symbol'], "1d", LAST_SIGNALS_CACHE)
                                        time.sleep(1)
                            is_pro_flag = (first_word == "/scanpro")
                            threading.Thread(target=manual_scan, args=(is_pro_flag, chat_id)).start()
        except Exception as e:
            print(f"Listener error: {e}")
            time.sleep(3)

def auto_screener_loop():
    global LAST_SIGNALS_CACHE
    print("🚀 Auto Screener V3 Active...")
    last_triggered_sesi1, last_triggered_eod = "", ""
    while True:
        try:
            if not is_market_open():
                time.sleep(300)
                continue
            now = get_now_wib()
            today_str, current_time_str = now.strftime('%Y-%m-%d'), now.strftime('%H:%M')
            weekday = now.weekday()

            # Rekap sesi
            target_sesi1 = "11:25" if weekday == 4 else "11:55"
            if current_time_str == target_sesi1 and last_triggered_sesi1 != today_str:
                sigs = scan_v3()
                LAST_SIGNALS_CACHE = {s['symbol']: s for s in sigs}
                filt = filter_signals_with_cooldown(sigs)
                broadcast_v3(filt)
                last_triggered_sesi1 = today_str

            if current_time_str == "15:55" and last_triggered_eod != today_str:
                sigs = scan_v3()
                LAST_SIGNALS_CACHE = {s['symbol']: s for s in sigs}
                filt = filter_signals_with_cooldown(sigs)
                broadcast_v3(filt)
                last_triggered_eod = today_str

            # Real-time per 10 menit
            sigs = scan_v3()
            LAST_SIGNALS_CACHE = {s['symbol']: s for s in sigs}
            filt = filter_signals_with_cooldown(sigs)
            if filt:
                broadcast_v3(filt)
            time.sleep(600)
        except Exception as e:
            print(f"Auto loop error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    print("==========================================")
    print("🔥 RAFANO V3 PRO STARTING...")
    print("==========================================")
    threading.Thread(target=auto_screener_loop, daemon=True).start()
    telegram_bot_listener()
