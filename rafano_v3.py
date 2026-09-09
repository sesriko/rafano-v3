"""
RAFANO V3 - FIXED SELL BROKER
Fix utama:
- broker-summary sekarang pakai start_date/end_date YYYY-MM-DD sesuai spec API
- bval/sval REAL dari API tidak direkayasa
- Weekly/Monthly REAL dari API, bukan estimasi
- format_top_brokers menampilkan SELL saat DIST
- Chart Generator TIDAK DIUBAH
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
def safe_get_env(key):
    v = os.getenv(key)
    if v:
        v = str(v).strip()
        if len(v)>=2 and ((v[0]=='"' and v[-1]=='"') or (v[0]=="'" and v[-1]=="'")):
            v = v[1:-1].strip()
        return v
    try:
        from google.colab import userdata
        vv = userdata.get(key)
        if vv:
            vv = str(vv).strip().strip('"').strip("'")
            os.environ[key] = vv
            return vv
    except Exception as ee:
        pass
    return None

TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')

TELEGRAM_BOT_TOKEN = safe_get_env("TELEGRAM_BOT_TOKEN")
TARGET_CHAT_ID = safe_get_env("TARGET_CHAT_ID")
ARJUM_API_KEY = safe_get_env("ARJUM_API_KEY")

print(f"🔑 ENV Loaded - TOKEN exists={bool(TELEGRAM_BOT_TOKEN)} len={len(TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else 0}, CHAT_ID={TARGET_CHAT_ID}, ARJUM len={len(ARJUM_API_KEY) if ARJUM_API_KEY else 0}")

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

def format_timeframe_label(tf):
    tf_clean = (tf or "1d").lower().strip()
    mapping = {
        "1m": "1 Menit", "1min": "1 Menit",
        "5m": "5 Menit", "5min": "5 Menit",
        "15m": "15 Menit", "15min": "15 Menit",
        "30m": "30 Menit", "30min": "30 Menit",
        "1h": "1 Jam", "60m": "1 Jam", "1hour": "1 Jam",
        "4h": "4 Jam", "4hour": "4 Jam",
        "1d": "Daily", "daily": "Daily", "d": "Daily",
        "1w": "Weekly", "weekly": "Weekly", "w": "Weekly",
        "1mo": "Monthly", "1M": "Monthly", "monthly": "Monthly",
    }
    return mapping.get(tf_clean, tf_clean.upper())

def is_intraday_tf(tf):
    return (tf or "1d").lower().strip() in ["1m","5m","15m","30m","1h","4h","1min","5min","15min","30min","1hour","4hour"]

def source_marker(source):
    if source == "VSA_ESTIMATE":
        return "≈"
    if source == "EMPTY":
        return "?"
    return ""

def grade_from_strength(strength, side):
    strength = strength or 0
    if side == "BUY":
        if strength >= 85:
            return "STRONG BUY", "#00ff00"
        elif strength >= 70:
            return "BUY", "#7CFC00"
        elif strength >= 55:
            return "WATCH", "#ffd700"
        else:
            return "NO SIGNAL", "#888888"
    elif side == "SELL":
        if strength >= 85:
            return "STRONG SELL", "#ff0000"
        elif strength >= 70:
            return "SELL", "#ff6666"
        else:
            return "WATCH SELL", "#ffaa00"
    else:
        return "WAIT", "#888888"

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
    price_range = (df['High'] - df['Low']).replace(0, 0.1)
    close_pos = (df['Close'] - df['Low']) / price_range
    close_pos = np.clip(close_pos, 0.05, 0.95)
    buy_ratio = 0.30 + close_pos * 0.60
    if 'V1' in df.columns:
        vol_ratio = df['Volume'] / df['V1'].replace(0, 1)
        is_green = df['Close'] >= df['Open']
        boost = np.where((vol_ratio > 1.5) & is_green, 0.10, 0)
        boost += np.where((vol_ratio > 2.5) & is_green, 0.10, 0)
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

def calculate_trading_plan(df, signals=None, multi_tf=None, timeframe="1d"):
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

        if signals is None:
            buy_sigs, _ = detect_buy_signals(df, multi_tf)
            sell_sigs, _ = detect_sell_signals(df, multi_tf)
            signals = buy_sigs + sell_sigs
        else:
            buy_sigs = [s for s in signals if s.get('side')=='BUY']
            sell_sigs = [s for s in signals if s.get('side')=='SELL']

        mtf_trend = {}
        mtf_confirm = "NEUTRAL"
        weekly_bullish = False
        monthly_bullish = False
        if multi_tf:
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
            entry = round_to_ihsg_fraction(last_close)
            sl = round_to_ihsg_fraction(max(df['Low'].tail(5).min(), last_close - atr*1.5))
            signal_type = "NO SIGNAL"
            signal_reason = "Tunggu BO EMA50 / BOW BB / BOS EMA (BUY) atau BD EMA50 / SOS BB (SELL)"
            signal_strength = 0
            signal_date = df.index[-1]
            side = "WAIT"
            is_buy = False

        if side == "BUY" and mtf_confirm == "STRONG BULLISH MTF":
            signal_strength = min(100, signal_strength + 10)
            signal_reason += " + MTF Weekly+Monthly AKUM"
        elif side == "BUY" and mtf_confirm == "BULLISH MTF":
            signal_strength = min(95, signal_strength + 5)
            signal_reason += " + MTF Bullish"
        elif side == "SELL" and mtf_confirm == "BEARISH MTF":
            signal_strength = min(100, signal_strength + 10)
            signal_reason += " + MTF Weekly+Monthly DIST"

        min_sl = last_close * 0.92
        max_sl = last_close * 0.98
        sl = max(min(sl, max_sl), min_sl)
        sl = round_to_ihsg_fraction(sl)
        if entry <= sl and side != "SELL":
            entry = round_to_ihsg_fraction(sl * 1.03)

        if side == "BUY":
            if "BOW" in signal_type:
                tp1 = round_to_ihsg_fraction(entry * 1.04)
                tp2 = round_to_ihsg_fraction(entry * 1.08)
            elif "BO EMA50" in signal_type:
                tp1 = round_to_ihsg_fraction(entry + atr*1.5)
                tp2 = round_to_ihsg_fraction(entry + atr*3.0)
                if mtf_confirm == "STRONG BULLISH MTF":
                    tp2 = round_to_ihsg_fraction(entry + atr*4.0)
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

        if side in ("BUY", "WAIT") and tp1 == tp2:
            tp2 = round_to_ihsg_fraction(entry + atr*3.0)
            if tp2 <= tp1:
                tp2 = round_to_ihsg_fraction(tp1 + max(atr, entry*0.01))
            reward2 = tp2 - entry
        elif side == "SELL" and tp1 == tp2:
            tp2 = round_to_ihsg_fraction(entry - atr*3.0)
            if tp2 >= tp1:
                tp2 = round_to_ihsg_fraction(tp1 - max(atr, entry*0.01))
            reward2 = entry - tp2

        rr1 = reward1 / risk if risk>0 else 0
        rr2 = reward2 / risk if risk>0 else 0

        if last_close > ema20 and last_close > ema50 and last_close > ema200:
            trend = "STRONG UPTREND"
        elif last_close > ema20 and last_close > ema50:
            trend = "UPTREND"
        elif last_close > ema20:
            trend = "WEAK UPTREND"
        else:
            trend = "DOWNTREND"

        trend_mtf = f"{trend} + {mtf_confirm}" if mtf_confirm != "NEUTRAL" else trend

        sr_lookback = 10
        if is_intraday_tf(timeframe):
            tf_bar_map = {"1m": 180, "5m": 60, "15m": 40, "30m": 30, "1h": 24, "4h": 15}
            sr_lookback = tf_bar_map.get((timeframe or "").lower(), 30)
        sr_lookback = min(sr_lookback, len(df))

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
            "support": int(df['Low'].tail(sr_lookback).min()),
            "resistance": int(df['High'].tail(sr_lookback).max()),
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

def calculate_trading_plan_with_signals(df, signals=None, multi_tf=None, timeframe="1d"):
    return calculate_trading_plan(df, signals=signals, multi_tf=multi_tf, timeframe=timeframe)

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
import json
from pathlib import Path

BROKER_CACHE = {}
HISTORY_CACHE = {}
SCREENER_CACHE = {}
CACHE_FILE = Path("/tmp/rafano_cache.json")
BROKER_CACHE_TTL = 300
HISTORY_CACHE_TTL = 600
SCREENER_CACHE_TTL = 180

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
    if not params:
        return path
    try:
        sorted_params = sorted(params.items())
        param_str = "&".join([f"{k}={v}" for k, v in sorted_params])
        return f"{path}?{param_str}"
    except:
        return path

def arjum_get(path, params=None, use_cache=True):
    cache_key = make_cache_key(path, params) if use_cache else None
    if use_cache and cache_key:
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
                if use_cache and cache_key:
                    if 'broker' in path:
                        set_cached_broker(cache_key, j)
                    elif 'screener' in path:
                        set_cached_screener(j)
                return j
            except Exception as je:
                print(f"⚠ arjum_get {path} JSON parse fail: {je}")
                return None
        else:
            print(f"⚠ arjum_get {path} params={params} -> {r.status_code} {r.text[:200]}")
            return None
    except Exception as e:
        print(f"arjum_get error {path}: {e}")
        return None

# ========== SCREENER ==========
def get_screener_latest():
    cached = get_cached_screener()
    if cached:
        data = cached
    else:
        data = arjum_get("/screener/latest")
    if not data:
        return []
    if isinstance(data, dict):
        if 'rows' in data and isinstance(data['rows'], list) and len(data['rows'])>0:
            normalized = []
            for r in data['rows']:
                code = r.get('stock_code') or r.get('symbol') or r.get('code') or r.get('stock')
                if code:
                    item = {'symbol': code.replace(".JK","").upper(), 'raw': r, 'bucket': r.get('bucket',''), 'summary': r.get('summary','')}
                    normalized.append(item)
            return normalized
        for k in ['data','results','stocks','screener','latest','items']:
            if k in data and isinstance(data[k], list) and len(data[k])>0:
                return data[k]
        try:
            first_val = list(data.values())[0]
            if isinstance(first_val, list) and len(first_val)>0:
                return first_val
        except:
            pass
        return data.get('data') or data.get('results') or data.get('stocks') or []
    return data if isinstance(data, list) else []

def get_broker_accumulation(symbol, top=20, days=None):
    params = {"top": top}
    if days:
        params["days"] = days
        params["period"] = days
    data = arjum_get(f"/broker-accumulation/{symbol}", params=params, use_cache=False)
    if not data:
        return 0.0, []
    if isinstance(data, dict) and ('top_buyers' in data or 'series' in data or 'top_sellers' in data):
        raw_brokers = []
        accum_total = 0
        top_buyers = data.get('top_buyers') or []
        top_sellers = data.get('top_sellers') or []
        series = data.get('series') or []
        for b in top_buyers[:20]:
            if not isinstance(b, dict): continue
            code = b.get('broker_code') or b.get('code') or '??'
            nval = b.get('nval') or b.get('net_val') or b.get('net_value') or 0
            bval = b.get('bval') or b.get('buy_value') or (nval if float(nval or 0)>0 else 0)
            sval = b.get('sval') or b.get('sell_value') or (abs(float(nval or 0)) if float(nval or 0)<0 else 0)
            raw_brokers.append({
                "broker_code": str(code).upper(),
                "broker": str(code).upper(),
                "buy_value": float(bval),
                "sell_value": float(sval),
                "net_value": float(nval),
                "bval": float(bval), "sval": float(sval), "nval": float(nval),
                "avg_price": float(b.get('bavg',0) or 0)
            })
        for b in top_sellers[:20]:
            if not isinstance(b, dict): continue
            code = str(b.get('broker_code') or '??').upper()
            if any(x['broker_code']==code for x in raw_brokers):
                continue
            nval = b.get('nval') or 0
            bval = b.get('bval') or 0
            sval = b.get('sval') or 0
            raw_brokers.append({
                "broker_code": code, "broker": code,
                "buy_value": float(bval), "sell_value": float(sval),
                "net_value": float(nval),
                "bval": float(bval), "sval": float(sval), "nval": float(nval),
                "avg_price": 0
            })
        return float(accum_total), raw_brokers
    return 0.0, []

# ========== FIXED: BROKER SUMMARY - SELL MUNCUL ==========
def _fmt_yyyy_mm_dd(d):
    if d is None:
        return None
    if hasattr(d, 'strftime'):
        return d.strftime('%Y-%m-%d')
    s = str(d).strip()
    if '/' in s:
        try:
            dd, mm, yyyy = s.split('/')
            return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
        except:
            return s
    return s

def get_broker_summary(symbol, date_from=None, date_to=None):
    """
    FIX FINAL sesuai spec: /api/broker-summary/{code}?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
    Response: brokers: [{broker_code, bval, sval, nval, nvol, bfrq, sfrq}]
    """
    base_params = {
        "net": "false",
        "broker_limit": 20,
        "level_limit": 25,
        "all_data": "false",
        "flow": "all"
    }
    if date_from and date_to:
        base_params["start_date"] = _fmt_yyyy_mm_dd(date_from)
        base_params["end_date"] = _fmt_yyyy_mm_dd(date_to)

    data = None
    # coba dengan tanggal dulu
    for params in [base_params, {k:v for k,v in base_params.items() if k not in ['start_date','end_date']}]:
        d = arjum_get(f"/broker-summary/{symbol}", params=params, use_cache=False)
        if d and isinstance(d, dict) and (d.get('brokers') or d.get('data')):
            data = d
            print(f"DEBUG broker-summary {symbol} OK params={params} brokers={len(d.get('brokers',[]))}")
            break
        if data is None:
            data = d

    net_value = 0
    brokers = []
    if data and isinstance(data, dict):
        raw_list = data.get('brokers') or data.get('data') or []
        for b in raw_list[:20]:
            if not isinstance(b, dict):
                continue
            code = b.get('broker_code') or b.get('code') or '??'
            bval = float(b.get('bval') or b.get('buy_value') or 0)
            sval = float(b.get('sval') or b.get('sell_value') or 0)
            nval = float(b.get('nval') or b.get('net_value') or (bval - sval))
            brokers.append({
                "broker_code": str(code).upper(),
                "broker": str(code).upper(),
                "broker_name": b.get('broker_name',''),
                "buy_value": bval,
                "sell_value": sval,
                "buy_volume": float(b.get('bvol',0) or b.get('nvol',0) or 0),
                "sell_volume": float(b.get('svol',0) or 0),
                "net_value": nval,
                "net_volume": float(b.get('nvol',0) or 0),
                "avg_price": float(b.get('avg_price',0) or 0),
                "bval": bval, "sval": sval, "nval": nval
            })
        if brokers:
            net_value = sum([x['net_value'] for x in brokers])

    status = "AKUM" if net_value > 0 else "DIST" if net_value < 0 else "NEUTRAL"
    print(f"DEBUG broker-summary parsed {symbol}: net={net_value:.0f} brokers={len(brokers)} status={status} Buy={sum([x['buy_value'] for x in brokers]):.0f} Sell={sum([x['sell_value'] for x in brokers]):.0f}")
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
                    if float(b.get('net_value',0)) >0:
                        total_value += float(b.get('avg_price'))
                        total_vol += 1
                bv = float(b.get('buy_value',0) or 0)
                bvol = float(b.get('buy_volume',0) or 0)
                if bv!=0 and bvol!=0:
                    avg = bv / bvol if bvol!=0 else 0
                    if avg>0:
                        total_value += avg
                        total_vol += 1
            if total_vol>0 and total_value>0:
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
    cache_key = f"multi_{symbol}"
    cached = get_cached_broker(cache_key)
    if cached and hist_df is None:
        try:
            is_empty = (cached.get('buy_d',0)==0 and cached.get('sell_d',0)==0 and len(cached.get('brokers',[]))==0)
            if not is_empty:
                return cached
        except:
            return cached

    def _trading_day_n_ago(n):
        try:
            if hist_df is not None and len(hist_df) > n:
                idx = hist_df.index[-(n+1)]
                return idx.date() if hasattr(idx, 'date') else idx
        except:
            pass
        return (get_now_wib() - datetime.timedelta(days=int(n*1.45)+2)).date()

    today = get_now_wib().date()
    if hist_df is not None and len(hist_df) > 0:
        try:
            last_idx = hist_df.index[-1]
            today = last_idx.date() if hasattr(last_idx, 'date') else today
        except:
            pass
    date_from_5d = _trading_day_n_ago(5)
    date_from_20d = _trading_day_n_ago(20)

    # REAL dari broker-summary dengan range tanggal YYYY-MM-DD
    net_d, status_d, brokers_d = get_broker_summary(symbol)
    net_5d, status_5d, brokers_5d = get_broker_summary(symbol, date_from=date_from_5d, date_to=today)
    net_20d, status_20d, brokers_20d = get_broker_summary(symbol, date_from=date_from_20d, date_to=today)

    def calc_buy_sell_status(brokers_list):
        if not brokers_list or len(brokers_list)==0:
            return 0, 0, 0, "NEUTRAL"
        buy_sum = sum([float(b.get('buy_value',0) or b.get('bval',0) or 0) for b in brokers_list])
        sell_sum = sum([float(b.get('sell_value',0) or b.get('sval',0) or 0) for b in brokers_list])
        net_sum = sum([float(b.get('net_value',0) or b.get('nval',0) or 0) for b in brokers_list])
        if net_sum==0 and (buy_sum or sell_sum):
            net_sum = buy_sum - sell_sum
        status = "AKUM" if net_sum > 0 else "DIST" if net_sum < 0 else "NEUTRAL"
        return buy_sum, sell_sum, net_sum, status

    buy_d, sell_d, net_d_calc, status_d_calc = calc_buy_sell_status(brokers_d)
    buy_5d, sell_5d, net_5d_calc, status_5d_calc = calc_buy_sell_status(brokers_5d)
    buy_20d, sell_20d, net_20d_calc, status_20d_calc = calc_buy_sell_status(brokers_20d)

    if net_d != 0: net_d_calc = net_d
    if net_5d != 0: net_5d_calc = net_5d
    if net_20d != 0: net_20d_calc = net_20d
    if status_d != "NEUTRAL": status_d_calc = status_d
    if status_5d != "NEUTRAL": status_5d_calc = status_5d
    if status_20d != "NEUTRAL": status_20d_calc = status_20d

    result = {
        "accum_d": float(abs(net_d_calc)),
        "accum_5d": float(abs(net_5d_calc)),
        "accum_20d": float(abs(net_20d_calc)),
        "buy_d": float(buy_d),
        "sell_d": float(sell_d),
        "buy_5d": float(buy_5d),
        "sell_5d": float(sell_5d),
        "buy_20d": float(buy_20d),
        "sell_20d": float(sell_20d),
        "net_d": float(net_d_calc),
        "net_5d": float(net_5d_calc),
        "net_20d": float(net_20d_calc),
        "avg_d": float(calculate_bandars_avg(brokers_d, hist_df, 1)),
        "avg_5d": float(calculate_bandars_avg(brokers_5d, hist_df, 5)),
        "avg_20d": float(calculate_bandars_avg(brokers_20d, hist_df, 20)),
        "source_d": "API_SUMMARY" if brokers_d else "EMPTY",
        "source_5d": "API_SUMMARY_RANGE" if brokers_5d else "EMPTY",
        "source_20d": "API_SUMMARY_RANGE" if brokers_20d else "EMPTY",
        "brokers": brokers_d,
        "brokers_5d": brokers_5d,
        "brokers_20d": brokers_20d,
        "status": status_d_calc,
        "status_d": status_d_calc,
        "status_5d": status_5d_calc,
        "status_20d": status_20d_calc,
    }
    is_empty_result = (buy_d==0 and sell_d==0 and len(brokers_d)==0)
    if not is_empty_result:
        set_cached_broker(cache_key, result)
    return result

def format_top_brokers(brokers, top=3, status="AKUM"):
    if not brokers or not isinstance(brokers, list) or len(brokers)==0:
        return "-"
    valid = [b for b in brokers if isinstance(b, dict) and (b.get('broker_code') or b.get('broker'))]
    if not valid:
        valid = [b for b in brokers if isinstance(b, dict)]
    if not valid:
        return "-"
    try:
        sorted_b = sorted(valid, key=lambda x: abs(float(x.get('net_value',0) or x.get('nval',0) or x.get('buy_value',0) or x.get('sell_value',0) or 0)), reverse=True)
    except:
        sorted_b = valid

    parts = []
    for b in sorted_b[:top]:
        code = b.get('broker_code') or b.get('broker') or "??"
        bval = float(b.get('buy_value',0) or b.get('bval',0) or 0)
        sval = float(b.get('sell_value',0) or b.get('sval',0) or 0)
        nval = float(b.get('net_value',0) or b.get('nval',0) or 0)
        if status == "DIST":
            val = sval if sval !=0 else abs(nval)
        else:
            val = bval if bval !=0 else abs(nval)
        if val == 0:
            # kalau sell/buy 0 tapi ada net, tetap tampilkan net
            val = abs(nval) if nval !=0 else 0
        if val == 0:
            continue
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
    hist_key = f"{symbol}_{timeframe}_{limit}"
    cached_hist = get_cached_history(hist_key)
    if cached_hist is not None:
        return cached_hist
    tf = timeframe.lower().strip()
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
    data = arjum_get(f"/history/{symbol}", params={"limit": limit, "frame": arjum_frame})
    rows = []
    if data:
        if isinstance(data, dict):
            rows = data.get('data') or data.get('history') or data.get('results') or data.get('candles') or data.get('klines') or []
        elif isinstance(data, list):
            rows = data
    if not rows:
        try:
            import yfinance as yf
            yf_map = {
                "1m":  ("7d", "1m"), "5m":  ("5d", "5m"), "15m": ("5d", "15m"),
                "30m": ("1mo", "30m"), "1h":  ("1mo", "60m"), "4h":  ("3mo", "90m"),
                "1d":  ("6mo", "1d"), "1w":  ("1y", "1wk"), "1mo": ("2y", "1mo"),
            }
            period, interval = yf_map.get(tf, ("6mo", "1d"))
            hist = yf.Ticker(f"{symbol}.JK").history(period=period, interval=interval, timeout=10)
            if (hist is None or len(hist) < 10) and tf in ["1m","5m","15m","30m","1h","4h"]:
                hist = yf.Ticker(f"{symbol}.JK").history(period="6mo", interval="1d", timeout=10)
            if hist is not None and len(hist) > 10:
                set_cached_history(hist_key, hist.tail(limit))
                return hist.tail(limit)
            else:
                return None
        except Exception as e:
            print(f"yfinance error {symbol} {tf}: {e}")
            return None
    try:
        df = pd.DataFrame(rows)
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
        return df
    except Exception as e:
        print(f"History parse {symbol} {tf}: {e}")
        return None

# ========== CHART GENERATOR - TIDAK DIUBAH ==========
def generate_pro_chart(df, symbol="BBCA", timeframe="1d", sector_info="IHSG", output_filename="chart.png", extra_info=None):
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.patches as patches
        import matplotlib.gridspec as gridspec
        extra_info = extra_info or {}
        tf_clean = timeframe.lower()
        tf_label_disp = extra_info.get('tf_label') or format_timeframe_label(timeframe)

        df = df.copy()
        df = df.ffill().bfill()
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.sort_index()
        else:
            df.index = pd.to_datetime(df.index)

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

        avg_price = df['Close'].tail(20).mean()
        vchg1 = (last_vol / df['Volume'].iloc[-2]) if len(df) > 1 and df['Volume'].iloc[-2] > 0 else 1
        avg5 = df['Volume'].tail(5).mean()
        vchg5 = (last_vol / avg5) if avg5 > 0 else 1

        speed = "FAST" if vchg1 > 2.0 else "SLOW" if vchg1 < 0.8 else "NORMAL"
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

        real_accum = extra_info.get('accum_value', 0)
        real_net = extra_info.get('broker_net', 0)
        nbsa_rp = abs(real_net) if real_net != 0 else abs(net_vol * last_close)
        nbsa_pct = min(99, max(5, abs(int((real_net / (real_accum+1e9))*10)))) if real_accum else 30.4

        plt.style.use('dark_background')
        fig = plt.figure(figsize=(16, 9), dpi=200, facecolor='#000000')
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

        ax_main.plot(x, df['EMA13'], color='#ffff00', linewidth=1.0, alpha=0.9)
        ax_main.plot(x, df['EMA20'], color='#ff0000', linewidth=1.0, alpha=0.9)
        ax_main.plot(x, df['EMA50'], color='#ffffff', linewidth=1.0, alpha=0.9)
        ax_main.plot(x, df['EMA200'], color='#a020f0', linewidth=1.2, alpha=0.9)

        if 'BB_UPPER' in plot_df.columns:
            ax_main.plot(x, plot_df['BB_UPPER'], color='#8888ff', linewidth=0.8, alpha=0.4, linestyle='--')
            ax_main.plot(x, plot_df['BB_LOWER'], color='#8888ff', linewidth=0.8, alpha=0.4, linestyle='--')
            ax_main.fill_between(x, plot_df['BB_LOWER'], plot_df['BB_UPPER'], color='#8888ff', alpha=0.04)

        if buy_signals:
            for sig in buy_signals:
                idx = sig['index']
                if idx < len(df):
                    low = df['Low'].iloc[idx]
                    atr = plot_df['ATR'].iloc[idx] if not pd.isna(plot_df['ATR'].iloc[idx]) else df['Close'].iloc[idx]*0.02
                    ax_main.annotate('▲', xy=(idx, low - atr*0.6), fontsize=14, color='#00ff00', fontweight='bold', ha='center', va='center')
                    label_color = '#00ff00' if 'BO EMA50' in sig['type'] else '#ffff00' if 'BOW' in sig['type'] else '#00ffff'
                    ax_main.text(idx, low - atr*1.3, sig['type'], fontsize=6, color=label_color, fontweight='bold', ha='center', va='top', bbox=dict(facecolor='black', alpha=0.7, edgecolor=label_color, boxstyle='round,pad=0.2'))
        if sell_signals:
            for sig in sell_signals:
                idx = sig['index']
                if idx < len(df):
                    high = df['High'].iloc[idx]
                    atr = plot_df['ATR'].iloc[idx] if not pd.isna(plot_df['ATR'].iloc[idx]) else df['Close'].iloc[idx]*0.02
                    ax_main.annotate('▼', xy=(idx, high + atr*0.6), fontsize=14, color='#ff0000', fontweight='bold', ha='center', va='center')
                    label_color = '#ff4444' if 'BD EMA50' in sig['type'] else '#ffaa00' if 'SOS BB' in sig['type'] else '#ff8888'
                    ax_main.text(idx, high + atr*1.3, sig['type'], fontsize=6, color=label_color, fontweight='bold', ha='center', va='bottom', bbox=dict(facecolor='black', alpha=0.7, edgecolor=label_color, boxstyle='round,pad=0.2'))

        if len(df) > 15:
            box_left = len(df) - 15
            box_right = len(df) - 1
            y_low = df['Low'].iloc[-15:].min() * 0.99
            y_high = df['High'].iloc[-15:].max() * 1.01
            ax_main.plot([box_left, box_right], [y_high, y_high], color='white', linestyle='--', linewidth=0.6, alpha=0.6)
            ax_main.plot([box_left, box_right], [y_low, y_low], color='white', linestyle='--', linewidth=0.6, alpha=0.6)
            ax_main.plot([box_left, box_left], [y_low, y_high], color='white', linestyle='--', linewidth=0.6, alpha=0.6)
            ax_main.plot([box_right, box_right], [y_low, y_high], color='white', linestyle='--', linewidth=0.6, alpha=0.6)

        right_pad = max(3, int(len(df) * 0.04))
        ax_main.set_xlim(-1, len(df) - 1 + right_pad)
        ax_main.set_ylim(df['Low'].min()*0.95, df['High'].max()*1.08)

        left_text = (
            f"Avg Price : {avg_price:,.1f}\n"
            f"Vchg 1 Bar: {vchg1:.1f} x\n"
            f"Vchg 5 Bar: {vchg5:.1f} x\n"
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

        fig.text(0.01, 0.96, f"{symbol} :    {last_close:.0f} ({chg_pct:+.2f}%)", color='#ffff00', fontsize=13, fontweight='bold', ha='left', va='center')
        company = sector_info if sector_info else "IDX Stock"
        fig.text(0.01, 0.93, f"{company}", color='#ffaa00', fontsize=8, ha='left')

        grade_label = extra_info.get('signal_grade')
        grade_color = extra_info.get('signal_grade_color', '#888888')
        if grade_label:
            fig.text(0.01, 0.905 + 0.0, f"● {grade_label}", color=grade_color, fontsize=9, fontweight='bold', ha='left', va='center')

        fig.text(0.5, 0.96, "RAFANO TRADER", color='white', fontsize=14, fontweight='bold', ha='center', va='center')

        if is_intraday_tf(timeframe) and hasattr(df.index[-1], 'strftime'):
            date_str = df.index[-1].strftime('%d %b %Y %H:%M')
        elif hasattr(df.index[-1], 'strftime'):
            date_str = df.index[-1].strftime('%d %b %Y')
        else:
            date_str = get_now_wib().strftime('%d %b %Y')
        fig.text(0.99, 0.96, f"{tf_label_disp} | {date_str}", color='#ffcc00', fontsize=10, ha='right', va='center')
        fig.text(0.99, 0.93, f"Command BOT /C {symbol}", color='white', fontsize=8, ha='right')

        fig.text(0.01, 0.885, f"High:{last_high:.0f}   Low:{last_low:.0f}   Open:{last_open:.0f}   Volume:{last_vol:,.0f}   V1:{df['V1'].iloc[-1]:,.0f}   V2:{df['V2'].iloc[-1]:,.0f}",
                 color='#00ffff', fontsize=8, ha='left')

        ax_main.text(1.005, ema200, f" EMA 200 ", transform=ax_main.get_yaxis_transform(), color='black', backgroundcolor='#a020f0',
                     fontsize=7, fontweight='bold', va='center')
        ax_main.text(1.005, last_close, f" {last_close:.0f} ", transform=ax_main.get_yaxis_transform(),
                     color='black', backgroundcolor='white', fontsize=8, fontweight='bold', va='center')

        vol_info = f"Buy Percent = {buy_pct}%   Sell Percent = {sell_pct}%   Net Vol = {net_vol:,.0f}   Net 5D = {net_vol_5d:,.0f}"
        ax_vol.text(0.005, 0.88, vol_info, transform=ax_vol.transAxes, color='#ffffff', fontsize=8, va='top')
        ax_vol.bar(x, df['Vol_Sell'], color='#cc0000', width=0.8, alpha=0.8, label='Sell')
        ax_vol.bar(x, df['Vol_Buy'], bottom=df['Vol_Sell'], color='#00cc00', width=0.8, alpha=0.9, label='Buy')
        ax_vol.plot(x, df['V1'], color='white', linewidth=0.8, alpha=0.9)
        ax_vol.set_ylim(0, df['Volume'].max()*1.8)
        plt.setp(ax_vol.get_xticklabels(), visible=False)

        nbsa_info = f"NBSA Rp. {nbsa_rp/1e9:.2f} Milyar   NBSA Value : {nbsa_pct:.1f}%"
        ax_nbsa.text(0.005, 0.85, nbsa_info, transform=ax_nbsa.transAxes, color='#ffffff', fontsize=8, va='top')
        nbsa_vals = df['Net_Vol_VSA'].tail(80) / (df['Net_Vol_VSA'].abs().max() or 1) * 50
        x_nbsa = np.arange(len(df)-len(nbsa_vals), len(df))
        for i, v in zip(x_nbsa, nbsa_vals):
            col = '#00ffff' if v >= 0 else '#ff4444'
            ax_nbsa.bar(i, v, color=col, width=0.6)
        ax_nbsa.axhline(0, color='#444444', linewidth=0.5)
        ax_nbsa.set_ylim(-60, 60)

        ax_mm.text(0.005, 0.85, "Market Maker", transform=ax_mm.transAxes, color='#ffffff', fontsize=8, va='top')
        if 'MM' not in df.columns:
            df['MM'] = (df['Close'] - df['EMA50']) / df['EMA50'] * 1000
        mm_vals = df['MM'].tail(80)
        x_mm = np.arange(len(df)-len(mm_vals), len(df))
        ax_mm.bar(x_mm, mm_vals, color='#cccccc', width=0.5, alpha=0.8)
        last_mm = df['MM'].iloc[-1]
        ax_mm.text(1.005, last_mm, f" {last_mm:.4f} ", transform=ax_mm.get_yaxis_transform(),
                   color='black', backgroundcolor='#ffff00', fontsize=7, fontweight='bold', va='center')
        step = max(1, len(df) // 8)
        ax_mm.set_xticks(x[::step])
        if is_intraday_tf(timeframe):
            ax_mm.set_xticklabels([df.index[i].strftime('%H:%M') if hasattr(df.index[i], 'strftime') else str(i) for i in range(0, len(df), step)], fontsize=7)
        else:
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
            hist_df = get_history_pro(sym, limit=120, timeframe="1d")
            multi = get_broker_multi_tf(sym, hist_df)
            accum_val = multi['accum_d']
            broker_net = multi['net_d']
            brokers_combined = multi['brokers']
            broker_status = multi['status']
            analysis = get_analysis(sym)
            score, label, reasons = calculate_score_v2(sym, hist_df, accum_val, broker_net, analysis)
            import datetime
            now_w = datetime.datetime.now(__import__('pytz').timezone('Asia/Jakarta'))
            is_weekend_scan = now_w.weekday() >= 5
            threshold = 20 if is_weekend_scan else (40 if is_fallback else 55)
            if score >= threshold:
                last_close = 0
                change_pct = 0
                if hist_df is not None and len(hist_df) >= 2:
                    last_close = int(hist_df['Close'].iloc[-1])
                    prev = hist_df['Close'].iloc[-2]
                    change_pct = ((last_close/prev)-1)*100 if prev else 0
                tp = calculate_trading_plan(hist_df, multi_tf=multi, timeframe="1d") if hist_df is not None else None
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
        return None

    with ThreadPoolExecutor(max_workers=16) as executor:
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
    header = f"*RAFANO V3 PRO - REAL ACCUM (FIXED SELL)*\n{now_str}\nTotal: {len(signals)} | Cooldown 60m\n============================\n\n"
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
            brokers_d = multi.get('brokers', []) or item.get('brokers', []) or []
            brokers_5d = multi.get('brokers_5d', [])
            brokers_20d = multi.get('brokers_20d', [])
            top_d = format_top_brokers(brokers_d, 3, st_d)
            top_5d = format_top_brokers(brokers_5d, 3, st_5d)
            top_20d = format_top_brokers(brokers_20d, 3, st_20d)
            daily_str = f"{emoji_d} Daily: {st_d} | Buy {fmt(multi.get('buy_d',0))} Sell {fmt(multi.get('sell_d',0))} Net {fmt(multi.get('net_d',0))} | Avg {fmt_avg(multi.get('avg_d',0))}"
            weekly_str = f"{emoji_5d} Weekly 5D: {st_5d} | Buy {fmt(multi.get('buy_5d',0))} Sell {fmt(multi.get('sell_5d',0))} Net {fmt(multi.get('net_5d',0))} | Avg {fmt_avg(multi.get('avg_5d',0))}"
            monthly_str = f"{emoji_20d} Monthly 20D: {st_20d} | Buy {fmt(multi.get('buy_20d',0))} Sell {fmt(multi.get('sell_20d',0))} Net {fmt(multi.get('net_20d',0))} | Avg {fmt_avg(multi.get('avg_20d',0))}"
        else:
            daily_str = f"Akum: {fmt(item.get('accum_value',0))} | Net: {fmt(item.get('broker_net',0))}"
            weekly_str = ""
            monthly_str = ""
        item_str = f"{idx}. *{item['symbol']}* -- {item['close']} ({item['change_pct']:+.2f}%)\n   |- Score: {item['score']}% ({item['score_label']})\n   |- {daily_str}\n   |  └ Top: {top_d}\n   |- {weekly_str}\n   |  └ Top: {top_5d}\n   |- {monthly_str}\n   |  └ Top: {top_20d}\n   +- {reasons_str}\n\n"
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
    tf_label = format_timeframe_label(timeframe)
    send_reply(chat_id, f"📊 *Generating Pro Chart {stock_code.upper()} ({tf_label})...*")
    df = get_history_pro(stock_code, limit=150, timeframe=timeframe)
    if df is None or len(df) < 20:
        send_reply(chat_id, f"⚠ Data {stock_code} tidak ketemu TF {tf_label}")
        return

    if extra_info_cache and stock_code in extra_info_cache and 'multi_tf' in extra_info_cache[stock_code]:
        extra = extra_info_cache[stock_code]
        multi = extra.get('multi_tf', {})
    else:
        multi = get_broker_multi_tf(stock_code, df)
        brokers_cached = multi.get('brokers', [])
        extra = {"accum_value": multi.get('accum_d',0), "broker_net": multi.get('net_d',0), "broker_status": multi.get('status','NEUTRAL'), "brokers": brokers_cached, "multi_tf": multi}

    brokers_d = multi.get('brokers', []) if multi else []
    st_d_tmp = multi.get('status_d','AKUM') if multi else 'AKUM'
    top_d_str = format_top_brokers(brokers_d, 3, st_d_tmp)

    tp = calculate_trading_plan(df, signals=None, multi_tf=multi, timeframe=timeframe)
    side = tp.get('side', 'WAIT') if tp else 'WAIT'
    sig_strength = tp.get('signal_strength', 0) if tp else 0
    grade_label, grade_color = grade_from_strength(sig_strength, side)
    extra['signal_grade'] = grade_label
    extra['signal_grade_color'] = grade_color
    extra['tf_label'] = tf_label

    chart_file = f"chart_{stock_code.upper()}_{timeframe}_{int(time.time())}.png"
    try:
        file_path = generate_pro_chart(df=df, symbol=stock_code.upper(), timeframe=timeframe, sector_info=f"{stock_code.upper()} | IHSG", output_filename=chart_file, extra_info=extra)
        if multi:
            daily_line = f"{multi.get('status_d')} | Buy {format_large_number(multi.get('buy_d',0),True)} Sell {format_large_number(multi.get('sell_d',0),True)} Net {format_large_number(multi.get('net_d',0),True)} Avg {multi.get('avg_d',0):.0f} | {top_d_str}"
            weekly_line = f"{multi.get('status_5d')} | Buy {format_large_number(multi.get('buy_5d',0),True)} Sell {format_large_number(multi.get('sell_5d',0),True)} Net {format_large_number(multi.get('net_5d',0),True)}"
            monthly_line = f"{multi.get('status_20d')} | Buy {format_large_number(multi.get('buy_20d',0),True)} Sell {format_large_number(multi.get('sell_20d',0),True)} Net {format_large_number(multi.get('net_20d',0),True)}"
        else:
            daily_line = f"Akum: {format_large_number(extra.get('accum_value',0), True)} Net: {format_large_number(extra.get('broker_net',0), True)}"
            weekly_line = ""
            monthly_line = ""

        if tp:
            sig_type = tp.get('signal_type','NO SIGNAL')
            sig_reason = tp.get('signal_reason','')
            is_buy = tp.get('is_buy_signal', False)
            is_sell = tp.get('is_sell_signal', False)
            sig_emoji = "🟢" if is_buy else "🔴" if is_sell else ("🟡" if grade_label == "WATCH" else "⚪")
            mtf_confirm = tp.get('mtf_confirm','')
            if side == "WAIT":
                caption = (
                    f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])} | {tp['trend']}\n"
                    f"{sig_emoji} Grade: *{grade_label}* | Score: {sig_strength}%\n"
                    f"Daily: {daily_line}\n"
                    f"Weekly 5D: {weekly_line}\n"
                    f"Monthly 20D: {monthly_line}\n"
                    f"Timeframe: {tf_label}\n"
                    f"------------------\n"
                    f"TRIGGER: {sig_reason}\n"
                    f"------------------\n"
                    f"Sup: {tp['support']} | Res: {tp['resistance']} | ATR: {tp['atr']:.1f}"
                )
            else:
                caption = (
                    f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])} | {tp['trend']}\n"
                    f"{sig_emoji} Grade: *{grade_label}* | Score: {sig_strength}% | MTF: {mtf_confirm}\n"
                    f"Daily: {daily_line}\n"
                    f"Weekly 5D: {weekly_line}\n"
                    f"Monthly 20D: {monthly_line}\n"
                    f"Timeframe: {tf_label}\n"
                    f"------------------\n"
                    f"TRIGGER: {sig_reason}\n"
                    f"------------------\n"
                    f"Entry: {tp['entry']} | SL: {tp['sl']} ({tp['risk_pct']}%)\n"
                    f"TP1: {tp['tp1']} (RR {tp['rr1']}) | TP2: {tp['tp2']} (RR {tp['rr2']})\n"
                )
        else:
            caption = f"*{stock_code.upper()}* -- {safe_int(df['Close'].iloc[-1])}\nGrade: {grade_label} | {daily_line}\nTimeframe: {tf_label}"
        send_photo_reply(chat_id, file_path, caption=caption)
        if os.path.exists(file_path):
            os.remove(file_path)
    except Exception as e:
        import traceback
        traceback.print_exc()
        send_reply(chat_id, f"❌ Gagal render: `{e}`")

LAST_SIGNALS_CACHE = {}

def telegram_bot_listener():
    global LAST_SIGNALS_CACHE
    offset = 0
    print("🤖 Telegram Listener V3 Running...")
    try:
        requests.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteWebhook?drop_pending_updates=true", timeout=10)
    except: pass
    while True:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates?offset={offset}&timeout=20"
            res = requests.get(url, timeout=25)
            if res.status_code != 200:
                time.sleep(3)
                continue
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
                    if first_word in ["/start","/help"]:
                        help_msg = (
                            "🤖 *RAFANO V3 PRO FIXED SELL*\n"
                            "`/c <KODE> [TF]` - Chart Pro + Real Akum\n"
                            "`/b <KODE>` - Detail Bandar\n"
                            "`/scan` - Scan V3\n"
                            "`/clearcache` - Hapus cache\n"
                        )
                        send_reply(chat_id, help_msg)
                    elif first_word in ["/c","/chart","!chart"]:
                        parts = text.split()
                        if len(parts) >=2:
                            sym = parts[1].upper()
                            raw_tf = parts[2] if len(parts)>=3 else "1d"
                            tf_map = {"5":"5m","15":"15m","30":"30m","1h":"1h","4h":"4h","1":"1d","d":"1d","w":"1w","m":"1M","1d":"1d","1w":"1w","5m":"5m","15m":"15m","30m":"30m","1M":"1M"}
                            tf = tf_map.get(raw_tf.lower(), raw_tf.lower())
                            threading.Thread(target=process_chart_request, args=(chat_id, sym, tf, LAST_SIGNALS_CACHE)).start()
                    elif first_word in ["/b","/broker","/bandar"]:
                        parts = text.split()
                        if len(parts) >=2:
                            sym = parts[1].upper()
                            def broker_detail(target_chat, symbol):
                                try:
                                    multi = get_broker_multi_tf(symbol)
                                    msg = f"🏦 *BROKER DETAIL {symbol} - REAL SELL*\n"
                                    msg += f"Daily: {multi.get('status_d')} Net {format_large_number(multi.get('net_d',0),True)}\n"
                                    msg += f"  Buy {format_large_number(multi.get('buy_d',0),True)} Sell {format_large_number(multi.get('sell_d',0),True)} | Avg {multi.get('avg_d',0):.0f}\n"
                                    msg += f"  Top: {format_top_brokers(multi.get('brokers',[]),3,multi.get('status_d'))}\n"
                                    for b in multi.get('brokers',[])[:5]:
                                        msg += f"    - {b.get('broker_code')}: Buy {format_large_number(b.get('buy_value',0),True)} Sell {format_large_number(b.get('sell_value',0),True)} Net {format_large_number(b.get('net_value',0),True)}\n"
                                    msg += f"\nWeekly: {multi.get('status_5d')} Net {format_large_number(multi.get('net_5d',0),True)} Buy {format_large_number(multi.get('buy_5d',0),True)} Sell {format_large_number(multi.get('sell_5d',0),True)}\n"
                                    msg += f"Monthly: {multi.get('status_20d')} Net {format_large_number(multi.get('net_20d',0),True)} Buy {format_large_number(multi.get('buy_20d',0),True)} Sell {format_large_number(multi.get('sell_20d',0),True)}\n"
                                    send_reply(target_chat, msg)
                                except Exception as e:
                                    send_reply(target_chat, f"❌ Error {e}")
                            threading.Thread(target=broker_detail, args=(chat_id, sym)).start()
                    elif first_word in ["/clearcache","/cc","/clear"]:
                        try:
                            BROKER_CACHE.clear()
                            HISTORY_CACHE.clear()
                            SCREENER_CACHE.clear()
                            LAST_SIGNALS_CACHE.clear()
                            if os.path.exists("/tmp/rafano_cache.json"):
                                os.remove("/tmp/rafano_cache.json")
                            send_reply(chat_id, "🧹 Cache cleared")
                        except Exception as e:
                            send_reply(chat_id, f"❌ Error clear: {e}")
                    elif first_word in ["/scan","!scan","/scanpro"]:
                        send_reply(chat_id, "🔍 *V3 Scanning Real Accumulation (FIXED SELL)...*")
                        def manual_scan(target_chat=chat_id):
                            global LAST_SIGNALS_CACHE
                            sigs = scan_v3()
                            LAST_SIGNALS_CACHE = {s['symbol']: s for s in sigs}
                            broadcast_v3(sigs)
                        threading.Thread(target=manual_scan, args=(chat_id,)).start()
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
    print("🔥 RAFANO V3 PRO FIXED SELL STARTING...")
    print("==========================================")
    threading.Thread(target=auto_screener_loop, daemon=True).start()
    telegram_bot_listener()
