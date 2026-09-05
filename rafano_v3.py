"""
RAFANO V3.1 - REVISED & OPTIMIZED
Perbaikan:
- Error handling robust dengan retry & exponential backoff
- Caching untuk mengurangi API calls
- Validasi data (filter fallback palsu)
- Modular structure
- Logging system
- Thread safety dengan Lock
- Trading plan validation
- Rate limiting protection
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
from dotenv import load_dotenv
import json
from threading import Lock

load_dotenv()

# ========== LOGGING CONFIGURATION ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rafano_v3.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ========== CONFIGURATION ==========
TIMEZONE_WIB = pytz.timezone('Asia/Jakarta')

class SignalStrength(Enum):
    VERY_STRONG = "VERY STRONG"
    STRONG_BUY = "STRONG BUY"
    WEAK_BUY = "WEAK BUY"
    NO_SIGNAL = "NO SIGNAL"

@dataclass
class Config:
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    target_chat_id: str = os.getenv("TARGET_CHAT_ID", "")
    arjum_api_key: str = os.getenv("ARJUM_API_KEY", "")
    arjum_base: str = "https://stock.arjum.com/api"
    max_retries: int = 3
    retry_delay: float = 1.0
    request_timeout: int = 12
    max_workers: int = 8
    cooldown_seconds: int = 3600
    chart_dpi: int = 200
    chart_figsize: Tuple[int, int] = (16, 9)

@dataclass
class BrokerData:
    broker_code: str
    buy_value: float = 0.0
    sell_value: float = 0.0
    net_value: float = 0.0
    avg_price: float = 0.0
    buy_volume: float = 0.0
    sell_volume: float = 0.0

@dataclass
class TradingPlan:
    entry: int
    sl: int
    tp1: int
    tp2: int
    atr: float
    risk_pct: float
    rr1: float
    rr2: float
    trend: str
    support: int
    resistance: int
    
    def is_valid(self) -> bool:
        if self.sl >= self.entry:
            return False
        if self.tp1 <= self.entry:
            return False
        if self.tp2 <= self.tp1:
            return False
        if self.risk_pct > 15:  # Max 15% risk
            return False
        return True

@dataclass
class Signal:
    symbol: str
    close: float
    change_pct: float
    score: float
    score_label: str
    accum_value: float
    broker_net: float
    broker_status: str
    reasons: List[str]
    trading_plan: Optional[TradingPlan] = None
    brokers: List[BrokerData] = field(default_factory=list)

# ========== CONFIG LOADING ==========
config = Config()

def safe_get_env(key: str) -> Optional[str]:
    """Safe environment variable retrieval with quote stripping"""
    try:
        value = os.getenv(key)
        if value:
            value = str(value).strip()
            # Strip quotes jika ada
            if len(value) >= 2 and ((value[0] == '"' and value[-1] == '"') or 
                                   (value[0] == "'" and value[-1] == "'")):
                value = value[1:-1].strip()
            return value
    except Exception as e:
        logger.warning(f"Error reading env {key}: {e}")
    
    # Fallback to Colab userdata
    try:
        from google.colab import userdata
        value = userdata.get(key)
        if value:
            value = str(value).strip().strip('"').strip("'")
            os.environ[key] = value
            return value
    except Exception:
        pass
    
    return None

# Validate required configs
def validate_config():
    """Validate required configuration"""
    errors = []
    if not config.telegram_bot_token:
        errors.append("TELEGRAM_BOT_TOKEN is missing")
    if not config.target_chat_id:
        errors.append("TARGET_CHAT_ID is missing")
    if not config.arjum_api_key:
        errors.append("ARJUM_API_KEY is missing")
    
    if errors:
        for err in errors:
            logger.error(f"❌ {err}")
        logger.warning("⚠️ Some features may not work properly")
        return False
    return True

# ========== API CLIENT WITH RETRY & CIRCUIT BREAKER ==========
class ArjumClient:
    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": config.arjum_api_key,
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0"
        })
        self._circuit_state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._failure_count = 0
        self._last_failure_time = 0
        self._lock = Lock()
        
    def _check_circuit(self) -> bool:
        """Check if circuit breaker allows request"""
        with self._lock:
            if self._circuit_state == "OPEN":
                if time.time() - self._last_failure_time > 60:  # 1 minute timeout
                    self._circuit_state = "HALF_OPEN"
                    return True
                return False
            return True
    
    def _record_success(self):
        """Record successful request"""
        with self._lock:
            self._failure_count = 0
            self._circuit_state = "CLOSED"
    
    def _record_failure(self):
        """Record failed request"""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._failure_count >= 5:
                self._circuit_state = "OPEN"
                logger.warning("⚠️ Circuit breaker OPEN - API failing too much")
    
    def get(self, path: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """GET request with retry and circuit breaker"""
        if not self.config.arjum_api_key:
            logger.error("ARJUM_API_KEY not configured")
            return None
        
        if not self._check_circuit():
            logger.warning("Circuit breaker OPEN - request blocked")
            return None
        
        url = f"{self.config.arjum_base}{path}"
        
        for attempt in range(self.config.max_retries):
            try:
                logger.debug(f"Arjum GET {path} (attempt {attempt+1}/{self.config.max_retries})")
                response = self.session.get(
                    url, 
                    params=params, 
                    timeout=self.config.request_timeout
                )
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        self._record_success()
                        return data
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON response: {response.text[:200]}")
                        self._record_failure()
                        if attempt == self.config.max_retries - 1:
                            return None
                elif response.status_code == 429:
                    logger.warning(f"Rate limit hit for {path}")
                    time.sleep(2 ** attempt * 5)  # Backoff 5, 10, 20s
                elif response.status_code >= 500:
                    logger.warning(f"Server error {response.status_code} for {path}")
                    time.sleep(2 ** attempt)
                else:
                    logger.warning(f"API error {response.status_code} for {path}: {response.text[:200]}")
                    self._record_failure()
                    return None
                    
            except requests.Timeout:
                logger.warning(f"Timeout for {path} (attempt {attempt+1})")
                self._record_failure()
                time.sleep(2 ** attempt)
            except requests.ConnectionError:
                logger.warning(f"Connection error for {path} (attempt {attempt+1})")
                self._record_failure()
                time.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"Unexpected error for {path}: {e}")
                self._record_failure()
                if attempt == self.config.max_retries - 1:
                    return None
        
        return None

# ========== GLOBAL STATE ==========
arjum_client = ArjumClient(config)
LAST_SIGNALS_CACHE: Dict[str, Signal] = {}
CACHE_LOCK = Lock()
LAST_SENT_SIGNALS: Dict[str, float] = {}
LAST_RESET_DATE = ""

# ========== HELPER FUNCTIONS ==========
def get_now_wib() -> datetime.datetime:
    return datetime.datetime.now(TIMEZONE_WIB)

def safe_int(val, default: int = 0) -> int:
    try:
        if pd.isna(val) or np.isinf(val):
            return default
        return int(float(val))
    except (ValueError, TypeError):
        return default

def safe_float(val, default: float = 0.0) -> float:
    try:
        if pd.isna(val) or np.isinf(val):
            return default
        return float(val)
    except (ValueError, TypeError):
        return default

def format_large_number(val: float, show_sign: bool = False) -> str:
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

def round_to_ihsg_fraction(price: float) -> int:
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

def is_market_open() -> bool:
    now = get_now_wib()
    weekday = now.weekday()
    if weekday >= 5:
        return False
    current_time = now.time()
    if weekday == 4:  # Jumat
        s1_start, s1_end = datetime.time(9, 0), datetime.time(11, 30)
        s2_start, s2_end = datetime.time(14, 0), datetime.time(15, 50)
    else:
        s1_start, s1_end = datetime.time(9, 0), datetime.time(12, 0)
        s2_start, s2_end = datetime.time(13, 30), datetime.time(15, 50)
    return (s1_start <= current_time <= s1_end) or (s2_start <= current_time <= s2_end)

def escape_markdown(text: str) -> str:
    """Escape Telegram Markdown special characters"""
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text

# ========== DATA PARSING FUNCTIONS ==========
def parse_broker_data(data: Any) -> List[BrokerData]:
    """Robust broker data parsing with validation"""
    brokers = []
    
    if not data:
        return brokers
    
    # Extract list from various formats
    raw_list = []
    if isinstance(data, dict):
        for key in ['data', 'brokers', 'top_brokers', 'result', 'results', 'list', 'summary']:
            if key in data and isinstance(data[key], list):
                raw_list = data[key]
                break
        if not raw_list:
            for value in data.values():
                if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                    raw_list = value
                    break
    elif isinstance(data, list):
        raw_list = data
    
    # Parse each broker
    for item in raw_list[:20]:
        if not isinstance(item, dict):
            continue
        
        try:
            # Extract fields with fallbacks
            code = (
                item.get('broker_code') or 
                item.get('broker') or 
                item.get('code') or 
                item.get('name') or 
                'UNKNOWN'
            ).upper()
            
            buy_val = safe_float(item.get('buy_value') or item.get('buy') or 0)
            sell_val = safe_float(item.get('sell_value') or item.get('sell') or 0)
            buy_vol = safe_float(item.get('buy_volume') or item.get('buy_vol') or 0)
            sell_vol = safe_float(item.get('sell_volume') or item.get('sell_vol') or 0)
            
            # Net value - try multiple fields
            net_val = safe_float(
                item.get('net_value') or 
                item.get('net') or 
                item.get('net_buy') or 
                item.get('value') or 
                item.get('total_value') or 
                item.get('accum') or 
                0
            )
            
            # Calculate net if not provided
            if net_val == 0 and (buy_val or sell_val):
                net_val = buy_val - sell_val
            
            # Average price
            avg_price = safe_float(item.get('avg_price') or item.get('avg') or 0)
            if avg_price == 0 and buy_val > 0 and buy_vol > 0:
                avg_price = buy_val / buy_vol
            
            # Validate and store
            if code != 'UNKNOWN' and (abs(net_val) > 0 or buy_val > 0):
                brokers.append(BrokerData(
                    broker_code=code,
                    buy_value=buy_val,
                    sell_value=sell_val,
                    net_value=net_val,
                    avg_price=avg_price,
                    buy_volume=buy_vol,
                    sell_volume=sell_vol
                ))
        except Exception as e:
            logger.warning(f"Error parsing broker item: {e}")
            continue
    
    # Sort by absolute net value (top accumulators)
    brokers.sort(key=lambda x: abs(x.net_value), reverse=True)
    return brokers

def get_total_accum_from_brokers(brokers: List[BrokerData]) -> float:
    """Calculate total accumulation from broker list"""
    if not brokers:
        return 0.0
    # Sum positive net values (buying)
    total = sum(b.net_value for b in brokers if b.net_value > 0)
    # If all are negative, use absolute sum
    if total == 0:
        total = sum(abs(b.net_value) for b in brokers)
    return total

# ========== API FUNCTIONS ==========
def get_screener_latest() -> List[Dict]:
    """Get latest screener data with validation"""
    data = arjum_client.get("/screener/latest")
    
    if not data:
        logger.warning("Screener API returned no data")
        return []
    
    normalized = []
    
    # Handle different response formats
    if isinstance(data, dict):
        # V5.2 format: {'rows': [{'stock_code': 'IBOS', ...}]}
        if 'rows' in data and isinstance(data['rows'], list):
            for item in data['rows']:
                code = item.get('stock_code') or item.get('symbol') or item.get('code')
                if code:
                    normalized.append({
                        'symbol': str(code).replace('.JK', '').upper(),
                        'raw': item,
                        'bucket': item.get('bucket', ''),
                        'summary': item.get('summary', '')
                    })
        else:
            # Try other common formats
            for key in ['data', 'results', 'stocks', 'items']:
                if key in data and isinstance(data[key], list):
                    for item in data[key]:
                        code = item.get('symbol') or item.get('code') or item.get('stock')
                        if code:
                            normalized.append({
                                'symbol': str(code).replace('.JK', '').upper(),
                                'raw': item
                            })
                    break
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                code = item.get('symbol') or item.get('code') or item.get('stock')
                if code:
                    normalized.append({
                        'symbol': str(code).replace('.JK', '').upper(),
                        'raw': item
                    })
    
    if not normalized:
        logger.warning("No valid stock codes found in screener data")
        return []
    
    logger.info(f"✅ Screener: {len(normalized)} stocks found")
    return normalized

def get_broker_accumulation(symbol: str, top: int = 3, days: int = None) -> Tuple[float, List[BrokerData]]:
    """Get broker accumulation with validation"""
    params = {"top": top}
    if days:
        params["days"] = days
        params["period"] = days
    
    data = arjum_client.get(f"/broker-accumulation/{symbol}", params=params)
    
    if not data:
        logger.warning(f"Broker accumulation API returned no data for {symbol}")
        return 0.0, []
    
    # Parse brokers
    brokers = parse_broker_data(data)
    
    # Calculate accumulation from various sources
    accum = 0.0
    
    # Try to get total from root
    if isinstance(data, dict):
        for key in ['total_accum', 'accumulation', 'net_value', 'total', 'accumulated_value', 'total_value']:
            if key in data and isinstance(data[key], (int, float)):
                accum = safe_float(data[key])
                break
    
    # If no total found, calculate from brokers
    if accum == 0 and brokers:
        accum = get_total_accum_from_brokers(brokers)
    
    # Validate - reject suspicious fallback values
    if accum > 0 and accum < 1000000:  # Less than 1M is suspicious
        logger.warning(f"Unusually low accumulation {accum} for {symbol}, using 0")
        accum = 0
    
    logger.debug(f"Broker accumulation {symbol} {days}d: {format_large_number(accum)}")
    return accum, brokers

def get_broker_summary(symbol: str) -> Tuple[float, str, List[BrokerData]]:
    """Get broker summary with robust parsing"""
    brokers = []
    net_value = 0.0
    status = "NEUTRAL"
    
    # Try with net=true first
    params = {
        "net": "true",
        "broker_limit": 20,
        "level_limit": 25,
        "all_data": "false",
        "flow": "all"
    }
    
    data = arjum_client.get(f"/broker-summary/{symbol}", params=params)
    
    if data:
        parsed_brokers = parse_broker_data(data)
        if parsed_brokers:
            brokers = parsed_brokers
            net_value = get_total_accum_from_brokers(parsed_brokers)
            
            # Try to get total from root
            if isinstance(data, dict):
                for key in ['total_net', 'net_buy', 'net_value', 'net', 'total']:
                    if key in data and isinstance(data[key], (int, float)):
                        net_value = safe_float(data[key])
                        break
    
    # If no data or net=0, try with net=false
    if net_value == 0:
        params["net"] = "false"
        data2 = arjum_client.get(f"/broker-summary/{symbol}", params=params)
        
        if data2:
            parsed_brokers2 = parse_broker_data(data2)
            if parsed_brokers2 and len(parsed_brokers2) > len(brokers):
                brokers = parsed_brokers2
                net_value = get_total_accum_from_brokers(parsed_brokers2)
                
                if isinstance(data2, dict):
                    for key in ['total_net', 'net_buy', 'net_value']:
                        if key in data2 and isinstance(data2[key], (int, float)):
                            net_value = safe_float(data2[key])
                            break
    
    # Determine status
    if net_value > 0:
        status = "ACCUM"
    elif net_value < 0:
        status = "DISTRIB"
    
    logger.debug(f"Broker summary {symbol}: net={format_large_number(net_value)} status={status}")
    return net_value, status, brokers

@lru_cache(maxsize=100)
def get_history_pro(symbol: str, limit: int = 150, timeframe: str = "1d"):
    """Get historical data with caching"""
    tf = timeframe.lower().strip()
    
    # Map timeframe to Arjum format
    arjum_frame_map = {
        "1m": "1min", "1min": "1min",
        "5m": "5min", "5min": "5min",
        "15m": "15min", "15min": "15min",
        "30m": "30min", "30min": "30min",
        "1h": "1hour", "60m": "1hour",
        "4h": "4hour", "4hour": "4hour",
        "1d": "daily", "daily": "daily",
        "1w": "weekly", "weekly": "weekly",
        "1M": "monthly", "1mo": "monthly"
    }
    arjum_frame = arjum_frame_map.get(tf, "daily")
    
    # Try Arjum first
    data = arjum_client.get(f"/history/{symbol}", params={"limit": limit, "frame": arjum_frame})
    
    if data:
        try:
            rows = []
            if isinstance(data, dict):
                for key in ['data', 'history', 'results', 'candles', 'klines']:
                    if key in data and isinstance(data[key], list):
                        rows = data[key]
                        break
            elif isinstance(data, list):
                rows = data
            
            if rows:
                df = pd.DataFrame(rows)
                
                # Rename columns
                rename_map = {}
                for col in df.columns:
                    col_lower = str(col).lower()
                    if col_lower in ['o', 'open', 'open_price']:
                        rename_map[col] = 'Open'
                    elif col_lower in ['h', 'high', 'high_price']:
                        rename_map[col] = 'High'
                    elif col_lower in ['l', 'low', 'low_price']:
                        rename_map[col] = 'Low'
                    elif col_lower in ['c', 'close', 'close_price']:
                        rename_map[col] = 'Close'
                    elif col_lower in ['v', 'volume', 'vol']:
                        rename_map[col] = 'Volume'
                    elif col_lower in ['date', 'time', 't', 'datetime', 'timestamp']:
                        rename_map[col] = 'Date'
                
                df = df.rename(columns=rename_map)
                
                if 'Date' in df.columns:
                    df['Date'] = pd.to_datetime(df['Date'])
                    df = df.set_index('Date')
                
                # Convert to numeric
                for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                df = df.dropna(subset=['Close']).sort_index()
                
                if len(df) >= 10:
                    logger.info(f"✅ History {symbol} {tf}: {len(df)} candles")
                    return df
        except Exception as e:
            logger.warning(f"Error parsing history {symbol}: {e}")
    
    # Fallback to yfinance
    logger.info(f"Arjum history failed for {symbol} {tf}, trying yfinance...")
    try:
        import yfinance as yf
        ticker = f"{symbol}.JK"
        
        yf_map = {
            "1m": ("7d", "1m"),
            "5m": ("5d", "5m"),
            "15m": ("5d", "15m"),
            "30m": ("1mo", "30m"),
            "1h": ("1mo", "60m"),
            "4h": ("3mo", "90m"),
            "1d": ("6mo", "1d"),
            "1w": ("1y", "1wk"),
            "1mo": ("2y", "1mo"),
        }
        period, interval = yf_map.get(tf, ("6mo", "1d"))
        
        yf_ticker = yf.Ticker(ticker)
        hist = yf_ticker.history(period=period, interval=interval)
        
        if hist is not None and len(hist) >= 10:
            logger.info(f"✅ yfinance {symbol} {tf}: {len(hist)} candles")
            return hist.tail(limit)
        else:
            logger.warning(f"yfinance returned insufficient data for {symbol} {tf}")
            return None
    except Exception as e:
        logger.error(f"yfinance error for {symbol}: {e}")
        return None

def get_analysis(symbol: str) -> Dict:
    """Get analysis data"""
    data = arjum_client.get(f"/analysis/{symbol}")
    return data if isinstance(data, dict) else {}

# ========== VSA ANALYSIS ==========
def calculate_vsa_metrics(df: pd.DataFrame) -> Tuple[pd.DataFrame, np.ndarray]:
    """Calculate VSA metrics (OKE SAHAM style) with validation"""
    if df is None or len(df) < 2:
        return df, np.array([])
    
    df = df.copy()
    
    # Calculate price range
    price_range = (df['High'] - df['Low']).replace(0, 0.1)
    
    # Buy% based on close position in candle range
    close_pos = np.clip((df['Close'] - df['Low']) / price_range, 0.05, 0.95)
    buy_ratio = 0.30 + close_pos * 0.60  # 30%-90%
    
    # Volume boost
    if 'V1' not in df.columns:
        df['V1'] = df['Volume'].rolling(20, min_periods=1).mean()
    
    vol_ratio = df['Volume'] / df['V1'].replace(0, 1)
    is_green = df['Close'] >= df['Open']
    
    # Boost for high volume + green candle
    boost = np.where((vol_ratio > 1.5) & is_green, 0.10, 0)
    boost += np.where((vol_ratio > 2.5) & is_green, 0.10, 0)  # Turbo boost
    
    buy_ratio = np.clip(buy_ratio + boost, 0.05, 0.95)
    
    # Calculate volumes
    df['Vol_Buy'] = df['Volume'] * buy_ratio
    df['Vol_Sell'] = df['Volume'] - df['Vol_Buy']
    df['Net_Vol_VSA'] = df['Vol_Buy'] - df['Vol_Sell']
    df['Net_Val_VSA'] = df['Net_Vol_VSA'] * df['Close']
    df['Buy_Pct'] = buy_ratio * 100
    
    return df, buy_ratio

def calculate_emas(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate EMA indicators"""
    if df is None or len(df) < 10:
        return df
    
    df = df.copy()
    df['EMA13'] = df['Close'].ewm(span=13, adjust=False).mean()
    df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
    df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
    df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()
    df['V1'] = df['Volume'].rolling(20, min_periods=1).mean()
    df['V2'] = df['Volume'].rolling(50, min_periods=1).mean()
    return df

def calculate_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Calculate RSI"""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, 0.00001)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(50)

def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate ATR"""
    high, low, close = df['High'], df['Low'], df['Close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=1).mean()

def calculate_trading_plan(df: pd.DataFrame) -> Optional[TradingPlan]:
    """Calculate trading plan with validation"""
    try:
        if df is None or len(df) < 20:
            return None
        
        last_close = df['Close'].iloc[-1]
        last_low = df['Low'].iloc[-1]
        last_high = df['High'].iloc[-1]
        
        # ATR
        atr = calculate_atr(df, 14).iloc[-1]
        if pd.isna(atr) or atr <= 0:
            atr = last_close * 0.03
        
        # EMAs
        ema20 = df['Close'].ewm(span=20).mean().iloc[-1]
        ema50 = df['Close'].ewm(span=50).mean().iloc[-1]
        
        # Support and Resistance
        support = df['Low'].tail(5).min()
        resistance = df['High'].tail(10).max()
        
        # Stop Loss: max of (pivot low, close - 1.2*ATR), minimum 7%
        sl_by_atr = last_close - (1.2 * atr)
        sl = max(min(support, sl_by_atr), last_close * 0.93)
        sl = round_to_ihsg_fraction(sl)
        
        # Entry
        entry = round_to_ihsg_fraction(last_close)
        
        # Take Profit targets
        tp1 = round_to_ihsg_fraction(entry * 1.035)  # 3.5% scalp
        tp2_by_atr = entry + (1.8 * atr)
        tp2_by_pct = entry * 1.07
        tp2 = round_to_ihsg_fraction(max(tp2_by_atr, tp2_by_pct))
        
        # Risk-Reward
        risk = entry - sl
        if risk <= 0:
            return None
        
        rr1 = (tp1 - entry) / risk if risk > 0 else 0
        rr2 = (tp2 - entry) / risk if risk > 0 else 0
        
        # Trend
        if last_close > ema20 and last_close > ema50:
            trend = "UPTREND"
        elif last_close > ema20:
            trend = "WEAK UPTREND"
        else:
            trend = "DOWNTREND"
        
        plan = TradingPlan(
            entry=entry,
            sl=sl,
            tp1=tp1,
            tp2=tp2,
            atr=float(atr),
            risk_pct=round((risk / entry) * 100, 2),
            rr1=round(rr1, 2),
            rr2=round(rr2, 2),
            trend=trend,
            support=safe_int(support),
            resistance=safe_int(resistance)
        )
        
        # Validate plan
        if not plan.is_valid():
            logger.warning(f"Invalid trading plan generated, adjusting...")
            # Adjust if invalid
            if plan.sl >= plan.entry:
                plan.sl = round_to_ihsg_fraction(entry * 0.93)
            if plan.tp1 <= plan.entry:
                plan.tp1 = round_to_ihsg_fraction(entry * 1.025)
            if plan.tp2 <= plan.tp1:
                plan.tp2 = round_to_ihsg_fraction(plan.tp1 * 1.03)
        
        return plan
    except Exception as e:
        logger.error(f"Error calculating trading plan: {e}")
        return None

def calculate_buy_signal_strength(df: pd.DataFrame) -> Tuple[int, str]:
    """Calculate signal strength with scoring"""
    if df is None or len(df) < 20:
        return 0, "NO DATA"
    
    try:
        last_row = df.iloc[-1]
        last_close = last_row['Close']
        last_open = last_row['Open']
        last_vol = last_row['Volume']
        
        # Calculate EMAs and VSA
        df = calculate_emas(df)
        df, buy_ratios = calculate_vsa_metrics(df)
        
        ema50 = df['EMA50'].iloc[-1]
        avg_vol_v1 = df['V1'].iloc[-1]
        last_buy_ratio = buy_ratios[-1] if len(buy_ratios) > 0 else 0.5
        
        net_5d_val = df['Net_Val_VSA'].tail(5).sum()
        vol_multiple = last_vol / avg_vol_v1 if avg_vol_v1 > 0 else 0
        
        # Scoring
        score = 0
        
        if last_close > ema50:
            score += 25
        
        if vol_multiple >= 2.5:
            score += 25
        elif vol_multiple >= 2.0:
            score += 20
        elif vol_multiple >= 1.8:
            score += 15
        
        if last_buy_ratio >= 0.75:
            score += 20
        elif last_buy_ratio >= 0.65:
            score += 15
        elif last_buy_ratio >= 0.55:
            score += 10
        
        if net_5d_val > 0:
            score += 20
        
        if last_close > last_open:
            score += 10
        
        # Label
        if score >= 85:
            label = SignalStrength.VERY_STRONG.value
        elif score >= 70:
            label = SignalStrength.STRONG_BUY.value
        elif score >= 50:
            label = SignalStrength.WEAK_BUY.value
        else:
            label = SignalStrength.NO_SIGNAL.value
        
        return min(100, max(0, score)), label
    except Exception as e:
        logger.error(f"Error calculating signal strength: {e}")
        return 0, "ERROR"

# ========== SCANNER ==========
def scan_v3(limit: int = 30) -> List[Signal]:
    """
    Scan for accumulation signals
    Returns list of validated signals
    """
    logger.info(f"🚀 Starting V3 scan...")
    signals = []
    
    # Get screener data
    screener_data = get_screener_latest()
    
    if not screener_data:
        logger.warning("Screener data empty, using fallback stocks")
        fallback_stocks = ["BBCA", "BBRI", "BMRI", "TLKM", "ASII", "GOTO", "AMMN", "ADRO", "ANTM", "MDKA"]
        candidates = fallback_stocks
    else:
        candidates = [item['symbol'] for item in screener_data[:limit]]
        logger.info(f"Processing {len(candidates)} stocks from screener")
    
    # Process each stock
    processed = 0
    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        futures = {
            executor.submit(_process_stock, symbol): symbol 
            for symbol in candidates
        }
        
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result(timeout=30)
                if result and result.score >= 40:  # Minimum score threshold
                    signals.append(result)
                processed += 1
                if processed % 5 == 0:
                    logger.info(f"Processed {processed}/{len(candidates)} stocks")
            except Exception as e:
                logger.error(f"Error processing {symbol}: {e}")
    
    # Sort by score descending
    signals.sort(key=lambda x: x.score, reverse=True)
    logger.info(f"✅ Scan complete: {len(signals)} signals found")
    
    return signals

def _process_stock(symbol: str) -> Optional[Signal]:
    """Process a single stock for signals"""
    try:
        # Get data
        accum_val, accum_brokers = get_broker_accumulation(symbol, top=3)
        broker_net, broker_status, broker_list = get_broker_summary(symbol)
        
        # Combine brokers
        all_brokers = broker_list if broker_list else accum_brokers
        
        # Get historical data
        hist_df = get_history_pro(symbol, limit=120, timeframe="1d")
        if hist_df is None or len(hist_df) < 20:
            return None
        
        # Calculate score
        score, label = calculate_buy_signal_strength(hist_df)
        
        # Enhance score with broker data
        if accum_val > 20_000_000_000:
            score += 10
        elif accum_val > 5_000_000_000:
            score += 5
        
        if broker_net > 10_000_000_000:
            score += 10
        elif broker_net > 0:
            score += 5
        
        score = min(100, score)
        
        # Reasons
        reasons = []
        if hist_df['Close'].iloc[-1] > hist_df['EMA50'].iloc[-1]:
            reasons.append(">EMA50")
        if accum_val > 5_000_000_000:
            reasons.append(f"Akum {accum_val/1e9:.1f}B")
        if broker_net > 0:
            reasons.append("Net+")
        
        # Trading plan
        trading_plan = calculate_trading_plan(hist_df)
        
        # Price and change
        last_close = float(hist_df['Close'].iloc[-1])
        prev_close = float(hist_df['Close'].iloc[-2]) if len(hist_df) > 1 else last_close
        change_pct = ((last_close / prev_close) - 1) * 100 if prev_close else 0
        
        return Signal(
            symbol=symbol,
            close=last_close,
            change_pct=change_pct,
            score=score,
            score_label=label,
            accum_value=accum_val,
            broker_net=broker_net,
            broker_status=broker_status,
            reasons=reasons,
            trading_plan=trading_plan,
            brokers=all_brokers
        )
    except Exception as e:
        logger.error(f"Error in _process_stock {symbol}: {e}")
        return None

def filter_signals_with_cooldown(signals: List[Signal]) -> List[Signal]:
    """Filter signals with cooldown"""
    global LAST_SENT_SIGNALS, LAST_RESET_DATE
    
    current_time = time.time()
    today_str = get_now_wib().strftime('%Y-%m-%d')
    
    # Reset daily
    if LAST_RESET_DATE != today_str:
        LAST_SENT_SIGNALS.clear()
        LAST_RESET_DATE = today_str
    
    filtered = []
    for signal in signals:
        last_sent = LAST_SENT_SIGNALS.get(signal.symbol, 0)
        if (current_time - last_sent) >= config.cooldown_seconds:
            filtered.append(signal)
            LAST_SENT_SIGNALS[signal.symbol] = current_time
    
    return filtered

# ========== CHART GENERATOR ==========
def generate_pro_chart(df: pd.DataFrame, symbol: str, timeframe: str = "1d", 
                       sector_info: str = "IHSG", output_filename: str = "chart.png",
                       extra_info: Optional[Dict] = None) -> Optional[str]:
    """Generate professional trading chart with all indicators"""
    try:
        import matplotlib
        matplotlib.use('Agg')
        
        extra_info = extra_info or {}
        
        # Validate data
        if df is None or len(df) < 20:
            logger.error(f"Insufficient data for chart: {len(df) if df is not None else 0} candles")
            return None
        
        df = df.copy()
        df = df.ffill().bfill()
        
        if isinstance(df.index, pd.DatetimeIndex):
            df = df.sort_index()
        else:
            df.index = pd.to_datetime(df.index)
        
        # Calculate all indicators
        df = calculate_emas(df)
        df, buy_ratios = calculate_vsa_metrics(df)
        
        # Get latest values
        last_close = float(df['Close'].iloc[-1])
        last_open = float(df['Open'].iloc[-1])
        last_high = float(df['High'].iloc[-1])
        last_low = float(df['Low'].iloc[-1])
        last_vol = float(df['Volume'].iloc[-1])
        prev_close = float(df['Close'].iloc[-2]) if len(df) > 1 else last_close
        chg_pct = ((last_close / prev_close) - 1) * 100 if prev_close else 0
        
        # Left panel metrics
        avg_price = df['Close'].tail(20).mean()
        avg5 = df['Volume'].tail(5).mean()
        vchg1 = last_vol / df['Volume'].iloc[-2] if len(df) > 1 and df['Volume'].iloc[-2] > 0 else 1
        vchg5 = last_vol / avg5 if avg5 > 0 else 1
        
        speed = "FAST" if vchg1 > 2.0 else "SLOW" if vchg1 < 0.8 else "NORMAL"
        
        buy_pct = int(buy_ratios[-1] * 100) if len(buy_ratios) > 0 else 50
        if buy_pct >= 85 and vchg1 >= 1.2:
            power = "TURBO"
        elif buy_pct >= 70 or vchg1 >= 1.5:
            power = "STRONG"
        elif buy_pct >= 60:
            power = "NORMAL"
        else:
            power = "WEAK"
        
        safety = "GOOD" if last_close > df['EMA200'].iloc[-1] else "BAD"
        
        ema13 = df['EMA13'].iloc[-1]
        ema20 = df['EMA20'].iloc[-1]
        ema50 = df['EMA50'].iloc[-1]
        ema200 = df['EMA200'].iloc[-1]
        
        net_vol_5d = df['Net_Vol_VSA'].tail(5).sum()
        
        # Broker data
        broker_net = extra_info.get('broker_net', 0)
        accum_value = extra_info.get('accum_value', 0)
        
        # NBSA
        nbsa_rp = abs(broker_net) if broker_net != 0 else abs(net_vol_5d * last_close)
        nbsa_pct = min(99, max(5, abs(int((broker_net / (accum_value + 1e9)) * 10)))) if accum_value else 30
        
        # Create figure
        plt.style.use('dark_background')
        fig = plt.figure(figsize=config.chart_figsize, dpi=config.chart_dpi, facecolor='#000000')
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
        
        # Draw candles
        for i in range(len(df)):
            o = float(df['Open'].iloc[i])
            h = float(df['High'].iloc[i])
            l = float(df['Low'].iloc[i])
            c = float(df['Close'].iloc[i])
            
            # Wick
            ax_main.plot([i, i], [l, h], color='#00ff00' if c >= o else '#ff0000', 
                        linewidth=0.8, alpha=0.8)
            
            # Body
            body_low = min(o, c)
            body_h = max(0.5, abs(c - o))
            if c >= o:
                rect = patches.Rectangle((i-0.35, body_low), 0.7, body_h, 
                                        facecolor='none', edgecolor='#00ff00', linewidth=0.8)
            else:
                rect = patches.Rectangle((i-0.35, body_low), 0.7, body_h, 
                                        facecolor='#ff3333', edgecolor='#ff3333', linewidth=0.8)
            ax_main.add_patch(rect)
        
        # EMAs
        ax_main.plot(x, df['EMA13'], color='#ffff00', linewidth=1.0, alpha=0.9, label='EMA13')
        ax_main.plot(x, df['EMA20'], color='#ff0000', linewidth=1.0, alpha=0.9, label='EMA20')
        ax_main.plot(x, df['EMA50'], color='#ffffff', linewidth=1.0, alpha=0.9, label='EMA50')
        ax_main.plot(x, df['EMA200'], color='#a020f0', linewidth=1.2, alpha=0.9, label='EMA200')
        
        # Consolidation box
        if len(df) > 15:
            box_left = len(df) - 15
            box_right = len(df) - 1
            y_low = df['Low'].iloc[-15:].min() * 0.99
            y_high = df['High'].iloc[-15:].max() * 1.01
            ax_main.plot([box_left, box_right], [y_high, y_high], color='white', 
                        linestyle='--', linewidth=0.6, alpha=0.6)
            ax_main.plot([box_left, box_right], [y_low, y_low], color='white', 
                        linestyle='--', linewidth=0.6, alpha=0.6)
            ax_main.plot([box_left, box_left], [y_low, y_high], color='white', 
                        linestyle='--', linewidth=0.6, alpha=0.6)
            ax_main.plot([box_right, box_right], [y_low, y_high], color='white', 
                        linestyle='--', linewidth=0.6, alpha=0.6)
        
        ax_main.set_xlim(-1, len(df))
        ax_main.set_ylim(df['Low'].min()*0.95, df['High'].max()*1.08)
        
        # Left panel text
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
        ax_main.text(0.01, 0.98, left_text, transform=ax_main.transAxes, 
                    va='top', ha='left', fontsize=8, family='monospace', color='#e0e0e0',
                    bbox=dict(facecolor='black', alpha=0.6, edgecolor='none'))
        
        # Header
        header_color = '#00ff00' if chg_pct >= 0 else '#ff0000'
        fig.text(0.01, 0.96, f"{symbol} :    {last_close:.0f} ({chg_pct:+.2f}%)", 
                color='#ffff00', fontsize=13, fontweight='bold', ha='left', va='center')
        fig.text(0.01, 0.93, f"{sector_info}", color='#ffaa00', fontsize=8, ha='left')
        fig.text(0.5, 0.96, "RAFANO TRADER", color='white', fontsize=14, 
                fontweight='bold', ha='center', va='center')
        
        date_str = df.index[-1].strftime('%d %b %Y') if hasattr(df.index[-1], 'strftime') else get_now_wib().strftime('%d %b %Y')
        fig.text(0.99, 0.96, f"Daily {date_str}", color='#ffcc00', fontsize=10, ha='right', va='center')
        fig.text(0.99, 0.93, f"Command BOT /C {symbol}", color='white', fontsize=8, ha='right')
        
        # Subheader
        fig.text(0.01, 0.905, 
                f"High:{last_high:.0f}   Low:{last_low:.0f}   Open:{last_open:.0f}   "
                f"Volume:{last_vol:,.0f}   V1:{df['V1'].iloc[-1]:,.0f}   V2:{df['V2'].iloc[-1]:,.0f}",
                color='#00ffff', fontsize=8, ha='left')
        
        # EMA labels
        ax_main.text(1.005, ema200, f" EMA 200 ", transform=ax_main.get_yaxis_transform(), 
                    color='black', backgroundcolor='#a020f0', fontsize=7, fontweight='bold', va='center')
        ax_main.text(1.005, last_close, f" {last_close:.0f} ", transform=ax_main.get_yaxis_transform(),
                    color='black', backgroundcolor='white', fontsize=8, fontweight='bold', va='center')
        
        # Volume panel
        vol_info = f"Buy Percent = {buy_pct}%   Sell Percent = {100-buy_pct}%   Net 5D = {net_vol_5d:,.0f}"
        ax_vol.text(0.005, 0.88, vol_info, transform=ax_vol.transAxes, color='#ffffff', fontsize=8, va='top')
        
        ax_vol.bar(x, df['Vol_Sell'], color='#cc0000', width=0.8, alpha=0.8)
        ax_vol.bar(x, df['Vol_Buy'], bottom=df['Vol_Sell'], color='#00cc00', width=0.8, alpha=0.9)
        ax_vol.plot(x, df['V1'], color='white', linewidth=0.8, alpha=0.9)
        
        if buy_pct >= 85 and vchg1 >= 1.2:
            ax_vol.bar(len(df)-1, df['Volume'].iloc[-1], color='#ffff00', width=0.8, alpha=0.3)
        
        ax_vol.set_ylim(0, df['Volume'].max()*1.8)
        plt.setp(ax_vol.get_xticklabels(), visible=False)
        
        # NBSA panel
        nbsa_info = f"NBSA Rp. {nbsa_rp/1e9:.2f} Milyar   NBSA Value : {nbsa_pct:.1f}%"
        ax_nbsa.text(0.005, 0.85, nbsa_info, transform=ax_nbsa.transAxes, color='#ffffff', fontsize=8, va='top')
        
        nbsa_vals = df['Net_Vol_VSA'].tail(80) / (df['Net_Vol_VSA'].abs().max() or 1) * 50
        x_nbsa = np.arange(len(df)-len(nbsa_vals), len(df))
        for i, v in zip(x_nbsa, nbsa_vals):
            col = '#00ffff' if v >= 0 else '#ff4444'
            ax_nbsa.bar(i, v, color=col, width=0.6)
        ax_nbsa.axhline(0, color='#444444', linewidth=0.5)
        ax_nbsa.set_ylim(-60, 60)
        
        # Market Maker panel
        ax_mm.text(0.005, 0.85, "Market Maker", transform=ax_mm.transAxes, color='#ffffff', fontsize=8, va='top')
        
        if 'MM' not in df.columns:
            df['MM'] = (df['Close'] - df['EMA50']) / df['EMA50'] * 1000
        
        mm_vals = df['MM'].tail(80)
        x_mm = np.arange(len(df)-len(mm_vals), len(df))
        ax_mm.bar(x_mm, mm_vals, color='#cccccc', width=0.5, alpha=0.8)
        
        last_mm = float(df['MM'].iloc[-1])
        ax_mm.text(1.005, last_mm, f" {last_mm:.4f} ", transform=ax_mm.get_yaxis_transform(),
                  color='black', backgroundcolor='#ffff00', fontsize=7, fontweight='bold', va='center')
        ax_mm.set_ylim(df['MM'].min()*1.2 - 10, df['MM'].max()*1.2 + 10)
        
        # X labels
        step = max(1, len(df) // 8)
        ax_mm.set_xticks(x[::step])
        ax_mm.set_xticklabels([df.index[i].strftime('%b') if hasattr(df.index[i], 'strftime') else str(i) 
                              for i in range(0, len(df), step)], fontsize=7)
        
        plt.savefig(output_filename, dpi=config.chart_dpi, bbox_inches='tight', facecolor='#000000')
        plt.close('all')
        
        logger.info(f"✅ Chart generated: {output_filename}")
        return output_filename
        
    except Exception as e:
        logger.error(f"Chart generation error: {e}")
        import traceback
        traceback.print_exc()
        try:
            plt.close('all')
        except:
            pass
        return None

def format_brokers_text(brokers: List[BrokerData], top: int = 3) -> str:
    """Format top brokers for display"""
    if not brokers:
        return "- (No broker data)"
    
    # Sort by net value absolute
    sorted_brokers = sorted(brokers, key=lambda x: abs(x.net_value), reverse=True)
    top_brokers = sorted_brokers[:top]
    
    parts = []
    for b in top_brokers:
        if abs(b.net_value) >= 1e9:
            net_str = f"{b.net_value/1e9:.1f}B"
        elif abs(b.net_value) >= 1e6:
            net_str = f"{b.net_value/1e6:.0f}M"
        else:
            net_str = f"{b.net_value:.0f}"
        parts.append(f"{b.broker_code} {net_str}")
    
    return ", ".join(parts) if parts else "- (No data)"

# ========== TELEGRAM BOT ==========
def send_telegram_message(chat_id: str, text: str, reply_markup: Optional[Dict] = None):
    """Send message via Telegram"""
    if not config.telegram_bot_token:
        logger.error("Telegram token missing")
        return
    
    url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    
    try:
        response = requests.post(url, json=payload, timeout=10)
        if response.status_code != 200:
            logger.warning(f"Telegram send error: {response.text[:200]}")
    except Exception as e:
        logger.error(f"Telegram send error: {e}")

def send_telegram_photo(chat_id: str, photo_path: str, caption: str = ""):
    """Send photo via Telegram"""
    if not config.telegram_bot_token:
        logger.error("Telegram token missing")
        return
    
    url = f"https://api.telegram.org/bot{config.telegram_bot_token}/sendPhoto"
    try:
        with open(photo_path, 'rb') as photo:
            response = requests.post(
                url, 
                data={'chat_id': chat_id, 'caption': caption, 'parse_mode': 'Markdown'},
                files={'photo': photo},
                timeout=30
            )
            if response.status_code != 200:
                logger.warning(f"Photo send error: {response.text[:200]}")
    except Exception as e:
        logger.error(f"Photo send error: {e}")

def broadcast_signals(signals: List[Signal], chat_id: str = None):
    """Broadcast signals to Telegram"""
    if not signals:
        send_telegram_message(
            chat_id or config.target_chat_id,
            "🔍 V3 Scan: Tidak ada sinyal REAL ACCUM hari ini."
        )
        return
    
    chat_id = chat_id or config.target_chat_id
    now_str = get_now_wib().strftime('%d %b %Y %H:%M WIB')
    header = f"*RAFANO V3 PRO - REAL ACCUM*\n{now_str}\nTotal: {len(signals)} | Cooldown 60m\n============================\n\n"
    
    msg = header
    keyboard = []
    
    for idx, signal in enumerate(signals, 1):
        # Trading plan
        tp = signal.trading_plan
        if tp and tp.is_valid():
            tp_str = (
                f"   ├ 🎯 Plan: Entry {tp.entry} | TP1 {tp.tp1} ({tp.rr1}R) | "
                f"TP2 {tp.tp2} ({tp.rr2}R) | SL {tp.sl} ({tp.risk_pct}%)\n"
            )
        else:
            tp_str = ""
        
        # Brokers
        brokers_str = format_brokers_text(signal.brokers, 3)
        reasons_str = " | ".join(signal.reasons[:3]) if signal.reasons else "No reasons"
        
        item_str = (
            f"{idx}. *{signal.symbol}* — {safe_int(signal.close)} ({signal.change_pct:+.2f}%)\n"
            f"   ├ Score: *{signal.score}% ({signal.score_label})*\n"
            f"   ├ Akum 3B: {format_large_number(signal.accum_value)} | "
            f"Net: {format_large_number(signal.broker_net)} ({signal.broker_status})\n"
            f"   ├ Top Brokers: {brokers_str}\n"
            f"{tp_str}"
            f"   └ {reasons_str}\n\n"
        )
        
        keyboard.append([
            {"text": f"📈 {signal.symbol} Pro Chart", "callback_data": f"chart_{signal.symbol}_1d"}
        ])
        
        if len(msg) + len(item_str) > 3500:
            send_telegram_message(chat_id, msg, {"inline_keyboard": keyboard})
            msg = item_str
            keyboard = []
        else:
            msg += item_str
    
    if msg:
        send_telegram_message(chat_id, msg, {"inline_keyboard": keyboard})

def process_chart_request(chat_id: str, stock_code: str, timeframe: str = "1d", 
                          extra_info: Optional[Dict] = None):
    """Process chart generation request"""
    send_telegram_message(
        chat_id,
        f"📊 *Generating Pro Chart {stock_code.upper()} ({timeframe.upper()}) + REAL DATA...*"
    )
    
    # Get data
    df = get_history_pro(stock_code, limit=150, timeframe=timeframe)
    if df is None or len(df) < 20:
        send_telegram_message(
            chat_id,
            f"⚠ Data {stock_code} tidak ketemu TF {timeframe}"
        )
        return
    
    # Get broker data if not provided
    if not extra_info:
        accum_val, accum_brokers = get_broker_accumulation(stock_code, top=3)
        broker_net, broker_status, broker_list = get_broker_summary(stock_code)
        brokers = broker_list if broker_list else accum_brokers
        
        extra_info = {
            "accum_value": accum_val,
            "broker_net": broker_net,
            "broker_status": broker_status,
            "brokers": brokers
        }
    
    # Trading plan
    tp = calculate_trading_plan(df)
    brokers_str = format_brokers_text(extra_info.get('brokers', []), 3)
    
    # Generate chart
    chart_file = f"chart_{stock_code.upper()}_{timeframe}_{int(time.time())}.png"
    
    try:
        file_path = generate_pro_chart(
            df=df,
            symbol=stock_code.upper(),
            timeframe=timeframe,
            sector_info=f"{stock_code.upper()} | IHSG",
            output_filename=chart_file,
            extra_info=extra_info
        )
        
        if file_path and os.path.exists(file_path):
            # Build caption
            caption = (
                f"*{stock_code.upper()}* — {safe_int(df['Close'].iloc[-1])}"
            )
            if tp and tp.is_valid():
                caption += f" | {tp.trend}\n"
                caption += (
                    f"Score REAL: {extra_info.get('score', 0)}% | "
                    f"Akum: {format_large_number(extra_info.get('accum_value', 0), True)}\n"
                    f"Net Broker: {format_large_number(extra_info.get('broker_net', 0), True)} "
                    f"({extra_info.get('broker_status', 'NEUTRAL')})\n"
                    f"Top Brokers: {brokers_str}\n"
                    f"Timeframe: {timeframe.upper()}\n"
                    f"──────────────────\n"
                    f"🎯 *TRADING PLAN*\n"
                    f"Entry: {tp.entry} | SL: {tp.sl} ({tp.risk_pct}%)\n"
                    f"TP1: {tp.tp1} (RR {tp.rr1}) | TP2: {tp.tp2} (RR {tp.rr2})\n"
                    f"Sup: {tp.support} | Res: {tp.resistance} | ATR: {tp.atr:.1f}"
                )
            else:
                caption += "\n"
                caption += (
                    f"Score REAL: {extra_info.get('score', 0)}% | "
                    f"Akum: {format_large_number(extra_info.get('accum_value', 0), True)}\n"
                    f"Net Broker: {format_large_number(extra_info.get('broker_net', 0), True)} "
                    f"({extra_info.get('broker_status', 'NEUTRAL')})\n"
                    f"Top Brokers: {brokers_str}\n"
                    f"Timeframe: {timeframe.upper()}"
                )
            
            send_telegram_photo(chat_id, file_path, caption=caption)
            os.remove(file_path)
        else:
            send_telegram_message(chat_id, f"❌ Gagal generate chart untuk {stock_code}")
            
    except Exception as e:
        logger.error(f"Chart request error: {e}")
        send_telegram_message(chat_id, f"❌ Error: `{str(e)[:100]}`")
        if os.path.exists(chart_file):
            os.remove(chart_file)

# ========== TELEGRAM LISTENER ==========
def telegram_bot_listener():
    """Listen for Telegram commands"""
    if not config.telegram_bot_token:
        logger.error("Telegram bot token missing, listener stopped")
        return
    
    # Delete webhook
    try:
        requests.get(
            f"https://api.telegram.org/bot{config.telegram_bot_token}/deleteWebhook?drop_pending_updates=true",
            timeout=10
        )
        logger.info("✅ Webhook deleted, polling mode active")
    except Exception as e:
        logger.warning(f"Webhook delete failed: {e}")
    
    # Test bot
    try:
        response = requests.get(
            f"https://api.telegram.org/bot{config.telegram_bot_token}/getMe",
            timeout=10
        )
        if response.status_code == 200:
            bot_info = response.json().get('result', {})
            logger.info(f"✅ Bot connected: @{bot_info.get('username', 'unknown')}")
        else:
            logger.error(f"❌ Bot token invalid: {response.text[:200]}")
            return
    except Exception as e:
        logger.error(f"❌ Bot connection error: {e}")
        return
    
    offset = 0
    while True:
        try:
            url = f"https://api.telegram.org/bot{config.telegram_bot_token}/getUpdates"
            params = {"offset": offset, "timeout": 20}
            
            response = requests.get(url, params=params, timeout=25)
            if response.status_code != 200:
                logger.warning(f"getUpdates error: {response.status_code}")
                time.sleep(3)
                continue
            
            data = response.json()
            for update in data.get("result", []):
                offset = update["update_id"] + 1
                
                # Handle callback queries
                if "callback_query" in update:
                    cb = update["callback_query"]
                    cb_id = cb.get("id")
                    cb_data = cb.get("data", "")
                    chat_id = str(cb["message"]["chat"]["id"])
                    
                    # Answer callback
                    try:
                        requests.post(
                            f"https://api.telegram.org/bot{config.telegram_bot_token}/answerCallbackQuery",
                            json={"callback_query_id": cb_id},
                            timeout=5
                        )
                    except Exception:
                        pass
                    
                    if cb_data.startswith("chart_"):
                        parts = cb_data.split("_")
                        if len(parts) >= 3:
                            sym = parts[1]
                            tf = parts[2]
                            threading.Thread(
                                target=process_chart_request,
                                args=(chat_id, sym, tf, LAST_SIGNALS_CACHE.get(sym))
                            ).start()
                
                # Handle messages
                elif "message" in update and "text" in update["message"]:
                    msg = update["message"]
                    text = msg.get("text", "").strip()
                    chat_id = str(msg["chat"]["id"])
                    
                    if not text:
                        continue
                    
                    first_word = text.split()[0].lower() if text else ""
                    logger.info(f"📩 Command: {text} from {chat_id}")
                    
                    if first_word in ["/start", "/help"]:
                        help_msg = (
                            "🤖 *RAFANO V3 PRO*\n"
                            "============================\n"
                            "Perintah:\n"
                            "📈 `/c <KODE> [TF]` - Chart Pro + Real Akum\n"
                            "   Contoh: `/c BBCA` `/c ANTM 15m`\n"
                            "🔍 `/scan` - Scan V3 Real Accumulation\n"
                            "🔥 `/scanpro` - Scan + langsung chart top 3\n"
                            "⏱ `/status` - Cek status bot\n"
                        )
                        send_telegram_message(chat_id, help_msg)
                    
                    elif first_word in ["/c", "/chart", "!chart"]:
                        parts = text.split()
                        if len(parts) >= 2:
                            sym = parts[1].upper()
                            tf = parts[2] if len(parts) >= 3 else "1d"
                            threading.Thread(
                                target=process_chart_request,
                                args=(chat_id, sym, tf)
                            ).start()
                        else:
                            send_telegram_message(chat_id, "⚠ Format: `/c <KODE> [TF]`")
                    
                    elif first_word in ["/scan", "!scan", "/scanpro"]:
                        send_telegram_message(chat_id, "🔍 *V3 Scanning Real Accumulation...*")
                        
                        def manual_scan(is_pro=False, target_chat=chat_id):
                            with CACHE_LOCK:
                                signals = scan_v3()
                                LAST_SIGNALS_CACHE.update({s.symbol: s for s in signals})
                            
                            # Filter signals (skip cooldown for manual)
                            if signals:
                                broadcast_signals(signals, target_chat)
                                
                                if is_pro:
                                    for signal in signals[:3]:
                                        process_chart_request(
                                            target_chat, 
                                            signal.symbol, 
                                            "1d",
                                            {k: getattr(signal, k) for k in ['accum_value', 'broker_net', 'broker_status', 'brokers']}
                                        )
                                        time.sleep(1)
                            else:
                                send_telegram_message(
                                    target_chat,
                                    f"🔍 Scan {get_now_wib().strftime('%H:%M')}: 0 sinyal"
                                )
                        
                        is_pro = (first_word == "/scanpro")
                        threading.Thread(target=manual_scan, args=(is_pro, chat_id)).start()
                    
                    elif first_word == "/status":
                        status_msg = (
                            f"📊 *RAFANO V3 Status*\n"
                            f"⏱ Time: {get_now_wib().strftime('%Y-%m-%d %H:%M:%S WIB')}\n"
                            f"📈 Market: {'🟢 OPEN' if is_market_open() else '🔴 CLOSED'}\n"
                            f"📊 Cached Signals: {len(LAST_SIGNALS_CACHE)}\n"
                            f"💾 Cooldown: {config.cooldown_seconds}s\n"
                            f"🌐 API: {'✅ Connected' if config.arjum_api_key else '❌ No API Key'}\n"
                        )
                        send_telegram_message(chat_id, status_msg)
            
        except Exception as e:
            logger.error(f"Listener error: {e}")
            time.sleep(5)

# ========== AUTO SCREENER LOOP ==========
def auto_screener_loop():
    """Automatic screener with scheduled runs"""
    logger.info("🚀 Auto Screener V3 Active...")
    last_triggered_sesi1 = ""
    last_triggered_eod = ""
    last_real_time = ""
    
    while True:
        try:
            if not is_market_open():
                time.sleep(60)
                continue
            
            now = get_now_wib()
            today_str = now.strftime('%Y-%m-%d')
            current_time_str = now.strftime('%H:%M')
            weekday = now.weekday()
            
            # Session 1 trigger
            target_sesi1 = "11:25" if weekday == 4 else "11:55"
            if current_time_str == target_sesi1 and last_triggered_sesi1 != today_str:
                logger.info("⏰ Session 1 trigger")
                signals = scan_v3()
                with CACHE_LOCK:
                    LAST_SIGNALS_CACHE.update({s.symbol: s for s in signals})
                filtered = filter_signals_with_cooldown(signals)
                broadcast_signals(filtered)
                last_triggered_sesi1 = today_str
            
            # End of day trigger
            if current_time_str == "15:55" and last_triggered_eod != today_str:
                logger.info("⏰ End of day trigger")
                signals = scan_v3()
                with CACHE_LOCK:
                    LAST_SIGNALS_CACHE.update({s.symbol: s for s in signals})
                filtered = filter_signals_with_cooldown(signals)
                broadcast_signals(filtered)
                last_triggered_eod = today_str
            
            # Real-time scan every 10 minutes (but not too frequent)
            if current_time_str.endswith('0') and last_real_time != current_time_str:
                logger.info("⏰ Real-time scan")
                signals = scan_v3()
                with CACHE_LOCK:
                    LAST_SIGNALS_CACHE.update({s.symbol: s for s in signals})
                filtered = filter_signals_with_cooldown(signals)
                if filtered:
                    broadcast_signals(filtered)
                last_real_time = current_time_str
            
            time.sleep(10)  # Check every 10 seconds
            
        except Exception as e:
            logger.error(f"Auto loop error: {e}")
            time.sleep(30)

# ========== MAIN ==========
def main():
    """Main entry point"""
    print("=" * 50)
    print("🔥 RAFANO V3.1 - REVISED & OPTIMIZED")
    print("=" * 50)
    
    # Validate config
    if not validate_config():
        print("⚠️ Configuration issues found. Some features may not work.")
    
    # Start auto screener
    threading.Thread(target=auto_screener_loop, daemon=True).start()
    logger.info("✅ Auto screener started")
    
    # Start Telegram listener
    telegram_bot_listener()

if __name__ == "__main__":
    main()
