import asyncio
import aiohttp
import sqlite3
import time
import traceback
import math
import json
import websockets
import statistics

# =========================
# CONFIG
# =========================

# SET TO True TO FLOOD TELEGRAM AND TEST, SET TO False TO RESUME NORMAL SNIPING
TEST_MODE = True 

CMC_API_KEY = "8a6ea4b9c73c45e8adac9e14f214f087"
TELEGRAM_BOT_TOKEN = "8703361353:AAFZoJu8UWVLxsuSwSzzeQ-9cPHnUIH6QSQ"
TELEGRAM_CHAT_IDS = ["7198809557", "6065933220"]

GATEIO_TICKERS = "https://api.gateio.ws/api/v4/spot/tickers"
MEXC_TICKERS = "https://api.mexc.com/api/v3/ticker/24hr"
CMC_LISTINGS = "https://api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

GATEIO_MIN_VOLUME = 5_000_000
MEXC_MIN_VOLUME = 5_000_000
CMC_MIN_MARKETCAP = 10_000_000
CMC_MIN_VOLUME = 1_000_000

PRICE_SPIKE_PERCENT = 5
VOLUME_SPIKE_PERCENT = 20

CHECK_INTERVAL = 60
COOLDOWN = 60 * 60

# Futures specific config
GATEIO_FUTURES_TICKERS = "https://api.gateio.ws/api/v4/futures/usdt/tickers"
MEXC_FUTURES_TICKERS = "https://contract.mexc.com/api/v1/contract/ticker"
FUTURES_MIN_VOLUME = 10_000_000
FUTURES_PRICE_CHANGE_THRESHOLD = 5

# Kline config
KLINE_INTERVAL = "5m"
MEXC_KLINE_INTERVAL = "Min5"
KLINE_LIMIT = 200
ATR_PERIOD = 14
LOOKBACK_200 = 200

# Binance Futures config
BINANCE_FUTURES_TICKERS = "https://fapi.binance.com/fapi/v1/ticker/24hr"
BINANCE_FUTURES_KLINES = "https://fapi.binance.com/fapi/v1/klines"
BINANCE_KLINE_INTERVAL = "5m"
BINANCE_KLINE_LIMIT = 200
BINANCE_WS_BASE = "wss://fstream.binance.com/stream?streams="
BINANCE_PRICE_CHANGE_THRESHOLD = 1.5
BINANCE_VOLUME_SPIKE_PERCENT = 12

BIG_COINS = ["BTC", "ETH", "SOL"]
BIG_COIN_PRICE_THRESHOLD = 2

# OWFAH / Order Flow Config
CONFLUENCE_THRESHOLD = 3
SVP_ATR_MULT = 0.25
PROXIMITY_ATR_MULT = 0.25
INVALIDATION_BUY = 1.0015
INVALIDATION_SELL = 0.9985

# =========================
# TIMEFRAME (EDITABLE VIA TELEGRAM)
# =========================

CURRENT_TIMEFRAME = "1hr"
ALLOWED_TIMEFRAMES = ("5m", "15m", "30m", "1hr", "4h", "1d", "1w", "1M")

TIMEFRAME_MAP = {
    "5m":  {"gateio": "5m",  "mexc": "Min5",  "binance": "5m"},
    "15m": {"gateio": "15m", "mexc": "Min15", "binance": "15m"},
    "30m": {"gateio": "30m", "mexc": "Min30", "binance": "30m"},
    "1hr": {"gateio": "1h",  "mexc": "Min60", "binance": "1h"},
    "4h":  {"gateio": "4h",  "mexc": "Hour4", "binance": "4h"},
    "1d":  {"gateio": "1d",  "mexc": "Day1",  "binance": "1d"},
    "1w":  {"gateio": "7d",  "mexc": "Week1", "binance": "1w"},
    "1M":  {"gateio": "30d", "mexc": "Month1","binance": "1M"},
}

def get_tf_gateio():
    return TIMEFRAME_MAP[CURRENT_TIMEFRAME]["gateio"]

def get_tf_mexc():
    return TIMEFRAME_MAP[CURRENT_TIMEFRAME]["mexc"]

def get_tf_binance():
    return TIMEFRAME_MAP[CURRENT_TIMEFRAME]["binance"]

# =========================
# WALL TRACKER & WATCHLIST STATE
# =========================

BTC_WATCHLIST = {
    "highest_bid_wall": {"price": 0.0, "vol": 0.0},
    "highest_ask_wall": {"price": 0.0, "vol": 0.0}
}

# =========================
# TRADINGVIEW URL HELPER
# =========================

def tradingview_url(exchange, raw_symbol):
    if exchange == "gateio_spot":
        base = raw_symbol.replace("_", "")
        tv_sym = f"GATEIO:{base}"
    elif exchange == "gateio_fut":
        base = raw_symbol.replace("_", "")
        tv_sym = f"GATEIO:{base}.P"
    elif exchange == "mexc_spot":
        tv_sym = f"MEXC:{raw_symbol}"
    elif exchange == "mexc_fut":
        base = raw_symbol.replace("_", "")
        tv_sym = f"MEXC:{base}.P"
    elif exchange == "binance_fut":
        tv_sym = f"BINANCE:{raw_symbol}.P"
    elif exchange == "cmc":
        tv_sym = f"BINANCE:{raw_symbol}USDT"
    else:
        tv_sym = raw_symbol
    return f"https://www.tradingview.com/chart/?symbol={tv_sym}"

# =========================
# ADVANCED INDICATOR HELPERS
# =========================

def calculate_atr(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        trs.append(tr)
    return sum(trs[-period:]) / period

def calculate_vwap_bands(highs, lows, closes, volumes):
    if not volumes or len(volumes) != len(closes) or sum(volumes) == 0:
        return None, 0.0, 0.0, 0.0
    typical_prices = [(h + l + c) / 3 for h, l, c in zip(highs, lows, closes)]
    vwap = sum(tp * v for tp, v in zip(typical_prices, volumes)) / sum(volumes)
    
    variance = sum(v * (tp - vwap)**2 for tp, v in zip(typical_prices, volumes)) / sum(volumes)
    std_dev = math.sqrt(variance)
    
    slope = 0.0
    if len(typical_prices) >= 5:
        slope = typical_prices[-1] - typical_prices[-5]
        
    return vwap, std_dev, slope, typical_prices

def calculate_poc_va(highs, lows, closes, volumes):
    if not volumes or len(volumes) != len(closes):
        return None, None, None
        
    min_p = min(lows)
    max_p = max(highs)
    if max_p - min_p == 0:
        return closes[-1], closes[-1], closes[-1]
        
    num_bins = 50
    bin_size = (max_p - min_p) / num_bins
    bins = {}
    
    for h, l, c, v in zip(highs, lows, closes, volumes):
        bin_idx = int((c - min_p) / bin_size)
        bin_idx = max(0, min(bin_idx, num_bins - 1))
        bins[bin_idx] = bins.get(bin_idx, 0) + v
        
    if not bins:
        return None, None, None
        
    sorted_bins = sorted(bins.items(), key=lambda x: x[1], reverse=True)
    poc_idx = sorted_bins[0][0]
    poc = min_p + (poc_idx + 0.5) * bin_size
    
    total_vol = sum(bins.values())
    target_vol = total_vol * 0.70
    current_vol = 0
    va_prices = []
    
    for idx, vol in sorted_bins:
        current_vol += vol
        va_prices.append(min_p + (idx + 0.5) * bin_size)
        if current_vol >= target_vol:
            break
            
    if not va_prices:
        return poc, poc, poc
        
    vah = max(va_prices)
    val = min(va_prices)
    return poc, vah, val

def detect_liquidity_wall(bids, asks):
    bid_vols = [abs(float(b[1])) for b in bids if len(b) >= 2]
    ask_vols = [abs(float(a[1])) for a in asks if len(a) >= 2]
    all_vols = bid_vols + ask_vols
    
    if not all_vols:
        return False, False, 0, 0, 0, 0
        
    median_vol = statistics.median(all_vols)
    
    max_bid_vol = max(bid_vols) if bid_vols else 0
    max_ask_vol = max(ask_vols) if ask_vols else 0
    
    max_bid_price = float(bids[bid_vols.index(max_bid_vol)][0]) if bid_vols else 0
    max_ask_price = float(asks[ask_vols.index(max_ask_vol)][0]) if ask_vols else 0
    
    bid_wall = max_bid_vol > 5 * median_vol and max_bid_vol * max_bid_price > 50000
    ask_wall = max_ask_vol > 5 * median_vol and max_ask_vol * max_ask_price > 50000
    
    return bid_wall, ask_wall, max_bid_vol, max_bid_price, max_ask_vol, max_ask_price

def calculate_cvd_divergence(closes, deltas):
    if len(closes) < 20:
        return False
    price_up = closes[-1] > closes[-20]
    cvd_sum = sum(deltas[-20:])
    price_down = closes[-1] < closes[-20]
    
    if (price_up and cvd_sum < 0) or (price_down and cvd_sum > 0):
        return True
    return False

def calculate_dynamic_d_vol(deltas, volumes, current_idx):
    if len(deltas) < 200 or len(volumes) < 200:
        return 25.0
    ratios = []
    for i in range(max(0, current_idx-200), current_idx):
        if volumes[i] > 0:
            ratios.append(abs(deltas[i]) / volumes[i] * 100)
    if not ratios:
        return 25.0
    ratios.sort()
    threshold_idx = int(len(ratios) * 0.90)
    return ratios[threshold_idx]

def format_cvd(cvd):
    a = abs(cvd)
    if a >= 1_000_000:
        return f"{cvd/1_000_000:+.2f}M"
    elif a >= 1_000:
        return f"{cvd/1_000:+.1f}K"
    elif a >= 10:
        return f"{cvd:+.2f}"
    else:
        return f"{cvd:+.4f}"

def format_footprint(fp):
    return f"{fp:+.2f}"

def format_of_line(of, confluence_score, details):
    fp = of.get('footprint', 0.0)
    cvd = of.get('cvd', 0.0)
    mb_vol = of.get('max_bid_vol', 0.0)
    mb_price = of.get('max_bid_price', 0.0)
    ma_vol = of.get('max_ask_vol', 0.0)
    ma_price = of.get('max_ask_price', 0.0)
    
    wall_str = f" │ Walls: B {mb_vol:.1f}@{mb_price:.4f} / A {ma_vol:.1f}@{ma_price:.4f}"
    base_str = f"📊 OF: FP {format_footprint(fp)} │ CVD {format_cvd(cvd)}{wall_str}"
    base_str += f"\n🎯 Confluence: {confluence_score}/5 ({details})"
    return base_str

# =========================
# API FETCHERS
# =========================

async def fetch_gateio_spot_orderflow(symbol):
    result = {'footprint': 0.0, 'cvd': 0.0, 'max_bid_vol': 0.0, 'max_bid_price': 0.0, 'max_ask_vol': 0.0, 'max_ask_price': 0.0, 'bids': [], 'asks': []}
    async with aiohttp.ClientSession() as session:
        try:
            url = f"https://api.gateio.ws/api/v4/spot/order_book?currency_pair={symbol}&limit=20"
            async with session.get(url) as resp:
                if resp.status == 200:
                    book = await resp.json()
                    bids = book.get('bids', [])
                    asks = book.get('asks', [])
                    result['bids'] = bids
                    result['asks'] = asks
                    bid_vol = sum(float(b[1]) for b in bids if len(b) >= 2)
                    ask_vol = sum(float(a[1]) for a in asks if len(a) >= 2)
                    total = bid_vol + ask_vol
                    if total > 0:
                        result['footprint'] = (bid_vol - ask_vol) / total
                    if bids: result['max_bid_vol'], result['max_bid_price'] = max((abs(float(b[1])), float(b[0])) for b in bids if len(b) >= 2)
                    if asks: result['max_ask_vol'], result['max_ask_price'] = max((abs(float(a[1])), float(a[0])) for a in asks if len(a) >= 2)
        except Exception:
            pass
        try:
            url = f"https://api.gateio.ws/api/v4/spot/trades?currency_pair={symbol}&limit=100"
            async with session.get(url) as resp:
                if resp.status == 200:
                    trades = await resp.json()
                    cvd = 0.0
                    for t in trades:
                        amt = float(t.get('amount', 0))
                        side = (t.get('side') or '').lower()
                        if side == 'buy': cvd += amt
                        elif side == 'sell': cvd -= amt
                    result['cvd'] = cvd
        except Exception:
            pass
    return result

async def fetch_gateio_fut_orderflow(symbol):
    result = {'footprint': 0.0, 'cvd': 0.0, 'max_bid_vol': 0.0, 'max_bid_price': 0.0, 'max_ask_vol': 0.0, 'max_ask_price': 0.0, 'bids': [], 'asks': []}
    async with aiohttp.ClientSession() as session:
        try:
            url = f"https://api.gateio.ws/api/v4/futures/usdt/order_book?contract={symbol}&limit=20"
            async with session.get(url) as resp:
                if resp.status == 200:
                    book = await resp.json()
                    bids = book.get('bids', [])
                    asks = book.get('asks', [])
                    result['bids'] = bids
                    result['asks'] = asks
                    bid_vol = sum(abs(float(b[1])) for b in bids if len(b) >= 2)
                    ask_vol = sum(abs(float(a[1])) for a in asks if len(a) >= 2)
                    total = bid_vol + ask_vol
                    if total > 0:
                        result['footprint'] = (bid_vol - ask_vol) / total
                    if bids: result['max_bid_vol'], result['max_bid_price'] = max((abs(float(b[1])), float(b[0])) for b in bids if len(b) >= 2)
                    if asks: result['max_ask_vol'], result['max_ask_price'] = max((abs(float(a[1])), float(a[0])) for a in asks if len(a) >= 2)
        except Exception:
            pass
        try:
            url = f"https://api.gateio.ws/api/v4/futures/usdt/trades?contract={symbol}&limit=100"
            async with session.get(url) as resp:
                if resp.status == 200:
                    trades = await resp.json()
                    cvd = 0.0
                    for t in trades:
                        size = float(t.get('size', 0))
                        cvd += size
                    result['cvd'] = cvd
        except Exception:
            pass
    return result

async def fetch_mexc_spot_orderflow(symbol):
    result = {'footprint': 0.0, 'cvd': 0.0, 'max_bid_vol': 0.0, 'max_bid_price': 0.0, 'max_ask_vol': 0.0, 'max_ask_price': 0.0, 'bids': [], 'asks': []}
    async with aiohttp.ClientSession() as session:
        try:
            url = f"https://api.mexc.com/api/v3/depth?symbol={symbol}&limit=20"
            async with session.get(url) as resp:
                if resp.status == 200:
                    book = await resp.json()
                    bids = book.get('bids', [])
                    asks = book.get('asks', [])
                    result['bids'] = bids
                    result['asks'] = asks
                    bid_vol = sum(float(b[1]) for b in bids if len(b) >= 2)
                    ask_vol = sum(float(a[1]) for a in asks if len(a) >= 2)
                    total = bid_vol + ask_vol
                    if total > 0:
                        result['footprint'] = (bid_vol - ask_vol) / total
                    if bids: result['max_bid_vol'], result['max_bid_price'] = max((abs(float(b[1])), float(b[0])) for b in bids if len(b) >= 2)
                    if asks: result['max_ask_vol'], result['max_ask_price'] = max((abs(float(a[1])), float(a[0])) for a in asks if len(a) >= 2)
        except Exception:
            pass
        try:
            url = f"https://api.mexc.com/api/v3/trades?symbol={symbol}&limit=100"
            async with session.get(url) as resp:
                if resp.status == 200:
                    trades = await resp.json()
                    cvd = 0.0
                    for t in trades:
                        qty = float(t.get('qty', 0))
                        if t.get('isBuyerMaker', True): cvd -= qty
                        else: cvd += qty
                    result['cvd'] = cvd
        except Exception:
            pass
    return result

async def fetch_mexc_fut_orderflow(symbol):
    result = {'footprint': 0.0, 'cvd': 0.0, 'max_bid_vol': 0.0, 'max_bid_price': 0.0, 'max_ask_vol': 0.0, 'max_ask_price': 0.0, 'bids': [], 'asks': []}
    async with aiohttp.ClientSession() as session:
        try:
            url = f"https://contract.mexc.com/api/v1/contract/depth/{symbol}?limit=20"
            async with session.get(url) as resp:
                if resp.status == 200:
                    book = await resp.json()
                    if book.get('success'):
                        data = book.get('data', {})
                        bids = data.get('bids', [])
                        asks = data.get('asks', [])
                        result['bids'] = bids
                        result['asks'] = asks
                        bid_vol = sum(abs(float(b[1])) for b in bids if len(b) >= 2)
                        ask_vol = sum(abs(float(a[1])) for a in asks if len(a) >= 2)
                        total = bid_vol + ask_vol
                        if total > 0:
                            result['footprint'] = (bid_vol - ask_vol) / total
                        if bids: result['max_bid_vol'], result['max_bid_price'] = max((abs(float(b[1])), float(b[0])) for b in bids if len(b) >= 2)
                        if asks: result['max_ask_vol'], result['max_ask_price'] = max((abs(float(a[1])), float(a[0])) for a in asks if len(a) >= 2)
        except Exception:
            pass
        try:
            url = f"https://contract.mexc.com/api/v1/contract/deals/{symbol}?limit=100"
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('success'):
                        trades = data.get('data', [])
                        cvd = 0.0
                        for t in trades:
                            vol = float(t.get('v', 0))
                            side = int(t.get('T', 0))
                            if side == 1: cvd -= vol
                            elif side == 2: cvd += vol
                        result['cvd'] = cvd
        except Exception:
            pass
    return result

async def fetch_binance_fut_orderflow(symbol):
    result = {'footprint': 0.0, 'cvd': 0.0, 'max_bid_vol': 0.0, 'max_bid_price': 0.0, 'max_ask_vol': 0.0, 'max_ask_price': 0.0, 'bids': [], 'asks': []}
    async with aiohttp.ClientSession() as session:
        try:
            url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=20"
            async with session.get(url) as resp:
                if resp.status == 200:
                    book = await resp.json()
                    bids = book.get('bids', [])
                    asks = book.get('asks', [])
                    result['bids'] = bids
                    result['asks'] = asks
                    bid_vol = sum(float(b[1]) for b in bids if len(b) >= 2)
                    ask_vol = sum(float(a[1]) for a in asks if len(a) >= 2)
                    total = bid_vol + ask_vol
                    if total > 0:
                        result['footprint'] = (bid_vol - ask_vol) / total
                    if bids: result['max_bid_vol'], result['max_bid_price'] = max((abs(float(b[1])), float(b[0])) for b in bids if len(b) >= 2)
                    if asks: result['max_ask_vol'], result['max_ask_price'] = max((abs(float(a[1])), float(a[0])) for a in asks if len(a) >= 2)
        except Exception:
            pass
        try:
            url = f"https://fapi.binance.com/fapi/v1/trades?symbol={symbol}&limit=1000"
            async with session.get(url) as resp:
                if resp.status == 200:
                    trades = await resp.json()
                    cvd = 0.0
                    for t in trades:
                        qty = float(t.get('qty', 0))
                        if t.get('isBuyerMaker', True): cvd -= qty
                        else: cvd += qty
                    result['cvd'] = cvd
        except Exception:
            pass
    return result

# =========================
# DATABASE
# =========================

conn = sqlite3.connect("listings.db")
cursor = conn.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS gateio_listings (symbol TEXT PRIMARY KEY, alerted INTEGER DEFAULT 0, baseline_volume REAL DEFAULT 0, baseline_price REAL DEFAULT 0, last_alert INTEGER DEFAULT 0)")
cursor.execute("CREATE TABLE IF NOT EXISTS mexc_listings (symbol TEXT PRIMARY KEY, alerted INTEGER DEFAULT 0, baseline_volume REAL DEFAULT 0, baseline_price REAL DEFAULT 0, last_alert INTEGER DEFAULT 0)")
cursor.execute("CREATE TABLE IF NOT EXISTS cmc_listings (id INTEGER PRIMARY KEY, alerted INTEGER DEFAULT 0, baseline_volume REAL DEFAULT 0, baseline_price REAL DEFAULT 0, last_alert INTEGER DEFAULT 0)")
cursor.execute("CREATE TABLE IF NOT EXISTS gateio_futures_listings (symbol TEXT PRIMARY KEY, alerted INTEGER DEFAULT 0, baseline_volume REAL DEFAULT 0, baseline_price REAL DEFAULT 0, last_alert INTEGER DEFAULT 0)")
cursor.execute("CREATE TABLE IF NOT EXISTS mexc_futures_listings (symbol TEXT PRIMARY KEY, alerted INTEGER DEFAULT 0, baseline_volume REAL DEFAULT 0, baseline_price REAL DEFAULT 0, last_alert INTEGER DEFAULT 0)")
cursor.execute("CREATE TABLE IF NOT EXISTS binance_futures_listings (symbol TEXT PRIMARY KEY, alerted INTEGER DEFAULT 0, baseline_volume REAL DEFAULT 0, baseline_price REAL DEFAULT 0, last_alert INTEGER DEFAULT 0)")
cursor.execute("CREATE TABLE IF NOT EXISTS user_settings (chat_id TEXT PRIMARY KEY, exchanges TEXT DEFAULT 'binance,gateio,mexc,cmc')")
cursor.execute("CREATE TABLE IF NOT EXISTS user_watchlists (chat_id TEXT, symbol TEXT, exchange TEXT, added_at INTEGER, PRIMARY KEY (chat_id, symbol))")

conn.commit()

# =========================
# GLOBAL WEB SOCKET DATA (Binance)
# =========================

binance_ws_data = {}
binance_ws_lock = asyncio.Lock()

def calculate_footprint(bids, asks):
    bid_vol = sum(float(q) for _, q in bids)
    ask_vol = sum(float(q) for _, q in asks)
    total = bid_vol + ask_vol
    if total == 0: return 0
    return (bid_vol - ask_vol) / total

def calculate_cvd(trades):
    cvd = 0.0
    for trade in trades:
        qty = float(trade.get('q', 0))
        if not trade.get('m', False): cvd += qty
        else: cvd -= qty
    return cvd

async def binance_ws_manager():
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as resp:
                if resp.status != 200: return
                data = await resp.json()
                symbols = [s['symbol'] for s in data['symbols'] if s['symbol'].endswith('USDT') and s['contractType'] == 'PERPETUAL']
        except Exception:
            return

    streams = []
    for sym in symbols:
        streams.append(f"{sym.lower()}@depth20@100ms")
        streams.append(f"{sym.lower()}@trade")
    batch_size = 150
    for i in range(0, len(streams), batch_size):
        batch = streams[i:i+batch_size]
        url = BINANCE_WS_BASE + "/".join(batch)
        asyncio.create_task(handle_binance_ws_connection(url))

async def handle_binance_ws_connection(url):
    while True:
        try:
            async with websockets.connect(url) as ws:
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    stream = data.get('stream', '')
                    result = data.get('data', {})
                    if not stream or not result: continue
                    symbol = stream.split('@')[0].upper()
                    async with binance_ws_lock:
                        if symbol not in binance_ws_data:
                            binance_ws_data[symbol] = {'footprint': 0, 'cvd': 0, 'trades': []}
                    if '@depth20' in stream:
                        bids = result.get('b', [])
                        asks = result.get('a', [])
                        fp = calculate_footprint(bids, asks)
                        async with binance_ws_lock:
                            binance_ws_data[symbol]['footprint'] = fp
                    elif '@trade' in stream:
                        qty = float(result.get('q', 0))
                        if not result.get('m', False): delta = qty
                        else: delta = -qty
                        async with binance_ws_lock:
                            trades_list = binance_ws_data[symbol].get('trades', [])
                            trades_list.append({'q': qty, 'm': result.get('m', False)})
                            if len(trades_list) > 500: trades_list.pop(0)
                            binance_ws_data[symbol]['trades'] = trades_list
                            cvd = calculate_cvd(trades_list)
                            binance_ws_data[symbol]['cvd'] = cvd
        except Exception:
            await asyncio.sleep(5)

# =========================
# TELEGRAM
# =========================

async def send_telegram(message, exchange=None, symbol=None, chart_url=None, force_send=False):
    try:
        async with aiohttp.ClientSession() as session:
            for chat_id in TELEGRAM_CHAT_IDS:
                if exchange and not force_send:
                    cursor.execute("SELECT exchanges FROM user_settings WHERE chat_id=?", (str(chat_id),))
                    row = cursor.fetchone()
                    if row:
                        enabled = row[0].split(',')
                    else:
                        enabled = ['binance', 'gateio', 'mexc', 'cmc']
                    # Safety: if user accidentally toggled everything off, allow all
                    if not enabled:
                        enabled = ['binance', 'gateio', 'mexc', 'cmc']
                    if exchange not in enabled: 
                        print(f"[FILTER] Skipping alert for {exchange} ({symbol}) - chat {chat_id} has it disabled")
                        continue
                
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                payload = {"chat_id": chat_id, "text": message}
                if chart_url:
                    kb = [[{"text": "📊 Chart", "url": chart_url}]]
                    if symbol:
                        kb[0].append({"text": "➕ Watchlist", "callback_data": f"wl:{exchange}:{symbol}"})
                    payload["reply_markup"] = {"inline_keyboard": kb}
                async with session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        print(f"[OK] Alert sent to {chat_id} for {symbol}")
                    else:
                        err_body = await resp.text()
                        print(f"[TG ERROR] chat {chat_id} -> status {resp.status}: {err_body[:200]}")
    except Exception as e:
        print(f"[EXCEPTION] send_telegram failed: {e}")
        traceback.print_exc()

def timeframe_keyboard():
    return {"inline_keyboard": [[{"text": "5m", "callback_data": "tf:5m"}, {"text": "15m", "callback_data": "tf:15m"}, {"text": "30m", "callback_data": "tf:30m"}], [{"text": "1hr", "callback_data": "tf:1hr"}, {"text": "4h", "callback_data": "tf:4h"}, {"text": "1d", "callback_data": "tf:1d"}], [{"text": "1w", "callback_data": "tf:1w"}, {"text": "1M", "callback_data": "tf:1M"}]]}

def exchange_keyboard(chat_id):
    cursor.execute("SELECT exchanges FROM user_settings WHERE chat_id=?", (str(chat_id),))
    row = cursor.fetchone()
    enabled = row[0].split(',') if row else ['binance', 'gateio', 'mexc', 'cmc']
    def btn(name):
        mark = "✅" if name in enabled else "❌"
        return {"text": f"{mark} {name.capitalize()}", "callback_data": f"ex:{name}"}
    return {"inline_keyboard": [[btn('binance'), btn('gateio'), btn('mexc')], [btn('cmc')]]}

async def send_timeframe_picker(session, chat_id, text=None):
    if text is None: text = f"⏱ Timeframe Settings\n\nCurrent: {CURRENT_TIMEFRAME}\n\nTap a button to change."
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    async with session.post(url, json={"chat_id": chat_id, "text": text, "reply_markup": timeframe_keyboard()}) as resp:
        if resp.status != 200: print(f"Telegram picker error for {chat_id}:", resp.status)

async def send_exchange_picker(session, chat_id, text=None):
    if text is None: text = "⚙️ Exchange Settings\n\nSelect which exchanges you want alerts from:"
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    async with session.post(url, json={"chat_id": chat_id, "text": text, "reply_markup": exchange_keyboard(chat_id)}) as resp:
        if resp.status != 200: print(f"Telegram picker error for {chat_id}:", resp.status)

async def answer_callback(session, callback_id, text=""):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    await session.post(url, json={"callback_query_id": callback_id, "text": text})

async def edit_message(session, chat_id, message_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text}
    if reply_markup: payload["reply_markup"] = reply_markup
    await session.post(url, json=payload)

async def handle_message(session, message):
    chat_id = message.get("chat", {}).get("id")
    if str(chat_id) not in TELEGRAM_CHAT_IDS: return
    text = (message.get("text") or "").strip()
    
    if text.startswith("/timeframe") or text.startswith("/start"):
        await send_timeframe_picker(session, chat_id)
    elif text.startswith("/test"):
        # Manual test - bypass everything
        test_msg = (
            f"🧪 MANUAL TEST ALERT\n\n"
            f"Bot: Owfah/2\n"
            f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"TEST_MODE: {TEST_MODE}\n"
            f"Timeframe: {CURRENT_TIMEFRAME}\n\n"
            f"If you see this, Telegram delivery works."
        )
        await send_telegram(test_msg, force_send=True)
    elif text.startswith("/reset_settings"):
        cursor.execute("DELETE FROM user_settings WHERE chat_id=?", (str(chat_id),))
        conn.commit()
        await session.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json={"chat_id": chat_id, "text": "✅ Settings reset. All exchanges re-enabled."})
    elif text.startswith("/settings") or text.startswith("/exchanges"):
        await send_exchange_picker(session, chat_id)
    elif text.startswith("/watchlist"):
        try:
            cursor.execute("SELECT symbol, exchange FROM user_watchlists WHERE chat_id=? ORDER BY added_at DESC", (str(chat_id),))
            wl_list = cursor.fetchall()
            
            if not wl_list:
                await session.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json={"chat_id": chat_id, "text": "👀 <b>Your Watchlist</b>\n\nYour watchlist is empty.", "parse_mode": "HTML"})
                return
            
            resp = await session.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json={"chat_id": chat_id, "text": "⏳ Fetching watchlist details..."})
            load_msg = await resp.json()
            load_msg_id = load_msg["result"]["message_id"]
                
            msg = "👀 <b>Your Watchlist</b>\n\n"
            kb = []
            
            async with aiohttp.ClientSession() as s:
                for sym, ex in wl_list:
                    try:
                        current_price, price_change, volume, vwap, svp, of_data, klines = 0.0, 0.0, 0.0, None, None, None, None
                        if ex == "binance":
                            async with s.get(f"https://fapi.binance.com/fapi/v1/ticker/24hr?symbol={sym}") as r:
                                if r.status == 200:
                                    d = await r.json()
                                    current_price = float(d.get("lastPrice", 0))
                                    price_change = float(d.get("priceChangePercent", 0))
                                    volume = float(d.get("quoteVolume", 0))
                            klines = await fetch_binance_klines(sym, interval=get_tf_binance(), limit=200)
                            of_data = await fetch_binance_fut_orderflow(sym)
                        elif ex == "gateio":
                            g_sym = sym if "_" in sym else f"{sym[:-4]}_USDT"
                            async with s.get(f"https://api.gateio.ws/api/v4/futures/usdt/tickers?contract={g_sym}") as r:
                                if r.status == 200:
                                    d = await r.json()
                                    if d:
                                        current_price = float(d[0].get("last", 0))
                                        price_change = float(d[0].get("change_percentage", 0))
                                        volume = float(d[0].get("volume_24h_quote", 0))
                            klines = await fetch_gateio_klines(g_sym, interval=get_tf_gateio(), limit=200)
                            of_data = await fetch_gateio_fut_orderflow(g_sym)
                        elif ex == "mexc":
                            m_sym = sym if "_" in sym else f"{sym[:-4]}_USDT"
                            async with s.get(f"https://contract.mexc.com/api/v1/contract/ticker?symbol={m_sym}") as r:
                                if r.status == 200:
                                    d = await r.json()
                                    if d.get("success"):
                                        current_price = float(d["data"].get("lastPrice", 0))
                                        price_change = float(d["data"].get("riseFallRate", 0)) * 100
                                        volume = float(d["data"].get("amount24", 0))
                            klines = await fetch_mexc_klines(m_sym, interval=get_tf_mexc(), limit=200)
                            of_data = await fetch_mexc_fut_orderflow(m_sym)
                        elif ex == "cmc":
                            async with s.get(f"https://api.binance.com/api/v3/ticker/24hr?symbol={sym}USDT") as r:
                                if r.status == 200:
                                    d = await r.json()
                                    current_price = float(d.get("lastPrice", 0))
                                    price_change = float(d.get("priceChangePercent", 0))
                                    volume = float(d.get("quoteVolume", 0))
                            of_data = await fetch_binance_fut_orderflow(f"{sym}USDT")
                        
                        if klines:
                            atr = calculate_atr(klines["highs"], klines["lows"], klines["closes"], ATR_PERIOD)
                            vwap, _, _, _ = calculate_vwap_bands(klines["highs"], klines["lows"], klines["closes"], klines["volumes"])
                            svp, _, _ = calculate_poc_va(klines["highs"], klines["lows"], klines["closes"], klines["volumes"])
                        
                        msg += f"<b>{sym}</b> ({ex})\n"
                        msg += f"Price: ${current_price:.4f} ({price_change:+.2f}%)\n"
                        msg += f"Volume: ${volume:,.0f}\n"
                        if vwap: msg += f"VWAP: {vwap:.4f}\n"
                        if svp: msg += f"POC: {svp:.4f}\n"
                        if of_data:
                            msg += f"{format_of_line(of_data, 0, 'N/A')}\n"
                            if klines:
                                owfah_data = calculate_owfah(klines["deltas"], klines["volumes"], of_data.get('footprint', 0.0), klines, current_price)
                                msg += f"MaxD: {format_cvd(owfah_data['max_delta'])} │ MinD: {format_cvd(owfah_data['min_delta'])}\n"
                            msg += "\n"
                        kb.append([{"text": f"❌ Remove {sym} ({ex})", "callback_data": f"rwl:{sym}:{ex}"}])
                    except Exception as e:
                        msg += f"<b>{sym}</b> ({ex})\n⚠️ Error fetching data\n\n"
                        kb.append([{"text": f"❌ Remove {sym} ({ex})", "callback_data": f"rwl:{sym}:{ex}"}])
            
            payload = {"chat_id": chat_id, "message_id": load_msg_id, "text": msg, "parse_mode": "HTML"}
            if kb: payload["reply_markup"] = {"inline_keyboard": kb}
            await session.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText", json=payload)
        except Exception as e:
            print(f"Watchlist error: {e}")

async def handle_callback(session, callback):
    global CURRENT_TIMEFRAME
    data = callback.get("data", "")
    callback_id = callback.get("id")
    message = callback.get("message", {})
    chat_id = message.get("chat", {}).get("id")
    message_id = message.get("message_id")
    if str(chat_id) not in TELEGRAM_CHAT_IDS:
        await answer_callback(session, callback_id, "Not authorized.")
        return

    if data.startswith("ex:"):
        ex_name = data[3:]
        cursor.execute("SELECT exchanges FROM user_settings WHERE chat_id=?", (str(chat_id),))
        row = cursor.fetchone()
        enabled = row[0].split(',') if row else ['binance', 'gateio', 'mexc', 'cmc']
        if not row:
            cursor.execute("INSERT INTO user_settings (chat_id, exchanges) VALUES (?, ?)", (str(chat_id), ",".join(enabled)))
            conn.commit()
            
        if ex_name in enabled: enabled.remove(ex_name)
        else: enabled.append(ex_name)
            
        cursor.execute("UPDATE user_settings SET exchanges=? WHERE chat_id=?", (",".join(enabled), str(chat_id)))
        conn.commit()
        await answer_callback(session, callback_id, f"Toggled {ex_name}")
        await edit_message(session, chat_id, message_id, "⚙️ Exchange Settings\n\nSelect which exchanges you want alerts from:", exchange_keyboard(chat_id))
        return

    if data.startswith("wl:"):
        parts = data.split(":")
        if len(parts) >= 3:
            now = int(time.time())
            cursor.execute("INSERT OR REPLACE INTO user_watchlists (chat_id, symbol, exchange, added_at) VALUES (?, ?, ?, ?)", (str(chat_id), parts[2], parts[1], now))
            conn.commit()
            await answer_callback(session, callback_id, f"Added {parts[2]} to watchlist")
        return
        
    if data.startswith("rwl:"):
        parts = data.split(":")
        if len(parts) >= 3:
            cursor.execute("DELETE FROM user_watchlists WHERE chat_id=? AND symbol=?", (str(chat_id), parts[1]))
            conn.commit()
            await answer_callback(session, callback_id, f"Removed {parts[1]} from watchlist")
            
            cursor.execute("SELECT symbol, exchange FROM user_watchlists WHERE chat_id=? ORDER BY added_at DESC", (str(chat_id),))
            wl_list = cursor.fetchall()
            new_msg = "👀 <b>Your Watchlist</b>\n\n"
            kb = []
            if wl_list:
                for s, e in wl_list:
                    new_msg += f"• {s} ({e})\n"
                    kb.append([{"text": f"❌ Remove {s} ({e})", "callback_data": f"rwl:{s}:{e}"}])
            else:
                new_msg += "Your watchlist is empty."
                
            payload = {"chat_id": chat_id, "message_id": message_id, "text": new_msg, "parse_mode": "HTML"}
            if kb: payload["reply_markup"] = {"inline_keyboard": kb}
            await session.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText", json=payload)
        return

    if data.startswith("tf:"):
        new_tf = data[3:]
        if new_tf in ALLOWED_TIMEFRAMES:
            old_tf = CURRENT_TIMEFRAME
            CURRENT_TIMEFRAME = new_tf
            await answer_callback(session, callback_id, f"Set to {new_tf}")
            await edit_message(session, chat_id, message_id, f"⏱ Timeframe Settings\n\nCurrent: {CURRENT_TIMEFRAME}  (was {old_tf})\n\nTap a button to change.", timeframe_keyboard())
        return

async def poll_telegram():
    offset = None
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                params = {"timeout": 30, "allowed_updates": json.dumps(["message", "callback_query"])}
                if offset is not None: params["offset"] = offset
                async with session.get(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates", params=params) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(3)
                        continue
                    data = await resp.json()
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    if "message" in update: await handle_message(session, update["message"])
                    elif "callback_query" in update: await handle_callback(session, update["callback_query"])
            except Exception as e:
                print("Telegram poll error:", e)
                await asyncio.sleep(5)

# =========================
# KLINE FETCHERS
# =========================

async def fetch_gateio_klines(symbol, interval=None, limit=KLINE_LIMIT):
    if interval is None: interval = get_tf_gateio()
    url = f"https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={symbol}&interval={interval}&limit={limit}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                if resp.status != 200: return None
                data = await resp.json()
                data.sort(key=lambda x: x[0])
                return {
                    "closes": [float(c[2]) for c in data],
                    "opens": [float(c[1]) for c in data],
                    "highs": [float(c[3]) for c in data],
                    "lows": [float(c[4]) for c in data],
                    "volumes": [float(c[6]) for c in data],
                    "deltas": [float(c[6]) if float(c[2]) > float(c[1]) else -float(c[6]) for c in data]
                }
        except Exception:
            return None

async def fetch_mexc_klines(symbol, interval=None, limit=KLINE_LIMIT):
    if interval is None: interval = get_tf_mexc()
    url = f"https://futures.mexc.com/api/v1/contract/kline/{symbol}?interval={interval}&limit={limit}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                if resp.status != 200: return None
                data = await resp.json()
                if not data.get("success") or "data" not in data: return None
                klines = data["data"]
                times = klines.get("time", [])
                if not times: return None
                combined = list(zip(times, klines["open"], klines["close"], klines["high"], klines["low"], klines["vol"]))
                combined.sort(key=lambda x: x[0])
                opens = [float(o) for _, o, _, _, _, _ in combined]
                closes = [float(c) for _, _, c, _, _, _ in combined]
                highs = [float(h) for _, _, _, h, _, _ in combined]
                lows = [float(l) for _, _, _, _, l, _ in combined]
                volumes = [float(v) for _, _, _, _, _, v in combined]
                deltas = [v if c > o else -v for o, c, v in zip(opens, closes, volumes)]
                return {"closes": closes, "opens": opens, "highs": highs, "lows": lows, "volumes": volumes, "deltas": deltas}
        except Exception:
            return None

async def fetch_binance_klines(symbol, interval=None, limit=BINANCE_KLINE_LIMIT):
    if interval is None: interval = get_tf_binance()
    url = f"{BINANCE_FUTURES_KLINES}?symbol={symbol}&interval={interval}&limit={limit}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                if resp.status != 200: return None
                data = await resp.json()
                opens = [float(c[1]) for c in data]
                highs = [float(c[2]) for c in data]
                lows = [float(c[3]) for c in data]
                closes = [float(c[4]) for c in data]
                volumes = [float(c[5]) for c in data]
                deltas = [(2 * float(c[9])) - float(c[5]) for c in data]
                return {"closes": closes, "opens": opens, "highs": highs, "lows": lows, "volumes": volumes, "deltas": deltas}
        except Exception:
            return None

def stub_klines(price):
    """Returns dummy klines so TEST_MODE can still fire even if real data fails."""
    return {
        "closes": [price] * 20,
        "opens": [price] * 20,
        "highs": [price * 1.001] * 20,
        "lows": [price * 0.999] * 20,
        "volumes": [100.0] * 20,
        "deltas": [0.0] * 20
    }

# =========================
# ADVANCED OWFAH / CONFLUENCE ENGINE
# =========================

def calculate_owfah(deltas, volumes, footprint, klines, current_price):
    if len(deltas) < 20 or len(volumes) < 20:
        return {'score': 0, 'details': 'Insufficient Data', 'atr': 0, 'max_delta': 0, 'min_delta': 0, 'delta_vol_pct': 0, 'vwap_val': 0, 'poc_val': 0, 'delta_footprint_long': False, 'delta_footprint_short': False, 'vwap_long': False, 'vwap_short': False, 'poc_long': False, 'poc_short': False, 'cvd_div': False}
    
    max_delta = max(deltas[-20:])
    min_delta = min(deltas[-20:])
    curr_delta = deltas[-1]
    prev_delta = deltas[-2]
    prev_vol = volumes[-2]
    avg_vol = sum(volumes[-20:]) / 20 if len(volumes) >= 20 else 1.0
    
    delta_vol_pct = (abs(prev_delta) / prev_vol * 100) if prev_vol > 0 else 0.0
    d_vol_threshold = calculate_dynamic_d_vol(deltas, volumes, len(deltas)-1)
    d_vol_passed = delta_vol_pct >= d_vol_threshold
    
    long_delta = (curr_delta >= max_delta * 0.9) and (min_delta >= -0.10 * avg_vol)
    short_delta = (curr_delta <= min_delta * 0.9) and (max_delta <= 0.10 * avg_vol)
    
    delta_footprint_long = d_vol_passed and long_delta and footprint > 0.1
    delta_footprint_short = d_vol_passed and short_delta and footprint < -0.1
    
    atr = calculate_atr(klines["highs"], klines["lows"], klines["closes"], ATR_PERIOD)
    
    vwap, std_dev, slope, typical_prices = calculate_vwap_bands(klines["highs"], klines["lows"], klines["closes"], klines["volumes"])
    vwap_long = False
    vwap_short = False
    if vwap and std_dev > 0:
        if slope > 0 and (vwap - std_dev <= current_price <= vwap * 1.01):
            vwap_long = True
        if slope < 0 and (vwap * 0.99 <= current_price <= vwap + std_dev):
            vwap_short = True
            
    poc, vah, val = calculate_poc_va(klines["highs"], klines["lows"], klines["closes"], klines["volumes"])
    poc_long = False
    poc_short = False
    if poc:
        poc_dist = 0.25 * atr
        if typical_prices[-1] > typical_prices[-5] and any(abs(current_price - x) <= poc_dist for x in (poc, vah, val)):
            poc_long = True
        if typical_prices[-1] < typical_prices[-5] and any(abs(current_price - x) <= poc_dist for x in (poc, vah, val)):
            poc_short = True
            
    cvd_div = calculate_cvd_divergence(klines["closes"], deltas)
    
    return {
        'delta_footprint_long': delta_footprint_long,
        'delta_footprint_short': delta_footprint_short,
        'vwap_long': vwap_long,
        'vwap_short': vwap_short,
        'poc_long': poc_long,
        'poc_short': poc_short,
        'cvd_div': cvd_div,
        'max_delta': max_delta,
        'min_delta': min_delta,
        'delta_vol_pct': delta_vol_pct,
        'atr': atr,
        'poc_val': poc,
        'vwap_val': vwap
    }

# =========================
# SCANNERS
# =========================

async def scan_gateio_futures(first_run=False):
    print("[SCAN] Gate.io Futures...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(GATEIO_FUTURES_TICKERS) as resp:
                if resp.status != 200: 
                    print(f"[SCAN] Gate.io tickers failed: {resp.status}")
                    return
                ticker_data = await resp.json()
        print(f"[SCAN] Gate.io returned {len(ticker_data)} tickers")
        
        for item in ticker_data:
            symbol = item["contract"]
            if not symbol.endswith("_USDT"): continue
            volume = float(item.get("volume_24h_quote", 0))
            current_price = float(item.get("last", 0))
            price_change = float(item.get("change_percentage", 0))
            
            trigger = volume >= FUTURES_MIN_VOLUME and abs(price_change) >= FUTURES_PRICE_CHANGE_THRESHOLD
            if not TEST_MODE and not trigger: continue
            
            try:
                klines = await fetch_gateio_klines(symbol, interval=get_tf_gateio(), limit=200)
                if not klines or len(klines["closes"]) < 20:
                    if TEST_MODE:
                        klines = stub_klines(current_price)
                    else:
                        continue
                    
                of = await fetch_gateio_fut_orderflow(symbol)
                footprint = of.get('footprint', 0.0)
                
                res = calculate_owfah(klines["deltas"], klines["volumes"], footprint, klines, current_price)
                poc = res['poc_val']
                
                score = 0
                details = []
                if res['delta_footprint_long']:
                    score += 1; details.append("Delta/FP")
                    if res['vwap_long']: score += 1; details.append("VWAP")
                    if res['poc_long']: score += 1; details.append("POC")
                    if res['cvd_div']: score += 1; details.append("CVD")
                elif res['delta_footprint_short']:
                    score += 1; details.append("Delta/FP")
                    if res['vwap_short']: score += 1; details.append("VWAP")
                    if res['poc_short']: score += 1; details.append("POC")
                    if res['cvd_div']: score += 1; details.append("CVD")
                
                if TEST_MODE or score >= 3:
                    signal = "LONG" if res['delta_footprint_long'] else "SHORT"
                    vwap_str = f"{res['vwap_val']:.4f}" if res['vwap_val'] else "N/A"
                    poc_str = f"{poc:.4f}" if poc else "N/A"
                    of_line = format_of_line(of, score, ", ".join(details) if details else "TEST_MODE_BYPASS")
                    
                    prefix = "🚨 [TEST MODE] " if TEST_MODE else "🚨 "
                    
                    message = (
                        f"{prefix}GATE.IO FUTURES ALERT\n\n"
                        f"Contract: {symbol}\n"
                        f"⏱ Timeframe: {CURRENT_TIMEFRAME}\n"
                        f"Price Change: {price_change:+.2f}%\n"
                        f"Volume: ${volume:,.0f}\n"
                        f"Entry Signal: Consider {signal}\n"
                        f"VWAP: {vwap_str}\n"
                        f"POC (Vol Profile): {poc_str}\n"
                        f"{of_line}\n\n"
                        f"========================"
                    )
                    await send_telegram(message, "gateio", symbol, tradingview_url("gateio_fut", symbol), force_send=TEST_MODE)
            except Exception as e:
                print(f"[GATEIO] Error for {symbol}: {e}")
    except Exception:
        print("Gate.io Futures scan error:")
        traceback.print_exc()

async def scan_mexc_futures(first_run=False):
    print("[SCAN] MEXC Futures...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(MEXC_FUTURES_TICKERS) as resp:
                if resp.status != 200: 
                    print(f"[SCAN] MEXC tickers failed: {resp.status}")
                    return
                data = await resp.json()
        tickers = data.get("data", [])
        print(f"[SCAN] MEXC returned {len(tickers)} tickers")
        
        for item in tickers:
            symbol = item.get("symbol")
            if not symbol.endswith("USDT"): continue
            volume = float(item.get("amount24", 0))
            current_price = float(item.get("lastPrice", 0))
            price_change = float(item.get("riseFallRate", 0)) * 100
            
            trigger = volume >= FUTURES_MIN_VOLUME and abs(price_change) >= FUTURES_PRICE_CHANGE_THRESHOLD
            if not TEST_MODE and not trigger: continue
            
            try:
                klines = await fetch_mexc_klines(symbol, interval=get_tf_mexc(), limit=200)
                if not klines or len(klines["closes"]) < 20:
                    if TEST_MODE:
                        klines = stub_klines(current_price)
                    else:
                        continue
                    
                of = await fetch_mexc_fut_orderflow(symbol)
                footprint = of.get('footprint', 0.0)
                
                res = calculate_owfah(klines["deltas"], klines["volumes"], footprint, klines, current_price)
                poc = res['poc_val']
                
                score = 0
                details = []
                if res['delta_footprint_long']:
                    score += 1; details.append("Delta/FP")
                    if res['vwap_long']: score += 1; details.append("VWAP")
                    if res['poc_long']: score += 1; details.append("POC")
                    if res['cvd_div']: score += 1; details.append("CVD")
                elif res['delta_footprint_short']:
                    score += 1; details.append("Delta/FP")
                    if res['vwap_short']: score += 1; details.append("VWAP")
                    if res['poc_short']: score += 1; details.append("POC")
                    if res['cvd_div']: score += 1; details.append("CVD")
                
                if TEST_MODE or score >= 3:
                    signal = "LONG" if res['delta_footprint_long'] else "SHORT"
                    vwap_str = f"{res['vwap_val']:.4f}" if res['vwap_val'] else "N/A"
                    poc_str = f"{poc:.4f}" if poc else "N/A"
                    of_line = format_of_line(of, score, ", ".join(details) if details else "TEST_MODE_BYPASS")
                    
                    prefix = "🚨 [TEST MODE] " if TEST_MODE else "🚨 "
                    
                    message = (
                        f"{prefix}MEXC FUTURES ALERT\n\n"
                        f"Contract: {symbol}\n"
                        f"⏱ Timeframe: {CURRENT_TIMEFRAME}\n"
                        f"Price Change: {price_change:+.2f}%\n"
                        f"Volume: ${volume:,.0f}\n"
                        f"Entry Signal: Consider {signal}\n"
                        f"VWAP: {vwap_str}\n"
                        f"POC (Vol Profile): {poc_str}\n"
                        f"{of_line}\n\n"
                        f"========================"
                    )
                    await send_telegram(message, "mexc", symbol, tradingview_url("mexc_fut", symbol), force_send=TEST_MODE)
            except Exception as e:
                print(f"[MEXC] Error for {symbol}: {e}")
    except Exception:
        print("MEXC Futures scan error:")
        traceback.print_exc()

async def scan_binance_futures(first_run=False):
    print("[SCAN] Binance Futures...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(BINANCE_FUTURES_TICKERS) as resp:
                if resp.status != 200:
                    print(f"[SCAN] Binance tickers failed: {resp.status}")
                    return
                tickers = await resp.json()
        print(f"[SCAN] Binance returned {len(tickers)} tickers")
        
        for item in tickers:
            symbol = item["symbol"]
            if not symbol.endswith("USDT"): continue
            volume = float(item.get("quoteVolume", 0))
            current_price = float(item.get("lastPrice", 0))
            price_change = float(item.get("priceChangePercent", 0))
            
            trigger = volume >= FUTURES_MIN_VOLUME and abs(price_change) >= BINANCE_PRICE_CHANGE_THRESHOLD
            if not TEST_MODE and not trigger: continue
            
            try:
                klines = await fetch_binance_klines(symbol, interval=get_tf_binance(), limit=200)
                if not klines or len(klines["closes"]) < 20:
                    if TEST_MODE:
                        klines = stub_klines(current_price)
                    else:
                        continue
                    
                of = await fetch_binance_fut_orderflow(symbol)
                footprint = of.get('footprint', 0.0)
                
                res = calculate_owfah(klines["deltas"], klines["volumes"], footprint, klines, current_price)
                poc = res['poc_val']
                
                score = 0
                details = []
                if res['delta_footprint_long']:
                    score += 1; details.append("Delta/FP")
                    if res['vwap_long']: score += 1; details.append("VWAP")
                    if res['poc_long']: score += 1; details.append("POC")
                    if res['cvd_div']: score += 1; details.append("CVD")
                elif res['delta_footprint_short']:
                    score += 1; details.append("Delta/FP")
                    if res['vwap_short']: score += 1; details.append("VWAP")
                    if res['poc_short']: score += 1; details.append("POC")
                    if res['cvd_div']: score += 1; details.append("CVD")
                
                if TEST_MODE or score >= 3:
                    signal = "LONG" if res['delta_footprint_long'] else "SHORT"
                    vwap_str = f"{res['vwap_val']:.4f}" if res['vwap_val'] else "N/A"
                    poc_str = f"{poc:.4f}" if poc else "N/A"
                    of_line = format_of_line(of, score, ", ".join(details) if details else "TEST_MODE_BYPASS")
                    
                    prefix = "🚨 [TEST MODE] " if TEST_MODE else "🚨 "
                    
                    message = (
                        f"{prefix}BINANCE FUTURES ALERT\n\n"
                        f"Contract: {symbol}\n"
                        f"⏱ Timeframe: {CURRENT_TIMEFRAME}\n"
                        f"Price Change: {price_change:+.2f}%\n"
                        f"Volume: ${volume:,.0f}\n"
                        f"Entry Signal: Consider {signal}\n"
                        f"VWAP: {vwap_str}\n"
                        f"POC (Vol Profile): {poc_str}\n"
                        f"{of_line}\n\n"
                        f"========================"
                    )
                    await send_telegram(message, "binance", symbol, tradingview_url("binance_fut", symbol), force_send=TEST_MODE)
            except Exception as e:
                print(f"[BINANCE] Error for {symbol}: {e}")
    except Exception:
        print("Binance Futures scan error:")
        traceback.print_exc()

# =========================
# MAIN LOOP
# =========================

async def main():
    print(f"Starting Owfah/2... (TEST MODE: {TEST_MODE})")
    
    # HARD STARTUP TEST - bypasses ALL filters to verify Telegram works
    startup_msg = (
        f"🚀 BOT STARTUP TEST\n\n"
        f"Bot: Owfah/2\n"
        f"Mode: {'TEST (FLOODING)' if TEST_MODE else 'NORMAL'}\n"
        f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Timeframe: {CURRENT_TIMEFRAME}\n\n"
        f"If you see this message, Telegram delivery works.\n"
        f"If alerts follow, the pipeline works.\n"
        f"If NO alerts follow but you got this, the scanner is the issue."
    )
    print("[STARTUP] Sending hardcoded test message to all chat IDs...")
    await send_telegram(startup_msg, force_send=True)
    print("[STARTUP] Test message sent (check Telegram).")
    
    asyncio.create_task(binance_ws_manager())
    asyncio.create_task(poll_telegram())
    
    print("Initializing database silently...")
    await scan_gateio_futures(first_run=True)
    await scan_mexc_futures(first_run=True)
    await scan_binance_futures(first_run=True)

    print("Bot running...")
    while True:
        print(f"\n===== CYCLE START ({time.strftime('%H:%M:%S')}) =====")
        await scan_gateio_futures()
        await scan_mexc_futures()
        await scan_binance_futures()
        print(f"===== CYCLE END - Sleeping {CHECK_INTERVAL}s =====\n")
        await asyncio.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
