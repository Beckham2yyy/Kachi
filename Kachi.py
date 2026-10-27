import asyncio
import aiohttp
import sqlite3
import time
import traceback
import math
import json
import websockets

# =========================
# CONFIG
# =========================

CMC_API_KEY = "8a6ea4b9c73c45e8adac9e14f214f087"
TELEGRAM_BOT_TOKEN = "8703361353:AAFZoJu8UWVLxsuSwSzzeQ-9cPHnUIH6QSQ"
TELEGRAM_CHAT_IDS = ["7198809557", "6065933220"]

GATEIO_TICKERS = "https://api.gateio.ws/api/v4/spot/tickers"
MEXC_TICKERS = "https://api.mexc.com/api/v3/ticker/24hr"
CMC_LISTINGS = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

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
KLINE_LIMIT = 20

# Binance Futures config
BINANCE_FUTURES_TICKERS = "https://fapi.binance.com/fapi/v1/ticker/24hr"
BINANCE_FUTURES_KLINES = "https://fapi.binance.com/fapi/v1/klines"
BINANCE_KLINE_INTERVAL = "5m"
BINANCE_KLINE_LIMIT = 20
BINANCE_WS_BASE = "wss://fstream.binance.com/stream?streams="
BINANCE_FOOTPRINT_THRESHOLD = 0.2
BINANCE_CVD_THRESHOLD = 20000
BINANCE_PRICE_CHANGE_THRESHOLD = 1.5
BINANCE_VOLUME_SPIKE_PERCENT = 12

RSI_POINTS_THRESHOLD = 4

BIG_COINS = ["BTC", "ETH", "SOL"]
BIG_COIN_PRICE_THRESHOLD = 2

# =========================
# TIMEFRAME (EDITABLE VIA TELEGRAM)
# =========================

CURRENT_TIMEFRAME = "1hr"
ALLOWED_TIMEFRAMES = ("1d", "30m", "1hr")

TIMEFRAME_MAP = {
    "1d":  {"gateio": "1d",  "mexc": "Day1",  "binance": "1d"},
    "30m": {"gateio": "30m", "mexc": "Min30", "binance": "30m"},
    "1hr": {"gateio": "1h",  "mexc": "Min60", "binance": "1h"},
}

def get_tf_gateio():
    return TIMEFRAME_MAP[CURRENT_TIMEFRAME]["gateio"]

def get_tf_mexc():
    return TIMEFRAME_MAP[CURRENT_TIMEFRAME]["mexc"]

def get_tf_binance():
    return TIMEFRAME_MAP[CURRENT_TIMEFRAME]["binance"]

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
# ORDER FLOW HELPERS
# =========================

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

def format_of_line(of):
    return f"📊 OF: FP {format_footprint(of.get('footprint', 0.0))} │ CVD {format_cvd(of.get('cvd', 0.0))}"

async def fetch_gateio_spot_orderflow(symbol):
    result = {'footprint': 0.0, 'cvd': 0.0}
    async with aiohttp.ClientSession() as session:
        try:
            url = f"https://api.gateio.ws/api/v4/spot/order_book?currency_pair={symbol}&limit=20"
            async with session.get(url) as resp:
                if resp.status == 200:
                    book = await resp.json()
                    bids = book.get('bids', [])
                    asks = book.get('asks', [])
                    bid_vol = sum(float(b[1]) for b in bids if len(b) >= 2)
                    ask_vol = sum(float(a[1]) for a in asks if len(a) >= 2)
                    total = bid_vol + ask_vol
                    if total > 0:
                        result['footprint'] = (bid_vol - ask_vol) / total
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
                        if side == 'buy':
                            cvd += amt
                        elif side == 'sell':
                            cvd -= amt
                    result['cvd'] = cvd
        except Exception:
            pass
    return result

async def fetch_gateio_fut_orderflow(symbol):
    result = {'footprint': 0.0, 'cvd': 0.0}
    async with aiohttp.ClientSession() as session:
        try:
            url = f"https://api.gateio.ws/api/v4/futures/usdt/order_book?contract={symbol}&limit=20"
            async with session.get(url) as resp:
                if resp.status == 200:
                    book = await resp.json()
                    bids = book.get('bids', [])
                    asks = book.get('asks', [])
                    bid_vol = sum(abs(float(b[1])) for b in bids if len(b) >= 2)
                    ask_vol = sum(abs(float(a[1])) for a in asks if len(a) >= 2)
                    total = bid_vol + ask_vol
                    if total > 0:
                        result['footprint'] = (bid_vol - ask_vol) / total
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
    result = {'footprint': 0.0, 'cvd': 0.0}
    async with aiohttp.ClientSession() as session:
        try:
            url = f"https://api.mexc.com/api/v3/depth?symbol={symbol}&limit=20"
            async with session.get(url) as resp:
                if resp.status == 200:
                    book = await resp.json()
                    bids = book.get('bids', [])
                    asks = book.get('asks', [])
                    bid_vol = sum(float(b[1]) for b in bids if len(b) >= 2)
                    ask_vol = sum(float(a[1]) for a in asks if len(a) >= 2)
                    total = bid_vol + ask_vol
                    if total > 0:
                        result['footprint'] = (bid_vol - ask_vol) / total
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
                        if t.get('isBuyerMaker', True):
                            cvd -= qty
                        else:
                            cvd += qty
                    result['cvd'] = cvd
        except Exception:
            pass
    return result

async def fetch_mexc_fut_orderflow(symbol):
    result = {'footprint': 0.0, 'cvd': 0.0}
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
                        bid_vol = sum(abs(float(b[1])) for b in bids if len(b) >= 2)
                        ask_vol = sum(abs(float(a[1])) for a in asks if len(a) >= 2)
                        total = bid_vol + ask_vol
                        if total > 0:
                            result['footprint'] = (bid_vol - ask_vol) / total
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
                            if side == 1:
                                cvd -= vol
                            elif side == 2:
                                cvd += vol
                        result['cvd'] = cvd
        except Exception:
            pass
    return result

async def fetch_binance_fut_orderflow(symbol):
    result = {'footprint': 0.0, 'cvd': 0.0}
    async with aiohttp.ClientSession() as session:
        try:
            url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=20"
            async with session.get(url) as resp:
                if resp.status == 200:
                    book = await resp.json()
                    bids = book.get('bids', [])
                    asks = book.get('asks', [])
                    bid_vol = sum(float(b[1]) for b in bids if len(b) >= 2)
                    ask_vol = sum(float(a[1]) for a in asks if len(a) >= 2)
                    total = bid_vol + ask_vol
                    if total > 0:
                        result['footprint'] = (bid_vol - ask_vol) / total
        except Exception:
            pass
        try:
            url = f"https://fapi.binance.com/fapi/v1/trades?symbol={symbol}&limit=500"
            async with session.get(url) as resp:
                if resp.status == 200:
                    trades = await resp.json()
                    cvd = 0.0
                    for t in trades:
                        qty = float(t.get('qty', 0))
                        if t.get('isBuyerMaker', True):
                            cvd -= qty
                        else:
                            cvd += qty
                    result['cvd'] = cvd
        except Exception:
            pass
    return result

# =========================
# DATABASE
# =========================

conn = sqlite3.connect("listings.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS gateio_listings (
    symbol TEXT PRIMARY KEY,
    alerted INTEGER DEFAULT 0,
    baseline_volume REAL DEFAULT 0,
    baseline_price REAL DEFAULT 0,
    last_alert INTEGER DEFAULT 0,
    prev_rsi REAL DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS mexc_listings (
    symbol TEXT PRIMARY KEY,
    alerted INTEGER DEFAULT 0,
    baseline_volume REAL DEFAULT 0,
    baseline_price REAL DEFAULT 0,
    last_alert INTEGER DEFAULT 0,
    prev_rsi REAL DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS cmc_listings (
    id INTEGER PRIMARY KEY,
    alerted INTEGER DEFAULT 0,
    baseline_volume REAL DEFAULT 0,
    baseline_price REAL DEFAULT 0,
    last_alert INTEGER DEFAULT 0,
    prev_rsi REAL DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS gateio_futures_listings (
    symbol TEXT PRIMARY KEY,
    alerted INTEGER DEFAULT 0,
    baseline_volume REAL DEFAULT 0,
    baseline_price REAL DEFAULT 0,
    last_alert INTEGER DEFAULT 0,
    prev_rsi REAL DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS mexc_futures_listings (
    symbol TEXT PRIMARY KEY,
    alerted INTEGER DEFAULT 0,
    baseline_volume REAL DEFAULT 0,
    baseline_price REAL DEFAULT 0,
    last_alert INTEGER DEFAULT 0,
    prev_rsi REAL DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS binance_futures_listings (
    symbol TEXT PRIMARY KEY,
    alerted INTEGER DEFAULT 0,
    baseline_volume REAL DEFAULT 0,
    baseline_price REAL DEFAULT 0,
    last_alert INTEGER DEFAULT 0,
    prev_rsi REAL DEFAULT 0
)
""")

conn.commit()

# =========================
# GLOBAL WEB SOCKET DATA (Binance)
# =========================

binance_ws_data = {}
binance_ws_lock = asyncio.Lock()

# =========================
# HELPER FUNCTIONS (Indicators)
# =========================

def calculate_ema(closes, period=20):
    if len(closes) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = closes[0]
    for price in closes[1:]:
        ema = (price - ema) * multiplier + ema
    return ema

def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains = []
    losses = []
    for i in range(1, period + 1):
        change = closes[i] - closes[i-1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def calculate_candle_strength(open_price, close, high, low):
    body = abs(close - open_price)
    range_ = high - low
    if range_ == 0:
        return 0
    return body / range_

def calculate_footprint(bids, asks):
    bid_vol = sum(float(q) for _, q in bids)
    ask_vol = sum(float(q) for _, q in asks)
    total = bid_vol + ask_vol
    if total == 0:
        return 0
    return (bid_vol - ask_vol) / total

def calculate_cvd(trades):
    cvd = 0.0
    for trade in trades:
        qty = float(trade.get('q', 0))
        if not trade.get('m', False):
            cvd += qty
        else:
            cvd -= qty
    return cvd

async def fetch_gateio_klines(symbol, interval=None, limit=KLINE_LIMIT):
    if interval is None:
        interval = get_tf_gateio()
    url = f"https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={symbol}&interval={interval}&limit={limit}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                data.sort(key=lambda x: x[0])
                closes = [float(c[2]) for c in data]
                opens = [float(c[1]) for c in data]
                highs = [float(c[3]) for c in data]
                lows = [float(c[4]) for c in data]
                return {"closes": closes, "opens": opens, "highs": highs, "lows": lows}
        except Exception:
            return None

async def fetch_mexc_klines(symbol, interval=None, limit=KLINE_LIMIT):
    if interval is None:
        interval = get_tf_mexc()
    url = f"https://futures.mexc.com/api/v1/contract/kline/{symbol}?interval={interval}&limit={limit}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if not data.get("success") or "data" not in data:
                    return None
                klines = data["data"]
                times = klines.get("time", [])
                if not times:
                    return None
                combined = list(zip(times, klines["open"], klines["close"], klines["high"], klines["low"]))
                combined.sort(key=lambda x: x[0])
                opens = [float(o) for _, o, _, _, _ in combined]
                closes = [float(c) for _, _, c, _, _ in combined]
                highs = [float(h) for _, _, _, h, _ in combined]
                lows = [float(l) for _, _, _, _, l in combined]
                return {"closes": closes, "opens": opens, "highs": highs, "lows": lows}
        except Exception:
            return None

async def fetch_binance_klines(symbol, interval=None, limit=BINANCE_KLINE_LIMIT):
    if interval is None:
        interval = get_tf_binance()
    url = f"{BINANCE_FUTURES_KLINES}?symbol={symbol}&interval={interval}&limit={limit}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                opens = [float(c[1]) for c in data]
                highs = [float(c[2]) for c in data]
                lows = [float(c[3]) for c in data]
                closes = [float(c[4]) for c in data]
                return {"closes": closes, "opens": opens, "highs": highs, "lows": lows}
        except Exception:
            return None

# =========================
# WEB SOCKET MANAGER (Binance)
# =========================

async def binance_ws_manager():
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as resp:
                if resp.status != 200:
                    print("Failed to fetch Binance exchange info")
                    return
                data = await resp.json()
                symbols = [s['symbol'] for s in data['symbols'] if s['symbol'].endswith('USDT') and s['contractType'] == 'PERPETUAL']
        except Exception as e:
            print(f"Error fetching Binance symbols: {e}")
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
                    if not stream or not result:
                        continue
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
                        if not result.get('m', False):
                            delta = qty
                        else:
                            delta = -qty
                        async with binance_ws_lock:
                            trades_list = binance_ws_data[symbol].get('trades', [])
                            trades_list.append({'q': qty, 'm': result.get('m', False)})
                            if len(trades_list) > 500:
                                trades_list.pop(0)
                            binance_ws_data[symbol]['trades'] = trades_list
                            cvd = calculate_cvd(trades_list)
                            binance_ws_data[symbol]['cvd'] = cvd
        except Exception as e:
            print(f"Binance WebSocket error: {e}, reconnecting in 5s...")
            await asyncio.sleep(5)

# =========================
# TELEGRAM (send)
# =========================

async def send_telegram(message, chart_url=None):
    try:
        async with aiohttp.ClientSession() as session:
            for chat_id in TELEGRAM_CHAT_IDS:
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                payload = {"chat_id": chat_id, "text": message}
                if chart_url:
                    payload["reply_markup"] = {
                        "inline_keyboard": [[
                            {"text": "📊 Chart", "url": chart_url}
                        ]]
                    }
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        print(f"Telegram error for {chat_id}:", resp.status)
    except Exception as e:
        print("Telegram send failed:", e)

# =========================
# TELEGRAM (timeframe UI + polling)
# =========================

def timeframe_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "1d",  "callback_data": "tf:1d"},
                {"text": "30m", "callback_data": "tf:30m"},
                {"text": "1hr", "callback_data": "tf:1hr"},
            ]
        ]
    }

async def send_timeframe_picker(session, chat_id, text=None):
    if text is None:
        text = (
            f"⏱ Timeframe Settings\n\n"
            f"Current: {CURRENT_TIMEFRAME}\n\n"
            f"Tap a button to change."
        )
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "reply_markup": timeframe_keyboard()}
    async with session.post(url, json=payload) as resp:
        if resp.status != 200:
            print(f"Telegram picker error for {chat_id}:", resp.status)

async def answer_callback(session, callback_id, text=""):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    payload = {"callback_query_id": callback_id, "text": text}
    async with session.post(url, json=payload):
        pass

async def edit_message(session, chat_id, message_id, text, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text}
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    async with session.post(url, json=payload):
        pass

async def handle_message(session, message):
    chat_id = message.get("chat", {}).get("id")
    if str(chat_id) not in TELEGRAM_CHAT_IDS:
        return
    text = (message.get("text") or "").strip()
    if text.startswith("/timeframe") or text.startswith("/start"):
        await send_timeframe_picker(session, chat_id)

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
    if not data.startswith("tf:"):
        await answer_callback(session, callback_id, "Unknown action.")
        return
    new_tf = data[3:]
    if new_tf not in ALLOWED_TIMEFRAMES:
        await answer_callback(session, callback_id, "Invalid timeframe.")
        return
    old_tf = CURRENT_TIMEFRAME
    CURRENT_TIMEFRAME = new_tf
    await answer_callback(session, callback_id, f"Set to {new_tf}")
    new_text = (
        f"⏱ Timeframe Settings\n\n"
        f"Current: {CURRENT_TIMEFRAME}  (was {old_tf})\n\n"
        f"Tap a button to change."
    )
    await edit_message(session, chat_id, message_id, new_text, timeframe_keyboard())

async def poll_telegram():
    offset = None
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
                params = {
                    "timeout": 30,
                    "allowed_updates": json.dumps(["message", "callback_query"]),
                }
                if offset is not None:
                    params["offset"] = offset
                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(3)
                        continue
                    data = await resp.json()
                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    try:
                        if "message" in update:
                            await handle_message(session, update["message"])
                        elif "callback_query" in update:
                            await handle_callback(session, update["callback_query"])
                    except Exception as e:
                        print("Telegram update handling error:", e)
            except Exception as e:
                print("Telegram poll error:", e)
                await asyncio.sleep(5)

# =========================
# GATE.IO SCANNER
# =========================

async def scan_gateio(first_run=False):
    print("Scanning Gate.io...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(GATEIO_TICKERS) as resp:
                if resp.status != 200:
                    print("Gate.io API error:", resp.status)
                    return
                ticker_data = await resp.json()
        for item in ticker_data:
            symbol = item["currency_pair"]
            if not symbol.endswith("_USDT"):
                continue
            base = symbol.split("_")[0]
            threshold = BIG_COIN_PRICE_THRESHOLD if base in BIG_COINS else PRICE_SPIKE_PERCENT
            volume = float(item.get("quote_volume", 0))
            current_price = float(item.get("last", 0))
            price_change = float(item.get("change_percentage", 0))
            meets_threshold = volume >= GATEIO_MIN_VOLUME
            cursor.execute(
                "SELECT alerted, baseline_volume, baseline_price, last_alert, prev_rsi FROM gateio_listings WHERE symbol=?",
                (symbol,)
            )
            row = cursor.fetchone()
            now = int(time.time())
            if not row:
                cursor.execute(
                    "INSERT INTO gateio_listings (symbol, alerted, baseline_volume, baseline_price, last_alert) VALUES (?, ?, ?, ?, ?)",
                    (symbol, 0, volume, current_price, 0)
                )
                conn.commit()
                continue
            alerted, baseline_volume, baseline_price, last_alert, prev_rsi = row
            if last_alert and (now - last_alert) < COOLDOWN:
                continue
            instant_volume_spike = (baseline_volume > 0) and ((volume - baseline_volume) / baseline_volume * 100 >= VOLUME_SPIKE_PERCENT)
            instant_price_spike = price_change >= threshold
            cumulative_volume_growth = ((volume - baseline_volume) / baseline_volume * 100) if baseline_volume > 0 else 0
            cumulative_price_growth = ((current_price - baseline_price) / baseline_price * 100) if baseline_price > 0 else 0
            cumulative_growth_trigger = cumulative_volume_growth >= VOLUME_SPIKE_PERCENT and cumulative_price_growth >= threshold
            pump_condition = meets_threshold and ((instant_volume_spike and instant_price_spike) or cumulative_growth_trigger)
            if pump_condition and alerted == 0:
                signal = "Long" if price_change > 0 else "Short"
                cursor.execute(
                    "UPDATE gateio_listings SET alerted=1, baseline_volume=?, baseline_price=?, last_alert=? WHERE symbol=?",
                    (volume, current_price, now, symbol)
                )
                conn.commit()
                of = await fetch_gateio_spot_orderflow(symbol)
                of_line = format_of_line(of)
                message = (
                    f"🚨 GATE.IO PUMP ALERT\n\n"
                    f"Pair: {symbol}\n"
                    f"⏱ Timeframe: {CURRENT_TIMEFRAME}\n"
                    f"Price Change: {price_change:+.2f}%\n"
                    f"Volume Growth: {cumulative_volume_growth:.2f}%\n"
                    f"Volume: ${volume:,.0f}\n"
                    f"Entry Signal: Consider {signal}\n"
                    f"{of_line}\n\n"
                    f"========================"
                )
                await send_telegram(message, tradingview_url("gateio_spot", symbol))
            elif not pump_condition and alerted == 1:
                cursor.execute("UPDATE gateio_listings SET alerted=0 WHERE symbol=?", (symbol,))
                conn.commit()
    except Exception:
        print("Gate.io scan error:")
        traceback.print_exc()

# =========================
# MEXC SCANNER
# =========================

async def scan_mexc(first_run=False):
    print("Scanning MEXC...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(MEXC_TICKERS) as resp:
                if resp.status != 200:
                    print("MEXC API error:", resp.status)
                    return
                ticker_data = await resp.json()
        for item in ticker_data:
            symbol = item["symbol"]
            if not symbol.endswith("USDT"):
                continue
            base = symbol[:-4] if symbol.endswith("USDT") else symbol
            threshold = BIG_COIN_PRICE_THRESHOLD if base in BIG_COINS else PRICE_SPIKE_PERCENT
            volume = float(item.get("quoteVolume", 0))
            current_price = float(item.get("lastPrice", 0))
            price_change = float(item.get("priceChangePercent", 0)) * 100
            meets_threshold = volume >= MEXC_MIN_VOLUME
            cursor.execute(
                "SELECT alerted, baseline_volume, baseline_price, last_alert, prev_rsi FROM mexc_listings WHERE symbol=?",
                (symbol,)
            )
            row = cursor.fetchone()
            now = int(time.time())
            if not row:
                cursor.execute(
                    "INSERT INTO mexc_listings (symbol, alerted, baseline_volume, baseline_price, last_alert) VALUES (?, ?, ?, ?, ?)",
                    (symbol, 0, volume, current_price, 0)
                )
                conn.commit()
                continue
            alerted, baseline_volume, baseline_price, last_alert, prev_rsi = row
            if last_alert and (now - last_alert) < COOLDOWN:
                continue
            instant_volume_spike = (baseline_volume > 0) and ((volume - baseline_volume) / baseline_volume * 100 >= VOLUME_SPIKE_PERCENT)
            instant_price_spike = price_change >= threshold
            cumulative_volume_growth = ((volume - baseline_volume) / baseline_volume * 100) if baseline_volume > 0 else 0
            cumulative_price_growth = ((current_price - baseline_price) / baseline_price * 100) if baseline_price > 0 else 0
            cumulative_growth_trigger = cumulative_volume_growth >= VOLUME_SPIKE_PERCENT and cumulative_price_growth >= threshold
            pump_condition = meets_threshold and ((instant_volume_spike and instant_price_spike) or cumulative_growth_trigger)
            if pump_condition and alerted == 0:
                signal = "Long" if price_change > 0 else "Short"
                cursor.execute(
                    "UPDATE mexc_listings SET alerted=1, baseline_volume=?, baseline_price=?, last_alert=? WHERE symbol=?",
                    (volume, current_price, now, symbol)
                )
                conn.commit()
                of = await fetch_mexc_spot_orderflow(symbol)
                of_line = format_of_line(of)
                message = (
                    f"🚨 MEXC PUMP ALERT\n\n"
                    f"Pair: {symbol}\n"
                    f"⏱ Timeframe: {CURRENT_TIMEFRAME}\n"
                    f"Price Change: {price_change:+.2f}%\n"
                    f"Volume Growth: {cumulative_volume_growth:.2f}%\n"
                    f"Volume: ${volume:,.0f}\n"
                    f"Entry Signal: Consider {signal}\n"
                    f"{of_line}\n\n"
                    f"========================"
                )
                await send_telegram(message, tradingview_url("mexc_spot", symbol))
            elif not pump_condition and alerted == 1:
                cursor.execute("UPDATE mexc_listings SET alerted=0 WHERE symbol=?", (symbol,))
                conn.commit()
    except Exception:
        print("MEXC scan error:")
        traceback.print_exc()

# =========================
# CMC SCANNER
# =========================

async def scan_cmc(first_run=False):
    print("Scanning CMC...")
    headers = {"X-CMC_PRO_API_KEY": CMC_API_KEY}
    params = {"start": "1", "limit": "200", "sort": "date_added", "sort_dir": "desc", "convert": "USD"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(CMC_LISTINGS, headers=headers, params=params) as resp:
                if resp.status != 200:
                    print("CMC API error:", resp.status)
                    return
                data = await resp.json()
        for coin in data.get("data", []):
            coin_id = coin["id"]
            volume = coin["quote"]["USD"].get("volume_24h") or 0
            current_price = coin["quote"]["USD"].get("price") or 0
            price_change = coin["quote"]["USD"].get("percent_change_1h") or 0
            marketcap = coin["quote"]["USD"].get("market_cap") or 0
            slug = coin.get("slug")
            meets_threshold = marketcap >= CMC_MIN_MARKETCAP and volume >= CMC_MIN_VOLUME
            cursor.execute(
                "SELECT alerted, baseline_volume, baseline_price, last_alert, prev_rsi FROM cmc_listings WHERE id=?",
                (coin_id,)
            )
            row = cursor.fetchone()
            now = int(time.time())
            if not row:
                cursor.execute(
                    "INSERT INTO cmc_listings (id, alerted, baseline_volume, baseline_price, last_alert) VALUES (?, ?, ?, ?, ?)",
                    (coin_id, 0, volume, current_price, 0)
                )
                conn.commit()
                continue
            alerted, baseline_volume, baseline_price, last_alert, prev_rsi = row
            if last_alert and (now - last_alert) < COOLDOWN:
                continue
            instant_volume_spike = (baseline_volume > 0) and ((volume - baseline_volume) / baseline_volume * 100 >= VOLUME_SPIKE_PERCENT)
            instant_price_spike = price_change >= PRICE_SPIKE_PERCENT
            cumulative_volume_growth = ((volume - baseline_volume) / baseline_volume * 100) if baseline_volume > 0 else 0
            cumulative_price_growth = ((current_price - baseline_price) / baseline_price * 100) if baseline_price > 0 else 0
            cumulative_growth_trigger = cumulative_volume_growth >= VOLUME_SPIKE_PERCENT and cumulative_price_growth >= PRICE_SPIKE_PERCENT
            pump_condition = meets_threshold and ((instant_volume_spike and instant_price_spike) or cumulative_growth_trigger)
            if pump_condition and alerted == 0:
                signal = "Long" if price_change > 0 else "Short"
                cursor.execute(
                    "UPDATE cmc_listings SET alerted=1, baseline_volume=?, baseline_price=?, last_alert=? WHERE id=?",
                    (volume, current_price, now, coin_id)
                )
                conn.commit()
                chart_link = f"https://coinmarketcap.com/currencies/{slug}/"
                of = await fetch_binance_fut_orderflow(f"{coin['symbol']}USDT")
                of_line = format_of_line(of)
                message = (
                    f"🚨 CMC PUMP ALERT\n\n"
                    f"Pair: {coin['symbol']}USDT\n"
                    f"⏱ Timeframe: {CURRENT_TIMEFRAME}\n"
                    f"Price Change: {cumulative_price_growth:+.2f}%\n"
                    f"Volume Growth: {cumulative_volume_growth:.2f}%\n"
                    f"Volume: ${volume:,.0f}\n"
                    f"Entry Signal: Consider {signal}\n"
                    f"{of_line}\n\n"
                    f"Chart:\n{chart_link}\n\n"
                    f"========================"
                )
                await send_telegram(message, tradingview_url("cmc", coin['symbol']))
            elif not pump_condition and alerted == 1:
                cursor.execute("UPDATE cmc_listings SET alerted=0 WHERE id=?", (coin_id,))
                conn.commit()
    except Exception:
        print("CMC scan error:")
        traceback.print_exc()

# =========================
# GATE.IO FUTURES SCANNER
# =========================

async def scan_gateio_futures(first_run=False):
    print("Scanning Gate.io Futures...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(GATEIO_FUTURES_TICKERS) as resp:
                if resp.status != 200:
                    print("Gate.io Futures API error:", resp.status)
                    return
                ticker_data = await resp.json()
        for item in ticker_data:
            symbol = item["contract"]
            if not symbol.endswith("_USDT"):
                continue
            base = symbol.split("_")[0]
            threshold = BIG_COIN_PRICE_THRESHOLD if base in BIG_COINS else FUTURES_PRICE_CHANGE_THRESHOLD
            volume = float(item.get("volume_24h_quote", 0))
            current_price = float(item.get("last", 0))
            price_change = float(item.get("change_percentage", 0))
            meets_threshold = volume >= FUTURES_MIN_VOLUME
            cursor.execute(
                "SELECT alerted, baseline_volume, baseline_price, last_alert, prev_rsi FROM gateio_futures_listings WHERE symbol=?",
                (symbol,)
            )
            row = cursor.fetchone()
            now = int(time.time())
            if not row:
                cursor.execute(
                    "INSERT INTO gateio_futures_listings (symbol, alerted, baseline_volume, baseline_price, last_alert) VALUES (?, ?, ?, ?, ?)",
                    (symbol, 0, volume, current_price, 0)
                )
                conn.commit()
                continue
            alerted, baseline_volume, baseline_price, last_alert, prev_rsi = row
            if last_alert and (now - last_alert) < COOLDOWN:
                continue
            instant_volume_spike = (baseline_volume > 0) and ((volume - baseline_volume) / baseline_volume * 100 >= VOLUME_SPIKE_PERCENT)
            instant_price_spike = abs(price_change) >= threshold
            cumulative_volume_growth = ((volume - baseline_volume) / baseline_volume * 100) if baseline_volume > 0 else 0
            cumulative_price_growth = ((current_price - baseline_price) / baseline_price * 100) if baseline_price > 0 else 0
            cumulative_growth_trigger = cumulative_volume_growth >= VOLUME_SPIKE_PERCENT and abs(cumulative_price_growth) >= threshold
            trigger = meets_threshold and ((instant_volume_spike and instant_price_spike) or cumulative_growth_trigger)
            if not trigger:
                if alerted == 1:
                    cursor.execute("UPDATE gateio_futures_listings SET alerted=0 WHERE symbol=?", (symbol,))
                    conn.commit()
                continue
            klines = await fetch_gateio_klines(symbol)
            if not klines or len(klines["closes"]) < 14:
                continue
            closes = klines["closes"]
            opens = klines["opens"]
            highs = klines["highs"]
            lows = klines["lows"]
            ema20 = calculate_ema(closes, period=20)
            rsi = calculate_rsi(closes, period=14)
            latest_strength = calculate_candle_strength(opens[-1], closes[-1], highs[-1], lows[-1])
            candle_bullish = closes[-1] > opens[-1]
            candle_bearish = closes[-1] < opens[-1]
            if price_change > 0:
                signal = "Long"
                ema_confirmed = (ema20 is not None and current_price > ema20)
                if rsi is not None:
                    if prev_rsi < 55 and rsi >= 55 and rsi > prev_rsi:
                        rsi_confirmed = True
                    elif prev_rsi >= 55 and rsi >= 55 and (rsi - prev_rsi) >= RSI_POINTS_THRESHOLD:
                        rsi_confirmed = True
                    else:
                        rsi_confirmed = False
                else:
                    rsi_confirmed = False
                candle_confirmed = (latest_strength >= 0.6 and candle_bullish)
            else:
                signal = "Short"
                ema_confirmed = (ema20 is not None and current_price < ema20)
                if rsi is not None:
                    if prev_rsi > 45 and rsi <= 45 and rsi < prev_rsi:
                        rsi_confirmed = True
                    elif prev_rsi <= 45 and rsi <= 45 and (prev_rsi - rsi) >= RSI_POINTS_THRESHOLD:
                        rsi_confirmed = True
                    else:
                        rsi_confirmed = False
                else:
                    rsi_confirmed = False
                candle_confirmed = (latest_strength >= 0.6 and candle_bearish)
            confirmations = sum([ema_confirmed, rsi_confirmed, candle_confirmed])
            if rsi is not None:
                cursor.execute("UPDATE gateio_futures_listings SET prev_rsi=? WHERE symbol=?", (rsi, symbol))
                conn.commit()
            if confirmations >= 2:
                if alerted == 0:
                    cursor.execute(
                        "UPDATE gateio_futures_listings SET alerted=1, baseline_volume=?, baseline_price=?, last_alert=? WHERE symbol=?",
                        (volume, current_price, now, symbol)
                    )
                    conn.commit()
                    ema_str = f"{ema20:.2f}" if ema20 is not None else "N/A"
                    rsi_str = f"{rsi:.2f}" if rsi is not None else "N/A"
                    strength_str = f"{latest_strength:.2f}"
                    of = await fetch_gateio_fut_orderflow(symbol)
                    of_line = format_of_line(of)
                    message = (
                        f"🚨 GATE.IO FUTURES PUMP ALERT\n\n"
                        f"Contract: {symbol}\n"
                        f"⏱ Timeframe: {CURRENT_TIMEFRAME}\n"
                        f"Price Change: {price_change:+.2f}%\n"
                        f"Volume Growth: {cumulative_volume_growth:.2f}%\n"
                        f"Volume: ${volume:,.0f}\n"
                        f"Entry Signal: Consider {signal}\n"
                        f"Confirmations: {confirmations}/3\n"
                        f"EMA20: {ema_str}\n"
                        f"RSI: {rsi_str}\n"
                        f"Candle Strength: {strength_str}\n"
                        f"{of_line}\n\n"
                        f"========================"
                    )
                    await send_telegram(message, tradingview_url("gateio_fut", symbol))
            else:
                if alerted == 1:
                    cursor.execute("UPDATE gateio_futures_listings SET alerted=0 WHERE symbol=?", (symbol,))
                    conn.commit()
    except Exception:
        print("Gate.io Futures scan error:")
        traceback.print_exc()

# =========================
# MEXC FUTURES SCANNER
# =========================

async def scan_mexc_futures(first_run=False):
    print("Scanning MEXC Futures...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(MEXC_FUTURES_TICKERS) as resp:
                if resp.status != 200:
                    print("MEXC Futures API error:", resp.status)
                    return
                data = await resp.json()
        tickers = data.get("data", [])
        for item in tickers:
            symbol = item.get("symbol")
            if not symbol.endswith("USDT"):
                continue
            base = symbol.split("_")[0]
            threshold = BIG_COIN_PRICE_THRESHOLD if base in BIG_COINS else FUTURES_PRICE_CHANGE_THRESHOLD
            volume = float(item.get("amount24", 0))
            current_price = float(item.get("lastPrice", 0))
            price_change = float(item.get("riseFallRate", 0)) * 100
            meets_threshold = volume >= FUTURES_MIN_VOLUME
            cursor.execute(
                "SELECT alerted, baseline_volume, baseline_price, last_alert, prev_rsi FROM mexc_futures_listings WHERE symbol=?",
                (symbol,)
            )
            row = cursor.fetchone()
            now = int(time.time())
            if not row:
                cursor.execute(
                    "INSERT INTO mexc_futures_listings (symbol, alerted, baseline_volume, baseline_price, last_alert) VALUES (?, ?, ?, ?, ?)",
                    (symbol, 0, volume, current_price, 0)
                )
                conn.commit()
                continue
            alerted, baseline_volume, baseline_price, last_alert, prev_rsi = row
            if last_alert and (now - last_alert) < COOLDOWN:
                continue
            instant_volume_spike = (baseline_volume > 0) and ((volume - baseline_volume) / baseline_volume * 100 >= VOLUME_SPIKE_PERCENT)
            instant_price_spike = abs(price_change) >= threshold
            cumulative_volume_growth = ((volume - baseline_volume) / baseline_volume * 100) if baseline_volume > 0 else 0
            cumulative_price_growth = ((current_price - baseline_price) / baseline_price * 100) if baseline_price > 0 else 0
            cumulative_growth_trigger = cumulative_volume_growth >= VOLUME_SPIKE_PERCENT and abs(cumulative_price_growth) >= threshold
            trigger = meets_threshold and ((instant_volume_spike and instant_price_spike) or cumulative_growth_trigger)
            if not trigger:
                if alerted == 1:
                    cursor.execute("UPDATE mexc_futures_listings SET alerted=0 WHERE symbol=?", (symbol,))
                    conn.commit()
                continue
            klines = await fetch_mexc_klines(symbol)
            if not klines or len(klines["closes"]) < 14:
                continue
            closes = klines["closes"]
            opens = klines["opens"]
            highs = klines["highs"]
            lows = klines["lows"]
            ema20 = calculate_ema(closes, period=20)
            rsi = calculate_rsi(closes, period=14)
            latest_strength = calculate_candle_strength(opens[-1], closes[-1], highs[-1], lows[-1])
            candle_bullish = closes[-1] > opens[-1]
            candle_bearish = closes[-1] < opens[-1]
            if price_change > 0:
                signal = "Long"
                ema_confirmed = (ema20 is not None and current_price > ema20)
                if rsi is not None:
                    if prev_rsi < 55 and rsi >= 55 and rsi > prev_rsi:
                        rsi_confirmed = True
                    elif prev_rsi >= 55 and rsi >= 55 and (rsi - prev_rsi) >= RSI_POINTS_THRESHOLD:
                        rsi_confirmed = True
                    else:
                        rsi_confirmed = False
                else:
                    rsi_confirmed = False
                candle_confirmed = (latest_strength >= 0.6 and candle_bullish)
            else:
                signal = "Short"
                ema_confirmed = (ema20 is not None and current_price < ema20)
                if rsi is not None:
                    if prev_rsi > 45 and rsi <= 45 and rsi < prev_rsi:
                        rsi_confirmed = True
                    elif prev_rsi <= 45 and rsi <= 45 and (prev_rsi - rsi) >= RSI_POINTS_THRESHOLD:
                        rsi_confirmed = True
                    else:
                        rsi_confirmed = False
                else:
                    rsi_confirmed = False
                candle_confirmed = (latest_strength >= 0.6 and candle_bearish)
            confirmations = sum([ema_confirmed, rsi_confirmed, candle_confirmed])
            if rsi is not None:
                cursor.execute("UPDATE mexc_futures_listings SET prev_rsi=? WHERE symbol=?", (rsi, symbol))
                conn.commit()
            if confirmations >= 2:
                if alerted == 0:
                    cursor.execute(
                        "UPDATE mexc_futures_listings SET alerted=1, baseline_volume=?, baseline_price=?, last_alert=? WHERE symbol=?",
                        (volume, current_price, now, symbol)
                    )
                    conn.commit()
                    ema_str = f"{ema20:.2f}" if ema20 is not None else "N/A"
                    rsi_str = f"{rsi:.2f}" if rsi is not None else "N/A"
                    strength_str = f"{latest_strength:.2f}"
                    of = await fetch_mexc_fut_orderflow(symbol)
                    of_line = format_of_line(of)
                    message = (
                        f"🚨 MEXC FUTURES PUMP ALERT\n\n"
                        f"Contract: {symbol}\n"
                        f"⏱ Timeframe: {CURRENT_TIMEFRAME}\n"
                        f"Price Change: {price_change:+.2f}%\n"
                        f"Volume Growth: {cumulative_volume_growth:.2f}%\n"
                        f"Volume: ${volume:,.0f}\n"
                        f"Entry Signal: Consider {signal}\n"
                        f"Confirmations: {confirmations}/3\n"
                        f"EMA20: {ema_str}\n"
                        f"RSI: {rsi_str}\n"
                        f"Candle Strength: {strength_str}\n"
                        f"{of_line}\n\n"
                        f"========================"
                    )
                    await send_telegram(message, tradingview_url("mexc_fut", symbol))
            else:
                if alerted == 1:
                    cursor.execute("UPDATE mexc_futures_listings SET alerted=0 WHERE symbol=?", (symbol,))
                    conn.commit()
    except Exception:
        print("MEXC Futures scan error:")
        traceback.print_exc()

# =========================
# BINANCE FUTURES SCANNER
# =========================

async def scan_binance_futures(first_run=False):
    print("Scanning Binance Futures...")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(BINANCE_FUTURES_TICKERS) as resp:
                if resp.status != 200:
                    print("Binance Futures API error:", resp.status)
                    return
                tickers = await resp.json()
        for item in tickers:
            symbol = item["symbol"]
            if not symbol.endswith("USDT"):
                continue
            base = symbol[:-4]
            threshold = BIG_COIN_PRICE_THRESHOLD if base in BIG_COINS else BINANCE_PRICE_CHANGE_THRESHOLD
            volume = float(item.get("quoteVolume", 0))
            current_price = float(item.get("lastPrice", 0))
            price_change = float(item.get("priceChangePercent", 0))
            meets_threshold = volume >= FUTURES_MIN_VOLUME
            cursor.execute(
                "SELECT alerted, baseline_volume, baseline_price, last_alert, prev_rsi FROM binance_futures_listings WHERE symbol=?",
                (symbol,)
            )
            row = cursor.fetchone()
            now = int(time.time())
            if not row:
                cursor.execute(
                    "INSERT INTO binance_futures_listings (symbol, alerted, baseline_volume, baseline_price, last_alert) VALUES (?, ?, ?, ?, ?)",
                    (symbol, 0, volume, current_price, 0)
                )
                conn.commit()
                continue
            alerted, baseline_volume, baseline_price, last_alert, prev_rsi = row
            if last_alert and (now - last_alert) < COOLDOWN:
                continue
            instant_volume_spike = (baseline_volume > 0) and ((volume - baseline_volume) / baseline_volume * 100 >= BINANCE_VOLUME_SPIKE_PERCENT)
            instant_price_spike = abs(price_change) >= threshold
            cumulative_volume_growth = ((volume - baseline_volume) / baseline_volume * 100) if baseline_volume > 0 else 0
            cumulative_price_growth = ((current_price - baseline_price) / baseline_price * 100) if baseline_price > 0 else 0
            cumulative_growth_trigger = cumulative_volume_growth >= BINANCE_VOLUME_SPIKE_PERCENT and abs(cumulative_price_growth) >= threshold
            trigger = meets_threshold and ((instant_volume_spike and instant_price_spike) or cumulative_growth_trigger)
            if not trigger:
                if alerted == 1:
                    cursor.execute("UPDATE binance_futures_listings SET alerted=0 WHERE symbol=?", (symbol,))
                    conn.commit()
                continue
            klines = await fetch_binance_klines(symbol)
            if not klines or len(klines["closes"]) < 14:
                continue
            closes = klines["closes"]
            opens = klines["opens"]
            highs = klines["highs"]
            lows = klines["lows"]
            ema20 = calculate_ema(closes, period=20)
            rsi = calculate_rsi(closes, period=14)
            latest_strength = calculate_candle_strength(opens[-1], closes[-1], highs[-1], lows[-1])
            candle_bullish = closes[-1] > opens[-1]
            candle_bearish = closes[-1] < opens[-1]
            if price_change > 0:
                signal = "Long"
                ema_confirmed = (ema20 is not None and current_price > ema20)
                if rsi is not None:
                    if prev_rsi < 55 and rsi >= 55 and rsi > prev_rsi:
                        rsi_confirmed = True
                    elif prev_rsi >= 55 and rsi >= 55 and (rsi - prev_rsi) >= RSI_POINTS_THRESHOLD:
                        rsi_confirmed = True
                    else:
                        rsi_confirmed = False
                else:
                    rsi_confirmed = False
                candle_confirmed = (latest_strength >= 0.6 and candle_bullish)
            else:
                signal = "Short"
                ema_confirmed = (ema20 is not None and current_price < ema20)
                if rsi is not None:
                    if prev_rsi > 45 and rsi <= 45 and rsi < prev_rsi:
                        rsi_confirmed = True
                    elif prev_rsi <= 45 and rsi <= 45 and (prev_rsi - rsi) >= RSI_POINTS_THRESHOLD:
                        rsi_confirmed = True
                    else:
                        rsi_confirmed = False
                else:
                    rsi_confirmed = False
                candle_confirmed = (latest_strength >= 0.6 and candle_bearish)
            confirmations = sum([ema_confirmed, rsi_confirmed, candle_confirmed])
            if rsi is not None:
                cursor.execute("UPDATE binance_futures_listings SET prev_rsi=? WHERE symbol=?", (rsi, symbol))
                conn.commit()
            if confirmations < 3:
                if alerted == 1:
                    cursor.execute("UPDATE binance_futures_listings SET alerted=0 WHERE symbol=?", (symbol,))
                    conn.commit()
                continue
            async with binance_ws_lock:
                ws_data = binance_ws_data.get(symbol, {})
                footprint = ws_data.get('footprint', 0)
                cvd = ws_data.get('cvd', 0)
            if signal == "Long":
                if footprint <= BINANCE_FOOTPRINT_THRESHOLD or cvd <= BINANCE_CVD_THRESHOLD:
                    if alerted == 1:
                        cursor.execute("UPDATE binance_futures_listings SET alerted=0 WHERE symbol=?", (symbol,))
                        conn.commit()
                    continue
            else:
                if footprint >= -BINANCE_FOOTPRINT_THRESHOLD or cvd >= -BINANCE_CVD_THRESHOLD:
                    if alerted == 1:
                        cursor.execute("UPDATE binance_futures_listings SET alerted=0 WHERE symbol=?", (symbol,))
                        conn.commit()
                    continue
            if alerted == 0:
                cursor.execute(
                    "UPDATE binance_futures_listings SET alerted=1, baseline_volume=?, baseline_price=?, last_alert=? WHERE symbol=?",
                    (volume, current_price, now, symbol)
                )
                conn.commit()
                ema_str = f"{ema20:.2f}" if ema20 is not None else "N/A"
                rsi_str = f"{rsi:.2f}" if rsi is not None else "N/A"
                strength_str = f"{latest_strength:.2f}"
                if footprint == 0 and cvd == 0:
                    of = await fetch_binance_fut_orderflow(symbol)
                else:
                    of = {'footprint': footprint, 'cvd': cvd}
                of_line = format_of_line(of)
                message = (
                    f"🚨 BINANCE FUTURES PUMP ALERT\n\n"
                    f"Contract: {symbol}\n"
                    f"⏱ Timeframe: {CURRENT_TIMEFRAME}\n"
                    f"Price Change: {price_change:+.2f}%\n"
                    f"Volume Growth: {cumulative_volume_growth:.2f}%\n"
                    f"Volume: ${volume:,.0f}\n"
                    f"Entry Signal: Consider {signal}\n"
                    f"Confirmations: {confirmations}/3\n"
                    f"EMA20: {ema_str}\n"
                    f"RSI: {rsi_str}\n"
                    f"Candle Strength: {strength_str}\n"
                    f"{of_line}\n\n"
                    f"========================"
                )
                await send_telegram(message, tradingview_url("binance_fut", symbol))
    except Exception:
        print("Binance Futures scan error:")
        traceback.print_exc()

# =========================
# MAIN LOOP
# =========================

async def main():
    print("Starting Kachi...")
    await send_telegram(
        f"Sniping 🍁\n"
        f"⏱ Timeframe: {CURRENT_TIMEFRAME}\n\n"
        f"Send /timeframe to change."
    )

    asyncio.create_task(binance_ws_manager())
    asyncio.create_task(poll_telegram())

    print("Initializing database silently...")
    await scan_gateio(first_run=True)
    await scan_mexc(first_run=True)
    await scan_cmc(first_run=True)
    await scan_gateio_futures(first_run=True)
    await scan_mexc_futures(first_run=True)
    await scan_binance_futures(first_run=True)

    print("Bot running...")
    while True:
        await scan_gateio()
        await scan_mexc()
        await scan_cmc()
        await scan_gateio_futures()
        await scan_mexc_futures()
        await scan_binance_futures()
        print("Sleeping 60 seconds...\n")
        await asyncio.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
