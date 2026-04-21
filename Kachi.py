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
FUTURES_PRICE_CHANGE_THRESHOLD = 5  # absolute % change required

# Kline config for confirmations
KLINE_INTERVAL = "5m"          # Gate.io
MEXC_KLINE_INTERVAL = "Min5"   # MEXC
KLINE_LIMIT = 20               # enough for EMA20 and RSI14

# Binance Futures config
BINANCE_FUTURES_TICKERS = "https://fapi.binance.com/fapi/v1/ticker/24hr"
BINANCE_FUTURES_KLINES = "https://fapi.binance.com/fapi/v1/klines"
BINANCE_KLINE_INTERVAL = "5m"
BINANCE_KLINE_LIMIT = 20
BINANCE_WS_BASE = "wss://fstream.binance.com/stream?streams="
BINANCE_FOOTPRINT_THRESHOLD = 0.2   # reduced from 0.3
BINANCE_CVD_THRESHOLD = 20000       # reduced from 50000
BINANCE_PRICE_CHANGE_THRESHOLD = 1.5  # reduced from 3
BINANCE_VOLUME_SPIKE_PERCENT = 12   # new, reduced from 20

# RSI points threshold for already-in-zone confirmation (unchanged)
RSI_POINTS_THRESHOLD = 4

# Big coins that get a lower price change threshold (2%)
BIG_COINS = ["BTC", "ETH", "SOL"]
BIG_COIN_PRICE_THRESHOLD = 2

# =========================
# DATABASE
# =========================

conn = sqlite3.connect("listings.db")
cursor = conn.cursor()

# Gate.io spot
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

# MEXC spot
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

# CMC
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

# Gate.io futures
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

# MEXC futures
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

# Binance futures
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
    """Exponential Moving Average (simple implementation)"""
    if len(closes) < period:
        return None
    multiplier = 2 / (period + 1)
    ema = closes[0]  # start with first close
    for price in closes[1:]:
        ema = (price - ema) * multiplier + ema
    return ema

def calculate_rsi(closes, period=14):
    """Relative Strength Index (14 period)"""
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
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_candle_strength(open_price, close, high, low):
    """Candle strength = body / range (absolute)"""
    body = abs(close - open_price)
    range_ = high - low
    if range_ == 0:
        return 0
    return body / range_

def calculate_footprint(bids, asks):
    """Compute bid/ask imbalance ratio from depth snapshots"""
    bid_vol = sum(float(q) for _, q in bids)
    ask_vol = sum(float(q) for _, q in asks)
    total = bid_vol + ask_vol
    if total == 0:
        return 0
    return (bid_vol - ask_vol) / total

def calculate_cvd(trades):
    """Cumulative Volume Delta from a list of trade messages"""
    cvd = 0.0
    for trade in trades:
        qty = float(trade.get('q', 0))
        # m = true if buyer is maker (sell aggressive)
        if not trade.get('m', False):
            cvd += qty
        else:
            cvd -= qty
    return cvd

async def fetch_gateio_klines(symbol, interval=KLINE_INTERVAL, limit=KLINE_LIMIT):
    """Fetch klines from Gate.io futures"""
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

async def fetch_mexc_klines(symbol, interval=MEXC_KLINE_INTERVAL, limit=KLINE_LIMIT):
    """Fetch klines from MEXC futures (contract)"""
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

async def fetch_binance_klines(symbol, interval=BINANCE_KLINE_INTERVAL, limit=BINANCE_KLINE_LIMIT):
    """Fetch klines from Binance futures"""
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
    """Maintain WebSocket connections for all Binance USDT perpetuals"""
    async with aiohttp.ClientSession() as session:
        # Get exchange info to list all USDT perpetual symbols
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

    # Build combined stream URL for all symbols (depth20@100ms and trade)
    streams = []
    for sym in symbols:
        streams.append(f"{sym.lower()}@depth20@100ms")
        streams.append(f"{sym.lower()}@trade")
    # Binance combined streams have a limit of ~200 streams per connection.
    # Split into batches of 150 to be safe.
    batch_size = 150
    for i in range(0, len(streams), batch_size):
        batch = streams[i:i+batch_size]
        url = BINANCE_WS_BASE + "/".join(batch)
        asyncio.create_task(handle_binance_ws_connection(url))

async def handle_binance_ws_connection(url):
    """Connect to a single combined stream and process messages"""
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
                    # Extract symbol from stream name (e.g., "btcusdt@depth20@100ms")
                    symbol = stream.split('@')[0].upper()
                    async with binance_ws_lock:
                        if symbol not in binance_ws_data:
                            binance_ws_data[symbol] = {'footprint': 0, 'cvd': 0, 'trades': []}
                    if '@depth20' in stream:
                        # Order book snapshot
                        bids = result.get('b', [])
                        asks = result.get('a', [])
                        fp = calculate_footprint(bids, asks)
                        async with binance_ws_lock:
                            binance_ws_data[symbol]['footprint'] = fp
                    elif '@trade' in stream:
                        # Trade event (single trade)
                        qty = float(result.get('q', 0))
                        # m = true if buyer is maker (sell aggressive)
                        if not result.get('m', False):
                            delta = qty
                        else:
                            delta = -qty
                        async with binance_ws_lock:
                            # Keep a rolling sum for CVD (last 500 trades)
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
# TELEGRAM
# =========================

async def send_telegram(message):
    try:
        async with aiohttp.ClientSession() as session:
            for chat_id in TELEGRAM_CHAT_IDS:
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                payload = {
                    "chat_id": chat_id,
                    "text": message
                }
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        print(f"Telegram error for {chat_id}:", resp.status)
    except Exception as e:
        print("Telegram send failed:", e)

# =========================
# GATE.IO SCANNER (with big coin threshold)
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
            # Determine base asset
            base = symbol.split("_")[0]
            # Select threshold
            if base in BIG_COINS:
                threshold = BIG_COIN_PRICE_THRESHOLD
            else:
                threshold = PRICE_SPIKE_PERCENT
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
                message = (
                    f"🚨 GATE.IO PUMP ALERT\n\n"
                    f"Pair: {symbol}\n"
                    f"Price Change: {price_change:+.2f}%\n"
                    f"Volume Growth: {cumulative_volume_growth:.2f}%\n"
                    f"Volume: ${volume:,.0f}\n"
                    f"Entry Signal: Consider {signal}\n\n"
                    f"========================\n"
                    f"powered by @ZeusisHIM"
                )
                await send_telegram(message)
            elif not pump_condition and alerted == 1:
                cursor.execute("UPDATE gateio_listings SET alerted=0 WHERE symbol=?", (symbol,))
                conn.commit()
    except Exception:
        print("Gate.io scan error:")
        traceback.print_exc()

# =========================
# MEXC SCANNER (with big coin threshold)
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
            # Determine base asset (e.g., "BTCUSDT" -> "BTC")
            if symbol.endswith("USDT"):
                base = symbol[:-4]
            else:
                base = symbol
            if base in BIG_COINS:
                threshold = BIG_COIN_PRICE_THRESHOLD
            else:
                threshold = PRICE_SPIKE_PERCENT
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
                message = (
                    f"🚨 MEXC PUMP ALERT\n\n"
                    f"Pair: {symbol}\n"
                    f"Price Change: {price_change:+.2f}%\n"
                    f"Volume Growth: {cumulative_volume_growth:.2f}%\n"
                    f"Volume: ${volume:,.0f}\n"
                    f"Entry Signal: Consider {signal}\n\n"
                    f"========================\n"
                    f"powered by @ZeusisHIM"
                )
                await send_telegram(message)
            elif not pump_condition and alerted == 1:
                cursor.execute("UPDATE mexc_listings SET alerted=0 WHERE symbol=?", (symbol,))
                conn.commit()
    except Exception:
        print("MEXC scan error:")
        traceback.print_exc()

# =========================
# CMC SCANNER (unchanged – no klines)
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
                message = (
                    f"🚨 CMC PUMP ALERT\n\n"
                    f"Pair: {coin['symbol']}USDT\n"
                    f"Price Change: {cumulative_price_growth:+.2f}%\n"
                    f"Volume Growth: {cumulative_volume_growth:.2f}%\n"
                    f"Volume: ${volume:,.0f}\n"
                    f"Entry Signal: Consider {signal}\n\n"
                    f"Chart:\n{chart_link}\n\n"
                    f"========================\n"
                    f"powered by @ZeusisHIM"
                )
                await send_telegram(message)
            elif not pump_condition and alerted == 1:
                cursor.execute("UPDATE cmc_listings SET alerted=0 WHERE id=?", (coin_id,))
                conn.commit()
    except Exception:
        print("CMC scan error:")
        traceback.print_exc()

# =========================
# GATE.IO FUTURES SCANNER (with big coin threshold)
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
            if base in BIG_COINS:
                threshold = BIG_COIN_PRICE_THRESHOLD
            else:
                threshold = FUTURES_PRICE_CHANGE_THRESHOLD
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
            # Hybrid RSI logic: crossing OR 4-point move in zone
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
                    message = (
                        f"🚨 GATE.IO FUTURES PUMP ALERT\n\n"
                        f"Contract: {symbol}\n"
                        f"Price Change: {price_change:+.2f}%\n"
                        f"Volume Growth: {cumulative_volume_growth:.2f}%\n"
                        f"Volume: ${volume:,.0f}\n"
                        f"Entry Signal: Consider {signal}\n"
                        f"Confirmations: {confirmations}/3\n"
                        f"EMA20: {ema_str}\n"
                        f"RSI: {rsi_str}\n"
                        f"Candle Strength: {strength_str}\n\n"
                        f"========================\n"
                        f"powered by @ZeusisHIM"
                    )
                    await send_telegram(message)
            else:
                if alerted == 1:
                    cursor.execute("UPDATE gateio_futures_listings SET alerted=0 WHERE symbol=?", (symbol,))
                    conn.commit()
    except Exception:
        print("Gate.io Futures scan error:")
        traceback.print_exc()

# =========================
# MEXC FUTURES SCANNER (with big coin threshold)
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
            if base in BIG_COINS:
                threshold = BIG_COIN_PRICE_THRESHOLD
            else:
                threshold = FUTURES_PRICE_CHANGE_THRESHOLD
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
                    message = (
                        f"🚨 MEXC FUTURES PUMP ALERT\n\n"
                        f"Contract: {symbol}\n"
                        f"Price Change: {price_change:+.2f}%\n"
                        f"Volume Growth: {cumulative_volume_growth:.2f}%\n"
                        f"Volume: ${volume:,.0f}\n"
                        f"Entry Signal: Consider {signal}\n"
                        f"Confirmations: {confirmations}/3\n"
                        f"EMA20: {ema_str}\n"
                        f"RSI: {rsi_str}\n"
                        f"Candle Strength: {strength_str}\n\n"
                        f"========================\n"
                        f"powered by @ZeusisHIM"
                    )
                    await send_telegram(message)
            else:
                if alerted == 1:
                    cursor.execute("UPDATE mexc_futures_listings SET alerted=0 WHERE symbol=?", (symbol,))
                    conn.commit()
    except Exception:
        print("MEXC Futures scan error:")
        traceback.print_exc()

# =========================
# BINANCE FUTURES SCANNER (with reduced thresholds)
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
            # Base asset (e.g., "BTCUSDT" -> "BTC")
            base = symbol[:-4]
            if base in BIG_COINS:
                threshold = BIG_COIN_PRICE_THRESHOLD
            else:
                threshold = BINANCE_PRICE_CHANGE_THRESHOLD
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
            # Trigger conditions using Binance-specific volume spike threshold
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
            # Fetch klines for EMA/RSI/candle
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
            # Get footprint and CVD from WebSocket data
            async with binance_ws_lock:
                ws_data = binance_ws_data.get(symbol, {})
                footprint = ws_data.get('footprint', 0)
                cvd = ws_data.get('cvd', 0)
            # Apply footprint and CVD filters with reduced thresholds
            if signal == "Long":
                if footprint <= BINANCE_FOOTPRINT_THRESHOLD or cvd <= BINANCE_CVD_THRESHOLD:
                    if alerted == 1:
                        cursor.execute("UPDATE binance_futures_listings SET alerted=0 WHERE symbol=?", (symbol,))
                        conn.commit()
                    continue
            else:  # Short
                if footprint >= -BINANCE_FOOTPRINT_THRESHOLD or cvd >= -BINANCE_CVD_THRESHOLD:
                    if alerted == 1:
                        cursor.execute("UPDATE binance_futures_listings SET alerted=0 WHERE symbol=?", (symbol,))
                        conn.commit()
                    continue
            # All conditions met – send alert
            if alerted == 0:
                cursor.execute(
                    "UPDATE binance_futures_listings SET alerted=1, baseline_volume=?, baseline_price=?, last_alert=? WHERE symbol=?",
                    (volume, current_price, now, symbol)
                )
                conn.commit()
                ema_str = f"{ema20:.2f}" if ema20 is not None else "N/A"
                rsi_str = f"{rsi:.2f}" if rsi is not None else "N/A"
                strength_str = f"{latest_strength:.2f}"
                footprint_str = f"{footprint:.2f}"
                cvd_str = f"{cvd:.2f}"
                message = (
                    f"🚨 BINANCE FUTURES PUMP ALERT\n\n"
                    f"Contract: {symbol}\n"
                    f"Price Change: {price_change:+.2f}%\n"
                    f"Volume Growth: {cumulative_volume_growth:.2f}%\n"
                    f"Volume: ${volume:,.0f}\n"
                    f"Entry Signal: Consider {signal}\n"
                    f"Confirmations: {confirmations}/3\n"
                    f"EMA20: {ema_str}\n"
                    f"RSI: {rsi_str}\n"
                    f"Candle Strength: {strength_str}\n"
                    f"Footprint (Bid/Ask Imbalance): {footprint_str}\n"
                    f"CVD (Net Buying Volume): {cvd_str}\n\n"
                    f"========================\n"
                    f"powered by @ZeusisHIM"
                )
                await send_telegram(message)
    except Exception:
        print("Binance Futures scan error:")
        traceback.print_exc()

# =========================
# MAIN LOOP
# =========================

async def main():
    print("Starting Kachi...")
    await send_telegram("Sniping 🍁")

    # Start Binance WebSocket manager in background
    asyncio.create_task(binance_ws_manager())

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
