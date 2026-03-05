import asyncio
import aiohttp
import sqlite3
import time
import traceback

# =========================
# CONFIG
# =========================

CMC_API_KEY = "6881c6f6d56b4cf58727255319ec235e"
TELEGRAM_BOT_TOKEN = "8673294426:AAGSrC6j_aUJmzHqlgowolKEBDEMjn01YwA"
TELEGRAM_CHAT_IDS = ["7198809557", "6065933220"]  # Added new chat ID

BINANCE_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"
BINANCE_TICKER = "https://api.binance.com/api/v3/ticker/24hr"
CMC_LISTINGS = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

BINANCE_MIN_VOLUME = 5_000_000
CMC_MIN_MARKETCAP = 10_000_000
CMC_MIN_VOLUME = 1_000_000

PRICE_SPIKE_PERCENT = 5
VOLUME_SPIKE_PERCENT = 20

CHECK_INTERVAL = 60  # 1 minute
COOLDOWN = 60 * 60  # 1 hour cooldown in seconds

# =========================
# DATABASE
# =========================

conn = sqlite3.connect("listings.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS binance_listings (
symbol TEXT PRIMARY KEY,
alerted INTEGER DEFAULT 0,
baseline_volume REAL DEFAULT 0,
baseline_price REAL DEFAULT 0,
last_alert INTEGER DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS cmc_listings (
id INTEGER PRIMARY KEY,
alerted INTEGER DEFAULT 0,
baseline_volume REAL DEFAULT 0,
baseline_price REAL DEFAULT 0,
last_alert INTEGER DEFAULT 0
)
""")

conn.commit()

# =========================
# TELEGRAM
# =========================

async def send_telegram(message):
    try:
        async with aiohttp.ClientSession() as session:
            for chat_id in TELEGRAM_CHAT_IDS:  # send to all chat IDs
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
# BINANCE SCANNER
# =========================

async def scan_binance(first_run=False):
    print("Scanning Binance...")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(BINANCE_EXCHANGE_INFO) as resp:
                if resp.status != 200:
                    print("Binance API error:", resp.status)
                    return
                exchange_data = await resp.json()

            async with session.get(BINANCE_TICKER) as ticker_resp:
                if ticker_resp.status != 200:
                    print("Binance Ticker API error:", resp.status)
                    return
                ticker_data = await ticker_resp.json()

        ticker_map = {item["symbol"]: item for item in ticker_data}

        for symbol_data in exchange_data.get("symbols", []):
            symbol = symbol_data["symbol"]

            if not symbol.endswith("USDT"):
                continue

            if any(x in symbol for x in ["UP", "DOWN", "BULL", "BEAR"]):
                continue

            if symbol not in ticker_map:
                continue

            vol_data = ticker_map[symbol]
            volume = float(vol_data.get("quoteVolume", 0))
            current_price = float(vol_data.get("lastPrice", 0))
            price_change = float(vol_data.get("priceChangePercent", 0))

            meets_threshold = volume >= BINANCE_MIN_VOLUME

            cursor.execute(
                "SELECT alerted, baseline_volume, baseline_price, last_alert FROM binance_listings WHERE symbol=?",
                (symbol,)
            )
            row = cursor.fetchone()

            now = int(time.time())

            if not row:
                cursor.execute(
                    "INSERT INTO binance_listings (symbol, alerted, baseline_volume, baseline_price, last_alert) VALUES (?, ?, ?, ?, ?)",
                    (symbol, 0, volume, current_price, 0)
                )
                conn.commit()
                continue

            alerted, baseline_volume, baseline_price, last_alert = row

            # Skip alert if still in cooldown
            if last_alert and (now - last_alert) < COOLDOWN:
                continue

            # Instant spike checks
            instant_volume_spike = (baseline_volume > 0) and ((volume - baseline_volume) / baseline_volume * 100 >= VOLUME_SPIKE_PERCENT)
            instant_price_spike = price_change >= PRICE_SPIKE_PERCENT

            # Cumulative growth checks
            cumulative_volume_growth = ((volume - baseline_volume) / baseline_volume * 100) if baseline_volume > 0 else 0
            cumulative_price_growth = ((current_price - baseline_price) / baseline_price * 100) if baseline_price > 0 else 0
            cumulative_growth_trigger = cumulative_volume_growth >= VOLUME_SPIKE_PERCENT and cumulative_price_growth >= PRICE_SPIKE_PERCENT

            pump_condition = meets_threshold and ((instant_volume_spike and instant_price_spike) or cumulative_growth_trigger)

            if pump_condition and alerted == 0:
                cursor.execute(
                    "UPDATE binance_listings SET alerted=1, baseline_volume=?, baseline_price=?, last_alert=? WHERE symbol=?",
                    (volume, current_price, now, symbol)
                )
                conn.commit()

                message = (
                    f"🚨 BINANCE PUMP ALERT\n\n"
                    f"Pair: {symbol}\n"
                    f"Price Change: {price_change:+.2f}%\n"
                    f"Volume Growth: {cumulative_volume_growth:.2f}%\n"
                    f"Volume: ${volume:,.0f}\n"
                    f"Entry Signal: Consider Long\n\n"
                    f"========================\n"
                    f"powered by @ZeusisHIM"
                )
                await send_telegram(message)

            elif not pump_condition and alerted == 1:
                cursor.execute(
                    "UPDATE binance_listings SET alerted=0 WHERE symbol=?",
                    (symbol,)
                )
                conn.commit()

    except Exception:
        print("Binance scan error:")
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
            volume = coin["quote"]["USD"].get("volume_24h", 0)
            current_price = coin["quote"]["USD"].get("price", 0)
            price_change = coin["quote"]["USD"].get("percent_change_1h", 0)
            marketcap = coin["quote"]["USD"].get("market_cap", 0)

            meets_threshold = marketcap >= CMC_MIN_MARKETCAP and volume >= CMC_MIN_VOLUME

            cursor.execute(
                "SELECT alerted, baseline_volume, baseline_price, last_alert FROM cmc_listings WHERE id=?",
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

            alerted, baseline_volume, baseline_price, last_alert = row

            if last_alert and (now - last_alert) < COOLDOWN:
                continue

            instant_volume_spike = (baseline_volume > 0) and ((volume - baseline_volume) / baseline_volume * 100 >= VOLUME_SPIKE_PERCENT)
            instant_price_spike = price_change >= PRICE_SPIKE_PERCENT

            cumulative_volume_growth = ((volume - baseline_volume) / baseline_volume * 100) if baseline_volume > 0 else 0
            cumulative_price_growth = ((current_price - baseline_price) / baseline_price * 100) if baseline_price > 0 else 0
            cumulative_growth_trigger = cumulative_volume_growth >= VOLUME_SPIKE_PERCENT and cumulative_price_growth >= PRICE_SPIKE_PERCENT

            pump_condition = meets_threshold and ((instant_volume_spike and instant_price_spike) or cumulative_growth_trigger)

            if pump_condition and alerted == 0:
                cursor.execute(
                    "UPDATE cmc_listings SET alerted=1, baseline_volume=?, baseline_price=?, last_alert=? WHERE id=?",
                    (volume, current_price, now, coin_id)
                )
                conn.commit()

                message = (
                    f"🚨 CMC PUMP ALERT\n\n"
                    f"Pair: {coin['symbol']}USDT\n"
                    f"Price Change: {cumulative_price_growth:+.2f}%\n"
                    f"Volume Growth: {cumulative_volume_growth:.2f}%\n"
                    f"Volume: ${volume:,.0f}\n"
                    f"Entry Signal: Consider Long\n\n"
                    f"========================\n"
                    f"powered by @ZeusisHIM"
                )
                await send_telegram(message)

            elif not pump_condition and alerted == 1:
                cursor.execute(
                    "UPDATE cmc_listings SET alerted=0 WHERE id=?",
                    (coin_id,)
                )
                conn.commit()

    except Exception:
        print("CMC scan error:")
        traceback.print_exc()

# =========================
# MAIN LOOP
# =========================

async def main():
    print("Starting Kachi...")

    await send_telegram("Sniping 🍁")

    print("Initializing database silently...")
    await scan_binance(first_run=True)
    await scan_cmc(first_run=True)

    print("Bot running...")

    while True:
        await scan_binance()
        await scan_cmc()
        print("Sleeping 60 seconds...\n")
        await asyncio.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
