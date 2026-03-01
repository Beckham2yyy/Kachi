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
TELEGRAM_CHAT_ID = "7198809557"

BINANCE_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"
BINANCE_TICKER = "https://api.binance.com/api/v3/ticker/24hr"
CMC_LISTINGS = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

BINANCE_MIN_VOLUME = 5_000_000
CMC_MIN_MARKETCAP = 10_000_000
CMC_MIN_VOLUME = 1_000_000

CHECK_INTERVAL = 60  # 1 minute

# =========================
# DATABASE
# =========================

conn = sqlite3.connect("listings.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS binance_listings (
    symbol TEXT PRIMARY KEY,
    alerted INTEGER DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS cmc_listings (
    id INTEGER PRIMARY KEY,
    alerted INTEGER DEFAULT 0
)
""")

conn.commit()

# =========================
# TELEGRAM
# =========================

async def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    print("Telegram error:", resp.status)
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
                data = await resp.json()

        for symbol_data in data.get("symbols", []):
            symbol = symbol_data["symbol"]

            if not symbol.endswith("USDT"):
                continue

            if any(x in symbol for x in ["UP", "DOWN", "BULL", "BEAR"]):
                continue

            async with aiohttp.ClientSession() as session:
                async with session.get(f"{BINANCE_TICKER}?symbol={symbol}") as vol_resp:
                    if vol_resp.status != 200:
                        continue
                    vol_data = await vol_resp.json()

            volume = float(vol_data.get("quoteVolume", 0))
            meets_threshold = volume >= BINANCE_MIN_VOLUME

            cursor.execute("SELECT alerted FROM binance_listings WHERE symbol=?", (symbol,))
            row = cursor.fetchone()

            if not row:
                alerted_value = 1 if (meets_threshold and not first_run) else 0
                cursor.execute(
                    "INSERT INTO binance_listings (symbol, alerted) VALUES (?, ?)",
                    (symbol, alerted_value)
                )
                conn.commit()

                if meets_threshold and not first_run:
                    message = (
                        f"🚨 BINANCE POTENTIAL 10X\n\n"
                        f"🪙 Pair: {symbol}\n"
                        f"💰 24H Volume: ${volume:,.0f}\n"
                        f"📊 Threshold: ${BINANCE_MIN_VOLUME:,.0f}\n"
                        f"⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC\n\n"
                        f"Volume just crossed your trigger level."
                    )
                    await send_telegram(message)

            else:
                alerted = row[0]

                # Fresh breakout
                if meets_threshold and alerted == 0:
                    cursor.execute(
                        "UPDATE binance_listings SET alerted=1 WHERE symbol=?",
                        (symbol,)
                    )
                    conn.commit()

                    message = (
                        f"🚨 BINANCE POTENTIAL 10X\n\n"
                        f"🪙 Pair: {symbol}\n"
                        f"💰 24H Volume: ${volume:,.0f}\n"
                        f"📊 Threshold: ${BINANCE_MIN_VOLUME:,.0f}\n"
                        f"⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC\n\n"
                        f"Volume just crossed your trigger level."
                    )
                    await send_telegram(message)

                # Reset when drops below
                elif not meets_threshold and alerted == 1:
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

    headers = {
        "X-CMC_PRO_API_KEY": CMC_API_KEY
    }

    params = {
        "start": "1",
        "limit": "200",
        "sort": "date_added",
        "sort_dir": "desc",
        "convert": "USD"
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(CMC_LISTINGS, headers=headers, params=params) as resp:
                if resp.status != 200:
                    print("CMC API error:", resp.status)
                    return
                data = await resp.json()

        for coin in data.get("data", []):
            coin_id = coin["id"]
            marketcap = coin["quote"]["USD"].get("market_cap", 0)
            volume = coin["quote"]["USD"].get("volume_24h", 0)

            meets_threshold = (
                marketcap and marketcap >= CMC_MIN_MARKETCAP and
                volume and volume >= CMC_MIN_VOLUME
            )

            cursor.execute("SELECT alerted FROM cmc_listings WHERE id=?", (coin_id,))
            row = cursor.fetchone()

            if not row:
                alerted_value = 1 if (meets_threshold and not first_run) else 0
                cursor.execute(
                    "INSERT INTO cmc_listings (id, alerted) VALUES (?, ?)",
                    (coin_id, alerted_value)
                )
                conn.commit()

                if meets_threshold and not first_run:
                    message = (
                        f"🚨 CMC POTENTIAL 10X\n\n"
                        f"🪙 Name/Symbol: {coin['name']} / {coin['symbol']}\n"
                        f"💰 24H Volume: ${volume:,.0f}\n"
                        f"📊 Market Cap Threshold: ${CMC_MIN_MARKETCAP:,.0f}\n"
                        f"⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC\n\n"
                        f"Coin just crossed your threshold."
                    )
                    await send_telegram(message)

            else:
                alerted = row[0]

                # Fresh breakout
                if meets_threshold and alerted == 0:
                    cursor.execute(
                        "UPDATE cmc_listings SET alerted=1 WHERE id=?",
                        (coin_id,)
                    )
                    conn.commit()

                    message = (
                        f"🚨 CMC POTENTIAL 10X\n\n"
                        f"🪙 Name/Symbol: {coin['name']} / {coin['symbol']}\n"
                        f"💰 24H Volume: ${volume:,.0f}\n"
                        f"📊 Market Cap Threshold: ${CMC_MIN_MARKETCAP:,.0f}\n"
                        f"⏰ Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC\n\n"
                        f"Coin just crossed your threshold."
                    )
                    await send_telegram(message)

                # Reset when drops below
                elif not meets_threshold and alerted == 1:
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
