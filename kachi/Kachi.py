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
CMC_MAX_RANK = 1500

CHECK_INTERVAL = 300  # 5 minutes

# =========================
# DATABASE
# =========================

conn = sqlite3.connect("listings.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS binance_listings (
    symbol TEXT PRIMARY KEY
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS cmc_listings (
    id INTEGER PRIMARY KEY
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

            cursor.execute("SELECT symbol FROM binance_listings WHERE symbol=?", (symbol,))
            exists = cursor.fetchone()

            if not exists:
                cursor.execute("INSERT INTO binance_listings VALUES (?)", (symbol,))
                conn.commit()

                if first_run:
                    continue

                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{BINANCE_TICKER}?symbol={symbol}") as vol_resp:
                        if vol_resp.status != 200:
                            print("Volume fetch error:", vol_resp.status)
                            continue

                        vol_data = await vol_resp.json()

                volume = float(vol_data.get("quoteVolume", 0))

                if volume >= BINANCE_MIN_VOLUME:
                    print("New Binance listing detected:", symbol)

                    message = (
                        f"New Binance Listing\n\n"
                        f"Symbol: {symbol}\n"
                        f"24h Volume: ${volume:,.0f}\n"
                        f"Detected: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}"
                    )

                    await send_telegram(message)

    except Exception as e:
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

            cursor.execute("SELECT id FROM cmc_listings WHERE id=?", (coin_id,))
            exists = cursor.fetchone()

            if not exists:
                cursor.execute("INSERT INTO cmc_listings VALUES (?)", (coin_id,))
                conn.commit()

                if first_run:
                    continue

                marketcap = coin["quote"]["USD"].get("market_cap", 0)
                volume = coin["quote"]["USD"].get("volume_24h", 0)
                rank = coin.get("cmc_rank")

                if (
                    marketcap and marketcap >= CMC_MIN_MARKETCAP and
                    volume and volume >= CMC_MIN_VOLUME and
                    rank and rank <= CMC_MAX_RANK
                ):
                    print("New CMC listing detected:", coin["symbol"])

                    message = (
                        f"New CMC Listing\n\n"
                        f"Name: {coin['name']}\n"
                        f"Symbol: {coin['symbol']}\n"
                        f"Rank: {rank}\n"
                        f"Market Cap: ${marketcap:,.0f}\n"
                        f"24h Volume: ${volume:,.0f}\n"
                        f"Detected: {time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())}"
                    )

                    await send_telegram(message)

    except Exception as e:
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
        print("Sleeping...\n")
        await asyncio.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
