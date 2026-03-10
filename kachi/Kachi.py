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
TELEGRAM_CHAT_IDS = ["7198809557", "6065933220"]

GATEIO_TICKERS = "https://api.gateio.ws/api/v4/spot/tickers"
BITGET_TICKERS = "https://api.bitget.com/api/v2/spot/market/tickers"
CMC_LISTINGS = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

METALS_API = "https://api.metals.live/v1/spot"

GATEIO_MIN_VOLUME = 5_000_000
CMC_MIN_MARKETCAP = 10_000_000
CMC_MIN_VOLUME = 1_000_000

PRICE_SPIKE_PERCENT = 5
VOLUME_SPIKE_PERCENT = 20

GOLD_SPIKE_PERCENT = 1
SILVER_SPIKE_PERCENT = 2

CHECK_INTERVAL = 60
COOLDOWN = 60 * 60

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
last_alert INTEGER DEFAULT 0
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS bitget_listings (
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

cursor.execute("""
CREATE TABLE IF NOT EXISTS metals_prices (
symbol TEXT PRIMARY KEY,
alerted INTEGER DEFAULT 0,
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
            for chat_id in TELEGRAM_CHAT_IDS:
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                payload = {"chat_id": chat_id, "text": message}

                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        print(f"Telegram error for {chat_id}:", resp.status)

    except Exception as e:
        print("Telegram send failed:", e)

# =========================
# METALS SCANNER
# =========================

async def scan_metals(first_run=False):
    print("Scanning metals...")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(METALS_API) as resp:
                if resp.status != 200:
                    print("Metals API error:", resp.status)
                    return

                data = await resp.json()

        prices = {}
        for item in data:
            prices.update(item)

        metals = {
            "GOLD": prices.get("gold"),
            "SILVER": prices.get("silver")
        }

        for symbol, price in metals.items():

            if price is None:
                continue

            spike_threshold = GOLD_SPIKE_PERCENT if symbol == "GOLD" else SILVER_SPIKE_PERCENT

            cursor.execute(
                "SELECT alerted, baseline_price, last_alert FROM metals_prices WHERE symbol=?",
                (symbol,)
            )
            row = cursor.fetchone()

            now = int(time.time())

            if not row:
                cursor.execute(
                    "INSERT INTO metals_prices (symbol, alerted, baseline_price, last_alert) VALUES (?, ?, ?, ?)",
                    (symbol, 0, price, 0)
                )
                conn.commit()
                continue

            alerted, baseline_price, last_alert = row

            if last_alert and (now - last_alert) < COOLDOWN:
                continue

            price_growth = ((price - baseline_price) / baseline_price * 100) if baseline_price > 0 else 0

            if price_growth >= spike_threshold and alerted == 0:

                cursor.execute(
                    "UPDATE metals_prices SET alerted=1, baseline_price=?, last_alert=? WHERE symbol=?",
                    (price, now, symbol)
                )
                conn.commit()

                message = (
                    f"🚨 {symbol} ALERT\n\n"
                    f"Price: ${price:,.2f}\n"
                    f"Price Change: {price_growth:+.2f}%\n\n"
                    f"Entry Signal: Consider Long\n\n"
                    f"========================\n"
                    f"powered by @ZeusisHIM"
                )

                await send_telegram(message)

            elif price_growth < spike_threshold and alerted == 1:
                cursor.execute(
                    "UPDATE metals_prices SET alerted=0 WHERE symbol=?",
                    (symbol,)
                )
                conn.commit()

    except Exception:
        print("Metals scan error:")
        traceback.print_exc()

# =========================
# (Gate.io, Bitget, CMC scanners unchanged)
# =========================

# --- your existing scanners stay exactly the same ---
# (scan_gateio, scan_bitget, scan_cmc)
# I am not repeating them here since they are unchanged.

# =========================
# MAIN LOOP
# =========================

async def main():
    print("Starting Kachi...")

    await send_telegram("Sniping 🍁")

    print("Initializing database silently...")
    await scan_gateio(first_run=True)
    await scan_bitget(first_run=True)
    await scan_cmc(first_run=True)
    await scan_metals(first_run=True)

    print("Bot running...")

    while True:
        await scan_gateio()
        await scan_bitget()
        await scan_cmc()
        await scan_metals()

        print("Sleeping 60 seconds...\n")
        await asyncio.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
