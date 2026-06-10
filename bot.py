import ccxt
import os
import sys

print("=== OKX API Diagnostic Test ===")

api_key = os.getenv('OKX_API_KEY')
api_secret = os.getenv('OKX_API_SECRET')
passphrase = os.getenv('OKX_PASSPHRASE')

print(f"API Key loaded:     {'Yes' if api_key else 'MISSING'}")
print(f"Secret loaded:      {'Yes' if api_secret else 'MISSING'}")
print(f"Passphrase loaded:  {'Yes' if passphrase else 'MISSING'}")

if not all([api_key, api_secret, passphrase]):
    print("❌ Missing credentials. Set them as environment variables.")
    sys.exit(1)

exchange = ccxt.okx({
    'apiKey': api_key,
    'secret': api_secret,
    'password': passphrase,
    'enableRateLimit': True,
    'options': {
        'defaultType': 'spot',
        'headers': {'x-simulated-trading': '1'}   # This is for DEMO trading
    }
})

try:
    print("Loading markets...")
    exchange.load_markets()
    print("✅ load_markets() passed")

    print("Fetching balance...")
    balance = exchange.fetch_balance()
    usdt = balance.get('USDT', {}).get('free', 0)
    print(f"✅ Balance fetched successfully! USDT: {usdt}")

    print("Fetching ticker...")
    ticker = exchange.fetch_ticker('DOGE/USDT')
    print(f"✅ Current DOGE price: {ticker['last']}")

    print("\n🎉 ALL TESTS PASSED - API is working!")

except ccxt.AuthenticationError as e:
    print(f"❌ AUTHENTICATION ERROR: {e}")
    print("→ Check: Are you using DEMO keys? Is passphrase correct?")
except Exception as e:
    print(f"❌ ERROR: {type(e).__name__}: {e}")
