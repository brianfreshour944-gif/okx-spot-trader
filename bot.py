import asyncio
import ccxt
import os
import logging
import sys
from sqlalchemy import create_engine, text

# ====================== CONFIGURATION ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("GridBot")

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL and "psycopg2" in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql+psycopg2://", "postgresql://")

if not DATABASE_URL:
    DATABASE_URL = "sqlite:///grid_bot.db"
    logger.info("⚠️ No DATABASE_URL set → using local SQLite")

engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)

# Bot Settings
BOT_NAME = "okx_grid_bot"
SYMBOL = "DOGE/USDT"
GRID_LEVELS = 5
GRID_SPACING = 0.01
BASE_ORDER_SIZE = 100
MIN_PRICE = 0.08
MAX_PRICE = 0.12
POST_ONLY = True
STOP_LOSS_AMOUNT = -50
TAKE_PROFIT_AMOUNT = 100
MAX_DRAWDOWN_PCT = 15
CHECK_INTERVAL = 5

# ====================== DATABASE ======================
def init_db():
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS bot_status (
                    bot_name TEXT PRIMARY KEY,
                    status TEXT DEFAULT 'STOP',
                    daily_loss NUMERIC DEFAULT 0,
                    daily_loss_limit NUMERIC DEFAULT 100,
                    config TEXT
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_name TEXT,
                    exchange TEXT,
                    symbol TEXT,
                    side TEXT,
                    price NUMERIC,
                    quantity NUMERIC,
                    value NUMERIC,
                    fee NUMERIC,
                    order_id TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS bot_errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_name TEXT,
                    error_message TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            conn.execute(text("""
                INSERT INTO bot_status (bot_name, status)
                VALUES (:name, 'STOP')
                ON CONFLICT (bot_name) DO NOTHING;
            """), {"name": BOT_NAME})
            conn.commit()
        logger.info("✅ Database initialized")
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
        raise

# (Keep the same log_trade, update_daily_loss, get_bot_status, log_error functions from previous version)

def log_trade(...):  # ← copy from previous version
    ...

def update_daily_loss(...):
    ...

def get_bot_status(...):
    ...

def log_error(...):
    ...

# ====================== GRID BOT ======================
class GridBot:
    def __init__(self):
        logger.info("=== Starting GridBot Initialization ===")

        # Check credentials
        api_key = os.getenv('OKX_API_KEY')
        api_secret = os.getenv('OKX_API_SECRET')
        passphrase = os.getenv('OKX_PASSPHRASE')

        if not api_key or not api_secret or not passphrase:
            logger.error("❌ Missing OKX API credentials!")
            logger.error("Please set: OKX_API_KEY, OKX_API_SECRET, OKX_PASSPHRASE")
            sys.exit(1)

        logger.info(f"OKX_API_KEY loaded: {api_key[:6]}...")

        self.exchange = ccxt.okx({
            'apiKey': api_key,
            'secret': api_secret,
            'password': passphrase,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'spot',
                'headers': {'x-simulated-trading': '1'}
            }
        })

        # Test connection
        try:
            self.exchange.load_markets()
            balance = self.exchange.fetch_balance()
            usdt = balance.get('USDT', {}).get('free', 0)
            logger.info(f"✅ OKX Connection Successful | USDT: {usdt}")
        except ccxt.AuthenticationError as e:
            logger.error(f"❌ Authentication Failed: {e}")
            logger.error("→ Check your API keys and that they have trading permissions (demo account recommended)")
            raise
        except Exception as e:
            logger.error(f"❌ Exchange connection failed: {e}")
            raise

        self.active_orders = {}
        self.running = True
        self.net_pnl = 0.0
        self.peak_equity = None

    # ... (rest of the class stays exactly the same as my previous version)

    async def run(self):
        try:
            logger.info(f"🚀 Starting {BOT_NAME} on {SYMBOL}")
            init_db()

            status = get_bot_status()
            if status['status'] != 'RUNNING':
                logger.warning("Bot status is STOP in database.")
                logger.info("Update bot_status table to 'RUNNING' to start trading.")
                return

            await self.deploy_initial_grid()
            await asyncio.gather(self.monitor_orders(), self.safety_monitor())

        except Exception as e:
            logger.error(f"Critical error: {e}")
            log_error(str(e))
        finally:
            await self.cancel_all_orders()


if __name__ == "__main__":
    init_db()  # init early
    bot = GridBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Shutdown requested...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
