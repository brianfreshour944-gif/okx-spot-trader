
import asyncio
import ccxt
import os
import logging
import sys
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("TakerBot")

# ====================== CONFIG ======================
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///taker_bot.db")
engine = create_engine(DATABASE_URL, echo=False)

BOT_NAME = "okx_taker_bot"
SYMBOL = "DOGE/USDT"
TIMEFRAME = '5m'
POSITION_SIZE_USDT = 100
RSI_PERIOD = 14
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 35

# ====================== DB ======================
def init_db():
    try:
        with engine.connect() as conn:
            # Create table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS bot_status (
                    bot_name TEXT PRIMARY KEY,
                    status TEXT DEFAULT 'STOP'
                );
            """))
            
            # Insert or update status (compatible with SQLite + PostgreSQL)
            conn.execute(text("""
                INSERT INTO bot_status (bot_name, status)
                VALUES (:name, 'RUNNING')
                ON CONFLICT (bot_name) DO UPDATE SET status = 'RUNNING'
            """), {"name": BOT_NAME})
            
            conn.commit()
        logger.info("✅ Database initialized")
    except Exception as e:
        logger.error(f"Database init failed: {e}")

# ====================== BOT ======================
class TakerBot:
    def __init__(self):
        logger.info("Initializing Taker Bot...")

        # WORKING CONFIG FROM YOUR OTHER BOT
        self.exchange = ccxt.okx({
            'apiKey': os.getenv('OKX_API_KEY'),
            'secret': os.getenv('OKX_API_SECRET'),
            'password': os.getenv('OKX_PASSPHRASE'),
            'enableRateLimit': True,
            'hostname': 'app.okx.com',
            'options': {
                'defaultType': 'spot',
                'x-simulated-trading': '1'
            }
        })
        self.exchange.set_sandbox_mode(True)

        try:
            self.exchange.load_markets()
            logger.info("✅ OKX Connection Successful (Sandbox)")
        except Exception as e:
            logger.error(f"OKX Connection Failed: {e}")
            raise

        self.position = None
        self.running = True

    async def fetch_ohlcv(self, limit=150):
        ohlcv = await asyncio.to_thread(self.exchange.fetch_ohlcv, SYMBOL, TIMEFRAME, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df

    def calculate_rsi(self, series):
        delta = series.diff()
        gain = delta.where(delta > 0, 0).rolling(window=RSI_PERIOD).mean()
        loss = -delta.where(delta < 0, 0).rolling(window=RSI_PERIOD).mean()
        rs = gain / loss
        return 100 - (100 / (1 + rs))

    def generate_signal(self, df):
        df = df.copy()
        df['ema_fast'] = df['close'].ewm(span=9).mean()
        df['ema_slow'] = df['close'].ewm(span=21).mean()
        df['rsi'] = self.calculate_rsi(df['close'])

        latest = df.iloc[-1]
        prev = df.iloc[-2]

        if (latest['ema_fast'] > latest['ema_slow'] and 
            latest['rsi'] < RSI_OVERBOUGHT and 
            prev['rsi'] < latest['rsi']):
            return "BUY"

        if (latest['ema_fast'] < latest['ema_slow'] and 
            latest['rsi'] > RSI_OVERSOLD and 
            prev['rsi'] > latest['rsi']):
            return "SELL"

        return None

    async def run(self):
        init_db()
        logger.info(f"🚀 Taker Bot Started on {SYMBOL}")

        while self.running:
            try:
                df = await self.fetch_ohlcv()
                signal = self.generate_signal(df)

                if signal and not self.position:
                    ticker = await asyncio.to_thread(self.exchange.fetch_ticker, SYMBOL)
                    price = ticker['last']
                    amount = POSITION_SIZE_USDT / price

                    logger.info(f"🔍 Signal: {signal} at {price}")

                    order = await asyncio.to_thread(
                        self.exchange.create_order, SYMBOL, 'market', signal.lower(), amount
                    )
                    self.position = {'side': signal.lower(), 'entry': price}
                    logger.info(f"✅ Market {signal} executed | Order ID: {order.get('id')}")

                await asyncio.sleep(15)

            except Exception as e:
                logger.error(f"Main loop error: {e}")
                await asyncio.sleep(30)


if __name__ == "__main__":
    try:
        bot = TakerBot()
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
