import asyncio
import ccxt
import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("ReactiveGridBot")

# ====================== CONFIG ======================
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if DATABASE_URL and "psycopg2" in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql+psycopg2://", "postgresql://")
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///grid_bot.db"

engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)

BOT_NAME            = "okx_grid_bot"
SYMBOL              = "DOGE/USDT"
GRID_LEVELS         = 6
BASE_ORDER_SIZE_USDT = 80
MIN_PRICE           = 0.07
MAX_PRICE           = 0.14
RECENTER_THRESHOLD_PCT = 0.015
CHECK_INTERVAL      = 60
MIN_REDEPLOY_COOLDOWN  = 300
STOP_LOSS_AMOUNT    = -80
TAKE_PROFIT_AMOUNT  = 180
MAX_DAILY_LOSS_USDT = 150
MAX_DRAWDOWN_PCT    = 12

# ====================== GLOBAL STATE ======================
# Track when THIS INSTANCE started to define "today"
SESSION_START_TIME = datetime.utcnow()


# ====================== DATABASE HELPERS ======================

def init_db():
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS bot_status (
                    bot_name TEXT PRIMARY KEY,
                    status TEXT DEFAULT 'RUNNING',
                    daily_loss_limit NUMERIC DEFAULT 150,
                    session_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS trades (
                    id SERIAL PRIMARY KEY,
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
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS bot_errors (
                    id SERIAL PRIMARY KEY,
                    bot_name TEXT,
                    error_message TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.execute(text("""
                INSERT INTO bot_status (bot_name, status, session_start_time)
                VALUES (:name, 'RUNNING', :start)
                ON CONFLICT (bot_name) DO UPDATE SET 
                    status = 'RUNNING',
                    session_start_time = :start
            """), {"name": BOT_NAME, "start": SESSION_START_TIME})
            conn.commit()
        logger.info("✅ Database initialized successfully")
    except Exception as e:
        logger.error(f"DB init failed: {e}")
        raise

def log_trade(bot_name, exchange, symbol, side, price, quantity, value, fee, order_id):
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO trades
                    (bot_name, exchange, symbol, side, price, quantity,
                     value, fee, order_id, timestamp)
                VALUES (:bot, :ex, :sym, :side, :price, :qty,
                        :val, :fee, :oid, CURRENT_TIMESTAMP)
            """), {"bot": bot_name, "ex": exchange, "sym": symbol, "side": side,
                   "price": price, "qty": quantity, "val": value,
                   "fee": fee, "oid": order_id})
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to log trade: {e}")

def get_daily_loss_today() -> float:
    """
    FIXED: Calculate loss only for THIS SESSION (since bot started).
    Uses session_start_time instead of CURRENT_DATE to avoid timezone/
    date rollover issues.
    
    Returns a negative number when losing, e.g. -80.50 means $80.50 loss.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT
                    COALESCE(SUM(CASE WHEN side = 'sell' THEN  value - fee
                                     WHEN side = 'buy'  THEN -value - fee
                                     ELSE 0 END), 0)
                FROM trades
                WHERE bot_name = :name
                  AND timestamp >= (
                    SELECT session_start_time 
                    FROM bot_status 
                    WHERE bot_name = :name
                  )
            """), {"name": BOT_NAME})
            row = result.fetchone()
            return float(row[0]) if row else 0.0
    except Exception as e:
        logger.error(f"Failed to get daily loss: {e}")
        return 0.0

def get_bot_status() -> dict:
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT status, daily_loss_limit
                FROM bot_status WHERE bot_name = :name
            """), {"name": BOT_NAME})
            row = result.fetchone()
            if row:
                return {
                    "status": row[0],
                    "daily_loss_limit": float(row[1] or MAX_DAILY_LOSS_USDT),
                }
    except Exception as e:
        logger.error(f"Failed to get bot status: {e}")
    return {"status": "RUNNING", "daily_loss_limit": MAX_DAILY_LOSS_USDT}

def log_error(msg):
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO bot_errors (bot_name, error_message, timestamp)
                VALUES (:name, :msg, CURRENT_TIMESTAMP)
            """), {"name": BOT_NAME, "msg": msg})
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to log error: {e}")


# ====================== REACTIVE GRID BOT ======================

class ReactiveGridBot:
    def __init__(self):
        logger.info("=== Starting Reactive Chasing Grid Bot ===")
        self.exchange = ccxt.okx({
            'apiKey':    os.getenv('OKX_API_KEY'),
            'secret':    os.getenv('OKX_API_SECRET'),
            'password':  os.getenv('OKX_PASSPHRASE'),
            'enableRateLimit': True,
            'hostname':  'app.okx.com',
            'options':   {'defaultType': 'spot', 'x-simulated-trading': '1'},
        })
        self.exchange.set_sandbox_mode(True)
        self.exchange.load_markets()
        logger.info("✅ OKX Connected")

        self.active_orders      = {}
        self.running            = True
        self.net_pnl            = 0.0
        self.last_grid_center   = None
        self.last_redeploy_time = 0
        
        # Rate limit tracking to prevent API spam
        self.last_order_check_time = 0
        self.min_check_interval = 5  # Minimum 5 seconds between order checks

    async def _run_sync(self, func, *args, **kwargs):
        return await asyncio.to_thread(func, *args, **kwargs)

    async def fetch_ticker(self):
        return await self._run_sync(self.exchange.fetch_ticker, SYMBOL)

    async def fetch_ohlcv(self, limit=100):
        data = await self._run_sync(self.exchange.fetch_ohlcv, SYMBOL, '5m', limit=limit)
        df = pd.DataFrame(
            data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df

    async def get_balance(self, currency):
        try:
            balance = await self._run_sync(self.exchange.fetch_balance)
            return balance['free'].get(currency, 0.0)
        except Exception as e:
            logger.error(f"Failed to fetch balance: {e}")
            return 0.0

    async def is_in_downtrend(self):
        try:
            df = await self.fetch_ohlcv(120)
            sma_8h        = df['close'].rolling(96).mean().iloc[-1]
            current_price = df['close'].iloc[-1]
            downtrend     = current_price < sma_8h
            if downtrend:
                logger.info(
                    f"📉 Price {current_price:.6f} < 8h SMA {sma_8h:.6f} "
                    f"(downtrend) - continuing grid")
            return downtrend
        except Exception as e:
            logger.error(f"Error calculating trend: {e}")
            return False

    async def place_order(self, side, price, amount):
        try:
            params = {'postOnly': True}
            order = await self._run_sync(
                self.exchange.create_order,
                SYMBOL, 'limit', side, amount, price, params)
            self.active_orders[order['id']] = {
                'side': side, 'price': price, 'amount': amount}
            logger.info(f"✅ Placed {side.upper()} {amount:.4f} @ {price:.6f}")
            return order
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            log_error(str(e))
            return None

    async def cancel_all_orders(self):
        for oid in list(self.active_orders.keys()):
            try:
                await self._run_sync(self.exchange.cancel_order, oid, SYMBOL)
            except Exception as e:
                logger.warning(f"Could not cancel {oid}: {e}")
        self.active_orders.clear()

    async def deploy_grid(self, mid_price: float):
        now = datetime.now().timestamp()
        if self.last_redeploy_time and \
                (now - self.last_redeploy_time) < MIN_REDEPLOY_COOLDOWN:
            logger.info(
                f"Skipping redeploy — cooldown active "
                f"({now - self.last_redeploy_time:.0f}s < {MIN_REDEPLOY_COOLDOWN}s)")
            return

        self.last_grid_center   = mid_price
        self.last_redeploy_time = now

        df         = await self.fetch_ohlcv(80)
        atr        = (df['high'] - df['low']).rolling(14).mean().iloc[-1]
        volatility = atr / mid_price
        spacing    = max(0.004, min(0.035, volatility * 1.0))
        amount     = BASE_ORDER_SIZE_USDT / mid_price

        logger.info(
            f"🔄 Deploying Grid | Center: {mid_price:.6f} | "
            f"Spacing: {spacing:.5f} | Amount: {amount:.4f}")

        await self.cancel_all_orders()

        doge_balance = await self.get_balance('DOGE')
        if doge_balance < amount * GRID_LEVELS:
            logger.warning(
                f"Low DOGE balance: {doge_balance:.2f} "
                f"(need ~{amount * GRID_LEVELS:.2f}) — SELL orders might fail")

        for i in range(1, GRID_LEVELS + 1):
            buy_price  = mid_price * (1 - i * spacing)
            sell_price = mid_price * (1 + i * spacing)
            if MIN_PRICE <= buy_price  <= MAX_PRICE:
                await self.place_order('buy',  buy_price,  amount)
            if MIN_PRICE <= sell_price <= MAX_PRICE:
                await self.place_order('sell', sell_price, amount)

    async def monitor_orders(self):
        """
        FIXED: Respect rate limits and add exponential backoff on errors.
        Only check orders at most every 5 seconds, not every 2 seconds.
        """
        consecutive_errors = 0
        
        while self.running:
            try:
                now = datetime.now().timestamp()
                
                # Rate limit: don't check more than every 5 seconds
                time_since_last_check = now - self.last_order_check_time
                if time_since_last_check < self.min_check_interval:
                    await asyncio.sleep(self.min_check_interval - time_since_last_check)
                    continue
                
                self.last_order_check_time = now
                
                for oid in list(self.active_orders.keys()):
                    try:
                        order = await self._run_sync(self.exchange.fetch_order, oid, SYMBOL)
                        
                        if order is None:
                            logger.warning(f"Order {oid} returned None, skipping")
                            continue
                        
                        if order.get('status') == 'closed' and oid in self.active_orders:
                            # FIXED: safely extract filled_price and amount
                            filled_price = order.get('average')
                            if filled_price is None:
                                filled_price = order.get('price')
                            if filled_price is None:
                                logger.warning(f"Order {oid} has no price, skipping")
                                continue
                            
                            filled_price = float(filled_price)
                            amount = float(order.get('filled', 0) or 0)
                            
                            if amount <= 0:
                                logger.warning(f"Order {oid} has zero fill, skipping")
                                continue
                            
                            side = order['side']
                            fee = float(order.get('fee', {}).get('cost', 0) if order.get('fee') else 0)
                            value = amount * filled_price
                            
                            # Calculate realized P&L
                            realized_pnl = 0.0
                            if side == 'sell':
                                try:
                                    with engine.connect() as conn:
                                        res = conn.execute(text("""
                                            SELECT price FROM trades 
                                            WHERE bot_name = :bot AND symbol = :sym AND side = 'buy' 
                                            ORDER BY timestamp DESC LIMIT 1
                                        """), {"bot": BOT_NAME, "sym": SYMBOL})
                                        last_buy = res.fetchone()
                                        if last_buy:
                                            buy_price = float(last_buy[0])
                                            realized_pnl = (filled_price - buy_price) * amount
                                except Exception as pnl_err:
                                    logger.warning(f"Could not calculate P&L: {pnl_err}")
                            
                            # Log to DB
                            log_trade(BOT_NAME, 'OKX', SYMBOL, side, filled_price, amount, value, fee, oid)
                            
                            # Update memory P&L
                            self.net_pnl += realized_pnl
                            
                            logger.info(f"✅ Filled {side.upper()} | Realized P&L: {realized_pnl:+.2f}")
                            del self.active_orders[oid]
                            await self.place_opposite_order(side, filled_price, amount)
                            
                            # Reset error counter on success
                            consecutive_errors = 0
                    
                    except Exception as order_err:
                        logger.warning(f"Error checking order {oid}: {order_err}")
                        # Don't crash, just skip this order
                        continue
                
                await asyncio.sleep(self.min_check_interval)
                
            except Exception as e:
                consecutive_errors += 1
                backoff = min(60, 5 * (2 ** consecutive_errors))  # Exponential backoff, max 60s
                logger.error(f"Monitor error (will retry in {backoff}s): {e}")
                await asyncio.sleep(backoff)

    async def place_opposite_order(self, filled_side, price, amount):
        try:
            multiplier = (1 + 0.012) if filled_side == 'buy' else (1 - 0.012)
            new_price  = price * multiplier
            if MIN_PRICE <= new_price <= MAX_PRICE:
                new_side = 'sell' if filled_side == 'buy' else 'buy'
                if new_side == 'sell':
                    doge_balance = await self.get_balance('DOGE')
                    if doge_balance < amount:
                        logger.warning(
                            f"Insufficient DOGE for opposite SELL: "
                            f"need {amount:.4f}, have {doge_balance:.4f}")
                        return
                await self.place_order(new_side, new_price, amount)
        except Exception as e:
            logger.error(f"Error placing opposite order: {e}")

    async def safety_monitor(self):
        """
        Checks THIS SESSION's realized P&L (immune to date rollover).
        """
        while self.running:
            await asyncio.sleep(10)
            try:
                daily_loss_today = get_daily_loss_today()
                status           = get_bot_status()
                limit            = status.get('daily_loss_limit', MAX_DAILY_LOSS_USDT)

                if daily_loss_today < 0:
                    logger.info(
                        f"📊 This session's P&L: {daily_loss_today:+.2f} | "
                        f"Stop loss at: -{limit:.2f}")

                if daily_loss_today <= -limit:
                    logger.critical(
                        f"Loss limit reached "
                        f"({daily_loss_today:.2f} <= -{limit:.2f}). "
                        f"Stopping bot.")
                    self.running = False

                # Also check external STOP signal
                if status.get('status') == 'STOP':
                    logger.info("🛑 External STOP signal. Shutting down.")
                    self.running = False

            except Exception as e:
                logger.warning(f"Safety monitor error: {e}")

    async def chase_monitor(self):
        while self.running:
            try:
                ticker  = await self.fetch_ticker()
                current = ticker['last']
                if self.last_grid_center is not None:
                    move_pct = abs(current - self.last_grid_center) / self.last_grid_center
                    if move_pct > RECENTER_THRESHOLD_PCT:
                        logger.info(
                            f"Price moved {move_pct:.2%} from center "
                            f"{self.last_grid_center:.6f} → redeploying")
                        await self.deploy_grid(current)
                await asyncio.sleep(CHECK_INTERVAL)
            except Exception as e:
                logger.error(f"Chase monitor error: {e}")
                await asyncio.sleep(30)

    async def run(self):
        init_db()

        # Check THIS SESSION's loss before doing anything
        daily_loss_today = get_daily_loss_today()
        if daily_loss_today <= -MAX_DAILY_LOSS_USDT:
            logger.critical(
                f"This session's loss limit already reached "
                f"({daily_loss_today:.2f}). Not starting.")
            return

        logger.info(f"📊 This session's starting P&L: {daily_loss_today:+.2f}")
        ticker = await self.fetch_ticker()
        await self.deploy_grid(ticker['last'])
        
        # FIXED: catch exceptions in gather, don't let one task kill the bot
        try:
            await asyncio.gather(
                self.monitor_orders(),
                self.safety_monitor(),
                self.chase_monitor(),
                return_exceptions=True
            )
        except Exception as e:
            logger.critical(f"Main loop crashed: {e}")
            self.running = False


if __name__ == "__main__":
    bot = ReactiveGridBot()
    asyncio.run(bot.run())
