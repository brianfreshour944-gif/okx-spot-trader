import asyncio
import ccxt.pro as ccxt
import os
import logging
from sqlalchemy import create_engine, text
from datetime import datetime

# ====================== CONFIGURATION ======================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GridBot")

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "").replace("postgresql+psycopg2://", "postgresql://")
engine = create_engine(DATABASE_URL)

# Bot Settings
BOT_NAME = "okx_grid_bot"
SYMBOL = "DOGE/USDT"
GRID_LEVELS = 5
GRID_SPACING = 0.01       # 1%
BASE_ORDER_SIZE = 100     # USDT per order
MIN_PRICE = 0.08
MAX_PRICE = 0.12
POST_ONLY = True

# Stop & Profit Settings
STOP_LOSS_AMOUNT = -50    # USD – if net P&L <= this, stop everything
TAKE_PROFIT_AMOUNT = 100  # USD – if net P&L >= this, stop and take profit
MAX_DRAWDOWN_PCT = 15     # % – if equity drops by this from peak, stop
CHECK_INTERVAL = 5        # seconds between safety checks

# ====================== DATABASE HELPERS ======================
def log_trade(bot_name, exchange, symbol, side, price, quantity, value, fee, order_id):
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO trades (bot_name, exchange, symbol, side, price, quantity, value, fee, order_id, timestamp)
            VALUES (:bot, :ex, :sym, :side, :price, :qty, :val, :fee, :oid, NOW())
        """), {"bot": bot_name, "ex": exchange, "sym": symbol, "side": side, "price": price,
               "qty": quantity, "val": value, "fee": fee, "oid": order_id})
        conn.commit()

def update_daily_loss(amount):
    with engine.connect() as conn:
        conn.execute(text("UPDATE bot_status SET daily_loss = daily_loss + :amt WHERE bot_name = :name"),
                     {"amt": amount, "name": BOT_NAME})
        conn.commit()

def get_bot_status():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT status, daily_loss, daily_loss_limit FROM bot_status WHERE bot_name = :name"),
                              {"name": BOT_NAME})
        row = result.fetchone()
        if row:
            return {"status": row[0], "daily_loss": row[1] or 0, "daily_loss_limit": row[2] or 100}
        else:
            # Auto-register if missing
            conn.execute(text("""
                INSERT INTO bot_status (bot_name, status, daily_loss, daily_loss_limit, config)
                VALUES (:name, 'STOP', 0, 100, '{}')
            """), {"name": BOT_NAME})
            conn.commit()
            return {"status": "STOP", "daily_loss": 0, "daily_loss_limit": 100}

def log_error(msg):
    with engine.connect() as conn:
        conn.execute(text("INSERT INTO bot_errors (bot_name, error_message, timestamp) VALUES (:name, :msg, NOW())"),
                     {"name": BOT_NAME, "msg": msg})
        conn.commit()

# ====================== GRID BOT ======================
class GridBot:
    self.exchange = ccxt.okx({
    'apiKey': os.getenv('OKX_API_KEY'),
    'secret': os.getenv('OKX_API_SECRET'),
    'password': os.getenv('OKX_PASSPHRASE'),
    'enableRateLimit': True,
    'options': {
        'defaultType': 'spot'
    }
})

# This enables the sandbox environment
self.exchange.set_sandbox_mode(True) 

        self.active_orders = {}
        self.running = True
        self.net_pnl = 0.0
        self.peak_equity = None

    # ---------- Order Management ----------
    async def place_order(self, side, price, amount):
        try:
            params = {'postOnly': True} if POST_ONLY else {}
            order = await self.exchange.create_order(SYMBOL, 'limit', side, amount, price, params)
            self.active_orders[order['id']] = {'side': side, 'price': price, 'amount': amount}
            logger.info(f"Placed {side} {amount:.2f} @ {price:.6f}")
            return order
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            log_error(f"place_order failed: {e}")
            return None

    async def cancel_all_orders(self):
        for oid in list(self.active_orders.keys()):
            try:
                await self.exchange.cancel_order(oid, SYMBOL)
                logger.info(f"Cancelled order {oid}")
            except Exception as e:
                logger.warning(f"Could not cancel {oid}: {e}")
        self.active_orders.clear()

    # ---------- WebSocket Listener ----------
    async def watch_orders(self):
        while self.running:
            try:
                orders = await self.exchange.watch_orders(SYMBOL)
                for order in orders:
                    if order['id'] in self.active_orders and order['status'] == 'closed':
                        filled_price = float(order['average'])
                        amount = float(order['filled'])
                        side = order['side']
                        fee = float(order['fee']['cost']) if order['fee'] else 0.0
                        value = amount * filled_price

                        # Update net P&L (buy = -value - fee, sell = +value - fee)
                        if side == 'buy':
                            trade_pnl = -value - fee
                        else:
                            trade_pnl = value - fee
                        self.net_pnl += trade_pnl
                        update_daily_loss(trade_pnl)

                        # Log trade
                        log_trade(BOT_NAME, 'OKX', SYMBOL, side, filled_price, amount, value, fee, order['id'])
                        logger.info(f"Filled {side} {amount:.2f} @ {filled_price:.6f} | trade P&L: {trade_pnl:.2f} | total: {self.net_pnl:.2f}")

                        # Remove from active orders
                        del self.active_orders[order['id']]

                        # Replenish opposite order (with boundary check)
                        await self.place_opposite_order(side, filled_price, amount)
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                log_error(f"watch_orders error: {e}")
                await asyncio.sleep(5)

    async def place_opposite_order(self, filled_side, price, amount):
        new_price = price * (1 + GRID_SPACING) if filled_side == 'buy' else price * (1 - GRID_SPACING)
        if MIN_PRICE <= new_price <= MAX_PRICE:
            new_side = 'sell' if filled_side == 'buy' else 'buy'
            await self.place_order(new_side, new_price, amount)
        else:
            logger.warning(f"Boundary reached: {new_price:.6f} outside [{MIN_PRICE}, {MAX_PRICE}] – order not placed")

    # ---------- Safety Monitor (Stops) ----------
    async def safety_monitor(self):
        """Periodically check stop-loss, take-profit, drawdown, and dashboard status."""
        while self.running:
            await asyncio.sleep(CHECK_INTERVAL)

            # 1. Check dashboard status (RUNNING/STOP)
            status = get_bot_status()
            if status['status'] != 'RUNNING':
                logger.info("Bot stopped by dashboard command")
                self.running = False
                break

            # 2. Daily loss limit
            daily_loss = status['daily_loss']
            daily_limit = status['daily_loss_limit']
            if daily_loss <= -daily_limit:
                logger.warning(f"Daily loss limit reached: {daily_loss:.2f} <= -{daily_limit}")
                log_error(f"Stopped due to daily loss limit: {daily_loss:.2f}")
                self.running = False
                break

            # 3. Global stop-loss (based on net P&L)
            if self.net_pnl <= STOP_LOSS_AMOUNT:
                logger.warning(f"Stop-loss triggered: net P&L = {self.net_pnl:.2f} <= {STOP_LOSS_AMOUNT}")
                log_error(f"Stop-loss triggered at {self.net_pnl:.2f}")
                self.running = False
                break

            # 4. Take-profit
            if self.net_pnl >= TAKE_PROFIT_AMOUNT:
                logger.info(f"Take-profit reached: {self.net_pnl:.2f} >= {TAKE_PROFIT_AMOUNT}. Stopping bot.")
                log_error(f"Take-profit target hit: {self.net_pnl:.2f}")
                self.running = False
                break

            # 5. Drawdown protection (requires current equity = cash + unrealized)
            try:
                balance = await self.exchange.fetch_balance()
                usdt_balance = balance['USDT']['free'] if 'USDT' in balance else 0
                ticker = await self.exchange.fetch_ticker(SYMBOL)
                current_price = ticker['last']
                base_currency = SYMBOL.split('/')[0]
                base_balance = balance[base_currency]['free'] if base_currency in balance else 0
                base_value = base_balance * current_price
                equity = usdt_balance + base_value

                if self.peak_equity is None:
                    self.peak_equity = equity
                else:
                    self.peak_equity = max(self.peak_equity, equity)
                    drawdown_pct = (self.peak_equity - equity) / self.peak_equity * 100
                    if drawdown_pct >= MAX_DRAWDOWN_PCT:
                        logger.warning(f"Max drawdown reached: {drawdown_pct:.2f}% (equity: {equity:.2f}, peak: {self.peak_equity:.2f})")
                        log_error(f"Stopped due to {drawdown_pct:.2f}% drawdown")
                        self.running = False
                        break
            except Exception as e:
                logger.warning(f"Drawdown check failed: {e}")

    # ---------- Initial Grid Deployment ----------
    async def deploy_initial_grid(self):
        ticker = await self.exchange.fetch_ticker(SYMBOL)
        mid = ticker['last']
        logger.info(f"Initial price: {mid:.6f}")
        for i in range(1, GRID_LEVELS + 1):
            buy_price = mid * (1 - i * GRID_SPACING)
            sell_price = mid * (1 + i * GRID_SPACING)
            amount = BASE_ORDER_SIZE / mid
            if MIN_PRICE <= buy_price <= MAX_PRICE:
                await self.place_order('buy', buy_price, amount)
            if MIN_PRICE <= sell_price <= MAX_PRICE:
                await self.place_order('sell', sell_price, amount)

    # ---------- Main Run ----------
    async def run(self):
        await self.exchange.load_markets()
        logger.info(f"Bot started: {BOT_NAME} on {SYMBOL}")

        status = get_bot_status()
        if status['status'] != 'RUNNING':
            logger.info("Bot is STOPPED in database. Exiting.")
            return

        await self.deploy_initial_grid()

        await asyncio.gather(
            self.watch_orders(),
            self.safety_monitor()
        )

        logger.info("Stopping bot – cancelling all orders...")
        await self.cancel_all_orders()
        logger.info("Bot exited cleanly.")

if __name__ == "__main__":
    bot = GridBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logger.info("Received interrupt – shutting down...")
        asyncio.run(bot.cancel_all_orders())
