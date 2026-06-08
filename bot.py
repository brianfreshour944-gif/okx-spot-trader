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
GRID_SPACING = 0.01        # 1%
BASE_ORDER_SIZE = 100      # USDT per order
MIN_PRICE = 0.08
MAX_PRICE = 0.12
POST_ONLY = True

# Stop & Profit Settings
STOP_LOSS_AMOUNT = -50    # USD
TAKE_PROFIT_AMOUNT = 100  # USD
MAX_DRAWDOWN_PCT = 15     # %
CHECK_INTERVAL = 5        # seconds

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
# ====================== GRID BOT ======================
class GridBot:
    def __init__(self):
        # Everything here is now correctly inside the __init__ method
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
        
        # Explicitly enable sandbox mode
        self.exchange.set_sandbox_mode(True)
        self.exchange.headers = {'x-simulated-trading': '1'}
        
        self.active_orders = {}
        self.running = True
        self.net_pnl = 0.0
        self.peak_equity = None

    # ---------- Order Management ----------
    # ... (Rest of your methods remain the same)

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

    # ---------- Logic Methods ----------
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
                        
                        trade_pnl = (-value - fee) if side == 'buy' else (value - fee)
                        self.net_pnl += trade_pnl
                        update_daily_loss(trade_pnl)
                        
                        log_trade(BOT_NAME, 'OKX', SYMBOL, side, filled_price, amount, value, fee, order['id'])
                        logger.info(f"Filled {side} | P&L: {trade_pnl:.2f} | Total: {self.net_pnl:.2f}")
                        
                        del self.active_orders[order['id']]
                        await self.place_opposite_order(side, filled_price, amount)
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                await asyncio.sleep(5)

    async def place_opposite_order(self, filled_side, price, amount):
        new_price = price * (1 + GRID_SPACING) if filled_side == 'buy' else price * (1 - GRID_SPACING)
        if MIN_PRICE <= new_price <= MAX_PRICE:
            await self.place_order('sell' if filled_side == 'buy' else 'buy', new_price, amount)

    async def safety_monitor(self):
        while self.running:
            await asyncio.sleep(CHECK_INTERVAL)
            status = get_bot_status()
            if status['status'] != 'RUNNING':
                self.running = False
                break
            # Add other safety checks (Drawdown/P&L) here...

    async def deploy_initial_grid(self):
        ticker = await self.exchange.fetch_ticker(SYMBOL)
        mid = ticker['last']
        for i in range(1, GRID_LEVELS + 1):
            await self.place_order('buy', mid * (1 - i * GRID_SPACING), BASE_ORDER_SIZE / mid)
            await self.place_order('sell', mid * (1 + i * GRID_SPACING), BASE_ORDER_SIZE / mid)

    async def run(self):
        try:
            await self.exchange.load_markets()
            logger.info(f"Bot started: {BOT_NAME}")
            await self.deploy_initial_grid()
            await asyncio.gather(self.watch_orders(), self.safety_monitor())
        except Exception as e:
            logger.error(f"Critical error: {e}")
        finally:
            await self.cancel_all_orders()
            await self.exchange.close()

if __name__ == "__main__":
    bot = GridBot()
    asyncio.run(bot.run())
