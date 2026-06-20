"""
FILE: main.py
FUNCTION: Core entry point application controller script.
"""
import asyncio
import logging
from datetime import datetime
import database as db
from engine import ReactiveGridEngine

# Initialize professional log formats
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("ReactiveGridBot.Main")

# ====================== PARAMETER DEFINITIONS ======================
BOT_NAME               = "okx_grid_bot"
SYMBOL                 = "DOGE/USDT"
GRID_LEVELS            = 6
BASE_ORDER_SIZE_USDT   = 80
MIN_PRICE              = 0.07
MAX_PRICE              = 0.14
RECENTER_THRESHOLD_PCT = 0.015

SESSION_START_TIME = datetime.utcnow()

async def main():
    logger.info("Initializing system node protocols...")
    
    # 1. Initialize relational data schemas safely
    db.init_db(BOT_NAME, SESSION_START_TIME)
    
    # 2. Check current session status safety criteria before execution
    daily_loss = db.get_daily_loss_today(BOT_NAME)
    logger.info(f"Session accounting check complete | Live Session P&L: ${daily_loss:+.2f}")
    
    # 3. Instantiate core grid engine instance
    bot = ReactiveGridEngine(
        bot_name=BOT_NAME,
        symbol=SYMBOL,
        grid_levels=GRID_LEVELS,
        base_order_size=BASE_ORDER_SIZE_USDT,
        min_price=MIN_PRICE,
        max_price=MAX_PRICE,
        recenter_threshold=RECENTER_THRESHOLD_PCT
    )
    
    # 4. Fetch initial index parameters to establish starting grid lines
    ticker = await bot.ex.fetch_ticker()
    initial_price = float(ticker['last'])
    await bot.deploy_grid(initial_price)
    
    # 5. Launch simultaneous background tasks processing loops cleanly
    logger.info("Firing up processing monitors...")
    await asyncio.gather(
        bot.monitor_orders_loop(),
        bot.chase_monitor_loop(),
        return_exceptions=True
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Manual termination signal detected. Shutting down system engines safely.")
