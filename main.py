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
BOT_NAME               = "Static-Repo-okx-bot"
SYMBOL                 = "DOGE/USDT"
GRID_LEVELS            = 6
BASE_ORDER_SIZE_USDT   = 80
MIN_PRICE              = 0.07
MAX_PRICE              = 0.14
RECENTER_THRESHOLD_PCT = 0.015

SESSION_START_TIME = datetime.utcnow()

MAX_DRAWDOWN_PCT = 10.0  # Halt trading if equity drops this much from session start

# ====================== NEW HEARTBEAT FUNCTION ======================
async def heartbeat_loop(bot):
    """Periodically update the bot's status in the database so the dashboard
    sees it as ALIVE, report real account equity for the dashboard's equity
    tracking, and halt trading if a drawdown safety threshold is breached.
    Runs independently of the grid logic -- doesn't touch order placement."""
    while True:
        try:
            db.update_status(BOT_NAME, 'RUNNING' if bot.running else f'HALTED: drawdown')

            equity = await bot.ex.get_total_equity_usdt()
            if equity > 0:
                db.report_equity(BOT_NAME, equity)

            if bot.running:
                halted, drawdown_pct = db.check_drawdown_halted(BOT_NAME, MAX_DRAWDOWN_PCT)
                if halted:
                    logger.error(f"🚨 MAX DRAWDOWN HIT ({drawdown_pct:.2f}%) — halting trading. "
                                f"Existing orders are left as-is; restart the bot once you've "
                                f"reviewed the market to resume.")
                    bot.running = False
                    db.update_status(BOT_NAME, f'HALTED: drawdown {drawdown_pct:.2f}%')
        except Exception as e:
            logger.error(f"Heartbeat update failed: {e}")
        await asyncio.sleep(10)  # Update every 10 seconds

# ====================== MAIN ======================
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

    # 4b. Report starting equity to the dashboard (only sets it once --
    # safe to call on every restart without overwriting an existing value)
    try:
        starting_equity = await bot.ex.get_total_equity_usdt()
        if starting_equity > 0:
            db.report_equity(BOT_NAME, starting_equity)
            logger.info(f"💰 Starting equity reported: {starting_equity:.2f} USDT "
                       f"(OKX sandbox mode -- not real funds)")
    except Exception as e:
        logger.warning(f"⚠️ Could not report starting equity: {e}")

    # 5. Launch simultaneous background tasks processing loops cleanly
    logger.info("Firing up processing monitors...")
    await asyncio.gather(
        bot.monitor_orders_loop(),
        bot.chase_monitor_loop(),
        heartbeat_loop(bot),          # <-- THIS IS THE NEW ADDITION
        return_exceptions=True
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Manual termination signal detected. Shutting down system engines safely.")
