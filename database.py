"""
FILE: database.py
FUNCTION: Handles all local state tracking and trade logging tables.
"""
import os
import logging
from sqlalchemy import create_engine, text
from datetime import datetime

logger = logging.getLogger("ReactiveGridBot.Database")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

# 1. Normalize database dialect strings for SQLAlchemy 2.0+ compatibility
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
elif DATABASE_URL.startswith("postgresql+psycopg2://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql+psycopg2://", "postgresql://", 1)

# Fallback to local SQLite if no cloud database environment variable is active
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///grid_bot.db"

# Global database engine
engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)

def init_db(bot_name, session_start_time):
    """Initializes standard logging tables safely."""
    # Handle auto-increment syntax variations between Postgres (SERIAL) and SQLite (AUTOINCREMENT)
    id_column_type = "INTEGER PRIMARY KEY AUTOINCREMENT" if "sqlite" in DATABASE_URL else "SERIAL PRIMARY KEY"
    
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
            # bot_status is shared across multiple bots -- it may already
            # exist (created by another bot) without these columns, so
            # CREATE TABLE IF NOT EXISTS alone won't add them.
            conn.execute(text("ALTER TABLE bot_status ADD COLUMN IF NOT EXISTS daily_loss_limit NUMERIC DEFAULT 150"))
            conn.execute(text("ALTER TABLE bot_status ADD COLUMN IF NOT EXISTS session_start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP"))
            conn.execute(text("ALTER TABLE bot_status ADD COLUMN IF NOT EXISTS starting_equity NUMERIC"))
            conn.execute(text("ALTER TABLE bot_status ADD COLUMN IF NOT EXISTS live_equity NUMERIC"))
            conn.execute(text("ALTER TABLE bot_status ADD COLUMN IF NOT EXISTS live_equity_updated_at TIMESTAMP"))
            
            # Use the dynamically selected primary key identity token
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS trades (
                    id {id_column_type},
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
                INSERT INTO bot_status (bot_name, status, session_start_time)
                VALUES (:name, 'RUNNING', :start)
                ON CONFLICT (bot_name) DO UPDATE SET 
                    status = 'RUNNING',
                    session_start_time = :start
            """), {"name": bot_name, "start": session_start_time})
            conn.commit()
        logger.info("✅ Core local database layers initialized successfully")
    except Exception as e:
        logger.error(f"Database setup initialization failed: {e}")
        raise

def log_trade(bot_name, symbol, side, price, quantity, value, fee, order_id):
    """Saves every completed trade directly to historical records."""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO trades (bot_name, exchange, symbol, side, price, quantity, value, fee, order_id, timestamp)
                VALUES (:bot, 'OKX', :sym, :side, :price, :qty, :val, :fee, :oid, CURRENT_TIMESTAMP)
            """), {"bot": bot_name, "sym": symbol, "side": side, "price": price, "qty": quantity, "val": value, "fee": fee, "oid": order_id})
            conn.commit()
    except Exception as e:
        logger.error(f"Failed logging trade transaction to storage records: {e}")

# ... (your existing get_daily_loss_today function) ...

def get_daily_loss_today(bot_name) -> float:
    """Calculates cumulative session earnings directly from trades table."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT COALESCE(SUM(CASE WHEN side = 'sell' THEN value - fee WHEN side = 'buy' THEN -value - fee ELSE 0 END), 0)
                FROM trades
                WHERE bot_name = :name AND timestamp >= (SELECT session_start_time FROM bot_status WHERE bot_name = :name)
            """), {"name": bot_name})
            row = result.fetchone()
            return float(row[0]) if row else 0.0
    except Exception as e:
        logger.error(f"Error reading historical session P&L limits: {e}")
        return 0.0

# ==================== PASTE THE NEW FUNCTION RIGHT HERE ====================
def update_status(bot_name, status):
    """Update the bot's heartbeat (last_update) and status; insert if missing."""
    try:
        with engine.connect() as conn:
            # 1. Ensure the last_update column exists (safe migration)
            conn.execute(text("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_name='bot_status' AND column_name='last_update') 
                    THEN
                        ALTER TABLE bot_status ADD COLUMN last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                    END IF;
                END $$;
            """))
            conn.commit()

            # 2. Upsert: update if exists, else insert
            result = conn.execute(text("""
                UPDATE bot_status 
                SET status = :status, last_update = CURRENT_TIMESTAMP 
                WHERE bot_name = :name
            """), {"name": bot_name, "status": status})
            
            if result.rowcount == 0:
                # Insert new row with default values
                conn.execute(text("""
                    INSERT INTO bot_status (bot_name, status, last_update, session_start_time)
                    VALUES (:name, :status, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """), {"name": bot_name, "status": status})
            
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to update status for {bot_name}: {e}")


def report_equity(bot_name, current_equity):
    """
    Reports this bot's real account equity to the dashboard.
    starting_equity is set the first time a bot reports in and is never
    overwritten afterward. live_equity and live_equity_updated_at are
    overwritten on every call.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO bot_status (bot_name, starting_equity, live_equity, live_equity_updated_at, last_update)
                VALUES (:name, :equity, :equity, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (bot_name) DO UPDATE SET
                    live_equity = :equity,
                    live_equity_updated_at = CURRENT_TIMESTAMP,
                    last_update = CURRENT_TIMESTAMP,
                    starting_equity = COALESCE(bot_status.starting_equity, :equity)
            """), {"name": bot_name, "equity": float(current_equity)})
            conn.commit()
    except Exception as e:
        logger.error(f"report_equity failed for {bot_name}: {e}")


def check_drawdown_halted(bot_name, max_drawdown_pct=10.0):
    """
    Lightweight drawdown safety check: compares live_equity against
    starting_equity and returns True if the loss exceeds max_drawdown_pct.
    Does not halt anything itself -- main.py decides what to do with the
    result. Returns False (not halted) if either value is missing, since
    we can't compute a drawdown without both.
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT starting_equity, live_equity FROM bot_status WHERE bot_name = :name
            """), {"name": bot_name})
            row = result.fetchone()
            if not row or row[0] is None or row[1] is None:
                return False, 0.0
            starting, live = float(row[0]), float(row[1])
            if starting <= 0:
                return False, 0.0
            drawdown_pct = (live - starting) / starting * 100
            return drawdown_pct < -abs(max_drawdown_pct), drawdown_pct
    except Exception as e:
        logger.error(f"check_drawdown_halted failed for {bot_name}: {e}")
        return False, 0.0
