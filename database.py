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
if DATABASE_URL and "psycopg2" in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgresql+psycopg2://", "postgresql://")
if not DATABASE_URL:
    DATABASE_URL = "sqlite:///grid_bot.db"

# Global database engine
engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)

def init_db(bot_name, session_start_time):
    """Initializes standard logging tables safely."""
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
