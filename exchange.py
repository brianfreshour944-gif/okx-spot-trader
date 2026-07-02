"""
FILE: exchange.py
FUNCTION: Manages raw OKX exchange connections and direct order placements.
"""
import os
import logging
import asyncio
import ccxt

logger = logging.getLogger("ReactiveGridBot.Exchange")

class OKXExchangeManager:
    def __init__(self, symbol):
        self.symbol = symbol
        logger.info("Initializing connection parameters for OKX API...")
        
        # Instantiate CCXT connection wrapper instance
        self.exchange = ccxt.okx({
            'apiKey':     os.getenv('OKX_API_KEY'),
            'secret':     os.getenv('OKX_API_SECRET'),
            'password':   os.getenv('OKX_PASSPHRASE'),
            'enableRateLimit': True,
            'hostname':   'app.okx.com',
            'options':    {'defaultType': 'spot', 'x-simulated-trading': '1'},
        })
        self.exchange.set_sandbox_mode(True)
        
    async def _run_sync(self, func, *args, **kwargs):
        """Helper to run synchronous CCXT network tasks in an async loop."""
        return await asyncio.to_thread(func, *args, **kwargs)

    async def fetch_ticker(self):
        """Queries current market price ticks."""
        return await self._run_sync(self.exchange.fetch_ticker, self.symbol)

    async def get_balance(self, currency):
        """Retrieves currently unreserved free capital wallet limits."""
        try:
            balance = await self._run_sync(self.exchange.fetch_balance)
            return balance['free'].get(currency, 0.0)
        except Exception as e:
            logger.error(f"Failed to fetch exchange wallet balance for {currency}: {e}")
            return 0.0

    async def get_total_equity_usdt(self):
        """
        Returns total account equity in USDT terms -- cash AND the current
        market value of any held crypto (e.g. DOGE this bot is holding),
        using OKX's own totalEq figure rather than summing free balances,
        which would miss inventory value entirely.

        NOTE: this account is in OKX sandbox/demo mode (see __init__) --
        the number returned here is demo money, not real funds.
        """
        try:
            balance = await self._run_sync(self.exchange.fetch_balance)
            data = balance.get('info', {}).get('data', [])
            if data and data[0].get('totalEq'):
                return float(data[0]['totalEq'])
            # Fallback: sum eqUsd across all currency details
            total = 0.0
            for d in data:
                for detail in d.get('details', []):
                    total += float(detail.get('eqUsd', 0) or 0)
            return total
        except Exception as e:
            logger.error(f"Failed to fetch total equity: {e}")
            return 0.0

    async def place_limit_order(self, side, price, amount):
        """Places a single raw spot order utilizing strict post-only protocols."""
        try:
            params = {'postOnly': True}
            order = await self._run_sync(self.exchange.create_order, self.symbol, 'limit', side, amount, price, params)
            logger.info(f"✅ Placed {side.upper()} limit order via exchange API: {amount:.4f} @ ${price:.5f}")
            return order
        except Exception as e:
            logger.error(f"Failed creating exchange limit order footprint: {e}")
            return None

    async def fetch_order_status(self, order_id):
        """Fetches the latest tracking payload for an individual order ID."""
        try:
            return await self._run_sync(self.exchange.fetch_order, order_id, self.symbol)
        except Exception as e:
            logger.error(f"Failed gathering current state tracking for transaction token {order_id}: {e}")
            return None

    async def cancel_single_order(self, order_id):
        """Attempts to cancel a single standing order ID."""
        try:
            await self._run_sync(self.exchange.cancel_order, order_id, self.symbol)
            return True
        except Exception as e:
            logger.warning(f"Order cancellation adjustment failed for token {order_id}: {e}")
            return False
