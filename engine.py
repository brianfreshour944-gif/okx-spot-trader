"""
FILE: engine.py
FUNCTION: Contains the core Reactive Grid logic and async tracking monitors.
"""
import asyncio
import logging
from datetime import datetime
import database as db
from exchange import OKXExchangeManager

logger = logging.getLogger("ReactiveGridBot.Engine")

class ReactiveGridEngine:
    def __init__(self, bot_name, symbol, grid_levels, base_order_size, min_price, max_price, recenter_threshold):
        self.bot_name = bot_name
        self.symbol = symbol
        self.grid_levels = grid_levels
        self.base_order_size = base_order_size
        self.min_price = min_price
        self.max_price = max_price
        self.recenter_threshold = recenter_threshold
        
        # Initialize our exchange layer connection
        self.ex = OKXExchangeManager(symbol=self.symbol)
        
        self.active_orders = {}
        self.running = True
        self.last_grid_center = None
        self.last_redeploy_time = 0
        self.last_order_check_time = 0
        self.min_check_interval = 5

    async def cancel_all_orders(self):
        """Iterates through and clears out active working orders."""
        logger.info("Sweeping and clearing remaining active limits...")
        for oid in list(self.active_orders.keys()):
            success = await self.ex.cancel_single_order(oid)
            if success:
                del self.active_orders[oid]

    async def deploy_grid(self, mid_price: float):
        """Calculates, limits, and spaces execution grid orders safely."""
        now = datetime.now().timestamp()
        self.last_grid_center = mid_price
        self.last_redeploy_time = now
        
        spacing = 0.012  # Strategy variance tracking parameter
        amount = self.base_order_size / mid_price

        logger.info(f"🔄 Deploying New Grid Layout | Center: ${mid_price:.5f} | Size: {amount:.4f}")
        await self.cancel_all_orders()

        for i in range(1, self.grid_levels + 1):
            buy_price  = mid_price * (1 - i * spacing)
            sell_price = mid_price * (1 + i * spacing)
            
            # Bound tracking orders inside user max/min price rules
            if self.min_price <= buy_price <= self.max_price:
                order = await self.ex.place_limit_order('buy', buy_price, amount)
                if order:
                    self.active_orders[order['id']] = {'side': 'buy', 'price': buy_price, 'amount': amount}
                    
            if self.min_price <= sell_price <= self.max_price:
                order = await self.ex.place_limit_order('sell', sell_price, amount)
                if order:
                    self.active_orders[order['id']] = {'side': 'sell', 'price': sell_price, 'amount': amount}

    async def monitor_orders_loop(self):
        """Monitors order fill states continuously."""
        while self.running:
            try:
                now = datetime.now().timestamp()
                if (now - self.last_order_check_time) < self.min_check_interval:
                    await asyncio.sleep(1)
                    continue
                
                self.last_order_check_time = now
                for oid in list(self.active_orders.keys()):
                    order = await self.ex.fetch_order_status(oid)
                    if order and order.get('status') == 'closed':
                        logger.info(f"🎯 Order filled on exchange: {oid}")
                        
                        # Gather fill data parameters safely
                        side = order.get('side')
                        price = float(order.get('price', 0))
                        qty = float(order.get('filled', 0))
                        val = qty * price
                        fee = float(order.get('fee', {}).get('cost', 0) if order.get('fee') else 0)
                        
                        # Send transaction data right to database records
                        db.log_trade(self.bot_name, self.symbol, side, price, qty, val, fee, oid)
                        del self.active_orders[oid]
                        
            except Exception as e:
                logger.error(f"Error encountered during background order loop monitoring: {e}")
            await asyncio.sleep(self.min_check_interval)

    async def chase_monitor_loop(self):
        """Tracks underlying price action variance to manage grid displacement."""
        while self.running:
            try:
                ticker = await self.ex.fetch_ticker()
                current = float(ticker['last'])
                if self.last_grid_center:
                    move_pct = abs(current - self.last_grid_center) / self.last_grid_center
                    if move_pct > self.recenter_threshold:
                        logger.info(f"Price shifted {move_pct:.2%} outside boundaries. Re-centering grid.")
                        await self.deploy_grid(current)
            except Exception as e:
                logger.error(f"Error encountered in chase boundary tracker: {e}")
            await asyncio.sleep(60)
