import os
import time
import pandas as pd
import sys
import ccxt

class OKXDynamicGridBot:
    def __init__(self):
        print("--- RUNTIME DIAGNOSTIC CHECK (V2 25-INCREMENT PATCHED) ---")
        print(f"OKX_API_KEY Found: {bool(os.getenv('OKX_API_KEY'))}")
        print(f"OKX_API_SECRET Found: {bool(os.getenv('OKX_API_SECRET'))}")
        print(f"OKX_PASSPHRASE Found: {bool(os.getenv('OKX_PASSPHRASE'))}")
        print("---------------------------------------------------------")

        # SECURE API CONFIGURATION
        self.exchange = ccxt.okx({
            'apiKey': os.getenv('OKX_API_KEY'),
            'secret': os.getenv('OKX_API_SECRET'),
            'password': os.getenv('OKX_PASSPHRASE'),
            'enableRateLimit': True,
            'hostname': 'us.okx.com',  
            'options': {
                'defaultType': 'spot',  
            }
        })
        
        self.exchange.set_sandbox_mode(True)
        self.symbol = 'DOGE/USDT'
        
        # ADJUSTED BUDGET MANAGEMENT FOR OPTIMAL RISK
        self.total_bot_budget = 100.0  
        self.number_of_grids = 4       # 4 grids = $25 increments
        self.capital_per_grid = self.total_bot_budget / self.number_of_grids  
        
        # Internal tracking ledger balances
        self.bot_cash = 100.0          
        self.bot_doge = 0.0            
        
        self.grid_percentage = 0.015  
        
        self.current_buy_order = None
        self.current_sell_order = None

    def get_moving_average_center(self):
        """Fetches recent hourly candles and calculates the 20-period SMA anchor line."""
        try:
            candles = self.exchange.fetch_ohlcv(self.symbol, timeframe='1h', limit=30)
            df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            sma = df['close'].rolling(window=20).mean().iloc[-1]
            return float(sma)
        except Exception as e:
            print(f"Error extracting price matrix data (Public Loop): {e}")
            return None

    def cancel_safe(self, order_id):
        """Helper to drop a tracking trace block safely on the exchange core."""
        if order_id:
            try:
                self.exchange.cancel_order(order_id, self.symbol)
                return True
            except Exception:
                return False
        return False

    def sync_and_audit_fills(self):
        """Queries active order slots to accurately record filled states to the ledger."""
        # 1. AUDIT ACTIVE BUY CONTRACTS
        if self.current_buy_order:
            try:
                order = self.exchange.fetch_order(self.current_buy_order, self.symbol)
                if order['status'] == 'closed':
                    filled_amount = float(order['filled'])
                    buy_price = float(order['price'])
                    usd_spent = round(buy_price * filled_amount, 4)
                    
                    # FIXED: Deduct cash from tracking ledger when a buy fills
                    self.bot_cash -= usd_spent
                    self.bot_doge += filled_amount
                    
                    print(f"💥 [FILL EVENT] Buy Order hit at ${buy_price}! Spent ${usd_spent:.2f} to convert allocation into {filled_amount} DOGE.")
                    self.current_buy_order = None
                elif order['status'] == 'canceled':
                    self.current_buy_order = None
            except Exception as e:
                print(f"Error checking buy status: {e}")

        # 2. AUDIT ACTIVE SELL CONTRACTS
        if self.current_sell_order:
            try:
                order = self.exchange.fetch_order(self.current_sell_order, self.symbol)
                if order['status'] == 'closed':
                    sell_price = float(order['price'])
                    tokens_sold = float(order['filled'])
                    usd_returned = round(sell_price * tokens_sold, 4)
                    
                    # Add returned cash and remove sold tokens
                    self.bot_cash += usd_returned
                    self.bot_doge -= tokens_sold
                    print(f"💥 [FILL EVENT] Sell Order hit at ${sell_price}! Returned original capital + profit: Total ${usd_returned:.2f} USDT.")
                    self.current_sell_order = None
                elif order['status'] == 'canceled':
                    self.current_sell_order = None
            except Exception as e:
                print(f"Error checking sell status: {e}")

    def update_grid_positions(self):
        """Manages trailing targets and dynamically handles line adjustment overhead."""
        center_line = self.get_moving_average_center()
        if not center_line:
            return
            
        target_buy_price = round(center_line * (1 - self.grid_percentage), 5)
        target_sell_price = round(center_line * (1 + self.grid_percentage), 5)
        
        print(f"\n[MA Anchor Center]: ${center_line:.5f}")
        print(f" -> Desired Buy Grid: ${target_buy_price:.5f}")
        print(f" -> Desired Sell Grid: ${target_sell_price:.5f}")
        
        # Audit live states prior to performing tracking logic adjustments
        self.sync_and_audit_fills()
        print(f" -> INTERNAL LEDGER: ${self.bot_cash:.2f} Free Cash | {self.bot_doge:.2f} Available DOGE Tokens")

        # --- DYNAMIC CHASING CLEANUP ENGINE ---
        if self.current_buy_order:
            try:
                order = self.exchange.fetch_order(self.current_buy_order, self.symbol)
                if order['status'] == 'open' and float(order['price']) != target_buy_price:
                    print(f"🔄 Moving average shifted. Canceling old Buy at ${order['price']} to adjust to new target ${target_buy_price}")
                    if self.cancel_safe(self.current_buy_order):
                        self.current_buy_order = None
            except Exception as e:
                print(f"Error updating buy grid drift: {e}")

        if self.current_sell_order:
            try:
                order = self.exchange.fetch_order(self.current_sell_order, self.symbol)
                if order['status'] == 'open' and float(order['price']) != target_sell_price:
                    print(f"🔄 Moving average shifted. Canceling old Sell at ${order['price']} to adjust to new target ${target_sell_price}")
                    if self.cancel_safe(self.current_sell_order):
                        self.current_sell_order = None
            except Exception as e:
                print(f"Error updating sell grid drift: {e}")

        # --- DEPLOYMENT WINDOWS ---
        # 1. Buy Side Line Placement
        if not self.current_buy_order:
            # Recompute cash tracking rules accurately
            allocated_to_buy = self.capital_per_grid if self.current_buy_order else 0.0
            available_cash = self.bot_cash - allocated_to_buy
            
            if available_cash >= self.capital_per_grid:
                dynamic_buy_amount = round(self.capital_per_grid / target_buy_price, 1)
                try:
                    print(f"Placing Buy Grid Line: Allocating ${self.capital_per_grid:.2f} internally to target {dynamic_buy_amount} DOGE at ${target_buy_price}")
                    order = self.exchange.create_limit_buy_order(self.symbol, dynamic_buy_amount, target_buy_price)
                    self.current_buy_order = order['id']
                except Exception as e:
                    print(f"Execution Engine failed to place Buy Grid Line: {e}")
            else:
                print(f"⚠️ Buy Grid Idle: Waiting for Sell Grid to fill to free up required ${self.capital_per_grid:.2f} allocation cash.")
            
        # 2. Sell Side Line Placement
        if not self.current_sell_order:
            # FIXED: Base the sell amount off the current grid's allocation level ($25)
            dynamic_sell_amount = round(self.capital_per_grid / target_sell_price, 1)
            if self.bot_doge >= dynamic_sell_amount:
                try:
                    print(f"Placing Sell Grid Line: Selling {dynamic_sell_amount} DOGE at ${target_sell_price} (Target Return: ${self.capital_per_grid:.2f})")
                    order = self.exchange.create_limit_sell_order(self.symbol, dynamic_sell_amount, target_sell_price)
                    self.current_sell_order = order['id']
                except Exception as e:
                    print(f"Execution Engine failed to place Sell Grid Line: {e}")
            else:
                print(f"📌 Sell Grid Idle: Internal bot inventory has {self.bot_doge:.2f}/{dynamic_sell_amount} DOGE tokens required.")
def start_loop(self):
        print("Starting Dynamic Tracking Grid Bot (High-Frequency Loop Active)...")
        
        # Track when we last updated the moving average grid
        last_ma_update_time = 0
        ma_update_interval = 900  # 15 minutes in seconds

        while True:
            current_time = time.time()
            
            try:
                # OPTION A: Every 15 minutes, fetch candles and adjust for drift
                if current_time - last_ma_update_time >= ma_update_interval:
                    print("\n⏰ [15-MIN INTERVAL] Recalculating Moving Average Anchor and checking grid drift...")
                    self.update_grid_positions()
                    last_ma_update_time = current_time
                
                # OPTION B: Every loop cycle, check if an order filled!
                else:
                    self.sync_and_audit_fills()
                    
            except Exception as e:
                print(f"Main loop exception triggered: {e}")
            
            # Sleep for 15 seconds before checking fills again
            # OKX rate limits easily allow this (CCXT automatic rate limiter handles safety)
            time.sleep(15)

if __name__ == '__main__':
    bot = OKXDynamicGridBot()
    try:
        bot.start_loop()
    except KeyboardInterrupt:
        print("\nStopping bot instance cleanly.")
        sys.exit(0)
