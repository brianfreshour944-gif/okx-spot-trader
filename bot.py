import os
import time
import pandas as pd
import sys
import ccxt

class OKXDynamicGridBot:
    def __init__(self):
        print("--- RUNTIME DIAGNOSTIC CHECK ---")
        print(f"OKX_API_KEY Found: {bool(os.getenv('OKX_API_KEY'))}")
        print(f"OKX_API_SECRET Found: {bool(os.getenv('OKX_API_SECRET'))}")
        print(f"OKX_PASSPHRASE Found: {bool(os.getenv('OKX_PASSPHRASE'))}")
        print("--------------------------------")

        # SECURE API CONFIGURATION WITH US DOMAIN OVERRIDE
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
        
        # Enforce the Demo Trading environment cleanly via CCXT native method
        self.exchange.set_sandbox_mode(True)
        
        self.symbol = 'DOGE/USDT'
        
        # SELF-CONTAINED INTERNAL BUDGET LEDGER
        self.total_bot_budget = 100.0  # Hard budget ceiling limit
        self.number_of_grids = 2       # 1 Buy line + 1 Sell line
        self.capital_per_grid = self.total_bot_budget / self.number_of_grids  # Exactly $50.00
        
        # Internal wallet states (Forces bot to ignore the millions in demo account)
        self.bot_cash = 100.0          # Initial cash investment bank
        self.bot_doge = 0.0            # Initial token inventory balance
        
        # Grid parameter distance from moving average anchor line
        self.grid_percentage = 0.015 
        
        # Memory storage for active order tracking IDs
        self.current_buy_order = None
        self.current_sell_order = None

    def get_moving_average_center(self):
        """Fetches recent candles and calculates the 20-period SMA anchor line."""
        try:
            candles = self.exchange.fetch_ohlcv(self.symbol, timeframe='1h', limit=30)
            df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            sma = df['close'].rolling(window=20).mean().iloc[-1]
            return float(sma)
        except Exception as e:
            print(f"Error extracting price matrix data (Public Loop): {e}")
            return None

    def cancel_safe(self, order_id):
        """Helper to cancel an order safely without throwing script errors."""
        if order_id:
            try:
                self.exchange.cancel_order(order_id, self.symbol)
            except Exception:
                pass 

    def update_grid_positions(self):
        """Calculates dynamic levels and moves coordinates under strict internal budget limits."""
        center_line = self.get_moving_average_center()
        if not center_line:
            return
            
        target_buy_price = round(center_line * (1 - self.grid_percentage), 5)
        target_sell_price = round(center_line * (1 + self.grid_percentage), 5)
        
        print(f"\n[MA Anchor Center]: ${center_line:.5f}")
        print(f" -> Desired Buy Grid: ${target_buy_price:.5f}")
        print(f" -> Desired Sell Grid: ${target_sell_price:.5f}")
        
        # Print the self-contained internal ledger report metrics
        print(f" -> INTERNAL LEDGER: ${self.bot_cash:.2f} Free Cash | {self.bot_doge:.2f} Available DOGE Tokens")

        # --- DYNAMIC CHASING CLEANUP ENGINE ---
        # If the market center moved, cancel stale orders so they can be replaced at the new targets
        if self.current_buy_order:
            try:
                order = self.exchange.fetch_order(self.current_buy_order, self.symbol)
                if order['status'] == 'open' and float(order['price']) != target_buy_price:
                    print(f"🔄 Moving average shifted. Canceling old Buy at ${order['price']} to adjust to new target ${target_buy_price}")
                    self.cancel_safe(self.current_buy_order)
                    self.current_buy_order = None
                    self.bot_cash += self.capital_per_grid  # Credit the cash back to replace it
            except Exception as e:
                print(f"Error updating/checking buy grid drift: {e}")

        if self.current_sell_order:
            try:
                order = self.exchange.fetch_order(self.current_sell_order, self.symbol)
                if order['status'] == 'open' and float(order['price']) != target_sell_price:
                    print(f"🔄 Moving average shifted. Canceling old Sell at ${order['price']} to adjust to new target ${target_sell_price}")
                    self.cancel_safe(self.current_sell_order)
                    self.current_sell_order = None
            except Exception as e:
                print(f"Error updating/checking sell grid drift: {e}")
        # -------------------------------------

        # CHECK BUY ORDER FILL STATUS (If still active)
        if self.current_buy_order:
            try:
                order = self.exchange.fetch_order(self.current_buy_order, self.symbol)
                if order['status'] == 'closed':
                    # Extract the precise quantity filled from the exchange receipt
                    filled_amount = float(order['filled'])
                    self.bot_doge += filled_amount
                    print(f"💥 [FILL EVENT] Buy Order hit at ${order['price']}! Converted $50 allocation into {filled_amount} DOGE.")
                    self.current_buy_order = None
            except Exception as e:
                print(f"Error checking buy status: {e}")
                if "50119" in str(e): return

        # CHECK SELL ORDER FILL STATUS (If still active)
        if self.current_sell_order:
            try:
                order = self.exchange.fetch_order(self.current_sell_order, self.symbol)
                if order['status'] == 'closed':
                    # Calculate exact returns (Execution price * token volume filled)
                    sell_price = float(order['price'])
                    tokens_sold = float(order['filled'])
                    usd_returned = round(sell_price * tokens_sold, 4)
                    
                    # Return original capital + the accrued trade profits directly back to cash bank
                    self.bot_cash += usd_returned
                    self.bot_doge -= tokens_sold
                    
                    print(f"💥 [FILL EVENT] Sell Order hit at ${sell_price}! Returned original $50 capital + profit: Total ${usd_returned:.2f} USDT.")
                    self.current_sell_order = None
            except Exception as e:
                print(f"Error checking sell status: {e}")
                if "50119" in str(e): return

        # DEPLOYMENT WINDOW 1: Handle the Buy Side Allocation
        if not self.current_buy_order:
            # Check if our local cash ledger has enough uncommitted balance remaining
            if self.bot_cash >= self.capital_per_grid:
                dynamic_buy_amount = round(self.capital_per_grid / target_buy_price, 1)
                
                try:
                    print(f"Placing Buy Grid Line: Allocating ${self.capital_per_grid:.2f} internally to target {dynamic_buy_amount} DOGE at ${target_buy_price}")
                    order = self.exchange.create_limit_buy_order(self.symbol, dynamic_buy_amount, target_buy_price)
                    self.current_buy_order = order['id']
                    
                    # IMMEDIATELY lock up the cash internally so subsequent loop ticks cannot double-spend it
                    self.bot_cash -= self.capital_per_grid
                except Exception as e:
                    print(f"Execution Engine failed to place Buy Grid Line: {e}")
            else:
                print(f"⚠️ Buy Grid Idle: Waiting for Sell Grid to fill to free up required ${self.capital_per_grid:.2f} allocation cash.")
            
        # DEPLOYMENT WINDOW 2: Handle the Sell Side Allocation (Inventory Guardrail)
        if not self.current_sell_order:
            # Calculate the target token volume needed to capture our $50 segment block value
            dynamic_sell_amount = round(self.capital_per_grid / target_sell_price, 1)
            
            # Check if our local internal ledger balances show we own enough tokens to open the order
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
        print("Starting Dynamic Tracking Grid Bot...")
        while True:
            try:
                self.update_grid_positions()
            except Exception as e:
                print(f"Main loop exception triggered: {e}")
            
            print("Waiting 15 minutes before checking the moving average path...")
            time.sleep(900)

if __name__ == '__main__':
    bot = OKXDynamicGridBot()
    try:
        bot.start_loop()
    except KeyboardInterrupt:
        print("\nStopping bot instance cleanly.")
        sys.exit(0)
