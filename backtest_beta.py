import math
from datetime import datetime, timedelta

class BacktestBetaNode:
    def __init__(self, tier, active_capital):
        self.tier = tier
        self.active_capital = active_capital
        self.cold_storage = active_capital  # 1:1 backed
        self.drip_bucket = 0.00
        self.position = None
        self.total_realized_pnl = 0.0

    def get_position_size(self):
        return self.active_capital * (0.80 if self.tier == 0 else 0.40)

    def execute_trade(self, price, side='buy'):
        # Modeled friction
        friction = 0.02
        if side == 'buy':
            return price + friction
        return price - friction

    def simulate_tier_0_graduation(self):
        # Graduation Logic: Hit $50 -> Parent rebate + Tier 1
        if self.tier == 0 and self.active_capital >= 50.0:
            print("Graduating Tier 0 to Tier 1")
            return True
        return False

# Verification Test
def test_backtest_math():
    node = BacktestBetaNode(tier=0, active_capital=10.0)
    assert node.get_position_size() == 8.0, f"Expected 8.0, got {node.get_position_size()}"
    
    # Simulate Trade Outcome
    fill_price = 100.0
    entry = node.execute_trade(fill_price, 'buy')
    assert entry == 100.02, "Friction model failed"
    
    print("Backtester math verification passed.")

if __name__ == "__main__":
    test_backtest_math()
