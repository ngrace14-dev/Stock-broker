import math
from decimal import Decimal, ROUND_DOWN

def floor_cent(val):
    return Decimal(str(val)).quantize(Decimal('0.01'), rounding=ROUND_DOWN)

class LedgerValidator:
    def __init__(self, alpaca_settled, alpaca_unsettled):
        self.alpaca_settled = Decimal(str(alpaca_settled))
        self.alpaca_unsettled = Decimal(str(alpaca_unsettled))
        self.bank_savings_ready = Decimal('0.00')
        self.ytd_tax_escrow = Decimal('0.00')
        self.crystallized_irs = Decimal('0.00')
        self.cold_storage_total = Decimal('0.00')
        self.drip_buckets_total = Decimal('0.00')
        self.uninvested_insurance_cash = Decimal('0.00')
        self.active_capital_sum = Decimal('0.00')
        self.open_positions_basis_sum = Decimal('0.00')
        self.aristocrat_basis_sum = Decimal('0.00')

    def compute_locked_wallets(self):
        return (
            self.bank_savings_ready +
            self.ytd_tax_escrow +
            self.crystallized_irs +
            self.cold_storage_total +
            self.drip_buckets_total +
            self.uninvested_insurance_cash
        )

    def reconcile_morning_cash(self):
        # Total broker wealth = Locked Wallets + Uninvested Active Capital + Open Position Basis + Aristocrat Basis
        locked = self.compute_locked_wallets()
        total_virtual_wealth = locked + self.active_capital_sum + self.open_positions_basis_sum + self.aristocrat_basis_sum
        alpaca_total = self.alpaca_settled + self.alpaca_unsettled
        discrepancy = total_virtual_wealth - alpaca_total
        return total_virtual_wealth, alpaca_total, discrepancy

def test_v37_invariants():
    post_toll_pool = Decimal('105.01')
    next_active = floor_cent(post_toll_pool / Decimal('2'))
    next_cold = post_toll_pool - next_active
    assert next_active == Decimal('52.50')
    assert next_cold == Decimal('52.51')
    assert next_active + next_cold == post_toll_pool

    drip_bucket = Decimal('15.00')
    drip_bucket += Decimal('20.00')
    drip_bucket -= Decimal('20.00')
    assert drip_bucket == Decimal('15.00')

    ledger = LedgerValidator('1000.00', '0.00')
    ledger.bank_savings_ready = Decimal('100.00')
    ledger.active_capital_sum = Decimal('800.00')
    ledger.open_positions_basis_sum = Decimal('100.00')
    virtual_total, alpaca_total, discrepancy = ledger.reconcile_morning_cash()
    assert discrepancy == Decimal('0.00'), f'Discrepancy: {discrepancy}'
    print('v3.7 Ledger Invariants & Deltas Verified Successfully!')

if __name__ == '__main__':
    test_v37_invariants()
