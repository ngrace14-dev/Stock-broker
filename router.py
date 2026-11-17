import logging
from decimal import Decimal

class MasterRouter:
    def __init__(self, db_conn):
        self.db = db_conn
        self.logger = logging.getLogger('MasterRouter')

    def check_firewall(self, node_id, ticker, intent_notional, total_equity):
        self.logger.info(f'Router: Checking firewall for Node {node_id} on {ticker}')
        
        # Correct Decimal conversion
        if Decimal(str(intent_notional)) > (Decimal(str(total_equity)) * Decimal('0.30')):
            self.logger.warning('Router: Systemic Exposure Cap triggered.')
            return False
            
        return True

    def sync_settlement_budgets(self):
        self.logger.info('Router: Running 9:29:30 AM Settlement Sync.')
        pass
