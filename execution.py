import logging
import asyncio

class ExecutionEngine:
    def __init__(self):
        self.logger = logging.getLogger('ExecutionEngine')
        self.state = 'IDLE'

    async def execute_exit(self, symbol, reason):
        self.logger.info(f'Execution: Triggering {reason} for {symbol}')
        # Two-Phase Async Flatten
        self.state = 'FLATTEN_CANCELING'
        self.logger.info('Phase 1: Canceling OCO legs...')
        await asyncio.sleep(0.5) 
        
        self.state = 'FLATTEN_SUBMITTING'
        self.logger.info('Phase 2: Market sweep liquidation...')
        # Implementation of DELETE /v2/positions/{symbol} would trigger here
        self.state = 'IDLE'

    async def catastrophic_escape_valve(self, symbol, price_breach):
        self.logger.critical(f'CATASTROPHIC ESCAPE: {symbol} breached {price_breach}%!')
        await self.execute_exit(symbol, 'ESCAPE_VALVE')
