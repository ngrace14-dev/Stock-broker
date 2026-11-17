import logging
from datetime import datetime, time

class VelocityBetaStrategy:
    def __init__(self):
        self.logger = logging.getLogger('StrategyBrain')

    def is_rth(self, now=None):
        return True # Mock for testing

    def evaluate_signal(self, data_buffer):
        return 'BUY'
