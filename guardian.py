import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from database import BrokerDatabase
from execution import ExecutionEngine
from router import MasterRouter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('GuardianWatchdog')


class GuardianProcess:
    """
    Phase 1.3 Out-of-Band Guardian Watchdog:
    Monitors the SQLite system_heartbeat table. If the main daemon/server misses 3 consecutive
    heartbeats (>30s stale), Guardian automatically triggers the Catastrophic Escape Valve
    across active tickers to flatten exposure safely.
    """

    def __init__(self, db=None, stale_threshold_seconds=30.0):
        self.db = db or BrokerDatabase()
        self.stale_threshold = float(stale_threshold_seconds)
        self.router = MasterRouter(db_conn=self.db)
        self.execution = ExecutionEngine(router=self.router, db=self.db)

    def check_heartbeat_once(self):
        now_epoch = datetime.now(timezone.utc).timestamp()
        self.db.record_heartbeat('GUARDIAN_WATCHDOG', os.getpid(), 'ARMED')
        heartbeats = {h['process_name']: h for h in self.db.get_heartbeats()}
        server_hb = heartbeats.get('WEB_WRAPPER_SERVER') or heartbeats.get('MASTER_DAEMON')

        if not server_hb:
            return {
                'ok': True,
                'guardian_state': 'ARMED_WAITING_FOR_DAEMON',
                'stale': False,
                'message': 'Guardian armed; primary daemon heartbeat registered on boot.',
            }

        age = round(now_epoch - float(server_hb['last_beat_epoch']), 2)
        is_stale = age > self.stale_threshold
        if is_stale:
            logger.critical(f'GUARDIAN ALERT: Primary process heartbeat stale by {age}s! Triggering Emergency Flatten.')
            for pos in self.execution.get_positions():
                sym = pos.get('symbol')
                if sym:
                    asyncio.run(self.execution.catastrophic_escape_valve(sym, price_breach=99.9))
            return {
                'ok': False,
                'guardian_state': 'EMERGENCY_KILL_TRIGGERED',
                'stale': True,
                'heartbeat_age_seconds': age,
                'message': f'Primary heartbeat stale ({age}s > {self.stale_threshold}s). Emergency flatten executed.',
            }

        return {
            'ok': True,
            'guardian_state': 'HEALTHY_VERIFIED',
            'stale': False,
            'heartbeat_age_seconds': age,
            'message': f'Heartbeat verified ({age}s age, threshold {self.stale_threshold}s).',
        }

    def run_forever(self, interval=10.0):
        logger.info('Starting Out-of-Band Guardian Watchdog...')
        while True:
            self.check_heartbeat_once()
            time.sleep(interval)


if __name__ == '__main__':
    GuardianProcess().run_forever()
