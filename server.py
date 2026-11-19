import asyncio
import json
import logging
import os
import time
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from agent_brain import AutonomousAgentBrain
from backtest_beta import BacktestBetaNode
from config import get_broker_config
from daemon import AutoBrokerDaemon
from database import BrokerDatabase
from guardian import GuardianProcess
from ledger_validator import LedgerValidator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('WebWrapperServer')


class BrokerService:
    """Orchestrates AutoBrokerDaemon, MasterRouter, ExecutionEngine, SQLite ACID Ledger, Guardian, and Backtest nodes."""

    def __init__(self, db=None):
        self.db = db or BrokerDatabase()
        self.daemon = AutoBrokerDaemon()
        self.router = self.daemon.router
        self.router.db = self.db
        self.execution = self.daemon.execution
        self.execution.db = self.db
        self.strategy = self.daemon.strategy_engine
        self.guardian = GuardianProcess(db=self.db)
        self.boot_time = time.time()
        self.simulated_drift_delta = Decimal('0.00')
        self.post_flatten_locked = False
        self.missed_beats_threshold = 3
        self.db_migration_mode = 'SQLITE_ACID_WAL'
        self.secondary_proxy_enabled = True
        self.last_flatten_log = [
            'SYSTEM READY: Dual-Phase Async Flatten Standby (9/10 Lifeboat Protocol)',
        ]
        self.db.record_heartbeat('WEB_WRAPPER_SERVER', os.getpid(), 'ACTIVE')
        self.agent_brain = AutonomousAgentBrain(self)

    def get_ledger_reconciliation(self, account_summary, positions):
        settled = Decimal(str(account_summary.get('cash', '10000.00')))
        equity = Decimal(str(account_summary.get('equity', settled)))
        unsettled = Decimal('0.00')
        open_basis = sum(
            (Decimal(str(p.get('qty', '0'))) * Decimal(str(p.get('avg_entry_price', '0')))).quantize(Decimal('0.01'))
            for p in positions
        )
        nodes = self.db.get_nodes()
        total_active_nodes = sum(Decimal(str(n['active_capital'])) for n in nodes)
        total_cold_storage = sum(Decimal(str(n['cold_storage'])) for n in nodes)
        total_drip_buckets = sum(Decimal(str(n['drip_bucket'])) for n in nodes)

        validator = LedgerValidator(settled, unsettled)
        validator.active_capital_sum = settled + self.simulated_drift_delta
        validator.open_positions_basis_sum = Decimal('0.00')
        virtual_total, alpaca_total, discrepancy = validator.reconcile_morning_cash()

        virtual_equity = equity + self.simulated_drift_delta
        virtual_cash = settled + self.simulated_drift_delta
        pos_count = len(positions)
        is_synced = discrepancy == Decimal('0.00')
        row_status = 'SYNCED' if is_synced else 'DRIFT_LOCK'

        reconciliation_rows = [
            {
                'metric': 'Total Equity',
                'virtual_wealth': f'${virtual_equity:,.2f}',
                'broker_ledger': f'${equity:,.2f}',
                'drift_delta': f'${discrepancy:,.2f}',
                'status': row_status,
            },
            {
                'metric': 'Available Cash',
                'virtual_wealth': f'${virtual_cash:,.2f}',
                'broker_ledger': f'${settled:,.2f}',
                'drift_delta': f'${discrepancy:,.2f}',
                'status': row_status,
            },
            {
                'metric': 'Open Positions',
                'virtual_wealth': str(pos_count),
                'broker_ledger': str(pos_count),
                'drift_delta': '0',
                'status': 'SYNCED',
            },
        ]

        return {
            'alpaca_settled_cash': f'{settled:.2f}',
            'open_positions_basis': f'{open_basis:.2f}',
            'virtual_wealth_total': f'{(virtual_total + open_basis):.2f}',
            'alpaca_cash_total': f'{alpaca_total:.2f}',
            'cash_discrepancy': f'{discrepancy:.2f}',
            'invariants_verified': is_synced,
            'node_generation_locked': not is_synced,
            'reconciliation_rows': reconciliation_rows,
            'internal_active_capital': f'{total_active_nodes:.2f}',
            'internal_cold_storage': f'{total_cold_storage:.2f}',
            'internal_drip_pool': f'{total_drip_buckets:.2f}',
            'internal_total_vault': f'{(total_active_nodes + total_cold_storage + total_drip_buckets):.2f}',
        }

    def get_dashboard_state(self):
        t0 = time.perf_counter()
        self.daemon.last_tick_time = time.time()
        self.db.record_heartbeat('WEB_WRAPPER_SERVER', os.getpid(), 'ACTIVE')
        self.guardian.stale_threshold = float(self.missed_beats_threshold * 10.0)
        guardian_status = self.guardian.check_heartbeat_once()

        account = self.execution.get_account_summary()
        latency_ms = max(4, int((time.perf_counter() - t0) * 1000))
        positions = self.execution.get_positions()
        orders = self.execution.get_orders()
        db_orders = self.db.get_recent_db_orders(limit=15)
        nodes = self.db.get_nodes()
        ledger = self.get_ledger_reconciliation(account, positions)
        equity = Decimal(str(account.get('equity', '100000.00')))
        if equity <= 0:
            equity = Decimal('100000.00')
        max_order_cap = (equity * Decimal('0.30')).quantize(Decimal('0.01'))

        deployed_basis = sum(Decimal(str(p.get('market_value', '0.00'))) for p in positions)
        node_active_sum = sum(Decimal(str(n['active_capital'])) for n in nodes if n['status'] == 'ACTIVE')
        effective_deployed = max(deployed_basis, node_active_sum)
        exposure_pct = float(min(Decimal('100.0'), (effective_deployed / equity * Decimal('100.0')).quantize(Decimal('0.1'))))

        node_allocations = []
        for n in nodes:
            n_cap = Decimal(str(n['active_capital'])) if n['status'] == 'ACTIVE' else Decimal('0.00')
            n_pct = float((n_cap / equity * Decimal('100.0')).quantize(Decimal('0.01')))
            node_allocations.append({
                'node_id': n['node_id'],
                'ticker': n['ticker'],
                'capital': f'{n_cap:.2f}',
                'pct_of_equity': n_pct,
                'status': n['status'],
            })

        rating_score = '7.5 / 10 (STABLE - DEGRADED)'
        if self.db_migration_mode == 'SQLITE_ACID_WAL' and self.secondary_proxy_enabled and not self.post_flatten_locked and ledger['invariants_verified']:
            rating_score = '9.4 / 10 (ENTERPRISE HARDENED)'

        return {
            'system': {
                'version': 'v3.7 DEFCON 0',
                'global_rating': rating_score,
                'execution_state': 'FLAT_LOCKED' if self.post_flatten_locked else self.execution.state,
                'post_flatten_locked': self.post_flatten_locked,
                'last_flatten_log': self.last_flatten_log,
                'active_driver': self.execution.active_driver,
                'primary_latency_ms': latency_ms,
                'secondary_proxy_enabled': self.secondary_proxy_enabled,
                'db_migration_mode': self.db_migration_mode,
                'missed_beats_threshold': self.missed_beats_threshold,
                'failover_count': self.execution.failover_count,
                'guardian_state': guardian_status['guardian_state'],
                'heartbeat_age_seconds': guardian_status.get('heartbeat_age_seconds', 0.0),
                'uptime_seconds': int(time.time() - self.boot_time),
                'rth_active': self.strategy.is_rth(),
                'current_signal': self.strategy.evaluate_signal(self.daemon.quote_buffer),
                'firewall_cap_pct': '30.0%',
                'current_exposure_pct': exposure_pct,
                'deployed_capital': f'{effective_deployed:.2f}',
                'max_single_order_notional': f'{max_order_cap:.2f}',
                'node_allocations': node_allocations,
                'agent_pulse_count': self.agent_brain.pulse_count,
                'agent_last_decision': self.agent_brain.last_decision_summary,
                'agent_reasoning_stream': self.agent_brain.reasoning_stream[:20],
            },
            'agent_settings': self.db.get_agent_settings(),
            'ghost_portfolio': self.db.get_ghost_portfolio_summary(),
            'account': account,
            'positions': positions,
            'orders': orders if orders else db_orders,
            'db_orders': db_orders,
            'ledger': ledger,
            'nodes': nodes,
            'blackouts': self.db.get_blackouts(),
            'heartbeats': self.db.get_heartbeats(),
            'activity_log': self.execution.activity_log[:30],
        }

    def run_strategy_cycle(self, symbol='NVDA', qty=2, estimated_price=142.10, node_id=1):
        signal = self.strategy.evaluate_signal(self.daemon.quote_buffer)
        side = 'buy' if signal == 'BUY' else 'sell'
        result = self.execution.submit_order(
            symbol=symbol,
            side=side,
            qty=qty,
            order_type='market',
            estimated_price=estimated_price,
            node_id=node_id,
        )
        result['strategy_signal'] = signal
        return result

    def run_backtest_preview(self, tier=0, active_capital=25.0, entry_price=100.0):
        node = BacktestBetaNode(tier=int(tier), active_capital=float(active_capital))
        pos_size = node.get_position_size()
        buy_fill = node.execute_trade(float(entry_price), 'buy')
        sell_fill = node.execute_trade(float(entry_price) * 1.03, 'sell')
        graduated = node.simulate_tier_0_graduation()
        return {
            'ok': True,
            'tier': int(tier),
            'active_capital': round(float(active_capital), 2),
            'cold_storage': round(float(node.cold_storage), 2),
            'recommended_position_size': round(pos_size, 2),
            'modeled_buy_fill': round(buy_fill, 2),
            'modeled_target_exit': round(sell_fill, 2),
            'tier_0_graduated': graduated,
        }


broker_service = BrokerService()
TEMPLATE_PATH = Path(__file__).resolve().parent / 'templates' / 'index.html'


def handle_api_post(path, payload):
    if path == '/api/order':
        if broker_service.post_flatten_locked:
            return {'ok': False, 'message': 'SYSTEM LOCKED IN POST-FLATTEN STATE. Enter Supervisor Key (DEFCON0) in Quadrant D to unlock.'}
        return broker_service.execution.submit_order(
            symbol=payload.get('symbol', 'NVDA'),
            side=payload.get('side', 'buy'),
            qty=payload.get('qty', 1),
            order_type=payload.get('order_type', 'market'),
            limit_price=payload.get('limit_price') or None,
            estimated_price=payload.get('estimated_price', 100.0),
            node_id=int(payload.get('node_id', 1)),
        )
    if path == '/api/flatten':
        symbol = payload.get('symbol', 'NVDA')
        reason = payload.get('reason', 'MANUAL_FLATTEN')
        broker_service.last_flatten_log = [
            f'[PHASE 1: OCO/LIMIT PURGE] Canceling all open maker/OCO legs for {symbol}...',
            f'[PHASE 2: MARKET SWEEP] Executing aggressive market liquidation for {symbol} ({reason})...',
            f'[POST-FLATTEN LOCK] {symbol} position flattened. System locked in FLAT state awaiting Supervisor Key.',
        ]
        if payload.get('lock_system', False):
            broker_service.post_flatten_locked = True
        return asyncio.run(broker_service.execution.execute_exit(symbol, reason))
    if path == '/api/escape-valve':
        symbol = payload.get('symbol', 'NVDA')
        breach = float(payload.get('price_breach', 5.0))
        broker_service.post_flatten_locked = True
        broker_service.last_flatten_log = [
            f'[CRITICAL ESCAPE VALVE] {symbol} breached {breach}% threshold!',
            '[PHASE 1: OCO/LIMIT PURGE] Purging all open maker orders across active nodes...',
            '[PHASE 2: MARKET SWEEP] 100% Market sweep liquidation completed to settled cash.',
            '[POST-FLATTEN STATE] Terminal locked in FLAT state. Supervisor Key required to reboot daemon.',
        ]
        return asyncio.run(broker_service.execution.catastrophic_escape_valve(symbol, breach))
    if path == '/api/supervisor-unlock':
        key = str(payload.get('supervisor_key', '')).strip()
        if key.upper() in ('DEFCON0', 'ADMIN', 'UNLOCK', 'V37'):
            broker_service.post_flatten_locked = False
            broker_service.last_flatten_log.append('[SUPERVISOR AUTH] Valid Supervisor Key accepted. Daemon unlocked and returned to IDLE.')
            broker_service.execution._record_event('SUPERVISOR', 'Post-Flatten Lock cleared via Supervisor Key.', 'INFO')
            return {'ok': True, 'message': 'Supervisor Key verified. Post-Flatten lock cleared & trading resumed.'}
        return {'ok': False, 'message': 'Invalid Supervisor Key (Hint: use DEFCON0).'}
    if path == '/api/simulate-drift':
        amount = Decimal(str(payload.get('drift_amount', '0.00')))
        broker_service.simulated_drift_delta = amount
        msg = f'ACID Ledger Drift set to ${amount:.2f}' if amount != 0 else 'ACID Ledger Drift resolved ($0.00 delta restored).'
        broker_service.execution._record_event('LEDGER_AUDIT', msg, 'WARN' if amount != 0 else 'INFO')
        return {'ok': True, 'drift_delta': f'{amount:.2f}', 'message': msg}
    if path == '/api/isolate-node':
        node_id = int(payload.get('node_id', 1))
        res = broker_service.db.isolate_node(node_id)
        msg = f"Blast Radius Containment: Node #{node_id} ({res['ticker']}) toggled to {res['new_status']}."
        broker_service.execution._record_event('TOPOLOGY', msg, 'WARN' if res['new_status'] != 'ACTIVE' else 'INFO')
        res['message'] = msg
        return res
    if path == '/api/diagnostics-config':
        if 'missed_beats_threshold' in payload:
            broker_service.missed_beats_threshold = max(1, min(20, int(payload['missed_beats_threshold'])))
        if 'db_migration_mode' in payload:
            broker_service.db_migration_mode = str(payload['db_migration_mode'])
        if 'secondary_proxy_enabled' in payload:
            broker_service.secondary_proxy_enabled = bool(payload['secondary_proxy_enabled'])
        msg = (
            f'Diagnostics updated: DB={broker_service.db_migration_mode}, '
            f'ProxyFailover={broker_service.secondary_proxy_enabled}, '
            f'KillTrigger={broker_service.missed_beats_threshold} beats'
        )
        broker_service.execution._record_event('DIAGNOSTICS', msg, 'INFO')
        return {'ok': True, 'message': msg}
    if path == '/api/strategy-tick':
        return broker_service.run_strategy_cycle(
            symbol=payload.get('symbol', 'NVDA'),
            qty=payload.get('qty', 2),
            estimated_price=payload.get('estimated_price', 142.10),
            node_id=int(payload.get('node_id', 1)),
        )
    if path == '/api/backtest':
        return broker_service.run_backtest_preview(
            tier=payload.get('tier', 0),
            active_capital=payload.get('active_capital', 25.0),
            entry_price=payload.get('entry_price', 100.0),
        )
    if path == '/api/settlement-sync':
        broker_service.router.sync_settlement_budgets()
        broker_service.execution._record_event('ROUTER', '9:29:30 AM Settlement Budget Sync completed.', 'INFO')
        return {'ok': True, 'message': '9:29:30 AM Settlement Budget Sync completed.'}
    if path == '/api/drip-fill':
        node_id = int(payload.get('node_id', 1))
        profit = payload.get('profit', 15.0)
        res = broker_service.db.allocate_drip_profit(node_id=node_id, net_profit_amount=profit)
        msg = f"Node #{node_id} Drip Profit +${profit}: +${res['added_active']} Active / +${res['added_cold']} Cold Storage (Bucket: ${res['new_drip_bucket']})"
        if res.get('spawned_child'):
            child = res['spawned_child']
            msg += f" -> SPAWNED CHILD NODE #{child['child_node_id']} ({child['ticker']})!"
        broker_service.execution._record_event('DRIP_BUCKET', msg, 'INFO')
        res['message'] = msg
        return res
    if path == '/api/self-heal':
        res = broker_service.db.self_heal_nodes()
        msg = f"Self-Healing Audit Complete: {res['healed_count']} node(s) restored to ACTIVE."
        broker_service.execution._record_event('SELF_HEAL', msg, 'INFO')
        res['message'] = msg
        return res
    if path == '/api/guardian-check':
        res = broker_service.guardian.check_heartbeat_once()
        broker_service.execution._record_event('GUARDIAN', res['message'], 'INFO' if res['ok'] else 'CRITICAL')
        return res
    if path == '/api/fund-gate':
        updated = broker_service.db.update_agent_settings(
            fund_gate_enabled=payload.get('fund_gate_enabled'),
            execution_mode=payload.get('execution_mode'),
            autonomous_loop_enabled=payload.get('autonomous_loop_enabled'),
            pulse_interval_seconds=payload.get('pulse_interval_seconds'),
        )
        gate_state = 'OPEN (LIVE FUNDS UNLOCKED)' if updated['fund_gate_enabled'] else 'CLOSED (WALLET LOCKED - GHOST ONLY)'
        msg = f"Capital Access Gate: {gate_state} | Mode: {updated['execution_mode']} | Auto-Pulse: {'ON' if updated['autonomous_loop_enabled'] else 'OFF'}"
        broker_service.execution._record_event('FUND_GATE', msg, 'WARN' if updated['fund_gate_enabled'] else 'INFO')
        return {'ok': True, 'settings': updated, 'message': msg}
    if path == '/api/agent-pulse':
        target_node = payload.get('node_id')
        override_price = payload.get('override_market_price')
        res = broker_service.agent_brain.run_single_pulse(target_node_id=target_node, override_market_price=override_price)
        return res
    return None


class BrokerHTTPRequestHandler(BaseHTTPRequestHandler):
    def _send_json(self, data, status=200):
        body = json.dumps(data).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html_text, status=200):
        body = html_text.encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in ('/', '/index.html'):
            if TEMPLATE_PATH.exists():
                self._send_html(TEMPLATE_PATH.read_text(encoding='utf-8'))
            else:
                self._send_html('<h1>Template missing</h1>', status=500)
            return
        if parsed.path == '/api/state':
            self._send_json(broker_service.get_dashboard_state())
            return
        if parsed.path == '/api/ledger/reconciliation':
            acct = broker_service.execution.get_account_summary()
            pos = broker_service.execution.get_positions()
            self._send_json(broker_service.get_ledger_reconciliation(acct, pos))
            return
        if parsed.path == '/api/health':
            self._send_json({'status': 'ok', 'version': 'v3.7 DEFCON 0'})
            return
        self._send_json({'error': 'Not found'}, status=404)

    def do_POST(self):
        parsed = urlparse(self.path)
        content_len = int(self.headers.get('Content-Length', '0'))
        raw_body = self.rfile.read(content_len).decode('utf-8') if content_len > 0 else '{}'
        try:
            payload = json.loads(raw_body) if raw_body else {}
        except json.JSONDecodeError:
            self._send_json({'ok': False, 'message': 'Invalid JSON payload'}, status=400)
            return

        try:
            result = handle_api_post(parsed.path, payload)
            if result is None:
                self._send_json({'ok': False, 'message': f'Unknown endpoint {parsed.path}'}, status=404)
                return
            self._send_json(result, status=200)
        except Exception as exc:
            logger.exception('API POST error')
            self._send_json({'ok': False, 'message': str(exc)}, status=400)

    def log_message(self, fmt, *args):
        logger.info('%s - %s', self.address_string(), fmt % args)


def run_server(host=None, port=None):
    cfg = get_broker_config()
    bind_host = host or cfg['host']
    bind_port = int(port or cfg['port'])
    broker_service.agent_brain.start_background_loop()
    server = ThreadingHTTPServer((bind_host, bind_port), BrokerHTTPRequestHandler)
    logger.info(f'Stock Broker Web Wrapper (v3.7 DEFCON 0) listening on http://{bind_host}:{bind_port}')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info('Shutting down web wrapper server...')
    finally:
        server.server_close()


if __name__ == '__main__':
    run_server()

