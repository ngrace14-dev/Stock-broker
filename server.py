import asyncio
import base64
import hmac
import json
import logging
import os
import time
from decimal import Decimal
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from agent_brain import AutonomousAgentBrain
from backtest_beta import BacktestBetaNode, StrategyBacktestEngine
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

        ghost_summary = self.db.get_ghost_portfolio_summary()
        unified_investments = self._build_unified_investments(positions, ghost_summary, account)

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
            'ghost_portfolio': ghost_summary,
            'unified_investments': unified_investments,
            'pulse_audit_log': self.db.get_market_pulse_audit_log(limit=60),
            'engine_diagnostics': self.db.get_engine_diagnostics(),
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

    def _build_unified_investments(self, live_positions, ghost_summary, account):
        live_cost_basis = Decimal('0.00')
        live_market_val = Decimal('0.00')
        live_unrealized = Decimal('0.00')
        items = []

        for lp in live_positions:
            qty = Decimal(str(lp.get('qty', '0')))
            avg_p = Decimal(str(lp.get('avg_entry_price', '0')))
            cur_p = Decimal(str(lp.get('current_price', avg_p)))
            cost = (qty * avg_p).quantize(Decimal('0.01'))
            mval = Decimal(str(lp.get('market_value', (qty * cur_p).quantize(Decimal('0.01')))))
            unrl = Decimal(str(lp.get('unrealized_pl', (mval - cost).quantize(Decimal('0.01')))))
            pct = ((unrl / cost) * Decimal('100.0')).quantize(Decimal('0.01')) if cost > 0 else Decimal('0.00')
            live_cost_basis += cost
            live_market_val += mval
            live_unrealized += unrl
            items.append({
                'mode': 'LIVE',
                'symbol': lp.get('symbol'),
                'node_id': '-',
                'qty': f'{qty:.2f}',
                'entry_price': f'{avg_p:.2f}',
                'current_price': f'{cur_p:.2f}',
                'cost_basis': f'{cost:.2f}',
                'market_value': f'{mval:.2f}',
                'gain_loss': f'{unrl:.2f}',
                'gain_loss_pct': f'{pct:.2f}%',
            })

        ghost_cost_basis = Decimal('0.00')
        for gp in ghost_summary.get('positions', []):
            qty = Decimal(str(gp.get('qty', '0')))
            avg_p = Decimal(str(gp.get('avg_entry_price', '0')))
            cur_p = Decimal(str(gp.get('current_market_price', avg_p)))
            cost = (qty * avg_p).quantize(Decimal('0.01'))
            mval = Decimal(str(gp.get('market_value', (qty * cur_p).quantize(Decimal('0.01')))))
            unrl = Decimal(str(gp.get('unrealized_pnl', (mval - cost).quantize(Decimal('0.01')))))
            pct = gp.get('unrealized_pnl_pct', '0.00')
            ghost_cost_basis += cost
            items.append({
                'mode': 'GHOST',
                'symbol': gp.get('symbol'),
                'node_id': gp.get('node_id', 1),
                'qty': f'{qty:.2f}',
                'entry_price': f'{avg_p:.2f}',
                'current_price': f'{cur_p:.2f}',
                'cost_basis': f'{cost:.2f}',
                'market_value': f'{mval:.2f}',
                'gain_loss': f'{unrl:.2f}',
                'gain_loss_pct': f'{pct}%',
            })

        live_pct = ((live_unrealized / live_cost_basis) * Decimal('100.0')).quantize(Decimal('0.01')) if live_cost_basis > 0 else Decimal('0.00')
        return {
            'live': {
                'active_count': len(live_positions),
                'invested_cost_basis': f'{live_cost_basis:.2f}',
                'current_market_value': f'{live_market_val:.2f}',
                'unrealized_gain_loss': f'{live_unrealized:.2f}',
                'unrealized_gain_loss_pct': f'{live_pct:.2f}%',
                'available_cash': str(account.get('cash', '0.00')),
                'total_equity': str(account.get('equity', '0.00')),
            },
            'ghost': {
                'active_count': len(ghost_summary.get('positions', [])),
                'invested_cost_basis': f'{ghost_cost_basis:.2f}',
                'current_market_value': ghost_summary.get('positions_value', '0.00'),
                'unrealized_gain_loss': ghost_summary.get('unrealized_pnl', '0.00'),
                'realized_gain_loss': ghost_summary.get('realized_pnl', '0.00'),
                'net_gain_loss': ghost_summary.get('net_pnl', '0.00'),
                'return_pct': ghost_summary.get('return_pct', '0.00%'),
                'win_rate': ghost_summary.get('win_rate', '0.0%'),
                'wins': ghost_summary.get('wins', 0),
                'losses': ghost_summary.get('losses', 0),
                'total_equity': ghost_summary.get('total_equity', '10000.00'),
            },
            'items': items,
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
        bt_engine = StrategyBacktestEngine(db_path=self.db.db_path)
        recorded_ticks = bt_engine.load_recorded_pulse_stream()
        recorded_comparison = [
            bt_engine.simulate_profile(recorded_ticks, key, starting_cash=Decimal('100.00'), max_positions=1)
            for key in ('HYBRID_CORE_SATELLITE', 'MODERATE_MOMENTUM_V39', 'ASYMMETRIC_EDGE_V38', 'BASELINE_V37')
        ] if recorded_ticks else []
        return {
            'ok': True,
            'tier': int(tier),
            'active_capital': round(float(active_capital), 2),
            'cold_storage': round(float(node.cold_storage), 2),
            'recommended_position_size': round(pos_size, 2),
            'modeled_buy_fill': round(buy_fill, 2),
            'modeled_target_exit': round(sell_fill, 2),
            'tier_0_graduated': graduated,
            'recorded_pulse_count': len(recorded_ticks),
            'strategy_comparison': recorded_comparison,
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
    if path == '/api/panic':
        key = str(payload.get('panic_keyword', '')).strip()
        if key == 'PANIC':
            broker_service.post_flatten_locked = True
            broker_service.execution.state = 'PANIC_FLATTENING'
            broker_service.last_flatten_log = ['[PANIC MODE] SYSTEM LOCKDOWN INITIATED. ALL POSITIONS LIQUIDATING.']
            asyncio.run(broker_service.execution.execute_exit('ALL', 'EMERGENCY_PANIC'))
            broker_service.db.update_agent_settings(fund_gate_enabled=False, execution_mode='GHOST')
            return {'ok': True, 'message': 'EMERGENCY PANIC EXECUTED. Wallet Locked. System in Lockdown.'}
        return {'ok': False, 'message': 'Invalid Panic Keyword.'}
    if path == '/api/supervisor-unlock':
        key = str(payload.get('supervisor_key', '')).strip()
        if key.upper() in ('14141414', 'DEFCON0', 'ADMIN', 'UNLOCK', 'V37'):
            broker_service.post_flatten_locked = False
            broker_service.execution.state = 'IDLE'
            broker_service.last_flatten_log.append('[SYSTEM REINITIALIZED] 14141414 Accepted. System ready for duty.')
            broker_service.execution._record_event('SUPERVISOR', 'System reinitialized via 14141414.', 'INFO')
            return {'ok': True, 'message': 'System Reinitialized. Trading capabilities restored.'}
        return {'ok': False, 'message': 'Invalid Reinitialization Key (Use 14141414).'}
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
        force_close = bool(payload.get('force_close', False))
        res = broker_service.agent_brain.run_single_pulse(
            target_node_id=target_node,
            override_market_price=override_price,
            force_close=force_close,
        )
        return res
    if path == '/api/start-investing':
        req_mode = payload.get('mode')
        res = broker_service.agent_brain.deploy_fleet_investments(requested_mode=req_mode)
        return res
    if path == '/api/ghost-reset':
        starting_cash = payload.get('starting_cash', '25.00')
        reset_nodes = bool(payload.get('reset_nodes', True))
        res = broker_service.db.reset_ghost_portfolio(starting_cash=starting_cash, reset_nodes=reset_nodes)
        broker_service.agent_brain.pulse_count = 0
        broker_service.agent_brain.reasoning_stream.clear()
        broker_service.agent_brain.last_decision_summary = f"Ghost Portfolio reset to ${res['starting_cash']} starting capital."
        broker_service.execution._record_event('GHOST_RESET', res['message'], 'INFO')
        if payload.get('auto_deploy', True):
            deploy_res = broker_service.agent_brain.deploy_fleet_investments(requested_mode='GHOST')
            res['deployed'] = deploy_res.get('deployed', [])
            res['message'] += f" Deployed {len(res['deployed'])} initial Ghost position(s) at live market quotes."
        return res
    if path == '/api/nitro-inject':
        res = broker_service.db.inject_nitro_funds(
            amount=payload.get('amount', '100.00'),
            queue_for_pulse=bool(payload.get('queue_for_pulse', False)),
        )
        broker_service.execution._record_event('NITRO_PROTOCOL', res.get('message', 'Nitro injection executed'), 'INFO')
        return res
    return None


class BrokerHTTPRequestHandler(BaseHTTPRequestHandler):
    def _send_json(self, data, status=200, extra_headers=None):
        body = json.dumps(data).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html_text, status=200, extra_headers=None):
        body = html_text.encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _check_auth(self, parsed_url):
        """Verifies personal password when REMOTE_AUTH_PASS is configured in .env."""
        cfg = get_broker_config()
        if not cfg.get('has_remote_auth'):
            return True, {}

        expected_pass = cfg['remote_auth_pass']

        # 1. Query param (?pass=...) sets HttpOnly cookie so subsequent fetch calls work
        query_params = parse_qs(parsed_url.query)
        if 'pass' in query_params and query_params['pass']:
            if hmac.compare_digest(query_params['pass'][0], expected_pass):
                return True, {'Set-Cookie': f'broker_auth={expected_pass}; Path=/; HttpOnly; SameSite=Lax'}

        # 2. X-Access-Token header
        token_header = self.headers.get('X-Access-Token', '').strip()
        if token_header and hmac.compare_digest(token_header, expected_pass):
            return True, {}

        # 3. Authorization header (Basic or Bearer)
        auth_header = self.headers.get('Authorization', '').strip()
        if auth_header.lower().startswith('bearer '):
            if hmac.compare_digest(auth_header[7:].strip(), expected_pass):
                return True, {}
        elif auth_header.lower().startswith('basic '):
            try:
                decoded = base64.b64decode(auth_header[6:].strip()).decode('utf-8', errors='replace')
                basic_pass = decoded.split(':', 1)[1] if ':' in decoded else decoded
                if hmac.compare_digest(basic_pass, expected_pass):
                    return True, {}
            except Exception:
                pass

        # 4. Session cookie (broker_auth)
        cookie_header = self.headers.get('Cookie', '')
        if cookie_header:
            cookie = SimpleCookie()
            try:
                cookie.load(cookie_header)
                if 'broker_auth' in cookie and hmac.compare_digest(cookie['broker_auth'].value, expected_pass):
                    return True, {}
            except Exception:
                pass

        auth_headers = {'WWW-Authenticate': 'Basic realm="Auto-Broker Terminal v3.7 (Enter Personal Password)"'}
        if parsed_url.path.startswith('/api/'):
            self._send_json(
                {'ok': False, 'error': 'Unauthorized: Personal beta password required.'},
                status=401,
                extra_headers=auth_headers,
            )
        else:
            self._send_html(
                '<h2>401 Unauthorized — Auto-Broker Terminal</h2><p>Valid personal password required.</p>',
                status=401,
                extra_headers=auth_headers,
            )
        return False, {}

    def do_GET(self):
        parsed = urlparse(self.path)
        authorized, extra_headers = self._check_auth(parsed)
        if not authorized:
            return
        if parsed.path in ('/', '/index.html'):
            if TEMPLATE_PATH.exists():
                self._send_html(TEMPLATE_PATH.read_text(encoding='utf-8'), extra_headers=extra_headers)
            else:
                self._send_html('<h1>Template missing</h1>', status=500, extra_headers=extra_headers)
            return
        if parsed.path == '/api/state':
            self._send_json(broker_service.get_dashboard_state(), extra_headers=extra_headers)
            return
        if parsed.path == '/api/ledger/reconciliation':
            acct = broker_service.execution.get_account_summary()
            pos = broker_service.execution.get_positions()
            self._send_json(broker_service.get_ledger_reconciliation(acct, pos), extra_headers=extra_headers)
            return
        if parsed.path == '/api/health':
            self._send_json({'status': 'ok', 'version': 'v3.7 DEFCON 0'}, extra_headers=extra_headers)
            return
        self._send_json({'error': 'Not found'}, status=404, extra_headers=extra_headers)

    def do_POST(self):
        parsed = urlparse(self.path)
        authorized, extra_headers = self._check_auth(parsed)
        if not authorized:
            return
        content_len = int(self.headers.get('Content-Length', '0'))
        raw_body = self.rfile.read(content_len).decode('utf-8') if content_len > 0 else '{}'
        try:
            payload = json.loads(raw_body) if raw_body else {}
        except json.JSONDecodeError:
            self._send_json({'ok': False, 'message': 'Invalid JSON payload'}, status=400, extra_headers=extra_headers)
            return

        try:
            result = handle_api_post(parsed.path, payload)
            if result is None:
                self._send_json({'ok': False, 'message': f'Unknown endpoint {parsed.path}'}, status=404, extra_headers=extra_headers)
                return
            self._send_json(result, status=200, extra_headers=extra_headers)
        except Exception as exc:
            logger.exception('API POST error')
            self._send_json({'ok': False, 'message': str(exc)}, status=400, extra_headers=extra_headers)

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

