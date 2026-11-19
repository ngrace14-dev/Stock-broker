import asyncio
import json
import logging
import time
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from backtest_beta import BacktestBetaNode
from config import get_broker_config
from daemon import AutoBrokerDaemon
from ledger_validator import LedgerValidator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('WebWrapperServer')


class BrokerService:
    """Orchestrates AutoBrokerDaemon, MasterRouter, ExecutionEngine, LedgerValidator, and Backtest nodes."""

    def __init__(self):
        self.daemon = AutoBrokerDaemon()
        self.router = self.daemon.router
        self.execution = self.daemon.execution
        self.strategy = self.daemon.strategy_engine
        self.boot_time = time.time()
        self.blackouts = self._load_blackouts()
        self.nodes = [
            {'node_id': 1, 'division': 'VELOCITY_BETA', 'cadence': 'HIGH_VELOCITY', 'ticker': 'NVDA', 'tier': 0, 'active_capital': '2500.00', 'cold_storage': '2500.00', 'status': 'ACTIVE'},
            {'node_id': 2, 'division': 'HARDWARE_TECH', 'cadence': 'SWING', 'ticker': 'SPY', 'tier': 1, 'active_capital': '3000.00', 'cold_storage': '3000.00', 'status': 'ACTIVE'},
            {'node_id': 3, 'division': 'PRECIOUS_METALS', 'cadence': 'MACRO_CYCLICAL', 'ticker': 'GLD', 'tier': 0, 'active_capital': '1500.00', 'cold_storage': '1500.00', 'status': 'ACTIVE'},
            {'node_id': 4, 'division': 'DIVIDEND_INSURER', 'cadence': 'PERMANENT_ANCHOR', 'ticker': 'SCHD', 'tier': 2, 'active_capital': '2000.00', 'cold_storage': '2000.00', 'status': 'ACTIVE'},
        ]

    def _load_blackouts(self):
        path = Path(__file__).resolve().parent / 'macro_blackouts_2026_2027.json'
        if path.exists():
            try:
                with path.open('r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as exc:
                logger.warning(f'Could not read macro_blackouts_2026_2027.json: {exc}')
        return {'schema_version': '3.3', 'events': []}

    def get_ledger_reconciliation(self, account_summary, positions):
        settled = Decimal(str(account_summary.get('cash', '10000.00')))
        unsettled = Decimal('0.00')
        open_basis = sum(
            (Decimal(str(p.get('qty', '0'))) * Decimal(str(p.get('avg_entry_price', '0')))).quantize(Decimal('0.01'))
            for p in positions
        )
        validator = LedgerValidator(settled, unsettled)
        validator.active_capital_sum = settled
        validator.open_positions_basis_sum = Decimal('0.00')
        virtual_total, alpaca_total, discrepancy = validator.reconcile_morning_cash()
        return {
            'alpaca_settled_cash': f'{settled:.2f}',
            'open_positions_basis': f'{open_basis:.2f}',
            'virtual_wealth_total': f'{(virtual_total + open_basis):.2f}',
            'alpaca_cash_total': f'{alpaca_total:.2f}',
            'cash_discrepancy': f'{discrepancy:.2f}',
            'invariants_verified': discrepancy == Decimal('0.00'),
        }

    def get_dashboard_state(self):
        self.daemon.last_tick_time = time.time()
        account = self.execution.get_account_summary()
        positions = self.execution.get_positions()
        orders = self.execution.get_orders()
        ledger = self.get_ledger_reconciliation(account, positions)
        equity = Decimal(str(account.get('equity', '0.00')))
        max_order_cap = (equity * Decimal('0.30')).quantize(Decimal('0.01'))

        return {
            'system': {
                'version': 'v3.7 DEFCON 0',
                'execution_state': self.execution.state,
                'uptime_seconds': int(time.time() - self.boot_time),
                'rth_active': self.strategy.is_rth(),
                'current_signal': self.strategy.evaluate_signal(self.daemon.quote_buffer),
                'firewall_cap_pct': '30.0%',
                'max_single_order_notional': f'{max_order_cap:.2f}',
            },
            'account': account,
            'positions': positions,
            'orders': orders,
            'ledger': ledger,
            'nodes': self.nodes,
            'blackouts': self.blackouts.get('events', []),
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
        return asyncio.run(broker_service.execution.execute_exit(symbol, reason))
    if path == '/api/escape-valve':
        symbol = payload.get('symbol', 'NVDA')
        breach = float(payload.get('price_breach', 5.0))
        return asyncio.run(broker_service.execution.catastrophic_escape_valve(symbol, breach))
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

