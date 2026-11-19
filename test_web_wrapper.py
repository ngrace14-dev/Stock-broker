import json
import threading
import unittest
import urllib.request
from http.server import ThreadingHTTPServer
from backtest_beta import test_backtest_math
from ledger_validator import test_v37_invariants
from server import BrokerHTTPRequestHandler, broker_service, handle_api_post


class TestWebWrapperIntegration(unittest.TestCase):
    def setUp(self):
        # Force deterministic sandbox mode during unit tests so we never fire unplanned live/paper orders
        broker_service.execution.refresh_config = lambda: broker_service.execution.config
        broker_service.execution.config['has_alpaca_keys'] = False

    def test_existing_invariants(self):
        test_v37_invariants()
        test_backtest_math()

    def test_dashboard_state_and_order_firewall(self):
        state = broker_service.get_dashboard_state()
        self.assertIn('account', state)
        self.assertIn('positions', state)
        self.assertIn('ledger', state)
        self.assertTrue(state['ledger']['invariants_verified'])

        # Valid order within 30% firewall cap
        res_ok = handle_api_post('/api/order', {
            'symbol': 'AAPL',
            'side': 'buy',
            'qty': 2,
            'order_type': 'market',
            'estimated_price': 190.00,
            'node_id': 1,
        })
        self.assertTrue(res_ok['ok'], res_ok)

        # Oversized order exceeding 30% systemic exposure cap should be blocked by MasterRouter
        res_blocked = handle_api_post('/api/order', {
            'symbol': 'AAPL',
            'side': 'buy',
            'qty': 100,
            'order_type': 'market',
            'estimated_price': 200.00,
            'node_id': 1,
        })
        self.assertFalse(res_blocked['ok'])
        self.assertTrue(res_blocked.get('blocked_by_firewall'))

    def test_two_phase_flatten_and_backtest(self):
        flatten_res = handle_api_post('/api/flatten', {'symbol': 'AAPL', 'reason': 'UNIT_TEST'})
        self.assertTrue(flatten_res['ok'])
        self.assertEqual(flatten_res['state'], 'IDLE')

        bt_res = handle_api_post('/api/backtest', {'tier': 0, 'active_capital': 50.0, 'entry_price': 100.0})
        self.assertTrue(bt_res['ok'])
        self.assertTrue(bt_res['tier_0_graduated'])

    def test_http_server_viewport_and_endpoints(self):
        httpd = ThreadingHTTPServer(('127.0.0.1', 0), BrokerHTTPRequestHandler)
        port = httpd.server_address[1]
        t = threading.Thread(target=httpd.serve_forever, daemon=True)
        t.start()
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{port}/', timeout=5) as resp:
                html = resp.read().decode('utf-8')
                self.assertEqual(resp.status, 200)
                self.assertIn('meta name="viewport"', html)
                self.assertIn('AUTO-BROKER TERMINAL // v3.7 DEFCON 0', html)
                self.assertIn('dripBucketsGrid', html)
                self.assertIn('DOMContentLoaded', html)

            with urllib.request.urlopen(f'http://127.0.0.1:{port}/api/state', timeout=5) as resp:
                payload = json.loads(resp.read().decode('utf-8'))
                self.assertEqual(resp.status, 200)
                self.assertIn('v3.7 DEFCON 0', payload['system']['version'])
                self.assertIn('internal_total_vault', payload['ledger'])
                self.assertEqual(len(payload['ledger']['reconciliation_rows']), 3)

            with urllib.request.urlopen(f'http://127.0.0.1:{port}/api/ledger/reconciliation', timeout=5) as resp:
                recon = json.loads(resp.read().decode('utf-8'))
                self.assertEqual(resp.status, 200)
                self.assertTrue(recon['invariants_verified'])

            drift_res = handle_api_post('/api/simulate-drift', {'drift_amount': '145.50'})
            self.assertTrue(drift_res['ok'])
            st_drifted = broker_service.get_dashboard_state()
            self.assertFalse(st_drifted['ledger']['invariants_verified'])
            self.assertTrue(st_drifted['ledger']['node_generation_locked'])
            handle_api_post('/api/simulate-drift', {'drift_amount': '0.00'})

            iso_res = handle_api_post('/api/isolate-node', {'node_id': 2})
            self.assertTrue(iso_res['ok'])

            drip_res = handle_api_post('/api/drip-fill', {'node_id': 1, 'profit': 20.0})
            self.assertTrue(drip_res['ok'])

            heal_res = handle_api_post('/api/self-heal', {})
            self.assertTrue(heal_res['ok'])

            guard_res = handle_api_post('/api/guardian-check', {})
            self.assertTrue(guard_res['ok'])

            esc_res = handle_api_post('/api/escape-valve', {'symbol': 'NVDA', 'price_breach': 5.0})
            self.assertTrue(esc_res['ok'])
            self.assertTrue(broker_service.post_flatten_locked)

            unlock_res = handle_api_post('/api/supervisor-unlock', {'supervisor_key': 'DEFCON0'})
            self.assertTrue(unlock_res['ok'])
            self.assertFalse(broker_service.post_flatten_locked)

            # Verify Fund Gate Lock + Ghost Mode Autonomous Pulse + Honest Realized Gain/Loss Tracking
            fg_res = handle_api_post('/api/fund-gate', {'fund_gate_enabled': False, 'execution_mode': 'GHOST'})
            self.assertTrue(fg_res['ok'])
            self.assertFalse(fg_res['settings']['fund_gate_enabled'])

            # Pulse 1 on Node 1 (NVDA): Autonomous Ghost Buy at $140.00
            p1 = handle_api_post('/api/agent-pulse', {'node_id': 1, 'override_market_price': 140.00})
            self.assertTrue(p1['ok'])
            self.assertIn('AUTO_BUY', p1['action'])

            # Pulse 2 on Node 1 (NVDA): Autonomous Ghost Sell at $145.00 (+5.00/share profit)
            p2 = handle_api_post('/api/agent-pulse', {'node_id': 1, 'override_market_price': 145.00})
            self.assertTrue(p2['ok'])
            self.assertIn('AUTO_SELL_CLOSE', p2['action'])
            self.assertGreater(float(p2['realized_pnl']), 0.0)

            ghost_state = broker_service.db.get_ghost_portfolio_summary()
            self.assertGreater(len(ghost_state['trades']), 0)
            self.assertTrue(ghost_state['trades'][0]['audit_hash'])
        finally:
            httpd.shutdown()
            httpd.server_close()


if __name__ == '__main__':
    unittest.main()

