import json
import threading
import unittest
import urllib.request
from http.server import ThreadingHTTPServer
from backtest_beta import test_backtest_math
from ledger_validator import test_v37_invariants
from server import BrokerHTTPRequestHandler, broker_service, handle_api_post


class TestWebWrapperIntegration(unittest.TestCase):
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

            with urllib.request.urlopen(f'http://127.0.0.1:{port}/api/state', timeout=5) as resp:
                payload = json.loads(resp.read().decode('utf-8'))
                self.assertEqual(resp.status, 200)
                self.assertEqual(payload['system']['version'], 'v3.7 DEFCON 0')
        finally:
            httpd.shutdown()
            httpd.server_close()


if __name__ == '__main__':
    unittest.main()

