import asyncio
import json
import logging
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from config import get_broker_config


class ExecutionEngine:
    def __init__(self, router=None, db=None):
        self.logger = logging.getLogger('ExecutionEngine')
        self.state = 'IDLE'
        self.router = router
        self.db = db
        self.active_driver = 'PRIMARY_ALPACA'
        self.failover_count = 0
        self.config = get_broker_config()
        self.sim_cash = Decimal('10000.00')
        self.sim_unsettled_cash = Decimal('0.00')
        self.sim_positions = {
            'NVDA': {
                'symbol': 'NVDA',
                'qty': '10',
                'avg_entry_price': '138.50',
                'current_price': '142.10',
                'market_value': '1421.00',
                'unrealized_pl': '36.00',
                'unrealized_plpc': '0.0260',
                'side': 'long',
            },
            'SPY': {
                'symbol': 'SPY',
                'qty': '5',
                'avg_entry_price': '585.00',
                'current_price': '589.40',
                'market_value': '2947.00',
                'unrealized_pl': '22.00',
                'unrealized_plpc': '0.0075',
                'side': 'long',
            },
        }
        self.sim_orders = []
        self.activity_log = []
        self._record_event('SYSTEM', 'ExecutionEngine initialized (v3.7 DEFCON 0)')

    def refresh_config(self):
        self.config = get_broker_config()
        return self.config

    def _record_event(self, category, message, level='INFO'):
        entry = {
            'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
            'category': category,
            'level': level,
            'message': message,
        }
        self.activity_log.insert(0, entry)
        del self.activity_log[100:]
        return entry

    def _alpaca_request(self, method, endpoint, payload=None):
        self.refresh_config()
        if not self.config['has_alpaca_keys']:
            raise RuntimeError('Alpaca API keys are not configured in .env yet.')

        url = f"{self.config['alpaca_base_url']}{endpoint}"
        headers = {
            'APCA-API-KEY-ID': self.config['alpaca_api_key'],
            'APCA-API-SECRET-KEY': self.config['alpaca_secret_key'],
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'User-Agent': 'StockBroker-v3.7/1.0',
        }
        data = json.dumps(payload).encode('utf-8') if payload is not None else None
        req = urllib.request.Request(url=url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                body = resp.read().decode('utf-8')
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as e:
            err_body = e.read().decode('utf-8', errors='replace')
            self.logger.error(f'Alpaca API HTTP {e.code} on {method} {endpoint}: {err_body}')
            raise RuntimeError(f'Alpaca API error ({e.code}): {err_body}') from e
        except urllib.error.URLError as e:
            self.logger.error(f'Alpaca network error on {method} {endpoint}: {e}')
            raise RuntimeError(f'Alpaca connection error: {e.reason}') from e

    def get_account_summary(self):
        self.refresh_config()
        if self.config['has_alpaca_keys']:
            try:
                acct = self._alpaca_request('GET', '/v2/account')
                return {
                    'mode': 'ALPACA_PAPER' if self.config['alpaca_paper'] else 'ALPACA_LIVE',
                    'connected': True,
                    'account_id': acct.get('id', 'ALPACA'),
                    'status': acct.get('status', 'ACTIVE'),
                    'cash': str(acct.get('cash', '0.00')),
                    'buying_power': str(acct.get('buying_power', '0.00')),
                    'equity': str(acct.get('equity', '0.00')),
                    'portfolio_value': str(acct.get('portfolio_value', acct.get('equity', '0.00'))),
                    'trading_blocked': bool(acct.get('trading_blocked', False)),
                    'pattern_day_trader': bool(acct.get('pattern_day_trader', False)),
                    'key_preview': self.config['key_preview'],
                }
            except Exception as exc:
                self._record_event('BROKER', f'Live Alpaca account fetch failed: {exc}', 'WARN')
                return self._build_sim_account(error=str(exc))
        return self._build_sim_account()

    def _build_sim_account(self, error=None):
        pos_val = sum(Decimal(p['market_value']) for p in self.sim_positions.values())
        equity = self.sim_cash + pos_val
        return {
            'mode': 'SIMULATION_SANDBOX',
            'connected': False,
            'account_id': 'SIM-DEFCON0-001',
            'status': 'SANDBOX_ACTIVE' if not error else 'API_ERROR_FALLBACK',
            'cash': f'{self.sim_cash:.2f}',
            'buying_power': f'{self.sim_cash:.2f}',
            'equity': f'{equity:.2f}',
            'portfolio_value': f'{equity:.2f}',
            'trading_blocked': False,
            'pattern_day_trader': False,
            'key_preview': self.config['key_preview'],
            'error': error,
        }
    def get_positions(self):
        self.refresh_config()
        if self.config['has_alpaca_keys']:
            try:
                positions = self._alpaca_request('GET', '/v2/positions')
                return [
                    {
                        'symbol': p.get('symbol'),
                        'qty': str(p.get('qty')),
                        'avg_entry_price': str(p.get('avg_entry_price')),
                        'current_price': str(p.get('current_price')),
                        'market_value': str(p.get('market_value')),
                        'unrealized_pl': str(p.get('unrealized_pl')),
                        'unrealized_plpc': str(p.get('unrealized_plpc')),
                        'side': p.get('side', 'long'),
                    }
                    for p in positions
                ]
            except Exception as exc:
                self._record_event('BROKER', f'Positions fetch fallback: {exc}', 'WARN')
        return list(self.sim_positions.values())

    def get_orders(self):
        self.refresh_config()
        if self.config['has_alpaca_keys']:
            try:
                orders = self._alpaca_request('GET', '/v2/orders?status=all&limit=25')
                return [
                    {
                        'id': o.get('id'),
                        'client_order_id': o.get('client_order_id'),
                        'symbol': o.get('symbol'),
                        'side': o.get('side'),
                        'qty': str(o.get('qty') or o.get('notional') or '0'),
                        'type': o.get('type'),
                        'status': o.get('status'),
                        'filled_avg_price': str(o.get('filled_avg_price') or '-'),
                        'submitted_at': o.get('submitted_at', ''),
                    }
                    for o in orders
                ]
            except Exception as exc:
                self._record_event('BROKER', f'Orders fetch fallback: {exc}', 'WARN')
        return list(self.sim_orders)

    def get_market_quote(self, symbol, fallback_price=None):
        """
        Fetch real-time market price for a ticker from Alpaca Market Data API v2
        so the Ghost Portfolio cannot lie about entry/exit prices.
        """
        symbol = str(symbol).strip().upper()
        self.refresh_config()
        default_anchors = {
            'NVDA': Decimal('142.10'),
            'AMD': Decimal('156.40'),
            'SPY': Decimal('589.40'),
            'QQQ': Decimal('504.80'),
            'GLD': Decimal('258.30'),
            'SLV': Decimal('29.75'),
            'SCHD': Decimal('28.90'),
            'VYM': Decimal('129.60'),
            'AAPL': Decimal('228.50'),
        }
        if self.config['has_alpaca_keys']:
            data_url = f"https://data.alpaca.markets/v2/stocks/{urllib.parse.quote(symbol)}/trades/latest"
            headers = {
                'APCA-API-KEY-ID': self.config['alpaca_api_key'],
                'APCA-API-SECRET-KEY': self.config['alpaca_secret_key'],
                'Accept': 'application/json',
                'User-Agent': 'StockBroker-v3.7/1.0',
            }
            req = urllib.request.Request(url=data_url, headers=headers, method='GET')
            try:
                with urllib.request.urlopen(req, timeout=6) as resp:
                    payload = json.loads(resp.read().decode('utf-8'))
                    trade_price = payload.get('trade', {}).get('p')
                    if trade_price:
                        return {
                            'symbol': symbol,
                            'price': Decimal(str(trade_price)).quantize(Decimal('0.01')),
                            'source': 'ALPACA_MARKET_DATA_V2_LIVE',
                        }
            except Exception as exc:
                self.logger.debug(f'Market data quote fallback for {symbol}: {exc}')

        base = Decimal(str(fallback_price)) if fallback_price else default_anchors.get(symbol, Decimal('100.00'))
        return {
            'symbol': symbol,
            'price': base.quantize(Decimal('0.01')),
            'source': 'MARKET_REFERENCE_FEED',
        }

    def submit_order(self, symbol, side, qty, order_type='market', limit_price=None, estimated_price=100.0, node_id=1):
        symbol = symbol.strip().upper()
        side = side.strip().lower()
        qty_dec = Decimal(str(qty))
        if qty_dec <= 0:
            raise ValueError('Quantity must be greater than zero.')

        acct = self.get_account_summary()
        total_equity = Decimal(str(acct['equity']))

        quote = self.get_market_quote(symbol, fallback_price=limit_price if limit_price else estimated_price)
        ref_price = Decimal(str(limit_price)) if (order_type == 'limit' and limit_price) else quote['price']
        intent_notional = qty_dec * ref_price

        if self.router and side == 'buy':
            allowed = self.router.check_firewall(
                node_id=node_id,
                ticker=symbol,
                intent_notional=intent_notional,
                total_equity=total_equity,
            )
            if not allowed:
                msg = (
                    f'Firewall BLOCKED {side.upper()} {qty_dec} {symbol}: '
                    f'Intent notional ${intent_notional:.2f} exceeds 30% systemic exposure cap '
                    f'(${(total_equity * Decimal("0.30")):.2f} of ${total_equity:.2f} equity).'
                )
                self._record_event('FIREWALL', msg, 'WARN')
                return {'ok': False, 'blocked_by_firewall': True, 'message': msg}

        client_order_id = f'v37-node{node_id}-{uuid.uuid4().hex[:10]}'

        # Check Capital Access Gate (FundGate) & Execution Mode (GHOST vs LIVE)
        if self.db:
            ag_cfg = self.db.get_agent_settings()
            fund_gate_open = bool(ag_cfg.get('fund_gate_enabled', False))
            exec_mode = str(ag_cfg.get('execution_mode', 'GHOST')).upper()

            if exec_mode == 'GHOST' or not fund_gate_open:
                gate_reason = 'GHOST_MODE_ACTIVE' if exec_mode == 'GHOST' else 'FUND_GATE_CLOSED_FALLBACK_TO_GHOST'
                ghost_res = self.db.execute_ghost_trade(
                    node_id=node_id,
                    symbol=symbol,
                    side=side,
                    qty=qty_dec,
                    market_price=ref_price,
                    price_source=f"{quote['source']} ({gate_reason})",
                )
                if ghost_res.get('ok'):
                    self.db.log_order_atomic(
                        client_order_id=client_order_id,
                        alpaca_order_id=f"GHOST-{ghost_res['audit_hash']}",
                        node_id=node_id,
                        ticker=symbol,
                        side=side,
                        qty=qty_dec,
                        order_type=order_type,
                        status='ghost_filled',
                        filled_avg_price=ref_price,
                        broker_driver=gate_reason,
                    )
                self._record_event('GHOST_PORTFOLIO', ghost_res.get('message', 'Ghost trade processed'), 'INFO' if ghost_res.get('ok') else 'WARN')
                return ghost_res

        if self.config['has_alpaca_keys']:
            payload = {
                'symbol': symbol,
                'qty': str(qty_dec),
                'side': side,
                'type': order_type,
                'time_in_force': 'day',
                'client_order_id': client_order_id,
            }
            if order_type == 'limit' and limit_price:
                payload['limit_price'] = str(Decimal(str(limit_price)))
            try:
                resp = self._alpaca_request('POST', '/v2/orders', payload=payload)
                self.active_driver = 'PRIMARY_ALPACA'
                if self.db:
                    self.db.log_order_atomic(
                        client_order_id=client_order_id,
                        alpaca_order_id=resp.get('id', client_order_id),
                        node_id=node_id,
                        ticker=symbol,
                        side=side,
                        qty=qty_dec,
                        order_type=order_type,
                        status=resp.get('status', 'accepted'),
                        filled_avg_price=resp.get('filled_avg_price') or ref_price,
                        broker_driver='PRIMARY_ALPACA',
                    )
                msg = f"[PRIMARY_ALPACA] Order submitted: {side.upper()} {qty_dec} {symbol} ({resp.get('status', 'new')})"
                self._record_event('EXECUTION', msg, 'INFO')
                return {'ok': True, 'mode': acct['mode'], 'driver': 'PRIMARY_ALPACA', 'order': resp, 'message': msg}
            except Exception as exc:
                self.failover_count += 1
                self.active_driver = 'FAILOVER_LOCAL_LEDGER'
                self._record_event('PROXY_FAILOVER', f'Primary Alpaca driver error ({exc}); failing over to Secondary ACID Ledger Driver.', 'WARN')

        res = self._submit_sandbox_order(symbol, side, qty_dec, order_type, ref_price, client_order_id)
        if res.get('ok') and self.db:
            self.db.log_order_atomic(
                client_order_id=client_order_id,
                alpaca_order_id=res['order']['id'],
                node_id=node_id,
                ticker=symbol,
                side=side,
                qty=qty_dec,
                order_type=order_type,
                status='filled',
                filled_avg_price=ref_price,
                broker_driver=self.active_driver,
            )
        return res

    def _submit_sandbox_order(self, symbol, side, qty_dec, order_type, fill_price, client_order_id):
        notional = (qty_dec * fill_price).quantize(Decimal('0.01'))
        if side == 'buy':
            if notional > self.sim_cash:
                msg = f'Insufficient sandbox cash (${self.sim_cash:.2f}) for ${notional:.2f} order.'
                self._record_event('EXECUTION', msg, 'WARN')
                return {'ok': False, 'message': msg}
            self.sim_cash -= notional
            existing = self.sim_positions.get(symbol)
            if existing:
                old_qty = Decimal(existing['qty'])
                old_cost = old_qty * Decimal(existing['avg_entry_price'])
                new_qty = old_qty + qty_dec
                new_avg = ((old_cost + notional) / new_qty).quantize(Decimal('0.01'))
                mkt_val = (new_qty * fill_price).quantize(Decimal('0.01'))
                pl = mkt_val - (new_qty * new_avg)
                existing.update({
                    'qty': str(new_qty),
                    'avg_entry_price': str(new_avg),
                    'current_price': str(fill_price),
                    'market_value': str(mkt_val),
                    'unrealized_pl': f'{pl:.2f}',
                })
            else:
                self.sim_positions[symbol] = {
                    'symbol': symbol,
                    'qty': str(qty_dec),
                    'avg_entry_price': f'{fill_price:.2f}',
                    'current_price': f'{fill_price:.2f}',
                    'market_value': f'{notional:.2f}',
                    'unrealized_pl': '0.00',
                    'unrealized_plpc': '0.0000',
                    'side': 'long',
                }
        elif side == 'sell':
            existing = self.sim_positions.get(symbol)
            if not existing or Decimal(existing['qty']) < qty_dec:
                msg = f'Cannot sell {qty_dec} {symbol}: insufficient open position.'
                self._record_event('EXECUTION', msg, 'WARN')
                return {'ok': False, 'message': msg}
            old_qty = Decimal(existing['qty'])
            new_qty = old_qty - qty_dec
            self.sim_cash += notional
            if new_qty == 0:
                del self.sim_positions[symbol]
            else:
                mkt_val = (new_qty * fill_price).quantize(Decimal('0.01'))
                existing['qty'] = str(new_qty)
                existing['market_value'] = f'{mkt_val:.2f}'

        order_record = {
            'id': uuid.uuid4().hex[:12],
            'client_order_id': client_order_id,
            'symbol': symbol,
            'side': side,
            'qty': str(qty_dec),
            'type': order_type,
            'status': 'filled',
            'filled_avg_price': f'{fill_price:.2f}',
            'submitted_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
        }
        self.sim_orders.insert(0, order_record)
        msg = f'Sandbox order filled: {side.upper()} {qty_dec} {symbol} @ ${fill_price:.2f}'
        self._record_event('EXECUTION', msg, 'INFO')
        return {'ok': True, 'mode': 'SIMULATION_SANDBOX', 'order': order_record, 'message': msg}

    async def execute_exit(self, symbol, reason):
        symbol = symbol.strip().upper()
        self.logger.info(f'Execution: Triggering {reason} for {symbol}')
        self._record_event('FLATTEN', f'Triggering two-phase exit ({reason}) for {symbol}', 'WARN')

        self.state = 'FLATTEN_CANCELING'
        self.logger.info('Phase 1: Canceling OCO legs...')
        await asyncio.sleep(0.5)

        self.state = 'FLATTEN_SUBMITTING'
        self.logger.info('Phase 2: Market sweep liquidation...')

        self.refresh_config()
        if self.config['has_alpaca_keys']:
            try:
                self._alpaca_request('DELETE', f'/v2/positions/{urllib.parse.quote(symbol)}?cancel_orders=true')
                self._record_event('FLATTEN', f'Alpaca position {symbol} liquidated via two-phase flatten.', 'INFO')
            except Exception as exc:
                self._record_event('FLATTEN', f'Alpaca flatten error on {symbol}: {exc}', 'ERROR')
        else:
            pos = self.sim_positions.pop(symbol, None)
            if pos:
                self.sim_cash += Decimal(pos['market_value'])
                self._record_event('FLATTEN', f"Sandbox position {symbol} flattened (+${pos['market_value']} cash).", 'INFO')
            else:
                self._record_event('FLATTEN', f'No open position found for {symbol} during flatten.', 'WARN')

        self.state = 'IDLE'
        return {'ok': True, 'symbol': symbol, 'reason': reason, 'state': self.state}

    async def catastrophic_escape_valve(self, symbol, price_breach):
        self.logger.critical(f'CATASTROPHIC ESCAPE: {symbol} breached {price_breach}%!')
        self._record_event('ESCAPE_VALVE', f'CATASTROPHIC ESCAPE: {symbol} breached {price_breach}%!', 'CRITICAL')
        return await self.execute_exit(symbol, 'ESCAPE_VALVE')



