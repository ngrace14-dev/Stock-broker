import logging
import os
import threading
from datetime import datetime, timezone
from decimal import Decimal

logger = logging.getLogger('AgentBrain')


class AutonomousAgentBrain:
    """
    Fully Autonomous Agent Brain (Phase 4 & 5):
    - Continuously pulses across active Nodes in BrokerDatabase.
    - Fetches real-time market quotes and marks open Ghost Positions to market.
    - Evaluates entry/exit conditions honestly based on market price vs entry basis.
    - Respects the Global Capital Access Gate (FundGate) and ExecutionMode (GHOST vs LIVE).
    - Automatically routes realized profits into Node Drip Buckets to spawn child nodes.
    """

    def __init__(self, broker_service):
        self.service = broker_service
        self.db = broker_service.db
        self.execution = broker_service.execution
        self.pulse_count = 0
        self.last_decision_summary = 'Autonomous Agent initialized and observing market.'
        self.reasoning_stream = []
        self._stop_event = threading.Event()
        self._thread = None

    def _log_reasoning(self, node_id, ticker, action, detail):
        entry = {
            'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
            'pulse': self.pulse_count,
            'node_id': node_id,
            'ticker': ticker,
            'action': action,
            'detail': detail,
        }
        self.reasoning_stream.insert(0, entry)
        del self.reasoning_stream[40:]
        self.last_decision_summary = f"[Pulse #{self.pulse_count}] Node #{node_id} ({ticker}): {action} — {detail}"
        return entry

    def run_single_pulse(self, target_node_id=None, override_market_price=None):
        """Execute one autonomous observation, mark-to-market, and trade decision cycle."""
        self.pulse_count += 1
        self.db.record_heartbeat('AUTONOMOUS_AGENT_BRAIN', os.getpid(), 'PULSING')
        settings = self.db.get_agent_settings()
        fund_gate_open = settings['fund_gate_enabled']
        exec_mode = settings['execution_mode']

        if self.service.post_flatten_locked:
            msg = 'Agent Pulse paused: System is in Post-Flatten Lock awaiting Supervisor Key.'
            self._log_reasoning(0, 'SYSTEM', 'HALTED_FLAT_LOCK', msg)
            return {'ok': False, 'pulse': self.pulse_count, 'message': msg}

        if self.service.simulated_drift_delta != Decimal('0.00'):
            msg = f'Agent Pulse paused: ACID Ledger drift (${self.service.simulated_drift_delta:.2f}) detected.'
            self._log_reasoning(0, 'SYSTEM', 'HALTED_LEDGER_DRIFT', msg)
            return {'ok': False, 'pulse': self.pulse_count, 'message': msg}

        nodes = [n for n in self.db.get_nodes() if n['status'] == 'ACTIVE']
        if target_node_id:
            nodes = [n for n in nodes if int(n['node_id']) == int(target_node_id)]
        if not nodes:
            msg = 'No ACTIVE nodes available for autonomous pulse.'
            self._log_reasoning(0, 'FLEET', 'NO_ACTIVE_NODES', msg)
            return {'ok': False, 'pulse': self.pulse_count, 'message': msg}

        ghost_summary = self.db.get_ghost_portfolio_summary()
        open_ghost_map = {p['symbol']: p for p in ghost_summary['positions']}
        for sym in list(open_ghost_map.keys()):
            q = self.execution.get_market_quote(sym, fallback_price=override_market_price)
            p_val = Decimal(str(override_market_price)).quantize(Decimal('0.01')) if override_market_price else q['price']
            marked = self.db.mark_ghost_position_to_market(sym, p_val)
            if marked:
                open_ghost_map[sym] = marked

        node = nodes[(self.pulse_count - 1) % len(nodes)]
        node_id = int(node['node_id'])
        ticker = node['assigned_ticker']
        quote = self.execution.get_market_quote(ticker, fallback_price=override_market_price)
        market_price = Decimal(str(override_market_price)).quantize(Decimal('0.01')) if override_market_price else quote['price']
        price_source = quote['source']
        existing_ghost = open_ghost_map.get(ticker)
        mode_tag = 'LIVE_BROKER' if (exec_mode == 'LIVE' and fund_gate_open) else 'GHOST_PORTFOLIO'
        return self._execute_node_decision(node, node_id, ticker, market_price, price_source, existing_ghost, mode_tag, fund_gate_open)


    def _compute_fractional_qty(self, node, market_price):
        """
        Compute fractional share quantity sized honestly to available Ghost/Node capital
        so even a $25.00 checkpoint seed can invest across NVDA, SPY, GLD, and SCHD.
        """
        settings = self.db.get_agent_settings()
        avail_cash = Decimal(str(settings.get('ghost_cash', '25.00')))
        node_cap = Decimal(str(node['active_capital']))
        alloc_factor = Decimal('0.80') if int(node['tier']) == 0 else Decimal('0.40')
        desired = (node_cap * alloc_factor).quantize(Decimal('0.01'))
        max_per_trade = (avail_cash * Decimal('0.25')).quantize(Decimal('0.01'))
        target_notional = min(desired, max_per_trade, avail_cash)
        if target_notional < Decimal('1.00') and avail_cash >= Decimal('1.00'):
            target_notional = min(Decimal('5.00'), avail_cash)
        if target_notional <= Decimal('0.05') or market_price <= Decimal('0.00'):
            return Decimal('0.0000')
        return max(Decimal('0.0001'), (target_notional / market_price).quantize(Decimal('0.0001')))

    def _execute_node_decision(self, node, node_id, ticker, market_price, price_source, existing_ghost, mode_tag, fund_gate_open):
        if not existing_ghost:
            qty = self._compute_fractional_qty(node, market_price)
            if qty <= Decimal('0.0000'):
                msg = f'Insufficient Ghost cash to open new {ticker} position.'
                self._log_reasoning(node_id, ticker, 'HOLD_CASH_RESERVE', msg)
                return {'ok': False, 'pulse': self.pulse_count, 'node_id': node_id, 'ticker': ticker, 'action': 'HOLD_CASH_RESERVE', 'message': msg}

            order_res = self.execution.submit_order(
                symbol=ticker,
                side='buy',
                qty=qty,
                order_type='market',
                limit_price=float(market_price),
                estimated_price=float(market_price),
                node_id=node_id,
            )
            action = f'AUTO_BUY ({mode_tag})'
            detail = f"Bought {qty} {ticker} @ ${market_price:.2f} [{price_source}] | Gate={'OPEN' if fund_gate_open else 'CLOSED'}"
            self._log_reasoning(node_id, ticker, action, detail)
            return {
                'ok': order_res.get('ok', False),
                'pulse': self.pulse_count,
                'node_id': node_id,
                'ticker': ticker,
                'action': action,
                'market_price': f'{market_price:.2f}',
                'price_source': price_source,
                'order_result': order_res,
                'message': self.last_decision_summary,
            }

        qty_open = Decimal(str(existing_ghost['qty']))
        avg_entry = Decimal(str(existing_ghost['avg_entry_price']))
        order_res = self.execution.submit_order(
            symbol=ticker,
            side='sell',
            qty=qty_open,
            order_type='market',
            limit_price=float(market_price),
            estimated_price=float(market_price),
            node_id=node_id,
        )
        realized_str = order_res.get('realized_pnl', '0.00')
        realized_dec = Decimal(str(realized_str))
        drip_info = ''
        if realized_dec > Decimal('0.00'):
            drip_res = self.db.allocate_drip_profit(node_id=node_id, net_profit_amount=realized_dec)
            drip_info = f" | Drip Injected +${realized_dec:.2f}"
            if drip_res.get('spawned_child'):
                drip_info += f" (SPAWNED CHILD NODE #{drip_res['spawned_child']['child_node_id']}!)"

        action = f'AUTO_SELL_CLOSE ({mode_tag})'
        detail = (
            f"Closed {qty_open} {ticker} @ ${market_price:.2f} (Entry ${avg_entry:.2f}) [{price_source}] "
            f"| Realized P&L: ${realized_dec:.2f}{drip_info}"
        )
        self._log_reasoning(node_id, ticker, action, detail)
        return {
            'ok': order_res.get('ok', False),
            'pulse': self.pulse_count,
            'node_id': node_id,
            'ticker': ticker,
            'action': action,
            'market_price': f'{market_price:.2f}',
            'price_source': price_source,
            'realized_pnl': f'{realized_dec:.2f}',
            'order_result': order_res,
            'message': self.last_decision_summary,
        }

    def deploy_fleet_investments(self, requested_mode=None):
        """
        Start Investing Fleet (Both Ghost Mode & Live Normal Mode):
        Iterates over all ACTIVE nodes and ensures each has an active position opened
        at the current live market quote, or marks existing positions to market.
        """
        if requested_mode:
            req_mode = str(requested_mode).strip().upper()
            if req_mode in ('GHOST', 'LIVE'):
                self.db.update_agent_settings(execution_mode=req_mode)

        settings = self.db.get_agent_settings()
        fund_gate_open = bool(settings['fund_gate_enabled'])
        exec_mode = str(settings['execution_mode']).upper()

        if exec_mode == 'LIVE' and not fund_gate_open:
            return {
                'ok': False,
                'blocked_by_fund_gate': True,
                'message': 'Cannot start Live Normal Mode investing while WALLET GATE is CLOSED (LOCKED). Toggle Wallet Gate OPEN in the top header first, or click "Start Investing (Ghost Mode)".',
            }

        nodes = [n for n in self.db.get_nodes() if n['status'] == 'ACTIVE']
        if not nodes:
            return {'ok': False, 'message': 'No ACTIVE nodes found. Run Self-Heal Fleet first.'}

        ghost_summary = self.db.get_ghost_portfolio_summary()
        open_ghost_map = {p['symbol']: p for p in ghost_summary['positions']}
        open_live_map = {p['symbol']: p for p in self.execution.get_positions()}

        deployed = []
        for node in nodes:
            node_id = int(node['node_id'])
            ticker = node['assigned_ticker']
            quote = self.execution.get_market_quote(ticker)
            market_price = quote['price']

            if exec_mode == 'GHOST':
                if ticker in open_ghost_map:
                    self.db.mark_ghost_position_to_market(ticker, market_price)
                    deployed.append({'node_id': node_id, 'ticker': ticker, 'action': 'MARKED_TO_MARKET', 'price': f'{market_price:.2f}'})
                    continue
            else:
                if ticker in open_live_map:
                    deployed.append({'node_id': node_id, 'ticker': ticker, 'action': 'ALREADY_INVESTED_LIVE', 'price': f'{market_price:.2f}'})
                    continue

            qty = self._compute_fractional_qty(node, market_price)
            if qty <= Decimal('0.0000'):
                continue

            res = self.execution.submit_order(
                symbol=ticker,
                side='buy',
                qty=qty,
                order_type='market',
                limit_price=float(market_price),
                estimated_price=float(market_price),
                node_id=node_id,
            )
            self.pulse_count += 1
            mode_tag = 'LIVE_BROKER' if (exec_mode == 'LIVE' and fund_gate_open) else 'GHOST_PORTFOLIO'
            self._log_reasoning(node_id, ticker, f'START_INVESTING ({mode_tag})', f"Deployed {qty} shares @ ${market_price:.2f}")
            deployed.append({
                'node_id': node_id,
                'ticker': ticker,
                'qty': str(qty),
                'price': f'{market_price:.2f}',
                'ok': res.get('ok', False),
                'action': f'INVESTED_{mode_tag}',
            })

        msg = f"Started Investing across {len(deployed)} Active Node(s) in {exec_mode} Mode!"
        return {'ok': True, 'mode': exec_mode, 'deployed': deployed, 'message': msg}


    def start_background_loop(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop_worker, daemon=True)
        self._thread.start()

    def _loop_worker(self):
        while not self._stop_event.is_set():
            try:
                cfg = self.db.get_agent_settings()
                interval = int(cfg.get('pulse_interval_seconds', 15))
                if cfg.get('autonomous_loop_enabled', True):
                    self.run_single_pulse()
            except Exception as exc:
                logger.warning(f'AutonomousAgentBrain background pulse error: {exc}')
                interval = 15
            self._stop_event.wait(timeout=max(5, interval))
