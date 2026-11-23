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
        self.pulse_count = self._load_initial_pulse_count()
        self.last_decision_summary = 'Autonomous Agent initialized and observing market.'
        self.reasoning_stream = []
        self._stop_event = threading.Event()
        self._thread = None

    def _load_initial_pulse_count(self):
        try:
            logs = self.db.get_market_pulse_audit_log(limit=1)
            if logs:
                return int(logs[0].get('pulse_number', 0))
        except Exception:
            pass
        return 0

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

    def run_single_pulse(self, target_node_id=None, override_market_price=None, force_close=False):
        """Execute one autonomous observation, mark-to-market, and trade decision cycle."""
        self.pulse_count += 1
        self.db.record_heartbeat('AUTONOMOUS_AGENT_BRAIN', os.getpid(), 'PULSING')
        nitro_res = self.db.consume_pending_nitro_injection()
        if Decimal(str(nitro_res.get('consumed', '0.00'))) > Decimal('0.00'):
            self._log_reasoning(0, 'NITRO_VAULT', 'NITRO_SUPERCHARGE', nitro_res['message'])
        scale_res = self.db.scale_fleet_automatically()
        if scale_res.get('consolidated_count', 0) > 0:
            self._log_reasoning(
                0,
                'ADAPTIVE_FLEET',
                'MICRO_SEED_CONSOLIDATED',
                f"Adaptive Fleet Governor [{scale_res.get('scale_tier')}]: Consolidated capital into 1 High-Conviction Sniper Node (${scale_res.get('per_node_capital')}) until equity crosses $200.00 branching threshold.",
            )
        elif scale_res.get('spawned_count', 0) > 0:
            sp_str = ', '.join(f"#{s['node_id']} ({s['ticker']})" for s in scale_res.get('spawned', []))
            self._log_reasoning(
                0,
                'ADAPTIVE_FLEET',
                'AUTO_SCALE_SPAWN',
                f"Adaptive Fleet Governor [{scale_res.get('scale_tier')}] spawned {scale_res['spawned_count']} new Node(s) @ ${scale_res.get('per_node_capital')}/node: {sp_str}",
            )
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
        return self._execute_node_decision(
            node,
            node_id,
            ticker,
            market_price,
            price_source,
            existing_ghost,
            mode_tag,
            fund_gate_open,
            force_close=bool(force_close or override_market_price is not None),
        )


    def _compute_fractional_qty(self, node, market_price, concentrated=False):
        """
        Compute fractional share quantity sized honestly to available Ghost/Node capital.
        Automatically compounds each node's accumulated drip_bucket into its active trade size
        so every winning trade immediately scales the next position (5x $100 Tandem Hybrid Mode).
        """
        settings = self.db.get_agent_settings()
        avail_cash = Decimal(str(settings.get('ghost_cash', '500.00')))
        node_cap = Decimal(str(node['active_capital'])) + Decimal(str(node.get('drip_bucket', '0.00')))
        alloc_factor = Decimal('0.90') if int(node['tier']) == 0 else Decimal('0.45')
        desired = (node_cap * alloc_factor).quantize(Decimal('0.01'))
        cap_ratio = Decimal('0.80') if (concentrated and avail_cash <= Decimal('100.00')) else Decimal('0.25')
        max_per_trade = max(desired, (avail_cash * cap_ratio).quantize(Decimal('0.01')))
        target_notional = min(desired, max_per_trade, avail_cash)
        if concentrated and avail_cash <= Decimal('100.00'):
            target_notional = min((avail_cash * Decimal('0.80')).quantize(Decimal('0.01')), avail_cash)
        if target_notional < Decimal('1.00') and avail_cash >= Decimal('1.00'):
            target_notional = min(Decimal('20.00') if concentrated else Decimal('5.00'), avail_cash)
        # Self-Sustaining Efficiency Gate: refuse sub-threshold dust trades when node is sized at $100+
        min_efficient_notional = Decimal('10.00') if node_cap >= Decimal('100.00') else Decimal('5.00')
        if target_notional < min_efficient_notional or market_price <= Decimal('0.00'):
            return Decimal('0.0000')
        return max(Decimal('0.0001'), (target_notional / market_price).quantize(Decimal('0.0001')))

    def _record_pulse_audit(self, node_id, ticker, market_price, price_source, ind, pos_state, action, qty='0.0000', realized_pnl='0.00', reason=''):
        ghost_eq = self.db.get_ghost_portfolio_summary()['total_equity']
        self.db.log_market_pulse_audit(
            pulse_number=self.pulse_count,
            node_id=node_id,
            symbol=ticker,
            market_price=market_price,
            price_source=price_source,
            rsi=ind['rsi'],
            sma=ind['sma'],
            position_state=pos_state,
            action_taken=action,
            qty=qty,
            realized_pnl=realized_pnl,
            ghost_equity_after=ghost_eq,
            reason=reason,
        )

    def _execute_node_decision(self, node, node_id, ticker, market_price, price_source, existing_ghost, mode_tag, fund_gate_open, force_close=False):
        eval_res = self.service.strategy.evaluate_position_action(
            symbol=ticker,
            current_price=market_price,
            existing_position=existing_ghost,
            force_close=force_close,
        )
        ind = eval_res['indicators']

        if not existing_ghost:
            if not eval_res['should_buy']:
                self._record_pulse_audit(node_id, ticker, market_price, price_source, ind, 'FLAT', eval_res['signal'], reason=eval_res['reason'])
                self._log_reasoning(node_id, ticker, eval_res['signal'], eval_res['reason'])
                return {'ok': True, 'pulse': self.pulse_count, 'node_id': node_id, 'ticker': ticker, 'action': eval_res['signal'], 'message': self.last_decision_summary}

            qty = self._compute_fractional_qty(node, market_price, concentrated=True)
            if qty <= Decimal('0.0000'):
                msg = f'Insufficient Ghost cash to open new {ticker} position.'
                self._record_pulse_audit(node_id, ticker, market_price, price_source, ind, 'FLAT_LOW_CASH', 'HOLD_CASH_RESERVE', reason=msg)
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
            detail = f"Invested {qty} {ticker} @ ${market_price:.2f} [{price_source}] | RSI {ind['rsi']} | SMA ${ind['sma']:.2f}"
            self._record_pulse_audit(node_id, ticker, market_price, price_source, ind, f'OPENED @ ${market_price:.2f}', action, qty=f'{qty:.4f}', reason=detail)
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
        pos_state_str = f"INVESTED ({qty_open} @ ${avg_entry:.2f} | {eval_res.get('pnl_pct', 0.0):.2f}%)"

        if not eval_res['should_sell']:
            action = f"HOLD_TREND ({mode_tag})"
            detail = eval_res['reason']
            self._record_pulse_audit(node_id, ticker, market_price, price_source, ind, pos_state_str, action, qty=f'{qty_open:.4f}', reason=detail)
            self._log_reasoning(node_id, ticker, action, detail)
            return {
                'ok': True,
                'pulse': self.pulse_count,
                'node_id': node_id,
                'ticker': ticker,
                'action': action,
                'market_price': f'{market_price:.2f}',
                'price_source': price_source,
                'realized_pnl': '0.00',
                'message': self.last_decision_summary,
            }

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

        action = f"AUTO_SELL_CLOSE [{eval_res['signal']}] ({mode_tag})"
        detail = (
            f"Sold {qty_open} {ticker} @ ${market_price:.2f} (Entry ${avg_entry:.2f}) [{price_source}] "
            f"| Realized P&L: ${realized_dec:.2f}{drip_info} | {eval_res['reason']}"
        )
        self._record_pulse_audit(node_id, ticker, market_price, price_source, ind, pos_state_str, action, qty=f'{qty_open:.4f}', realized_pnl=f'{realized_dec:.2f}', reason=detail)
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
                self.db.update_agent_settings(execution_mode=req_mode, autonomous_loop_enabled=True)

        self.db.scale_fleet_automatically()
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
        mode_tag = 'LIVE_BROKER' if (exec_mode == 'LIVE' and fund_gate_open) else 'GHOST_PORTFOLIO'

        deployed = []
        for node in nodes:
            node_id = int(node['node_id'])
            ticker = node['assigned_ticker']
            quote = self.execution.get_market_quote(ticker)
            market_price = quote['price']

            if exec_mode == 'GHOST':
                if ticker in open_ghost_map:
                    marked = self.db.mark_ghost_position_to_market(ticker, market_price)
                    self.pulse_count += 1
                    ind = self.service.strategy.compute_indicators(ticker, market_price)
                    qty_open = marked['qty'] if marked else open_ghost_map[ticker]['qty']
                    avg_entry = marked['avg_entry_price'] if marked else open_ghost_map[ticker]['avg_entry_price']
                    unrl_pct = marked['unrealized_pnl_pct'] if marked else open_ghost_map[ticker].get('unrealized_pnl_pct', '0.00')
                    detail = (
                        f"Active Ghost Fleet Marked-to-Market: Holding {qty_open} {ticker} @ ${market_price:.2f} "
                        f"(Entry ${avg_entry} | MTM {unrl_pct}%) [{quote['source']}] [DIAG: ACTIVE_FLEET_SYNC]"
                    )
                    self._record_pulse_audit(
                        node_id=node_id,
                        ticker=ticker,
                        market_price=market_price,
                        price_source=quote['source'],
                        ind=ind,
                        pos_state=f'INVESTED ({qty_open} @ ${avg_entry})',
                        action=f'FLEET_SYNC ({mode_tag})',
                        qty=str(qty_open),
                        reason=detail,
                    )
                    self._log_reasoning(node_id, ticker, f'FLEET_SYNC ({mode_tag})', detail)
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
            ind = self.service.strategy.compute_indicators(ticker, market_price)
            detail = (
                f"Fleet Initial Investment: Bought {qty} {ticker} @ ${market_price:.2f} [{quote['source']}] "
                f"| RSI {ind['rsi']} | SMA ${ind['sma']:.2f} | Spread {ind.get('sma_spread_pct', 0.0):+.2f}% [DIAG: INITIAL_DEPLOY]"
            )
            self._record_pulse_audit(
                node_id=node_id,
                ticker=ticker,
                market_price=market_price,
                price_source=quote['source'],
                ind=ind,
                pos_state=f'OPENED @ ${market_price:.2f}',
                action=f'START_INVESTING ({mode_tag})',
                qty=f'{qty:.4f}',
                reason=detail,
            )
            self._log_reasoning(node_id, ticker, f'START_INVESTING ({mode_tag})', detail)
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
