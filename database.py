import contextlib
import hashlib
import json
import sqlite3
import threading
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from ledger_validator import floor_cent

DB_PATH = Path(__file__).resolve().parent / 'broker_v37.db'


class BrokerDatabase:
    """ACID SQLite implementation of schema.sql (v3.7 DEFCON 0) for persistent nodes, drip buckets, ledger, and heartbeat."""

    def __init__(self, db_path=None):
        self.db_path = Path(db_path) if db_path else DB_PATH
        self._lock = threading.RLock()
        self._init_schema()

    @contextlib.contextmanager
    def _connect(self):
        conn = sqlite3.connect(str(self.db_path), timeout=15.0)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.execute('PRAGMA foreign_keys=ON;')
        try:
            yield conn
        finally:
            conn.close()

    def _init_schema(self):
        with self._lock, self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS global_ledger (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    alpaca_settled_cash TEXT NOT NULL DEFAULT '10000.00',
                    alpaca_unsettled_cash TEXT NOT NULL DEFAULT '0.00',
                    daily_settled_cash_consumed TEXT NOT NULL DEFAULT '0.00',
                    bank_savings_ready TEXT NOT NULL DEFAULT '0.00',
                    ytd_tax_escrow TEXT NOT NULL DEFAULT '0.00',
                    crystallized_irs_liability TEXT NOT NULL DEFAULT '0.00',
                    cumulative_ytd_net_pnl TEXT NOT NULL DEFAULT '0.00',
                    ytd_tax_hwm TEXT NOT NULL DEFAULT '0.00',
                    insurance_liquid_cash_buffer TEXT NOT NULL DEFAULT '0.00',
                    insurance_aristocrat_overflow_cash TEXT NOT NULL DEFAULT '0.00',
                    trading_halted INTEGER NOT NULL DEFAULT 0,
                    halt_reason TEXT,
                    last_reconciled_at TEXT
                );

                CREATE TABLE IF NOT EXISTS nodes (
                    node_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parent_node_id INTEGER,
                    division TEXT NOT NULL,
                    cadence_class TEXT NOT NULL,
                    assigned_ticker TEXT NOT NULL,
                    backup_ticker TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'ACTIVE',
                    tier INTEGER NOT NULL DEFAULT 0,
                    tier_starting_active_capital TEXT NOT NULL,
                    active_capital TEXT NOT NULL,
                    cold_storage TEXT NOT NULL,
                    drip_bucket TEXT NOT NULL DEFAULT '0.00',
                    drip_target TEXT NOT NULL DEFAULT '50.00',
                    tolls_paid_lifetime TEXT NOT NULL DEFAULT '0.00',
                    claims_drawn_lifetime TEXT NOT NULL DEFAULT '0.00',
                    round_trips_in_cycle INTEGER NOT NULL DEFAULT 0,
                    daily_starting_capital TEXT NOT NULL,
                    daily_settled_budget TEXT NOT NULL DEFAULT '0.00',
                    daily_realized_pnl TEXT NOT NULL DEFAULT '0.00',
                    consecutive_stop_outs INTEGER NOT NULL DEFAULT 0,
                    cooldown_until TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS node_positions (
                    position_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    regime TEXT NOT NULL DEFAULT 'FRACTIONAL_SW',
                    filled_qty TEXT NOT NULL,
                    filled_avg_price TEXT NOT NULL,
                    current_price TEXT NOT NULL,
                    is_open INTEGER NOT NULL DEFAULT 1,
                    opened_at TEXT NOT NULL,
                    closed_at TEXT
                );

                CREATE TABLE IF NOT EXISTS orders (
                    client_order_id TEXT PRIMARY KEY,
                    alpaca_order_id TEXT,
                    node_id INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    regime TEXT NOT NULL DEFAULT 'FRACTIONAL_SW',
                    side TEXT NOT NULL,
                    qty TEXT NOT NULL,
                    order_type TEXT NOT NULL DEFAULT 'market',
                    limit_price TEXT,
                    status TEXT NOT NULL,
                    filled_qty TEXT NOT NULL DEFAULT '0',
                    filled_avg_price TEXT NOT NULL DEFAULT '0.00',
                    broker_driver TEXT NOT NULL DEFAULT 'PRIMARY_ALPACA',
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS macro_blackouts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    event_name TEXT NOT NULL,
                    timestamp_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS system_heartbeat (
                    process_name TEXT PRIMARY KEY,
                    pid INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    last_beat_epoch REAL NOT NULL,
                    last_beat_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS agent_settings (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    fund_gate_enabled INTEGER NOT NULL DEFAULT 0,
                    execution_mode TEXT NOT NULL DEFAULT 'GHOST',
                    autonomous_loop_enabled INTEGER NOT NULL DEFAULT 1,
                    pulse_interval_seconds INTEGER NOT NULL DEFAULT 15,
                    ghost_starting_cash TEXT NOT NULL DEFAULT '10000.00',
                    ghost_cash TEXT NOT NULL DEFAULT '10000.00',
                    ghost_realized_pnl TEXT NOT NULL DEFAULT '0.00',
                    ghost_wins INTEGER NOT NULL DEFAULT 0,
                    ghost_losses INTEGER NOT NULL DEFAULT 0,
                    last_pulse_utc TEXT
                );

                CREATE TABLE IF NOT EXISTS ghost_positions (
                    symbol TEXT PRIMARY KEY,
                    node_id INTEGER NOT NULL,
                    qty TEXT NOT NULL,
                    avg_entry_price TEXT NOT NULL,
                    current_market_price TEXT NOT NULL,
                    market_value TEXT NOT NULL,
                    unrealized_pnl TEXT NOT NULL DEFAULT '0.00',
                    unrealized_pnl_pct TEXT NOT NULL DEFAULT '0.00',
                    opened_at_utc TEXT NOT NULL,
                    last_marked_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS ghost_trades (
                    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    qty TEXT NOT NULL,
                    market_price TEXT NOT NULL,
                    notional TEXT NOT NULL,
                    realized_pnl TEXT NOT NULL DEFAULT '0.00',
                    price_source TEXT NOT NULL,
                    prev_hash TEXT NOT NULL,
                    audit_hash TEXT NOT NULL,
                    executed_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS market_pulse_audit (
                    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pulse_number INTEGER NOT NULL,
                    timestamp_utc TEXT NOT NULL,
                    node_id INTEGER NOT NULL,
                    symbol TEXT NOT NULL,
                    market_price TEXT NOT NULL,
                    price_source TEXT NOT NULL,
                    rsi TEXT NOT NULL DEFAULT '50.0',
                    sma TEXT NOT NULL DEFAULT '0.00',
                    position_state TEXT NOT NULL,
                    action_taken TEXT NOT NULL,
                    qty TEXT NOT NULL DEFAULT '0.0000',
                    realized_pnl TEXT NOT NULL DEFAULT '0.00',
                    ghost_equity_after TEXT NOT NULL DEFAULT '0.00',
                    reason TEXT NOT NULL
                );
                """
            )
            conn.commit()
        self._ensure_nitro_columns()
        self._seed_defaults()

    def _ensure_nitro_columns(self):
        with self._lock, self._connect() as conn:
            cols = {r['name'] for r in conn.execute('PRAGMA table_info(global_ledger)').fetchall()}
            if 'pending_nitro_injection' not in cols:
                conn.execute("ALTER TABLE global_ledger ADD COLUMN pending_nitro_injection TEXT NOT NULL DEFAULT '0.00'")
            if 'total_nitro_injected' not in cols:
                conn.execute("ALTER TABLE global_ledger ADD COLUMN total_nitro_injected TEXT NOT NULL DEFAULT '0.00'")
            conn.commit()

    def _seed_defaults(self):
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        with self._lock, self._connect() as conn:
            row = conn.execute('SELECT id FROM global_ledger WHERE id = 1').fetchone()
            if not row:
                conn.execute(
                    """
                    INSERT INTO global_ledger (
                        id, alpaca_settled_cash, alpaca_unsettled_cash, bank_savings_ready,
                        ytd_tax_escrow, insurance_liquid_cash_buffer, last_reconciled_at
                    ) VALUES (1, '10000.00', '0.00', '500.00', '250.00', '250.00', ?)
                    """,
                    (now_utc,),
                )

            ag_row = conn.execute('SELECT id FROM agent_settings WHERE id = 1').fetchone()
            if not ag_row:
                conn.execute(
                    """
                    INSERT INTO agent_settings (
                        id, fund_gate_enabled, execution_mode, autonomous_loop_enabled,
                        pulse_interval_seconds, ghost_starting_cash, ghost_cash,
                        ghost_realized_pnl, ghost_wins, ghost_losses, last_pulse_utc
                    ) VALUES (1, 0, 'GHOST', 1, 15, '10000.00', '10000.00', '0.00', 0, 0, ?)
                    """,
                    (now_utc,),
                )

            node_count = conn.execute('SELECT COUNT(*) AS cnt FROM nodes').fetchone()['cnt']
            if node_count == 0:
                seed_nodes = [
                    ('VELOCITY_BETA', 'HIGH_VELOCITY', 'NVDA', 'AMD', 'ACTIVE', 0, '2500.00', '2500.00', '2500.00', '35.00', '50.00'),
                    ('HARDWARE_TECH', 'SWING', 'SPY', 'QQQ', 'ACTIVE', 1, '3000.00', '3000.00', '3000.00', '20.00', '50.00'),
                    ('PRECIOUS_METALS', 'MACRO_CYCLICAL', 'GLD', 'SLV', 'ACTIVE', 0, '1500.00', '1500.00', '1500.00', '45.00', '50.00'),
                    ('DIVIDEND_INSURER', 'PERMANENT_ANCHOR', 'SCHD', 'VYM', 'ACTIVE', 2, '2000.00', '2000.00', '2000.00', '15.00', '50.00'),
                ]
                for div, cad, tick, btick, st, tier, start_cap, act_cap, cold, drip, target in seed_nodes:
                    conn.execute(
                        """
                        INSERT INTO nodes (
                            division, cadence_class, assigned_ticker, backup_ticker, status, tier,
                            tier_starting_active_capital, active_capital, cold_storage, drip_bucket,
                            drip_target, daily_starting_capital, daily_settled_budget, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (div, cad, tick, btick, st, tier, start_cap, act_cap, cold, drip, target, act_cap, act_cap, now_utc),
                    )

            bo_count = conn.execute('SELECT COUNT(*) AS cnt FROM macro_blackouts').fetchone()['cnt']
            if bo_count == 0:
                json_path = Path(__file__).resolve().parent / 'macro_blackouts_2026_2027.json'
                if json_path.exists():
                    try:
                        data = json.loads(json_path.read_text(encoding='utf-8'))
                        for ev in data.get('events', []):
                            conn.execute(
                                'INSERT INTO macro_blackouts (event_type, event_name, timestamp_utc) VALUES (?, ?, ?)',
                                (ev.get('type', 'MACRO'), ev.get('name', 'Event'), ev.get('timestamp_utc', '')),
                            )
                    except Exception:
                        pass
            conn.commit()

    def record_heartbeat(self, process_name, pid, state='ACTIVE'):
        now_epoch = datetime.now(timezone.utc).timestamp()
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO system_heartbeat (process_name, pid, state, last_beat_epoch, last_beat_utc)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(process_name) DO UPDATE SET
                    pid=excluded.pid,
                    state=excluded.state,
                    last_beat_epoch=excluded.last_beat_epoch,
                    last_beat_utc=excluded.last_beat_utc
                """,
                (process_name, int(pid), state, now_epoch, now_utc),
            )
            conn.commit()
        return {'process_name': process_name, 'pid': pid, 'state': state, 'last_beat_utc': now_utc}

    def get_heartbeats(self):
        with self._lock, self._connect() as conn:
            rows = conn.execute('SELECT * FROM system_heartbeat').fetchall()
            return [dict(r) for r in rows]

    def get_nodes(self):
        with self._lock, self._connect() as conn:
            rows = conn.execute('SELECT * FROM nodes ORDER BY node_id ASC').fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d['ticker'] = d['assigned_ticker']
                d['cadence'] = d['cadence_class']
                drip = Decimal(str(d.get('drip_bucket', '0.00')))
                target = Decimal(str(d.get('drip_target', '50.00')))
                pct = min(Decimal('100.0'), (drip / target * Decimal('100.0')).quantize(Decimal('0.1'))) if target > 0 else Decimal('0.0')
                d['drip_pct'] = float(pct)
                result.append(d)
            return result

    def get_global_ledger(self):
        with self._lock, self._connect() as conn:
            row = conn.execute('SELECT * FROM global_ledger WHERE id = 1').fetchone()
            return dict(row) if row else {}

    def get_blackouts(self):
        with self._lock, self._connect() as conn:
            rows = conn.execute('SELECT event_type AS type, event_name AS name, timestamp_utc FROM macro_blackouts ORDER BY timestamp_utc ASC').fetchall()
            return [dict(r) for r in rows]

    def log_order_atomic(self, client_order_id, alpaca_order_id, node_id, ticker, side, qty, order_type, status, filled_avg_price, broker_driver='PRIMARY_ALPACA'):
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO orders (
                    client_order_id, alpaca_order_id, node_id, ticker, side, qty,
                    order_type, status, filled_qty, filled_avg_price, broker_driver, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(client_order_id),
                    str(alpaca_order_id or client_order_id),
                    int(node_id),
                    str(ticker),
                    str(side),
                    str(qty),
                    str(order_type),
                    str(status),
                    str(qty),
                    str(filled_avg_price),
                    str(broker_driver),
                    now_utc,
                ),
            )
            conn.commit()

    def get_recent_db_orders(self, limit=25):
        with self._lock, self._connect() as conn:
            rows = conn.execute('SELECT * FROM orders ORDER BY created_at DESC LIMIT ?', (int(limit),)).fetchall()
            return [
                {
                    'id': r['alpaca_order_id'],
                    'client_order_id': r['client_order_id'],
                    'node_id': r['node_id'],
                    'symbol': r['ticker'],
                    'side': r['side'],
                    'qty': r['qty'],
                    'type': r['order_type'],
                    'status': r['status'],
                    'filled_avg_price': r['filled_avg_price'],
                    'broker_driver': r['broker_driver'],
                    'submitted_at': r['created_at'],
                }
                for r in rows
            ]

    def allocate_drip_profit(self, node_id, net_profit_amount):
        """
        v3.7 Invariant Profit Split & Drip Bucket Spawner:
        50% floor_cent goes to Active Capital, remainder to Cold Storage (1:1 backed),
        and Drip Bucket accumulates toward $50.00 threshold to auto-spawn a child Node!
        """
        pool = Decimal(str(net_profit_amount)).quantize(Decimal('0.01'))
        if pool <= 0:
            raise ValueError('Drip profit amount must be positive.')

        add_active = floor_cent(pool / Decimal('2'))
        add_cold = pool - add_active
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        spawned_child = None

        with self._lock, self._connect() as conn:
            node = conn.execute('SELECT * FROM nodes WHERE node_id = ?', (int(node_id),)).fetchone()
            if not node:
                raise ValueError(f'Node {node_id} not found.')

            new_active = Decimal(str(node['active_capital'])) + add_active
            new_cold = Decimal(str(node['cold_storage'])) + add_cold
            new_drip = Decimal(str(node['drip_bucket'])) + pool
            target = Decimal(str(node['drip_target']))
            new_tier = int(node['tier'])
            if new_tier == 0 and new_active >= Decimal('50.00'):
                new_tier = 1

            if new_drip >= target:
                new_drip -= target
                child_ticker = node['backup_ticker'] or f"{node['assigned_ticker']}-B"
                cur = conn.execute(
                    """
                    INSERT INTO nodes (
                        parent_node_id, division, cadence_class, assigned_ticker, backup_ticker,
                        status, tier, tier_starting_active_capital, active_capital, cold_storage,
                        drip_bucket, drip_target, daily_starting_capital, daily_settled_budget, created_at
                    ) VALUES (?, ?, ?, ?, ?, 'ACTIVE', 0, '25.00', '25.00', '25.00', '0.00', '50.00', '25.00', '25.00', ?)
                    """,
                    (int(node_id), node['division'], node['cadence_class'], child_ticker, node['assigned_ticker'], now_utc),
                )
                spawned_child = {'child_node_id': cur.lastrowid, 'ticker': child_ticker, 'seeded_capital': '25.00'}

            conn.execute(
                """
                UPDATE nodes
                SET active_capital = ?, cold_storage = ?, drip_bucket = ?, tier = ?,
                    round_trips_in_cycle = round_trips_in_cycle + 1
                WHERE node_id = ?
                """,
                (f'{new_active:.2f}', f'{new_cold:.2f}', f'{new_drip:.2f}', new_tier, int(node_id)),
            )
            conn.commit()

        return {
            'ok': True,
            'node_id': int(node_id),
            'added_active': f'{add_active:.2f}',
            'added_cold': f'{add_cold:.2f}',
            'new_drip_bucket': f'{new_drip:.2f}',
            'spawned_child': spawned_child,
        }

    def self_heal_nodes(self):
        """Phase 2 Self-Healing: Recover any COOLDOWN, WIPED_OUT, or zero-capital nodes from Cold Storage & restore ACTIVE status."""
        healed = []
        with self._lock, self._connect() as conn:
            rows = conn.execute('SELECT * FROM nodes').fetchall()
            for r in rows:
                act = Decimal(str(r['active_capital']))
                cold = Decimal(str(r['cold_storage']))
                status = r['status']
                if status != 'ACTIVE' or act <= Decimal('0.00'):
                    restored_cap = max(act, floor_cent(cold / Decimal('2')), Decimal('25.00'))
                    conn.execute(
                        """
                        UPDATE nodes
                        SET status = 'ACTIVE', active_capital = ?, cooldown_until = NULL, consecutive_stop_outs = 0
                        WHERE node_id = ?
                        """,
                        (f'{restored_cap:.2f}', r['node_id']),
                    )
                    healed.append({'node_id': r['node_id'], 'ticker': r['assigned_ticker'], 'restored_capital': f'{restored_cap:.2f}'})
            conn.commit()
        return {'ok': True, 'healed_count': len(healed), 'healed_nodes': healed}

    def isolate_node(self, node_id):
        """Blast Radius Containment: Isolate a specific ticker node in red (PROBATION/ISOLATED) without cascading to neighboring nodes."""
        with self._lock, self._connect() as conn:
            row = conn.execute('SELECT * FROM nodes WHERE node_id = ?', (int(node_id),)).fetchone()
            if not row:
                raise ValueError(f'Node {node_id} not found.')
            new_status = 'ACTIVE' if row['status'] != 'ACTIVE' else 'WIPED_OUT'
            conn.execute('UPDATE nodes SET status = ? WHERE node_id = ?', (new_status, int(node_id)))
            conn.commit()
        return {'ok': True, 'node_id': int(node_id), 'ticker': row['assigned_ticker'], 'new_status': new_status}

    def get_agent_settings(self):
        with self._lock, self._connect() as conn:
            row = conn.execute('SELECT * FROM agent_settings WHERE id = 1').fetchone()
            if not row:
                return {
                    'fund_gate_enabled': False,
                    'execution_mode': 'GHOST',
                    'autonomous_loop_enabled': True,
                    'pulse_interval_seconds': 15,
                    'ghost_starting_cash': '10000.00',
                    'ghost_cash': '10000.00',
                    'ghost_realized_pnl': '0.00',
                    'ghost_wins': 0,
                    'ghost_losses': 0,
                    'last_pulse_utc': '',
                }
            d = dict(row)
            d['fund_gate_enabled'] = bool(d['fund_gate_enabled'])
            d['autonomous_loop_enabled'] = bool(d['autonomous_loop_enabled'])
            return d

    def update_agent_settings(self, fund_gate_enabled=None, execution_mode=None, autonomous_loop_enabled=None, pulse_interval_seconds=None):
        with self._lock, self._connect() as conn:
            cur = self.get_agent_settings()
            fg = int(bool(cur['fund_gate_enabled'] if fund_gate_enabled is None else fund_gate_enabled))
            mode = str(cur['execution_mode'] if execution_mode is None else execution_mode).strip().upper()
            if mode not in ('GHOST', 'LIVE'):
                mode = 'GHOST'
            auto_loop = int(bool(cur['autonomous_loop_enabled'] if autonomous_loop_enabled is None else autonomous_loop_enabled))
            interval = int(cur['pulse_interval_seconds'] if pulse_interval_seconds is None else pulse_interval_seconds)
            interval = max(5, min(300, interval))
            conn.execute(
                """
                UPDATE agent_settings
                SET fund_gate_enabled = ?, execution_mode = ?, autonomous_loop_enabled = ?, pulse_interval_seconds = ?
                WHERE id = 1
                """,
                (fg, mode, auto_loop, interval),
            )
            conn.commit()
        return self.get_agent_settings()

    def mark_ghost_position_to_market(self, symbol, current_market_price):
        symbol = str(symbol).strip().upper()
        price_dec = Decimal(str(current_market_price)).quantize(Decimal('0.01'))
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        with self._lock, self._connect() as conn:
            pos = conn.execute('SELECT * FROM ghost_positions WHERE symbol = ?', (symbol,)).fetchone()
            if not pos:
                return None
            qty_dec = Decimal(str(pos['qty']))
            avg_entry = Decimal(str(pos['avg_entry_price']))
            mkt_val = (qty_dec * price_dec).quantize(Decimal('0.01'))
            cost = (qty_dec * avg_entry).quantize(Decimal('0.01'))
            unrl = (mkt_val - cost).quantize(Decimal('0.01'))
            unrl_pct = ((unrl / cost) * Decimal('100.0')).quantize(Decimal('0.01')) if cost > 0 else Decimal('0.00')
            conn.execute(
                """
                UPDATE ghost_positions
                SET current_market_price = ?, market_value = ?, unrealized_pnl = ?, unrealized_pnl_pct = ?, last_marked_utc = ?
                WHERE symbol = ?
                """,
                (f'{price_dec:.2f}', f'{mkt_val:.2f}', f'{unrl:.2f}', f'{unrl_pct:.2f}', now_utc, symbol),
            )
            conn.commit()
            return {
                'symbol': symbol,
                'qty': f'{qty_dec:.4f}',
                'avg_entry_price': f'{avg_entry:.2f}',
                'current_market_price': f'{price_dec:.2f}',
                'market_value': f'{mkt_val:.2f}',
                'unrealized_pnl': f'{unrl:.2f}',
                'unrealized_pnl_pct': f'{unrl_pct:.2f}',
            }

    def get_ghost_portfolio_summary(self):
        with self._lock, self._connect() as conn:
            settings = self.get_agent_settings()
            positions = [dict(r) for r in conn.execute('SELECT * FROM ghost_positions ORDER BY symbol ASC').fetchall()]
            trades = [dict(r) for r in conn.execute('SELECT * FROM ghost_trades ORDER BY trade_id DESC LIMIT 30').fetchall()]

            start_cash = Decimal(str(settings['ghost_starting_cash']))
            cash = Decimal(str(settings['ghost_cash']))
            realized = Decimal(str(settings['ghost_realized_pnl']))
            pos_val = sum(Decimal(str(p['market_value'])) for p in positions)
            unrealized = sum(Decimal(str(p['unrealized_pnl'])) for p in positions)
            total_equity = (cash + pos_val).quantize(Decimal('0.01'))
            net_pnl = (total_equity - start_cash).quantize(Decimal('0.01'))
            return_pct = ((net_pnl / start_cash) * Decimal('100.0')).quantize(Decimal('0.01')) if start_cash > 0 else Decimal('0.00')
            wins = int(settings['ghost_wins'])
            losses = int(settings['ghost_losses'])
            total_closed = wins + losses
            win_rate = f'{(wins / total_closed * 100.0):.1f}%' if total_closed > 0 else '0.0%'

            return {
                'starting_cash': f'{start_cash:.2f}',
                'cash': f'{cash:.2f}',
                'positions_value': f'{pos_val:.2f}',
                'total_equity': f'{total_equity:.2f}',
                'realized_pnl': f'{realized:.2f}',
                'unrealized_pnl': f'{unrealized:.2f}',
                'net_pnl': f'{net_pnl:.2f}',
                'return_pct': f'{return_pct:.2f}%',
                'wins': wins,
                'losses': losses,
                'win_rate': win_rate,
                'positions': positions,
                'trades': trades,
            }

    def execute_ghost_trade(self, node_id, symbol, side, qty, market_price, price_source='ALPACA_LIVE_QUOTE'):
        symbol = str(symbol).strip().upper()
        side = str(side).strip().lower()
        qty_dec = Decimal(str(qty)).quantize(Decimal('0.0001'))
        price_dec = Decimal(str(market_price)).quantize(Decimal('0.01'))
        if qty_dec <= 0 or price_dec <= 0:
            raise ValueError('Ghost trade qty and market_price must be positive.')

        notional = (qty_dec * price_dec).quantize(Decimal('0.01'))
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        realized_pnl = Decimal('0.00')

        with self._lock, self._connect() as conn:
            settings = conn.execute('SELECT * FROM agent_settings WHERE id = 1').fetchone()
            ghost_cash = Decimal(str(settings['ghost_cash']))
            total_realized = Decimal(str(settings['ghost_realized_pnl']))
            wins = int(settings['ghost_wins'])
            losses = int(settings['ghost_losses'])
            pos = conn.execute('SELECT * FROM ghost_positions WHERE symbol = ?', (symbol,)).fetchone()

            if side == 'buy':
                if notional > ghost_cash:
                    return {
                        'ok': False,
                        'message': f'Ghost Portfolio insufficient cash (${ghost_cash:.2f}) for ${notional:.2f} {symbol} buy.',
                    }
                ghost_cash -= notional
                if pos:
                    old_qty = Decimal(str(pos['qty']))
                    old_avg = Decimal(str(pos['avg_entry_price']))
                    new_qty = old_qty + qty_dec
                    new_avg = ((old_qty * old_avg + notional) / new_qty).quantize(Decimal('0.01'))
                    mkt_val = (new_qty * price_dec).quantize(Decimal('0.01'))
                    unrl = mkt_val - (new_qty * new_avg)
                    unrl_pct = ((unrl / (new_qty * new_avg)) * Decimal('100.0')).quantize(Decimal('0.01')) if new_avg > 0 else Decimal('0.00')
                    conn.execute(
                        """
                        UPDATE ghost_positions
                        SET qty = ?, avg_entry_price = ?, current_market_price = ?,
                            market_value = ?, unrealized_pnl = ?, unrealized_pnl_pct = ?, last_marked_utc = ?
                        WHERE symbol = ?
                        """,
                        (f'{new_qty:.4f}', f'{new_avg:.2f}', f'{price_dec:.2f}', f'{mkt_val:.2f}', f'{unrl:.2f}', f'{unrl_pct:.2f}', now_utc, symbol),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO ghost_positions (
                            symbol, node_id, qty, avg_entry_price, current_market_price,
                            market_value, unrealized_pnl, unrealized_pnl_pct, opened_at_utc, last_marked_utc
                        ) VALUES (?, ?, ?, ?, ?, ?, '0.00', '0.00', ?, ?)
                        """,
                        (symbol, int(node_id), f'{qty_dec:.4f}', f'{price_dec:.2f}', f'{price_dec:.2f}', f'{notional:.2f}', now_utc, now_utc),
                    )
            elif side == 'sell':
                if not pos or Decimal(str(pos['qty'])) < qty_dec:
                    return {
                        'ok': False,
                        'message': f'Ghost Portfolio cannot sell {qty_dec} {symbol}: no sufficient open ghost position.',
                    }
                old_qty = Decimal(str(pos['qty']))
                avg_entry = Decimal(str(pos['avg_entry_price']))
                cost_basis = (qty_dec * avg_entry).quantize(Decimal('0.01'))
                realized_pnl = (notional - cost_basis).quantize(Decimal('0.01'))
                ghost_cash += notional
                total_realized += realized_pnl
                if realized_pnl > Decimal('0.00'):
                    wins += 1
                elif realized_pnl < Decimal('0.00'):
                    losses += 1

                new_qty = old_qty - qty_dec
                if new_qty <= Decimal('0.0001'):
                    conn.execute('DELETE FROM ghost_positions WHERE symbol = ?', (symbol,))
                else:
                    mkt_val = (new_qty * price_dec).quantize(Decimal('0.01'))
                    unrl = mkt_val - (new_qty * avg_entry)
                    unrl_pct = ((unrl / (new_qty * avg_entry)) * Decimal('100.0')).quantize(Decimal('0.01')) if avg_entry > 0 else Decimal('0.00')
                    conn.execute(
                        """
                        UPDATE ghost_positions
                        SET qty = ?, current_market_price = ?, market_value = ?,
                            unrealized_pnl = ?, unrealized_pnl_pct = ?, last_marked_utc = ?
                        WHERE symbol = ?
                        """,
                        (f'{new_qty:.4f}', f'{price_dec:.2f}', f'{mkt_val:.2f}', f'{unrl:.2f}', f'{unrl_pct:.2f}', now_utc, symbol),
                    )
            else:
                raise ValueError(f'Invalid side: {side}')


            last_trade = conn.execute('SELECT audit_hash FROM ghost_trades ORDER BY trade_id DESC LIMIT 1').fetchone()
            prev_hash = last_trade['audit_hash'] if last_trade else 'GENESIS_V37_DEFCON0'
            raw_record = f'{prev_hash}|{node_id}|{symbol}|{side}|{qty_dec:.4f}|{price_dec:.2f}|{notional:.2f}|{realized_pnl:.2f}|{price_source}|{now_utc}'
            audit_hash = hashlib.sha256(raw_record.encode('utf-8')).hexdigest()[:16]

            conn.execute(
                """
                INSERT INTO ghost_trades (
                    node_id, symbol, side, qty, market_price, notional,
                    realized_pnl, price_source, prev_hash, audit_hash, executed_at_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(node_id),
                    symbol,
                    side,
                    f'{qty_dec:.4f}',
                    f'{price_dec:.2f}',
                    f'{notional:.2f}',
                    f'{realized_pnl:.2f}',
                    str(price_source),
                    prev_hash,
                    audit_hash,
                    now_utc,
                ),
            )

            conn.execute(
                """
                UPDATE agent_settings
                SET ghost_cash = ?, ghost_realized_pnl = ?, ghost_wins = ?, ghost_losses = ?, last_pulse_utc = ?
                WHERE id = 1
                """,
                (f'{ghost_cash:.2f}', f'{total_realized:.2f}', wins, losses, now_utc),
            )
            conn.commit()

        return {
            'ok': True,
            'mode': 'GHOST_PORTFOLIO',
            'symbol': symbol,
            'side': side,
            'qty': f'{qty_dec:.4f}',
            'market_price': f'{price_dec:.2f}',
            'notional': f'{notional:.2f}',
            'realized_pnl': f'{realized_pnl:.2f}',
            'price_source': price_source,
            'audit_hash': audit_hash,
            'message': f'[GHOST PORTFOLIO] {side.upper()} {qty_dec} {symbol} @ ${price_dec:.2f} ({price_source}) | Realized P&L: ${realized_pnl:.2f} | SHA256: {audit_hash}',
        }

    def reset_ghost_portfolio(self, starting_cash='25.00', reset_nodes=True):
        """
        Hard-reset the Ghost Portfolio to a user-chosen starting capital (e.g., $25.00 checkpoint seed).
        Wipes all prior ghost positions, ghost trades, and test artifacts so overnight results are 100% pure.
        """
        cash = Decimal(str(starting_cash)).quantize(Decimal('0.01'))
        if cash <= Decimal('0.00'):
            raise ValueError('Starting cash must be greater than $0.00.')
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

        with self._lock, self._connect() as conn:
            conn.execute('DELETE FROM ghost_positions')
            conn.execute('DELETE FROM ghost_trades')
            conn.execute('DELETE FROM orders')
            conn.execute('DELETE FROM market_pulse_audit')
            conn.execute(
                """
                UPDATE agent_settings
                SET ghost_starting_cash = ?, ghost_cash = ?, ghost_realized_pnl = '0.00',
                    ghost_wins = 0, ghost_losses = 0, last_pulse_utc = ?
                WHERE id = 1
                """,
                (f'{cash:.2f}', f'{cash:.2f}', now_utc),
            )
            if reset_nodes:
                conn.execute('DELETE FROM nodes')
                target_nodes, per_node_cap, scale_tier = self._compute_optimal_node_topology(cash)
                blueprint = [
                    ('SATELLITE_ALPHA', 'HIGH_VELOCITY', 'NVDA', 'SOXL'),
                    ('CORE_TREND_ANCHOR', 'SWING', 'TQQQ', 'QQQ'),
                    ('SATELLITE_CRYPTO', 'CRYPTO_24_7', 'BTC/USD', 'ETH/USD'),
                    ('SATELLITE_ALPHA', 'HIGH_VELOCITY', 'TSLA', 'AMD'),
                    ('CORE_TREND_ANCHOR', 'PERMANENT_ANCHOR', 'SPY', 'GLD'),
                ]
                seed_nodes = []
                for idx in range(target_nodes):
                    div, cad, tick, btick = blueprint[idx % len(blueprint)]
                    if idx >= len(blueprint):
                        tick = f"{tick}"
                    drip_t = max(Decimal('50.00'), floor_cent(per_node_cap * Decimal('0.50')))
                    seed_nodes.append((
                        div,
                        cad,
                        tick,
                        btick,
                        'ACTIVE',
                        0,
                        f'{per_node_cap:.2f}',
                        f'{per_node_cap:.2f}',
                        f'{per_node_cap:.2f}',
                        '0.00',
                        f'{drip_t:.2f}',
                    ))
                for div, cad, tick, btick, st, tier, start_cap, act_cap, cold, drip, target in seed_nodes:
                    conn.execute(
                        """
                        INSERT INTO nodes (
                            division, cadence_class, assigned_ticker, backup_ticker, status, tier,
                            tier_starting_active_capital, active_capital, cold_storage, drip_bucket,
                            drip_target, daily_starting_capital, daily_settled_budget, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (div, cad, tick, btick, st, tier, start_cap, act_cap, cold, drip, target, act_cap, act_cap, now_utc),
                    )
            conn.commit()
        return {
            'ok': True,
            'starting_cash': f'{cash:.2f}',
            'message': f'Ghost Portfolio cleanly reset to ${cash:.2f} starting capital (0 trades, 0 open positions, pure market-price tracking).',
        }

    def inject_nitro_funds(self, amount='100.00', queue_for_pulse=False):
        """
        Nitro Fund Protocol:
        Queues or immediately deploys a monthly/ad-hoc capital injection (e.g. $100.00)
        directly into SATELLITE_ALPHA nodes' active_capital so 100% of Nitro funds are
        immediately leveraged on the next high-conviction signal instead of sitting as dead cash.
        """
        amt = Decimal(str(amount)).quantize(Decimal('0.01'))
        if amt <= Decimal('0.00'):
            raise ValueError('Nitro injection amount must be greater than $0.00.')
        with self._lock, self._connect() as conn:
            row = conn.execute('SELECT pending_nitro_injection FROM global_ledger WHERE id = 1').fetchone()
            cur_pending = Decimal(str(row['pending_nitro_injection'])) if row and row['pending_nitro_injection'] else Decimal('0.00')
            new_pending = (cur_pending + amt).quantize(Decimal('0.01'))
            conn.execute(
                'UPDATE global_ledger SET pending_nitro_injection = ? WHERE id = 1',
                (f'{new_pending:.2f}',),
            )
            conn.commit()
        if not queue_for_pulse:
            return self.consume_pending_nitro_injection()
        return {
            'ok': True,
            'queued_nitro': f'{new_pending:.2f}',
            'message': f'Queued +${amt:.2f} Nitro Fund injection for next Agent Brain pulse.',
        }

    def consume_pending_nitro_injection(self):
        with self._lock, self._connect() as conn:
            row = conn.execute(
                'SELECT pending_nitro_injection, total_nitro_injected FROM global_ledger WHERE id = 1'
            ).fetchone()
            if not row:
                return {'ok': True, 'consumed': '0.00', 'nodes_boosted': 0}
            pending = Decimal(str(row['pending_nitro_injection'] or '0.00')).quantize(Decimal('0.01'))
            if pending <= Decimal('0.00'):
                return {'ok': True, 'consumed': '0.00', 'nodes_boosted': 0}

            total_injected = (Decimal(str(row['total_nitro_injected'] or '0.00')) + pending).quantize(Decimal('0.01'))
            conn.execute(
                "UPDATE global_ledger SET pending_nitro_injection = '0.00', total_nitro_injected = ? WHERE id = 1",
                (f'{total_injected:.2f}',),
            )

            ag = conn.execute('SELECT ghost_starting_cash, ghost_cash FROM agent_settings WHERE id = 1').fetchone()
            new_start = (Decimal(str(ag['ghost_starting_cash'])) + pending).quantize(Decimal('0.01'))
            new_cash = (Decimal(str(ag['ghost_cash'])) + pending).quantize(Decimal('0.01'))
            conn.execute(
                'UPDATE agent_settings SET ghost_starting_cash = ?, ghost_cash = ? WHERE id = 1',
                (f'{new_start:.2f}', f'{new_cash:.2f}'),
            )

            sat_nodes = conn.execute(
                "SELECT node_id, active_capital FROM nodes WHERE status = 'ACTIVE' AND division IN ('SATELLITE_ALPHA', 'VELOCITY_BETA') ORDER BY node_id ASC"
            ).fetchall()
            if not sat_nodes:
                sat_nodes = conn.execute("SELECT node_id, active_capital FROM nodes WHERE status = 'ACTIVE' ORDER BY node_id ASC").fetchall()

            boost_count = len(sat_nodes)
            per_node_boost = floor_cent(pending / Decimal(str(max(1, boost_count))))
            remainder = (pending - (per_node_boost * Decimal(str(boost_count)))).quantize(Decimal('0.01'))
            boosted_ids = []
            for idx, n in enumerate(sat_nodes):
                nid = int(n['node_id'])
                add_amt = per_node_boost + (remainder if idx == 0 else Decimal('0.00'))
                new_cap = (Decimal(str(n['active_capital'])) + add_amt).quantize(Decimal('0.01'))
                conn.execute(
                    'UPDATE nodes SET active_capital = ?, daily_starting_capital = ?, daily_settled_budget = ? WHERE node_id = ?',
                    (f'{new_cap:.2f}', f'{new_cap:.2f}', f'{new_cap:.2f}', nid),
                )
                boosted_ids.append(nid)
            conn.commit()
            return {
                'ok': True,
                'consumed': f'{pending:.2f}',
                'total_nitro_injected': f'{total_injected:.2f}',
                'per_node_boost': f'{per_node_boost:.2f}',
                'nodes_boosted': boost_count,
                'boosted_node_ids': boosted_ids,
                'new_ghost_cash': f'{new_cash:.2f}',
                'message': f'[NITRO PROTOCOL] Supercharged {boost_count} Satellite Node(s) with +${pending:.2f} (+${per_node_boost:.2f}/node)!',
            }

    @staticmethod
    def _compute_optimal_node_topology(total_equity):
        """
        Infinite-Scale Adaptive Fleet Topology ($25.00 to $1,000,000,000,000+):
        - < $200.00 (e.g. $25.00 Micro-Seed): Consolidates 100% into 1 High-Conviction Sniper Node
          to eliminate transaction friction drag until equity reaches $200.00+.
        - $200.00 - $499.99: Scales linearly at 1 node per $100.00 (2 to 4 nodes).
        - $500.00 - $1,000,000,000,000+ (Trillion-Scale): Operates the 5-Node Tandem Core-Satellite
          architecture, scaling per-node capital and drip targets proportionally without ceilings.
        """
        eq = Decimal(str(total_equity)).quantize(Decimal('0.01'))
        if eq < Decimal('200.00'):
            return 1, eq, 'CONSOLIDATED_1_NODE_SNIPER'
        if eq < Decimal('500.00'):
            n_cnt = int(eq // Decimal('100.00'))
            return n_cnt, floor_cent(eq / Decimal(str(n_cnt))), f'BRANCHING_{n_cnt}_NODE_FLEET'
        n_cnt = 5
        if eq >= Decimal('1000000000.00'):
            tier_label = 'SOVEREIGN_TRILLION_5_NODE_TANDEM'
        elif eq >= Decimal('1000000.00'):
            tier_label = 'INSTITUTIONAL_MILLION_5_NODE_TANDEM'
        else:
            tier_label = 'TANDEM_5_NODE_HYBRID_FLEET'
        return n_cnt, floor_cent(eq / Decimal(str(n_cnt))), tier_label

    def scale_fleet_automatically(self):
        summary = self.get_ghost_portfolio_summary()
        total_eq = Decimal(str(summary['total_equity']))
        target_nodes, per_node_cap, scale_tier = self._compute_optimal_node_topology(total_eq)
        target_blueprint = [
            ('SATELLITE_ALPHA', 'HIGH_VELOCITY', 'NVDA', 'SOXL'),
            ('CORE_TREND_ANCHOR', 'SWING', 'TQQQ', 'QQQ'),
            ('SATELLITE_CRYPTO', 'CRYPTO_24_7', 'BTC/USD', 'ETH/USD'),
            ('SATELLITE_ALPHA', 'HIGH_VELOCITY', 'TSLA', 'AMD'),
            ('CORE_TREND_ANCHOR', 'PERMANENT_ANCHOR', 'SPY', 'GLD'),
        ]
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        spawned = []
        consolidated = []
        with self._lock, self._connect() as conn:
            active_nodes = conn.execute("SELECT * FROM nodes WHERE status = 'ACTIVE' ORDER BY node_id ASC").fetchall()
            cur_count = len(active_nodes)

            if cur_count > target_nodes and target_nodes == 1:
                primary = active_nodes[0]
                for extra in active_nodes[1:]:
                    conn.execute(
                        "UPDATE nodes SET status = 'STANDBY_CONSOLIDATED', active_capital = '0.00' WHERE node_id = ?",
                        (int(extra['node_id']),),
                    )
                    consolidated.append({'node_id': extra['node_id'], 'ticker': extra['assigned_ticker']})
                conn.execute(
                    "UPDATE nodes SET active_capital = ?, daily_starting_capital = ?, daily_settled_budget = ? WHERE node_id = ?",
                    (f'{total_eq:.2f}', f'{total_eq:.2f}', f'{total_eq:.2f}', int(primary['node_id'])),
                )
                conn.commit()
                return {
                    'ok': True,
                    'scale_tier': scale_tier,
                    'spawned_count': 0,
                    'consolidated_count': len(consolidated),
                    'consolidated': consolidated,
                    'active_count': 1,
                    'per_node_capital': f'{total_eq:.2f}',
                }

            existing_tickers = {str(n['assigned_ticker']).upper() for n in active_nodes}
            drip_t = max(Decimal('50.00'), floor_cent(per_node_cap * Decimal('0.50')))
            for div, cad, tick, btick in target_blueprint:
                if cur_count >= target_nodes:
                    break
                if tick in existing_tickers:
                    continue
                cur = conn.execute(
                    """
                    INSERT INTO nodes (
                        division, cadence_class, assigned_ticker, backup_ticker, status, tier,
                        tier_starting_active_capital, active_capital, cold_storage, drip_bucket,
                        drip_target, daily_starting_capital, daily_settled_budget, created_at
                    ) VALUES (?, ?, ?, ?, 'ACTIVE', 0, ?, ?, ?, '0.00', ?, ?, ?, ?)
                    """,
                    (
                        div,
                        cad,
                        tick,
                        btick,
                        f'{per_node_cap:.2f}',
                        f'{per_node_cap:.2f}',
                        f'{per_node_cap:.2f}',
                        f'{drip_t:.2f}',
                        f'{per_node_cap:.2f}',
                        f'{per_node_cap:.2f}',
                        now_utc,
                    ),
                )
                cur_count += 1
                existing_tickers.add(tick)
                spawned.append({'node_id': cur.lastrowid, 'ticker': tick, 'division': div})

            if spawned:
                for n in active_nodes:
                    if Decimal(str(n['active_capital'])) > per_node_cap:
                        conn.execute(
                            "UPDATE nodes SET active_capital = ?, drip_target = ? WHERE node_id = ?",
                            (f'{per_node_cap:.2f}', f'{drip_t:.2f}', int(n['node_id'])),
                        )
            conn.commit()
        return {
            'ok': True,
            'scale_tier': scale_tier,
            'spawned_count': len(spawned),
            'spawned': spawned,
            'consolidated_count': 0,
            'active_count': cur_count,
            'per_node_capital': f'{per_node_cap:.2f}',
        }



    def log_market_pulse_audit(
        self,
        pulse_number,
        node_id,
        symbol,
        market_price,
        price_source,
        rsi,
        sma,
        position_state,
        action_taken,
        qty='0.0000',
        realized_pnl='0.00',
        ghost_equity_after='0.00',
        reason='',
    ):
        now_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO market_pulse_audit (
                    pulse_number, timestamp_utc, node_id, symbol, market_price, price_source,
                    rsi, sma, position_state, action_taken, qty, realized_pnl, ghost_equity_after, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(pulse_number),
                    now_utc,
                    int(node_id),
                    str(symbol).upper(),
                    f'{Decimal(str(market_price)):.2f}',
                    str(price_source),
                    str(rsi),
                    f'{Decimal(str(sma)):.2f}',
                    str(position_state),
                    str(action_taken),
                    str(qty),
                    f'{Decimal(str(realized_pnl)):.2f}',
                    f'{Decimal(str(ghost_equity_after)):.2f}',
                    str(reason),
                ),
            )
            conn.commit()

    def get_market_pulse_audit_log(self, limit=50):
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                'SELECT * FROM market_pulse_audit ORDER BY audit_id DESC LIMIT ?',
                (int(limit),),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_engine_diagnostics(self):
        """
        Aggregates per-node and fleet-wide performance telemetry from market_pulse_audit,
        ghost_trades, ghost_positions, and nodes to pinpoint exactly where the engine is strong vs. weak.
        """
        with self._lock, self._connect() as conn:
            nodes = [dict(r) for r in conn.execute('SELECT * FROM nodes ORDER BY node_id ASC').fetchall()]
            positions = {r['symbol']: dict(r) for r in conn.execute('SELECT * FROM ghost_positions').fetchall()}
            trades = [dict(r) for r in conn.execute('SELECT * FROM ghost_trades ORDER BY trade_id DESC LIMIT 200').fetchall()]
            pulses = [dict(r) for r in conn.execute('SELECT * FROM market_pulse_audit ORDER BY audit_id DESC LIMIT 200').fetchall()]

        node_reports = []
        strong_count = 0
        weak_count = 0

        for n in nodes:
            nid = int(n['node_id'])
            sym = str(n['assigned_ticker']).upper()
            n_trades = [t for t in trades if int(t['node_id']) == nid or str(t['symbol']).upper() == sym]
            sells = [t for t in n_trades if str(t['side']).lower() == 'sell']
            wins = [t for t in sells if Decimal(str(t['realized_pnl'])) > Decimal('0.00')]
            losses = [t for t in sells if Decimal(str(t['realized_pnl'])) < Decimal('0.00')]
            realized_sum = sum((Decimal(str(t['realized_pnl'])) for t in sells), Decimal('0.00'))

            n_pulses = [p for p in pulses if int(p['node_id']) == nid or str(p['symbol']).upper() == sym]
            hard_stops = sum(1 for p in n_pulses if 'SELL_HARD_STOP' in str(p['action_taken']))
            take_profits = sum(1 for p in n_pulses if 'SELL_TAKE_PROFIT' in str(p['action_taken']) or 'SELL_TRAILING_RATCHET' in str(p['action_taken']))
            giveback_warnings = sum(1 for p in n_pulses if 'WEAK_PROFIT_GIVEBACK' in str(p['reason']) or 'WEAK_REVERSAL_FROM_PEAK' in str(p['reason']))

            pos = positions.get(sym)
            unrl_pnl = Decimal(str(pos['unrealized_pnl'])) if pos else Decimal('0.00')
            unrl_pct = Decimal(str(pos['unrealized_pnl_pct'])) if pos else Decimal('0.00')
            net_node_pnl = (realized_sum + unrl_pnl).quantize(Decimal('0.01'))

            latest_pulse = n_pulses[0] if n_pulses else None
            latest_rsi = float(latest_pulse['rsi']) if latest_pulse else 50.0
            latest_price = Decimal(str(latest_pulse['market_price'])) if latest_pulse else Decimal('0.00')
            latest_sma = Decimal(str(latest_pulse['sma'])) if latest_pulse else Decimal('0.00')
            sma_spread_pct = float(((latest_price - latest_sma) / latest_sma * Decimal('100.0')).quantize(Decimal('0.01'))) if latest_sma > 0 else 0.0

            drip_bucket = Decimal(str(n.get('drip_bucket', '0.00')))
            drip_target = Decimal(str(n.get('drip_target', '50.00')))
            drip_remaining = max(Decimal('0.00'), drip_target - drip_bucket)
            avg_win = (sum((Decimal(str(w['realized_pnl'])) for w in wins), Decimal('0.00')) / Decimal(str(len(wins)))).quantize(Decimal('0.01')) if wins else Decimal('0.05')
            est_trades_to_spawn = int((drip_remaining / max(Decimal('0.01'), avg_win)).to_integral_value())

            if net_node_pnl > Decimal('0.00') or (take_profits > hard_stops and unrl_pct >= Decimal('0.00')):
                verdict = 'STRONG_PERFORMER'
                strong_count += 1
                diagnosis = f'Positive capture (+${net_node_pnl:.2f} net | {take_profits} TP vs {hard_stops} Stop). Trend spread {sma_spread_pct:+.2f}%.'
            elif hard_stops > take_profits or net_node_pnl < Decimal('-0.02') or unrl_pct <= Decimal('-0.45'):
                verdict = 'WEAK_LINK_DRAWDOWN'
                weak_count += 1
                diagnosis = f'Drawdown / Stop vulnerability (${net_node_pnl:.2f} net | {hard_stops} Hard Stops | Giveback alerts: {giveback_warnings}).'
            elif giveback_warnings > 0:
                verdict = 'WEAK_HOLD_GIVEBACK'
                weak_count += 1
                diagnosis = f'Peak profit giveback detected ({giveback_warnings} pulses). Tighten ratchet or lower RSI take-profit threshold.'
            else:
                verdict = 'NEUTRAL_INCUBATING'
                diagnosis = f'Consolidating near SMA (Spread {sma_spread_pct:+.2f}% | RSI {latest_rsi:.1f}). Awaiting trend expansion.'

            node_reports.append({
                'node_id': nid,
                'symbol': sym,
                'division': n.get('division', ''),
                'verdict': verdict,
                'closed_trades': len(sells),
                'wins': len(wins),
                'losses': len(losses),
                'take_profits': take_profits,
                'hard_stops': hard_stops,
                'giveback_warnings': giveback_warnings,
                'realized_pnl': f'{realized_sum:.2f}',
                'unrealized_pnl': f'{unrl_pnl:.2f}',
                'unrealized_pct': f'{unrl_pct:.2f}%',
                'net_pnl': f'{net_node_pnl:.2f}',
                'sma_spread_pct': f'{sma_spread_pct:+.2f}%',
                'latest_rsi': f'{latest_rsi:.1f}',
                'drip_progress': f'${drip_bucket:.2f} / ${drip_target:.2f}',
                'est_wins_to_spawn': est_trades_to_spawn,
                'diagnosis': diagnosis,
            })

        return {
            'strong_nodes_count': strong_count,
            'weak_nodes_count': weak_count,
            'total_evaluated': len(node_reports),
            'node_reports': node_reports,
        }
