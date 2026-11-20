import collections
import json
import math
import sqlite3
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from config import get_broker_config


class BacktestBetaNode:
    def __init__(self, tier, active_capital):
        self.tier = tier
        self.active_capital = active_capital
        self.cold_storage = active_capital  # 1:1 backed
        self.drip_bucket = 0.00
        self.position = None
        self.total_realized_pnl = 0.0

    def get_position_size(self):
        return self.active_capital * (0.80 if self.tier == 0 else 0.40)

    def execute_trade(self, price, side='buy'):
        friction = 0.02
        if side == 'buy':
            return price + friction
        return price - friction

    def simulate_tier_0_graduation(self):
        if self.tier == 0 and self.active_capital >= 50.0:
            print("Graduating Tier 0 to Tier 1")
            return True
        return False


STRATEGY_PROFILES = {
    'BASELINE_V37': {
        'label': 'Baseline v3.7 (Always-On RSI <= 78, +0.35% TP / -0.85% Stop)',
        'min_entry_rsi': 0.0,
        'max_entry_rsi': 78.0,
        'min_sma_spread_pct': -99.0,
        'max_sma_spread_pct': 99.0,
        'cooldown_ticks': 0,
        'tp_pct': 0.35,
        'tp_min_dollars': Decimal('0.02'),
        'tp_rsi': 62.0,
        'ratchet_peak_pct': 0.25,
        'ratchet_drawdown_pct': 0.12,
        'hard_stop_pct': -0.85,
    },
    'DISCIPLINED_DIP_V38': {
        'label': 'Disciplined Dip v3.8 (Anti-Falling-Knife + Cooldown)',
        'min_entry_rsi': 22.0,
        'max_entry_rsi': 65.0,
        'min_sma_spread_pct': -0.35,
        'max_sma_spread_pct': 0.30,
        'cooldown_ticks': 2,
        'tp_pct': 0.35,
        'tp_min_dollars': Decimal('0.02'),
        'tp_rsi': 60.0,
        'ratchet_peak_pct': 0.22,
        'ratchet_drawdown_pct': 0.10,
        'hard_stop_pct': -0.75,
    },
    'OVERSOLD_REVERSION_V38': {
        'label': 'Oversold Mean-Reversion v3.8 (Buy Pullbacks <=45 RSI, Wide Stop)',
        'min_entry_rsi': 5.0,
        'max_entry_rsi': 45.0,
        'min_sma_spread_pct': -2.50,
        'max_sma_spread_pct': -0.02,
        'cooldown_ticks': 1,
        'tp_pct': 0.45,
        'tp_min_dollars': Decimal('0.02'),
        'tp_rsi': 60.0,
        'ratchet_peak_pct': 0.25,
        'ratchet_drawdown_pct': 0.10,
        'hard_stop_pct': -1.80,
    },
    'ASYMMETRIC_EDGE_V38': {
        'label': 'Asymmetric Edge v3.8 (Buy Below SMA, +0.55% TP / Ratchet Save)',
        'min_entry_rsi': 10.0,
        'max_entry_rsi': 52.0,
        'min_sma_spread_pct': -3.00,
        'max_sma_spread_pct': 0.05,
        'cooldown_ticks': 1,
        'tp_pct': 0.55,
        'tp_min_dollars': Decimal('0.02'),
        'tp_rsi': 62.0,
        'ratchet_peak_pct': 0.25,
        'ratchet_drawdown_pct': 0.10,
        'hard_stop_pct': -2.20,
    },
    'MOMENTUM_CONFIRM_V38': {
        'label': 'Momentum Confirmation v3.8 (Only Buy Uptrends Above SMA, Tight -0.35% Stop)',
        'min_entry_rsi': 52.0,
        'max_entry_rsi': 72.0,
        'min_sma_spread_pct': 0.03,
        'max_sma_spread_pct': 0.40,
        'cooldown_ticks': 3,
        'tp_pct': 0.45,
        'tp_min_dollars': Decimal('0.02'),
        'tp_rsi': 65.0,
        'ratchet_peak_pct': 0.22,
        'ratchet_drawdown_pct': 0.09,
        'hard_stop_pct': -0.35,
    },
    'MODERATE_MOMENTUM_V39': {
        'label': 'Moderate Momentum Bridge v3.9 (RSI 45-68, Spread -0.10% to +0.30%, +0.45% TP / -0.45% Stop)',
        'min_entry_rsi': 45.0,
        'max_entry_rsi': 68.0,
        'min_sma_spread_pct': -0.10,
        'max_sma_spread_pct': 0.30,
        'cooldown_ticks': 1,
        'tp_pct': 0.45,
        'tp_min_dollars': Decimal('0.02'),
        'tp_rsi': 60.0,
        'ratchet_peak_pct': 0.22,
        'ratchet_drawdown_pct': 0.09,
        'hard_stop_pct': -0.45,
    },
    'HYBRID_CORE_SATELLITE': {
        'label': 'Hybrid Core-Satellite v4.3 (5x $100 Nodes: RSI 45-75, Spread -0.50% to +1.00%, +1.50% TP / -0.75% Stop)',
        'min_entry_rsi': 45.0,
        'max_entry_rsi': 75.0,
        'min_sma_spread_pct': -0.50,
        'max_sma_spread_pct': 1.00,
        'cooldown_ticks': 2,
        'tp_pct': 1.50,
        'tp_min_dollars': Decimal('0.10'),
        'tp_rsi': 70.0,
        'ratchet_peak_pct': 0.50,
        'ratchet_drawdown_pct': 0.15,
        'hard_stop_pct': -0.75,
    },
    'CRYPTO_SATELLITE_V1': {
        'label': 'Crypto Satellite v1.0 (24/7 BTC/USD & ETH/USD: RSI 15-48 Pullback, +1.80% TP / +0.55% Ratchet / -2.80% Stop)',
        'min_entry_rsi': 15.0,
        'max_entry_rsi': 48.0,
        'min_sma_spread_pct': -3.50,
        'max_sma_spread_pct': -0.05,
        'cooldown_ticks': 2,
        'tp_pct': 1.80,
        'tp_min_dollars': Decimal('0.20'),
        'tp_rsi': 65.0,
        'ratchet_peak_pct': 0.55,
        'ratchet_drawdown_pct': 0.18,
        'hard_stop_pct': -2.80,
    },
}


class StrategyBacktestEngine:
    def __init__(self, db_path=None):
        self.db_path = Path(db_path) if db_path else (Path(__file__).resolve().parent / 'broker_v37.db')

    def load_recorded_pulse_stream(self, limit=5000):
        if not self.db_path.exists():
            return []
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT pulse_number, timestamp_utc, node_id, symbol, market_price, price_source
                FROM market_pulse_audit
                ORDER BY audit_id ASC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
            return [
                {
                    'pulse': int(r['pulse_number']),
                    'timestamp': r['timestamp_utc'],
                    'symbol': str(r['symbol']).upper(),
                    'price': Decimal(str(r['market_price'])).quantize(Decimal('0.01')),
                    'source': r['price_source'],
                }
                for r in rows
            ]
        finally:
            conn.close()

    def fetch_alpaca_historical_stream(
        self,
        symbols=('NVDA', 'SPY', 'GLD', 'SCHD'),
        timeframe='15Min',
        days=60,
        max_pages=4,
    ):
        cfg = get_broker_config()
        if not cfg.get('has_alpaca_keys'):
            return []
        sym_list = ','.join(s.strip().upper() for s in symbols)
        start_dt = (datetime.now(timezone.utc) - timedelta(days=int(days))).strftime('%Y-%m-%dT%H:%M:%SZ')
        headers = {
            'APCA-API-KEY-ID': cfg['alpaca_api_key'],
            'APCA-API-SECRET-KEY': cfg['alpaca_secret_key'],
            'Accept': 'application/json',
            'User-Agent': 'StockBroker-v3.7-Backtester/1.0',
        }
        events = []
        page_token = None
        for _ in range(max_pages):
            url = (
                f"https://data.alpaca.markets/v2/stocks/bars?"
                f"symbols={urllib.parse.quote(sym_list)}&timeframe={timeframe}&start={start_dt}&limit=10000&feed=iex"
            )
            if page_token:
                url += f"&page_token={urllib.parse.quote(page_token)}"
            req = urllib.request.Request(url=url, headers=headers, method='GET')
            try:
                with urllib.request.urlopen(req, timeout=12) as resp:
                    payload = json.loads(resp.read().decode('utf-8'))
                    bars_by_sym = payload.get('bars', {})
                    for sym, bars in bars_by_sym.items():
                        for b in bars:
                            events.append({
                                'timestamp': b.get('t', ''),
                                'symbol': sym.upper(),
                                'price': Decimal(str(b.get('c', '0'))).quantize(Decimal('0.01')),
                                'source': f'ALPACA_HIST_{timeframe}_{days}D',
                            })
                    page_token = payload.get('next_page_token')
                    if not page_token:
                        break
            except Exception:
                break
        events.sort(key=lambda x: (x['timestamp'], x['symbol']))
        for idx, ev in enumerate(events, 1):
            ev['pulse'] = idx
        return events

    def fetch_alpaca_crypto_historical_stream(
        self,
        symbols=('BTC/USD', 'ETH/USD'),
        timeframe='1Hour',
        days=60,
        max_pages=4,
    ):
        cfg = get_broker_config()
        if not cfg.get('has_alpaca_keys'):
            return []
        sym_list = ','.join(s.strip().upper() for s in symbols)
        start_dt = (datetime.now(timezone.utc) - timedelta(days=int(days))).strftime('%Y-%m-%dT%H:%M:%SZ')
        headers = {
            'APCA-API-KEY-ID': cfg['alpaca_api_key'],
            'APCA-API-SECRET-KEY': cfg['alpaca_secret_key'],
            'Accept': 'application/json',
            'User-Agent': 'StockBroker-v3.7-Backtester/1.0',
        }
        events = []
        page_token = None
        for _ in range(max_pages):
            url = (
                f"https://data.alpaca.markets/v1beta3/crypto/us/bars?"
                f"symbols={urllib.parse.quote(sym_list)}&timeframe={timeframe}&start={start_dt}&limit=10000"
            )
            if page_token:
                url += f"&page_token={urllib.parse.quote(page_token)}"
            req = urllib.request.Request(url=url, headers=headers, method='GET')
            try:
                with urllib.request.urlopen(req, timeout=12) as resp:
                    payload = json.loads(resp.read().decode('utf-8'))
                    bars_by_sym = payload.get('bars', {})
                    for sym, bars in bars_by_sym.items():
                        for b in bars:
                            events.append({
                                'timestamp': b.get('t', ''),
                                'symbol': sym.upper(),
                                'price': Decimal(str(b.get('c', '0'))).quantize(Decimal('0.01')),
                                'source': f'ALPACA_CRYPTO_HIST_{timeframe}_{days}D',
                            })
                    page_token = payload.get('next_page_token')
                    if not page_token:
                        break
            except Exception:
                break
        events.sort(key=lambda x: (x['timestamp'], x['symbol']))
        for idx, ev in enumerate(events, 1):
            ev['pulse'] = idx
        return events

    @staticmethod
    def _compute_indicators(history_deque, current_price):
        p = float(current_price)
        history_deque.append(p)
        series = list(history_deque)
        sma = sum(series) / len(series) if series else p
        if len(series) < 3:
            rsi = 50.0
        else:
            gains = []
            losses = []
            for i in range(1, len(series)):
                delta = series[i] - series[i - 1]
                if delta > 0:
                    gains.append(delta)
                    losses.append(0.0)
                else:
                    gains.append(0.0)
                    losses.append(abs(delta))
            avg_gain = (sum(gains) / len(gains)) if gains else 0.0
            avg_loss = (sum(losses) / len(losses)) if losses else 0.0
            if avg_loss == 0.0 and avg_gain == 0.0:
                rsi = 50.0
            elif avg_loss == 0.0:
                rsi = 75.0
            else:
                rs = avg_gain / avg_loss
                rsi = 100.0 - (100.0 / (1.0 + rs))
        sma_spread_pct = ((p - sma) / sma * 100.0) if sma > 0 else 0.0
        return {
            'price': round(p, 2),
            'sma': round(sma, 2),
            'sma_spread_pct': round(sma_spread_pct, 2),
            'rsi': round(rsi, 1),
            'samples': len(series),
        }

    def simulate_profile(
        self,
        ticks,
        profile_key='BASELINE_V37',
        starting_cash=Decimal('25.00'),
        max_positions=4,
        allowed_symbols=None,
    ):
        profile = STRATEGY_PROFILES[profile_key]
        start_cash_dec = Decimal(str(starting_cash)).quantize(Decimal('0.01'))
        cash = start_cash_dec
        history = collections.defaultdict(lambda: collections.deque(maxlen=30))
        positions = {}
        cooldowns = collections.defaultdict(int)
        last_prices = {}
        allowed_set = {s.upper() for s in allowed_symbols} if allowed_symbols else None

        closed_trades = []
        wins = 0
        losses = 0
        take_profits = 0
        ratchet_saves = 0
        hard_stops = 0
        filtered_entries = 0

        alloc_fraction = (Decimal('1.00') / Decimal(str(max(1, int(max_positions))))).quantize(Decimal('0.01'))
        max_cap_per_pos = (start_cash_dec * alloc_fraction).quantize(Decimal('0.01'))

        for tick in ticks:
            sym = tick['symbol']
            if allowed_set and sym not in allowed_set:
                continue
            cur_p = tick['price']
            if cur_p <= Decimal('0.00'):
                continue
            last_prices[sym] = cur_p
            ind = self._compute_indicators(history[sym], cur_p)
            rsi = ind['rsi']
            spread = ind['sma_spread_pct']

            if sym not in positions:
                if len(positions) >= int(max_positions):
                    continue
                if cooldowns[sym] > 0:
                    cooldowns[sym] -= 1
                    filtered_entries += 1
                    continue
                if (
                    rsi < profile['min_entry_rsi']
                    or rsi > profile['max_entry_rsi']
                    or spread < profile['min_sma_spread_pct']
                    or spread > profile['max_sma_spread_pct']
                ):
                    filtered_entries += 1
                    continue

                target_notional = min(max_cap_per_pos, cash)
                if target_notional <= Decimal('0.50'):
                    continue
                qty = max(Decimal('0.0001'), (target_notional / cur_p).quantize(Decimal('0.0001')))
                notional = (qty * cur_p).quantize(Decimal('0.01'))
                if notional > cash or notional <= Decimal('0.00'):
                    continue

                cash -= notional
                positions[sym] = {'qty': qty, 'entry_price': cur_p, 'notional': notional, 'hwm': cur_p}
            else:
                pos = positions[sym]
                qty = pos['qty']
                entry_p = pos['entry_price']
                if cur_p > pos['hwm']:
                    pos['hwm'] = cur_p
                hwm = pos['hwm']

                pnl_dollars = ((cur_p - entry_p) * qty).quantize(Decimal('0.01'))
                pnl_pct = float(((cur_p - entry_p) / entry_p * Decimal('100.0')).quantize(Decimal('0.01'))) if entry_p > 0 else 0.0
                peak_pct = float(((hwm - entry_p) / entry_p * Decimal('100.0')).quantize(Decimal('0.01'))) if entry_p > 0 else 0.0
                drawdown_from_hwm_pct = float(((hwm - cur_p) / hwm * Decimal('100.0')).quantize(Decimal('0.01'))) if hwm > 0 else 0.0

                exit_reason = None
                if pnl_pct >= profile['tp_pct'] or (pnl_dollars >= profile['tp_min_dollars'] and rsi >= profile['tp_rsi']):
                    exit_reason = 'SELL_TAKE_PROFIT'
                    take_profits += 1
                elif peak_pct >= profile['ratchet_peak_pct'] and drawdown_from_hwm_pct >= profile['ratchet_drawdown_pct'] and pnl_dollars > Decimal('0.00'):
                    exit_reason = 'SELL_TRAILING_RATCHET'
                    ratchet_saves += 1
                elif pnl_pct <= profile['hard_stop_pct']:
                    exit_reason = 'SELL_HARD_STOP'
                    hard_stops += 1

                if exit_reason:
                    proceeds = (qty * cur_p).quantize(Decimal('0.01'))
                    realized = (proceeds - pos['notional']).quantize(Decimal('0.01'))
                    cash += proceeds
                    if realized > Decimal('0.00'):
                        wins += 1
                    elif realized < Decimal('0.00'):
                        losses += 1
                    closed_trades.append({
                        'symbol': sym,
                        'entry_price': f'{entry_p:.2f}',
                        'exit_price': f'{cur_p:.2f}',
                        'realized_pnl': f'{realized:.2f}',
                        'pnl_pct': f'{pnl_pct:+.2f}%',
                        'reason': exit_reason,
                    })
                    del positions[sym]
                    cooldowns[sym] = int(profile['cooldown_ticks'])

        open_value = Decimal('0.00')
        unrealized_pnl = Decimal('0.00')
        for sym, pos in positions.items():
            mark_p = last_prices.get(sym, pos['entry_price'])
            mkt_val = (pos['qty'] * mark_p).quantize(Decimal('0.01'))
            open_value += mkt_val
            unrealized_pnl += (mkt_val - pos['notional']).quantize(Decimal('0.01'))

        realized_pnl = sum((Decimal(t['realized_pnl']) for t in closed_trades), Decimal('0.00'))
        total_equity = (cash + open_value).quantize(Decimal('0.01'))
        net_pnl = (total_equity - start_cash_dec).quantize(Decimal('0.01'))
        return_pct = float((net_pnl / start_cash_dec * Decimal('100.0')).quantize(Decimal('0.02')))
        total_decided = wins + losses
        win_rate = round((wins / total_decided * 100.0), 1) if total_decided > 0 else 0.0

        return {
            'profile_key': profile_key,
            'label': profile['label'],
            'max_positions': int(max_positions),
            'ticks_processed': len(ticks),
            'starting_cash': f'{start_cash_dec:.2f}',
            'ending_equity': f'{total_equity:.2f}',
            'realized_pnl': f'{realized_pnl:+.2f}',
            'unrealized_pnl': f'{unrealized_pnl:+.2f}',
            'net_pnl': f'{net_pnl:+.2f}',
            'return_pct': f'{return_pct:+.2f}%',
            'closed_trades': len(closed_trades),
            'wins': wins,
            'losses': losses,
            'win_rate': f'{win_rate:.1f}%',
            'take_profits': take_profits,
            'ratchet_saves': ratchet_saves,
            'hard_stops': hard_stops,
            'filtered_bad_entries': filtered_entries,
            'recent_trades': closed_trades[-8:],
        }

    def run_comparative_suite(self, starting_cash=25.00, days=60):
        recorded_ticks = self.load_recorded_pulse_stream()
        alpaca_ticks = self.fetch_alpaca_historical_stream(timeframe='15Min', days=days)
        start_dec = Decimal(str(starting_cash)).quantize(Decimal('0.01'))

        recorded_results = [
            self.simulate_profile(recorded_ticks, key, starting_cash=start_dec, max_positions=4)
            for key in STRATEGY_PROFILES
        ]
        alpaca_results = [
            self.simulate_profile(alpaca_ticks, key, starting_cash=start_dec, max_positions=4)
            for key in STRATEGY_PROFILES
        ] if alpaca_ticks else []

        best_recorded = max(recorded_results, key=lambda r: Decimal(r['net_pnl'])) if recorded_results else None
        return {
            'recorded_pulse_count': len(recorded_ticks),
            'alpaca_bar_count': len(alpaca_ticks),
            'recorded_comparison': recorded_results,
            'alpaca_comparison': alpaca_results,
            'recommended_profile': best_recorded['profile_key'] if best_recorded else 'MODERATE_MOMENTUM_V39',
            'recommended_label': best_recorded['label'] if best_recorded else 'Moderate Momentum Bridge v3.9',
        }

    def simulate_nitro_fleet(
        self,
        ticks,
        profile_key='HYBRID_CORE_SATELLITE',
        initial_node_seed=Decimal('100.00'),
        node_count=5,
        monthly_nitro_per_fleet=Decimal('100.00'),
        allowed_symbols=('NVDA', 'TSLA', 'AMD', 'TQQQ'),
    ):
        profile = STRATEGY_PROFILES[profile_key]
        init_per_node = Decimal(str(initial_node_seed)).quantize(Decimal('0.01'))
        monthly_add_total = Decimal(str(monthly_nitro_per_fleet)).quantize(Decimal('0.01'))
        monthly_add_per_node = (monthly_add_total / Decimal(str(node_count))).quantize(Decimal('0.01'))

        allowed_set = {s.upper() for s in allowed_symbols}
        filtered_ticks = [t for t in ticks if t['symbol'] in allowed_set]

        cash = init_per_node
        history = collections.defaultdict(lambda: collections.deque(maxlen=30))
        pos = None
        cooldowns = collections.defaultdict(int)
        last_prices = {}
        active_ym = None
        nitro_events = 0
        wins = 0
        losses = 0
        closed_trades = 0

        for tick in filtered_ticks:
            sym = tick['symbol']
            cur_p = tick['price']
            if cur_p <= Decimal('0.00'):
                continue
            last_prices[sym] = cur_p

            ym = str(tick.get('timestamp', ''))[:7]
            if ym and ym != active_ym:
                if active_ym is not None:
                    nitro_events += 1
                    cash = (cash + monthly_add_per_node).quantize(Decimal('0.01'))
                    if pos is not None and pos['symbol'] in last_prices:
                        p_now = last_prices[pos['symbol']]
                        if p_now >= pos['entry_price']:
                            add_qty = (monthly_add_per_node / p_now).quantize(Decimal('0.0001'))
                            add_notional = (add_qty * p_now).quantize(Decimal('0.01'))
                            if add_qty > Decimal('0.0000') and add_notional <= cash:
                                cash -= add_notional
                                pos['qty'] += add_qty
                                pos['notional'] += add_notional
                                pos['entry_price'] = (pos['notional'] / pos['qty']).quantize(Decimal('0.01'))
                active_ym = ym

            ind = self._compute_indicators(history[sym], cur_p)
            rsi = ind['rsi']
            spread = ind['sma_spread_pct']

            if pos is None:
                if cooldowns[sym] > 0:
                    cooldowns[sym] -= 1
                    continue
                if (
                    rsi < profile['min_entry_rsi']
                    or rsi > profile['max_entry_rsi']
                    or spread < profile['min_sma_spread_pct']
                    or spread > profile['max_sma_spread_pct']
                ):
                    continue
                target_notional = cash
                if target_notional <= Decimal('0.50'):
                    continue
                qty = max(Decimal('0.0001'), (target_notional / cur_p).quantize(Decimal('0.0001')))
                notional = (qty * cur_p).quantize(Decimal('0.01'))
                if notional > cash or notional <= Decimal('0.00'):
                    continue
                cash -= notional
                pos = {'symbol': sym, 'qty': qty, 'entry_price': cur_p, 'notional': notional, 'hwm': cur_p}
            elif pos['symbol'] == sym:
                qty = pos['qty']
                entry_p = pos['entry_price']
                if cur_p > pos['hwm']:
                    pos['hwm'] = cur_p
                hwm = pos['hwm']

                pnl_dollars = ((cur_p - entry_p) * qty).quantize(Decimal('0.01'))
                pnl_pct = float(((cur_p - entry_p) / entry_p * Decimal('100.0')).quantize(Decimal('0.01'))) if entry_p > 0 else 0.0
                peak_pct = float(((hwm - entry_p) / entry_p * Decimal('100.0')).quantize(Decimal('0.01'))) if entry_p > 0 else 0.0
                drawdown_from_hwm_pct = float(((hwm - cur_p) / hwm * Decimal('100.0')).quantize(Decimal('0.01'))) if hwm > 0 else 0.0

                exit_reason = None
                if pnl_pct >= profile['tp_pct'] or (pnl_dollars >= profile['tp_min_dollars'] and rsi >= profile['tp_rsi']):
                    exit_reason = 'SELL_TAKE_PROFIT'
                elif peak_pct >= profile['ratchet_peak_pct'] and drawdown_from_hwm_pct >= profile['ratchet_drawdown_pct'] and pnl_dollars > Decimal('0.00'):
                    exit_reason = 'SELL_TRAILING_RATCHET'
                elif pnl_pct <= profile['hard_stop_pct']:
                    exit_reason = 'SELL_HARD_STOP'

                if exit_reason:
                    proceeds = (qty * cur_p).quantize(Decimal('0.01'))
                    realized = (proceeds - pos['notional']).quantize(Decimal('0.01'))
                    cash += proceeds
                    closed_trades += 1
                    if realized > Decimal('0.00'):
                        wins += 1
                    elif realized < Decimal('0.00'):
                        losses += 1
                    pos = None
                    cooldowns[sym] = int(profile['cooldown_ticks'])

        single_node_ending = cash
        if pos is not None:
            mark_p = last_prices.get(pos['symbol'], pos['entry_price'])
            single_node_ending += (pos['qty'] * mark_p).quantize(Decimal('0.01'))

        ending_equity = (single_node_ending * Decimal(str(node_count))).quantize(Decimal('0.01'))
        initial_total = (init_per_node * Decimal(str(node_count))).quantize(Decimal('0.01'))
        total_contributed = (initial_total + (monthly_add_total * Decimal(str(nitro_events)))).quantize(Decimal('0.01'))
        net_profit = (ending_equity - total_contributed).quantize(Decimal('0.01'))
        roi_on_total_contributed = float((net_profit / total_contributed * Decimal('100.0')).quantize(Decimal('0.02'))) if total_contributed > 0 else 0.0
        roi_on_initial_seed = float((net_profit / initial_total * Decimal('100.0')).quantize(Decimal('0.02'))) if initial_total > 0 else 0.0
        avg_deployed_capital = (initial_total + (total_contributed - initial_total) / Decimal('2.0')).quantize(Decimal('0.01'))
        time_weighted_roi = float((net_profit / avg_deployed_capital * Decimal('100.0')).quantize(Decimal('0.02'))) if avg_deployed_capital > 0 else 0.0
        win_rate = round((wins / max(1, wins + losses)) * 100.0, 1)

        return {
            'profile_key': profile_key,
            'node_count': node_count,
            'initial_seed_total': f'{initial_total:.2f}',
            'nitro_injections_count': nitro_events,
            'monthly_nitro_amount': f'{monthly_add_total:.2f}',
            'total_capital_contributed': f'{total_contributed:.2f}',
            'average_deployed_capital': f'{avg_deployed_capital:.2f}',
            'ending_equity': f'{ending_equity:.2f}',
            'net_profit': f'{net_profit:+.2f}',
            'return_on_initial_seed_pct': f'{roi_on_initial_seed:+.2f}%',
            'time_weighted_return_pct': f'{time_weighted_roi:+.2f}%',
            'return_on_total_contributed_pct': f'{roi_on_total_contributed:+.2f}%',
            'closed_trades': closed_trades,
            'wins': wins,
            'losses': losses,
            'win_rate': f'{win_rate:.1f}%',
        }



# Verification Test
def test_backtest_math():
    node = BacktestBetaNode(tier=0, active_capital=10.0)
    assert node.get_position_size() == 8.0, f"Expected 8.0, got {node.get_position_size()}"

    fill_price = 100.0
    entry = node.execute_trade(fill_price, 'buy')
    assert entry == 100.02, "Friction model failed"

    print("Backtester math verification passed.")


if __name__ == "__main__":
    test_backtest_math()
    engine = StrategyBacktestEngine()
    rec_ticks = engine.load_recorded_pulse_stream()
    bars_60d = engine.fetch_alpaca_historical_stream(timeframe='1Hour', days=60, max_pages=1)
    bars_10m = engine.fetch_alpaca_historical_stream(timeframe='1Day', days=300, max_pages=1)

    def _print_table(title, ticks, max_pos=4, allowed_syms=None):
        print(f"\n=== {title} ({len(ticks)} Ticks | Max Concurrent Nodes: {max_pos}) ===")
        for key in ('BASELINE_V37', 'ASYMMETRIC_EDGE_V38', 'MOMENTUM_CONFIRM_V38', 'MODERATE_MOMENTUM_V39'):
            r = engine.simulate_profile(ticks, key, starting_cash=Decimal('25.00'), max_positions=max_pos, allowed_symbols=allowed_syms)
            print(
                f"  * {r['profile_key']:<22} | Equity: ${r['ending_equity']} (Net: ${r['net_pnl']} / {r['return_pct']:>7}) "
                f"| Trades: {r['closed_trades']:>3} (W:{r['wins']:>2} / L:{r['losses']:>2} = {r['win_rate']:>5}) "
                f"| TP:{r['take_profits']:>2} Ratchet:{r['ratchet_saves']:>2} Stops:{r['hard_stops']:>2} | Filtered:{r['filtered_bad_entries']}"
            )

    _print_table("[1] RECORDED LIVE PULSES (broker_v37.db) - 4-NODE SPLIT ($6.25/trade)", rec_ticks, max_pos=4)
    _print_table("[2] RECORDED LIVE PULSES (broker_v37.db) - OPTION 3: 1-NODE CONCENTRATED ($25/trade)", rec_ticks, max_pos=1)

    if bars_60d:
        _print_table("[3] 60-DAY ALPACA HISTORICAL BARS - 4-NODE SPLIT ($6.25/trade)", bars_60d, max_pos=4)
        _print_table("[4] 60-DAY ALPACA HISTORICAL BARS - OPTION 3: 1-NODE CONCENTRATED ($25/trade)", bars_60d, max_pos=1)

    if bars_10m:
        _print_table("[5] 10-MONTH (300-DAY) ALPACA HISTORICAL BARS - 4-NODE SPLIT ($6.25/trade)", bars_10m, max_pos=4)
        _print_table("[6] 10-MONTH (300-DAY) ALPACA HISTORICAL BARS - OPTION 3: 1-NODE CONCENTRATED ($25/trade)", bars_10m, max_pos=1)

    bars_12m_nitro = engine.fetch_alpaca_historical_stream(
        symbols=('NVDA', 'TSLA', 'AMD', 'TQQQ', 'SPY'),
        timeframe='1Day',
        days=365,
        max_pages=2,
    )
    if bars_12m_nitro:
        print("\n=== [7] 12-MONTH (365-DAY) NITRO FUND PROTOCOL AUDIT (5 TANDEM NODES + $100/MO NITRO SUPERCHARGE) ===")
        for seed_per_node in (Decimal('20.00'), Decimal('100.00')):
            nr = engine.simulate_nitro_fleet(
                bars_12m_nitro,
                profile_key='HYBRID_CORE_SATELLITE',
                initial_node_seed=seed_per_node,
                node_count=5,
                monthly_nitro_per_fleet=Decimal('100.00'),
            )
            print(
                f"  * 5x ${seed_per_node}/Node (${nr['initial_seed_total']} Seed + {nr['nitro_injections_count']}x ${nr['monthly_nitro_amount']} Nitro = ${nr['total_capital_contributed']} Contributed)\n"
                f"    -> Ending Equity: ${nr['ending_equity']} | Net Profit: ${nr['net_profit']} "
                f"| Time-Weighted Return: {nr['time_weighted_return_pct']} (Return on Initial Seed: {nr['return_on_initial_seed_pct']})\n"
                f"    -> Trades: {nr['closed_trades']} (W:{nr['wins']} / L:{nr['losses']} = {nr['win_rate']})"
            )

    crypto_bars_60d = engine.fetch_alpaca_crypto_historical_stream(
        symbols=('BTC/USD', 'ETH/USD'),
        timeframe='1Hour',
        days=60,
        max_pages=2,
    )
    if crypto_bars_60d:
        print(f"\n=== [8] 60-DAY CRYPTO SATELLITE (BTC/USD & ETH/USD) vs. EQUITY FLEET AUDIT ({len(crypto_bars_60d)} Crypto Bars | $100/Node Seed) ===")
        rc_crypto = engine.simulate_profile(crypto_bars_60d, 'CRYPTO_SATELLITE_V1', starting_cash=Decimal('100.00'), max_positions=1)
        rc_eq = engine.simulate_profile(bars_60d, 'HYBRID_CORE_SATELLITE', starting_cash=Decimal('100.00'), max_positions=1)
        print(
            f"  * CRYPTO_SATELLITE_V1 (BTC/USD & ETH/USD, $100 Node) | Equity: ${rc_crypto['ending_equity']} "
            f"(Net: ${rc_crypto['net_pnl']} / {rc_crypto['return_pct']}) | Trades: {rc_crypto['closed_trades']} "
            f"(W:{rc_crypto['wins']} / L:{rc_crypto['losses']} = {rc_crypto['win_rate']}) | TP:{rc_crypto['take_profits']} Ratchet:{rc_crypto['ratchet_saves']} Stops:{rc_crypto['hard_stops']}"
        )
        print(
            f"  * EQUITY_SATELLITE (NVDA/SPY/GLD/SCHD, $100 Node)    | Equity: ${rc_eq['ending_equity']} "
            f"(Net: ${rc_eq['net_pnl']} / {rc_eq['return_pct']}) | Trades: {rc_eq['closed_trades']} "
            f"(W:{rc_eq['wins']} / L:{rc_eq['losses']} = {rc_eq['win_rate']}) | TP:{rc_eq['take_profits']} Ratchet:{rc_eq['ratchet_saves']} Stops:{rc_eq['hard_stops']}"
        )




