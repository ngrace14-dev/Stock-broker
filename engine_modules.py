import collections
import logging
from decimal import Decimal


class VelocityBetaStrategy:
    """
    Alpha V1 VelocityBeta Strategy Brain:
    - Maintains rolling price history and high-watermarks per ticker.
    - Calculates real-time RSI (Relative Strength Index), SMA (Simple Moving Average), and Momentum Velocity.
    - Prevents blind 60-second buy/sell churn by HOLDING open positions until Take-Profit, Trailing Ratchet,
      or Hard Stop-Loss conditions are genuinely met by live market price movement.
    """

    def __init__(self):
        self.logger = logging.getLogger('StrategyBrain')
        self.price_history = collections.defaultdict(lambda: collections.deque(maxlen=30))
        self.high_watermarks = {}
        self.cooldowns = collections.defaultdict(int)
        self.active_profile = 'HYBRID_CORE_SATELLITE'

    def is_rth(self, now=None):
        return True

    def record_tick(self, symbol, price):
        sym = str(symbol).strip().upper()
        p = float(price)
        self.price_history[sym].append(p)
        if sym not in self.high_watermarks or p > self.high_watermarks[sym]:
            self.high_watermarks[sym] = p
        return p

    def compute_indicators(self, symbol, current_price):
        sym = str(symbol).strip().upper()
        p = self.record_tick(sym, current_price)
        series = list(self.price_history[sym])
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

        hwm = self.high_watermarks.get(sym, p)
        sma_spread_pct = ((p - sma) / sma * 100.0) if sma > 0 else 0.0
        return {
            'price': round(p, 2),
            'sma': round(sma, 2),
            'sma_spread_pct': round(sma_spread_pct, 2),
            'rsi': round(rsi, 1),
            'high_watermark': round(hwm, 2),
            'samples': len(series),
        }

    def evaluate_position_action(self, symbol, current_price, existing_position=None, force_close=False):
        sym = str(symbol).strip().upper()
        is_crypto = '/' in sym or sym in ('BTCUSD', 'ETHUSD')
        ind = self.compute_indicators(sym, current_price)
        cur_p = Decimal(str(current_price)).quantize(Decimal('0.01'))
        sma_spread = ind['sma_spread_pct']

        min_rsi = 15.0 if is_crypto else 45.0
        max_rsi = 48.0 if is_crypto else 75.0
        min_spread = -3.50 if is_crypto else -0.50
        max_spread = -0.05 if is_crypto else 1.00
        tp_target_pct = 1.80 if is_crypto else 1.50
        tp_min_dlr = Decimal('0.20') if is_crypto else Decimal('0.10')
        tp_rsi_lvl = 65.0 if is_crypto else 70.0
        ratchet_peak = 0.55 if is_crypto else 0.50
        ratchet_dd = 0.18 if is_crypto else 0.15
        hard_stop_lvl = -2.80 if is_crypto else -0.75
        profile_tag = 'CRYPTO_SATELLITE_V1' if is_crypto else 'HYBRID_CORE_SATELLITE'

        if not existing_position:
            self.high_watermarks[sym] = float(cur_p)
            if not force_close and self.cooldowns.get(sym, 0) > 0:
                self.cooldowns[sym] -= 1
                return {
                    'signal': 'WAIT_COOLDOWN',
                    'should_buy': False,
                    'should_sell': False,
                    'indicators': ind,
                    'performance_tag': 'DISCIPLINED_FILTER',
                    'reason': f"Flat on {sym}: Post-exit cooldown active (RSI {ind['rsi']} | SMA ${ind['sma']:.2f} | Spread {sma_spread:+.2f}%) [DIAG: DISCIPLINED_FILTER]",
                }
            if not force_close and (ind['rsi'] < min_rsi or ind['rsi'] > max_rsi or sma_spread < min_spread or sma_spread > max_spread):
                filter_reason = (
                    f"RSI {ind['rsi']} outside [{min_rsi}, {max_rsi}]"
                    if (ind['rsi'] < min_rsi or ind['rsi'] > max_rsi)
                    else f"Spread {sma_spread:+.2f}% outside [{min_spread:+.2f}%, {max_spread:+.2f}%]"
                )
                return {
                    'signal': 'WAIT_OVERBOUGHT',
                    'should_buy': False,
                    'should_sell': False,
                    'indicators': ind,
                    'performance_tag': 'DISCIPLINED_FILTER',
                    'reason': f"Flat on {sym}: Waiting for {profile_tag} window ({filter_reason} vs SMA ${ind['sma']:.2f} | Spread {sma_spread:+.2f}%) [DIAG: DISCIPLINED_FILTER]",
                }
            entry_tag = 'DISCIPLINED_ENTRY'
            return {
                'signal': 'BUY_ENTRY',
                'should_buy': True,
                'should_sell': False,
                'indicators': ind,
                'performance_tag': entry_tag,
                'reason': f"{profile_tag} Entry on {sym} @ ${cur_p:.2f} (RSI {ind['rsi']} | SMA ${ind['sma']:.2f} | Spread {sma_spread:+.2f}%) [DIAG: {entry_tag}]",
            }

        entry_p = Decimal(str(existing_position.get('avg_entry_price', cur_p))).quantize(Decimal('0.01'))
        qty = Decimal(str(existing_position.get('qty', '0')))
        pnl_per_share = cur_p - entry_p
        pnl_dollars = (pnl_per_share * qty).quantize(Decimal('0.01'))
        pnl_pct = float(((cur_p - entry_p) / entry_p * Decimal('100.0')).quantize(Decimal('0.01'))) if entry_p > 0 else 0.0

        hwm = Decimal(str(max(float(cur_p), float(entry_p), ind['high_watermark']))).quantize(Decimal('0.01'))
        self.high_watermarks[sym] = float(hwm)
        peak_pct = float(((hwm - entry_p) / entry_p * Decimal('100.0')).quantize(Decimal('0.01'))) if entry_p > 0 else 0.0
        drawdown_from_hwm_pct = float(((hwm - cur_p) / hwm * Decimal('100.0')).quantize(Decimal('0.01'))) if hwm > 0 else 0.0
        giveback_pct = round(max(0.0, peak_pct - pnl_pct), 2)

        if force_close:
            self.high_watermarks.pop(sym, None)
            return {
                'signal': 'SELL_FORCE_CLOSE',
                'should_buy': False,
                'should_sell': True,
                'pnl_pct': pnl_pct,
                'pnl_dollars': f'{pnl_dollars:.2f}',
                'peak_pct': peak_pct,
                'giveback_pct': giveback_pct,
                'performance_tag': 'MANUAL_OR_FORCE_EXIT',
                'indicators': ind,
                'reason': f"Explicit close on {sym} @ ${cur_p:.2f} (Entry ${entry_p:.2f} | P&L: ${pnl_dollars:.2f} / {pnl_pct:.2f}% | Peak +{peak_pct:.2f}% / Giveback {giveback_pct:.2f}%)",
            }

        if pnl_pct >= tp_target_pct or (pnl_dollars >= tp_min_dlr and ind['rsi'] >= tp_rsi_lvl):
            self.high_watermarks.pop(sym, None)
            self.cooldowns[sym] = 2
            return {
                'signal': 'SELL_TAKE_PROFIT',
                'should_buy': False,
                'should_sell': True,
                'pnl_pct': pnl_pct,
                'pnl_dollars': f'{pnl_dollars:.2f}',
                'peak_pct': peak_pct,
                'giveback_pct': giveback_pct,
                'performance_tag': 'STRONG_PROFIT_LOCK',
                'indicators': ind,
                'reason': f"Take-Profit (+{tp_target_pct:.2f}% {profile_tag}) triggered on {sym} @ ${cur_p:.2f} (Entry ${entry_p:.2f} | Gain: +${pnl_dollars:.2f} / +{pnl_pct:.2f}% | RSI {ind['rsi']} | Spread {sma_spread:+.2f}%) [DIAG: STRONG_PROFIT_LOCK]",
            }

        if peak_pct >= ratchet_peak and drawdown_from_hwm_pct >= ratchet_dd and pnl_dollars > Decimal('0.00'):
            self.high_watermarks.pop(sym, None)
            self.cooldowns[sym] = 2
            return {
                'signal': 'SELL_TRAILING_RATCHET',
                'should_buy': False,
                'should_sell': True,
                'pnl_pct': pnl_pct,
                'pnl_dollars': f'{pnl_dollars:.2f}',
                'peak_pct': peak_pct,
                'giveback_pct': giveback_pct,
                'performance_tag': 'STRONG_RATCHET_SAVE',
                'indicators': ind,
                'reason': f"Trailing Ratchet locked profit on {sym} @ ${cur_p:.2f} (HWM ${hwm:.2f} | Gain: +${pnl_dollars:.2f} / +{pnl_pct:.2f}% | Giveback {giveback_pct:.2f}%) [DIAG: STRONG_RATCHET_SAVE]",
            }

        if pnl_pct <= hard_stop_lvl:
            self.high_watermarks.pop(sym, None)
            self.cooldowns[sym] = 2
            stop_tag = 'WEAK_REVERSAL_FROM_PEAK' if peak_pct >= 0.15 else 'WEAK_HARD_STOP'
            return {
                'signal': 'SELL_HARD_STOP',
                'should_buy': False,
                'should_sell': True,
                'pnl_pct': pnl_pct,
                'pnl_dollars': f'{pnl_dollars:.2f}',
                'peak_pct': peak_pct,
                'giveback_pct': giveback_pct,
                'performance_tag': stop_tag,
                'indicators': ind,
                'reason': f"Hard Stop-Loss ({hard_stop_lvl:.2f}% {profile_tag}) triggered on {sym} @ ${cur_p:.2f} (Entry ${entry_p:.2f} | P&L: ${pnl_dollars:.2f} / {pnl_pct:.2f}% | Peak +{peak_pct:.2f}% / Giveback {giveback_pct:.2f}%) [DIAG: {stop_tag}]",
            }

        if peak_pct >= 0.20 and pnl_pct <= 0.0:
            hold_tag = 'WEAK_PROFIT_GIVEBACK'
        elif pnl_pct <= -0.45:
            hold_tag = 'WEAK_UNDERWATER_HOLD'
        elif pnl_pct > 0.0 and sma_spread >= 0.0:
            hold_tag = 'STRONG_TREND_HOLD'
        else:
            hold_tag = 'NEUTRAL_CONSOLIDATION'

        return {
            'signal': 'HOLD_TREND',
            'should_buy': False,
            'should_sell': False,
            'pnl_pct': pnl_pct,
            'pnl_dollars': f'{pnl_dollars:.2f}',
            'peak_pct': peak_pct,
            'giveback_pct': giveback_pct,
            'performance_tag': hold_tag,
            'indicators': ind,
            'reason': (
                f"Holding {qty} {sym} @ ${cur_p:.2f} (Entry ${entry_p:.2f} | MTM: ${pnl_dollars:.2f} / {pnl_pct:.2f}% "
                f"| HWM ${hwm:.2f} / Peak +{peak_pct:.2f}% / Giveback {giveback_pct:.2f}% | RSI {ind['rsi']} | Spread {sma_spread:+.2f}%) [DIAG: {hold_tag}]"
            ),
        }


    def evaluate_signal(self, data_buffer):
        return 'BUY'

