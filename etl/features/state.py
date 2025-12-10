"""
Symbol state management and bar building.
Orchestrates feature extraction, streaming statistics, and bar aggregation.
"""
import math
import logging
from typing import Dict, List, Optional, Any, Tuple, Deque
from dataclasses import dataclass, field
from collections import deque
from datetime import datetime, timezone
import numpy as np

from etl.processors.time_utils import add_time_fields
from .snapshot import extract_orderbook_features
from .streaming import RollingWelford, TimeBasedRollingSum, RollingSum, RegimeStats

logger = logging.getLogger(__name__)

@dataclass
class StateConfig:
    """Configuration for SymbolState."""
    # Horizons for rolling stats (seconds)
    horizons: List[int] = field(default_factory=lambda: [1, 5, 30, 60])
    # Bar durations to build (seconds)
    bar_durations: List[int] = field(default_factory=lambda: [1, 5, 30, 60])
    # High-frequency emission interval (seconds)
    hf_emit_interval: float = 1.0
    # Feature extraction settings
    max_levels: int = 10
    ofi_levels: int = 5
    bands_bps: List[int] = field(default_factory=lambda: [5, 10, 25, 50])
    keep_raw_arrays: bool = False
    # Spread regime thresholds (e.g. tight if spread <= X bps)
    tight_spread_threshold: float = 0.0001 # 1 bp

class BarBuilder:
    """
    Accumulates data for a single time bar (OHLCV + stats).
    """
    def __init__(self, duration: int):
        self.duration = duration
        self.current_bar_start: Optional[float] = None
        self.bar_stats: Dict[str, Any] = {}
        self._reset()

    def _reset(self):
        self.bar_stats = {
            'open': None, 'high': -float('inf'), 'low': float('inf'), 'close': None,
            'count': 0,
            'sum_spread': 0.0,
            'sum_rel_spread': 0.0,
            'sum_ofi': 0.0,
            'sum_l1_imb': 0.0,
            'min_spread': float('inf'),
            'max_spread': -float('inf'),
            'rv_welford': RollingWelford(self.duration * 2), # Just for variance calc within bar
        }
        # Reset welford manually since we use it as an accumulator here
        self.bar_stats['rv_welford'].count = 0
        self.bar_stats['rv_welford'].mean = 0.0
        self.bar_stats['rv_welford'].M2 = 0.0

    def update(self, timestamp: float, mid: float, spread: float, rel_spread: float, ofi: float, l1_imb: float, log_ret: float) -> Optional[Dict[str, Any]]:
        """
        Update bar state. Returns a completed bar dict if the bar closed.
        """
        # Initialize start time if needed (align to duration grid)
        if self.current_bar_start is None:
            self.current_bar_start = (math.floor(timestamp / self.duration)) * self.duration

        # Check if we moved to a new bar
        completed_bar = None
        if timestamp >= self.current_bar_start + self.duration:
            # Finalize current bar
            if self.bar_stats['count'] > 0:
                completed_bar = self._finalize_bar(self.current_bar_start)
            
            # Start new bar (handling gaps if necessary, but for now just snap to current)
            self.current_bar_start = (math.floor(timestamp / self.duration)) * self.duration
            self._reset()

        # Update stats
        s = self.bar_stats
        if s['open'] is None:
            s['open'] = mid
        s['high'] = max(s['high'], mid)
        s['low'] = min(s['low'], mid)
        s['close'] = mid
        s['count'] += 1
        
        if not math.isnan(spread):
            s['sum_spread'] += spread
            s['min_spread'] = min(s['min_spread'], spread)
            s['max_spread'] = max(s['max_spread'], spread)
        
        if not math.isnan(rel_spread):
            s['sum_rel_spread'] += rel_spread
            
        if not math.isnan(ofi):
            s['sum_ofi'] += ofi
            
        if not math.isnan(l1_imb):
            s['sum_l1_imb'] += l1_imb
            
        # Update realized variance within bar
        # We use RollingWelford but just as a standard Welford accumulator here
        # We pass timestamp but it doesn't matter for accumulation if we don't evict
        s['rv_welford']._add_new(log_ret, timestamp)

        return completed_bar

    def _finalize_bar(self, start_time: float) -> Dict[str, Any]:
        s = self.bar_stats
        count = s['count']
        return {
            'timestamp': start_time,
            'duration': self.duration,
            'open': s['open'],
            'high': s['high'],
            'low': s['low'],
            'close': s['close'],
            'mean_spread': s['sum_spread'] / count if count > 0 else None,
            'mean_relative_spread': s['sum_rel_spread'] / count if count > 0 else None,
            'mean_l1_imbalance': s['sum_l1_imb'] / count if count > 0 else None,
            'sum_ofi': s['sum_ofi'],
            'min_spread': s['min_spread'] if count > 0 else None,
            'max_spread': s['max_spread'] if count > 0 else None,
            'realized_variance': s['rv_welford'].variance,
            'count': count
        }

class SymbolState:
    """
    Manages state for a single symbol:
    - Last snapshot info
    - Rolling statistics (Returns, OFI, Volatility)
    - Bar builders
    """
    def __init__(self, symbol: str, exchange: str, config: StateConfig):
        self.symbol = symbol
        self.exchange = exchange
        self.config = config
        
        # State
        self.last_snapshot_time = 0.0
        self.last_emit_time = 0.0
        self.prev_mid = None
        
        # L1 State
        self.prev_best_bid = None
        self.prev_best_ask = None
        self.prev_bid_size = None
        self.prev_ask_size = None
        
        # Multi-level State (Arrays of [price, size])
        self.prev_bids = None
        self.prev_asks = None
        
        # Acceleration State
        self.mid_history = deque(maxlen=3)  # t, t-1, t-2
        
        # Rolling Stats Containers
        # Map: horizon_sec -> RollingObject
        self.rolling_returns: Dict[int, RollingWelford] = {
            h: RollingWelford(h) for h in config.horizons
        }
        self.rolling_ofi: Dict[int, RollingSum] = {
            h: RollingSum(h) for h in config.horizons
        }
        
        # Trade Flow Imbalance (TFI)
        self.rolling_buy_vol: Dict[int, RollingSum] = {
            h: RollingSum(h) for h in config.horizons
        }
        self.rolling_sell_vol: Dict[int, RollingSum] = {
            h: RollingSum(h) for h in config.horizons
        }
        
        # Spread Regime Stats (e.g. 30s horizon)
        self.spread_regime = RegimeStats(30)
        
        # Example: Rolling depth variance for 0-5bps band (using 30s horizon as default)
        self.rolling_depth_var = RollingWelford(30) 

        # Bar Builders
        self.bar_builders = [BarBuilder(d) for d in config.bar_durations]

    def process_trade(self, trade: Dict[str, Any]):
        """
        Process a trade message to update rolling trade volume stats.
        """
        try:
            ts_ms = trade.get('timestamp') or trade.get('collected_at')
            if not ts_ms:
                return
            
            ts_sec = ts_ms / 1000.0
            amount = float(trade.get('amount', 0))
            side = trade.get('side', '').lower() # 'buy' or 'sell'
            
            if side == 'buy':
                for tracker in self.rolling_buy_vol.values():
                    tracker.update(amount, ts_sec)
                # Update sell trackers with 0 to keep time moving? 
                # RollingSum handles time decay on update, but if no update comes, it might be stale.
                # Ideally we update all trackers with 0 if no event, but here we just update the relevant one.
                # The RollingSum implementation usually handles decay on read or update.
                # Let's assume we just update the active side.
                for tracker in self.rolling_sell_vol.values():
                    tracker.update(0, ts_sec)
                    
            elif side == 'sell':
                for tracker in self.rolling_sell_vol.values():
                    tracker.update(amount, ts_sec)
                for tracker in self.rolling_buy_vol.values():
                    tracker.update(0, ts_sec)
                    
        except Exception as e:
            logger.error(f"Error processing trade: {e}")

    def process_snapshot(self, snapshot: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Process a new orderbook snapshot.
        
        Returns:
            hf_row: High-frequency feature row (or None if not time to emit)
            bar_rows: List of completed bars
        """
        # 1. Extract Structural Features
        features = extract_orderbook_features(
            snapshot, 
            max_levels=self.config.max_levels,
            bands_bps=self.config.bands_bps,
            keep_raw_arrays=self.config.keep_raw_arrays
        )
        
        # Normalize timestamp to seconds for internal logic
        ts_ms = features['timestamp']
        ts_sec = ts_ms / 1000.0
        
        mid = features['mid_price']
        best_bid = features['best_bid']
        best_ask = features['best_ask']
        bid_size = features['bid_size_L0']
        ask_size = features['ask_size_L0']
        microprice = features['microprice']
        
        # 2. Calculate Deltas (Returns, OFI)
        log_ret = 0.0
        ofi = 0.0
        ofi_multi = 0.0
        
        # Reconstruct current top levels for Multi-level OFI
        current_bids = []
        current_asks = []
        for i in range(self.config.ofi_levels):
            bp = features.get(f'bid_price_L{i}')
            bs = features.get(f'bid_size_L{i}')
            ap = features.get(f'ask_price_L{i}')
            as_ = features.get(f'ask_size_L{i}')
            
            if bp is not None and not math.isnan(bp):
                current_bids.append((bp, bs))
            if ap is not None and not math.isnan(ap):
                current_asks.append((ap, as_))

        if self.prev_mid is not None and mid > 0 and self.prev_mid > 0:
            log_ret = math.log(mid / self.prev_mid)
            
            # L1 OFI Calculation (Cont's method)
            # Bid side
            if best_bid > self.prev_best_bid:
                ofi_bid = bid_size
            elif best_bid < self.prev_best_bid:
                ofi_bid = -self.prev_bid_size
            else: # Equal
                ofi_bid = bid_size - self.prev_bid_size
                
            # Ask side
            if best_ask > self.prev_best_ask:
                ofi_ask = -self.prev_ask_size
            elif best_ask < self.prev_best_ask:
                ofi_ask = ask_size
            else: # Equal
                ofi_ask = ask_size - self.prev_ask_size
                
            ofi = ofi_bid - ofi_ask # Standard: e_t = e_b_t - e_a_t
            
            # Multi-level OFI
            if self.prev_bids and self.prev_asks:
                ofi_multi_bid = 0.0
                ofi_multi_ask = 0.0
                
                # Sum OFI across levels
                # Note: This assumes levels correspond by index, which is a simplification.
                # A strict implementation matches by price, but for "aggregate impact" 
                # summing the per-level OFI contribution is a common proxy.
                
                # Bids
                for i in range(min(len(current_bids), len(self.prev_bids))):
                    curr_p, curr_s = current_bids[i]
                    prev_p, prev_s = self.prev_bids[i]
                    
                    if curr_p > prev_p:
                        ofi_multi_bid += curr_s
                    elif curr_p < prev_p:
                        ofi_multi_bid += -prev_s
                    else:
                        ofi_multi_bid += curr_s - prev_s
                        
                # Asks
                for i in range(min(len(current_asks), len(self.prev_asks))):
                    curr_p, curr_s = current_asks[i]
                    prev_p, prev_s = self.prev_asks[i]
                    
                    if curr_p > prev_p: # Higher ask price = retreat = positive for price? No.
                        # Higher ask price means liquidity moved away (up).
                        # OFI sign convention: 
                        # Increase in bid size -> +OFI (Bullish)
                        # Increase in ask size -> -OFI (Bearish)
                        # Ask price moves UP -> Bullish (OFI > 0)?
                        # Let's stick to Cont's definition per level:
                        # e_k,t = I(p_k,t > p_k,t-1) * q_k,t 
                        #       + I(p_k,t = p_k,t-1) * (q_k,t - q_k,t-1)
                        #       + I(p_k,t < p_k,t-1) * (-q_k,t-1)
                        # For Asks, we subtract this term.
                        
                        ofi_level = -prev_s # Price moved up (away)
                    elif curr_p < prev_p:
                        ofi_level = curr_s # Price moved down (closer)
                    else:
                        ofi_level = curr_s - prev_s
                        
                    ofi_multi_ask += ofi_level

                ofi_multi = ofi_multi_bid - ofi_multi_ask

        # 3. Acceleration & Velocity
        self.mid_history.append((ts_sec, mid))
        mid_velocity = 0.0
        mid_accel = 0.0
        
        if len(self.mid_history) >= 2:
            t1, p1 = self.mid_history[-1]
            t0, p0 = self.mid_history[-2]
            dt = t1 - t0
            if dt > 0:
                mid_velocity = (p1 - p0) / dt
                
        if len(self.mid_history) == 3:
            t2, p2 = self.mid_history[-1]
            t1, p1 = self.mid_history[-2]
            t0, p0 = self.mid_history[-3]
            
            dt1 = t2 - t1
            dt0 = t1 - t0
            
            if dt1 > 0 and dt0 > 0:
                v1 = (p2 - p1) / dt1
                v0 = (p1 - p0) / dt0
                mid_accel = (v1 - v0) / ((dt1 + dt0) / 2.0)

        features['mid_velocity'] = mid_velocity
        features['mid_accel'] = mid_accel
        features['ofi_multi'] = ofi_multi

        # 4. Update Rolling Stats & TFI
        for h, tracker in self.rolling_returns.items():
            tracker.update(log_ret, ts_sec)
            features[f'rv_{h}s'] = tracker.std
            
        for h, tracker in self.rolling_ofi.items():
            tracker.update(ofi, ts_sec)
            features[f'ofi_sum_{h}s'] = tracker.sum
            
        # TFI Features
        for h in self.config.horizons:
            buy_vol = self.rolling_buy_vol[h].sum
            sell_vol = self.rolling_sell_vol[h].sum
            total_vol = buy_vol + sell_vol
            tfi = (buy_vol - sell_vol) / total_vol if total_vol > 0 else 0.0
            features[f'tfi_{h}s'] = tfi
            features[f'trade_vol_{h}s'] = total_vol

        # Time Features
        # Use robust time_utils logic to add all time components
        # We use the 'datetime' field if available (ISO string), or fallback to 'timestamp' (ms int)
        # Note: extract_orderbook_features already ensures 'timestamp' is present
        
        # Create a temporary record for time parsing
        time_record = {
            'timestamp': features.get('timestamp'),
            'datetime': features.get('datetime') # Might be None if not in snapshot
        }
        
        # Enrich with time fields
        # time_utils.add_time_fields now handles int/float timestamps directly
        add_time_fields(time_record, 'datetime', 'timestamp')
        
        # Copy relevant time fields back to features
        # We want specific cyclic/categorical features for ML
        features['hour_of_day'] = time_record.get('hour')
        features['day_of_week'] = time_record.get('day_of_week')
        features['is_weekend'] = 1 if time_record.get('is_weekend') else 0
        features['minute_of_hour'] = time_record.get('minute')
        
        # Update depth variance (example: 0-5bps band)
        depth_0_5 = features.get('bid_vol_band_0_5bps', 0) + features.get('ask_vol_band_0_5bps', 0)
        self.rolling_depth_var.update(depth_0_5, ts_sec)
        features['depth_0_5bps_sigma'] = self.rolling_depth_var.std
        
        # Update Spread Regime
        rel_spread = features.get('relative_spread', 0)
        regime = "tight" if rel_spread <= self.config.tight_spread_threshold else "wide"
        self.spread_regime.update(regime, ts_sec)
        features['spread_regime_tight_frac'] = self.spread_regime.get_regime_fraction("tight")

        # Microprice drift
        if mid > 0:
            features['micro_minus_mid'] = microprice - mid
        else:
            features['micro_minus_mid'] = np.nan

        # 4. Update State
        self.prev_mid = mid
        self.prev_best_bid = best_bid
        self.prev_best_ask = best_ask
        self.prev_bid_size = bid_size
        self.prev_ask_size = ask_size
        self.prev_bids = current_bids
        self.prev_asks = current_asks
        self.last_snapshot_time = ts_sec

        # 5. Bar Building
        completed_bars = []
        for builder in self.bar_builders:
            bar = builder.update(
                ts_sec, 
                mid, 
                features['spread'], 
                features['relative_spread'], 
                ofi,
                features['imbalance_L1'],
                log_ret
            )
            if bar:
                bar['symbol'] = self.symbol
                bar['exchange'] = self.exchange
                bar['date'] = features.get('date')
                completed_bars.append(bar)

        # 6. HF Emission Check
        hf_row = None
        if ts_sec - self.last_emit_time >= self.config.hf_emit_interval:
            hf_row = features
            # Add derived rolling features to the emitted row
            hf_row['log_return_step'] = log_ret
            hf_row['ofi_step'] = ofi
            self.last_emit_time = ts_sec

        return hf_row, completed_bars
