"""
Streaming statistics module.
Provides online algorithms for rolling means, variances, and sums over time-based windows.
"""
from collections import deque
from typing import Deque, Tuple, Optional, Dict
import math

class Welford:
    """
    Numerically stable online mean and variance tracker (infinite history).
    """
    def __init__(self):
        self.count = 0
        self.mean = 0.0
        self.M2 = 0.0

    def update(self, x: float):
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        delta2 = x - self.mean
        self.M2 += delta * delta2

    @property
    def variance(self) -> float:
        if self.count < 2:
            return 0.0
        # Clamp M2 to 0 to avoid negative variance due to floating point errors
        return max(0.0, self.M2) / (self.count - 1)

    @property
    def std(self) -> float:
        return math.sqrt(self.variance)


class TimeBasedRolling:
    """
    Base class for time-based rolling statistics.
    Manages a deque of (timestamp, value) and handles eviction.
    """
    def __init__(self, window_seconds: float):
        self.window_seconds = window_seconds
        self.deque: Deque[Tuple[float, float]] = deque()

    def _evict(self, current_time: float):
        threshold = current_time - self.window_seconds
        while self.deque and self.deque[0][0] <= threshold:
            self._remove_oldest()

    def _remove_oldest(self):
        # To be implemented by subclasses to update stats before popping
        self.deque.popleft()

    def update(self, value: float, timestamp: float):
        self._evict(timestamp)
        self._add_new(value, timestamp)

    def _add_new(self, value: float, timestamp: float):
        self.deque.append((timestamp, value))


class TimeBasedRollingSum(TimeBasedRolling):
    """
    Rolling sum and mean over a time window.
    """
    def __init__(self, window_seconds: float):
        super().__init__(window_seconds)
        self.current_sum = 0.0

    def _remove_oldest(self):
        ts, val = self.deque.popleft()
        self.current_sum -= val

    def _add_new(self, value: float, timestamp: float):
        self.deque.append((timestamp, value))
        self.current_sum += value

    @property
    def sum(self) -> float:
        return self.current_sum

    @property
    def mean(self) -> float:
        if not self.deque:
            return 0.0
        return self.current_sum / len(self.deque)


class RollingWelford(TimeBasedRolling):
    """
    Rolling mean and variance over a time window using Welford's algorithm
    with support for removing expired values.
    """
    def __init__(self, window_seconds: float):
        super().__init__(window_seconds)
        self.count = 0
        self.mean = 0.0
        self.M2 = 0.0

    def _add_new(self, x: float, timestamp: float):
        self.deque.append((timestamp, x))
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        delta2 = x - self.mean
        self.M2 += delta * delta2

    def _remove_oldest(self):
        ts, x = self.deque.popleft()
        if self.count <= 1:
            self.count = 0
            self.mean = 0.0
            self.M2 = 0.0
            return

        # Welford removal logic
        # old_mean = mean_prev
        # new_mean = (n * old_mean - x) / (n - 1)
        # M2_new = M2_old - (x - old_mean) * (x - new_mean)
        
        old_mean = self.mean
        self.mean = (self.count * old_mean - x) / (self.count - 1)
        self.M2 -= (x - old_mean) * (x - self.mean)
        self.count -= 1

    @property
    def variance(self) -> float:
        if self.count < 2:
            return 0.0
        # Clamp M2 to 0 to avoid negative variance due to floating point errors
        return max(0.0, self.M2) / (self.count - 1)

    @property
    def std(self) -> float:
        return math.sqrt(self.variance)


class RollingSum(TimeBasedRollingSum):
    """Alias for TimeBasedRollingSum."""
    pass


class RegimeStats(TimeBasedRolling):
    """
    Tracks time spent in different regimes (e.g. tight vs wide spread).
    """
    def __init__(self, window_seconds: float):
        super().__init__(window_seconds)
        self.total_duration = 0.0
        self.regime_durations: Dict[str, float] = {}
        self.last_update_time: Optional[float] = None
        self.current_regime: Optional[str] = None

    def update(self, regime: str, timestamp: float):
        self._evict(timestamp)
        
        if self.last_update_time is not None:
            duration = timestamp - self.last_update_time
            if duration > 0 and self.current_regime is not None:
                self._add_duration(self.current_regime, duration, timestamp)
        
        self.current_regime = regime
        self.last_update_time = timestamp

    def _add_duration(self, regime: str, duration: float, timestamp: float):
        self.deque.append((timestamp, (regime, duration)))
        self.regime_durations[regime] = self.regime_durations.get(regime, 0.0) + duration
        self.total_duration += duration

    def _remove_oldest(self):
        ts, (regime, duration) = self.deque.popleft()
        self.regime_durations[regime] -= duration
        self.total_duration -= duration
        if self.regime_durations[regime] <= 0:
            del self.regime_durations[regime]

    def get_regime_fraction(self, regime: str) -> float:
        if self.total_duration <= 0:
            return 0.0
        return self.regime_durations.get(regime, 0.0) / self.total_duration

