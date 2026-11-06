"""
EMA20 滑动窗口 + 增量更新模块

设计目标：
  - 本地缓存每个 symbol 固定长度的已收盘 K线收盘价（滑动窗口）
  - 每根新 K线收盘，append 新收盘价、滑出最旧一根，窗口长度恒定
  - EMA20 用「递推增量」更新（O(1)，与全量重算数学等价）：
      EMA_new = close_new × α + EMA_prev × (1 - α),  α = 2/(period+1)
  - 拐点判定复用 reversal.EMA20Reversal 状态机
"""

import time
from typing import List, Optional


class SlidingEMA:
    """
    每个 symbol 维护：
      - closes: 已收盘 K线收盘价（旧→新，长度 <= window_size）
      - ema: 当前 EMA 值（基于 closes）
      - last_bar_time: 最后一根已收盘 K线的 openTime，用于去重
    """

    def __init__(self, period: int = 20, window_size: int = 300,
                 bar_interval_ms: int = 15 * 60 * 1000):
        self.period = period
        self.window_size = window_size
        self.alpha = 2.0 / (period + 1)
        self.bar_interval_ms = bar_interval_ms  # 单根 K线时长，用于判断是否已收盘
        self._closes: dict = {}        # {symbol: [close, ...]}
        self._ema: dict = {}           # {symbol: float}
        self._last_bar_time: dict = {}  # {symbol: int(ms)}

    # ── 初始化 ──

    def init_symbol(self, symbol: str, closes: List[float], bar_times: List[int]):
        """
        启动/新币时载入历史已收盘 K线。
        closes: 收盘价序列（旧→新）
        bar_times: 对应 K线 openTime(ms)，长度须 == closes

        注意：币安 K线接口返回的最后一根通常是「进行中」的未收盘 K线，
        其收盘价是实时变动的快照，不能当作已收盘数据。这里将其丢弃，
        确保窗口内只含「已完整收盘」的 K线，last_bar_time 对齐正确。
        """
        if len(closes) < self.period:
            return

        # 去掉最后一根「进行中」的 K线（其 closeTime 仍在未来）
        now_ms = int(time.time() * 1000)
        if bar_times and len(bar_times) == len(closes) and len(closes) >= 1:
            last_close_ms = bar_times[-1] + self.bar_interval_ms
            if now_ms < last_close_ms:
                closes = closes[:-1]
                bar_times = bar_times[:-1]

        if len(closes) < self.period:
            return

        # 截断到窗口大小
        if len(closes) > self.window_size:
            closes = closes[-self.window_size:]
            bar_times = bar_times[-self.window_size:]

        self._closes[symbol] = list(closes)
        self._last_bar_time[symbol] = bar_times[-1] if bar_times else 0
        self._ema[symbol] = self._calc_ema(closes)

    def _calc_ema(self, closes: List[float]) -> Optional[float]:
        """从收盘价序列算 EMA（仅初始化时用）"""
        if len(closes) < self.period:
            return None
        ema = sum(closes[:self.period]) / self.period
        for i in range(self.period, len(closes)):
            ema = closes[i] * self.alpha + ema * (1 - self.alpha)
        return ema

    # ── 增量更新 ──

    def push_close(self, symbol: str, bar_time: int, close: float) -> Optional[float]:
        """
        喂入一根「已收盘」K线。自动去重（bar_time <= last 则忽略）。
        返回更新后的 EMA 值；无更新返回 None。
        """
        if symbol not in self._closes:
            return None

        if bar_time <= self._last_bar_time.get(symbol, 0):
            return None  # 旧/重复 K线

        closes = self._closes[symbol]
        closes.append(close)
        if len(closes) > self.window_size:
            closes.pop(0)  # 滑出最旧一根

        self._last_bar_time[symbol] = bar_time

        # 递推更新 EMA
        prev = self._ema.get(symbol)
        if prev is None:
            self._ema[symbol] = self._calc_ema(closes)
        else:
            self._ema[symbol] = close * self.alpha + prev * (1 - self.alpha)

        return self._ema[symbol]

    def get_ema(self, symbol: str) -> Optional[float]:
        return self._ema.get(symbol)

    def get_last_bar_time(self, symbol: str) -> int:
        return self._last_bar_time.get(symbol, 0)

    def ready(self, symbol: str) -> bool:
        return symbol in self._closes and symbol in self._ema

    def reset(self, symbol: str):
        """清理某 symbol 的缓存（候选池移除时调用，避免内存泄漏）。"""
        self._closes.pop(symbol, None)
        self._ema.pop(symbol, None)
        self._last_bar_time.pop(symbol, None)
