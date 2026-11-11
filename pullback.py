"""
3m 回调入场模块（Pullback Entry）

策略：15m K线拐头出信号后，不立即开仓，而是切换到 3m 级别，
     等待价格回调到 3m 的 EMA20（触碰/穿越），再以当时 EMA20 值挂限价单入场。

核心概念：
  - 观察期：15m 信号K线收盘后 N 秒（默认 1800s = 30min）内，
    若 3m 价格没回调到 3m EMA20，则放弃该信号，不追高/追低。
  - 触碰判定（3m K线收盘后确认）：
      做多：3m K线 lowest <= 3m EMA20（价格跌下来碰到了均线）
      做空：3m K线 highest >= 3m EMA20（价格涨上去碰到了均线）
  - 入场价 = 触碰那根 3m K线收盘时刻算出的 3m EMA20 值（固定，不再变）。

依赖：
  - market.get_3m_klines 拉取 3m K线
  - sliding.SlidingEMA  复用滑动窗口做 3m EMA20 计算

本模块不直接下单，只负责「判定是否该入场 + 计算出 entry/stop/tp」，
下单动作由 trader.py 调用回调完成，保持职责单一。
"""
from dataclasses import dataclass
from typing import Optional, Dict

from sliding import SlidingEMA


@dataclass
class PendingSignal:
    """一笔「观察中」的 15m 拐头信号（等待 3m 回调入场）。"""
    symbol: str
    direction: str          # "LONG" / "SHORT"
    # 15m 信号K线价格（用于止盈止损计算）
    sig_open: float
    sig_close: float
    sig_low: float          # 信号K线最低价（做多止损比较用）
    sig_high: float         # 信号K线最高价（做空止损比较用）
    prev_open: float        # 信号K线前一根的开盘价（止损比较用）
    sig_close_ms: int       # 信号K线 closeTime(ms)，观察期计时起点
    entered: bool = False   # 是否已触发入场（防重复）


class PullbackManager:
    """管理所有「观察中」的 15m 信号，维护独立的 3m EMA20 窗口并执行触碰判定。"""

    def __init__(self, ema_period: int = 20, window_size: int = 300,
                 warmup_limit: int = 300, watch_seconds: int = 1800,
                 risk_reward: float = 2.0):
        self.ema_period = ema_period
        self.window_size = window_size
        self.warmup_limit = warmup_limit
        self.watch_seconds = watch_seconds
        self.risk_reward = risk_reward

        # 3m 滑动窗口 EMA（独立于 15m 窗口）
        self.sema_3m = SlidingEMA(period=ema_period, window_size=window_size,
                                  bar_interval_ms=3 * 60 * 1000)
        # 观察中的信号：key = (symbol, direction)
        self.pending: Dict[tuple, PendingSignal] = {}

    # ── 观察状态管理 ──

    def _key(self, symbol: str, direction: str) -> tuple:
        return (symbol, direction)

    def add_signal(self, symbol: str, direction: str, sig_open: float,
                   sig_close: float, sig_low: float, sig_high: float,
                   prev_open: float, sig_close_ms: int) -> bool:
        """登记一个 15m 拐头信号进入观察期。返回 False 表示该币同方向已在观察。"""
        k = self._key(symbol, direction)
        if k in self.pending:
            return False
        self.pending[k] = PendingSignal(
            symbol=symbol, direction=direction,
            sig_open=sig_open, sig_close=sig_close, sig_low=sig_low,
            sig_high=sig_high, prev_open=prev_open, sig_close_ms=sig_close_ms,
        )
        # 预热该 symbol 的 3m 窗口（若尚未初始化）
        self._ensure_3m_ready(symbol)
        return True

    def remove(self, symbol: str, direction: str):
        self.pending.pop(self._key(symbol, direction), None)

    # ── 3m 窗口预热 ──

    def _ensure_3m_ready(self, symbol: str):
        """为 symbol 预热 3m 滑动窗口（幂等）。"""
        if self.sema_3m.ready(symbol):
            return
        try:
            from market import get_3m_klines
            bars = get_3m_klines(symbol, limit=self.warmup_limit)
            if not bars:
                return
            closes = [b["c"] for b in bars]
            bar_times = [b["t"] for b in bars]
            self.sema_3m.init_symbol(symbol, closes, bar_times)
        except Exception as e:
            print(f"  [3m预热] {symbol} 失败: {e}")

    # ── 触碰判定 ──

    def check_touch(self, symbol: str, direction: str, now_ms: int) -> Optional[dict]:
        """
        检查某观察中的 symbol 是否已触发 3m 回调用入场。

        返回 None 表示未触发（仍继续观察）；
        返回 dict 表示触发，含 entry/stop/tp 三价（未对齐，调用方负责对齐）。

        同时处理「观察期超时」：超时返回特殊标记。
        """
        k = self._key(symbol, direction)
        ps = self.pending.get(k)
        if ps is None:
            return None
        if ps.entered:
            return None

        # 观察期超时检查：从信号K线收盘起算（达到 watch_seconds 即超时）
        if now_ms - ps.sig_close_ms >= self.watch_seconds * 1000:
            return {"__timeout__": True}

        # 拉最新 3m K线。拉取足够多的根数（窗口长度），确保能够：
        #  1) 重建信号收盘前已存在的 3m EMA20 基线；
        #  2) 逐根推进「信号收盘后」的已收盘 3m K线做触碰判定。
        # 若只拉 3 根，会因 push_close 的去重而跳过 K线，导致 EMA 滞后/错乱
        # （实盘中 ONE 入场价 0.00171389 vs 正确 0.0015698，即由此造成）。
        try:
            from market import get_3m_klines
            bars = get_3m_klines(symbol, limit=self.window_size)
        except Exception as e:
            print(f"  [3m拉取] {symbol} 失败: {e}")
            return None
        if not bars:
            return None

        # 逐根推进 3m 窗口，并对「已收盘的触碰 K线」做判定。
        # 关键：跳过「进行中」的 3m K线（其 closeTime 仍在未来）——
        # 它的收盘价是实时快照、不稳定，既不能算 EMA 也不能做触碰判定，
        # 严格遵循「3m K线收盘后确认」的语义。
        for b in bars:
            close_ms = int(b["t"]) + 3 * 60 * 1000
            is_closed = close_ms <= now_ms
            if not is_closed:
                continue  # 进行中的 K线：本轮跳过，等它收盘

            # 只对「信号K线收盘之后」的已收盘 3m K线做触碰判定
            if b["t"] < ps.sig_close_ms:
                # 早于信号K线收盘的 K线仍需推进 EMA（保持基线正确），但不判定触碰
                self.sema_3m.push_close(symbol, b["t"], b["c"])
                continue

            # 触碰判定用「推进前的 EMA20」作为参照均线，避免用「触碰K线自身的收盘价」
            # 推进后的 EMA 做判定（那会把入场价拉偏，见 ONE/AKE 的异常高价）。
            ema_before = self.sema_3m.get_ema(symbol)
            # 用本根收盘价推进 EMA（供后续 K线继续使用）
            ema_after = self.sema_3m.push_close(symbol, b["t"], b["c"])
            if ema_before is None:
                continue

            low = b["l"]
            high = b["h"]

            if direction == "LONG":
                touched = low <= ema_before
            else:  # SHORT
                touched = high >= ema_before

            if touched:
                # 触发：入场价 = 触碰那根 3m K线【收盘后】算出的 EMA20
                # （含触碰 K线自身的收盘价，符合策略注释第 13 行语义）。
                # 若 push_close 因去重跳过返回 None（该 K线未推进），则退回到
                # 触碰前的 EMA20，避免把 None 当作价格参与后续运算。
                entry = ema_after if ema_after is not None else ema_before
                ps.entered = True
                # 计算止盈止损（止盈以入场价 entry 为基准）
                stop, tp, half_body = self._compute_stop_tp(direction, ps, entry)
                return {"entry": entry, "stop": stop, "tp": tp,
                        "half_body": half_body, "touch_bar_time": b["t"]}

        return None

    def _compute_stop_tp(self, direction: str, ps: PendingSignal, entry: float) -> tuple:
        """
        计算止盈/止损。

        止盈 = 入场价 ± 信号K线实体的一半（做多加、做空减）。
        止损 = min(信号K线最低价, 前一根开盘价)（做多取更低），
               max(信号K线最高价, 前一根开盘价)（做空取更高）。
        返回 (stop, tp, half_body)，未对齐。half_body 用于成交后用真实成交价重算止盈。
        """
        o, c, prev = ps.sig_open, ps.sig_close, ps.prev_open
        half_body = (c - o) / 2.0 if direction == "LONG" else (o - c) / 2.0

        if direction == "LONG":
            tp = entry + half_body
            stop = min(ps.sig_low, prev)
        else:  # SHORT
            tp = entry - half_body
            stop = max(ps.sig_high, prev)
        return stop, tp, half_body

    # ── 生命周期 ──

    def reset(self, symbol: Optional[str] = None):
        """清理观察状态与 3m 窗口。不传 symbol 则清空全部。"""
        if symbol is None:
            self.pending.clear()
            for s in list(self.sema_3m._closes.keys()):
                self.sema_3m.reset(s)
        else:
            # 清掉该 symbol 所有方向的观察信号
            for direction in ("LONG", "SHORT"):
                self.pending.pop((symbol, direction), None)
            self.sema_3m.reset(symbol)
