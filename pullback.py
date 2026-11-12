"""
3m 回调入场模块（Pullback Entry）

策略：15m K线拐头出信号后，不立即开仓，而是切换到 3m 级别，
     等待价格回调到 3m 的 EMA（触碰/穿越），再以当时 EMA 值挂限价单入场。

核心概念：
  - 观察期：15m 信号K线收盘后 N 秒（默认 1800s = 30min）内，
    若 3m 价格没回调到 3m EMA，则放弃该信号，不追高/追低。
  - 触碰判定（3m K线收盘后确认）：
      做多：3m K线 lowest <= 3m EMA（价格跌下来碰到了均线）
      做空：3m K线 highest >= 3m EMA（价格涨上去碰到了均线）
  - 入场价 = 触碰那根 3m K线收盘时刻算出的 3m EMA 值（固定，不再变）。

依赖：
  - market.get_3m_klines 拉取 3m K线
  - sliding.SlidingEMA  复用滑动窗口做 3m EMA 计算

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
    """管理所有「观察中」的 15m 信号，维护独立的 3m EMA 窗口并执行触碰判定。"""

    def __init__(self, ema_period: int = 15, window_size: int = 300,
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
        # 若该 symbol 已无任何方向在观察，则清掉其残留的 3m 窗口状态，
        # 避免下一笔同 symbol 信号沿用旧的 EMA（ready() 幂等会跳过重 init），
        # 导致触碰判定的均线基准错误（如 ZEC 做空沿用了更早 LONG 的遗留 EMA）。
        if not self._has_pending(symbol):
            self.sema_3m.reset(symbol)

    def _has_pending(self, symbol: str) -> bool:
        """该 symbol 是否仍有任意方向在观察中。"""
        return any(sym == symbol for (sym, _d) in self.pending)

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

        # 拉全量 3m K线（覆盖信号收盘前基线 + 信号收盘后的回踩判定区间）。
        try:
            from market import get_3m_klines
            bars = get_3m_klines(symbol, limit=self.window_size)
        except Exception as e:
            print(f"  [3m拉取] {symbol} 失败: {e}")
            return None
        if not bars:
            return None

        # 关键：触碰判定必须用「独立重建的、按时间顺序推进」的 EMA，且一旦触碰就返回，
        # 绝不复用跨轮累积推进的 sema_3m 持久状态——否则前几轮已把 EMA 推进到很晚，
        # 本轮回头判定「信号收盘后早期 K线」时，用的 EMA 已被未来 K线污染（见 QUSDT
        # 做空在 22:36 才触发、入场价 0.0286 却是 22:15 那根的 EMA，实为 22:33 的值）。

        # 1) 重建「信号收盘时刻」的 EMA 基线：用信号收盘前已收盘的 3m 收盘价独立计算。
        baseline = SlidingEMA(period=self.ema_period,
                              window_size=self.window_size,
                              bar_interval_ms=3 * 60 * 1000)
        base_closes = [b["c"] for b in bars
                       if b["t"] + 3 * 60 * 1000 <= ps.sig_close_ms]
        base_times = [b["t"] for b in bars
                      if b["t"] + 3 * 60 * 1000 <= ps.sig_close_ms]
        baseline.init_symbol(symbol, base_closes, base_times)

        # 2) 从「信号收盘后」起，按时间顺序逐根推进已收盘 3m K线，触碰即返回。
        for b in bars:
            close_ms = int(b["t"]) + 3 * 60 * 1000
            if close_ms > now_ms:
                continue  # 进行中/未来的 K线：本轮跳过，等它收盘
            if b["t"] < ps.sig_close_ms:
                continue  # 信号收盘前的 K线：已包含在 baseline 里，跳过

            ema_before = baseline.get_ema(symbol)
            if ema_before is None:
                # 基线不足（历史 K线少于 period），继续推进直到足够
                baseline.push_close(symbol, b["t"], b["c"])
                continue

            low = b["l"]
            high = b["h"]
            if direction == "LONG":
                touched = low <= ema_before
            else:  # SHORT
                touched = high >= ema_before

            if touched:
                # 入场价 = 触碰那根 3m K线【收盘后】算出的 EMA（含触碰 K线自身收盘价）。
                ema_after = baseline.push_close(symbol, b["t"], b["c"])
                entry = ema_after if ema_after is not None else ema_before
                ps.entered = True
                stop, tp, half_body = self._compute_stop_tp(direction, ps, entry)
                return {"entry": entry, "stop": stop, "tp": tp,
                        "half_body": half_body, "touch_bar_time": b["t"]}

            # 未触碰：本轮 K线收盘价推进 EMA，供下一根继续判定
            baseline.push_close(symbol, b["t"], b["c"])

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
