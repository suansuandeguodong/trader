"""
EMA20 高位拐头向下信号模块（15m K线）—— 与 reversal.py 对称的向下版本

规则：
  - EMA20 基于 15m K线收盘价计算
  - 拐头向下：某根 K 线收盘后，其 EMA20 < 前一根的 EMA20
  - 前置条件：该拐头 K 线之前，连续「至少 MIN_UP_BARS（默认 13）根」
    EMA20 都是严格上涨的（后一根 > 前一根），不允许走平（相等）
  - 不需要价格配合（不看收盘价 vs EMA20、不看阴阳线）

实现为「状态机」：
  - 每个 symbol 有一个「连续严格上涨计数」up_count
  - 每根 15m K 线收盘，计算新的 EMA20 并更新：
      - 若新 EMA20 > 前 EMA20 → 严格上涨，up_count += 1
      - 若新 EMA20 == 前 EMA20 → 走平，up_count 清零（打断连续性）
      - 若新 EMA20 < 前 EMA20 → 拐头：
          若 up_count >= MIN_UP_BARS → 触发信号（并复位 up_count=0）
          否则 → 只是普通下跌，up_count 复位为 0
"""

from typing import List, Optional


def calc_ema(values: List[float], period: int) -> Optional[float]:
    """计算当前 EMA。values 旧→新。公式 EMA = Price×α + EMA_prev×(1-α)"""
    if len(values) < period:
        return None
    alpha = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    for i in range(period, len(values)):
        ema = values[i] * alpha + ema * (1.0 - alpha)
    return ema


def calc_ema_series(values: List[float], period: int) -> List[Optional[float]]:
    """
    计算 EMA 序列，返回与 values 等长的列表，前 period-1 位为 None。
    用于预热时逐根还原状态机 up_count。
    """
    n = len(values)
    result: List[Optional[float]] = [None] * n
    if n < period:
        return result

    alpha = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    result[period - 1] = ema

    for i in range(period, n):
        ema = values[i] * alpha + ema * (1.0 - alpha)
        result[i] = ema

    return result


class EMA20ReversalDown:
    """
    管理每个 symbol 的 EMA20 向下拐头信号状态机。

    up_count 的语义：
      「当前拐头前，已连续多少根 EMA20 是不跌的（>= 前一根）」。
      只要指数还在涨/平，计数累加；一旦下跌就触发判定，然后清零重新计数。
    """

    def __init__(self, min_up_bars: int = 13):
        self.min_up_bars = min_up_bars
        # {symbol: {"up_count": int, "prev_ema": float, "ema_history": [float, ...]}}
        self._state: dict = {}

    # ── 状态查询 ──

    def get_up_count(self, symbol: str) -> int:
        st = self._state.get(symbol)
        return st["up_count"] if st else 0

    def get_prev_ema(self, symbol: str) -> Optional[float]:
        st = self._state.get(symbol)
        return st["prev_ema"] if st else None

    def get_ema_history(self, symbol: str) -> list:
        """返回最近若干根 EMA 值（旧→新），最多 3 根。"""
        st = self._state.get(symbol)
        return list(st["ema_history"]) if st else []


    # ── 核心判定 ──

    def on_15m_close(self, symbol: str, curr_ema: float) -> bool:
        """
        每根 15m K 线收盘后调用，传入该 symbol 最新的 EMA20 值。

        返回 True 表示触发「拐头向下」信号（前置连续不跌根数满足要求）。
        返回 False 表示未触发（可能是继续上涨/平、或普通下跌）。

        首次见到该 symbol（无历史 prev_ema）时，仅记录基线，不判定。
        """
        if curr_ema is None or curr_ema <= 0:
            return False

        st = self._state.get(symbol)
        if st is None:
            # 首次：记录基线，不判定
            self._state[symbol] = {
                "up_count": 0,
                "prev_ema": curr_ema,
                "ema_history": [curr_ema],
            }
            return False

        prev_ema = st["prev_ema"]
        up_count = st["up_count"]

        # 更新 EMA 历史（保留最近 3 根）
        hist = st.get("ema_history", [])
        hist.append(curr_ema)
        if len(hist) > 3:
            hist = hist[-3:]

        if curr_ema < prev_ema:
            # 拐头向下
            triggered = up_count >= self.min_up_bars
            # 无论是否触发，下跌后重新计数
            self._state[symbol] = {
                "up_count": 0,
                "prev_ema": curr_ema,
                "ema_history": hist,
            }
            return triggered

        if curr_ema > prev_ema:
            # 严格上涨：计数 +1
            self._state[symbol] = {
                "up_count": up_count + 1,
                "prev_ema": curr_ema,
                "ema_history": hist,
            }
            return False

        # 走平（curr_ema == prev_ema）：打断「连续严格上涨」，计数清零重新计
        self._state[symbol] = {
            "up_count": 0,
            "prev_ema": curr_ema,
            "ema_history": hist,
        }
        return False

    # ── 预热还原 ──

    def warmup(self, symbol: str, closes: List[float], period: int = 20):
        """
        预热：用历史收盘价序列逐根还原状态机的 up_count 与 prev_ema。

        closes: 该 symbol 的历史已收盘 K线收盘价（旧→新）
        """
        ema_series = calc_ema_series(closes, period)
        for ema in ema_series:
            if ema is not None:
                self.on_15m_close(symbol, ema)

    # ── 生命周期 ──

    def reset(self, symbol: str):
        self._state.pop(symbol, None)

    def reset_all(self):
        self._state.clear()

    # ── 只读预判（用于「提前 10 秒快照」：判定拐头但不推进状态机）──

    def would_reverse(self, symbol: str, curr_ema: float) -> bool:
        """只读地判定：若此刻收盘，是否会触发「拐头向下」信号。

        与 on_15m_close 的判定逻辑一致，但**不写 _state**。
        """
        if curr_ema is None or curr_ema <= 0:
            return False
        st = self._state.get(symbol)
        if st is None:
            return False
        prev_ema = st["prev_ema"]
        up_count = st["up_count"]
        # 与 on_15m_close 中「curr_ema < prev_ema」分支的触发条件一致
        return curr_ema < prev_ema and up_count >= self.min_up_bars
