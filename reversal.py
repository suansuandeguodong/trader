"""
EMA20 低位拐头向上信号模块（15m K线）

规则：
  - EMA20 基于 15m K线收盘价计算
  - 拐头向上：某根 K 线收盘后，其 EMA20 > 前一根的 EMA20
  - 前置条件：该拐头 K 线之前，连续「至少 MIN_DOWN_BARS（默认 8）根」
    EMA20 都是不涨的（后一根 <= 前一根），即这段内没有任何一根上涨
  - 不需要价格配合（不看收盘价 vs EMA20、不看阴阳线）

实现为「状态机」：
  - 每个 symbol 有一个「连续不涨计数」down_count
  - 每根 15m K 线收盘，计算新的 EMA20 并更新：
      - 若新 EMA20 <= 前 EMA20 → 下跌或平，down_count += 1
      - 若新 EMA20 > 前 EMA20 → 拐头：
          若 down_count >= MIN_DOWN_BARS → 触发信号（并复位 down_count=0）
          否则 → 只是普通上涨，down_count 复位为 0
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
    用于预热时逐根还原状态机 down_count。
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


class EMA20Reversal:
    """
    管理每个 symbol 的 EMA20 拐头信号状态机。

    down_count 的语义：
      「当前拐头前，已连续多少根 EMA20 是不涨的（<= 前一根）」。
      只要指数还在跌/平，计数累加；一旦上涨就触发判定，然后清零重新计数。
    """

    def __init__(self, min_down_bars: int = 8):
        self.min_down_bars = min_down_bars
        # {symbol: {"down_count": int, "prev_ema": float, "ema_history": [float, ...]}}
        self._state: dict = {}

    # ── 状态查询 ──

    def get_down_count(self, symbol: str) -> int:
        st = self._state.get(symbol)
        return st["down_count"] if st else 0

    def get_prev_ema(self, symbol: str) -> Optional[float]:
        st = self._state.get(symbol)
        return st["prev_ema"] if st else None

    def get_ema_history(self, symbol: str) -> list:
        """返回最近若干根 EMA 值（旧→新），最多 3 根。"""
        st = self._state.get(symbol)
        return list(st["ema_history"]) if st else []

    def slope_ok(self, symbol: str) -> bool:
        """
        斜率过滤（理解 A）：
        向上拐头时，拐头上涨斜率 (E0 - E1) 要大于前一下跌斜率 (E2 - E1)，
        即 E0 > E2。这里 E0=当前EMA, E1=前一根EMA, E2=前二根EMA。
        需要至少 3 个 EMA 值，少于 3 个时视为不满足。
        """
        h = self.get_ema_history(symbol)
        if len(h) < 3:
            return False
        e2, e1, e0 = h[-3], h[-2], h[-1]
        # 向上拐头：E0 > E1 且 拐头斜率 > 前一下跌斜率
        # 即 (E0 - E1) > (E2 - E1) 等价于 E0 > E2
        return e0 > e1 and (e0 - e1) > (e2 - e1)

    # ── 核心判定 ──

    def on_15m_close(self, symbol: str, curr_ema: float) -> bool:
        """
        每根 15m K 线收盘后调用，传入该 symbol 最新的 EMA20 值。

        返回 True 表示触发「拐头向上」信号（前置连续不涨根数满足要求）。
        返回 False 表示未触发（可能是继续下跌/平、或普通上涨）。

        首次见到该 symbol（无历史 prev_ema）时，仅记录基线，不判定。
        """
        if curr_ema is None or curr_ema <= 0:
            return False

        st = self._state.get(symbol)
        if st is None:
            # 首次：记录基线，不计不涨数、不判定
            self._state[symbol] = {
                "down_count": 0,
                "prev_ema": curr_ema,
                "ema_history": [curr_ema],
            }
            return False

        prev_ema = st["prev_ema"]
        down_count = st["down_count"]

        # 更新 EMA 历史（保留最近 3 根）
        hist = st.get("ema_history", [])
        hist.append(curr_ema)
        if len(hist) > 3:
            hist = hist[-3:]

        if curr_ema > prev_ema:
            # 拐头向上
            triggered = down_count >= self.min_down_bars
            # 无论是否触发，上涨后重新计数
            self._state[symbol] = {
                "down_count": 0,
                "prev_ema": curr_ema,
                "ema_history": hist,
            }
            return triggered

        # 不涨（curr_ema <= prev_ema）：计数 +1
        self._state[symbol] = {
            "down_count": down_count + 1,
            "prev_ema": curr_ema,
            "ema_history": hist,
        }
        return False

    # ── 预热还原 ──

    def warmup(self, symbol: str, closes: List[float], period: int = 20):
        """
        预热：用历史收盘价序列逐根还原状态机的 down_count 与 prev_ema。

        closes: 该 symbol 的历史已收盘 K线收盘价（旧→新）
        效果：程序启动后，第一根新 K线收盘即可正确判断拐头，
              不受「预热时 down_count 从 0 起」的盲区影响。
        """
        ema_series = calc_ema_series(closes, period)
        for ema in ema_series:
            if ema is not None:
                # 逐根喂入状态机，精确累积连续不涨计数。
                # 注意：warmup 期间即使命中拐头也不应「触发」，
                # 我们只关心还原 down_count / prev_ema 状态。
                self.on_15m_close(symbol, ema)

    # ── 生命周期 ──

    def reset(self, symbol: str):
        self._state.pop(symbol, None)

    def reset_all(self):
        self._state.clear()
