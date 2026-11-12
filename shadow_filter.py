# -*- coding: utf-8 -*-
"""
30m 影线过滤（Shadow Filter）

新逻辑分支的前置条件：15m 信号 K线出现后，选取它所属 30m 区间「前一根」已收盘的
30m K线，判断其影线形态是否满足条件，满足则走「新流程」，不满足走原流程。

场景（信号 K线方向）：
  - 拐头向上（做多）：30m 前一根 K线 下影线 >= 1.5 * 实体
  - 拐头向下（做空）：30m 前一根 K线 上影线 >= 1.5 * 实体

「>=」（大于等于，边界相等也算满足）。

30m K线归属规则：
  15m K线 openTime（整点/15分/30分/45分）落在某个 [t, t+30m) 区间内。
  用 openTime 对 30*60*1000(ms) 取余归到该区间起点，再往前退 30m 即为
  「前一根已收盘 30m K线」的 openTime。由于 30m 边界对 15m 网格是整数对齐，
  该 openTime 永远是精确的 30m 边界，可稳定匹配到拉取到的 K线。
"""
from typing import Optional

BAR_30M_MS = 30 * 60 * 1000


def compute_shadow(b) -> tuple:
    """根据一根 OHLC K线计算 (实体, 上影线, 下影线)，实体取绝对值。"""
    o, h, l, c = b["o"], b["h"], b["l"], b["c"]
    body = abs(c - o)
    upper = h - max(o, c)   # 上影线
    lower = min(o, c) - l   # 下影线
    return body, upper, lower


def prev_30m_open_time(signal_open_ms: int) -> int:
    """
    返回信号 15m K线所属 30m 区间「前一根」30m K线的 openTime(ms)。

    15m K线 openTime 对 30m 取余只会得到 0 或 900000，因此归到所在 30m
    区间起点再退 30m，必定得到一个精确的 30m 边界（整点/半点）。
    """
    m30 = signal_open_ms % BAR_30M_MS
    cur_start = signal_open_ms - m30
    return cur_start - BAR_30M_MS


def find_30m_bar(bars: list, target_open_ms: int) -> Optional[dict]:
    """在 30m K线列表中精确匹配 openTime == target_open_ms 的那根。"""
    for b in bars:
        if int(b["t"]) == target_open_ms:
            return b
    return None


def get_prev_30m_bar(symbol: str, signal_open_ms: int) -> Optional[dict]:
    """拉取该 symbol 的 30m K线，返回信号 K线所属区间「前一根」已收盘 30m K线。

    拉不到或匹配不到返回 None（调用方兜底走原流程）。
    """
    try:
        from market import get_30m_klines
    except Exception:
        return None
    target = prev_30m_open_time(signal_open_ms)
    try:
        bars = get_30m_klines(symbol, limit=5)
    except Exception:
        return None
    if not bars:
        return None
    return find_30m_bar(bars, target)


def shadow_condition_met(direction: str, bar: dict) -> bool:
    """判断 30m K线是否满足影线条件（严格大于）。direction: LONG / SHORT。"""
    body, upper, lower = compute_shadow(bar)
    if direction == "LONG":
        # 拐头向上（做多）：下影线 >= 1.5 * 实体
        return lower >= 1.5 * body
    else:  # SHORT
        # 拐头向下（做空）：上影线 >= 1.5 * 实体
        return upper >= 1.5 * body


def is_shadow_signal(symbol: str, direction: str, signal_open_ms: int) -> bool:
    """
    完整判定：15m 信号是否满足「30m 前一根 K线影线」条件。

    返回 True 表示满足（走新流程），False 表示不满足（走原流程）。
    任何数据拉取/匹配失败都返回 False，兜底走原流程，不阻断交易。
    """
    bar = get_prev_30m_bar(symbol, signal_open_ms)
    if bar is None:
        return False
    return shadow_condition_met(direction, bar)
