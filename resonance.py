"""
EMA10 / EMA20 共振拐头确认模块（resonance）

策略：15m K线 EMA20 拐头出信号后，不立即开仓，额外确认「共振」——
     信号前一根 K 线的 EMA10 必须也是这波的最低点（做多）/ 最高点（做空），
     即 EMA10 与 EMA20 在同一位置附近同时拐头，赢面更大。

判定（严格比较，走平不算）：

  做多（EMA20 拐头向上）：
      信号前一根（bar[-1]）EMA10 是最低 ⟺ bar[-2].ema10 > bar[-1].ema10

  做空（EMA20 拐头向下）：
      信号前一根（bar[-1]）EMA10 是最高 ⟺ bar[-2].ema10 < bar[-1].ema10

说明：信号根（bar[0]）EMA20 已拐头向上，则其 EMA10 亦必然向上（EMA10 比
EMA20 更灵敏、先于 EMA20 转头），故只需确认「到达信号根之前，bar[-1] 的
EMA10 比更早的 bar[-2] 更低/更高」即可判定 bar[-1] 是这波 EMA10 的极值点。

本模块只负责维护 15m EMA10 的滚动历史 + 共振判定，不下单。
下单动作由 trader.py 完成，保持职责单一。
"""
from typing import List, Optional

from sliding import SlidingEMA
from reversal import calc_ema_series


class ResonanceManager:
    """维护每个 symbol 的 15m EMA10 滚动历史，提供共振拐头判定。"""

    def __init__(self, ema10_period: int = 10, window_size: int = 300):
        self.ema10_period = ema10_period
        self.window_size = window_size
        # 15m EMA10 滑动窗口（与 SemEMA20 同级别，period=10）
        self.sema10 = SlidingEMA(period=ema10_period, window_size=window_size,
                                 bar_interval_ms=15 * 60 * 1000)
        # 每个 symbol 的 EMA10 滚动历史（旧→新，最多保留 3 根，含当前）
        # 结构：{symbol: [ema10, ...]}
        self._history: dict = {}

    # ── 预热 ──

    def init_symbol(self, symbol: str, closes: List[float],
                    bar_times: List[int]) -> None:
        """启动/新币时载入历史收盘价，初始化 EMA10 窗口 + 历史。

        关键：sliding.init_symbol 会丢弃进行中的未收盘 K 线，这里必须以
        「丢弃后实际落入窗口」的收盘价序列为准，逐根还原 EMA10 序列，并取
        **最后两根**已收盘 K 线的 EMA10 作为历史基线。这样预热边界处，即
        便启动后第一根新 K 线就触发拐头信号，confirm() 也仍有 bar[-2] 与
        bar[-1] 两根可用，不会因历史不足(<2)而误判「共振不成立」漏单。

        与 reversal.warmup 逐根还原状态机的思路一致，保证预热与实时推进
        之间的无缝衔接。
        """
        self.sema10.init_symbol(symbol, closes, bar_times)
        # init_symbol 后，sema10._closes[symbol] 是「丢弃进行中后」的已收盘收盘价序列
        actual_closes = self.sema10._closes.get(symbol)
        if not actual_closes:
            return
        # 逐根还原 EMA10 序列（reversal.calc_ema_series 与 sema10 的递推完全一致）
        ema_series = calc_ema_series(actual_closes, self.ema10_period)
        valid = [e for e in ema_series if e is not None]
        if not valid:
            return
        # 取最后两根（bar[-2]、bar[-1]），作为 confirm 的历史基线；不足则全取
        self._history[symbol] = valid[-2:]

    # ── 实时推进 ──

    def push_close(self, symbol: str, bar_time: int, close: float) -> Optional[float]:
        """喂入一根已收盘 15m K 线收盘价，更新 EMA10 并记录历史。

        返回更新后的 EMA10；无更新返回 None。历史保留最近 3 根。
        """
        ema = self.sema10.push_close(symbol, bar_time, close)
        if ema is None:
            return None
        hist = self._history.get(symbol, [])
        hist.append(ema)
        if len(hist) > 3:
            hist = hist[-3:]
        self._history[symbol] = hist
        return ema

    # ── 共振判定 ──

    def confirm(self, symbol: str, direction: str) -> bool:
        """判断「信号前一根 bar[-1] 的 EMA10 是否为极值点」，即 EMA10/20 共振。

        调用时机：在 trader.py 里信号根（bar[0]）拐头已触发之后、开仓之前。
        此时 self._history 应已包含 bar[-1] 的 EMA10 历史。

        direction: "LONG" / "SHORT"
        返回 True 表示共振成立；历史不足（<2 根）或走平则返回 False。
        """
        hist = self._history.get(symbol)
        if not hist or len(hist) < 2:
            # 至少需要前第二根与前第一根两根的 EMA10
            return False
        # 历史末尾两根本应是 bar[-2], bar[-1]（信号触发前已推进的最新两根）
        # 但安全起见取 hist 的最后两个元素（即最近的、信号触发前已收盘的 EMA10）
        older = hist[-2]  # bar[-2]（更早）
        newer = hist[-1]  # bar[-1]（信号前一根）
        if direction == "LONG":
            return older > newer  # 严格：EMA10 在 bar[-1] 处仍下降/触底
        if direction == "SHORT":
            return older < newer  # 严格：EMA10 在 bar[-1] 处仍上升/触顶
        return False

    # ── 生命周期 ──

    def reset(self, symbol: Optional[str] = None) -> None:
        """清理某 symbol 的 EMA10 窗口与历史；不传则清空全部。"""
        if symbol is None:
            self.sema10 = SlidingEMA(period=self.ema10_period,
                                     window_size=self.window_size,
                                     bar_interval_ms=15 * 60 * 1000)
            self._history.clear()
        else:
            self.sema10.reset(symbol)
            self._history.pop(symbol, None)
