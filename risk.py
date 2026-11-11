"""
出入场规则 + 仓位管理（Risk / Trade Manager）

规则（与用户确认）：
  - 开仓：信号触发那根 K 线的收盘价挂限价单买入（做多 BUY / 做空 SELL）
  - 止损线 = 信号触发那根 K 线的开盘价
  - 止盈价 = 入场价 ± 止损幅度的一半（盈亏比 2:1，亏2赚1）
    做多：止盈 = close + (close - open) × 0.5
    做空：止盈 = close - (open - close) × 0.5
  - 每笔命名价值 15 USDT，数量 = 15 / 价格，向下取整对齐 stepSize
  - 15 分钟（从挂单成功算起）未成交则撤单
  - 最大同时持仓 30 个，持仓期间重复信号跳过
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, List

from broker import BinanceFutures


@dataclass
class Trade:
    """一笔交易/挂单的完整状态。"""
    symbol: str
    direction: str          # "LONG" / "SHORT"
    entry_price: float      # 限价（= 收盘价，对齐后）
    stop_price: float       # 止损价（= 开盘价，对齐后）
    take_profit_price: float  # 止盈价（对齐后）
    qty: float              # 下单数量（对齐后，即挂单总量）
    entry_order_id: Optional[int] = None   # 开仓限价单 ID
    entry_order_time: Optional[float] = None  # 挂单成功时间戳
    position_open: bool = False            # 是否已成交（有持仓）
    stop_order_id: Optional[int] = None
    tp_order_id: Optional[int] = None
    state: str = "PENDING"  # PENDING / FILLED / CANCELLED / CLOSED
    fixed_tp: bool = False  # 止盈价是否固定（3m回调策略），成交后不重算
    filled_qty: float = 0.0  # 已成交且已挂止损止盈保护的累计数量
    trade_id: Optional[str] = None  # 本地交易标识，用于落盘关联各事件
    signal_bar_time: Optional[int] = None  # 信号K线 openTime(ms)，观察期计时起点


class RiskManager:
    """负责出入场规则计算与仓位状态管理。"""

    def __init__(self, broker: BinanceFutures, notional_usdt: float = 108.0,
                 risk_reward: float = 2.0, max_positions: int = 30,
                 cancel_timeout_sec: int = 900,
                 cool_after_stops: int = 3, cool_seconds: float = 2.5 * 3600):
        self.broker = broker
        self.notional_usdt = notional_usdt
        self.risk_reward = risk_reward
        self.max_positions = max_positions
        self.cancel_timeout_sec = cancel_timeout_sec
        # 当前活跃的交易（挂单中 + 持仓中），key 为 (symbol, position_side)
        self.trades: Dict[tuple, Trade] = {}

        # ── 连续止损冷静期 ──
        self.cool_after_stops = cool_after_stops   # 连续 N 笔止损后触发冷静期
        self.cool_seconds = cool_seconds          # 冷静期时长（秒），默认 2.5h
        self.consecutive_stops = 0                # 当前连续止损笔数
        self.cool_until = 0.0                     # 冷静期结束时间戳（秒），0 表示不在冷静期

    # ── 规则计算 ──
    def compute_prices(self, direction: str, open_price: float,
                       close_price: float) -> tuple:
        """
        根据信号 K 线的 open/close 计算 entry/stop/take_profit 三价。

        做多：entry=close, stop=open, tp=close + (close-open)/2
        做空：entry=close, stop=open, tp=close - (open-close)/2
        返回 (entry, stop, tp)，未对齐（调用方后续对齐）。
        """
        if direction == "LONG":
            entry = close_price
            stop = open_price
            tp = close_price + (close_price - open_price) / self.risk_reward
        else:  # SHORT
            entry = close_price
            stop = open_price
            tp = close_price - (open_price - close_price) / self.risk_reward
        return entry, stop, tp

    def compute_qty(self, symbol: str, price: float) -> float:
        """数量 = 15 USDT / 价格，向下取整对齐 stepSize。"""
        qty = self.notional_usdt / price
        qty = self.broker.round_qty(symbol, qty)
        return qty

    def side_for_entry(self, direction: str) -> str:
        """开仓方向映射：做多=LONG(BUY)，做空=SHORT(SELL)。"""
        return "BUY" if direction == "LONG" else "SELL"

    def side_for_exit(self, direction: str) -> str:
        """平仓方向：做多平仓=SELL，做空平仓=BUY。"""
        return "SELL" if direction == "LONG" else "BUY"

    def position_side(self, direction: str) -> str:
        """持仓方向标识（双向持仓模式用）：LONG 或 SHORT。"""
        return "LONG" if direction == "LONG" else "SHORT"

    # ── 状态管理 ──
    def _key(self, symbol: str, direction: str) -> tuple:
        """账本 key = (symbol, position_side)。方向映射到双向持仓模式的 side。"""
        return (symbol, self.position_side(direction))

    def has_position_or_order(self, symbol: str, direction: str) -> bool:
        """该 (symbol, 方向) 是否已在挂单或持仓中。"""
        return self._key(symbol, direction) in self.trades

    def open_position_count(self) -> int:
        """当前实际持仓（含挂单中待成交）的数量。"""
        return len(self.trades)

    def can_open_new(self) -> bool:
        return self.open_position_count() < self.max_positions

    def register_trade(self, symbol: str, direction: str, entry: float,
                       stop: float, tp: float, qty: float,
                       fixed_tp: bool = False) -> Optional[Trade]:
        """
        注册一笔交易。若该 (symbol, 方向) 已存在持仓/挂单，则返回 None（不覆盖）。
        fixed_tp: True 表示止盈价是固定值（3m回调策略），成交后不重算。
        """
        key = self._key(symbol, direction)
        if key in self.trades:
            return None
        t = Trade(symbol=symbol, direction=direction, entry_price=entry,
                  stop_price=stop, take_profit_price=tp, qty=qty, state="PENDING",
                  fixed_tp=fixed_tp)
        self.trades[key] = t
        return t

    def remove_trade(self, symbol: str, direction: str):
        self.trades.pop(self._key(symbol, direction), None)

    # ── 连续止损冷静期 ──
    def in_cooling(self) -> bool:
        """是否处于冷静期（当前时间 < 冷静期结束时间）。"""
        import time as _time
        return self.cool_until > 0 and _time.time() < self.cool_until

    def cool_remaining(self) -> float:
        """冷静期还剩多少秒（不在冷静期则为 0）。"""
        import time as _time
        if not self.in_cooling():
            return 0.0
        return max(0.0, self.cool_until - _time.time())

    def _reset_stop_counter(self):
        self.consecutive_stops = 0

    def on_stop_loss(self, open_time: float):
        """
        记录一笔止损平仓。open_time 保留仅为接口兼容（原为记录开仓时间）。
        当连续止损达到阈值时，进入冷静期，冷静期起点 = 第 N 笔止损平仓发生的当前时刻。
        """
        self.consecutive_stops += 1

        if self.consecutive_stops >= self.cool_after_stops:
            # 冷静期从「第 N 笔止损平仓发生的当前时刻」起算 cool_seconds 秒。
            import time as _time
            self.cool_until = _time.time() + self.cool_seconds
            # 重置计数，避免冷静期内反复触发
            self._reset_stop_counter()
            return True  # 表示刚触发了冷静期
        return False

    def on_profit(self):
        """记录一笔盈利（止盈）平仓，重置连续止损计数。"""
        self._reset_stop_counter()
