# -*- coding: utf-8 -*-
"""
_close_and_cleanup 结清逻辑回归测试（选项A：止损触发后彻底结清，杜绝裸仓）。

运行：
    python test_close_cleanup.py

用 FakeBroker 记录调用，验证：
  A. 精准撤销本笔开仓单，不误伤同 symbol 另一方向（不调用 cancel_all_open_orders）
  B. 撤单期间又成交（executedQty > filled_qty）→ 立即市价补平增量
  C. 强校验发现残仓 → 市价补平残余
  D. 全部干净 → 才 remove_trade 移除本地记录
"""
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import trader

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name}  {detail}")


class FakeTrade:
    def __init__(self, direction="LONG", filled_qty=10.0):
        self.symbol = "BTCUSDT"
        self.direction = direction
        self.entry_order_id = 111
        self.stop_order_id = 222
        self.tp_order_id = 333
        self.filled_qty = filled_qty
        self.qty = 100.0
        self.trade_id = "BTCUSDT:LONG:123"


class FakeBroker:
    """记录所有下单/撤单/查询调用，可按配置模拟竞态。"""
    def __init__(self, query_result=None, residual_amt=0.0):
        self.calls = []
        self.query_result = query_result  # 模拟 query_order 的返回
        self.residual_amt = residual_amt  # 模拟 get_open_positions 返回的残仓
        self.market_orders = []  # (side, qty, position_side)
        self.cancelled_orders = []  # order_id
        self.cancelled_algos = []

    def side_for_exit(self, d):
        return "SELL" if d == "LONG" else "BUY"

    def round_qty(self, sym, qty):
        return qty

    def cancel_order(self, sym, order_id):
        self.cancelled_orders.append(order_id)
        return {}

    def cancel_algo_order(self, sym, algo_id):
        self.cancelled_algos.append(algo_id)
        return {}

    def cancel_all_open_orders(self, sym):
        self.calls.append(("cancel_all_open_orders", sym))
        return {}

    def query_order(self, sym, order_id):
        self.calls.append(("query_order", order_id))
        return self.query_result

    def place_market_order(self, sym, side, qty, position_side="BOTH", reduce_only=True):
        self.market_orders.append((side, qty, position_side))
        return {"orderId": 999}

    def get_open_positions(self):
        # 模拟返回残仓（positionSide == direction）
        if abs(self.residual_amt) < 1e-12:
            return []
        return [{"symbol": "BTCUSDT", "positionSide": "LONG",
                 "positionAmt": self.residual_amt}]


class FakeRM:
    def __init__(self, broker):
        self.broker = broker
        self.trades = {}
        self.removed = []

    def side_for_exit(self, d):
        return self.broker.side_for_exit(d)

    def remove_trade(self, sym, direction):
        self.removed.append((sym, direction))


# ─────────────────────────────────────────────────────────────
print("=" * 60)
print("A. 精准撤销，不误伤另一方向")
print("=" * 60)
fb = FakeBroker(query_result={"status": "CANCELED", "executedQty": 10})
rm = FakeRM(fb)
t = FakeTrade()
trader._close_and_cleanup(rm, t, "BTCUSDT", "LONG")
check("A1 撤了开仓单", 111 in fb.cancelled_orders)
check("A2 撤了止损单(algo)", 222 in fb.cancelled_algos)
check("A3 撤了止盈单", 333 in fb.cancelled_orders)
check("A4 未调用 cancel_all_open_orders",
      not any(c[0] == "cancel_all_open_orders" for c in fb.calls),
      f"calls={fb.calls}")
check("A5 移除本地记录", ("BTCUSDT", "LONG") in rm.removed)


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("B. 撤单期间又成交 → 市价补平增量")
print("=" * 60)
# 撤单后 executedQty=25，之前 filled_qty=10 → 又成交了 15，需市价补平
fb2 = FakeBroker(query_result={"status": "CANCELED", "executedQty": 25})
rm2 = FakeRM(fb2)
t2 = FakeTrade(filled_qty=10.0)
trader._close_and_cleanup(rm2, t2, "BTCUSDT", "LONG")
check("B1 有市价补平", len(fb2.market_orders) > 0,
      f"market_orders={fb2.market_orders}")
if fb2.market_orders:
    side, qty, ps = fb2.market_orders[0]
    check("B2 补平方向正确(SELL)", side == "SELL", f"side={side}")
    check("B3 补平数量正确(25-10=15)", abs(qty - 15) < 1e-9, f"qty={qty}")
    check("B4 补平 positionSide=LONG", ps == "LONG", f"ps={ps}")


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("C. 强校验发现残仓 → 市价补平残余")
print("=" * 60)
# 撤单后无新成交（executedQty==filled_qty），但交易所仍残 8 个币仓
fb3 = FakeBroker(query_result={"status": "CANCELED", "executedQty": 10},
                 residual_amt=8.0)
rm3 = FakeRM(fb3)
t3 = FakeTrade(filled_qty=10.0)
trader._close_and_cleanup(rm3, t3, "BTCUSDT", "LONG")
check("C1 有市价补平残仓", len(fb3.market_orders) > 0,
      f"market_orders={fb3.market_orders}")
if fb3.market_orders:
    side, qty, ps = fb3.market_orders[0]
    check("C2 残仓补平方向 SELL", side == "SELL")
    check("C3 残仓补平数量 8", abs(qty - 8) < 1e-9, f"qty={qty}")


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("D. 全部干净 → 无多余市价单")
print("=" * 60)
fb4 = FakeBroker(query_result={"status": "CANCELED", "executedQty": 10},
                 residual_amt=0.0)
rm4 = FakeRM(fb4)
t4 = FakeTrade(filled_qty=10.0)
trader._close_and_cleanup(rm4, t4, "BTCUSDT", "LONG")
check("D1 无市价补平（已干净）", len(fb4.market_orders) == 0,
      f"market_orders={fb4.market_orders}")
check("D2 仍移除本地记录", ("BTCUSDT", "LONG") in rm4.removed)


print()
print("=" * 60)
print(f"结果: {PASS} 通过, {FAIL} 失败")
print("=" * 60)
sys.exit(1 if FAIL else 0)
