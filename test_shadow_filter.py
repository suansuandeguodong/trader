# -*- coding: utf-8 -*-
"""
30m 影线过滤（shadow_filter）回归测试套件。

运行：
    python test_shadow_filter.py

覆盖：
  A. prev_30m_open_time：30m 归属边界（整点/15分/30分/45分 全部整数边界）
  B. compute_shadow：实体/上影/下影 的数值（含阴线/阳线）
  C. shadow_condition_met：做多/做空 满足、不满足、严格相等不算满足
  D. find_30m_bar：精确匹配 openTime
  E. is_shadow_signal：mock 数据拉取的兜底逻辑（拉不到 → False，不阻断交易）
  F. 手算基准：独立推导的 K线 OHLC 影线真值锚定代码
"""

import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import shadow_filter as sf
from market import get_30m_klines

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


BAR_15 = 15 * 60 * 1000
BAR_30 = 30 * 60 * 1000
BASE = 1704067200000  # 2024-01-01 00:00:00 UTC


# ─────────────────────────────────────────────────────────────
print("=" * 60)
print("A. prev_30m_open_time 归属边界")
print("=" * 60)
# 信号 15m K线 openTime 相对 BASE 的偏移 -> 期望前一根 30m K线 openTime 偏移
cases = [
    (0 * BAR_15, -1 * BAR_30),   # 0:00 -> 前一根 -0:30
    (1 * BAR_15, -1 * BAR_30),   # 0:15 -> 前一根 -0:30
    (2 * BAR_15, 0 * BAR_30),    # 0:30 -> 前一根  0:00
    (3 * BAR_15, 0 * BAR_30),    # 0:45 -> 前一根  0:00
    (4 * BAR_15, 1 * BAR_30),    # 1:00 -> 前一根  0:30
    (5 * BAR_15, 1 * BAR_30),    # 1:15 -> 前一根  0:30
    (6 * BAR_15, 2 * BAR_30),    # 1:30 -> 前一根  1:00
    (7 * BAR_15, 2 * BAR_30),    # 1:45 -> 前一根  1:00
]
for i, (sig_off, exp_off) in enumerate(cases):
    got = sf.prev_30m_open_time(BASE + sig_off) - BASE
    check(f"A{i+1} 信号={sig_off//BAR_15}刻 前一根={exp_off//BAR_30}根30m",
          got == exp_off, f"got={got} 期望={exp_off}")

# 跨日/跨小时边界：次日 0:00 信号（2024-01-02 00:00）
next_day = BASE + 24 * 60 * 60 * 1000
got = sf.prev_30m_open_time(next_day) - BASE
check("A9 跨日边界 次日0:00 -> 前一日23:30",
      got == 23 * 60 * 60 * 1000 + 30 * 60 * 1000, f"got={got}")


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("B. compute_shadow 实体/影线")
print("=" * 60)
# 阳线：o=100 c=110 h=115 l=95 -> 实体10 上影5 下影5
b_up = {'o': 100, 'c': 110, 'h': 115, 'l': 95}
body, upper, lower = sf.compute_shadow(b_up)
check("B1 阳线实体=10", body == 10, f"body={body}")
check("B2 阳线上影=5", upper == 5, f"upper={upper}")
check("B3 阳线下影=5", lower == 5, f"lower={lower}")

# 阴线：o=110 c=100 h=115 l=95 -> 实体10 上影5 下影5
b_down = {'o': 110, 'c': 100, 'h': 115, 'l': 95}
body, upper, lower = sf.compute_shadow(b_down)
check("B4 阴线实体=10", body == 10, f"body={body}")
check("B5 阴线上影=5", upper == 5, f"upper={upper}")
check("B6 阴线下影=5", lower == 5, f"lower={lower}")

# 十字线/无上下影
b_flat = {'o': 100, 'c': 100, 'h': 100, 'l': 100}
body, upper, lower = sf.compute_shadow(b_flat)
check("B7 十字线全0", body == 0 and upper == 0 and lower == 0,
      f"body={body} up={upper} lo={lower}")


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("C. shadow_condition_met 判定")
print("=" * 60)
# 新规则：下影线 >= 1.5*实体（做多）；上影线 >= 1.5*实体（做空）
# 做多：实体1 下影2 -> 2 >= 1.5 满足
check("C1 做多满足", sf.shadow_condition_met('LONG', {'o':100,'c':101,'h':104,'l':98}))
# 做多：实体2 下影3 -> 3 >= 3 相等（>=）满足
check("C2 做多边界相等满足",
      sf.shadow_condition_met('LONG', {'o':100,'c':102,'h':103,'l':97}))
# 做多：实体2 下影2.9 -> 2.9 < 3 不满足
check("C3 做多不满足", not sf.shadow_condition_met('LONG', {'o':100,'c':102,'h':103,'l':97.1}))

# 做空：实体1 上影2 -> 2 >= 1.5 满足
check("C4 做空满足", sf.shadow_condition_met('SHORT', {'o':100,'c':99,'h':103,'l':96}))
# 做空：实体2 上影3 -> 3 >= 3 相等（>=）满足
check("C5 做空边界相等满足",
      sf.shadow_condition_met('SHORT', {'o':100,'c':98,'h':103,'l':99}))
# 做空：实体2 上影2.9 -> 2.9 < 3 不满足
check("C6 做空不满足", not sf.shadow_condition_met('SHORT', {'o':100,'c':98,'h':102.9,'l':99}))


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("D. find_30m_bar 精确匹配")
print("=" * 60)
bars = [
    {'t': BASE - BAR_30, 'o': 1, 'h': 2, 'l': 0.5, 'c': 1.5},
    {'t': BASE, 'o': 2, 'h': 3, 'l': 1.5, 'c': 2.5},
]
check("D1 命中目标", sf.find_30m_bar(bars, BASE) is not None)
check("D2 未命中返回None", sf.find_30m_bar(bars, BASE + BAR_30) is None)


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("E. is_shadow_signal 兜底逻辑（mock get_30m_klines）")
print("=" * 60)
_orig = get_30m_klines


def _restore():
    sf.get_prev_30m_bar.__globals__  # noop，保持引用


# E1 拉取返回 [] -> False（不阻断交易，兜底走原流程）
def fake_empty(symbol, limit=500):
    return []
import market
market.get_30m_klines = fake_empty
check("E1 拉取为空返回False", sf.is_shadow_signal('X', 'LONG', BASE) is False)

# E2 拉取返回匹配不到的 K线 -> False
def fake_no_match(symbol, limit=500):
    return [{'t': BASE - 5 * BAR_30, 'o': 1, 'h': 2, 'l': 0.5, 'c': 1.5}]
market.get_30m_klines = fake_no_match
check("E2 匹配不到返回False", sf.is_shadow_signal('X', 'LONG', BASE) is False)

# E3 拉取异常 -> False
def fake_raise(symbol, limit=500):
    raise RuntimeError("network")
market.get_30m_klines = fake_raise
check("E3 拉取异常返回False", sf.is_shadow_signal('X', 'LONG', BASE) is False)

# E4 正常命中：做多 下影满足 -> True
def fake_hit(symbol, limit=500):
    target = sf.prev_30m_open_time(BASE)
    return [{'t': target, 'o': 100, 'h': 103, 'l': 90, 'c': 101}]
market.get_30m_klines = fake_hit
check("E4 命中且影线满足返回True", sf.is_shadow_signal('X', 'LONG', BASE) is True)

# E5 正常命中：做多 影线不满足 -> False
def fake_hit_no(symbol, limit=500):
    target = sf.prev_30m_open_time(BASE)
    return [{'t': target, 'o': 100, 'h': 104, 'l': 99, 'c': 101}]
market.get_30m_klines = fake_hit_no
check("E5 命中但影线不满足返回False", sf.is_shadow_signal('X', 'LONG', BASE) is False)

market.get_30m_klines = _orig


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("F. 手算基准（独立推导真值锚定）")
print("=" * 60)
# F1 手算影线：o=100 c=112 h=118 l=94
#   实体 = |112-100| = 12
#   上影 = 118 - max(100,112) = 118-112 = 6
#   下影 = min(100,112) - 94 = 100-94 = 6
b = {'o': 100, 'c': 112, 'h': 118, 'l': 94}
body, upper, lower = sf.compute_shadow(b)
check("F1 手算 实体=12 上影=6 下影=6",
      body == 12 and upper == 6 and lower == 6,
      f"body={body} up={upper} lo={lower}")

# F2 手算归属：信号 openTime = 2024-01-01 03:45:00 UTC
#   = BASE + 3h45m = BASE + 3*3600s*1000 + 45*60*1000
sig = BASE + 3 * 3600 * 1000 + 45 * 60 * 1000
expect_prev = BASE + 3 * 3600 * 1000  # 前一根 30m = 03:00
check("F2 手算归属 03:45 -> 前一根 03:00",
      sf.prev_30m_open_time(sig) == expect_prev,
      f"got={sf.prev_30m_open_time(sig)-BASE} 期望={expect_prev-BASE}")

# F3 手算归属：信号 = 03:15
sig2 = BASE + 3 * 3600 * 1000 + 15 * 60 * 1000
expect_prev2 = BASE + 2 * 3600 * 1000 + 30 * 60 * 1000  # 03:15 属于 [03:00,03:30) 前一根 02:30
check("F3 手算归属 03:15 -> 前一根 02:30",
      sf.prev_30m_open_time(sig2) == expect_prev2,
      f"got={sf.prev_30m_open_time(sig2)-BASE}")

# F4 手算新规则阈值：做多 下影 >= 1.5*实体
#   o=100 c=102 h=103 l=97 -> 实体2 上影1 下影3
#   阈值 1.5*2=3 -> 下影3 >= 3 满足
b4 = {'o': 100, 'c': 102, 'h': 103, 'l': 97}
check("F4 手算 做多 下影3>=1.5*实体2 满足",
      sf.shadow_condition_met('LONG', b4))
#   o=100 c=102 h=103 l=97.1 -> 实体2 下影2.9 < 3 不满足
b5 = {'o': 100, 'c': 102, 'h': 103, 'l': 97.1}
check("F5 手算 做多 下影2.9<1.5*实体2 不满足",
      not sf.shadow_condition_met('LONG', b5))
# F6 手算做空：o=100 c=98 h=103 l=99 -> 实体2 上影3 下影1
#   上影3 >= 1.5*2=3 满足
b6 = {'o': 100, 'c': 98, 'h': 103, 'l': 99}
check("F6 手算 做空 上影3>=1.5*实体2 满足",
      sf.shadow_condition_met('SHORT', b6))


print()
print("=" * 60)
print(f"结果: {PASS} 通过, {FAIL} 失败")
print("=" * 60)
sys.exit(1 if FAIL else 0)
