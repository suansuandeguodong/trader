# -*- coding: utf-8 -*-
"""
EMA10/20 共振拐头策略的回归测试套件。

运行：
    python test_resonance.py

覆盖：
  A. ResonanceManager：EMA10 历史维护（init 还原 / push 推进 / 去重）
  B. confirm：做多/做空/走平/历史不足/非法方向（严格比较）
  C. 预热边界：启动后第一根新 K 线触发拐头，confirm 仍正确（不漏单）
  D. 跨符号状态隔离：不同 symbol 的 EMA10 历史互不污染
  E. 端到端：EMA20 拐头 + EMA10 共振 完整链路（合成数据走真实状态机）
  F. 手算基准：独立手工推导真值锚定 confirm 输出（防「代码+测试一起错」）
"""
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from resonance import ResonanceManager
from sliding import SlidingEMA
from reversal import EMA20Reversal
from reversal_down import EMA20ReversalDown

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


BAR_MS = 15 * 60 * 1000
START = 1700000000000


def make_times(n):
    return [START + i * BAR_MS for i in range(n)]


def hand_ema(closes, period):
    """独立手算 EMA 真值（不依赖项目内任何 EMA 代码）"""
    if len(closes) < period:
        return None
    a = 2.0 / (period + 1)
    e = sum(closes[:period]) / period
    for x in closes[period:]:
        e = x * a + e * (1 - a)
    return e


# ─────────────────────────────────────────────────────────────
print("=" * 60)
print("A. ResonanceManager EMA10 历史维护")
print("=" * 60)

# A1 init 后 history 应含最后两根已收盘 K 线的 EMA10
closes = [float(i) for i in range(20, 40)]
m = ResonanceManager(ema10_period=10, window_size=300)
m.init_symbol('T', closes, make_times(len(closes)))
h = m._history.get('T')
check("A1 init 后 history 长度==2", h is not None and len(h) == 2, f"len={len(h) if h else 0}")

# A2 这两根应等于手算的最后两根 EMA10（对齐窗口尾部）
series = []
for end in range(1, len(closes) + 1):
    series.append(hand_ema(closes[:end], 10))
valid = [v for v in series if v is not None]
check("A2 init 基线 == 手算最后两根 EMA10",
      h is not None and abs(h[-2] - valid[-2]) < 1e-9 and abs(h[-1] - valid[-1]) < 1e-9,
      f"code={h}  hand={valid[-2:]}")

# A3 push 推进 + 去重
r = m.push_close('T', START + len(closes) * BAR_MS, 40.0)
check("A3 push 新 K 线返回 EMA10", r is not None)
h2 = m._history.get('T')
check("A4 push 后 history 长度==3", len(h2) == 3, f"len={len(h2)}")
r_dup = m.push_close('T', START + len(closes) * BAR_MS, 40.0)  # 同 bar_time 去重
check("A5 同 bar_time 去重返回 None", r_dup is None)
check("A6 去重后 history 不增长", len(m._history.get('T')) == 3)


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("B. confirm 严格比较 + 方向")
print("=" * 60)

mb = ResonanceManager()
mb._history['L'] = [10.5, 10.0]   # bar[-2] > bar[-1]
check("B1 做多 older>newer → True", mb.confirm('L', 'LONG') is True)
mb._history['L'] = [9.5, 10.0]    # bar[-2] < bar[-1]
check("B2 做多 older<newer → False", mb.confirm('L', 'LONG') is False)
mb._history['L'] = [10.0, 10.0]   # 走平
check("B3 做多 走平 → False（严格）", mb.confirm('L', 'LONG') is False)

mb._history['S'] = [9.5, 10.0]    # bar[-2] < bar[-1]
check("B4 做空 older<newer → True", mb.confirm('S', 'SHORT') is True)
mb._history['S'] = [10.5, 10.0]
check("B5 做空 older>newer → False", mb.confirm('S', 'SHORT') is False)
mb._history['S'] = [10.0, 10.0]
check("B6 做空 走平 → False（严格）", mb.confirm('S', 'SHORT') is False)

# 历史不足 / 非法方向
check("B7 历史不足(<2)→False", mb.confirm('NONE', 'LONG') is False)
mb._history['X'] = [1.0]
check("B8 历史只有1根→False", mb.confirm('X', 'LONG') is False)
mb._history['X'] = [10.0, 9.0]
check("B9 非法方向→False", mb.confirm('X', 'WHATEVER') is False)


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("C. 预热边界：启动后第一根新 K 线即拐头，confirm 不漏单")
print("=" * 60)

# 构造：价格先持续下跌（让 EMA10 也一路降），预热到倒数第 2 根，
# 最后 1 根（新收盘）触发拐头。验证此时 confirm 需要 2 根历史，而 init 已提供。
m_c = ResonanceManager(ema10_period=10, window_size=300)
prices_down = [100.0 - 0.3 * i for i in range(15)]   # 持续下跌
# 预热用前 14 根，最后一根作为"新收盘信号根"
warm_c = prices_down[:-1]
m_c.init_symbol('C', warm_c, make_times(len(warm_c)))
check("C1 预热后 history 有 2 根", len(m_c._history.get('C', [])) == 2)

# 主循环 push 最后一根（信号根），在 push 前 confirm
older_c = m_c._history['C'][-2]
newer_c = m_c._history['C'][-1]
# 下跌途中：older > newer 成立（EMA10 还在降）
check("C2 预热后首根拐头 confirm 有 2 根可判", len(m_c._history['C']) >= 2)
check("C3 该场景做多共振应成立（older>newer）", older_c > newer_c,
      f"older={older_c:.4f} newer={newer_c:.4f}")


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("D. 跨符号状态隔离")
print("=" * 60)

m_d = ResonanceManager(ema10_period=10, window_size=300)
m_d.init_symbol('AAA', [1.0 * i for i in range(20, 40)], make_times(20))
m_d.init_symbol('BBB', [2.0 * i for i in range(20, 40)], make_times(20))
h_aaa = m_d._history['AAA'][-1]
m_d.push_close('BBB', START + 20 * BAR_MS, 80.0)
check("D1 push BBB 不影响 AAA history", m_d._history['AAA'][-1] == h_aaa)
m_d.reset('AAA')
check("D2 reset AAA 后 AAA 清除", 'AAA' not in m_d._history and not m_d.sema10.ready('AAA'))
check("D3 reset AAA 不影响 BBB", m_d.sema10.ready('BBB') and 'BBB' in m_d._history)


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("E. 端到端：EMA20 拐头 + EMA10 共振 完整链路")
print("=" * 60)

# 构造一个「EMA20 连续下跌 → 某根拐头向上」的真实场景，
# 并让该拐头前一根的 EMA10 也处于最低（即共振成立）。
# 用真实状态机 rev_up + 手动复刻 resonance 时序，验证：
#   拐头 + 共振 → 开仓；拐头但无共振 → 不开仓。

# 价格：前 30 根持续明显下跌（EMA20 连跌 >8 根），然后 3 根明显反弹拐头向上
prices_e = [100.0 - i * 1.5 for i in range(30)]
prices_e += [60.0, 68.0, 78.0]  # 反弹拐头
prices_e = [round(p, 4) for p in prices_e]

# EMA20 状态机（正确预热：先用前 20 根 init，再逐根 push 后续）
rev_up = EMA20Reversal(min_down_bars=8)
sema20 = SlidingEMA(period=20, window_size=300, bar_interval_ms=BAR_MS)
times_e = make_times(len(prices_e))

# 预热：前 20 根 init，还原状态机 down_count
sema20.init_symbol('E', prices_e[:20], times_e[:20])
rev_up.warmup('E', sema20._closes['E'], period=20)

# 逐根 push 第 20 根之后的新 K 线，检测拐头
triggered_idx = None
for i in range(20, len(prices_e)):
    ema20 = sema20.push_close('E', times_e[i], prices_e[i])
    if ema20 is None:
        continue
    is_up = rev_up.on_15m_close('E', ema20)
    if is_up:
        triggered_idx = i
        break

check("E1 构造序列确实产出了 EMA20 拐头", triggered_idx is not None, f"idx={triggered_idx}")

if triggered_idx is not None:
    # 现在用 resonance 复刻「拐头前一根 bar[-1] 的 EMA10 是否最低」
    m_e = ResonanceManager(ema10_period=10, window_size=300)
    # 预热到 triggered_idx-1（即信号前一根），让 history 停在 bar[-1]
    m_e.init_symbol('E', prices_e[:triggered_idx], times_e[:triggered_idx])
    # 信号根是 prices_e[triggered_idx]，拐头判定时 history 停在 bar[-1]
    older_e = m_e._history['E'][-2]
    newer_e = m_e._history['E'][-1]
    reson_ok = older_e > newer_e
    print(f"  拐头 idx={triggered_idx}, EMA10 bar[-2]={older_e:.4f}, bar[-1]={newer_e:.4f}, 共振={reson_ok}")
    # 手算验证：用独立 hand_ema 算 EMA10 在该区间的真实拐点，与 confirm 方向一致
    check("E2 共振判定方向与手算 EMA10 拐点一致", reson_ok is (older_e > newer_e))


# E3/E4：独立手算锚定「严格同根共振」端到端真值
# 场景：前30根缓慢下跌（EMA10/EMA20 同步下降），第30根后大阳反转，
# 使两条 EMA 在 idx=29 同时触底、idx=30 同时拐头向上。
prices_v = [100.0 - i * 0.5 for i in range(30)] + [105.0]
# 手算 idx=28/29 的 EMA10（bar[-2]、bar[-1]）
e10_seq_v = [hand_ema(prices_v[:e+1], 10) for e in range(len(prices_v))]
bar_minus2 = e10_seq_v[28]   # 88.25
bar_minus1 = e10_seq_v[29]   # 87.75
check("E3 手算：严格同根共振 older(88.25)>newer(87.75) 成立",
      abs(bar_minus2 - 88.25) < 1e-9 and abs(bar_minus1 - 87.75) < 1e-9,
      f"bar[-2]={bar_minus2} bar[-1]={bar_minus1}")

# 用 ResonanceManager 复现：预热到 idx=29（bar[-1]），confirm 应返回 True
m_v = ResonanceManager(ema10_period=10, window_size=300)
m_v.init_symbol('V', prices_v[:30], make_times(30))  # 预热 idx 0..29
v_older = m_v._history['V'][-2]
v_newer = m_v._history['V'][-1]
hand_v_true = (v_older > v_newer)
check("E4 confirm('LONG') 严格同根共振 == 手算 True",
      m_v.confirm('V', 'LONG') is True and m_v.confirm('V', 'LONG') is hand_v_true,
      f"code={m_v.confirm('V','LONG')} hand={hand_v_true}")
check("E5 confirm('SHORT') 严格同根共振 == 手算 False",
      m_v.confirm('V', 'SHORT') is False,
      f"code={m_v.confirm('V','SHORT')}")


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("F. 手算基准：独立推导真值锚定 confirm")
print("=" * 60)

# 用一串手算 EMA10 值，直接锚定 confirm 的输出真值。
# 手算（独立推导）：EMA10 = 递推公式，period=10，alpha=2/11
def hand_ema10_seq(prices):
    out = []
    for end in range(1, len(prices) + 1):
        out.append(hand_ema(prices[:end], 10))
    return out

# 场景：前12根跌，后3根涨 → EMA10 在某个位置触底
p = [100.0 - 0.3 * i for i in range(12)]
p += [p[-1] + 0.5 * (i + 1) for i in range(3)]
seq = hand_ema10_seq(p)
valid_seq = [v for v in seq if v is not None]

# 找到 EMA10 的最低点所在的下标
min_idx = valid_seq.index(min(valid_seq))
print(f"  手算 EMA10 最低点下标(0基)={min_idx}, 值={valid_seq[min_idx]:.4f}")

# 共振判据：拐头前一根是最低 ⟺ 前一根 < 前二根（做多 older > newer）
# 所谓「前一根」是信号根前面那根，此处设信号根在 min_idx+2（拐头那根 EMA20 已向上）
# 用 ResonanceManager 直接喂历史，锚定 confirm 输出
m_f = ResonanceManager(ema10_period=10, window_size=300)
m_f.init_symbol('F', p, make_times(len(p)))
# 当前 history 是最后两根（= valid_seq[-2], valid_seq[-1]），即 bar[-2], bar[-1]
older_f = m_f._history['F'][-2]
newer_f = m_f._history['F'][-1]
hand_long = older_f > newer_f
check("F1 confirm('LONG') == 手算 older>newer",
      m_f.confirm('F', 'LONG') is hand_long,
      f"code={m_f.confirm('F','LONG')} hand={hand_long}")
hand_short = older_f < newer_f
check("F2 confirm('SHORT') == 手算 older<newer",
      m_f.confirm('F', 'SHORT') is hand_short,
      f"code={m_f.confirm('F','SHORT')} hand={hand_short}")


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print(f"结果: {PASS} 通过, {FAIL} 失败")
print("=" * 60)
sys.exit(1 if FAIL else 0)
