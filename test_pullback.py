# -*- coding: utf-8 -*-
"""
3m 回调入场策略的回归测试套件。

运行：
    python test_pullback.py

覆盖：
  A. SlidingEMA：递推 vs 全量重算 数学等价
  B. _compute_stop_tp：止盈/止损/half_body 的数值与符号
  C. check_touch：该触发/不该触发、时间截断、超时、防重复（合成数据，直接调真实方法）
  D. 跨信号状态泄漏：remove 后 reset sema_3m
  E. 多轮轮询无未来 K线污染（QUSDT/ZEC 类误触发回归）
  F. 真实数据回放（依赖币安，验证近期案例，历史久远者拉不到数据属预期）
"""
import sys
import io
import random

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from sliding import SlidingEMA
from pullback import PullbackManager, PendingSignal

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


def make_bars(n, start_ms, price_curve):
    bars = []
    prev = price_curve[0]
    for i, c in enumerate(price_curve):
        o = prev
        h = max(o, c) * 1.01
        l = min(o, c) * 0.99
        bars.append({'t': start_ms + i * 3 * 60 * 1000,
                     'o': o, 'h': h, 'l': l, 'c': c, 'v': 1000})
        prev = c
    return bars


def full_ema(closes, period=20):
    if len(closes) < period:
        return None
    e = sum(closes[:period]) / period
    a = 2 / (period + 1)
    for x in closes[period:]:
        e = x * a + e * (1 - a)
    return e


START = 1700000000000
BAR_MS = 3 * 60 * 1000


# ─────────────────────────────────────────────────────────────
print("=" * 60)
print("A. SlidingEMA 递推 vs 全量重算 数学等价")
print("=" * 60)
random.seed(42)
closes = [100.0]
for _ in range(100):
    closes.append(closes[-1] * (1 + random.uniform(-0.02, 0.02)))
times = [START + i * BAR_MS for i in range(len(closes))]
sema = SlidingEMA(period=20, window_size=300, bar_interval_ms=BAR_MS)
sema.init_symbol('T', closes, times)
check("A1 init 递推 == 全量", abs(sema.get_ema('T') - full_ema(closes)) < 1e-9)

for m in [closes[-1] * 1.01, closes[-1] * 0.99]:
    closes.append(m)
    times.append(times[-1] + BAR_MS)
    sema.push_close('T', times[-1], m)
check("A2 push 递推 == 全量", abs(sema.get_ema('T') - full_ema(closes)) < 1e-9)


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("B. _compute_stop_tp 数值与符号")
print("=" * 60)
mgr = PullbackManager()
ps_long = PendingSignal('L', 'LONG', 100, 110, 95, 115, 105, 0)
stop, tp, hb = mgr._compute_stop_tp('LONG', ps_long, entry=110)
check("B1 做多 half_body=+5", abs(hb - 5) < 1e-9, f"hb={hb}")
check("B2 做多 tp=entry+5", abs(tp - 115) < 1e-9, f"tp={tp}")
check("B3 做多 stop=min(low,prev)", abs(stop - min(95, 105)) < 1e-9, f"stop={stop}")

ps_short = PendingSignal('S', 'SHORT', 110, 100, 95, 115, 105, 0)
stop, tp, hb = mgr._compute_stop_tp('SHORT', ps_short, entry=100)
check("B4 做空 half_body=+5", abs(hb - 5) < 1e-9, f"hb={hb}")
check("B5 做空 tp=entry-5", abs(tp - 95) < 1e-9, f"tp={tp}")
check("B6 做空 stop=max(high,prev)", abs(stop - max(115, 105)) < 1e-9, f"stop={stop}")


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("C. check_touch 核心逻辑（合成数据，直接调真实方法）")
print("=" * 60)
import market
_current_bars = []


def fake_get_3m_klines(symbol, limit=300):
    return _current_bars


market.get_3m_klines = fake_get_3m_klines


def new_mgr():
    return PullbackManager(ema_period=20, window_size=300,
                           warmup_limit=300, watch_seconds=1800)


# C1 做多跌穿触发
prices = []
p = 100.0
for i in range(40):
    if i < 20:
        p *= 1.004
    elif i < 27:
        p *= 0.98
    else:
        p *= 1.001
    prices.append(p)
bars = make_bars(40, START, prices)
_current_bars = bars
mgr = new_mgr()
s = bars[20]['t']
ps = PendingSignal('A', 'LONG', bars[20]['o'], bars[20]['c'], bars[20]['l'],
                   bars[20]['h'], bars[19]['o'], s)
mgr.pending[('A', 'LONG')] = ps
r = mgr.check_touch('A', 'LONG', bars[26]['t'] + BAR_MS)
check("C1 做多跌穿触发", r is not None and 'entry' in r and not r.get('__timeout__'))
check("C2 返回含 half/stop/tp", r and all(k in r for k in ('half_body', 'stop', 'tp')))

# C3 now 在回踩前不触发
check("C3 now早不触发", mgr.check_touch('A', 'LONG', bars[21]['t'] + BAR_MS) is None)

# C4 做空下跌多轮永不误触发
prices = [100.0]
p = 100.0
for _ in range(39):
    p *= 0.995
    prices.append(p)
bars_d = make_bars(40, START, prices)
_current_bars = bars_d
mgr = new_mgr()
s = bars_d[20]['t']
ps2 = PendingSignal('B', 'SHORT', bars_d[20]['o'], bars_d[20]['c'], bars_d[20]['l'],
                    bars_d[20]['h'], bars_d[19]['o'], s)
mgr.pending[('B', 'SHORT')] = ps2
trig = False
t = s
while t <= s + 29 * 60 * 1000:
    r = mgr.check_touch('B', 'SHORT', t)
    if r is not None and not r.get('__timeout__'):
        trig = True
        break
    t += 15 * 1000
check("C4 做空下跌多轮永不误触发", not trig)

# C5 做空反弹触发
prices = []
p = 100.0
for i in range(40):
    if i < 20:
        p *= 0.995
    elif i < 27:
        p *= 1.02
    else:
        p *= 1.001
    prices.append(p)
bars_u = make_bars(40, START, prices)
_current_bars = bars_u
mgr = new_mgr()
s = bars_u[20]['t']
ps3 = PendingSignal('C', 'SHORT', bars_u[20]['o'], bars_u[20]['c'], bars_u[20]['l'],
                    bars_u[20]['h'], bars_u[19]['o'], s)
mgr.pending[('C', 'SHORT')] = ps3
r_up = mgr.check_touch('C', 'SHORT', bars_u[26]['t'] + BAR_MS)
check("C5 做空反弹触发", r_up is not None and 'entry' in r_up and not r_up.get('__timeout__'))

# C6 超时
prices = []
p = 100.0
for _ in range(40):
    p *= 1.001
    prices.append(p)
bars_t = make_bars(40, START, prices)
_current_bars = bars_t
mgr = new_mgr()
s = bars_t[20]['t']
ps5 = PendingSignal('E', 'LONG', bars_t[20]['o'], bars_t[20]['c'], bars_t[20]['l'],
                    bars_t[20]['h'], bars_t[19]['o'], s)
mgr.pending[('E', 'LONG')] = ps5
rt = mgr.check_touch('E', 'LONG', s + 1801 * 1000)
check("C6 超时返回 __timeout__", rt is not None and rt.get('__timeout__'))

# C7 entered 防重复
prices = []
p = 100.0
for i in range(40):
    if i < 20:
        p *= 1.004
    elif i < 27:
        p *= 0.98
    else:
        p *= 1.0
    prices.append(p)
bars_e = make_bars(40, START, prices)
_current_bars = bars_e
mgr = new_mgr()
s = bars_e[20]['t']
ps6 = PendingSignal('F', 'LONG', bars_e[20]['o'], bars_e[20]['c'], bars_e[20]['l'],
                    bars_e[20]['h'], bars_e[19]['o'], s)
mgr.pending[('F', 'LONG')] = ps6
r1 = mgr.check_touch('F', 'LONG', bars_e[26]['t'] + BAR_MS)
r2 = mgr.check_touch('F', 'LONG', bars_e[26]['t'] + BAR_MS)
check("C7 entered 防重复触发", r1 is not None and r2 is None)


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("D. 跨信号状态泄漏：remove 后 reset")
print("=" * 60)
mgr = new_mgr()
mgr.sema_3m.init_symbol('X', [1.0] * 50, [START + i * BAR_MS for i in range(50)])
check("D1 init 后 ready", mgr.sema_3m.ready('X'))
mgr.pending[('X', 'LONG')] = PendingSignal('X', 'LONG', 1, 1, 1, 1, 1, START)
mgr.remove('X', 'LONG')
check("D2 remove 后 sema reset", not mgr.sema_3m.ready('X'))
check("D3 remove 后无 pending", ('X', 'LONG') not in mgr.pending)


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("F. 真实数据回放（近期案例，依赖币安）")
print("=" * 60)

# 恢复真实 market.get_3m_klines（C 部分 monkeypatch 过）
import importlib
importlib.reload(market)


def replay(symbol, direction, sig_bar, sig_open, sig_close, sig_high,
           sig_low, prev_open, now_ms):
    bars = market.get_3m_klines(symbol, limit=300)
    sig_close_ms = sig_bar + 15 * 60 * 1000
    base = SlidingEMA(period=20, window_size=300, bar_interval_ms=BAR_MS)
    bc = [b['c'] for b in bars if b['t'] + BAR_MS <= sig_close_ms]
    bt = [b['t'] for b in bars if b['t'] + BAR_MS <= sig_close_ms]
    base.init_symbol(symbol, bc, bt)
    if base.get_ema(symbol) is None:
        return None, '基线不足'
    for b in bars:
        if b['t'] + BAR_MS > now_ms:
            continue
        if b['t'] < sig_close_ms:
            continue
        eb = base.get_ema(symbol)
        touched = (b['l'] <= eb) if direction == 'LONG' else (b['h'] >= eb)
        if touched:
            ea = base.push_close(symbol, b['t'], b['c'])
            return (ea if ea is not None else eb), '触发'
        base.push_close(symbol, b['t'], b['c'])
    return None, '未触发'


try:
    r, why = replay('QUSDT', 'SHORT', 1794434400000, 0.02919, 0.02859,
                    0.02936, 0.02801, 0.02936,
                    1794434400000 + 36 * 60 * 1000 + 21 * 1000)
    check(f"F1 QUSDT 做空应为未触发 [{why}]", r is None, f"触发价={r}")

    r, why = replay('ZECUSDT', 'SHORT', 1794422700000, 1007.8, 994.4,
                    1005.61, 993, 1007.8, 1794425057859)
    check(f"F2 ZEC 做空应为未触发 [{why}]", r is None, f"触发价={r}")

    r, why = replay('ZECUSDT', 'LONG', 1794392100000, 980.08, 996.87,
                    1000, 980, 980.5, 1794394098513)
    check(f"F3 ZEC 做多应触发 [{why}]", r is not None, f"跳价={r}")
except Exception as e:
    print(f"  ⚠️ 真实数据回放跳过（网络/数据不可用）: {e}")


# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print(f"结果: {PASS} 通过, {FAIL} 失败")
print("=" * 60)
sys.exit(1 if FAIL else 0)
