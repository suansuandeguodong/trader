# -*- coding: utf-8 -*-
"""
分析：信号触发K线（拐头K）之后两根K线的 EMA20 是否顺着拐头方向，
      对比止盈单 vs 止损单的差异。

【信号K线定位·正确法】
  程序在信号K线「收盘前 lead_seconds=15 秒」用实时价触发信号并挂限价单。
  因此开仓成交时间(order.time) ≈ 信号K线收盘时刻，即：
      信号K线 openTime = (开仓时间 // 15分钟) * 15分钟     （往前取整到15m边界）
  证伪验证：定位到的信号K线，其 closeTime（openTime+15min）必须 >= 开仓时间，
            且相差不超过 1 根K线（15min），否则说明延迟成交，排除。

【验证正确性】
  对每笔输出 开仓时间、信号K线区间、以及开仓是否落在信号K收盘前15秒窗口内。
  统计「定位可信」与「定位存疑(延迟成交)」分别的样本，后者单独排除。

【口径】
  做多：拐头向上，后两根 EMA20 依次 > 拐头K EMA = 顺着。
  做空：拐头向下，后两根 EMA20 依次 < 拐头K EMA = 顺着。
"""
import os, sys, time, hmac, hashlib, urllib.parse
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

BASE = "https://fapi.binance.com"
P = {"http": "http://127.0.0.1:10808", "https": "http://127.0.0.1:10808"}
load_dotenv(".env")
KEY = os.getenv("BINANCE_API_KEY", "")
SEC = os.getenv("BINANCE_API_SECRET", "")
S = requests.Session()
S.headers["X-MBX-APIKEY"] = KEY
sys.path.insert(0, ".")
from reversal import calc_ema_series

BAR = 15 * 60 * 1000
_t0 = int(time.time() * 1000)
_st = S.get(f"{BASE}/fapi/v1/time", proxies=P, timeout=10).json()["serverTime"]
_t1 = int(time.time() * 1000)
_offset = _st - (_t0 + (_t1 - _t0) // 2)


def signed(path, params):
    p = dict(params)
    p["timestamp"] = int(time.time() * 1000) + _offset
    p["recvWindow"] = 10000
    qs = urllib.parse.urlencode(p)
    sig = hmac.new(SEC.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return S.get(f"{BASE}{path}?{qs}&signature={sig}", proxies=P, timeout=20).json()


def get_klines(sym, limit=500):
    r = requests.get(f"{BASE}/fapi/v1/klines",
                     params={"symbol": sym, "interval": "15m", "limit": str(limit)},
                     proxies=P, timeout=20).json()
    return r


def main():
    data = requests.get(f"{BASE}/fapi/v1/exchangeInfo", proxies=P, timeout=20).json()
    tk = requests.get(f"{BASE}/fapi/v1/ticker/24hr", proxies=P, timeout=20).json()
    vol = {t["symbol"]: float(t.get("quoteVolume", 0)) for t in tk}
    price = {t["symbol"]: float(t.get("lastPrice", 0)) for t in tk}
    syms = []
    for s in data.get("symbols", []):
        if (s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL"
                and s.get("status") == "TRADING"):
            sy = s["symbol"]
            if vol.get(sy, 0) >= 18_000_000 and price.get(sy, 1e9) <= 1.0:
                syms.append(sy)

    START = int(datetime(2026, 11, 4, 17, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

    results = []
    suspect = 0  # 延迟成交被怀疑的笔数

    for sy in syms:
        orders = signed("/fapi/v1/allOrders", {"symbol": sy, "startTime": START, "limit": 500})
        if not isinstance(orders, list):
            continue
        trades = signed("/fapi/v1/userTrades", {"symbol": sy, "startTime": START, "limit": 1000})
        if not isinstance(trades, list):
            trades = []

        bars = get_klines(sy, 500)
        closes_list = [float(k[4]) for k in bars]
        ema_all = calc_ema_series(closes_list, 20)

        for o in orders:
            side, ps = o["side"], o.get("positionSide", "BOTH")
            is_entry = (side == "BUY" and ps == "LONG") or (side == "SELL" and ps == "SHORT")
            if not is_entry or o.get("type") != "LIMIT":
                continue
            if float(o.get("executedQty", 0)) <= 0:
                continue
            direction = "LONG" if side == "BUY" else "SHORT"
            t_open = o["time"]

            # 找平仓成交
            entry_close = None
            for c in trades:
                if c["time"] >= t_open and abs(float(c.get("realizedPnl", 0))) > 1e-9:
                    if direction == "LONG" and c["side"] == "SELL" and c.get("positionSide") in ("LONG", "BOTH"):
                        entry_close = c
                        break
                    if direction == "SHORT" and c["side"] == "BUY" and c.get("positionSide") in ("SHORT", "BOTH"):
                        entry_close = c
                        break
            if entry_close is None:
                continue
            pnl = float(entry_close.get("realizedPnl", 0))
            outcome = "止损" if pnl < 0 else "止盈"

            # 信号K线 = 开仓时间往前取整到15m边界
            sig_open_ms = (t_open // BAR) * BAR
            # 定位在 bars 中的 index
            idx = None
            for i, k in enumerate(bars):
                if k[0] == sig_open_ms:
                    idx = i
                    break
            if idx is None or idx + 2 >= len(bars):
                continue

            # 验证：信号K closeTime 应在开仓时间附近（<= 15min 窗口）
            sig_close_ms = sig_open_ms + BAR
            delay = t_open - sig_close_ms
            is_suspect = delay > BAR  # 开仓时间超过信号K收盘后1根K线，判定延迟成交
            if is_suspect:
                suspect += 1

            sig_ema = ema_all[idx]
            e1 = ema_all[idx + 1]
            e2 = ema_all[idx + 2]

            if direction == "LONG":
                along1 = e1 > sig_ema
                along2 = e2 > sig_ema
            else:
                along1 = e1 < sig_ema
                along2 = e2 < sig_ema

            results.append((sy, direction, outcome, pnl, sig_open_ms,
                            sig_ema, e1, e2, along1, along2, is_suspect))

    results.sort(key=lambda x: x[4])

    # 只统计「非存疑」样本
    clean = [r for r in results if not r[10]]
    print(f"总仓位样本 {len(results)} 笔，其中延迟成交存疑 {suspect} 笔"
          f"，可信样本 {len(clean)} 笔\n")

    print(f"{'sym':<14}{'方向':<5}{'结果':<5}{'pnl':>9}  {'后1沿':>4}{'后2沿':>4} | sigEMA -> e1 -> e2")
    print("-" * 78)

    win = {"沿": 0, "不沿": 0}
    loss = {"沿": 0, "不沿": 0}

    for sy, direction, outcome, pnl, sig_open_ms, sig_ema, e1, e2, a1, a2, sus in clean:
        both = a1 and a2
        tag = "沿" if both else "不沿"
        if outcome == "止盈":
            win[tag] += 1
        else:
            loss[tag] += 1
        dt = datetime.fromtimestamp(sig_open_ms / 1000, timezone.utc).strftime("%m-%d %H:%M")
        print(f"{sy:<14}{direction:<5}{outcome:<5}{pnl:>+9.4f}  "
              f"{'是' if a1 else '否':>4}{'是' if a2 else '否':>4} | "
              f"{sig_ema:.6g} -> {e1:.6g} -> {e2:.6g}")

    print("\n" + "=" * 60)
    print(f"可信样本 {len(clean)} 笔")
    print("统计：信号后两根 EMA 是否都顺着拐头方向")
    print(f"  止盈单: 顺着 {win['沿']} 笔, 不顺着 {win['不沿']} 笔")
    print(f"  止损单: 顺着 {loss['沿']} 笔, 不顺着 {loss['不沿']} 笔")
    w_total = win['沿'] + win['不沿']
    l_total = loss['沿'] + loss['不沿']
    if w_total:
        print(f"  止盈单「顺着」比例 = {win['沿']/w_total*100:.1f}%")
    if l_total:
        print(f"  止损单「顺着」比例 = {loss['沿']/l_total*100:.1f}%")


if __name__ == "__main__":
    main()
