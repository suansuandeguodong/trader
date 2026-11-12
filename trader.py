"""
EMA20 拐头反转 · 量化实盘交易主程序（多空双向 · 自动下单 · 自动止盈止损）

流程：
  15m K线收盘前 lead_seconds 秒 → 识别拐头信号（向上做多 / 向下做空）
  → 计算 entry/stop/tp 三价 → 挂限价单开仓
  → 15 分钟未成交撤单；成交后挂 STOP_MARKET 止损 + TAKE_PROFIT_MARKET 止盈
  → 监控持仓，止盈/止损触发自动平仓

用法：
  python trader.py
"""
import os
import sys
import signal
import time
import math
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import yaml
from dotenv import load_dotenv

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

sys.path.insert(0, str(Path(__file__).parent))

from market import get_all_usdt_perpetuals, get_24hr_tickers, get_24hr_prices, get_15m_klines
from sliding import SlidingEMA
from reversal import EMA20Reversal
from reversal_down import EMA20ReversalDown
from broker import BinanceFutures
from risk import RiskManager
from pullback import PullbackManager
from shadow_filter import is_shadow_signal
from wecom_notifier import WeComNotifier
import trade_log as tlog

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

# ── 配置加载 ──
with open(BASE_DIR / "config.yaml", "r", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

MIN_VOLUME_USDT = CFG.get("min_volume_usdt", 18_000_000)
EMA_PERIOD = CFG.get("ema_period", 20)
MIN_DOWN_BARS = CFG.get("min_down_bars", 8)
MIN_UP_BARS = CFG.get("min_up_bars", 8)
MIN_BODY_PCT = CFG.get("min_body_pct", 1.0)
MAX_BODY_PCT = CFG.get("max_body_pct", 3.0)
WINDOW_SIZE = CFG.get("window_size", 300)
WARMUP_LIMIT = CFG.get("warmup_limit", 500)
MAX_WORKERS = CFG.get("max_workers", 20)
BATCH_SIZE = CFG.get("batch_size", 25)
BATCH_INTERVAL = CFG.get("batch_interval", 1.5)
RESCAN_INTERVAL = CFG.get("rescan_interval", 14400)
LEAD_SECONDS = CFG.get("lead_seconds", 15)

NOTIONAL_USDT = CFG.get("order_notional_usdt", 108.0)
RISK_REWARD = CFG.get("risk_reward_ratio", 2.0)
MAX_POSITIONS = CFG.get("max_positions", 15)
CANCEL_TIMEOUT_SEC = CFG.get("cancel_timeout_sec", 900)
LEVERAGE = CFG.get("leverage", 10)

# 3m 回调入场
ENABLE_PULLBACK = CFG.get("enable_pullback", True)
PULLBACK_EMA_PERIOD = CFG.get("pullback_ema_period", 20)
PULLBACK_WARMUP = CFG.get("pullback_warmup", 300)
PULLBACK_WINDOW = CFG.get("pullback_window", 300)
PULLBACK_WATCH_SECONDS = CFG.get("pullback_watch_seconds", 1800)
PULLBACK_CHECK_EVERY = CFG.get("pullback_check_every", 60)

BAR_INTERVAL_MS = 15 * 60 * 1000  # 15m
_running = True
_notifier = None  # 企微通知器（main 里初始化，模块级函数复用）


# ── 数据拉取 ──
def build_candidates() -> list:
    all_symbols = get_all_usdt_perpetuals()
    volumes = get_24hr_tickers()
    prices = get_24hr_prices()
    result = []
    for s in all_symbols:
        if volumes.get(s, 0) < MIN_VOLUME_USDT:
            continue
        price = prices.get(s, 0)
        if price <= 0:
            continue
        result.append(s)
    return result


def _fetch_bars(sym, limit):
    try:
        return sym, get_15m_klines(sym, limit=limit)
    except Exception:
        return sym, None


def _fetch_batch(symbols, limit, workers):
    result = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_fetch_bars, s, limit): s for s in symbols}
        for fut in as_completed(futures):
            sym, bars = fut.result()
            if bars:
                result[sym] = bars
    return result


def parallel_fetch(symbols, limit, workers=MAX_WORKERS,
                   batch_size=BATCH_SIZE, batch_interval=BATCH_INTERVAL):
    result = {}
    symbols = list(symbols)
    per_batch_workers = min(workers, batch_size)
    for i in range(0, len(symbols), batch_size):
        batch = symbols[i:i + batch_size]
        result.update(_fetch_batch(batch, limit, per_batch_workers))
        if i + batch_size < len(symbols):
            time.sleep(batch_interval)
    return result


# ── 持仓管理 ──
def sync_positions_from_exchange(rm: RiskManager):
    """
    用交易所当前持仓校正本地状态（程序重启后恢复 + 检测止盈止损触发后的平仓）。

    关键：止盈/止损单是 reduceOnly 单，触发后交易所持仓消失。
    这里检测「本地认为持仓中、但交易所已无该方向仓位」的 (symbol, direction)，
    撤销其所有残留挂单（止盈/止损单）并移除本地记录。

    双向持仓模式：同一 symbol 可有 LONG 和 SHORT 两个独立仓位，
    用 (symbol, position_side) 区分。
    """
    try:
        positions = rm.broker.get_open_positions()
        # key = (symbol, positionSide)，只保留 positionAmt != 0 的
        exchange_keys = set()
        for p in positions:
            amt = float(p.get("positionAmt", 0))
            if amt != 0:
                exchange_keys.add((p["symbol"], p.get("positionSide", "BOTH")))

        for key in list(rm.trades.keys()):
            t = rm.trades[key]
            if not t.position_open:
                continue
            sym, pos_side = key
            if (sym, pos_side) in exchange_keys:
                continue
            # 每笔平仓处理独立 try，避免单笔异常（如撤单 API 失败）影响其它持仓的平仓通知与清理
            try:
                _handle_one_close(rm, t, sym, pos_side)
            except Exception as e:
                print(f"  [平仓处理] {sym} {t.direction} 异常: {e}")
                # 即便处理异常，仍尽力移除本地记录，避免下轮重复检测造成死循环
                try:
                    rm.remove_trade(sym, t.direction)
                except Exception:
                    pass
    except Exception as e:
        print(f"  [同步] 异常: {e}")


def _handle_one_close(rm: RiskManager, t, sym: str, pos_side: str):
    """处理一笔「交易所已无该方向仓位」的平仓：算盈亏 → 记日志 → 通知 → 撤残留单 → 移除。"""
    print(f"  [平仓] {sym} {t.direction} 交易所已无该方向仓位（止盈/止损触发），清理残留")

    # 判定止损/止盈：拉该 symbol 成交，汇总「本笔方向」的非零 realizedPnl。
    # 关键：币安 userTrades 只有「平仓成交」带非零 realizedPnl（开仓成交为 0），
    # 所以只要是本方向(positionSide==direction)的非零 realizedPnl，就是本笔
    # 平仓的精确已实现盈亏（交易所已算好），直接求和即可，无需时间戳反推。
    realized = None
    try:
        # 时间窗口起点 = 开仓挂单时间往前留 1 分钟余量（覆盖挂单前已成交）
        # 若无挂单时间，退回到前 1 小时（足够覆盖一笔持仓的生命周期）。
        if t.entry_order_time:
            start_ms = int(t.entry_order_time * 1000) - 60 * 1000
        else:
            start_ms = int(time.time() * 1000) - 3600 * 1000
        # 用 startTime 精确限定，避免拉满 50 笔时漏掉本笔平仓成交
        trades = rm.broker.get_user_trades(sym, limit=100, start_time=start_ms)
        total = 0.0
        found = False
        for tr in trades:
            tr_side = tr.get("positionSide", "BOTH")
            # 只累积「本笔方向」的成交（LONG/SHORT 精确匹配）
            if tr_side != t.direction:
                continue
            rp = float(tr.get("realizedPnl", 0) or 0)
            # 平仓成交带非零 realizedPnl，求和即为本笔精确盈亏
            if abs(rp) > 1e-9:
                total += rp
                found = True
        if found:
            realized = total
    except Exception as e:
        print(f"  [盈亏] {sym} 拉取 realizedPnl 失败: {e}")
        realized = None

    is_stop = False
    close_reason = "平仓（盈亏未知）"
    if realized is not None:
        is_stop = realized < 0
        close_reason = "止损触发" if is_stop else "止盈触发"

    # 计算持仓时长（秒），附带进平仓记录
    hold_sec = None
    if t.entry_order_time:
        hold_sec = round(time.time() - t.entry_order_time, 1)
    tlog.log_close(sym, t.direction, getattr(t, "trade_id", ""),
                   close_reason,
                   price=t.entry_price,
                   pnl=realized,
                   close_time_ms=int(time.time() * 1000))
    if hold_sec is not None:
        # 补充持仓时长到已写记录
        tlog.log_position(sym, t.direction, getattr(t, "trade_id", ""),
                          {"hold_sec": hold_sec, "entry_price": t.entry_price,
                           "stop_price": t.stop_price, "tp_price": t.take_profit_price})

    # 企微平仓通知。无论盈亏是否算出，都必须发通知（避免「平仓了但没通知」）。
    # 盈亏算不出时，仍发通知但盈亏标注「未知」。
    if realized is not None:
        emoji = "❌" if is_stop else "✅"
    else:
        emoji = "⚪"
    pnl_str = (f"{realized:+.4f} USDT"
               if realized is not None else "未知（未取到盈亏）")
    color = ("warning" if is_stop else "info") if realized is not None else "comment"
    _notify(
        f"**{emoji} {close_reason} · {sym}**\n"
        f"\n"
        f"方向: {'做多 LONG' if t.direction == 'LONG' else '做空 SHORT'}\n"
        f"入场价: {t.entry_price:.6g} USDT\n"
        f"已实现盈亏: <font color=\"{color}\">"
        f"{pnl_str}</font>"
        + (f"\n持仓时长: {hold_sec/60:.1f} 分钟" if hold_sec else "")
    )

    # 撤销残留单：
    #   止损单是 Algo 条件单 -> cancel_algo_order
    #   止盈单是普通限价单 -> cancel_order
    # 两单一剩另一，需全部撤掉。
    if t.stop_order_id:
        rm.broker.cancel_algo_order(sym, t.stop_order_id)
    if t.tp_order_id:
        rm.broker.cancel_order(sym, t.tp_order_id)

    # 兜底：撤销该 symbol 的所有普通挂单（防遗漏）
    rm.broker.cancel_all_open_orders(sym)
    rm.remove_trade(sym, t.direction)


def manage_pending_orders(rm: RiskManager):
    """
    处理挂单中（PENDING / PARTIALLY_FILLED）的交易：
      - 每轮查询订单累计成交量，对「增量」部分挂止盈止损（成交一点、保护一点）。
      - FILLED：整笔成交完毕，状态收尾。
      - PARTIALLY_FILLED：给新成交的增量补挂止盈止损，剩余挂单继续等，超时撤剩余。
      - NEW：未成交，超时撤单。
    """
    now = time.time()
    for key in list(rm.trades.keys()):
        t = rm.trades[key]
        if t.state not in ("PENDING", "PARTIALLY_FILLED"):
            continue
        if t.entry_order_id is None:
            continue

        sym, pos_side = key

        # 查询订单状态
        order = rm.broker.query_order(sym, t.entry_order_id)
        if not order:
            continue
        status = order.get("status")

        # 已累计成交的数量，与「已保护数量」对比，只对新增量挂止损止盈
        cum_qty = float(order.get("executedQty", 0))
        avg_price = float(order.get("avgPrice", 0))
        new_qty = cum_qty - t.filled_qty

        if new_qty > 0 and avg_price > 0:
            # 对增量做 stepSize 对齐，避免浮点误差传入下单接口
            try:
                new_qty = rm.broker.round_qty(sym, new_qty)
            except Exception:
                pass
            if new_qty <= 0:
                new_qty = cum_qty - t.filled_qty  # 对齐后过小，回退原始增量
            # 新成交了一批，给它单独挂止损止盈（reduceOnly，可多张并存）
            t.filled_qty = cum_qty
            t.position_open = True
            _place_stop_tp(rm, t, qty=new_qty, fill_price=avg_price)
            tlog.log_fill(sym, t.direction, getattr(t, "trade_id", ""),
                          new_qty, avg_price, cum_qty, t.qty)
            print(f"  [成交] {sym} {t.direction} 新增成交 {new_qty} 个 "
                  f"（累计 {cum_qty}/{t.qty}），已挂止损止盈")

        # 状态收尾
        if status == "FILLED":
            t.state = "FILLED"
            t.qty = cum_qty
        elif status == "PARTIALLY_FILLED":
            t.state = "PARTIALLY_FILLED"
            # 剩余未成交的部分超时撤单
            if t.entry_order_time and now - t.entry_order_time >= CANCEL_TIMEOUT_SEC:
                rm.broker.cancel_order(sym, t.entry_order_id)
                tlog.log_cancel(sym, t.direction, getattr(t, "trade_id", ""),
                                "剩余未成交部分超时")
                print(f"  [撤剩余] {sym} 剩余未成交部分超时，已撤销")
                t.state = t.state  # 已成交部分继续持有，保留当前状态
        elif status == "NEW":
            if t.entry_order_time and now - t.entry_order_time >= CANCEL_TIMEOUT_SEC:
                rm.broker.cancel_order(sym, t.entry_order_id)
                t.state = "CANCELLED"
                tlog.log_cancel(sym, t.direction, getattr(t, "trade_id", ""),
                                "开仓单 15 分钟未成交")
                print(f"  [撤单] {sym} 开仓单 15 分钟未成交，已撤销")
                rm.remove_trade(sym, t.direction)
        elif status == "CANCELED" or status == "EXPIRED":
            # 若已成交部分存在，保留持仓；若完全没成交，清除
            if t.filled_qty <= 0:
                t.state = "CANCELLED"
                tlog.log_cancel(sym, t.direction, getattr(t, "trade_id", ""),
                                f"订单状态 {status}")
                rm.remove_trade(sym, t.direction)
        elif status == "REJECTED":
            t.state = "CANCELLED"
            tlog.log_reject(sym, t.direction, getattr(t, "trade_id", ""),
                            "开仓单被拒")
            print(f"  [拒绝] {sym} 开仓单被拒")
            rm.remove_trade(sym, t.direction)


def _place_stop_tp(rm: RiskManager, t, qty=None, fill_price=None):
    """
    给已成交的仓位挂止损 + 止盈单（都在交易所端，程序崩溃也能执行）。

    止损价 = 信号 K 线开盘价（t.stop_price，保持不变）。
    止盈价基于「实际成交价」重新计算，保证盈亏比严格 2:1：
      - 做多：止盈 = 成交价 + (成交价 - 止损价) / 2
      - 做空：止盈 = 成交价 - (止损价 - 成交价) / 2

    qty: 本次要保护的数量（默认整笔 t.qty；部分成交增量时传入新增量）。
    fill_price: 实际成交均价（avgPrice）。若未传，则回退用 entry_price。
    """
    if fill_price is None:
        fill_price = t.entry_price
    if qty is None:
        qty = t.qty

    stop_side = rm.side_for_exit(t.direction)
    pos_side = rm.position_side(t.direction)

    # 基于成交价重算止盈价（止损价保持 = 信号 open 不变）
    stop = t.stop_price
    if t.half_body is not None:
        # 3m回调策略：止盈 = 真实成交价 ± 信号实体的一半
        if t.direction == "LONG":
            tp = fill_price + t.half_body
        else:  # SHORT
            tp = fill_price - t.half_body
    else:
        if t.direction == "LONG":
            tp = fill_price + (fill_price - stop) / 2.0
        else:  # SHORT
            tp = fill_price - (stop - fill_price) / 2.0

    # 对齐精度
    stop = rm.broker.round_price(t.symbol, stop)
    tp = rm.broker.round_price(t.symbol, tp)

    # 更新到 Trade
    t.stop_price = stop
    t.take_profit_price = tp

    # 关键防御：成交价已越过预定止损价时，止损单（STOP_MARKET）会因触发价方向
    # 违反交易所规则而被拒（做多要求 triggerPrice < 当前价，做空反之）。
    # 此时应立即市价平仓止损，避免裸仓暴露，而不是干等下一轮。
    crossed = False
    if t.direction == "LONG" and fill_price <= stop:
        crossed = True
    elif t.direction == "SHORT" and fill_price >= stop:
        crossed = True

    if crossed:
        print(f"  [止损越界] {t.symbol} {t.direction} 成交价 {fill_price:.6g} "
              f"已越过止损价 {stop:.6g}，立即市价平仓止损")
        close_r = rm.broker.place_market_order(
            t.symbol, stop_side, qty, position_side=pos_side)
        if close_r:
            tlog.log_close(t.symbol, t.direction, getattr(t, "trade_id", ""),
                           "止损价越界·市价平仓",
                           price=fill_price, pnl=None,
                           close_time_ms=int(time.time() * 1000))
            _notify(
                f"**⚠️ 止损越界平仓 · {t.symbol}**\n"
                f"\n"
                f"方向: {'做多 LONG' if t.direction == 'LONG' else '做空 SHORT'}\n"
                f"成交价 {fill_price:.6g} 已越过止损价 {stop:.6g}，已市价平仓止损。"
            )
        else:
            print(f"  [止损越界] {t.symbol} 市价平仓也失败，陷入裸仓，人工介入！")
            _notify(
                f"**🚨 急·市价平仓失败 · {t.symbol}**\n"
                f"\n"
                f"成交价 {fill_price:.6g} 已越过止损价 {stop:.6g}，"
                f"且市价平仓失败，请立即人工处理。"
            )
        return

    ok = True
    # 挂止损单（条件单，Algo 端点，STOP_MARKET）
    stop_r = rm.broker.place_stop_market(
        t.symbol, stop_side, qty, stop,
        position_side=pos_side)
    if stop_r:
        t.stop_order_id = stop_r.get("orderId")
    else:
        ok = False
        print(f"  [止损止盈·失败] {t.symbol} 止损单挂失败")

    # 挂止盈单（普通限价单，固定价格平仓，无需触发价）
    tp_r = rm.broker.place_limit_order(
        t.symbol, stop_side, qty, tp, position_side=pos_side)
    if tp_r:
        t.tp_order_id = tp_r.get("orderId")
    else:
        ok = False
        print(f"  [止损止盈·失败] {t.symbol} 止盈限价单挂失败")

    if ok:
        tlog.log_stop_tp(t.symbol, t.direction, getattr(t, "trade_id", ""),
                         stop, tp, qty,
                         stop_order_id=t.stop_order_id,
                         tp_order_id=t.tp_order_id)
        print(f"  [止损止盈] {t.symbol} 止损@{stop:.6g}（条件单） "
              f"止盈@{tp:.6g}（限价单）已挂单")


# 全局锁：保护 rm.trades 的并发访问（后台线程 + 主循环）
_rm_lock = threading.Lock()


def monitor_loop(rm: RiskManager):
    """
    后台监控线程：每 15 秒检查一次挂单成交状态 + 持仓状态。

    解决「开仓单延迟成交后，主循环每 15 分钟才检查一次，导致成交后
    长时间不挂止盈止损」的问题。这个线程持续高频检查，成交后很快
    就能补挂止盈止损。
    """
    global _running
    while _running:
        try:
            with _rm_lock:
                manage_pending_orders(rm)
                sync_positions_from_exchange(rm)
        except Exception as e:
            print(f"  [监控线程] 异常: {e}")
        # 分段 sleep，便于响应退出
        for _ in range(15):
            if not _running:
                break
            time.sleep(1)


def _notify(markdown: str):
    """统一企微通知出口：notifier 未初始化或未启用时静默跳过。"""
    if _notifier is not None and _notifier.enabled:
        _notifier.send_markdown(markdown)


def pullback_monitor_loop(rm: RiskManager, pullback_mgr: PullbackManager,
                          check_every: int = 15):
    """
    3m 回调监控线程：周期性检查「观察中」的 15m 信号，判定是否回调触碰 3m EMA20。

    触发后调用 open_trade 以 3m EMA20 值挂限价单入场；
    观察期超时则放弃该信号。
    """
    global _running
    while _running:
        try:
            now_ms = int(time.time() * 1000)
            # 快照观察中的信号（避免迭代中修改字典）
            to_check = list(pullback_mgr.pending.items())
            for key, ps in to_check:
                sym, direction = ps.symbol, ps.direction
                # 超时尽早判：跳过仍会继续拉数据的币
                result = pullback_mgr.check_touch(sym, direction, now_ms)
                if result is None:
                    continue
                if result.get("__timeout__"):
                    pullback_mgr.remove(sym, direction)
                    print(f"  [放弃] {sym} {direction} 观察期超时未回调，放弃信号")
                    _notify(
                        f"**⌛ 放弃信号 · {sym}**\n"
                        f"\n"
                        f"方向: {'做多 LONG' if direction == 'LONG' else '做空 SHORT'}\n"
                        f"15m拐头后 {PULLBACK_WATCH_SECONDS // 60} 分钟内未回调到 3m EMA20，放弃。"
                    )
                    continue

                # 触发：用 3m EMA20 作为入场价，开仓
                entry = result["entry"]
                stop = result["stop"]
                tp = result["tp"]
                half_body = result["half_body"]

                # 先移除观察中状态（避免重复触发）
                pullback_mgr.remove(sym, direction)

                with _rm_lock:
                    if rm.has_position_or_order(sym, direction):
                        print(f"  [跳过] {sym} {direction} 已在持仓/挂单中")
                        continue
                    if not rm.can_open_new():
                        print(f"  [跳过] {sym} 已达最大持仓 {MAX_POSITIONS}")
                        continue
                    try:
                        # 防御：3m 入场价与止损价不成立则放弃（做多 entry 必须 > stop）
                        if direction == "LONG" and entry <= stop:
                            print(f"  [放弃] {sym} 3m入场价 {entry:.6g} <= 止损 {stop:.6g}，放弃开仓")
                            continue
                        if direction == "SHORT" and entry >= stop:
                            print(f"  [放弃] {sym} 3m入场价 {entry:.6g} >= 止损 {stop:.6g}，放弃开仓")
                            continue
                        # 触发入场通知
                        _notify(
                            f"**🎯 回踩到位 · {sym}**\n"
                            f"\n"
                            f"方向: **{'做多 LONG' if direction == 'LONG' else '做空 SHORT'}**\n"
                            f"3m 已触碰 EMA20，入场价 {entry:.6g}。"
                        )
                        meta = {
                            "bar_time": ps.sig_close_ms - BAR_INTERVAL_MS,
                            "open": ps.sig_open,
                            "close": ps.sig_close,
                            "prev_open": ps.prev_open,
                            "entry_source": "3m_ema20",
                        }
                        open_trade(rm, sym, direction, ps.sig_open, ps.sig_close,
                                   meta, entry=entry, stop=stop, tp=tp,
                                   half_body=half_body)
                    except Exception as e:
                        print(f"  [开仓异常] {sym}: {e}")
        except Exception as e:
            print(f"  [3m监控线程] 异常: {e}")

        # 分段 sleep，便于响应退出
        for _ in range(check_every):
            if not _running:
                break
            time.sleep(1)


def open_trade(rm: RiskManager, sym: str, direction: str, o: float, c: float,
               meta: dict = None, entry: float = None, stop: float = None,
               tp: float = None, half_body: float = None):
    """按信号开仓。meta 为信号元数据（用于详细落盘）。

    entry/stop/tp 为可选：不传则用 15m 信号价计算（compute_prices），
    传了（3m 回调入场场景）则直接用传入的三价。
    half_body: 信号K线实体的一半（3m策略），非 None 时成交后用真实成交价重算止盈。
    """
    if entry is None or stop is None or tp is None:
        entry, stop, tp = rm.compute_prices(direction, o, c)
    qty = rm.compute_qty(sym, entry)
    if qty <= 0:
        print(f"  [跳过] {sym} 数量为 0，无法开仓")
        return

    # 对齐价格（做多止损向下取整、止盈向上取整，做空对称，避免价格碰撞）
    if direction == "LONG":
        entry = rm.broker.round_price(sym, entry)
        stop = rm.broker.round_price(sym, stop, "DOWN")
        tp = rm.broker.round_price(sym, tp, "UP")
    else:  # SHORT
        entry = rm.broker.round_price(sym, entry)
        stop = rm.broker.round_price(sym, stop, "UP")
        tp = rm.broker.round_price(sym, tp, "DOWN")

    # 防御：止盈/止损价格不能反向或重叠（超低价币 tick 不足时会碰撞）
    if direction == "LONG":
        if tp <= entry:
            print(f"  [跳过] {sym} 止盈价 {tp} <= 入场价 {entry}，精度不足无法设置")
            return
        if stop >= entry:
            print(f"  [跳过] {sym} 止损价 {stop} >= 入场价 {entry}，精度不足无法设置")
            return
    else:  # SHORT
        if tp >= entry:
            print(f"  [跳过] {sym} 止盈价 {tp} >= 入场价 {entry}，精度不足无法设置")
            return
        if stop <= entry:
            print(f"  [跳过] {sym} 止损价 {stop} <= 入场价 {entry}，精度不足无法设置")
            return

    # 校验最小名义值（用对齐后的 entry）
    notional = qty * entry
    min_notional = rm.broker.get_min_notional(sym)
    if notional < min_notional:
        print(f"  [跳过] {sym} 名义值 {notional:.2f}U < 最小 {min_notional}U")
        return

    # 设置杠杆（尝试，失败不阻断，但打印真实原因）
    try:
        lev_r = rm.broker.set_leverage(sym, LEVERAGE)
        if lev_r:
            # 成功：币安返回 leverage 字段
            actual = lev_r.get("leverage")
            if actual is not None and int(actual) != LEVERAGE:
                print(f"  [杠杆] {sym} 实际杠杆 {actual}x（请求 {LEVERAGE}x）")
        else:
            # 失败：打印提示（时间戳错误修复后，这里通常是币种不支持该杠杆）
            print(f"  [杠杆] {sym} 设置 {LEVERAGE}x 未成功，将使用账户现有杠杆")
    except Exception as e:
        print(f"  [杠杆] {sym} 设置 {LEVERAGE}x 异常: {e}")

    side = rm.side_for_entry(direction)
    pos_side = rm.position_side(direction)
    # 注册交易状态（先注册，再挂单）
    t = rm.register_trade(sym, direction, entry, stop, tp, qty, half_body=half_body)
    if t is None:
        print(f"  [跳过] {sym} {direction} 已在持仓/挂单中，拒绝重复开仓")
        return
    order = rm.broker.place_limit_order(sym, side, qty, entry, position_side=pos_side)
    if order is None:
        rm.remove_trade(sym, direction)
        print(f"  [失败] {sym} 开仓单下单失败，已清除")
        return
    t.entry_order_id = order.get("orderId")
    t.entry_order_time = time.time()
    # 记录信号K线时间（观察期计时起点）
    if meta:
        t.signal_bar_time = meta.get("bar_time")
    # 生成 trade_id 并记录开仓事件
    t.trade_id = tlog.make_trade_id(sym, direction, t.entry_order_time)
    tlog.log_open(sym, direction, t.trade_id, entry, stop, tp, qty,
                  order_id=t.entry_order_id, signal_meta=meta)
    print(f"  [开仓] {sym} {direction} 限价单 qty={qty} @ {entry:.6g} "
          f"(止损@{stop:.6g} 止盈@{tp:.6g})")
    # 企微开仓通知
    _notify(
        f"**📈 开仓信号已下单**\n"
        f"\n"
        f"币种: <font color=\"warning\">{sym}</font>\n"
        f"方向: **{'做多 LONG' if direction == 'LONG' else '做空 SHORT'}**\n"
        f"入场价: {entry:.6g} USDT\n"
        f"止损价: {stop:.6g} USDT\n"
        f"止盈价: {tp:.6g} USDT\n"
        f"数量: {qty:.6g}"
    )


# ── 主循环 ──
def main():
    global _running

    print("=" * 72)
    print("  EMA20 拐头反转 · 量化实盘交易（多空双向）")
    print(f"  币池: 24h成交额>{MIN_VOLUME_USDT/1e6:.0f}M")
    print(f"  仓位: 每笔 {NOTIONAL_USDT}U | 盈亏比 {RISK_REWARD}:1 | 最大持仓 {MAX_POSITIONS}")
    print(f"  杠杆: {LEVERAGE}x | 撤单超时 {CANCEL_TIMEOUT_SEC//60} 分钟")
    print("=" * 72)

    api_key = os.getenv("BINANCE_API_KEY", "")
    api_secret = os.getenv("BINANCE_API_SECRET", "")
    proxy = CFG.get("proxy", "")
    testnet = CFG.get("testnet", False)

    if not api_key or not api_secret:
        print("[错误] 未配置 API 密钥，请在 .env 中填写 BINANCE_API_KEY / BINANCE_API_SECRET")
        return

    broker = BinanceFutures(api_key, api_secret, proxy=proxy, testnet=testnet)
    rm = RiskManager(broker, notional_usdt=NOTIONAL_USDT,
                     risk_reward=RISK_REWARD, max_positions=MAX_POSITIONS,
                     cancel_timeout_sec=CANCEL_TIMEOUT_SEC)

    # 企微通知（开仓/平仓/风控）
    global _notifier
    _notifier = WeComNotifier(os.getenv("WEBHOOK_URL", ""))
    if _notifier.enabled:
        print("企业微信通知已启用")
    else:
        print("[提示] WEBHOOK_URL 未配置，跳过企业微信通知")

    # 校验账户
    bal = broker.get_balance()
    print(f"账户可用余额: {bal:.2f} USDT\n")
    if bal <= 0:
        print("[错误] 账户余额为 0，退出")
        return

    print("构建候选币池...")
    candidates = build_candidates()
    print(f"候选币数量: {len(candidates)} 个\n")
    if not candidates:
        print("[错误] 候选币池为空，退出")
        return

    # 滑动窗口 + 双方向状态机
    sema = SlidingEMA(period=EMA_PERIOD, window_size=WINDOW_SIZE)
    rev_up = EMA20Reversal(min_down_bars=MIN_DOWN_BARS)
    rev_down = EMA20ReversalDown(min_up_bars=MIN_UP_BARS)

    # 3m 回调入场管理器（观察中状态 + 3m EMA20 触碰判定）
    pullback_mgr = PullbackManager(
        ema_period=PULLBACK_EMA_PERIOD,
        window_size=PULLBACK_WINDOW,
        warmup_limit=PULLBACK_WARMUP,
        watch_seconds=PULLBACK_WATCH_SECONDS,
        risk_reward=RISK_REWARD,
    ) if ENABLE_PULLBACK else None

    print("预热滑动窗口（并发拉历史 K 线 + 还原状态机）...")
    t0 = time.time()
    warm = parallel_fetch(candidates, WARMUP_LIMIT, workers=MAX_WORKERS)
    for sym, bars in warm.items():
        closes = [b["c"] for b in bars]
        bar_times = [b["t"] for b in bars]
        sema.init_symbol(sym, closes, bar_times)
        rev_up.warmup(sym, sema._closes[sym], period=EMA_PERIOD)
        rev_down.warmup(sym, sema._closes[sym], period=EMA_PERIOD)
    print(f"预热完成: {len(warm)}/{len(candidates)} 就绪, 耗时 {time.time()-t0:.1f}s\n")

    def shutdown(sig=None, frame=None):
        global _running
        print("\n正在关闭...")
        _running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # 启动后台监控线程：每 15 秒检查挂单成交 + 持仓状态，
    # 及时补挂止盈止损（解决开仓单延迟成交后长时间裸仓的问题）
    monitor_thread = threading.Thread(target=monitor_loop, args=(rm,), daemon=True)
    monitor_thread.start()
    print("后台监控线程已启动（每 15 秒检查挂单成交 + 持仓）\n")

    # 3m 回调监控线程（仅当启动 pullback 模式）
    if ENABLE_PULLBACK and pullback_mgr is not None:
        pb_thread = threading.Thread(
            target=pullback_monitor_loop,
            args=(rm, pullback_mgr, PULLBACK_CHECK_EVERY),
            daemon=True,
        )
        pb_thread.start()
        print(f"3m 回调监控线程已启动（每 {PULLBACK_CHECK_EVERY} 秒检查观察中的信号）\n")

    last_rescan = time.time()
    iteration = 0

    def _ts():
        return time.strftime("%H:%M:%S")

    while _running:
        iteration += 1
        loop_start = time.time()

        print(f"[{_ts()}] 第{iteration}轮：拉取 {len(candidates)} 币最新 K线...")
        data = parallel_fetch(candidates, 3, workers=MAX_WORKERS)
        fetch_s = time.time() - loop_start
        print(f"[{_ts()}] 拉取完成 {len(data)}/{len(candidates)} 币, 耗时 {fetch_s:.1f}s")

        now_ms = int(time.time() * 1000)
        new_closed = 0
        signals = []  # (sym, direction, open, close)

        for sym, bars in data.items():
            if not bars:
                continue
            for idx, b in enumerate(bars):
                bar_close_ms = b["t"] + BAR_INTERVAL_MS
                if bar_close_ms > now_ms + LEAD_SECONDS * 1000:
                    continue
                ema = sema.push_close(sym, b["t"], b["c"])
                if ema is None:
                    continue
                new_closed += 1
                o = b["o"]
                c = b["c"]
                # 信号K线前一根的开盘价（止损比较用）
                prev_open = bars[idx - 1]["o"] if idx > 0 else None

                # 向上拐头（做多）
                up_before = rev_up.get_down_count(sym)
                is_up = rev_up.on_15m_close(sym, ema)
                if is_up:
                    body_pct = (c - o) / o * 100 if o > 0 else 0.0
                    if body_pct > MAX_BODY_PCT:
                        # 实体涨幅过大，止损幅度毁灭，过滤
                        print(f"  [过滤] {sym} 拐头但实涨 {body_pct:.2f}% > {MAX_BODY_PCT}%（实体过大）")
                    elif body_pct >= MIN_BODY_PCT:
                        meta = {
                            "bar_time": b["t"],
                            "open": o, "high": b["h"], "low": b["l"],
                            "close": c, "volume": b["v"],
                            "ema": ema,
                            "down_count": up_before,
                            "body_pct": body_pct,
                            "ema_history": list(rev_up.get_ema_history(sym)),
                            "prev_open": prev_open,
                        }
                        signals.append((sym, "LONG", o, c, meta))
                        tlog.log_signal(sym, "LONG", meta)
                        print(f"\n  >>> [信号·做多] {sym} 拐头向上 "
                              f"(前持续不涨 {up_before} 根, 实涨 {body_pct:.2f}%) <<<\n")
                    elif body_pct < MIN_BODY_PCT:
                        print(f"  [过滤] {sym} 拐头但实涨 {body_pct:.2f}% < {MIN_BODY_PCT}%")

                # 向下拐头（做空）
                down_before = rev_down.get_up_count(sym)
                is_down = rev_down.on_15m_close(sym, ema)
                if is_down:
                    body_pct = (c - o) / o * 100 if o > 0 else 0.0
                    if body_pct < -MAX_BODY_PCT:
                        # 实体跌幅过大，过滤
                        print(f"  [过滤] {sym} 拐头但实跌 {body_pct:.2f}% < -{MAX_BODY_PCT}%（实体过大）")
                    elif body_pct <= -MIN_BODY_PCT:
                        meta = {
                            "bar_time": b["t"],
                            "open": o, "high": b["h"], "low": b["l"],
                            "close": c, "volume": b["v"],
                            "ema": ema,
                            "up_count": down_before,
                            "body_pct": body_pct,
                            "ema_history": list(rev_down.get_ema_history(sym)),
                            "prev_open": prev_open,
                        }
                        signals.append((sym, "SHORT", o, c, meta))
                        tlog.log_signal(sym, "SHORT", meta)
                        print(f"\n  >>> [信号·做空] {sym} 拐头向下 "
                              f"(前持续不跌 {down_before} 根, 实跌 {body_pct:.2f}%) <<<\n")
                    elif body_pct > -MIN_BODY_PCT:
                        print(f"  [过滤] {sym} 拐头但实跌 {body_pct:.2f}% > {-MIN_BODY_PCT}%")

        # 信号处理：开仓（加锁，与后台监控线程互斥）
        if signals:
            print(f"[{_ts()}] 本轮共 {len(signals)} 个信号，处理开仓...")
            with _rm_lock:
                for sym, direction, o, c, meta in signals:
                    if rm.has_position_or_order(sym, direction):
                        print(f"  [跳过] {sym} {direction} 已在持仓/挂单中")
                        continue
                    if not rm.can_open_new():
                        print(f"  [跳过] {sym} 已达最大持仓 {MAX_POSITIONS}")
                        continue
                    try:
                        # 新增逻辑分支：30m 影线条件
                        signal_open_ms = meta.get("bar_time") if meta else None
                        shadow_met = (
                            signal_open_ms is not None
                            and is_shadow_signal(sym, direction, signal_open_ms)
                        )
                        if shadow_met:
                            # 影线条件满足 → 新流程：信号收盘价挂限价单，
                            # 开盘价止损，盈亏比 1(盈):2(亏)（compute_prices 已实现）。
                            print(f"  [影线·新流程] {sym} {direction} 30m影线满足，直接挂单")
                            open_trade(rm, sym, direction, o, c, meta)
                        elif ENABLE_PULLBACK and pullback_mgr is not None:
                            # 3m 回调入场：登记为「观察中」，等 3m 回调触碰 EMA20 再开仓
                            prev_open = meta.get("prev_open") if meta else None
                            sig_low = meta.get("low") if meta else None
                            sig_high = meta.get("high") if meta else None
                            sig_close_ms = (meta.get("bar_time") if meta else 0) + BAR_INTERVAL_MS
                            ok = pullback_mgr.add_signal(
                                sym, direction, o, c, sig_low, sig_high,
                                prev_open, sig_close_ms)
                            if ok:
                                print(f"  [观察] {sym} {direction} 15m拐头，等待3m回调到EMA20")
                                _notify(
                                    f"**👀 等待回踩入场 · {sym}**\n"
                                    f"\n"
                                    f"方向: **{'做多 LONG' if direction == 'LONG' else '做空 SHORT'}**\n"
                                    f"15m已拐头，等待价格回调到 3m EMA20 再挂单。"
                                )
                            else:
                                print(f"  [跳过] {sym} {direction} 已在观察中，忽略重复")
                        else:
                            open_trade(rm, sym, direction, o, c, meta)
                    except Exception as e:
                        print(f"  [开仓异常] {sym}: {e}")

        # 管理挂单 + 持仓（撤单 / 挂止盈止损 / 同步），加锁
        try:
            with _rm_lock:
                manage_pending_orders(rm)
                sync_positions_from_exchange(rm)
        except Exception as e:
            print(f"  [管理异常]: {e}")

        # 打印当前状态
        if rm.trades:
            filled = sum(1 for t in rm.trades.values() if t.position_open)
            pending = sum(1 for t in rm.trades.values() if t.state == "PENDING")
            print(f"[{_ts()}] 当前持仓 {filled} 个，挂单中 {pending} 个")

        # 定期刷新候选池
        if time.time() - last_rescan >= RESCAN_INTERVAL:
            print("\n刷新候选币池...")
            try:
                new_candidates = build_candidates()
                added = [s for s in new_candidates if s not in candidates]
                if added:
                    w = parallel_fetch(added, WARMUP_LIMIT, workers=MAX_WORKERS)
                    for sym, bars in w.items():
                        closes = [b["c"] for b in bars]
                        bar_times = [b["t"] for b in bars]
                        sema.init_symbol(sym, closes, bar_times)
                        rev_up.warmup(sym, sema._closes[sym], period=EMA_PERIOD)
                        rev_down.warmup(sym, sema._closes[sym], period=EMA_PERIOD)
                for s in [x for x in candidates if x not in new_candidates]:
                    rev_up.reset(s)
                    rev_down.reset(s)
                    sema.reset(s)
                    if pullback_mgr is not None:
                        pullback_mgr.reset(s)
                candidates = new_candidates
                print(f"候选币数量: {len(candidates)} 个（新增 {len(added)}）\n")
            except Exception as e:
                print(f"[刷新] 异常: {e}")
            last_rescan = time.time()

        # 对齐下一根 15m K 线收盘前 lead 秒
        now = time.time()
        next_close = (now // (BAR_INTERVAL_MS / 1000) + 1) * (BAR_INTERVAL_MS / 1000)
        trigger_at = next_close - LEAD_SECONDS
        wait_s = trigger_at - now
        if wait_s > 0:
            print(f"[{_ts()}] 等待下一根 15m K线收盘前 {LEAD_SECONDS} 秒触发 "
                  f"({time.strftime('%H:%M:%S', time.localtime(trigger_at))})...\n")
            while wait_s > 0 and _running:
                sleep = min(10, wait_s)
                time.sleep(sleep)
                wait_s -= sleep

    print("已退出")


if __name__ == "__main__":
    main()
