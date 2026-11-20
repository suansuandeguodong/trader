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
import queue
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

import market
from market import get_all_usdt_perpetuals, get_24hr_tickers, get_24hr_prices, get_klines
from sliding import SlidingEMA
from reversal import EMA20Reversal
from reversal_down import EMA20ReversalDown
from broker import BinanceFutures
from risk import RiskManager
from pullback import PullbackManager
from resonance import ResonanceManager
from wecom_notifier import WeComNotifier
import trade_log as tlog

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

# ── 配置加载 ──
with open(BASE_DIR / "config.yaml", "r", encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

MIN_VOLUME_USDT = CFG.get("min_volume_usdt", 18_000_000)
SIGNAL_INTERVAL = CFG.get("signal_interval", "15m")  # 信号 K 线周期："15m" / "5m"
EMA_PERIOD = CFG.get("ema_period", 20)
MIN_DOWN_BARS = CFG.get("min_down_bars", 13)
MIN_UP_BARS = CFG.get("min_up_bars", 13)
MIN_BODY_PCT = CFG.get("min_body_pct", 1.0)
MAX_BODY_PCT = CFG.get("max_body_pct", 3.0)
WINDOW_SIZE = CFG.get("window_size", 300)
WARMUP_LIMIT = CFG.get("warmup_limit", 500)
MAX_WORKERS = CFG.get("max_workers", 20)
JUDGE_WORKERS = CFG.get("judge_workers", 8)  # 判断信号并发度（纯CPU，独立于拉取线程池）
BATCH_SIZE = CFG.get("batch_size", 25)
BATCH_INTERVAL = CFG.get("batch_interval", 1.5)
RESCAN_INTERVAL = CFG.get("rescan_interval", 14400)
LEAD_SECONDS = CFG.get("lead_seconds", 15)

# ── 休息时段：本机时间在此区间内，停止拉取/判断/开仓，一次性 sleep 到结束 ──
REST_START = CFG.get("rest_start", "")   # 形如 "19:57"，空串表示不启用
REST_END = CFG.get("rest_end", "")       # 形如 "23:57"，空串表示不启用

NOTIONAL_USDT = CFG.get("order_notional_usdt", 30.0)
RISK_REWARD = CFG.get("risk_reward_ratio", 2.0)
MAX_POSITIONS = CFG.get("max_positions", 7)
CANCEL_TIMEOUT_SEC = CFG.get("cancel_timeout_sec", 900)
LEVERAGE = CFG.get("leverage", 25)

# 策略模式：resonance = 拐头 + EMA10/20 共振确认立即开仓；pullback = 3m 回调入场
STRATEGY_MODE = CFG.get("strategy_mode", "resonance")

# 3m 回调入场
ENABLE_PULLBACK = CFG.get("enable_pullback", True)
PULLBACK_EMA_PERIOD = CFG.get("pullback_ema_period", 20)
PULLBACK_WARMUP = CFG.get("pullback_warmup", 300)
PULLBACK_WINDOW = CFG.get("pullback_window", 300)
PULLBACK_WATCH_SECONDS = CFG.get("pullback_watch_seconds", 1800)
PULLBACK_CHECK_EVERY = CFG.get("pullback_check_every", 60)

def _interval_to_ms(interval: str) -> int:
    """把币安 interval 字符串转成毫秒时长。支持 m/h/d 后缀。"""
    unit = interval[-1].lower()
    num = interval[:-1]
    try:
        n = int(num)
    except ValueError:
        return 15 * 60 * 1000
    mul = {"m": 60 * 1000, "h": 3600 * 1000, "d": 86400 * 1000}
    return n * mul.get(unit, 60 * 1000)


BAR_INTERVAL_MS = _interval_to_ms(SIGNAL_INTERVAL)  # 信号 K 线周期（默认 15m）
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


def _fetch_bars(sym, limit, startTime=None):
    try:
        return sym, get_klines(sym, SIGNAL_INTERVAL, limit=limit,
                               startTime=startTime)
    except Exception:
        return sym, None


def _fetch_batch(symbols, limit, workers):
    result = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_fetch_bars, s, limit): s for s in symbols}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                # 单个 symbol 的硬超时兜底：即使 _fetch_json 内部仍在重试，
                # 也不阻塞整批（as_completed 本会等最慢的那个）。
                sym, bars = fut.result(timeout=30)
                if bars:
                    result[sym] = bars
            except Exception as e:
                # 超时/异常：该币本轮放弃，记日志，下一轮再补
                print(f"  [拉取] {sym} 超时放弃本轮: {type(e).__name__}")
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


# ── 完整 K 线缓存（内存换速度）：{symbol: [完整K线dict, ...]}，旧→新 ──
# 预热时全量填充，每轮增量追加，判断时从缓存读，避免重复拉取。
# 每根 K 线含 o/h/l/c/v/t 五元组 + 时间戳，用于判断拐头、止损 prev_open 等。
_bars_cache = {}
# 缓存访问锁（候选池刷新时会并发读写）
_bars_cache_lock = threading.Lock()

# ── 快照预判去重水位：{symbol: 最后预判过的快照 openTime} ──
# 同一根「进行中」快照在收盘前 lead 秒窗口内会被反复拉取，用此水位避免
# 对同一根 bar_time 重复预判/重复下单（完整推进那根由 push_close 的
# last_bar_time 去重，快照分支需独立水位）。
_snapshot_seen = {}
_snapshot_seen_lock = threading.Lock()

# ── 已开仓信号 bar_time 去重：{(symbol, direction): set(bar_time)} ──
# 关键：同一根 K 线的拐头信号会被「快照预判」和「完整推进」各触发一次。
# 若首次触发后止盈/止损平仓（rm.trades 的 key 被 remove_trade 清掉），
# 下一轮同一根 bar_time 的旧信号会再次通过 has_position_or_order 检查、
# 造成「用 5 分钟前的旧信号重复开仓」。此记录跨平仓存续，挡住同一根
# bar_time 不再开仓。
_opened_bars = {}
_opened_bars_lock = threading.Lock()


def _mark_bar_opened(symbol, direction, bar_time):
    """记录「已为某根 bar_time 开过仓」，跨平仓存续，用于去重旧信号。"""
    if bar_time is None:
        return
    key = (symbol, direction)
    with _opened_bars_lock:
        opened = _opened_bars.setdefault(key, set())
        opened.add(bar_time)
        # 只保留最近 50 根，防止无限增长（50 根足够覆盖相邻信号去重需求）
        if len(opened) > 50:
            # 转列表按时间排序丢弃最旧的（bar_time 是 ms 时间戳，可排序）
            keep = sorted(opened)[-50:]
            _opened_bars[key] = set(keep)


def _clear_bar_opened(symbol, direction=None):
    """清理某 symbol 的已开仓去重记录（候选池移除 / reset 时调用）。"""
    with _opened_bars_lock:
        if direction is None:
            for k in list(_opened_bars.keys()):
                if k[0] == symbol:
                    _opened_bars.pop(k, None)
        else:
            _opened_bars.pop((symbol, direction), None)


def cache_add_bars(symbol, bars):
    """把增量 K 线按 bar_time 去重后追加进缓存。bars 须旧→新。

    关键：只缓存「已收盘」的 K 线（closeTime <= now），跳过「进行中未收盘」的根。
    否则进行中 K 线的 close 是快照，会被固化，收盘后的准确值因 bar_time 去重而
    永远进不来，导致 EMA/拐头状态机用错收盘价（如 up_count 虚高）。
    """
    if not bars:
        return
    now_ms = int(time.time() * 1000)
    with _bars_cache_lock:
        existing = _bars_cache.setdefault(symbol, [])
        last_t = existing[-1]["t"] if existing else 0
        for b in bars:
            # 只收已收盘的根（进行中的根本轮丢弃，等它下轮收盘再以准确值进入）
            if b["t"] + BAR_INTERVAL_MS > now_ms:
                continue
            if b["t"] > last_t:
                existing.append(b)
                last_t = b["t"]
        # 滑出最旧的，控制内存
        if len(existing) > WINDOW_SIZE:
            del existing[:len(existing) - WINDOW_SIZE]


def cache_clear_symbol(symbol):
    """候选池移除某 symbol 时清理缓存。"""
    with _bars_cache_lock:
        _bars_cache.pop(symbol, None)


# 哨兵：标记一轮拉取结束
_fetch_sentinel = object()


def _produce_bars(symbols, limit, workers, out_queue):
    """生产者：线程池并发拉取，谁先完成谁 put 进有界队列。

    增量拉取：每个 symbol 只拉「缓存水位 last_t 之后」的 K 线（startTime 精确
    定位），默认 limit=2 —— 返回的最后一根可能是「进行中未收盘」的快照，倒数
    第二根是「刚收盘」的完整根。二者由消费者按 closeTime 区分：
      - closeTime > now  → 进行中快照，只用于提前预判拐头（不进缓存）
      - closeTime <= now → 已收盘完整根，进缓存 + 推进状态机

    无缓存（新币/首次）时全量拉 WARMUP_LIMIT。
    """
    def _worker(sym):
        try:
            with _bars_cache_lock:
                last_t = _bars_cache[sym][-1]["t"] if _bars_cache.get(sym) else None
            if last_t is None:
                sym, bars = _fetch_bars(sym, WARMUP_LIMIT)
            else:
                sym, bars = _fetch_bars(sym, limit, startTime=last_t + 1)
        except Exception as e:
            bars = None
            print(f"  [拉取] {sym} 异常: {type(e).__name__}")
        return sym, bars

    try:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(_worker, s) for s in symbols]
            for fut in as_completed(futures):
                try:
                    sym, bars = fut.result(timeout=30)
                except Exception as e:
                    print(f"  [拉取] 超时放弃本轮: {type(e).__name__}")
                    continue
                if bars:
                    out_queue.put((sym, bars))  # 有界队列满时阻塞 = 背压
    finally:
        # 无论异常与否，都要放入哨兵，否则消费者会永久阻塞
        out_queue.put(_fetch_sentinel)


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

    # 选项A：彻底结清这一笔（精准撤销开仓单 → 确认撤干净 → 强校验持仓归零 → 才移除）
    _close_and_cleanup(rm, t, sym, pos_side)


def _close_and_cleanup(rm: RiskManager, t, sym: str, pos_side: str):
    """
    选项A：止损/止盈触发后，彻底结清这一笔，杜绝「剩余开仓单复活成裸仓」。

    相较于旧逻辑的「撤一下就走」，这里执行「精准撤销 → 确认撤干净 → 强校验持仓归零」：

      1) 精准撤销本笔开仓单（entry_order_id），不误伤同 symbol 另一方向。
      2) 撤销止损单（algoOrder）与止盈限价单。
      3) 轮询确认开仓单已 CANCELED/EXPIRED；若撤单过程中又有新成交
         （executedQty > t.filled_qty），立即市价平掉这部分增量，避免裸仓。
      4) 再次强校验该 (symbol, direction) 的交易所持仓已归零；若不归零，市价补平。
      5) 全部确认干净后才移除本地记录。

    任何一步异常都不隐藏，打印告警并保守保留本地记录，交由下一轮重试（而不盲目移除）。
    """
    stop_side = rm.side_for_exit(t.direction)

    # 1) 精准撤销本笔开仓限价单（而非 cancel_all_open_orders，避免误伤另一方向）
    if t.entry_order_id:
        rm.broker.cancel_order(sym, t.entry_order_id)

    # 2) 撤止损单（Algo）与止盈限价单
    if t.stop_order_id:
        rm.broker.cancel_algo_order(sym, t.stop_order_id)
    if t.tp_order_id:
        rm.broker.cancel_order(sym, t.tp_order_id)

    # 3) 轮询确认开仓单确已撤销，并检测撤单期间是否又成交
    if t.entry_order_id:
        # 撤单生效有延迟，轮询最多 ~3 秒，确认状态终态
        for _ in range(6):
            o = rm.broker.query_order(sym, t.entry_order_id)
            if not o:
                break  # 查询失败，交给下一轮重试
            status = o.get("status")
            if status in ("CANCELED", "EXPIRED", "FILLED"):
                # 终态：检查撤单期间是否有新增成交（executedQty 越过已保护量）
                cum_qty = float(o.get("executedQty", 0))
                extra = cum_qty - t.filled_qty
                if extra > 1e-12:
                    # 撤单过程中又成交了 extra，立即市价平掉，避免裸仓
                    try:
                        extra = rm.broker.round_qty(sym, extra)
                    except Exception:
                        pass
                    if extra > 0:
                        rm.broker.place_market_order(sym, stop_side, extra,
                                                     position_side=pos_side)
                        print(f"  [结清] {sym} {t.direction} 撤单期间又成交 "
                              f"{extra}，已市价补平")
                break
            # 尚未到终态，稍等再查
            time.sleep(0.5)

    # 4) 强校验该方向持仓归零；不归零则市价补平残余
    try:
        positions = rm.broker.get_open_positions()
        residual = 0.0
        for p in positions:
            if p.get("symbol") == sym and p.get("positionSide", "BOTH") == t.direction:
                residual += float(p.get("positionAmt", 0))
        if abs(residual) > 1e-12:
            qty = abs(residual)
            rm.broker.place_market_order(sym, stop_side, qty,
                                         position_side=pos_side)
            print(f"  [结清] {sym} {t.direction} 仍有残仓 {residual}，已市价补平")
    except Exception as e:
        print(f"  [结清] {sym} 强校验持仓失败: {e}")

    # 5) 确认干净后移除本地记录
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


def _is_rest_time(now=None):
    """判断当前本机时间是否在休息时段内（模块级，供主循环和监控线程共用）。"""
    if not REST_START or not REST_END:
        return False
    now = now if now is not None else time.localtime()
    hm = now.tm_hour * 60 + now.tm_min
    def _to_min(t):
        h, m = t.split(":")
        return int(h) * 60 + int(m)
    s = _to_min(REST_START)
    e = _to_min(REST_END)
    if s <= e:
        return s <= hm < e
    return hm >= s or hm < e


def monitor_loop(rm: RiskManager):
    """
    后台监控线程：每 15 秒检查一次挂单成交状态 + 持仓状态。

    解决「开仓单延迟成交后，主循环每 15 分钟才检查一次，导致成交后
    长时间不挂止盈止损」的问题。这个线程持续高频检查，成交后很快
    就能补挂止盈止损。

    休息时段内：主循环不产生信号、止盈止损单已挂交易所，本线程也停止检查，
    真正「什么都不做」。
    """
    global _running
    while _running:
        if not _is_rest_time():
            try:
                with _rm_lock:
                    manage_pending_orders(rm)
                    sync_positions_from_exchange(rm)
            except Exception as e:
                print(f"  [监控线程] 异常: {e}")
        # 分段 sleep（休息时段内也保持可响应退出），便于响应退出
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
    3m 回调监控线程：周期性检查「观察中」的 15m 信号，判定是否回调触碰 3m EMA。

    触发后调用 open_trade 以 3m EMA 值挂限价单入场；
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
                        f"15m拐头后 {PULLBACK_WATCH_SECONDS // 60} 分钟内未回调到 3m EMA，放弃。"
                    )
                    continue

                # 触发：用 3m EMA 作为入场价，开仓
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
                            f"3m 已触碰 EMA，入场价 {entry:.6g}。"
                        )
                        meta = {
                            "bar_time": ps.sig_close_ms - BAR_INTERVAL_MS,
                            "open": ps.sig_open,
                            "close": ps.sig_close,
                            "prev_open": ps.prev_open,
                            "entry_source": "3m_ema",
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

    # 设置杠杆：实际杠杆 = min(全局配置 LEVERAGE, 该币 maxLeverage)。
    # 避免对某些不支持高杠杆的币设过高导致失败，同时不超全局上限。
    try:
        target_lev = min(LEVERAGE, rm.broker.get_max_leverage(sym))
        lev_r = rm.broker.set_leverage(sym, target_lev)
        if lev_r:
            actual = lev_r.get("leverage")
            if actual is not None and int(actual) != target_lev:
                print(f"  [杠杆] {sym} 实际杠杆 {actual}x（请求 {target_lev}x）")
        else:
            print(f"  [杠杆] {sym} 设置 {target_lev}x 未成功，将使用账户现有杠杆")
    except Exception as e:
        print(f"  [杠杆] {sym} 设置杠杆异常: {e}")

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
    # 记录信号K线时间（观察期计时起点）。
    # 注：bar_time 去重已在 _process_signals 里「开仓尝试前」统一标记，
    # 这里不再重复，避免下单失败路径遗漏标记导致旧信号重试。
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

    # 关键：把 config.yaml 的 proxy 注入 market 模块，使拉 K 线也走代理，
    # 否则 market 直连币安在国内网络下会频繁超时（下单走 broker 代理，
    # 拉数据却走环境变量，二者不一致会导致「下单正常但拉 K 线超时」）。
    market.set_proxy(proxy)

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
    sema = SlidingEMA(period=EMA_PERIOD, window_size=WINDOW_SIZE,
                      bar_interval_ms=BAR_INTERVAL_MS)
    rev_up = EMA20Reversal(min_down_bars=MIN_DOWN_BARS)
    rev_down = EMA20ReversalDown(min_up_bars=MIN_UP_BARS)

    # 3m 回调入场管理器（观察中状态 + 3m EMA 触碰判定）
    pullback_mgr = PullbackManager(
        ema_period=PULLBACK_EMA_PERIOD,
        window_size=PULLBACK_WINDOW,
        warmup_limit=PULLBACK_WARMUP,
        watch_seconds=PULLBACK_WATCH_SECONDS,
        risk_reward=RISK_REWARD,
    ) if ENABLE_PULLBACK else None

    # EMA10/20 共振确认管理器（resonance 模式）
    resonance_mgr = ResonanceManager(ema10_period=10, window_size=WINDOW_SIZE,
                                     bar_interval_ms=BAR_INTERVAL_MS)

    def _do_warmup(symbols):
        """全量预热：拉历史 K 线，重建所有状态机 + 缓存（启动和休息恢复时复用）。"""
        print("预热滑动窗口（并发拉历史 K 线 + 还原状态机）...")
        t0 = time.time()
        warm = parallel_fetch(symbols, WARMUP_LIMIT, workers=MAX_WORKERS)
        for sym, bars in warm.items():
            closes = [b["c"] for b in bars]
            bar_times = [b["t"] for b in bars]
            sema.init_symbol(sym, closes, bar_times)
            rev_up.warmup(sym, sema._closes[sym], period=EMA_PERIOD)
            rev_down.warmup(sym, sema._closes[sym], period=EMA_PERIOD)
            # resonance 模式：初始化 EMA10 窗口 + 历史基线
            resonance_mgr.init_symbol(sym, closes, bar_times)
            # 缓存完整 K 线（含 o/h/l/c/v/t），后续增量判断复用
            cache_add_bars(sym, bars)
        print(f"预热完成: {len(warm)}/{len(symbols)} 就绪, 耗时 {time.time()-t0:.1f}s\n")
        return len(warm)

    def _reset_all_state(symbols):
        """清空所有状态机 + 缓存（休息恢复时，等同程序重启）。"""
        for s in symbols:
            rev_up.reset(s)
            rev_down.reset(s)
            sema.reset(s)
            resonance_mgr.reset(s)
            cache_clear_symbol(s)
            with _snapshot_seen_lock:
                _snapshot_seen.pop(s, None)
            _clear_bar_opened(s)
            if pullback_mgr is not None:
                pullback_mgr.reset(s)

    def _sleep_until_rest_end():
        """一次性 sleep 到 rest_end（本机时间），分段 sleep 便于响应退出信号。

        用绝对时间点计算目标时刻，正确处理边界：当 now == rest_end 时立即退出，
        跨午夜场景也能正确收敛（rest_end 是「今天的 HH:MM」）。
        """
        h, m = REST_END.split(":")
        while _running:
            now = time.localtime()
            # 构造「今天 rest_end」的绝对时间；若已过（now 的时分 > rest_end），
            # 说明 rest_end 已在今天过去，理论上此刻不应再睡（由 _is_rest_time 保证）。
            target = time.mktime((now.tm_year, now.tm_mon, now.tm_mday,
                                  int(h), int(m), 0, now.tm_wday, now.tm_yday, now.tm_isdst))
            now_ts = time.time()
            # 若目标时刻已到或已过（含恰好相等），立即退出
            if target <= now_ts:
                break
            # 分段 sleep（最多 60 秒一段），便于响应 Ctrl+C / _running=False
            sleep_s = min(60, target - now_ts)
            time.sleep(sleep_s)

    _do_warmup(candidates)

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

    # 3m 回调监控线程（仅 pullback 模式启动）
    if STRATEGY_MODE == "pullback" and ENABLE_PULLBACK and pullback_mgr is not None:
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

    def _process_signals(signals, rm, _rm_lock):
        """处理一批信号开仓。返回处理数量（供日志）。"""
        n = 0
        if not signals:
            return n
        print(f"[{_ts()}] 处理 {len(signals)} 个信号开仓...")
        with _rm_lock:
            for sym, direction, o, c, meta in signals:
                if rm.has_position_or_order(sym, direction):
                    print(f"  [跳过] {sym} {direction} 已在持仓/挂单中")
                    continue
                # 去重：同一根 bar_time 的旧信号不重复开仓（防止快照开仓→止盈→
                # 完整推进又用同一根 K 线旧信号再开一次）。
                bar_time = meta.get("bar_time") if meta else None
                if bar_time is not None:
                    with _opened_bars_lock:
                        opened = _opened_bars.get((sym, direction), set())
                    if bar_time in opened:
                        print(f"  [跳过] {sym} {direction} 该 K 线已开过仓（bar_time={bar_time}），"
                              f"忽略旧信号")
                        continue
                if not rm.can_open_new():
                    print(f"  [跳过] {sym} 已达最大持仓 {MAX_POSITIONS}")
                    continue
                try:
                    if STRATEGY_MODE == "resonance":
                        resonance_ok = meta.get("resonance_ok", False) if meta else False
                        if not resonance_ok:
                            print(f"  [跳过] {sym} {direction} 共振不成立（EMA10未同向拐头），放弃信号")
                            continue
                        # 失败即作废：开仓尝试前就标记 bar_time，无论下单成功还是
                        # 失败（如余额不足），同一根 K 线信号都不再重试。
                        if bar_time is not None:
                            _mark_bar_opened(sym, direction, bar_time)
                        open_trade(rm, sym, direction, o, c, meta)
                    elif ENABLE_PULLBACK and pullback_mgr is not None:
                        prev_open = meta.get("prev_open") if meta else None
                        sig_low = meta.get("low") if meta else None
                        sig_high = meta.get("high") if meta else None
                        sig_close_ms = (meta.get("bar_time") if meta else 0) + BAR_INTERVAL_MS
                        ok = pullback_mgr.add_signal(
                            sym, direction, o, c, sig_low, sig_high,
                            prev_open, sig_close_ms)
                        if ok:
                            print(f"  [观察] {sym} {direction} 15m拐头，等待3m回调到EMA")
                            _notify(
                                f"**👀 等待回踩入场 · {sym}**\n"
                                f"\n"
                                f"方向: **{'做多 LONG' if direction == 'LONG' else '做空 SHORT'}**\n"
                                f"15m已拐头，等待价格回调到 3m EMA 再挂单。"
                            )
                        else:
                            print(f"  [跳过] {sym} {direction} 已在观察中，忽略重复")
                    else:
                        open_trade(rm, sym, direction, o, c, meta)
                except Exception as e:
                    print(f"  [开仓异常] {sym}: {e}")
                n += 1
        return n

    def _judge_symbol(sym, bars, now_ms):
        """判断单个 symbol 的 K 线，返回该币本轮产生的信号列表。

        本函数处理「单次拉取」返回的 1~2 根，按 closeTime 区分，职责分离：
          - closeTime > now_ms  → 进行中快照：算领先 EMA 做只读预判拐头，
            **这是唯一的下单判断入口**。快照不写缓存、不推状态机。
          - closeTime <= now_ms → 已收盘完整根：只推进状态机（写缓存 + 推
            EMA20/EMA10 + 拐头计数），**不再做开仓判断**，避免同一根 K 线
            判开仓两次。

        返回 signals 仅来自「快照预判」分支。
        """
        signals = []
        for idx, b in enumerate(bars):
            bar_close_ms = b["t"] + BAR_INTERVAL_MS
            o = b["o"]
            c = b["c"]
            # 信号K线前一根的开盘价（stop 比较用）——从缓存回读（当前根尚未入缓存，
            # cache_list[-1] 即「最新已收盘完整根」，也就是当前信号根的前一根）。
            with _bars_cache_lock:
                cache_list = _bars_cache.get(sym, [])
            prev_open = cache_list[-1]["o"] if cache_list else None

            if bar_close_ms > now_ms:
                # ── 进行中快照：只读预判，不写状态机、不写缓存 ──
                # 去重：同一根进行中快照在 lead 窗口内反复拉取，只预判一次。
                with _snapshot_seen_lock:
                    seen_t = _snapshot_seen.get(sym, 0)
                if b["t"] <= seen_t:
                    continue
                with _snapshot_seen_lock:
                    _snapshot_seen[sym] = b["t"]

                ema_snap = sema.get_ema(sym)
                if ema_snap is None:
                    continue
                # 用快照收盘价临时算领先 EMA（不写回 sema）
                lead_ema = c * sema.alpha + ema_snap * (1 - sema.alpha)

                up_before = rev_up.get_down_count(sym)
                down_before = rev_down.get_up_count(sym)

                # 做多预判
                if rev_up.would_reverse(sym, lead_ema):
                    body_pct = (c - o) / o * 100 if o > 0 else 0.0
                    if MAX_BODY_PCT >= body_pct >= MIN_BODY_PCT:
                        # 共振判定：快照时刻 resonance_mgr._history 里 bar[-2]/bar[-1]
                        # 的 EMA10 已就绪（它们是完整收盘价推进的），可正常判定。
                        resonance_ok = resonance_mgr.confirm(sym, "LONG")
                        meta = {
                            "bar_time": b["t"],
                            "open": o, "high": b["h"], "low": b["l"],
                            "close": c, "volume": b["v"],
                            "ema": lead_ema,
                            "down_count": up_before,
                            "body_pct": body_pct,
                            "ema_history": list(rev_up.get_ema_history(sym)),
                            "prev_open": prev_open,
                            "resonance_ok": resonance_ok,
                        }
                        signals.append((sym, "LONG", o, c, meta))
                        tlog.log_signal(sym, "LONG", meta)
                        print(f"\n  >>> [预判·做多] {sym} 收盘前快照拐头向上"
                              f"(领先EMA {lead_ema:.6f}, 快照价 {c}, "
                              f"共振{'成立' if resonance_ok else '不成立'}) <<<\n")

                # 做空预判
                if rev_down.would_reverse(sym, lead_ema):
                    body_pct = (c - o) / o * 100 if o > 0 else 0.0
                    if -MAX_BODY_PCT <= body_pct <= -MIN_BODY_PCT:
                        resonance_ok = resonance_mgr.confirm(sym, "SHORT")
                        meta = {
                            "bar_time": b["t"],
                            "open": o, "high": b["h"], "low": b["l"],
                            "close": c, "volume": b["v"],
                            "ema": lead_ema,
                            "up_count": down_before,
                            "body_pct": body_pct,
                            "ema_history": list(rev_down.get_ema_history(sym)),
                            "prev_open": prev_open,
                            "resonance_ok": resonance_ok,
                        }
                        signals.append((sym, "SHORT", o, c, meta))
                        tlog.log_signal(sym, "SHORT", meta)
                        print(f"\n  >>> [预判·做空] {sym} 收盘前快照拐头向下"
                              f"(领先EMA {lead_ema:.6f}, 快照价 {c}, "
                              f"共振{'成立' if resonance_ok else '不成立'}) <<<\n")
                continue

            # ── 已收盘完整根：只推进状态机，不做开仓判断 ──
            # 开仓判断已由「快照预判」在收盘前 10 秒完成；此处只负责把准确
            # 收盘价写入缓存、推进 EMA20/EMA10 与拐头状态机，供下一根快照
            # 预判使用。不再产生信号，避免「同一根 K 线判开仓两次」。
            cache_add_bars(sym, [b])
            ema = sema.push_close(sym, b["t"], b["c"])
            if ema is None:
                continue
            # 推进拐头方向状态机（只更新 down_count/up_count/prev_ema，不触发信号）
            rev_up.on_15m_close(sym, ema)
            rev_down.on_15m_close(sym, ema)
            # 推进 EMA10 历史
            resonance_mgr.push_close(sym, b["t"], b["c"])

        return signals

    while _running:
        iteration += 1
        loop_start = time.time()

        print(f"[{_ts()}] 第{iteration}轮：生产者拉取 {len(candidates)} 币 → 并发判断 → 串行下单")

        # ── 三级流水线：生产者拉取 → 判断线程池并发判断 → 下单线程串行下单 ──
        # 1) fetch_queue：生产者(_produce_bars) 并发拉取，随完成随放入
        # 2) judge 线程池：并发取 (sym,bars) 调 _judge_symbol，信号塞入 signal_queue
        # 3) order 线程：串行从 signal_queue 取信号，_process_signals 拿 _rm_lock 下单
        # 关键：判断纯 CPU 可并发（状态机按 symbol 隔离），下单受币安限流必须串行。
        fetch_queue = queue.Queue(maxsize=max(8, MAX_WORKERS * 2))
        signal_queue = queue.Queue()  # 信号量少，不设上限

        producer = threading.Thread(
            target=_produce_bars,
            args=(list(candidates), 2, MAX_WORKERS, fetch_queue),
            daemon=True,
        )
        producer.start()

        # 判断线程池 worker：消费 fetch_queue，产出信号
        judge_pool = ThreadPoolExecutor(max_workers=JUDGE_WORKERS)

        def _judge_worker():
            """单个判断 worker：持续从 fetch_queue 取币判断，直到哨兵退出。"""
            while True:
                try:
                    item = fetch_queue.get(timeout=3.0)
                except queue.Empty:
                    # 生产者可能还在拉，稍等继续；生产者死了且队列空则退出
                    if not producer.is_alive() and fetch_queue.empty():
                        break
                    continue
                if item is _fetch_sentinel:
                    break  # 哨兵：本轮数据已取尽，退出 worker
                sym, bars = item
                now_ms = int(time.time() * 1000)
                # 直接传本轮拉取的增量 bars（含可能的进行中快照）给 _judge_symbol，
                # 快照/完整、进缓存/预判，全部在 _judge_symbol 内部区分处理。
                sigs = _judge_symbol(sym, bars, now_ms)
                for sig in sigs:
                    signal_queue.put(sig)

        # 启动判断线程池
        judge_futures = [judge_pool.submit(_judge_worker) for _ in range(JUDGE_WORKERS)]
        judge_pool.shutdown(wait=False)

        # 下单线程：串行消费 signal_queue，收到「结束哨兵」即排空退出
        _order_sentinel = object()

        def _order_worker():
            """串行下单：从 signal_queue 取信号开仓，直到收到结束哨兵。"""
            while True:
                sig = signal_queue.get()  # 阻塞等信号；判断线程结束时会放哨兵唤醒
                if sig is _order_sentinel:
                    break
                _process_signals([sig], rm, _rm_lock)

        order_thread = threading.Thread(target=_order_worker, daemon=True)
        order_thread.start()

        # 等待：生产者完成 → 投 fetch 哨兵让 judge 收尾 → 全部判断完 → 投 order 哨兵
        producer.join()
        for _ in range(JUDGE_WORKERS):
            fetch_queue.put(_fetch_sentinel)
        for f in judge_futures:
            try:
                f.result(timeout=15)
            except Exception:
                pass
        judge_pool.shutdown(wait=True)
        # 判断全部结束后，放 order 哨兵唤醒下单线程退出（即便无信号也立刻退出，不空等）
        signal_queue.put(_order_sentinel)
        order_thread.join(timeout=15)

        fetched = len(candidates)  # 用于日志（本轮拉取币数量）

        # 轮末日志
        print(f"[{_ts()}] 本轮流式处理完成, 耗时 {time.time()-loop_start:.1f}s")

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
                        resonance_mgr.init_symbol(sym, closes, bar_times)
                        cache_add_bars(sym, bars)
                for s in [x for x in candidates if x not in new_candidates]:
                    rev_up.reset(s)
                    rev_down.reset(s)
                    sema.reset(s)
                    resonance_mgr.reset(s)
                    cache_clear_symbol(s)
                    with _snapshot_seen_lock:
                        _snapshot_seen.pop(s, None)
                    _clear_bar_opened(s)
                    if pullback_mgr is not None:
                        pullback_mgr.reset(s)
                candidates = new_candidates
                print(f"候选币数量: {len(candidates)} 个（新增 {len(added)}）\n")
            except Exception as e:
                print(f"[刷新] 异常: {e}")
            last_rescan = time.time()

        # ── 休息时段：一次性 sleep 到 rest_end，期间不拉取/不判断/不开仓 ──
        if _is_rest_time():
            print(f"[{_ts()}] 进入休息时段（{REST_START}–{REST_END}），停止一切拉取/判断/开仓，"
                  f"sleep 到 {REST_END}...\n")
            _sleep_until_rest_end()
            # 醒来：等同程序重启，清空状态机 + 全量预热
            print(f"[{_ts()}] 休息结束，全量预热恢复...\n")
            _reset_all_state(list(candidates))
            _do_warmup(list(candidates))
            continue

        # 对齐下一根信号 K 线收盘前 lead 秒
        now = time.time()
        bar_sec = BAR_INTERVAL_MS / 1000
        next_close = (now // bar_sec + 1) * bar_sec
        trigger_at = next_close - LEAD_SECONDS
        if now < trigger_at:
            # 还没到收盘前 lead 秒：正常睡到 trigger_at
            wait_s = trigger_at - now
        else:
            # 已越过 trigger_at（落在 lead 窗口内）：不再空转，睡到真正收盘点，
            # 让「进行中快照」完整收盘后再拉，避免同一根快照被高频重复判断。
            wait_s = next_close - now
        if wait_s > 0:
            print(f"[{_ts()}] 等待下一根 {SIGNAL_INTERVAL} K线收盘前 {LEAD_SECONDS} 秒触发 "
                  f"({time.strftime('%H:%M:%S', time.localtime(trigger_at))})...\n")
            while wait_s > 0 and _running:
                sleep = min(10, wait_s)
                time.sleep(sleep)
                wait_s -= sleep

    print("已退出")


if __name__ == "__main__":
    main()
