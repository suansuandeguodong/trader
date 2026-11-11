# -*- coding: utf-8 -*-
"""
交易历史落盘模块（运行时实时记录，JSONL 追加写入）。

每条记录是一个 JSON 对象（一行），含统一公共字段：
  - event:     事件类型 OPEN / SIGNAL / FILL / STOP_TP / CLOSE / CANCEL / REJECT / POSITION / ERROR
  - ts:        事件时间戳（秒，含毫秒）
  - ts_iso:    事件时间（本地时区 ISO 格式，便于复盘）
  - symbol:    交易对
  - direction: LONG / SHORT
  - trade_id:  本地交易标识（symbol:direction:挂单时间戳ms），用于关联同一笔的多个事件
  - ...        各事件特有字段

设计目标：
  - 记录粒度尽可能细，覆盖「信号 → 开仓 → 成交 → 挂止盈止损 → 平仓 → 撤单」全生命周期
  - 程序崩溃也不丢已写内容（每行立即 flush + fsync）
  - 不依赖任何第三方库，纯标准库
  - 复盘时可按 trade_id 聚合出每笔完整链路，或用 event 过滤某类事件
"""
import json
import os
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any

# 日志文件路径（项目根目录下）
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "trade_history.jsonl")

# 日志文件大小上限（字节）。超过则归档，避免长期运行磁盘无限增长。
LOG_MAX_BYTES = 50 * 1024 * 1024  # 50 MB


def _now_iso() -> str:
    """本地时区 ISO 时间字符串（含时区）。"""
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def _utc_iso(ms: Optional[int]) -> Optional[str]:
    """毫秒时间戳 → UTC ISO 字符串。"""
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat()
    except Exception:
        return None


def _rotate_if_needed():
    """日志文件超过阈值时归档，避免长期运行磁盘无限增长。"""
    try:
        if not os.path.exists(LOG_PATH):
            return
        if os.path.getsize(LOG_PATH) >= LOG_MAX_BYTES:
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            archive = LOG_PATH.replace(".jsonl", f"-{ts}.jsonl")
            # 若同一秒内多次触发，追加序号避免覆盖
            i = 1
            while os.path.exists(archive):
                archive = LOG_PATH.replace(".jsonl", f"-{ts}-{i}.jsonl")
                i += 1
            os.rename(LOG_PATH, archive)
            print(f"  [日志] 已归档为 {os.path.basename(archive)}")
    except Exception as e:
        print(f"  [日志] 轮转失败: {e}")


def _append(record: Dict[str, Any]):
    """追加一条记录到 JSONL 文件（立即 flush + fsync）。"""
    line = json.dumps(record, ensure_ascii=False, default=str) + "\n"
    try:
        _rotate_if_needed()
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line)
            f.flush()
            os.fsync(f.fileno())
    except Exception as e:
        # 日志失败不应影响交易主流程
        print(f"  [日志] 写入失败: {e}")


def make_trade_id(symbol: str, direction: str, open_time: float) -> str:
    """
    生成本地 trade_id：symbol + direction + 挂单时间戳(ms取整)。
    同一笔交易（含开仓、成交、挂止损止盈、平仓）共享同一个 trade_id。
    """
    return f"{symbol}:{direction}:{int(open_time * 1000)}"


def log_event(event: str, symbol: str, direction: str, trade_id: str,
              extra: Optional[Dict[str, Any]] = None):
    """记录一个交易事件（底层）。"""
    rec = {
        "event": event,
        "ts": round(time.time(), 3),
        "ts_iso": _now_iso(),
        "symbol": symbol,
        "direction": direction,
        "trade_id": trade_id,
    }
    if extra:
        rec.update(extra)
    _append(rec)


# ── 便捷封装（字段尽可能详细） ──

def log_signal(symbol, direction, meta: Optional[Dict[str, Any]] = None):
    """
    记录一个「信号」事件（在开仓决策前，即使最终被过滤也会记录）。
    meta 应含：bar_time, open/high/low/close/volume, ema, down_count/up_count,
    body_pct, ema_history 等信号上下文。
    """
    tid = ""  # 信号阶段尚无 trade_id
    rec = {}
    if meta:
        rec = dict(meta)
        if "bar_time" in rec:
            rec["bar_time_iso"] = _utc_iso(rec.get("bar_time"))
    log_event("SIGNAL", symbol, direction, tid, rec)


def log_open(symbol, direction, trade_id, entry, stop, tp, qty,
             order_id=None, signal_meta: Optional[Dict[str, Any]] = None):
    """
    记录「开仓下单」事件。
    signal_meta 为信号元数据（bar时间/EMA/down_count/body_pct 等），会一并落盘。
    """
    rec = {
        "entry_price": entry,
        "stop_price": stop,
        "tp_price": tp,
        "qty": qty,
        "notional": round(qty * entry, 6) if entry else None,
        "order_id": order_id,
    }
    if signal_meta:
        sm = dict(signal_meta)
        if "bar_time" in sm:
            sm["bar_time_iso"] = _utc_iso(sm.get("bar_time"))
        rec["signal"] = sm
    log_event("OPEN", symbol, direction, trade_id, rec)


def log_fill(symbol, direction, trade_id, qty, avg_price, cum_qty,
             total_qty, order_id=None):
    """
    记录一次「成交」事件（含部分成交增量）。
    qty 为本次新增成交量，cum_qty 为累计成交量，total_qty 为挂单总量。
    """
    log_event("FILL", symbol, direction, trade_id, {
        "fill_qty": qty,
        "avg_price": avg_price,
        "cum_qty": cum_qty,
        "total_qty": total_qty,
        "order_id": order_id,
    })


def log_stop_tp(symbol, direction, trade_id, stop_price, tp_price, qty,
                stop_order_id=None, tp_order_id=None):
    """记录一次「挂止损止盈」事件（含两个单的 ID、价格、数量）。"""
    log_event("STOP_TP", symbol, direction, trade_id, {
        "stop_price": stop_price,
        "tp_price": tp_price,
        "qty": qty,
        "stop_order_id": stop_order_id,
        "tp_order_id": tp_order_id,
    })


def log_close(symbol, direction, trade_id, close_reason, price=None,
              pnl=None, close_time_ms=None):
    """记录一次「平仓」事件。尽量附带价格与已实现盈亏。"""
    log_event("CLOSE", symbol, direction, trade_id, {
        "reason": close_reason,
        "price": price,
        "pnl": pnl,
        "close_time_iso": _utc_iso(close_time_ms),
    })


def log_cancel(symbol, direction, trade_id, reason):
    """记录一次「撤单/超时」事件。"""
    log_event("CANCEL", symbol, direction, trade_id, {"reason": reason})


def log_reject(symbol, direction, trade_id, reason):
    """记录一次「下单被拒」事件。"""
    log_event("REJECT", symbol, direction, trade_id, {"reason": reason})


def log_position(symbol, direction, trade_id, extra: Optional[Dict[str, Any]] = None):
    """记录一次「持仓状态」快照（可选，用于补充账户上下文）。"""
    log_event("POSITION", symbol, direction, trade_id, extra or {})


def log_error(symbol, direction, trade_id, error):
    """记录一次「异常/错误」事件。"""
    log_event("ERROR", symbol, direction, trade_id, {"error": str(error)})
