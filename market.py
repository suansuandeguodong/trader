"""
15m K线数据获取 + USDT 永续枚举

独立于主项目的 symbol_filter，避免硬依赖。
复用其 REST 请求方式（带重试 + 代理）。
"""
import os
import time
from typing import List

import requests

FAPI_BASE = "https://fapi.binance.com"

# 模块级显式代理（由 trader.py 启动时调用 set_proxy 注入 config.yaml 的 proxy）。
# 值为 None 时回退到环境变量 HTTP_PROXY/HTTPS_PROXY（向后兼容旧用法）。
_PROXY = None

# 连接池复用：避免每请求都新建 TCP+TLS 握手（这是 Read timed out 高发的主因之一）。
# 单线程内并发有限，池大小给足即可；复用 keep-alive 连接显著降低抖动带来的握手开销。
_SESSION = None


def set_proxy(proxy: str):
    """设置全局代理（如 config.yaml 里的 proxy）。传空串则清除，回退环境变量。"""
    global _PROXY
    _PROXY = proxy or None
    # 代理变了，连接池要重建，否则新代理不生效
    _reset_session()


def _reset_session():
    global _SESSION
    if _SESSION is not None:
        try:
            _SESSION.close()
        except Exception:
            pass
    _SESSION = None


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        # 代理在 session 层固定；连接池复用 keep-alive
        proxies = _get_proxies()
        if proxies:
            s.proxies.update(proxies)
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=32, pool_maxsize=32, max_retries=0)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        _SESSION = s
    return _SESSION


def _get_proxies() -> dict:
    proxy = _PROXY or os.getenv("HTTP_PROXY", "") or os.getenv("HTTPS_PROXY", "")
    return {"http": proxy, "https": proxy} if proxy else None


def _fetch_json(url: str, params: dict = None, timeout: int = 15,
                max_retries: int = 2):
    """REST 请求，带重试。失败返回空容器（list 接口返回 []，dict 接口返回 {}）。

    timeout=15 是刻意的：币安 K 线接口在负载高时会偶发响应慢（8~12s），
    若 timeout 过短，这些稍慢但最终会成功的请求会被误杀成超时，导致预热/拉取缺币。
    走连接池复用。
    """
    expects_list = "klines" in url or "exchangeInfo" in url
    session = _get_session()

    for attempt in range(max_retries):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            print(f"  [REST] {url} HTTP {resp.status_code}, retry {attempt+1}/{max_retries}")
        except Exception as e:
            print(f"  [REST] {url} error: {e}, retry {attempt+1}/{max_retries}")
        time.sleep(2 ** attempt)

    print(f"  [REST] {url} all {max_retries} retries exhausted")
    return [] if expects_list else {}


def get_all_usdt_perpetuals() -> List[str]:
    """获取所有 USDT 本位永续合约（状态 TRADING）"""
    url = f"{FAPI_BASE}/fapi/v1/exchangeInfo"
    data = _fetch_json(url)

    symbols = []
    if isinstance(data, list):
        return symbols
    for s in data.get("symbols", []):
        if (s.get("quoteAsset") == "USDT"
                and s.get("contractType") == "PERPETUAL"
                and s.get("status") == "TRADING"):
            symbols.append(s["symbol"])
    return symbols


def get_24hr_tickers() -> dict:
    """获取所有合约 24h 数据，返回 {symbol: quoteVolume}"""
    url = f"{FAPI_BASE}/fapi/v1/ticker/24hr"
    data = _fetch_json(url)

    volumes = {}
    if not isinstance(data, list):
        return volumes
    for t in data:
        symbol = t.get("symbol", "")
        if symbol.endswith("USDT"):
            volumes[symbol] = float(t.get("quoteVolume", 0))
    return volumes


def get_24hr_prices() -> dict:
    """获取所有合约最新价格，返回 {symbol: lastPrice}"""
    url = f"{FAPI_BASE}/fapi/v1/ticker/24hr"
    data = _fetch_json(url)

    prices = {}
    if not isinstance(data, list):
        return prices
    for t in data:
        symbol = t.get("symbol", "")
        if symbol.endswith("USDT"):
            prices[symbol] = float(t.get("lastPrice", 0))
    return prices


def get_klines(symbol: str, interval: str, limit: int = 500,
               startTime: int = None) -> list:
    """
    获取指定周期的 K线，返回 [{t,o,h,l,c,v}, ...]（旧 → 新）。
    interval: "15m" / "3m" / "5m" 等币安支持的周期。
    startTime: 可选，毫秒时间戳；传入后只返回 >= startTime 的 K线（用于增量拉取）。
    """
    url = f"{FAPI_BASE}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if startTime is not None:
        params["startTime"] = startTime
    raw = _fetch_json(url, params)

    bars = []
    for k in raw:
        bars.append({
            "t": k[0],
            "o": float(k[1]),
            "h": float(k[2]),
            "l": float(k[3]),
            "c": float(k[4]),
            "v": float(k[5]),
        })
    return bars


def get_15m_klines(symbol: str, limit: int = 500) -> list:
    """获取 15m K线（便捷封装，向后兼容）。"""
    return get_klines(symbol, "15m", limit)


def get_3m_klines(symbol: str, limit: int = 500) -> list:
    """获取 3m K线。"""
    return get_klines(symbol, "3m", limit)
