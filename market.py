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


def _get_proxies() -> dict:
    proxy = os.getenv("HTTP_PROXY", "") or os.getenv("HTTPS_PROXY", "")
    return {"http": proxy, "https": proxy} if proxy else None


def _fetch_json(url: str, params: dict = None, timeout: int = 12):
    """REST 请求，带重试。失败返回空容器（list 接口返回 []，dict 接口返回 {}）。"""
    expects_list = "klines" in url or "exchangeInfo" in url
    proxies = _get_proxies()

    for attempt in range(5):
        try:
            resp = requests.get(url, params=params, timeout=timeout, proxies=proxies)
            if resp.status_code == 200:
                return resp.json()
            print(f"  [REST] {url} HTTP {resp.status_code}, retry {attempt+1}/5")
        except Exception as e:
            print(f"  [REST] {url} error: {e}, retry {attempt+1}/5")
        time.sleep(2 ** attempt)

    print(f"  [REST] {url} all 5 retries exhausted")
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


def get_klines(symbol: str, interval: str, limit: int = 500) -> list:
    """
    获取指定周期的 K线，返回 [{t,o,h,l,c,v}, ...]（旧 → 新）。
    interval: "15m" / "3m" / "5m" 等币安支持的周期。
    """
    url = f"{FAPI_BASE}/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
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


def get_30m_klines(symbol: str, limit: int = 500) -> list:
    """获取 30m K线。"""
    return get_klines(symbol, "30m", limit)
