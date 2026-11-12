"""
币安 USDT 永续合约交易执行层（Broker）

职责：
  - 封装签名请求（下单/平仓/撤单/查询）
  - 价格/数量精度对齐（从 exchangeInfo 读取 filters）
  - 开仓限价单、止盈止损 OCO 单
  - 持仓查询、挂单管理

所有请求走本地代理（fapi.binance.com）。
"""
import os
import time
import hashlib
import hmac
import urllib.parse
from decimal import Decimal, ROUND_DOWN, ROUND_UP, ROUND_HALF_UP
from typing import Optional, Dict, Any, List

import requests

FAPI_BASE = "https://fapi.binance.com"
# 测试网（如需可切换）：https://testnet.binancefuture.com


class BinanceFutures:
    def __init__(self, api_key: str, api_secret: str, proxy: str = None,
                 testnet: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base = "https://testnet.binancefuture.com" if testnet else FAPI_BASE
        self.proxies = {"http": proxy, "https": proxy} if proxy else None
        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": api_key})
        # 缓存 exchangeInfo 的符号精度信息
        self._symbol_info: Dict[str, Dict[str, Any]] = {}
        self._symbol_info_ts = 0.0
        # 本地时间与币安服务器时间的偏移量（毫秒）。启动时校准，避免 timestamp 超前后报 -1021。
        self._time_offset_ms = 0
        self._last_calibrate_ts = 0.0  # 上次校准的本地时刻（秒），用于周期性重校准
        self._calibrate_time()

    def _calibrate_time(self):
        """拉取币安服务器时间，计算本地时钟偏移，用于修正签名 timestamp。"""
        try:
            url = f"{self.base}/fapi/v1/time"
            t0 = int(time.time() * 1000)
            resp = self.session.get(url, proxies=self.proxies, timeout=10)
            if resp.status_code == 200:
                server_ms = resp.json().get("serverTime", t0)
                # 粗略估算网络往返一半作为误差
                t1 = int(time.time() * 1000)
                rtt = (t1 - t0) // 2
                # 偏移 = 服务器时间 - (本地时间中间值)
                local_mid = t0 + rtt
                self._time_offset_ms = server_ms - local_mid
        except Exception:
            self._time_offset_ms = 0

    def _server_timestamp(self) -> int:
        """返回修正后的、接近币安服务器时间的毫秒时间戳。

        长期运行时本地时钟与币安服务器时钟会缓慢漂移，若只在启动时校准一次，
        漂移累积会导致签名 timestamp 超差（-1021）。这里在距上次校准超过
        30 分钟后自动重校准，保证长期运行签名仍然有效。
        """
        if time.time() - self._last_calibrate_ts >= 1800:
            self._calibrate_time()
            self._last_calibrate_ts = time.time()
        return int(time.time() * 1000) + self._time_offset_ms

    # ── 签名请求 ──
    def _signed(self, method: str, path: str, params: dict = None) -> dict:
        """带签名 + 重试的请求。失败返回 {}。每次重试重新生成 timestamp。"""
        if params is None:
            params = {}

        for attempt in range(5):
            # 每次重试都重新生成 timestamp（用修正后的服务器近似时间），
            # 并带 recvWindow 增加容错，避免 -1021 时间戳超时/超前错误。
            params["timestamp"] = self._server_timestamp()
            params.setdefault("recvWindow", 10000)
            qs = urllib.parse.urlencode(params)
            sig = hmac.new(self.api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
            qs += "&signature=" + sig
            url = f"{self.base}{path}?{qs}"

            try:
                if method == "GET":
                    resp = self.session.get(url, proxies=self.proxies, timeout=15)
                elif method == "POST":
                    resp = self.session.post(url, proxies=self.proxies, timeout=15)
                elif method == "DELETE":
                    resp = self.session.delete(url, proxies=self.proxies, timeout=15)
                else:
                    return {}
                if resp.status_code == 200:
                    return resp.json()
                # -2011 Unknown order：订单已不存在（已成交/已撤/已触发），
                # 属确定性结果，重试无意义，直接返回标记让上层识别，避免空等 5 次。
                if resp.status_code == 400 and '"code":-2011' in resp.text:
                    print(f"  [BROKER] {method} {path} 订单已不存在(-2011)，不再重试")
                    return {"__order_gone__": True}
                print(f"  [BROKER] {method} {path} HTTP {resp.status_code}: "
                      f"{resp.text[:200]}, retry {attempt+1}/5")
                # 若仍报时间戳错误，重新校准一次时间
                if "ahead of the server" in resp.text or "Timestamp" in resp.text:
                    self._calibrate_time()
            except Exception as e:
                print(f"  [BROKER] {method} {path} error: {e}, retry {attempt+1}/5")
            time.sleep(2 ** attempt)
        return {}

    # ── 精度信息 ──
    def _load_symbol_info(self, force: bool = False):
        """加载/缓存 exchangeInfo 的符号过滤信息。"""
        if self._symbol_info and not force and time.time() - self._symbol_info_ts < 3600:
            return
        # exchangeInfo 是公共接口，无需签名，直接用 GET
        url = f"{self.base}/fapi/v1/exchangeInfo"
        data = None
        for attempt in range(3):
            try:
                resp = self.session.get(url, proxies=self.proxies, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    break
            except Exception:
                pass
            time.sleep(2 ** attempt)
        if not data:
            # 拉取失败，保留旧缓存
            return
        for s in data.get("symbols", []):
            if s.get("contractType") == "PERPETUAL" and s.get("quoteAsset") == "USDT":
                info = {"tick_size": None, "step_size": None, "min_notional": 5.0}
                for f in s.get("filters", []):
                    ft = f.get("filterType")
                    if ft == "PRICE_FILTER":
                        tick = f.get("tickSize", "0")
                        if tick and tick != "0":
                            info["tick_size"] = Decimal(tick)
                    elif ft == "LOT_SIZE":
                        ss = f.get("stepSize", "0")
                        if ss and ss != "0":
                            info["step_size"] = Decimal(ss)
                    elif ft == "MIN_NOTIONAL":
                        info["min_notional"] = float(f.get("notional", 5.0))
                self._symbol_info[s["symbol"]] = info
        self._symbol_info_ts = time.time()

    def get_symbol_info(self, symbol: str) -> Dict[str, Any]:
        self._load_symbol_info()
        return self._symbol_info.get(symbol, {})

    def round_price(self, symbol: str, price: float, direction: str = "") -> float:
        """
        价格对齐到 tickSize。
        direction:
          ""  -> 四舍五入（默认）
          "UP" -> 向上取整（买单/止损/止盈在不利方向时用）
          "DOWN" -> 向下取整
        用 Decimal 保证精确，避免浮点误差。
        """
        tick = self.get_symbol_info(symbol).get("tick_size")
        if tick is None or tick <= 0:
            return price
        d = Decimal(str(price))
        q = (d / tick).to_integral_value(
            rounding=ROUND_HALF_UP if direction == "" else
                     (ROUND_UP if direction == "UP" else ROUND_DOWN))
        return float(q * tick)

    def round_qty(self, symbol: str, qty: float) -> float:
        """数量对齐到 stepSize（向下取整，保证名义值不超）。"""
        step = self.get_symbol_info(symbol).get("step_size")
        if step is None or step <= 0:
            return qty
        d = Decimal(str(qty))
        q = (d / step).to_integral_value(rounding=ROUND_DOWN)
        return float(q * step)

    def get_min_notional(self, symbol: str) -> float:
        return self.get_symbol_info(symbol).get("min_notional", 5.0)

    # ── 账户/持仓 ──
    def get_account(self) -> dict:
        return self._signed("GET", "/fapi/v2/account")

    def get_balance(self) -> float:
        a = self.get_account()
        try:
            return float(a.get("availableBalance", 0))
        except Exception:
            return 0.0

    def get_open_positions(self) -> List[dict]:
        """返回当前有非零仓位的 symbol 列表。"""
        a = self.get_account()
        out = []
        for p in a.get("positions", []):
            amt = float(p.get("positionAmt", 0))
            if amt != 0:
                out.append(p)
        return out

    def get_position(self, symbol: str) -> Optional[dict]:
        for p in self.get_open_positions():
            if p.get("symbol") == symbol:
                return p
        return None

    def get_open_orders(self, symbol: str = None) -> List[dict]:
        params = {"symbol": symbol} if symbol else {}
        return self._signed("GET", "/fapi/v1/openOrders", params)

    def get_open_order_count(self) -> int:
        # 用 allOpenOrders 需要 5 秒限频，用 openOrders 空参数会报错，这里取账户里的
        # 已在别处计数。此方法仅作兼容占位。
        return 0

    # ── 下单 ──
    @staticmethod
    def _fmt(v: float, prec: int = 12) -> str:
        """把浮点数格式化为固定小数字符串，避免科学计数法。

        prec: 小数位数上限。币安价格最多 8 位小数，数量最多 3 位小数级 stepSize，
        12 位完全覆盖且不会触发科学计数法。
        """
        if v is None:
            return "0"
        val = float(v)
        # 用足够多的小数位格式化，再去掉末尾多余的 0
        s = f"{val:.{prec}f}"
        # 去掉末尾 0 但保留至少 1 位小数（币安需要）
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s

    def set_leverage(self, symbol: str, leverage: int) -> dict:
        return self._signed("POST", "/fapi/v1/leverage",
                            {"symbol": symbol, "leverage": leverage})

    def place_limit_order(self, symbol: str, side: str, qty: float,
                          price: float, position_side: str = "BOTH") -> Optional[dict]:
        """
        挂限价单。传入的 price/qty 应已对齐（调用方负责），这里仅格式化。
        side: BUY/SELL
        position_side: LONG/SHORT/BOTH（双向持仓模式需传 LONG 或 SHORT）
        """
        params = {
            "symbol": symbol,
            "side": side,
            "type": "LIMIT",
            "quantity": self._fmt(qty),
            "price": self._fmt(price),
            "timeInForce": "GTC",
        }
        if position_side != "BOTH":
            params["positionSide"] = position_side
        r = self._signed("POST", "/fapi/v1/order", params)
        if not r:
            return None
        if "orderId" in r:
            return r
        print(f"  [BROKER] 下单被拒 {symbol}: {r}")
        return None

    def place_market_order(self, symbol: str, side: str, qty: float,
                           position_side: str = "BOTH",
                           reduce_only: bool = True) -> Optional[dict]:
        """市价单（主要用于平仓）。传入的 qty 应已对齐。默认 reduceOnly 防止误开反向仓。"""
        params = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": self._fmt(qty),
            "reduceOnly": "true" if reduce_only else "false",
        }
        if position_side != "BOTH":
            params["positionSide"] = position_side
        r = self._signed("POST", "/fapi/v1/order", params)
        if not r:
            return None
        if "orderId" in r:
            return r
        print(f"  [BROKER] 平仓被拒 {symbol}: {r}")
        return None

    def place_stop_market(self, symbol: str, side: str, qty: float,
                          stop_price: float, reduce_only: bool = True,
                          position_side: str = "BOTH") -> Optional[dict]:
        """
        止损市价单（STOP_MARKET），走 Algo 条件单端点。

        注意：该账户开启了「多资产保证金模式」，普通 /fapi/v1/order 接口
        对条件单返回 -4120，必须走 /fapi/v1/algoOrder + algoType=CONDITIONAL。
        触发价字段是 triggerPrice（不是 stopPrice）。
        """
        params = {
            "algoType": "CONDITIONAL",
            "type": "STOP_MARKET",
            "symbol": symbol,
            "side": side,
            "quantity": self._fmt(qty),
            "triggerPrice": self._fmt(stop_price),
        }
        if position_side != "BOTH":
            params["positionSide"] = position_side
        r = self._signed("POST", "/fapi/v1/algoOrder", params)
        if not r:
            return None
        if "algoId" in r:
            r["orderId"] = r["algoId"]  # 统一字段，便于上层处理
            return r
        print(f"  [BROKER] 止损单被拒 {symbol}: {r}")
        return None

    def place_take_profit_market(self, symbol: str, side: str, qty: float,
                                 stop_price: float, reduce_only: bool = True,
                                 position_side: str = "BOTH") -> Optional[dict]:
        """
        止盈市价单（TAKE_PROFIT_MARKET），走 Algo 条件单端点。
        触发价字段是 triggerPrice。
        """
        params = {
            "algoType": "CONDITIONAL",
            "type": "TAKE_PROFIT_MARKET",
            "symbol": symbol,
            "side": side,
            "quantity": self._fmt(qty),
            "triggerPrice": self._fmt(stop_price),
        }
        if position_side != "BOTH":
            params["positionSide"] = position_side
        r = self._signed("POST", "/fapi/v1/algoOrder", params)
        if not r:
            return None
        if "algoId" in r:
            r["orderId"] = r["algoId"]  # 统一字段
            return r
        print(f"  [BROKER] 止盈单被拒 {symbol}: {r}")
        return None

    def cancel_order(self, symbol: str, order_id: int) -> dict:
        """撤销普通订单（开仓限价单）。"""
        return self._signed("DELETE", "/fapi/v1/order",
                            {"symbol": symbol, "orderId": order_id})

    def cancel_algo_order(self, symbol: str, algo_id: int) -> dict:
        """撤销 Algo 条件单（止盈/止损单）。"""
        return self._signed("DELETE", "/fapi/v1/algoOrder", {"algoId": algo_id})

    def cancel_all_open_orders(self, symbol: str) -> dict:
        """撤销某 symbol 的所有普通挂单（不含 Algo 条件单）。"""
        return self._signed("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})

    def query_order(self, symbol: str, order_id: int) -> dict:
        """查询普通订单状态。"""
        return self._signed("GET", "/fapi/v1/order",
                            {"symbol": symbol, "orderId": order_id})

    def query_algo_order(self, symbol: str, algo_id: int) -> dict:
        """查询 Algo 条件单状态。"""
        return self._signed("GET", "/fapi/v1/algoOrder", {"algoId": algo_id})

    def get_user_trades(self, symbol: str, limit: int = 20,
                        start_time: int = None, end_time: int = None) -> List[dict]:
        """
        查询某 symbol 的成交记录（userTrades）。
        返回列表，每项含 time/side/positionSide/price/qty/realizedPnl 等。
        """
        params = {"symbol": symbol, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time
        r = self._signed("GET", "/fapi/v1/userTrades", params)
        return r if isinstance(r, list) else []
