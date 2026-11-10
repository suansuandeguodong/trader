"""
企业微信群机器人消息推送
使用 Webhook 发送消息，支持 text 和 markdown 格式
"""
import requests


class WeComNotifier:
    """企业微信群机器人推送器"""

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self._enabled = bool(webhook_url and "qyapi.weixin.qq.com" in webhook_url)

    @property
    def enabled(self) -> bool:
        return self._enabled

    def send_text(self, content: str) -> bool:
        """发送纯文本消息"""
        if not self._enabled:
            print(f"[企微] 未配置，跳过: {content[:50]}...")
            return False

        payload = {
            "msgtype": "text",
            "text": {
                "content": content
            }
        }
        try:
            resp = requests.post(self.webhook_url, json=payload, timeout=10)
            result = resp.json()
            if result.get("errcode") == 0:
                return True
            else:
                print(f"[企微] 发送失败: {result}")
                return False
        except Exception as e:
            print(f"[企微] 异常: {e}")
            return False

    def send_markdown(self, content: str) -> bool:
        """发送 Markdown 格式消息"""
        if not self._enabled:
            print(f"[企微] 未配置，跳过")
            return False

        payload = {
            "msgtype": "markdown",
            "markdown": {
                "content": content
            }
        }
        try:
            resp = requests.post(self.webhook_url, json=payload, timeout=10)
            result = resp.json()
            if result.get("errcode") == 0:
                return True
            else:
                # markdown 失败时降级为纯文本
                print(f"[企微] markdown 发送失败，降级: {result}")
                return self.send_text(content)
        except Exception as e:
            print(f"[企微] 异常: {e}")
            return False

    def format_ema55_signal(
        self,
        symbol: str,
        direction: str,
        trigger_price: float,
        ema55_value: float,
    ) -> str:
        """格式化 EMA55 状态机信号消息（Markdown）

        direction: "up"（上穿→阴线）/ "down"（下穿→阳线）
        trigger_price: 触发信号的那根 30m K 线收盘价
        ema55_value: 穿越时的 EMA55 参考值
        """
        from datetime import datetime

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        def _fmt(v):
            if v >= 1000:
                return f"{v:,.2f}"
            elif v >= 1:
                return f"{v:,.4f}"
            return f"{v:.6f}"

        if direction == "up":
            scenario = "上穿后出现阴线"
        else:
            scenario = "下穿后出现阳线"

        message = (
            f"**{symbol} EMA55 穿越信号**\n"
            f"\n"
            f"信号类型: {scenario}\n"
            f"触发收盘价: <font color=\"warning\">{_fmt(trigger_price)}</font> USDT\n"
            f"穿越时 EMA55: <font color=\"info\">{_fmt(ema55_value)}</font>\n"
            f"时间: {now}"
        )
        return message
