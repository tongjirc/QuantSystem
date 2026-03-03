import os
import sys
import requests
import json
import time

# Allow running as: python sender/feishu_sender.py (from any cwd)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import (
    TRADER_APP_ID,
    TRADER_APP_SECRET,
    DEFAULT_RECEIVE_ID,
    DEFAULT_RECEIVE_ID_TYPE,
)


def _normalize_receive(receive_id: str, receive_id_type: str) -> tuple[str, str]:
    rid = (receive_id or "").strip()
    rtype = (receive_id_type or "").strip()
    if rid.startswith("ou_"):
        return rid, "open_id"
    if rid.startswith("oc_"):
        return rid, "chat_id"
    return rid, (rtype or "chat_id")

class FeishuAppBot:
    def __init__(self, app_id, app_secret):
        """
        初始化飞书应用机器人
        :param app_id: 应用的App ID
        :param app_secret: 应用的App Secret
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.tenant_access_token = None
        self.token_expire_time = 0
        # 默认不使用环境代理，避免被本机/企业代理错误拦截导致 403
        self.session = requests.Session()
        self.session.trust_env = False
    
    def get_tenant_access_token(self):
        """获取租户访问令牌"""
        # 如果token未过期，直接返回缓存的token
        if self.tenant_access_token and time.time() < self.token_expire_time:
            return self.tenant_access_token
        
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        try:
            headers = {"Content-Type": "application/json"}
            response = self.session.post(url, headers=headers, data=json.dumps(data), timeout=10)
            result = response.json()
            
            if result.get("code") == 0:
                self.tenant_access_token = result["tenant_access_token"]
                # 提前5分钟刷新token
                self.token_expire_time = time.time() + result["expire"] - 300
                print(f"✅ Token获取成功，有效期至: {time.ctime(self.token_expire_time)}")
                return self.tenant_access_token
            else:
                print(f"❌ 获取token失败: {result}")
                return None
                
        except Exception as e:
            print(f"❌ 请求token异常: {e}")
            return None
    
    def send_text_message(self, content, receive_id, receive_id_type="chat_id"):
        """
        发送文本消息
        :param content: 消息内容
        :param receive_id: 接收者ID（用户ID或群ID）
        :param receive_id_type: 接收者类型，user_id/chat_id/open_id
        :return: 发送结果
        """
        token = self.get_tenant_access_token()
        if not token:
            return {"success": False, "error": "无法获取access_token"}
        
        url = "https://open.feishu.cn/open-apis/im/v1/messages"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        params = {
            "receive_id_type": receive_id_type
        }
        
        data = {
            "receive_id": receive_id,
            "msg_type": "text",
            "content": json.dumps({"text": content})
        }
        
        try:
            response = self.session.post(
                url,
                headers=headers,
                params=params,
                data=json.dumps(data),
                timeout=10,
            )
            result = response.json()

            if result.get("code") == 0:
                print(f"✅ 消息发送成功！消息ID: {result.get('data', {}).get('message_id')}")
                return {"success": True, "data": result.get("data")}
            else:
                print(f"❌ 消息发送失败: {result}")
                return {
                    "success": False,
                    "error": result.get("msg"),
                    "detail": result,
                }

        except Exception as e:
            error_msg = f"发送消息异常: {e}"
            print(f"❌ {error_msg}")
            return {"success": False, "error": error_msg}
    
    def send_markdown_message(self, title, content, receive_id, receive_id_type="chat_id"):
        """
        发送Markdown卡片消息
        :param title: 消息标题
        :param content: Markdown内容
        :param receive_id: 接收者ID
        :param receive_id_type: 接收者类型
        """
        token = self.get_tenant_access_token()
        if not token:
            return {"success": False, "error": "无法获取access_token"}
        
        url = "https://open.feishu.cn/open-apis/im/v1/messages"
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        params = {
            "receive_id_type": receive_id_type
        }
        
        # 构建卡片消息
        card_data = {
            "config": {
                "wide_screen_mode": True
            },
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": title
                },
                "template": "blue"  # 蓝色主题，可选：blue, wathet, turquoise, green, yellow, orange, red, violet, purple, indigo, grey
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": content
                },
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {
                                "tag": "plain_text",
                                "content": "查看详情"
                            },
                            "type": "primary",
                            "url": "https://trading.example.com"
                        },
                        {
                            "tag": "button",
                            "text": {
                                "tag": "plain_text",
                                "content": "忽略"
                            },
                            "type": "default"
                        }
                    ]
                }
            ]
        }
        
        data = {
            "receive_id": receive_id,
            "msg_type": "interactive",
            "content": json.dumps(card_data)
        }
        
        try:
            response = self.session.post(url, headers=headers, params=params, data=json.dumps(data), timeout=10)
            result = response.json()
            
            if result.get("code") == 0:
                print(f"✅ 卡片消息发送成功！消息ID: {result.get('data', {}).get('message_id')}")
                return {"success": True, "data": result.get("data")}
            else:
                print(f"❌ 卡片消息发送失败: {result}")
                return {"success": False, "error": result.get("msg")}
                
        except Exception as e:
            error_msg = f"发送卡片消息异常: {e}"
            print(f"❌ {error_msg}")
            return {"success": False, "error": error_msg}
    
    def send_trading_signal(self, signal_type, symbol, price, quantity=None, reason=""):
        """
        发送交易信号消息
        :param signal_type: 信号类型（买入/卖出）
        :param symbol: 交易对
        :param price: 价格
        :param quantity: 数量（可选）
        :param reason: 原因（可选）
        """
        # 这里需要您提供接收者的ID（用户ID或群ID）
        receive_id = DEFAULT_RECEIVE_ID  # 例如：ou_xxxxxxxx (用户ID) 或 oc_xxxxxxxx (群ID)
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        
        if signal_type.lower() in ["buy", "买入"]:
            emoji = "🟢"
            action = "买入"
            color = "green"
        elif signal_type.lower() in ["sell", "卖出"]:
            emoji = "🔴"
            action = "卖出"
            color = "red"
        else:
            emoji = "⚪"
            action = signal_type
            color = "grey"
        
        # 文本消息版本
        text_content = f"{emoji} {action}信号\n\n品种: {symbol}\n价格: {price}"
        if quantity:
            text_content += f"\n数量: {quantity}"
        if reason:
            text_content += f"\n原因: {reason}"
        text_content += f"\n时间: {timestamp}"
        
        # Markdown卡片版本（更美观）
        markdown_content = f"""**{emoji} {action}信号**

📈 **品种**: {symbol}
💰 **价格**: {price}
"""
        if quantity:
            markdown_content += f"📊 **数量**: {quantity}\n"
        if reason:
            markdown_content += f"📝 **原因**: {reason}\n"
        
        markdown_content += f"⏰ **时间**: {timestamp}"""
        
        # 发送卡片消息
        return self.send_markdown_message(
            title=f"{action}信号提醒",
            content=markdown_content,
            receive_id=receive_id,
            receive_id_type=DEFAULT_RECEIVE_ID_TYPE,  # 或 "user_id"
        )


def send_by_feishu(content: str) -> bool:
    """
    对外暴露的简化接口：
    send_by_feishu("买入信号 XXX") 即可把消息发到默认接收者。
    """
    bot = FeishuAppBot(TRADER_APP_ID, TRADER_APP_SECRET)
    receive_id, receive_id_type = _normalize_receive(DEFAULT_RECEIVE_ID, DEFAULT_RECEIVE_ID_TYPE)
    result = bot.send_text_message(
        content=content,
        receive_id=receive_id,
        receive_id_type=receive_id_type,
    )
    return bool(result.get("success"))

