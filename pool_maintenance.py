#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Warp账号池维护脚本
管理已注册的账号，包括token刷新、状态检查、额度检查等
"""

import asyncio
import base64
import json
import logging
import sqlite3
import time
import traceback
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

import httpx
import requests

# ==================== 配置部分 ====================
import config

# 日志配置
logging.basicConfig(
    level=config.LOG_LEVEL,
    format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


# ==================== 数据模型 ====================
@dataclass
class Account:
    """账号数据模型"""
    id: Optional[int] = None
    email: str = ""
    email_password: Optional[str] = None
    local_id: str = ""
    id_token: str = ""
    refresh_token: str = ""
    status: str = "active"
    created_at: Optional[datetime] = None
    last_used: Optional[datetime] = None
    last_refresh_time: Optional[datetime] = None
    use_count: int = 0
    proxy_info: Optional[str] = None
    user_agent: Optional[str] = None


# ==================== 数据库管理 ====================
class DatabaseManager:
    """数据库管理器"""

    def __init__(self, db_path=config.DATABASE_PATH):
        self.db_path = db_path

    def _to_account(self, row: sqlite3.Row) -> Optional[Account]:
        """将数据库行转换为Account对象"""
        if not row:
            return None
        return Account(
            id=row['id'],
            email=row['email'],
            email_password=row['email_password'],
            local_id=row['local_id'],
            id_token=row['id_token'],
            refresh_token=row['refresh_token'],
            status=row['status'],
            created_at=datetime.fromisoformat(row['created_at']) if row['created_at'] else None,
            last_used=datetime.fromisoformat(row['last_used']) if row['last_used'] else None,
            last_refresh_time=datetime.fromisoformat(row['last_refresh_time']) if row['last_refresh_time'] else None,
            use_count=row['use_count'] or 0,
            proxy_info=row['proxy_info'],
            user_agent=row['user_agent']
        )

    def get_all_accounts(self, status: str = None) -> List[Account]:
        """获取所有账号"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        if status:
            cursor.execute('SELECT * FROM accounts WHERE status = ?', (status,))
        else:
            cursor.execute('SELECT * FROM accounts')

        rows = cursor.fetchall()
        accounts = [self._to_account(row) for row in rows if row]
        conn.close()
        return accounts

    def get_account_by_email(self, email: str) -> Optional[Account]:
        """通过邮箱获取单个账号"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM accounts WHERE email = ?', (email,))
        row = cursor.fetchone()
        conn.close()
        return self._to_account(row)

    def update_account_token(self, email: str, id_token: str, refresh_token: str = None):
        """更新账号token"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        if refresh_token:
            cursor.execute('''
                           UPDATE accounts
                           SET id_token = ?, refresh_token = ?, last_refresh_time = ?
                           WHERE email = ?
                           ''', (id_token, refresh_token, datetime.now().isoformat(), email))
        else:
            cursor.execute('''
                           UPDATE accounts
                           SET id_token = ?, last_refresh_time = ?
                           WHERE email = ?
                           ''', (id_token, datetime.now().isoformat(), email))

        conn.commit()
        conn.close()
        logger.debug(f"✅ 更新账号token: {email}")

    def update_account_status(self, email: str, status: str):
        """更新账号状态"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('UPDATE accounts SET status = ? WHERE email = ?', (status, email))

        conn.commit()
        conn.close()
        logger.info(f"📝 更新账号状态: {email} -> {status}")

    def get_statistics(self) -> Dict[str, int]:
        """获取统计信息"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        stats = {}
        cursor.execute('SELECT status, COUNT(*) FROM accounts GROUP BY status')
        for row in cursor.fetchall():
            stats[row[0]] = row[1]
        cursor.execute('SELECT COUNT(*) FROM accounts')
        stats['total'] = cursor.fetchone()[0]
        conn.close()
        return stats

    def cleanup_expired_accounts(self, days: int = 30):
        """清理过期的、长期未使用的账号"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cutoff_date = datetime.now() - timedelta(days=days)
        cursor.execute('''
                       DELETE FROM accounts
                       WHERE status = 'expired' AND last_refresh_time < ?
                       ''', (cutoff_date.isoformat(),))
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        if deleted_count > 0:
            logger.info(f"🗑️ 清理了 {deleted_count} 个长期无效的账号")
        return deleted_count


# ==================== Token刷新服务 ====================
class TokenRefreshService:
    """Token刷新服务"""

    def __init__(self, firebase_api_key: str = config.FIREBASE_API_KEY):
        self.firebase_api_key = firebase_api_key
        self.base_url = "https://securetoken.googleapis.com/v1/token"

    def is_token_expired(self, id_token: str, buffer_minutes: int = 10) -> bool:
        """检查JWT token是否过期"""
        try:
            if not id_token: return True
            parts = id_token.split('.')
            if len(parts) != 3: return True

            payload_part = parts[1]
            payload_part += '=' * (4 - len(payload_part) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_part).decode('utf-8'))

            exp_timestamp = payload.get('exp')
            if not exp_timestamp: return True

            return (exp_timestamp - time.time()) <= (buffer_minutes * 60)
        except Exception as e:
            logger.error(f"检查Token过期状态失败: {e}")
            return True

    def can_refresh_token(self, account: Account) -> bool:
        """检查是否可以刷新token（遵守1小时限制）"""
        if not account.last_refresh_time:
            return True
        time_elapsed = datetime.now() - account.last_refresh_time
        return time_elapsed >= timedelta(hours=config.TOKEN_REFRESH_HOURS)

    def refresh_firebase_token(self, refresh_token: str) -> Tuple[bool, Optional[str], Optional[str]]:
        """刷新Firebase Token"""
        try:
            payload = {"grant_type": "refresh_token", "refresh_token": refresh_token}
            url = f"{self.base_url}?key={self.firebase_api_key}"
            response = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=30)
            if response.ok:
                data = response.json()
                return True, data.get('id_token'), data.get('refresh_token')
            return False, None, f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            return False, None, str(e)

    async def refresh_account_if_needed(self, account: Account, db_manager: DatabaseManager) -> bool:
        """根据需要刷新账号token，返回是否成功"""
        if not self.is_token_expired(account.id_token):
            return True

        if not self.can_refresh_token(account):
            logger.debug(f"⏰ {account.email} 刷新冷却中...")
            return True # 认为它仍然有效，只是暂时不能刷新

        success, new_id_token, new_refresh_token = self.refresh_firebase_token(account.refresh_token)
        if success and new_id_token:
            db_manager.update_account_token(account.email, new_id_token, new_refresh_token)
            logger.info(f"✨ 刷新token成功: {account.email}")
            return True
        else:
            logger.error(f"❌ 刷新token失败: {account.email} - {new_refresh_token}. 标记为过期.")
            db_manager.update_account_status(account.email, 'expired')
            return False


# ==================== 账号池维护器 ====================
class PoolMaintainer:
    """账号池维护器"""

    def __init__(self):
        self.db_manager = DatabaseManager()
        self.token_refresh_service = TokenRefreshService()
        self.running = False

    async def check_pool_health(self):
        """检查账号池健康状态"""
        stats = self.db_manager.get_statistics()
        total = stats.get('total', 0)
        active = stats.get('active', 0)
        expired = stats.get('expired', 0)
        blocked = stats.get('blocked', 0)

        logger.info("=" * 50)
        logger.info("📊 账号池状态")
        logger.info(f"📦 总账号数: {total}")
        logger.info(f"✅ 活跃账号: {active}")
        logger.info(f"❌ 过期账号: {expired}")
        logger.info(f"⛔️ 封禁账号: {blocked}")

        if active < config.MIN_POOL_SIZE:
            logger.warning(f"⚠️ 活跃账号不足 (当前: {active}, 最小: {config.MIN_POOL_SIZE})")
        else:
            logger.info(f"💚 账号池健康")
        logger.info("=" * 50)

    async def _get_request_limit(self, id_token: str, client: httpx.AsyncClient) -> Dict[str, Any]:
        """获取账户请求额度"""
        if not id_token:
            return {"success": False, "error": "缺少ID Token"}
        try:
            url = "https://app.warp.dev/graphql/v2"
            query = """query GetRequestLimitInfo($requestContext: RequestContext!) {\n  user(requestContext: $requestContext) {\n    __typename\n    ... on UserOutput {\n      user {\n        requestLimitInfo {\n          requestLimit\n          requestsUsedSinceLastRefresh\n        }\n      }\n    }\n  }\n}\n"""
            data = {
                "operationName": "GetRequestLimitInfo",
                "variables": {"requestContext": {"clientContext": {}, "osContext": {}}},
                "query": query
            }
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {id_token}",
                "User-Agent": "Mozilla/5.0"
            }
            response = await client.post(url, params={"op": "GetRequestLimitInfo"}, json=data, headers=headers)
            if response.status_code == 200:
                result = response.json()
                if "errors" in result:
                    return {"success": False, "error": result["errors"][0].get("message", "GraphQL error")}
                
                limit_info = result.get("data", {}).get("user", {}).get("user", {}).get("requestLimitInfo")
                if limit_info:
                    return {
                        "success": True,
                        "requestLimit": limit_info.get("requestLimit", 0),
                        "requestsUsed": limit_info.get("requestsUsedSinceLastRefresh", 0),
                        "requestsRemaining": limit_info.get("requestLimit", 0) - limit_info.get("requestsUsedSinceLastRefresh", 0)
                    }
            return {"success": False, "error": f"HTTP {response.status_code}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def verify_accounts(self):
        """
        验证账号可用性并检查额度。
        这是一个核心维护任务，它会：
        1. 刷新即将过期的Token。
        2. 查询账号的剩余请求额度。
        3. 如果额度低于阈值，则将账号标记为'expired'。
        """
        logger.info("🔍 开始验证账号Token和请求额度...")
        accounts = self.db_manager.get_all_accounts(status='active')
        if not accounts:
            logger.info("没有活跃账号需要验证。")
            return

        healthy_count, low_quota_count, failed_count = 0, 0, 0
        
        async with httpx.AsyncClient(verify=False, timeout=45.0) as client:
            for account in accounts:
                try:
                    # 步骤 1: 确保Token有效或尝试刷新
                    is_token_valid = await self.token_refresh_service.refresh_account_if_needed(account, self.db_manager)
                    if not is_token_valid:
                        failed_count += 1
                        continue # 刷新失败，账号已被标记为expired

                    # 获取最新的token（可能已被刷新）
                    current_account = self.db_manager.get_account_by_email(account.email)
                    if not current_account or not current_account.id_token:
                        logger.warning(f"无法获取 {account.email} 的最新信息，跳过额度检查。")
                        failed_count += 1
                        continue
                    
                    # 步骤 2: 检查额度
                    quota_info = await self._get_request_limit(current_account.id_token, client)
                    
                    if quota_info.get("success"):
                        remaining = quota_info.get("requestsRemaining", 0)
                        if remaining < config.MIN_QUOTA_THRESHOLD:
                            logger.warning(f"📉 额度不足: {account.email} (剩余: {remaining})。标记为过期。")
                            self.db_manager.update_account_status(account.email, 'expired')
                            low_quota_count += 1
                        else:
                            logger.info(f"👍 账号健康: {account.email} (剩余: {remaining})")
                            healthy_count += 1
                    else:
                        error_msg = quota_info.get("error", "未知错误")
                        logger.error(f"❌ 无法获取 {account.email} 的额度: {error_msg}。标记为过期。")
                        self.db_manager.update_account_status(account.email, 'expired')
                        failed_count += 1
                
                except Exception as e:
                    logger.error(f"验证账号 {account.email} 期间发生未知异常: {e}")
                    self.db_manager.update_account_status(account.email, 'expired')
                    failed_count += 1
                
                await asyncio.sleep(1) # 添加短暂延迟，避免请求过于频繁

        logger.info(f"🔍 验证完成 - 健康: {healthy_count}, 额度不足: {low_quota_count}, 失败/无效: {failed_count}")


    async def cleanup(self):
        """清理任务"""
        logger.info("🗑️ 执行清理任务...")
        deleted = self.db_manager.cleanup_expired_accounts(days=7) # 清理一周前就已过期的账号
        logger.info(f"🗑️ 清理完成，删除 {deleted} 个旧的过期账号")

    async def maintenance_loop(self):
        """维护循环"""
        logger.info("🔧 账号池维护服务启动")
        cycle = 0
        while self.running:
            cycle += 1
            logger.info(f"\n🔄 第 {cycle} 个维护周期开始")
            try:
                await self.check_pool_health()
                await self.verify_accounts() # 这个函数现在包含了token刷新和额度检查
                if cycle % 10 == 0: # 每10个周期（约10分钟）执行一次清理
                    await self.cleanup()
                logger.info(f"✅ 第 {cycle} 个维护周期完成")
            except Exception as e:
                logger.error(f"❌ 维护周期异常: {e}\n{traceback.format_exc()}")

            logger.info(f"⏰ 等待 {config.MAINTENANCE_CHECK_INTERVAL} 秒后进行下一次检查...")
            await asyncio.sleep(config.MAINTENANCE_CHECK_INTERVAL)

    async def start(self):
        """启动维护服务"""
        self.running = True
        try:
            await self.maintenance_loop()
        except KeyboardInterrupt:
            logger.info("⌨️ 收到停止信号")
        finally:
            self.running = False
            logger.info("🛑 维护服务已停止")


# ==================== 命令行接口 ====================
async def interactive_mode():
    """交互模式"""
    maintainer = PoolMaintainer()
    print("\n" + "=" * 60)
    print("🎮 Warp账号池维护 - 交互模式")
    print("=" * 60)
    print("  status  - 查看账号池状态")
    print("  verify  - 手动验证所有活跃账号的Token和额度")
    print("  clean   - 清理过期账号")
    print("  auto    - 启动自动维护")
    print("  exit    - 退出程序")
    print("=" * 60)
    while True:
        try:
            cmd = input("\n> ").strip().lower()
            if cmd == "status": await maintainer.check_pool_health()
            elif cmd == "verify": await maintainer.verify_accounts()
            elif cmd == "clean": await maintainer.cleanup()
            elif cmd == "auto":
                print("🔧 启动自动维护模式...")
                await maintainer.start()
            elif cmd == "exit": break
            else: print(f"❓ 未知命令: {cmd}")
        except KeyboardInterrupt: break
        except Exception as e: print(f"❌ 错误: {e}")
    print("👋 再见!")


# ==================== 主函数 ====================
async def main():
    """主函数"""
    import sys
    if len(sys.argv) > 1 and sys.argv[1].lower() == "interactive":
        await interactive_mode()
    else:
        logger.info("🔧 启动自动维护模式 (默认)")
        maintainer = PoolMaintainer()
        await maintainer.start()


if __name__ == "__main__":
    asyncio.run(main())
