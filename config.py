#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一配置管理
"""

# ==================== 临时邮箱API配置 ====================
# 临时邮箱服务的基础URL
TEMP_MAIL_BASE_URL = "https://mail.chatgpt.org.uk/api"

# ==================== 代理配置 ====================
# HTTP代理，用于常规请求
PROXY_URL = "None"
OPENAI_COMPAT_HOST = "0.0.0.0"

# ==================== 账号池维护 (pool_maintenance.py) ====================
MIN_POOL_SIZE = 25  # 最小账号池大小
MAX_POOL_SIZE = 60  # 最大账号池大小
TOKEN_REFRESH_HOURS = 1  # Token刷新间隔（小时）
MAINTENANCE_CHECK_INTERVAL = 120  # 维护检查间隔（秒）
MIN_QUOTA_THRESHOLD = 10 # 剩余请求额度阈值，低于此值的账号将被标记为过期

# ==================== 数据库配置 ====================
DATABASE_PATH = "warp_accounts.db"
DB_TIMEOUT = 10.0  # 数据库操作超时时间（秒）

# ==================== Firebase API 配置 ====================
FIREBASE_API_KEY = "AIzaSyBdy3O3S9hrdayLJxJ7mriBR4qgUaUygAs"
FIREBASE_API_KEYS = [
    FIREBASE_API_KEY
]

# ==================== 账号池服务 (pool_service.py) ====================
POOL_SERVICE_HOST = "0.0.0.0"
POOL_SERVICE_PORT = 8019
MAX_SESSION_DURATION = 30 * 60  # 会话最大持续时间（30分钟）

# ==================== 账号注册 (warp_register.py) ====================
TARGET_ACCOUNTS = 200  # 目标账号数
MAX_CONCURRENT_REGISTER = 10  # 最大并发注册数
MAX_PROXY_RETRIES = 5  # 代理重试次数

# ==================== OpenAI兼容服务 (openai_compat.py) ====================
OPENAI_COMPAT_HOST = "0.0.0.0"
OPENAI_COMPAT_PORT = 7777

# ==================== Protobuf主服务 (server.py) ====================
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 8000

# ==================== 日志配置 ====================
LOG_LEVEL = "INFO"
LOG_FORMAT = '%(asctime)s - %(levelname)s - [%(processName)s] - %(message)s'
