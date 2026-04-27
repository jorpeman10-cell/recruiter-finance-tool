"""
数据库配置管理模块
用于安全存储和管理 Gllue 数据库连接配置
"""

import json
import base64
import os
from pathlib import Path


CONFIG_DIR = Path(__file__).parent / "config"
CONFIG_FILE = CONFIG_DIR / "db_config.json"


def _encode_password(password: str) -> str:
    """简单编码密码（混淆，非加密）"""
    if not password:
        return ""
    return base64.b64encode(password.encode()).decode()


def _decode_password(encoded: str) -> str:
    """解码密码"""
    if not encoded:
        return ""
    try:
        return base64.b64decode(encoded.encode()).decode()
    except Exception:
        return ""


def load_db_config() -> dict:
    """加载数据库配置，返回配置字典（密码已解码）"""
    if not CONFIG_FILE.exists():
        return _default_config()
    
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
        # 解码密码
        if config.get('password'):
            config['password'] = _decode_password(config['password'])
        if config.get('ssh_password'):
            config['ssh_password'] = _decode_password(config['ssh_password'])
        return config
    except Exception:
        return _default_config()


def save_db_config(config: dict) -> bool:
    """保存数据库配置（密码编码存储）"""
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        # 复制配置并编码密码
        save_config = config.copy()
        if save_config.get('password'):
            save_config['password'] = _encode_password(save_config['password'])
        if save_config.get('ssh_password'):
            save_config['ssh_password'] = _encode_password(save_config['ssh_password'])
        
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(save_config, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"保存配置失败: {e}")
        return False


def _default_config() -> dict:
    """默认配置"""
    return {
        "host": "127.0.0.1",
        "port": 3306,
        "database": "gllue",
        "username": "",
        "password": "",
        "use_ssh": True,
        "ssh_host": "118.190.96.172",
        "ssh_port": 9998,
        "ssh_user": "root",
        "ssh_password": "",
    }


def has_config() -> bool:
    """检查是否已配置数据库连接"""
    if not CONFIG_FILE.exists():
        return False
    config = load_db_config()
    return bool(config.get('username') and config.get('password'))


def get_gllue_db_config():
    """返回 GllueDBConfig 对象"""
    from gllue_db_client import GllueDBConfig
    cfg = load_db_config()
    return GllueDBConfig(
        host=cfg.get('host', '127.0.0.1'),
        port=cfg.get('port', 3306),
        database=cfg.get('database', 'gllue'),
        username=cfg.get('username', ''),
        password=cfg.get('password', ''),
        use_ssh=cfg.get('use_ssh', True),
        ssh_host=cfg.get('ssh_host', '118.190.96.172'),
        ssh_port=cfg.get('ssh_port', 9998),
        ssh_user=cfg.get('ssh_user', 'root'),
        ssh_password=cfg.get('ssh_password', ''),
    )
