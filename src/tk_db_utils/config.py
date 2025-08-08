"""配置管理模块

负责读取和解析config.toml配置文件。
"""

import tomllib
from typing import Dict, Any
from pathlib import Path
from dotenv import load_dotenv
from tk_base_utils.tk_logger import set_logger_config_path,get_logger_config
from tk_base_utils.tk_logger.config import TkLoggerConfig



class DatabaseConfig:
    """数据库配置类"""
    
    def __init__(self, config_path: str | Path = "config.toml",secret_path: str | Path = ".env"):
        self.secret_path = Path(secret_path)
        self.config_path = Path(config_path)
        self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        default_config = {
            "db_config": {
                # 数据库基本配置
                "database": "test_db",  # 数据库名称
                "driver": "pymysql",           # 数据库驱动 (pymysql, psycopg2, sqlite等)
                "dialect": "mysql",               # 数据库方言 (mysql, postgresql, sqlite等)
                "charset": "utf8mb4",             # 字符集
                "collation": "utf8mb4_general_ci", # 排序规则

                # SQLAlchemy 引擎配置
                "echo": False,          # 是否打印SQL语句到控制台
                "pool_size": 5,         # 连接池大小
                "max_overflow": 10,     # 连接池最大溢出连接数
                "pool_timeout": 30,     # 获取连接的超时时间（秒）
                "pool_recycle": 3600,   # 连接回收时间（秒）
                "pool_pre_ping": True,  # 连接前是否ping测试连接有效性
                # 连接配置
                "default_port": 3306,        # 默认端口（当环境变量未设置时使用）
                "connection_timeout": 30,    # 连接超时时间
                "read_timeout": 30,          # 读取超时时间
                "write_timeout": 30,         # 写入超时时间
            }
        }
        if self.secret_path.exists():
            load_dotenv(self.secret_path)
        if self.config_path.exists():
            try:
                with open(self.config_path, 'rb') as f:
                    config = tomllib.load(f)
                # 合并默认配置和用户配置
                default_config.update(config.get("db_config", {}))
                return default_config
            except Exception as e:
                print(f"警告: 读取配置文件失败 {e}，使用默认配置")
                return default_config
        else:
            # # 如果配置文件不存在，创建默认配置文件
            # self._create_default_config(default_config)
            return default_config


        
        
        
    
    @property
    def db_config(self) -> Dict[str, Any]:
        """获取数据库配置"""
        return self._config.get("db_config", {})
    
    @property
    def db_name(self) -> str:
        """获取数据库名称"""
        return self.db_config.get("database", "test_db")
    
    @property
    def db_driver(self) -> str:
        """获取数据库驱动"""
        return self.db_config.get("driver", "pymysql")
    
    @property
    def db_dialect(self) -> str:
        """获取数据库方言"""
        return self.db_config.get("dialect", "mysql")
    
    @property
    def db_charset(self) -> str:
        """获取数据库字符集"""
        return self.db_config.get("charset", "utf8mb4")
    
    @property
    def db_collation(self) -> str:
        """获取数据库排序规则"""
        return self.db_config.get("collation", "utf8mb4_general_ci")
    
    @property
    def db_echo(self) -> bool:
        """获取数据库是否打印SQL语句"""
        return self.db_config.get("echo", False)
    
    @property
    def db_pool_size(self) -> int:
        """获取数据库连接池大小"""
        return self.db_config.get("pool_size", 5)
    
    @property
    def db_max_overflow(self) -> int:
        """获取数据库连接池最大溢出连接数"""
        return self.db_config.get("max_overflow", 10)
    
    @property
    def db_pool_timeout(self) -> int:
        """获取数据库获取连接的超时时间（秒）"""
        return self.db_config.get("pool_timeout", 30)
    
    @property
    def db_pool_recycle(self) -> int:
        """获取数据库连接回收时间（秒）"""
        return self.db_config.get("pool_recycle", 3600)
    
    @property
    def db_pool_pre_ping(self) -> bool:
        """获取数据库连接前是否ping测试连接有效性"""
        return self.db_config.get("pool_pre_ping", True)
    
    @property
    def db_default_port(self) -> int:
        """获取数据库默认端口"""
        return self.db_config.get("default_port", 3306)

    @property
    def db_connection_timeout(self) -> int:
        """获取数据库连接超时时间（秒）"""
        return self.db_config.get("connection_timeout", 30)
    
    @property
    def db_read_timeout(self) -> int:
        """获取数据库读取超时时间（秒）"""
        return self.db_config.get("read_timeout", 30)
    
    @property
    def db_write_timeout(self) -> int:
        """获取数据库写入超时时间（秒）"""
        return self.db_config.get("write_timeout", 30)

# 全局配置实例
_config_instance = None


def set_db_config_path(config_path: str|Path,secret_path:str|Path) -> None:

    """设置配置文件路径并重新初始化配置
    
    Args:
        config_path: config.toml文件的路径
    """
    global _config_instance
    _config_instance = DatabaseConfig(config_path,secret_path)



def get_db_config() -> DatabaseConfig:
    """获取配置实例，如果未初始化则使用默认路径"""
    global _config_instance
    if _config_instance is None:
        _config_instance = DatabaseConfig()
    return _config_instance


# 为了向后兼容，保持config变量的访问方式
class DbConfigProxy:
    """配置代理类，确保配置的延迟初始化"""
    
    def __getattr__(self, name)->DatabaseConfig:
        return getattr(get_db_config(), name)



db_config:DatabaseConfig = DbConfigProxy()

def set_db_logger_config_path(config_path: str|Path) -> None:
    """设置数据库日志配置文件路径"""
    set_logger_config_path(config_path)

class LoggerConfigProxy:
    """日志配置代理类，确保配置的延迟初始化"""
    def __getattr__(self, name)->TkLoggerConfig:
        return getattr(get_logger_config(), name)
db_logger_config:TkLoggerConfig = LoggerConfigProxy()
