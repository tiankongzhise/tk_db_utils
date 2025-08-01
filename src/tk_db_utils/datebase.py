from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase
from contextlib import contextmanager
from typing import Generator, Type, Optional, Dict, Any
from .models import DbOrmBaseMixedIn
from .message import message

import os
import traceback
import tomllib
from pathlib import Path

def get_env_db_config() -> Dict[str, Any]:
    """从环境变量获取数据库配置"""
    return {
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT", "3306"),
        "username": os.getenv("DB_USERNAME", "root"),
        "password": os.getenv("DB_PASSWORD", "password"),
    }

# 数据库配置类
class DatabaseConfig:
    """数据库配置管理类
    
    从 .env 文件读取敏感信息（主机、端口、用户名、密码）
    从 db_config.toml 文件读取引擎配置参数
    """
    
    def __init__(self, config_file: Optional[str] = None,env_settings:dict|None=None):
        if env_settings is None:
            # 从环境变量读取敏感信息
            self.host: Optional[str] = os.getenv("DB_HOST")
            self.port: str = os.getenv("DB_PORT", "3306")
            self.username: str = os.getenv("DB_USERNAME", "root")
            self.password: str = os.getenv("DB_PASSWORD", "password")
        else:
            self.host = env_settings.get("host")
            self.port = env_settings.get("port")
            self.username = env_settings.get("username")
            self.password = env_settings.get("password")
        # 加载TOML配置文件
        self._load_toml_config(config_file)
        
    def _load_toml_config(self, config_file: Optional[str] = None) -> None:
        """加载TOML配置文件"""
        if config_file is None:
            # 默认配置文件路径：项目根目录下的 db_config.toml
            # 从当前文件位置向上查找项目根目录
            current_path = Path(__file__).parent
            while current_path.parent != current_path:
                potential_config = current_path / "db_config.toml"
                if potential_config.exists():
                    config_file = potential_config
                    break
                # 检查是否有pyproject.toml或setup.py，表示这是项目根目录
                if (current_path / "pyproject.toml").exists() or (current_path / "setup.py").exists():
                    config_file = current_path / "db_config.toml"
                    break
                current_path = current_path.parent
            else:
                # 如果没找到项目根目录，使用当前工作目录
                config_file = Path.cwd() / "db_config.toml"
        else:
            config_file = Path(config_file)
            
        try:
            if config_file.exists():
                with open(config_file, "rb") as f:
                    config = tomllib.load(f)
                    
                # 数据库基本配置
                db_config = config.get("database", {})
                self.database: str = db_config.get("database", "test_db")
                self.driver: str = db_config.get("driver", "pymysql")
                self.dialect: str = db_config.get("dialect", "mysql")
                self.charset: str = db_config.get("charset", "utf8mb4")
                self.collation: str = db_config.get("collation", "utf8mb4_unicode_ci")
                
                # 引擎配置
                self._engine_config = config.get("engine", {})
                
                # 连接配置
                conn_config = config.get("connection", {})
                if not os.getenv("DB_PORT"):
                    self.port = str(conn_config.get("default_port", 3306))
                    
                message.info(f"已加载数据库配置文件: {config_file}")
            else:
                message.warning(f"配置文件不存在，使用默认配置: {config_file}")
                self._set_default_config()
        except Exception as e:
            message.error(f"加载配置文件失败: {str(e)}，使用默认配置")
            self._set_default_config()
            
    def _set_default_config(self) -> None:
        """设置默认配置"""
        self.database = "test_db"
        self.driver = "pymysql"
        self.dialect = "mysql"
        self.charset = "utf8mb4"
        self.collation = "utf8mb4_unicode_ci"
        self._engine_config = {
            "echo": False,
            "pool_size": 5,
            "max_overflow": 10,
            "pool_timeout": 30,
            "pool_recycle": 3600,
            "pool_pre_ping": True
        }
        
    @property
    def database_url(self) -> Optional[str]:
        """构建数据库连接URL"""
        if not self.host:
            return None
        return f"{self.dialect}+{self.driver}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?charset={self.charset}&collation={self.collation}"
    
    def get_engine_kwargs(self) -> Dict[str, Any]:
        """获取引擎配置参数"""
        return {
            'echo': self._engine_config.get("echo", False),
            'pool_size': self._engine_config.get("pool_size", 5),
            'max_overflow': self._engine_config.get("max_overflow", 10),
            'pool_timeout': self._engine_config.get("pool_timeout", 30),
            'pool_recycle': self._engine_config.get("pool_recycle", 3600),
            'pool_pre_ping': self._engine_config.get("pool_pre_ping", True),
        }

# 全局数据库配置
db_config = DatabaseConfig()

# 创建数据库引擎
engine: Optional[Engine] = None
if db_config.database_url:
    try:
        engine = create_engine(db_config.database_url, **db_config.get_engine_kwargs())
    except Exception as e:
        message.error(f"数据库引擎创建失败: {str(e)}")
        engine = None

# 创建会话工厂
SessionLocal: Optional[sessionmaker] = None
if engine:
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

def init_db(sqlalchemy_base: Optional[Type[DeclarativeBase]] = None) -> None:
    """
    初始化数据库表结构
    
    Args:
        sqlalchemy_base: 继承自DeclarativeBase的基类，如果为None则使用默认DbOrmBaseMixedIn
    
    Raises:
        ValueError: 当数据库配置缺失时
        RuntimeError: 当表创建失败时
    """
    if not db_config.host:
        error_msg = "数据库连接信息未配置，请检查.env文件中的DB_HOST设置"
        message.error(error_msg)
        raise ValueError(error_msg)
    
    if not engine:
        error_msg = "数据库引擎初始化失败，请检查数据库配置"
        message.error(error_msg)
        raise RuntimeError(error_msg)
    
    try:
        base = sqlalchemy_base or DbOrmBaseMixedIn
        base.metadata.create_all(bind=engine)
        message.info("数据库表初始化成功")
    except Exception as e:
        error_msg = f"数据库表初始化失败: {str(e)}"
        message.error(error_msg)
        raise RuntimeError(error_msg) from e

@contextmanager
def get_session(auto_commit: bool = True) -> Generator[Session, None, None]:
    """
    获取数据库会话上下文管理器 (SQLAlchemy 2.0风格)
    
    Args:
        auto_commit: 是否在上下文结束时自动提交事务
        
    Yields:
        Session: SQLAlchemy会话对象
        
    Raises:
        RuntimeError: 如果会话工厂未正确初始化
    """
    if not SessionLocal:
        error_msg = "会话工厂未初始化，请先检查数据库配置"
        message.error(error_msg)
        raise RuntimeError(error_msg)
    
    session = SessionLocal()
    try:
        yield session
        if auto_commit:
            session.commit()
    except Exception as e:
        session.rollback()
        # 获取完整的错误堆栈信息
        error_detail = traceback.format_exc()
        message.error(f"数据库操作出错: {str(e)}\n完整错误信息:\n{error_detail}，已回滚事务")
        raise  # 重新抛出异常以便调用方处理
    finally:
        session.close()

def get_engine() -> Optional[Engine]:
    """
    获取数据库引擎
    
    Returns:
        Optional[Engine]: 数据库引擎实例，如果未配置则返回None
    """
    return engine

def configure_database(
    host: Optional[str] = None,
    port: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    driver: Optional[str] = None,
    dialect: Optional[str] = None,
    config_file: Optional[str] = None,
    **engine_kwargs
) -> Engine:
    """
    动态配置数据库连接
    
    Args:
        host: 数据库主机
        port: 数据库端口
        username: 用户名
        password: 密码
        database: 数据库名
        driver: 数据库驱动
        dialect: 数据库方言
        config_file: 配置文件路径，如果提供则重新加载配置
        **engine_kwargs: 引擎额外参数
        
    Returns:
        Engine: 配置好的数据库引擎
        
    Raises:
        ValueError: 当必要参数缺失时
        RuntimeError: 当引擎创建失败时
    """
    global engine, SessionLocal, db_config
    
    # 如果提供了配置文件路径，重新创建配置对象
    if config_file is not None:
        db_config = DatabaseConfig(config_file)
    
    # 更新敏感信息配置（这些通常来自环境变量或参数）
    if host is not None:
        db_config.host = host
    if port is not None:
        db_config.port = port
    if username is not None:
        db_config.username = username
    if password is not None:
        db_config.password = password
        
    # 更新数据库基本配置（这些通常来自配置文件，但也允许参数覆盖）
    if database is not None:
        db_config.database = database
    if driver is not None:
        db_config.driver = driver
    if dialect is not None:
        db_config.dialect = dialect
    
    if not db_config.database_url:
        raise ValueError("数据库主机地址不能为空")
    
    try:
        # 合并引擎参数
        final_kwargs = db_config.get_engine_kwargs()
        final_kwargs.update(engine_kwargs)
        
        # 创建新引擎
        engine = create_engine(db_config.database_url, **final_kwargs)
        SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
        
        message.info(f"数据库引擎配置成功: {db_config.database_url}")
        return engine
        
    except Exception as e:
        error_msg = f"数据库引擎配置失败: {str(e)}"
        message.error(error_msg)
        raise RuntimeError(error_msg) from e
