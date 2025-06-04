from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase
from dotenv import load_dotenv
from contextlib import contextmanager
from typing import Generator, Type, Optional, Dict, Any
from .models import DbOrmBaseMixedIn
from .message import message

import os
import traceback

# 加载环境变量
load_dotenv()

# 数据库配置类
class DatabaseConfig:
    """数据库配置管理类"""
    
    def __init__(self):
        self.host: Optional[str] = os.getenv("DB_HOST")
        self.port: str = os.getenv("DB_PORT", "3306")
        self.username: str = os.getenv("DB_USERNAME", "root")
        self.password: str = os.getenv("DB_PASSWORD", "password")
        self.database: str = os.getenv("DB_NAME_DEFINE", "test_db")
        self.driver: str = os.getenv("DB_DRIVER", "pymysql")
        self.dialect: str = os.getenv("DB_DIALECT", "mysql")
        self.charset: str = os.getenv("DB_CHARSET", "utf8mb4")
        self.collation: str = os.getenv("DB_COLLATION", "utf8mb4_unicode_ci")
        
    @property
    def database_url(self) -> Optional[str]:
        """构建数据库连接URL"""
        if not self.host:
            return None
        return f"{self.dialect}+{self.driver}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?charset={self.charset}&collation={self.collation}"
    
    def get_engine_kwargs(self) -> Dict[str, Any]:
        """获取引擎配置参数"""
        return {
            'echo': os.getenv("DB_ECHO", "false").lower() == "true",
            'pool_size': int(os.getenv("DB_POOL_SIZE", "10")),
            'max_overflow': int(os.getenv("DB_MAX_OVERFLOW", "20")),
            'pool_timeout': int(os.getenv("DB_POOL_TIMEOUT", "30")),
            'pool_recycle': int(os.getenv("DB_POOL_RECYCLE", "3600")),
            'pool_pre_ping': os.getenv("DB_POOL_PRE_PING", "true").lower() == "true",
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
        **engine_kwargs: 引擎额外参数
        
    Returns:
        Engine: 配置好的数据库引擎
        
    Raises:
        ValueError: 当必要参数缺失时
        RuntimeError: 当引擎创建失败时
    """
    global engine, SessionLocal
    
    # 更新配置
    if host is not None:
        db_config.host = host
    if port is not None:
        db_config.port = port
    if username is not None:
        db_config.username = username
    if password is not None:
        db_config.password = password
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
