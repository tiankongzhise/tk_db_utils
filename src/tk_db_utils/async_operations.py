import atexit
import signal
import weakref
import asyncio
import os
import traceback
import tomllib
import threading
import time

from pathlib import Path
from dotenv import load_dotenv
from typing import Type, Iterable, Optional, List, Dict, Any, Union, Generator,Coroutine
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from collections import defaultdict
from asyncio import Task

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy import event, select, update, delete, text, func, Insert, inspect, and_, or_, MetaData, Table, Column
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.sql.schema import UniqueConstraint, Index
from sqlalchemy.orm import DeclarativeBase
from pydantic import BaseModel

from .models import SqlAlChemyBase, DbOrmBaseMixedIn
from .message import message
from .utlis import get_unique_constraints, get_column_name
from .schema_validator import SchemaValidationError



# 加载环境变量
load_dotenv()




class AsyncDatabaseConfig:
    """异步数据库配置管理类"""
    
    def __init__(self, config_file: Optional[str] = None):
        # 从环境变量读取敏感信息
        self.host: Optional[str] = os.getenv("DB_HOST")
        self.port: str = os.getenv("DB_PORT", "3306")
        self.username: str = os.getenv("DB_USERNAME", "root")
        self.password: str = os.getenv("DB_PASSWORD", "password")
        
        # 加载TOML配置文件
        self._load_toml_config(config_file)
        
    def _load_toml_config(self, config_file: Optional[str] = None) -> None:
        """加载TOML配置文件"""
        if config_file is None:
            # 默认配置文件路径：项目根目录下的 db_config.toml
            current_path = Path(__file__).parent
            while current_path.parent != current_path:
                potential_config = current_path / "db_config.toml"
                if potential_config.exists():
                    config_file = potential_config
                    break
                if (current_path / "pyproject.toml").exists() or (current_path / "setup.py").exists():
                    config_file = current_path / "db_config.toml"
                    break
                current_path = current_path.parent
            else:
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
                self.driver: str = db_config.get("async_driver", "aiomysql")  # 使用异步驱动
                self.dialect: str = db_config.get("dialect", "mysql")
                self.charset: str = db_config.get("charset", "utf8mb4")
                self.collation: str = db_config.get("collation", "utf8mb4_unicode_ci")
                
                # 引擎配置
                self._engine_config = config.get("engine", {})
                
                # 连接配置
                conn_config = config.get("connection", {})
                if not os.getenv("DB_PORT"):
                    self.port = str(conn_config.get("default_port", 3306))
                
                # 程序结束时 数据库最大等待时间
                self.max_db_async_event_loop_wait_time = conn_config.get("max_db_async_event_loop_wait_time", 60)
                
                message.info(f"已加载异步数据库配置文件: {config_file}")
            else:
                message.warning(f"配置文件不存在，使用默认配置: {config_file}")
                self._set_default_config()
        except Exception as e:
            message.error(f"加载配置文件失败: {str(e)}，使用默认配置")
            self._set_default_config()
            
    def _set_default_config(self) -> None:
        """设置默认配置"""
        self.database = "test_db"
        self.driver = "aiomysql"  # 异步MySQL驱动
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
    def async_database_url(self) -> Optional[str]:
        """构建异步数据库连接URL"""
        if not self.host:
            return None
        
        base_url = f"{self.dialect}+{self.driver}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        
        # 根据不同的驱动添加不同的参数
        if self.driver == "aiomysql":
            # aiomysql 不支持 collation 参数，只使用 charset
            return f"{base_url}?charset={self.charset}"
        elif self.driver == "asyncpg":
            # PostgreSQL 驱动通常不需要这些参数
            return base_url
        elif self.driver == "aiosqlite":
            # SQLite 驱动不需要这些参数
            return base_url
        else:
            # 其他驱动尝试使用完整参数
            return f"{base_url}?charset={self.charset}&collation={self.collation}"
    
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


# 全局异步数据库配置
async_db_config = AsyncDatabaseConfig()



# 创建异步数据库引擎
async_engine: Optional[AsyncEngine] = None
if async_db_config.async_database_url:
    try:
        async_engine = create_async_engine(async_db_config.async_database_url, **async_db_config.get_engine_kwargs())
    except Exception as e:
        message.error(f"异步数据库引擎创建失败: {str(e)}")
        async_engine = None

# 创建异步会话工厂
AsyncSessionLocal: Optional[async_sessionmaker] = None
if async_engine:
    AsyncSessionLocal = async_sessionmaker(bind=async_engine, expire_on_commit=False)

def get_or_create_loop():
    try:
        loop = asyncio.get_running_loop()
        return loop
    except RuntimeError:
        pass  # 没有运行中的循环
    
    try:
        loop = asyncio.get_event_loop()
        if not loop.is_closed():
            return loop
    except RuntimeError:
        pass  # 没有现有循环
    
    # 创建新循环
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    return loop


async def async_init_db(sqlalchemy_base: Optional[Type[DeclarativeBase]] = None) -> None:
    """异步初始化数据库表结构"""
    if not async_db_config.host:
        error_msg = "数据库连接信息未配置，请检查.env文件中的DB_HOST设置"
        message.error(error_msg)
        raise ValueError(error_msg)
    
    if not async_engine:
        error_msg = "异步数据库引擎初始化失败，请检查数据库配置"
        message.error(error_msg)
        raise RuntimeError(error_msg)
    
    try:
        base = sqlalchemy_base or DbOrmBaseMixedIn
        async with async_engine.begin() as conn:
            await conn.run_sync(base.metadata.create_all)
        message.info("异步数据库表初始化成功")
    except Exception as e:
        error_msg = f"异步数据库表初始化失败: {str(e)}"
        message.error(error_msg)
        raise RuntimeError(error_msg) from e
def async_init_db_call_back(task:Task) -> None:
    """异步初始化数据库回调函数"""
    if task.cancelled():
        message.error("异步数据库初始化任务已取消")
    if task.exception():
        raise
    


@asynccontextmanager
async def async_get_session(auto_commit: bool = True) -> Generator[AsyncSession, None, None]:
    """获取异步数据库会话上下文管理器"""
    if not AsyncSessionLocal:
        error_msg = "异步会话工厂未初始化，请先检查数据库配置"
        message.error(error_msg)
        raise RuntimeError(error_msg)
    
    async with AsyncSessionLocal() as session:
        try:
            yield session
            if auto_commit:
                await session.commit()
        except Exception as e:
            await session.rollback()
            error_detail = traceback.format_exc()
            message.error(f"异步数据库操作出错: {str(e)}\n完整错误信息:\n{error_detail}，已回滚事务")
            raise


def get_async_engine() -> Optional[AsyncEngine]:
    """获取异步数据库引擎"""
    return async_engine


async def configure_async_database(
    host: Optional[str] = None,
    port: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    database: Optional[str] = None,
    driver: Optional[str] = None,
    dialect: Optional[str] = None,
    config_file: Optional[str] = None,
    **engine_kwargs
) -> AsyncEngine:
    """动态配置异步数据库连接"""
    global async_engine, AsyncSessionLocal, async_db_config
    
    # 如果提供了配置文件路径，重新创建配置对象
    if config_file is not None:
        async_db_config = AsyncDatabaseConfig(config_file)
    
    # 更新配置
    if host is not None:
        async_db_config.host = host
    if port is not None:
        async_db_config.port = port
    if username is not None:
        async_db_config.username = username
    if password is not None:
        async_db_config.password = password
    if database is not None:
        async_db_config.database = database
    if driver is not None:
        async_db_config.driver = driver
    if dialect is not None:
        async_db_config.dialect = dialect
    
    if not async_db_config.async_database_url:
        raise ValueError("数据库主机地址不能为空")
    
    try:
        # 合并引擎参数
        final_kwargs = async_db_config.get_engine_kwargs()
        final_kwargs.update(engine_kwargs)
        
        # 创建新的异步引擎
        async_engine = create_async_engine(async_db_config.async_database_url, **final_kwargs)

        AsyncSessionLocal = async_sessionmaker(bind=async_engine, expire_on_commit=False)
        
        message.info(f"异步数据库引擎配置成功: {async_db_config.async_database_url}")
        return async_engine
        
    except Exception as e:
        error_msg = f"异步数据库引擎配置失败: {str(e)}"
        message.error(error_msg)
        raise RuntimeError(error_msg) from e


class AsyncBaseCurd:
    """异步基础CRUD操作类"""
    
    def __init__(self, db_engine: Optional[AsyncEngine] = None, auto_init_db: bool = True):
        """初始化异步CRUD操作类"""            
        self.engine = db_engine or get_async_engine()
        if auto_init_db:
            task = asyncio.create_task(async_init_db())
            task.add_done_callback(async_init_db_call_back)
        if not self.engine:
            raise RuntimeError("异步数据库引擎未配置，请先配置数据库连接")
            
        self.max_db_async_event_loop_wait_time = async_db_config.max_db_async_event_loop_wait_time
    
    def _get_async_insert_ignore_stmt(self, table: Type[SqlAlChemyBase], data: List[Dict[str, Any]]):
        """获取异步INSERT IGNORE语句"""
        dialect_name = self.engine.dialect.name
        
        if dialect_name == 'mysql':
            stmt = mysql_insert(table).values(data)
            return stmt.prefix_with("IGNORE")
        elif dialect_name == 'postgresql':
            stmt = postgresql_insert(table).values(data)
            return stmt.on_conflict_do_nothing()
        elif dialect_name == 'sqlite':
            stmt = sqlite_insert(table).values(data)
            return stmt.prefix_with("OR IGNORE")
        else:
            raise NotImplementedError(f"不支持的数据库类型: {dialect_name}")
    
    def _get_unique_and_primary_keys(self, table: Type[SqlAlChemyBase]) -> List[str]:
        """获取表的唯一约束和主键列名称"""
        result_set = set()
        # 获取唯一约束
        unique_constraints = get_unique_constraints(table)
        for constraint in unique_constraints:
            result_set.update(constraint['columns'])
        # 获取主键列
        primary_keys = [key.name for key in table.__table__.primary_key]
        result_set.update(primary_keys)
        return list(result_set)
    
    def _get_async_replace_into_stmt(self, table: Type[SqlAlChemyBase], data: List[Dict[str, Any]]):
        """获取异步REPLACE INTO语句"""
        dialect_name = self.engine.dialect.name
        
        if dialect_name == 'mysql':
            insert_stmt = mysql_insert(table).values(data)
            all_columns = [col.name for col in table.__table__.columns]
            unique_and_primary_keys = self._get_unique_and_primary_keys(table)
            columns_to_update = [col for col in all_columns if col not in unique_and_primary_keys]
            
            update_dict = {}
            for col in columns_to_update:
                update_dict[col] = text(f"VALUES({col})")
            
            if 'updated_at' in columns_to_update:
                update_dict['updated_at'] = func.now()
            
            return insert_stmt.on_duplicate_key_update(update_dict)
        elif dialect_name == 'postgresql':
            stmt = postgresql_insert(table).values(data)
            primary_keys = [key.name for key in table.__table__.primary_key]
            if not primary_keys:
                raise ValueError(f"表 {table.__tablename__} 没有定义主键，无法执行REPLACE操作")
            
            update_dict = {c.name: stmt.excluded[c.name] 
                          for c in table.__table__.columns 
                          if c.name not in primary_keys}
            
            return stmt.on_conflict_do_update(
                index_elements=primary_keys,
                set_=update_dict
            )
        elif dialect_name == 'sqlite':
            stmt = sqlite_insert(table).values(data)
            return stmt.prefix_with("OR REPLACE")
        else:
            raise NotImplementedError(f"不支持的数据库类型: {dialect_name}")
    
    def _convert_objects_to_dict(self, objects: List[Union[Dict[str, Any], SqlAlChemyBase, BaseModel]]) -> List[Dict[str, Any]]:
        """将对象列表转换为字典列表"""
        if not objects:
            raise ValueError("对象列表不能为空")
        
        first_obj = objects[0]
        
        if isinstance(first_obj, dict):
            return objects
        elif hasattr(first_obj, '__table__'):  # SQLAlchemy模型
            result = []
            for obj in objects:
                if hasattr(obj, 'to_dict'):
                    result.append(obj.to_dict())
                elif hasattr(obj, 'special_fields'):
                    result.append({
                        c.name: getattr(obj, c.name)
                        for c in obj.__table__.columns 
                        if c.name not in getattr(obj, 'special_fields', [])
                    })
                else:
                    result.append({
                        c.name: getattr(obj, c.name)
                        for c in obj.__table__.columns
                    })
            return result
        elif isinstance(first_obj, BaseModel):  # Pydantic模型
            return [obj.model_dump() for obj in objects]
        else:
            raise TypeError(f"不支持的对象类型: {type(first_obj).__name__}，请传入字典、SQLAlchemy模型或Pydantic模型")

    async def async_bulk_insert_ignore(self, table: Type[SqlAlChemyBase], objects: Iterable, chunk_size: int = 3000) -> int:
        """异步分块批量插入数据，支持 INSERT IGNORE"""
        try:
            objects_list = list(objects)
            if not objects_list:
                message.warning('没有需要插入的数据')
                return 0
            
            if chunk_size <= 0:
                raise ValueError("chunk_size必须大于0")
                
            total = len(objects_list)
            inserted_count = 0
            
            async with self.engine.begin() as conn:
                for i in range(0, total, chunk_size):
                    chunk = objects_list[i:i + chunk_size]
                    chunk_dict = self._convert_objects_to_dict(chunk)
                    
                    stmt = self._get_async_insert_ignore_stmt(table, chunk_dict)
                    result = await conn.execute(stmt)
                    inserted_count += result.rowcount
                    
                    message.info(f"已处理: {min(i + chunk_size, total)}/{total} 条记录")
            
            message.info(f"异步批量INSERT IGNORE完成，共插入 {inserted_count} 条记录")
            return inserted_count
            
        except Exception as e:
            message.error(f"异步批量INSERT IGNORE失败: {str(e)}")
            raise RuntimeError(f"异步批量INSERT IGNORE失败: {str(e)}") from e
    
    async def async_bulk_replace_into(self, table: Type[SqlAlChemyBase], objects: Iterable, chunk_size: int = 3000) -> int:
        """异步分块批量替换数据，支持 REPLACE INTO"""
        try:
            objects_list = list(objects)
            if not objects_list:
                message.warning('没有需要替换的数据')
                return 0
            
            if chunk_size <= 0:
                raise ValueError("chunk_size必须大于0")
                
            total = len(objects_list)
            processed_count = 0
            
            async with self.engine.begin() as conn:
                for i in range(0, total, chunk_size):
                    chunk = objects_list[i:i + chunk_size]
                    chunk_dict = self._convert_objects_to_dict(chunk)
                    
                    stmt = self._get_async_replace_into_stmt(table, chunk_dict)
                    result = await conn.execute(stmt)
                    processed_count += result.rowcount
                    
                    message.info(f"已处理: {min(i + chunk_size, total)}/{total} 条记录")
            
            message.info(f"异步批量REPLACE INTO完成，共处理 {processed_count} 条记录")
            return processed_count
            
        except Exception as e:
            message.error(f"异步批量REPLACE INTO失败: {str(e)}")
            raise RuntimeError(f"异步批量REPLACE INTO失败: {str(e)}") from e
    
    async def async_bulk_insert(self, table: Type[SqlAlChemyBase], objects: Iterable, chunk_size: int = 3000) -> int:
        """异步分块批量插入数据"""
        try:
            objects_list = list(objects)
            if not objects_list:
                message.warning('没有需要插入的数据')
                return 0
            
            if chunk_size <= 0:
                raise ValueError("chunk_size必须大于0")
                
            total = len(objects_list)
            inserted_count = 0
            
            async with self.engine.begin() as conn:
                for i in range(0, total, chunk_size):
                    chunk = objects_list[i:i + chunk_size]
                    chunk_dict = self._convert_objects_to_dict(chunk)
                    
                    stmt = Insert(table).values(chunk_dict)
                    result = await conn.execute(stmt)
                    inserted_count += result.rowcount
                    
                    message.info(f"已处理: {min(i + chunk_size, total)}/{total} 条记录")
            
            message.info(f"异步批量INSERT完成，共插入 {inserted_count} 条记录")
            return inserted_count
            
        except Exception as e:
            message.error(f"异步批量INSERT失败: {str(e)}")
            raise RuntimeError(f"异步批量INSERT失败: {str(e)}") from e
    
    async def async_insert_one(self, table: Type[SqlAlChemyBase], data: Union[Dict[str, Any], SqlAlChemyBase, BaseModel]) -> int:
        """异步插入单条记录"""
        try:
            data_dict = self._convert_objects_to_dict([data])[0]
            
            async with self.engine.begin() as conn:
                stmt = Insert(table).values(data_dict)
                result = await conn.execute(stmt)
                
                message.info(f"异步成功插入1条记录到表 {table.__tablename__}")
                return result.lastrowid or result.rowcount
                
        except Exception as e:
            message.error(f"异步插入记录失败: {str(e)}")
            raise RuntimeError(f"异步插入记录失败: {str(e)}") from e
    
    async def async_select_all(self, table: Type[SqlAlChemyBase], limit: Optional[int] = None, offset: Optional[int] = None) -> List[SqlAlChemyBase]:
        """异步查询所有记录"""
        try:
            async with async_get_session(auto_commit=True) as session:
                stmt = select(table)
                if offset is not None:
                    stmt = stmt.offset(offset)
                if limit is not None:
                    stmt = stmt.limit(limit)
                    
                result = await session.execute(stmt)
                records = result.scalars().all()
                
                message.info(f"异步从表 {table.__tablename__} 查询到 {len(records)} 条记录")
                return records
                
        except Exception as e:
            message.error(f"异步查询记录失败: {str(e)}")
            raise RuntimeError(f"异步查询记录失败: {str(e)}") from e
    
    async def async_select_by_id(self, table: Type[SqlAlChemyBase], record_id: Any) -> Optional[SqlAlChemyBase]:
        """异步根据ID查询单条记录"""
        try:
            async with async_get_session(auto_commit=True) as session:
                record = await session.get(table, record_id)
                
                if record:
                    message.info(f"异步从表 {table.__tablename__} 查询到ID为 {record_id} 的记录")
                else:
                    message.warning(f"表 {table.__tablename__} 中不存在ID为 {record_id} 的记录")
                    
                return record
                
        except Exception as e:
            message.error(f"异步根据ID查询记录失败: {str(e)}")
            raise RuntimeError(f"异步根据ID查询记录失败: {str(e)}") from e
    
    async def async_select_by_conditions(self, table: Type[SqlAlChemyBase], conditions: Dict[str, Any], 
                           limit: Optional[int] = None, offset: Optional[int] = None) -> List[SqlAlChemyBase]:
        """异步根据条件查询记录"""
        try:
            async with async_get_session(auto_commit=True) as session:
                stmt = select(table)
                
                # 添加查询条件
                for column_name, value in conditions.items():
                    if hasattr(table, column_name):
                        column = getattr(table, column_name)
                        stmt = stmt.where(column == value)
                    else:
                        raise ValueError(f"表 {table.__tablename__} 不存在列 {column_name}")
                
                if offset is not None:
                    stmt = stmt.offset(offset)
                if limit is not None:
                    stmt = stmt.limit(limit)
                    
                result = await session.execute(stmt)
                records = result.scalars().all()
                
                message.info(f"异步从表 {table.__tablename__} 根据条件查询到 {len(records)} 条记录")
                return records
                
        except Exception as e:
            message.error(f"异步根据条件查询记录失败: {str(e)}")
            raise RuntimeError(f"异步根据条件查询记录失败: {str(e)}") from e
    
    async def async_update_by_id(self, table: Type[SqlAlChemyBase], record_id: Any, data: Dict[str, Any]) -> int:
        """异步根据ID更新记录"""
        try:
            primary_key_columns = [key.name for key in table.__table__.primary_key]
            if not primary_key_columns:
                raise ValueError(f"表 {table.__tablename__} 没有定义主键")
            
            primary_key_column = primary_key_columns[0]
            
            async with self.engine.begin() as conn:
                stmt = update(table).where(getattr(table, primary_key_column) == record_id).values(**data)
                result = await conn.execute(stmt)
                
                message.info(f"异步成功更新表 {table.__tablename__} 中 {result.rowcount} 条记录")
                return result.rowcount
                
        except Exception as e:
            message.error(f"异步根据ID更新记录失败: {str(e)}")
            raise RuntimeError(f"异步根据ID更新记录失败: {str(e)}") from e
    
    async def async_update_by_conditions(self, table: Type[SqlAlChemyBase], conditions: Dict[str, Any], data: Dict[str, Any]) -> int:
        """异步根据条件更新记录"""
        try:
            async with self.engine.begin() as conn:
                stmt = update(table)
                
                # 添加更新条件
                for column_name, value in conditions.items():
                    if hasattr(table, column_name):
                        column = getattr(table, column_name)
                        stmt = stmt.where(column == value)
                    else:
                        raise ValueError(f"表 {table.__tablename__} 不存在列 {column_name}")
                
                stmt = stmt.values(**data)
                result = await conn.execute(stmt)
                
                message.info(f"异步成功更新表 {table.__tablename__} 中 {result.rowcount} 条记录")
                return result.rowcount
                
        except Exception as e:
            message.error(f"异步根据条件更新记录失败: {str(e)}")
            raise RuntimeError(f"异步根据条件更新记录失败: {str(e)}") from e
    
    async def async_delete_by_id(self, table: Type[SqlAlChemyBase], record_id: Any) -> int:
        """异步根据ID删除记录"""
        try:
            primary_key_columns = [key.name for key in table.__table__.primary_key]
            if not primary_key_columns:
                raise ValueError(f"表 {table.__tablename__} 没有定义主键")
            
            primary_key_column = primary_key_columns[0]
            
            async with self.engine.begin() as conn:
                stmt = delete(table).where(getattr(table, primary_key_column) == record_id)
                result = await conn.execute(stmt)
                
                message.info(f"异步成功删除表 {table.__tablename__} 中 {result.rowcount} 条记录")
                return result.rowcount
                
        except Exception as e:
            message.error(f"异步根据ID删除记录失败: {str(e)}")
            raise RuntimeError(f"异步根据ID删除记录失败: {str(e)}") from e
    
    async def async_delete_by_conditions(self, table: Type[SqlAlChemyBase], conditions: Dict[str, Any]) -> int:
        """异步根据条件删除记录"""
        try:
            async with self.engine.begin() as conn:
                stmt = delete(table)
                
                # 添加删除条件
                for column_name, value in conditions.items():
                    if hasattr(table, column_name):
                        column = getattr(table, column_name)
                        stmt = stmt.where(column == value)
                    else:
                        raise ValueError(f"表 {table.__tablename__} 不存在列 {column_name}")
                
                result = await conn.execute(stmt)
                
                message.info(f"异步成功删除表 {table.__tablename__} 中 {result.rowcount} 条记录")
                return result.rowcount
                
        except Exception as e:
            message.error(f"异步根据条件删除记录失败: {str(e)}")
            raise RuntimeError(f"异步根据条件删除记录失败: {str(e)}") from e
    
    async def async_count(self, table: Type[SqlAlChemyBase], conditions: Optional[Dict[str, Any]] = None) -> int:
        """异步统计记录数"""
        try:
            async with async_get_session(auto_commit=True) as session:
                stmt = select(table)
                
                # 添加统计条件
                if conditions:
                    for column_name, value in conditions.items():
                        if hasattr(table, column_name):
                            column = getattr(table, column_name)
                            stmt = stmt.where(column == value)
                        else:
                            raise ValueError(f"表 {table.__tablename__} 不存在列 {column_name}")
                
                result = await session.execute(stmt)
                count = len(result.scalars().all())
                
                message.info(f"异步表 {table.__tablename__} 统计结果: {count} 条记录")
                return count
                
        except Exception as e:
            message.error(f"异步统计记录失败: {str(e)}")
            raise RuntimeError(f"异步统计记录失败: {str(e)}") from e
    
    async def async_execute_raw_sql(self, sql: str, params: Optional[Dict[str, Any]] = None):
        """异步执行原生SQL语句"""
        try:
            async with self.engine.begin() as conn:
                if params:
                    result = await conn.execute(text(sql), params)
                else:
                    result = await conn.execute(text(sql))
                
                message.info(f"异步成功执行原生SQL语句")
                return result
                
        except Exception as e:
            message.error(f"异步执行原生SQL失败: {str(e)}")
            raise RuntimeError(f"异步执行原生SQL失败: {str(e)}") from e
            
        
        
class AsyncSchemaValidator:
    """异步ORM模型与数据库表结构一致性检查器"""
    
    def __init__(self, engine: AsyncEngine, session: AsyncSession):
        self.engine = engine
        self.session = session
    
    async def async_validate_model_schema(self, model: Type[SqlAlChemyBase], 
                            strict_mode: bool = True) -> Dict[str, Any]:
        """异步验证ORM模型与数据库表结构的一致性"""
        table_name = model.__tablename__
        
        # 检查表是否存在
        if not await self._async_table_exists(model):
            error_msg = f"表 '{table_name}' 在数据库中不存在"
            message.error(error_msg)
            if strict_mode:
                raise SchemaValidationError(error_msg)
            return {
                'valid': False,
                'table_exists': False,
                'errors': [error_msg]
            }
        
        # 获取数据库表结构
        db_table_info = await self._async_get_database_table_info(table_name)
        
        # 获取ORM模型结构
        orm_table_info = self._get_orm_table_info(model)
        
        # 比较结构
        validation_result = self._compare_table_structures(
            orm_table_info, db_table_info, table_name
        )
        
        # 记录验证结果
        if validation_result['valid']:
            message.info(f"异步表 '{table_name}' 结构验证通过")
        else:
            error_msg = f"异步表 '{table_name}' 结构验证失败，发现 {len(validation_result['errors'])} 个不一致项"
            message.error(error_msg)
            
            if strict_mode:
                raise SchemaValidationError(
                    f"{error_msg}\n详细错误:\n" + "\n".join(validation_result['errors'])
                )
        
        return validation_result
    
    async def _async_table_exists(self, table_model: Type[SqlAlChemyBase]) -> bool:
        """异步检查表是否存在"""
        try:
            table_name = table_model.__tablename__
            table_schema = table_model.__table__.schema
            if table_schema:
                result = await self.session.execute(
                    text("SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = :table_schema AND table_name = :table_name"),
                    {"table_schema":table_schema,"table_name": table_name}
                )
            else:
                result = await self.session.execute(
                    text("SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = DATABASE() AND table_name = :table_name"),
                    {"table_name": table_name}
                )
            return result.fetchone() is not None
        except Exception as e:
            message.error(f"异步检查表存在性时出错: {str(e)}")
            return False
    
    async def _async_get_database_table_info(self, table_name: str) -> Dict[str, Any]:
        """异步获取数据库中表的实际结构信息"""
        # 使用反射获取表结构
        metadata = MetaData()
        
        # 异步反射表结构
        async with self.engine.begin() as conn:
            await conn.run_sync(lambda sync_conn: metadata.reflect(bind=sync_conn, only=[table_name]))
        
        table = metadata.tables[table_name]
        
        # 获取唯一索引信息
        unique_columns = set()
        for idx in table.indexes:
            if idx.unique and len(idx.columns) == 1:
                unique_columns.add(list(idx.columns)[0].name)
        
        # 提取列信息
        columns = {}
        for col in table.columns:
            is_unique = col.name in unique_columns or col.unique
            columns[col.name] = {
                'type': str(col.type),
                'nullable': col.nullable,
                'default': self._get_column_default(col),
                'primary_key': col.primary_key,
                'unique': is_unique,
                'autoincrement': getattr(col, 'autoincrement', False)
            }
        
        # 提取索引信息
        indexes = []
        for idx in table.indexes:
            indexes.append({
                'name': idx.name,
                'columns': [col.name for col in idx.columns],
                'unique': idx.unique
            })
        
        # 提取约束信息
        constraints = []
        try:
            # 查询唯一约束
            unique_constraints_query = text("""
                SELECT CONSTRAINT_NAME, COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = :table_name
                AND CONSTRAINT_NAME != 'PRIMARY'
                AND CONSTRAINT_NAME IN (
                    SELECT CONSTRAINT_NAME 
                    FROM information_schema.TABLE_CONSTRAINTS 
                    WHERE CONSTRAINT_TYPE = 'UNIQUE'
                    AND TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = :table_name
                )
                ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
            """)
            
            result = await self.session.execute(unique_constraints_query, {"table_name": table_name})
            constraint_columns = {}
            
            for row in result:
                constraint_name = row[0]
                column_name = row[1]
                if constraint_name not in constraint_columns:
                    constraint_columns[constraint_name] = []
                constraint_columns[constraint_name].append(column_name)
            
            for constraint_name, columns_list in constraint_columns.items():
                constraints.append({
                    'type': 'unique',
                    'name': constraint_name,
                    'columns': columns_list
                })
                
        except Exception:
            # 如果查询失败，回退到反射机制
            for constraint in table.constraints:
                if isinstance(constraint, UniqueConstraint):
                    constraints.append({
                        'type': 'unique',
                        'name': constraint.name,
                        'columns': [col.name for col in constraint.columns]
                    })
        
        # 提取外键信息
        foreign_keys = []
        for fk in table.foreign_keys:
            foreign_keys.append({
                'column': fk.parent.name,
                'referenced_table': fk.column.table.name,
                'referenced_column': fk.column.name
            })
        
        return {
            'name': table_name,
            'columns': columns,
            'indexes': indexes,
            'constraints': constraints,
            'foreign_keys': foreign_keys
        }
    
    def _get_orm_table_info(self, model: Type[SqlAlChemyBase]) -> Dict[str, Any]:
        """获取ORM模型的表结构信息"""
        table = model.__table__
        
        # 提取列信息
        columns = {}
        for col in table.columns:
            columns[col.name] = {
                'type': str(col.type),
                'nullable': col.nullable,
                'default': self._get_column_default(col),
                'primary_key': col.primary_key,
                'unique': col.unique,
                'autoincrement': getattr(col, 'autoincrement', False)
            }
        
        # 提取索引信息
        indexes = []
        for idx in table.indexes:
            indexes.append({
                'name': idx.name,
                'columns': [col.name for col in idx.columns],
                'unique': idx.unique
            })
        
        # 提取约束信息
        constraints = []
        
        # 1. 提取表级约束
        for constraint in table.constraints:
            if isinstance(constraint, UniqueConstraint):
                constraints.append({
                    'type': 'unique',
                    'name': constraint.name,
                    'columns': [col.name for col in constraint.columns]
                })
        
        # 2. 提取字段级unique约束
        for col in table.columns:
            if col.unique and not col.primary_key:
                # 检查是否已经有表级约束覆盖了这个列
                column_covered = False
                for constraint in constraints:
                    if constraint['columns'] == [col.name]:
                        column_covered = True
                        break
                
                # 如果没有被表级约束覆盖，则添加字段级约束
                if not column_covered:
                    constraints.append({
                        'type': 'unique',
                        'name': f'uq_{table.name}_{col.name}',
                        'columns': [col.name]
                    })
        
        # 提取外键信息
        foreign_keys = []
        for col in table.columns:
            for fk in col.foreign_keys:
                foreign_keys.append({
                    'column': col.name,
                    'referenced_table': fk.column.table.name,
                    'referenced_column': fk.column.name
                })
        
        return {
            'name': table.name,
            'columns': columns,
            'indexes': indexes,
            'constraints': constraints,
            'foreign_keys': foreign_keys
        }
    
    def _get_column_default(self, column: Column) -> Any:
        """获取列的默认值"""
        if column.default is None:
            return None
        
        if hasattr(column.default, 'arg'):
            return column.default.arg
        elif hasattr(column.default, 'name'):
            return f"FUNCTION: {column.default.name}"
        else:
            return str(column.default)
    
    def _compare_table_structures(self, orm_info: Dict[str, Any], 
                                db_info: Dict[str, Any], table_name: str) -> Dict[str, Any]:
        """比较ORM模型和数据库表结构"""
        errors = []
        
        # 比较列
        orm_columns = orm_info['columns']
        db_columns = db_info['columns']
        
        # 检查缺失的列
        for col_name in orm_columns:
            if col_name not in db_columns:
                errors.append(f"ORM模型中的列 '{col_name}' 在数据库表中不存在")
        
        # 检查多余的列
        for col_name in db_columns:
            if col_name not in orm_columns:
                errors.append(f"数据库表中的列 '{col_name}' 在ORM模型中不存在")
        
        # 检查列属性差异
        for col_name in set(orm_columns.keys()) & set(db_columns.keys()):
            orm_col = orm_columns[col_name]
            db_col = db_columns[col_name]
            
            # 比较类型（简化比较）
            if str(orm_col['type']).upper() != str(db_col['type']).upper():
                errors.append(f"列 '{col_name}' 类型不匹配: ORM={orm_col['type']}, DB={db_col['type']}")
            
            # 比较可空性
            if orm_col['nullable'] != db_col['nullable']:
                errors.append(f"列 '{col_name}' 可空性不匹配: ORM={orm_col['nullable']}, DB={db_col['nullable']}")
            
            # 比较主键
            if orm_col['primary_key'] != db_col['primary_key']:
                errors.append(f"列 '{col_name}' 主键属性不匹配: ORM={orm_col['primary_key']}, DB={db_col['primary_key']}")
        
        return {
            'valid': len(errors) == 0,
            'table_exists': True,
            'errors': errors
        }


# 异步工具函数
async def async_filter_unique_conflicts(session: AsyncSession, model: Type[SqlAlChemyBase], object_list: list[Any]):
    """异步优化后的去重方法，批量处理唯一约束冲突检查"""
    # 获取模型的所有唯一约束
    unique_constraints = get_unique_constraints(model)
    
    # 如果没有唯一约束，直接返回原始列表
    if not unique_constraints:
        return object_list, []
    
    # 用于存储已存在的唯一键组合
    seen_keys = defaultdict(lambda: defaultdict(set))
    kept_objects = []
    conflict_objects = []
    
    # 先收集所有需要检查的唯一键组合
    all_constraint_values = defaultdict(list)
    
    for obj in object_list:
        for constraint in unique_constraints:
            key_values = tuple(getattr(obj, col_name) for col_name in constraint['columns'])
            all_constraint_values[constraint['name']].append((obj, key_values))
    
    # 批量查询数据库检查已存在的记录
    db_existing_keys = defaultdict(set)
    
    for constraint in unique_constraints:
        # 收集所有需要检查的值组合
        value_combinations = set()
        for obj, key_values in all_constraint_values[constraint['name']]:
            value_combinations.add(key_values)
        
        if not value_combinations:
            continue
            
        # 构建批量查询条件
        conditions = []
        for values in value_combinations:
            condition_parts = []
            for col_name, value in zip(constraint['columns'], values):
                if value is None:
                    condition_parts.append(getattr(model, col_name).is_(None))
                else:
                    condition_parts.append(getattr(model, col_name) == value)
            conditions.append(and_(*condition_parts))
        
        # 执行批量查询
        if len(conditions) == 1:
            stmt = select(model).where(conditions[0])
        else:
            stmt = select(model).where(or_(*conditions))
        
        result = await session.execute(stmt)
        
        # 获取数据库中已存在的键组合
        for record in result.scalars():
            key = tuple(getattr(record, col_name) for col_name in constraint['columns'])
            db_existing_keys[constraint['name']].add(key)
    
    # 第二次遍历检查冲突
    for obj in object_list:
        is_conflict = False
        
        for constraint in unique_constraints:
            # 获取当前对象的约束键值组合
            key_values = tuple(getattr(obj, col_name) for col_name in constraint['columns'])
            
            # 检查内存中是否已存在
            if key_values in seen_keys[constraint['name']]['memory']:
                is_conflict = True
                break
                
            # 检查数据库中是否已存在
            if key_values in db_existing_keys[constraint['name']]:
                is_conflict = True
                break
            
            # 标记为已存在（内存中）
            seen_keys[constraint['name']]['memory'].add(key_values)
        
        if is_conflict:
            conflict_objects.append(obj)
        else:
            kept_objects.append(obj)
    
    return kept_objects, conflict_objects


async def async_process_objects_with_conflicts(session: AsyncSession, model: Type[SqlAlChemyBase], objects):
    """异步处理对象冲突"""
    message.info('正在异步对数据进行预处理,去除冲突对象')
    kept, conflicts = await async_filter_unique_conflicts(session, model, objects)
    
    # 打印冲突警告
    for obj in conflicts:
        message.warning(f"发现冲突对象 - {obj}")
    message.info('异步数据预处理完成')
    
    return kept


# 异步验证函数
async def async_validate_schema_consistency(engine: AsyncEngine, models: List[Type[SqlAlChemyBase]], 
                                          strict_mode: bool = True) -> Dict[str, Any]:
    """异步验证多个模型的模式一致性"""
    async with async_get_session(auto_commit=True) as session:
        validator = AsyncSchemaValidator(engine, session)
        
        results = {}
        all_valid = True
        
        for model in models:
            try:
                result = await validator.async_validate_model_schema(model, strict_mode=False)
                results[model.__tablename__] = result
                if not result['valid']:
                    all_valid = False
            except Exception as e:
                results[model.__tablename__] = {
                    'valid': False,
                    'error': str(e)
                }
                all_valid = False
        
        if strict_mode and not all_valid:
            error_details = []
            for table_name, result in results.items():
                if not result.get('valid', False):
                    if 'errors' in result:
                        error_details.extend([f"{table_name}: {error}" for error in result['errors']])
                    elif 'error' in result:
                        error_details.append(f"{table_name}: {result['error']}")
            
            raise SchemaValidationError(
                f"模式验证失败，发现以下问题:\n" + "\n".join(error_details)
            )
        
        return {
            'all_valid': all_valid,
            'results': results
        }


def run_async(coro:Coroutine):
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(coro)
    finally:
        engine = get_async_engine()
        if engine:
            loop.run_until_complete(engine.dispose())
