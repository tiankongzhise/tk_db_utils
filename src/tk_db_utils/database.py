from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase
from contextlib import contextmanager
from typing import Generator, Type, Dict, Any
import os
import traceback
from .config import db_config,set_db_config_path,DatabaseConfig
from .logger import db_logger,reload_logger,logger_wrapper
from .models import SqlAlChemyBase
from pathlib import Path
from dotenv import load_dotenv

class SqlalchemyMysqlClient(object):
    
    def __init__(self) -> None:
        self.db_config :DatabaseConfig = None
        self.engine :Engine = None
    
    @logger_wrapper()
    def init_client(self,env_file_path:str|Path|None = None,db_config_path:str|Path|None = None,db_logger_config_path:str|Path|None = None):
        if db_config_path and Path(db_config_path).exists():
            set_db_config_path(db_config_path)
        if env_file_path and Path(env_file_path).exists():
            load_dotenv(env_file_path)
        if db_logger_config_path and Path(db_logger_config_path).exists():
            reload_logger(db_logger_config_path)
        self.db_config = db_config
        self.create_engine()        
        return self
    @logger_wrapper()
    def init_db(self,database:Type[DeclarativeBase]):
        try:
            database.metadata.create_all(bind=self.engine)
            return self
        except Exception as e:
            raise ValueError(f"init db table failed,err:{e}")
    
    @logger_wrapper()
    def create_engine(self) -> Engine:
        # 从环境变量获取数据库连接信息
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT",3306)
        user_name = os.getenv("DB_USERNAME")
        password = os.getenv("DB_PASSWORD")
        
        db_connect_error = ''
        if  not host:
            db_connect_error += 'host not found,'
        if  not port:
            db_connect_error += 'port not found,'
        if  not user_name:
            db_connect_error += 'user_name not found,'
        if  not password:
            db_connect_error += 'password not found'
        if db_connect_error:
            raise ValueError(f"db connect error,{db_connect_error}")
        
        # 从配置文件获取数据库连接信息
        driver: str = self.db_config.db_driver
        dialect: str =self.db_config.db_dialect
        charset: str = self.db_config.db_charset
        collation: str = self.db_config.db_collation
        self.database = self.db_config.db_name
        engine_url = f"{dialect}+{driver}://{user_name}:{password}@{host}:{port}/{self.database}?charset={charset}&collation={collation}"
        # 从配置文件获取数据库引擎配置参数
        engine_kwargs = {
            'echo': self.db_config.db_echo,
            'pool_size': self.db_config.db_pool_size,
            'max_overflow': self.db_config.db_max_overflow,
            'pool_timeout': self.db_config.db_pool_timeout,
            'pool_recycle': self.db_config.db_pool_recycle,
            'pool_pre_ping': self.db_config.db_pool_pre_ping,
        }
        db_logger.debug(
            f"create engine,engine_url:{engine_url},engine_kwargs:{engine_kwargs}"
        )
        self.engine = create_engine(engine_url,**engine_kwargs)
        if not self.engine:
            raise ValueError(f"create engine failed,host:{host},port:{port},user_name:{user_name},password:{password},database:{self.database},driver:{driver},dialect:{dialect},charset:{charset},collation:{collation}")
        return self
    @logger_wrapper()
    def create_session_factory(self) -> sessionmaker:
        self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        return self
    @logger_wrapper()
    def get_engine(self) -> Engine:
        if not self.engine:
            raise ValueError("db engine not init")
        return self.engine
    @logger_wrapper()
    def auto_init(self,env_file_path:str|Path|None = None,
                  db_config_path:str|Path|None = None,
                  db_logger_config_path:str|Path|None = None,
                  database:Type[DeclarativeBase] = SqlAlChemyBase):
        self.init_client(env_file_path,db_config_path,db_logger_config_path).init_db(database).create_session_factory()
        return self
    @logger_wrapper()
    def get_session(self) -> Session:
        if not self.session_factory:
            raise ValueError("db session factory not init")
        return self.session_factory()
    @property
    @logger_wrapper()
    def session_scope(self):
        return self._session_scope_context()
    @logger_wrapper()
    @contextmanager
    def _session_scope_context(self) -> Generator[Session, None, None]:
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            error_detail = traceback.format_exc()
            db_logger.error(f"db session scope context error,err:{e},error detail:{error_detail}")
            raise e
        finally:
            session.close()
    
    
_db_client = None

@logger_wrapper()
def get_db_client(env_file_path:str|Path|None = None,
                  db_config_path:str|Path|None = None,
                  db_logger_config_path:str|Path|None = None,
                  database:Type[DeclarativeBase] = SqlAlChemyBase,
                  single_client:bool = True) -> SqlalchemyMysqlClient:
    global _db_client
    if not _db_client:
        _db_client = SqlalchemyMysqlClient().auto_init(env_file_path,db_config_path,db_logger_config_path,database)
        return _db_client
    if single_client:
        return _db_client
    else:
        return SqlalchemyMysqlClient().auto_init(env_file_path,db_config_path,db_logger_config_path,database)

        

