from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase
from contextlib import contextmanager
from typing import Generator, Type, Dict, Any
from .message import message

import os
import traceback
import tomllib
from dotenv import load_dotenv
from pathlib import Path


class SqlalchemyMysqlClient(object):
    def __init__(self) -> None:
        message.debug('init sqlalchemy mysql client')
        self.db_config :Dict[str,Any] = None
        self.logger_settings :Dict[str,Any] = None
        self.engine :Engine = None
        self.env_file_path :Path|str = None
        self.db_config_path :Path|str = None
        
    def init_client(self,env_file_path:str|Path,db_config_path:str|Path):
        message.debug(f"init client,env_file_path:{env_file_path},db_config_path:{db_config_path}")
        message.info('db init client is run!')
        self.env_file_path = env_file_path
        self.db_config_path = db_config_path
        self.load_db_config_settings()
        message.debug(f"db config:{self.db_config}")
        self.init_message()
        message.debug(f"logger settings:{self.logger_settings}")
        self.load_env_settings()
        message.debug(f"env settings:DB_HOST:{os.getenv('DB_HOST')},DB_PORT:{os.getenv('DB_PORT')},DB_USERNAME:{os.getenv('DB_USERNAME')},DB_PASSWORD:{os.getenv('DB_PASSWORD')}")
        self.create_engine()        
        message.debug(f"engine:{self.engine}")
        message.info('db init client is success!')
        return self

    def init_db(self,database:Type[DeclarativeBase]):
        message.debug(f"init db,db:{database}")
        message.info('db init db is run!')
        try:
            database.metadata.create_all(bind=self.engine)
            message.debug(f"db init db success,db:{database}")
            message.info('db init db is success!')
            return self
        except Exception as e:
            message.error(f"init db table failed,err:{e}")
            raise ValueError(f"init db table failed,err:{e}")
    
    def create_engine(self) -> Engine:
        message.info('db create engine is run!')
        # 从环境变量获取数据库连接信息
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        user_name = os.getenv("DB_USERNAME")
        password = os.getenv("DB_PASSWORD")
        # 从配置文件获取数据库连接信息
        driver: str = self.db_config.get("driver", "pymysql")
        dialect: str =self.db_config.get("dialect", "mysql")
        charset: str = self.db_config.get("charset", "utf8mb4")
        collation: str = self.db_config.get("collation", "utf8mb4_unicode_ci")
        self.database = self.db_config.get("database", "test")
        engine_url = f"{dialect}+{driver}://{user_name}:{password}@{host}:{port}/{self.database}?charset={charset}&collation={collation}"
        # 从配置文件获取数据库引擎配置参数
        engine_kwargs = {
            'echo': self.db_config.get("echo", False),
            'pool_size': self.db_config.get("pool_size", 5),
            'max_overflow': self.db_config.get("max_overflow", 10),
            'pool_timeout': self.db_config.get("pool_timeout", 30),
            'pool_recycle': self.db_config.get("pool_recycle", 3600),
            'pool_pre_ping': self.db_config.get("pool_pre_ping", True),
        }
        message.debug(
            f"create engine,engine_url:{engine_url},engine_kwargs:{engine_kwargs}"
        )
        self.engine = create_engine(engine_url,**engine_kwargs)
        if not self.engine:
            message.error(f"create engine failed,host:{host},port:{port},user_name:{user_name},password:{password},database:{self.database},driver:{driver},dialect:{dialect},charset:{charset},collation:{collation}")
            raise ValueError(f"create engine failed,host:{host},port:{port},user_name:{user_name},password:{password},database:{self.database},driver:{driver},dialect:{dialect},charset:{charset},collation:{collation}")
        message.info('db create engine is success!')
        return self
        
    def create_session_factory(self) -> sessionmaker:
        message.info('db create session factory is run!')
        self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        message.debug(f"create session factory,engine:{self.engine}")
        message.info('db create session factory is success!')
        return self
    def load_db_config_settings(self):
        message.info('db load db config settings is run!')
        if isinstance(self.db_config_path,str):
            self.db_config_path = Path(self.db_config_path)
        if not self.db_config_path.exists():
            message.error(f"db config file not found:{self.db_config_path}")
            raise FileNotFoundError(f"db config file not found:{self.db_config_path}")
        try:
            with self.db_config_path.open("rb") as f:
                config_settings = tomllib.load(f)
                self.db_config = config_settings.get("db_config",{})
        except Exception as e:
            message.error(f"load db config file failed,err:{e}")
            raise ValueError(f"load db config file failed,err:{e}")
        self.logger_settings = self.db_config.get("logger",{})
        message.debug(f"db config:{self.db_config}")
        message.info('db load db config settings is success!')

    def init_message(self):
        message.info('db init message is run!')
        message.debug(f"logger settings:{self.logger_settings}")
        message.init(self.logger_settings)
        message.info('db init message is success!')
        
    def load_env_settings(self):
        message.info('db load env settings is run!')
        if isinstance(self.env_file_path,str):
            self.env_file_path = Path(self.env_file_path)
        if not self.env_file_path.exists():
            message.error(f"env file not found:{self.env_file_path}")
            raise FileNotFoundError(f"env file not found:{self.env_file_path}")
        message.debug(f"env file path:{self.env_file_path}")
        load_dotenv(self.env_file_path)
        err_msg = ""
        if os.getenv("DB_HOST") is None:
            err_msg += "DB_HOST not found in env file;\n"
        if os.getenv("DB_PORT") is None:
            err_msg += "DB_PORT not found in env file;\n"
        if os.getenv("DB_USERNAME") is None:
            err_msg += "DB_USERNAME not found in env file;\n"   
        if os.getenv("DB_PASSWORD") is None:
            err_msg += "DB_PASSWORD not found in env file;\n"
        if err_msg:
            message.error(f"load env settings failed,err:{err_msg}")
            raise ValueError(err_msg)
        message.info('db load env settings is success!')
    
    def get_engine(self) -> Engine:
        message.info('db get engine is run!')
        message.debug(f"engine:{self.engine}")
        if not self.engine:
            message.error("db engine not init")
            raise ValueError("db engine not init")
        message.info('db get engine is success!')
        return self.engine
    
    def auto_init(self,env_file_path:str|Path,db_config_path:str|Path,database:Type[DeclarativeBase]):
        message.info('db auto init is run!')
        message.debug(f"env file path:{env_file_path},db config path:{db_config_path},database:{database}")
        self.init_client(env_file_path,db_config_path).init_db(database).create_session_factory()
        message.info('db auto init is success!')
        return self

    def get_session(self) -> Session:
        message.info('db get session is run!')
        message.debug(f"session factory:{self.session_factory}")
        if not self.session_factory:
            message.error("db session factory not init")
            raise ValueError("db session factory not init")
        message.info('db get session is success!')
        return self.session_factory()
    
    @property
    def session_scope(self):
        message.info('db session scope is run!')
        return self._session_scope_context()

    @contextmanager
    def _session_scope_context(self) -> Generator[Session, None, None]:
        message.debug('db _session scope context is run!')
        message.debug(f"session factory:{self.session_factory}")
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            error_detail = traceback.format_exc()
            message.error(f"db session scope context error,err:{e},error detail:{error_detail}")
            raise e
        finally:
            session.close()
    
    
_db_client = SqlalchemyMysqlClient()

def get_db_client() -> SqlalchemyMysqlClient:
    return _db_client
        

