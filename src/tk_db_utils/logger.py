from .config import db_logger_config
from tk_base_utils.tk_logger import get_logger,logger_wrapper,reload_logger
from logging import Logger

class DbLoggerProxy:
    def __init__(self):
        print(f'DbLoggerProxy init,db_logger_config.config_path:{db_logger_config.config_path}')
    """数据库日志配置代理类，确保配置的延迟初始化"""
    def __getattr__(self, name)->Logger:
        return getattr(get_logger(), name)

db_logger = DbLoggerProxy()
db_logger_wrapper = logger_wrapper



