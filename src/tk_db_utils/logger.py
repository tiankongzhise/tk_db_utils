from tk_base_utils.tk_logger import get_logger,logger_wrapper,reload_logger
from tk_base_utils.tk_logger.logger import EnhancedLogger

class DbLoggerProxy:
    """数据库日志配置代理类，确保配置的延迟初始化"""
    def __getattr__(self, name)->EnhancedLogger:
        """获取日志记录器的属性"""
        return getattr(get_logger(), name)

db_logger:EnhancedLogger = DbLoggerProxy()

db_logger_wrapper = logger_wrapper



