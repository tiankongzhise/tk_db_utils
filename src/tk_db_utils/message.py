import logging
from typing import Optional, Dict, Any

class Message:
    """消息处理类，支持多种日志处理器"""
    
    _instance: Optional['Message'] = None
    _initialized: bool = False
    
    def __new__(cls, message_handler=None):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, message_handler=None):
        # 避免重复初始化
        if self._initialized:
            return
            
        if message_handler is None:
            # 创建一个专用的logger，避免与其他模块冲突
            logger = logging.getLogger('tk_db_tool_default')
            if not logger.handlers:
                # 只有在没有handler时才添加默认handler
                handler = logging.StreamHandler()
                formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
                handler.setFormatter(formatter)
                logger.addHandler(handler)
                logger.setLevel(logging.INFO)
            self.message_handler = logger
        else:
            self.message_handler = message_handler
            
        self._initialized = True
    
    def init(self,logging_config:Dict[str,Any]):
        # 创建一个专用的logger，避免与其他模块冲突
        logger = logging.getLogger('tk_db_tool_custom')
        path = logging_config.get('path')
        level = logging_config.get('level','default')
        level_map = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
            "none": logging.NOTSET,
            "all": logging.DEBUG,
            "default": logging.INFO,
        }
        level = level_map.get(level.lower(),level_map['default'])
        formatter = logging_config.get('formatter')
        if formatter is None:
            formatter = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        if path:
            handler = logging.FileHandler(path)
        else:
            handler = logging.StreamHandler()
        formatter = logging.Formatter(
            formatter
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)
        self.message_handler = logger
        
        
    
    def debug(self, message: str):
        """输出调试信息"""
        self.message_handler.debug(message)
    
    def info(self, message: str):
        """输出信息"""
        self.message_handler.info(message)
        
    def warning(self, message: str):
        """输出警告信息"""
        self.message_handler.warning(message)
        
    def error(self, message: str):
        """输出错误信息"""
        self.message_handler.error(message)
        
    def critical(self, message: str):
        """输出严重错误信息"""
        self.message_handler.critical(message)
    
    def set_logger_level(self, level: str):
        """设置日志级别"""
        level_maps = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
            "none": logging.NOTSET,
            "all": logging.DEBUG,
            "default": logging.INFO,
        }
        
        temp_level_str = level.lower()
        level_value = level_maps.get(temp_level_str)
        
        if level_value is None:
            available_levels = ", ".join(level_maps.keys())
            raise ValueError(
                f"无效的日志级别: {level}\n"
                f"可用级别: {available_levels}"
            )
        
        try:
            # 处理Logger对象
            if isinstance(self.message_handler, logging.Logger):
                self.message_handler.setLevel(level_value)
            # 处理Handler对象
            elif isinstance(self.message_handler, logging.Handler):
                self.message_handler.setLevel(level_value)
            # 有setLevel方法的自定义handler
            elif hasattr(self.message_handler, "setLevel"):
                self.message_handler.setLevel(level_value)
            # 有level属性的自定义handler
            elif hasattr(self.message_handler, "level"):
                self.message_handler.level = level_value
            else:
                raise ValueError(f"无法设置日志级别 (handler类型: {type(self.message_handler).__name__})")
        except Exception as e:
            raise RuntimeError(
                f"设置日志级别失败 (handler类型: {type(self.message_handler).__name__}): {str(e)}"
            ) from e

    def set_message_handler(self, message_handler):
        """设置消息处理器"""
        self.message_handler = message_handler
    
    def set_message_config(self, config: Dict[str, Any]):
        """设置日志配置"""
        try:
            if isinstance(self.message_handler, logging.Logger):
                # 对于Logger对象，我们需要配置其handlers
                if 'level' in config:
                    level = config['level']
                    if isinstance(level, str):
                        level = getattr(logging, level.upper())
                    self.message_handler.setLevel(level)
                
                if 'format' in config and self.message_handler.handlers:
                    formatter = logging.Formatter(config['format'])
                    for handler in self.message_handler.handlers:
                        handler.setFormatter(formatter)
            else:
                # 尝试使用logging.basicConfig
                logging.basicConfig(**config)
        except Exception as e:
            raise RuntimeError(
                f"设置日志配置失败 (handler类型: {type(self.message_handler).__name__}): {str(e)}"
            ) from e
    
   

# 全局单例实例
_global_message = None

def get_message_instance() -> Message:
    """获取全局消息实例"""
    global _global_message
    if _global_message is None:
        _global_message = Message()
    return _global_message

# 向后兼容的全局实例
message = get_message_instance()

# 提供全局函数接口，确保在pip安装后也能正常工作
def set_message_handler(handler):
    """设置全局消息处理器"""
    get_message_instance().set_message_handler(handler)

def set_message_config(config: Dict[str, Any]):
    """设置全局消息配置"""
    get_message_instance().set_message_config(config)

def set_logger_level(level: str):
    """设置全局日志级别"""
    get_message_instance().set_logger_level(level)

