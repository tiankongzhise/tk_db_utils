# SQLAlchemy 2.0 异步引擎自动清理机制

## 概述

`tk_db_utils` 现在提供了完整的 SQLAlchemy 2.0 异步引擎自动清理机制，解决了作为 CRUD 工具需要使用者在 main 函数中手动关闭数据库连接的问题。

## 主要特性

### ✅ 自动资源管理
- **无需手动清理**: 程序退出时自动清理所有异步数据库引擎
- **智能注册**: 自动检测和注册所有创建的异步引擎
- **弱引用管理**: 使用 `weakref.WeakSet` 避免循环引用和内存泄漏

### ✅ 多种触发方式
- **程序正常退出**: 通过 `atexit` 模块注册清理函数
- **信号处理**: 支持 `SIGTERM` 和 `SIGINT` 信号
- **异常退出**: 确保在各种退出场景下都能正确清理

### ✅ 超时保护
- **可配置超时**: 通过 `max_db_async_event_loop_wait_time` 配置清理超时时间
- **防止阻塞**: 避免清理过程无限期阻塞程序退出
- **优雅降级**: 超时后强制退出，记录警告信息

## 使用方法

### 1. 基本使用（推荐）

```python
import asyncio
from tk_db_utils.async_operations import async_get_session, AsyncBaseCurd

async def main():
    # 直接使用，无需手动清理
    async with async_get_session() as session:
        # 执行数据库操作
        pass
    
    # 或使用 CRUD 类
    crud = AsyncBaseCurd()
    # 执行 CRUD 操作
    
    # 程序结束时会自动清理所有资源

if __name__ == "__main__":
    asyncio.run(main())
    # 无需手动调用 engine.dispose()
```

### 2. 动态配置数据库

```python
from tk_db_utils.async_operations import configure_async_database

async def main():
    # 动态配置数据库连接
    engine = await configure_async_database(
        host="localhost",
        port="3306",
        username="root",
        password="password",
        database="test_db"
    )
    # 引擎会自动注册到清理注册表
    
    # 使用配置的引擎...
    
    # 程序退出时自动清理
```

### 3. 自定义引擎

```python
from sqlalchemy.ext.asyncio import create_async_engine
from tk_db_utils.async_operations import AsyncBaseCurd

async def main():
    # 创建自定义引擎
    custom_engine = create_async_engine(
        "sqlite+aiosqlite:///./test.db",
        echo=True
    )
    
    # 使用自定义引擎（会自动注册）
    crud = AsyncBaseCurd(db_engine=custom_engine)
    
    # 程序退出时自动清理
```

## 配置选项

### 数据库配置文件 (db_config.toml)

```toml
[connection]
# 程序结束时数据库最大等待时间（秒）
max_db_async_event_loop_wait_time = 60
```

### 环境变量

```bash
# 基本连接信息
DB_HOST=localhost
DB_PORT=3306
DB_USERNAME=root
DB_PASSWORD=password
```

## 工作原理

### 1. 引擎注册机制

```python
# 全局引擎注册表，使用弱引用
_engine_registry = weakref.WeakSet()

def register_engine_for_cleanup(engine: AsyncEngine):
    """注册引擎到清理注册表"""
    _engine_registry.add(engine)
    _register_cleanup()  # 注册清理函数
```

### 2. 自动注册时机

- **全局引擎创建时**: 模块加载时自动注册
- **动态配置时**: `configure_async_database()` 调用时注册
- **CRUD 初始化时**: `AsyncBaseCurd` 使用自定义引擎时注册

### 3. 清理触发机制

```python
# 注册 atexit 清理函数
atexit.register(_cleanup_all_engines)

# 注册信号处理器
signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)
```

### 4. 清理执行流程

1. **收集引擎**: 从弱引用注册表获取所有活跃引擎
2. **创建清理任务**: 异步调用每个引擎的 `dispose()` 方法
3. **超时控制**: 使用 `asyncio.wait_for()` 防止无限等待
4. **错误处理**: 捕获并记录清理过程中的异常
5. **循环管理**: 正确处理事件循环的创建和关闭

## 最佳实践

### ✅ 推荐做法

1. **使用全局配置**: 优先使用模块级别的全局引擎配置
2. **配置超时时间**: 根据应用需求设置合理的清理超时时间
3. **监控日志**: 关注清理过程的日志输出，及时发现问题
4. **测试清理**: 在开发环境测试各种退出场景

### ❌ 避免的做法

1. **手动清理**: 不要再手动调用 `engine.dispose()`
2. **忽略日志**: 不要忽略清理过程中的警告和错误信息
3. **过短超时**: 不要设置过短的清理超时时间
4. **阻塞清理**: 避免在清理过程中执行耗时操作

## 故障排除

### 常见问题

1. **清理超时**
   ```
   WARNING: 数据库清理超时（60秒），强制退出
   ```
   **解决方案**: 增加 `max_db_async_event_loop_wait_time` 配置值

2. **引擎未注册**
   ```
   INFO: 正在清理 0 个异步数据库引擎...
   ```
   **解决方案**: 检查引擎创建方式，确保使用了库提供的方法

3. **循环已关闭错误**
   ```
   ERROR: 清理异步数据库引擎时出错: Event loop is closed
   ```
   **解决方案**: 这通常是正常现象，清理机制会自动处理

### 调试技巧

1. **启用详细日志**: 在配置中设置 `echo=True`
2. **监控引擎数量**: 观察清理时的引擎数量是否符合预期
3. **测试信号处理**: 使用 `Ctrl+C` 测试信号处理是否正常

## 版本兼容性

- **SQLAlchemy**: 2.0+
- **Python**: 3.8+
- **异步驱动**: aiomysql, asyncpg, aiosqlite

## 更新日志

### v1.0.0
- ✅ 实现异步引擎自动清理机制
- ✅ 支持弱引用管理避免内存泄漏
- ✅ 添加信号处理和超时控制
- ✅ 提供完整的使用示例和文档

---

通过这个自动清理机制，开发者可以专注于业务逻辑，而不用担心数据库连接的资源管理问题。系统会在程序退出时自动、安全地清理所有异步数据库资源。