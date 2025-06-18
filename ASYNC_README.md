# tk_db_utils 异步操作指南

## 🎉 新功能：异步引擎自动清理

**现在无需手动关闭数据库连接！** `tk_db_utils` 提供了完整的 SQLAlchemy 2.0 异步引擎自动清理机制。

### 主要优势
- ✅ **零配置**: 自动检测和注册所有异步引擎
- ✅ **智能清理**: 程序退出时自动清理所有资源
- ✅ **信号支持**: 支持 SIGTERM/SIGINT 信号处理
- ✅ **内存安全**: 使用弱引用避免内存泄漏
- ✅ **超时保护**: 防止清理过程阻塞程序退出

### 使用示例
```python
import asyncio
from tk_db_utils.async_operations import async_get_session, AsyncBaseCurd

async def main():
    # 直接使用，无需手动清理
    async with async_get_session() as session:
        # 执行数据库操作
        pass
    
    # 程序结束时会自动清理所有资源

if __name__ == "__main__":
    asyncio.run(main())
    # 🎉 无需手动调用 engine.dispose()!
```

📖 **详细文档**: [异步引擎自动清理机制](./ASYNC_AUTO_CLEANUP.md)

---

# tk_db_utils 异步功能使用指南

本文档介绍 `tk_db_utils` 包中新增的异步数据库操作功能。所有异步功能都集中在 `async_operations.py` 模块中。

## 功能概述

异步功能包括以下主要组件：

1. **AsyncDatabaseConfig** - 异步数据库配置管理
2. **AsyncBaseCurd** - 异步CRUD操作类
3. **AsyncSchemaValidator** - 异步模式验证器
4. **异步工具函数** - 各种异步辅助功能

## 安装依赖

使用异步功能需要安装额外的异步数据库驱动：

```bash
# MySQL异步驱动
pip install aiomysql

# PostgreSQL异步驱动
pip install asyncpg

# SQLite异步驱动（通常已包含在aiosqlite中）
pip install aiosqlite
```

## 基本使用

### 1. 导入异步功能

```python
from tk_db_utils import (
    AsyncDatabaseConfig,
    AsyncBaseCurd,
    async_init_db,
    async_get_session,
    configure_async_database
)
```

### 2. 配置异步数据库连接

#### 方法一：使用环境变量和配置文件

创建 `.env` 文件：
```env
DB_HOST=localhost
DB_PORT=3306
DB_USERNAME=root
DB_PASSWORD=your_password
```

创建 `db_config.toml` 文件：
```toml
[database]
database = "your_database"
async_driver = "aiomysql"  # 或 "asyncpg" 用于PostgreSQL
dialect = "mysql"  # 或 "postgresql"
charset = "utf8mb4"
collation = "utf8mb4_unicode_ci"

[engine]
echo = false
pool_size = 5
max_overflow = 10
pool_timeout = 30
pool_recycle = 3600
pool_pre_ping = true
```

#### 方法二：动态配置

```python
import asyncio
from tk_db_utils import configure_async_database

async def setup_database():
    engine = await configure_async_database(
        host="localhost",
        port="3306",
        username="root",
        password="your_password",
        database="your_database",
        driver="aiomysql",
        dialect="mysql"
    )
    return engine
```

### 3. 初始化数据库表

```python
import asyncio
from tk_db_utils import async_init_db, DbOrmBaseMixedIn

# 定义你的模型
class User(DbOrmBaseMixedIn):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    email = Column(String(100), unique=True)

async def init_tables():
    await async_init_db()

# 运行初始化
asyncio.run(init_tables())
```

### 4. 使用异步CRUD操作

```python
import asyncio
from tk_db_utils import AsyncBaseCurd, async_get_session

async def crud_example():
    # 创建异步CRUD实例
    crud = AsyncBaseCurd()
    
    # 插入单条记录
    user_data = {"name": "张三", "email": "zhangsan@example.com"}
    user_id = await crud.async_insert_one(User, user_data)
    print(f"插入用户ID: {user_id}")
    
    # 批量插入
    users_data = [
        {"name": "李四", "email": "lisi@example.com"},
        {"name": "王五", "email": "wangwu@example.com"}
    ]
    inserted_count = await crud.async_bulk_insert(User, users_data)
    print(f"批量插入 {inserted_count} 条记录")
    
    # 查询所有记录
    all_users = await crud.async_select_all(User)
    print(f"查询到 {len(all_users)} 个用户")
    
    # 根据ID查询
    user = await crud.async_select_by_id(User, user_id)
    if user:
        print(f"找到用户: {user.name}")
    
    # 条件查询
    users = await crud.async_select_by_conditions(
        User, 
        {"name": "张三"}
    )
    print(f"条件查询结果: {len(users)} 条记录")
    
    # 更新记录
    updated_count = await crud.async_update_by_id(
        User, 
        user_id, 
        {"email": "new_email@example.com"}
    )
    print(f"更新了 {updated_count} 条记录")
    
    # 统计记录数
    count = await crud.async_count(User)
    print(f"总用户数: {count}")
    
    # 删除记录
    deleted_count = await crud.async_delete_by_id(User, user_id)
    print(f"删除了 {deleted_count} 条记录")

# 运行示例
asyncio.run(crud_example())
```

### 5. 使用异步会话管理

```python
from tk_db_utils import async_get_session

async def session_example():
    async with async_get_session() as session:
        # 在这里执行数据库操作
        # 会话会自动提交和关闭
        result = await session.execute(
            select(User).where(User.name == "张三")
        )
        users = result.scalars().all()
        return users
```

### 6. 异步批量操作

```python
async def bulk_operations_example():
    crud = AsyncBaseCurd()
    
    # 批量插入（忽略重复）
    users_data = [
        {"name": "用户1", "email": "user1@example.com"},
        {"name": "用户2", "email": "user2@example.com"},
        # 可能包含重复数据
    ]
    
    inserted_count = await crud.async_bulk_insert_ignore(User, users_data)
    print(f"INSERT IGNORE 插入了 {inserted_count} 条记录")
    
    # 批量替换
    replaced_count = await crud.async_bulk_replace_into(User, users_data)
    print(f"REPLACE INTO 处理了 {replaced_count} 条记录")
```

### 7. 异步冲突检测

```python
from tk_db_utils import async_filter_unique_conflicts, async_get_session

async def conflict_detection_example():
    users_data = [
        User(name="用户1", email="user1@example.com"),
        User(name="用户2", email="user1@example.com"),  # 邮箱重复
        User(name="用户3", email="user3@example.com"),
    ]
    
    async with async_get_session() as session:
        kept_users, conflict_users = await async_filter_unique_conflicts(
            session, User, users_data
        )
        
        print(f"保留 {len(kept_users)} 个用户")
        print(f"发现 {len(conflict_users)} 个冲突用户")
```

### 8. 异步模式验证

```python
from tk_db_utils import async_validate_schema_consistency, get_async_engine

async def schema_validation_example():
    engine = get_async_engine()
    
    # 验证单个模型
    async with async_get_session() as session:
        validator = AsyncSchemaValidator(engine, session)
        result = await validator.async_validate_model_schema(User)
        
        if result['valid']:
            print("模式验证通过")
        else:
            print(f"模式验证失败: {result['errors']}")
    
    # 验证多个模型
    models = [User]  # 添加更多模型
    validation_result = await async_validate_schema_consistency(
        engine, models, strict_mode=False
    )
    
    if validation_result['all_valid']:
        print("所有模型验证通过")
    else:
        print("部分模型验证失败")
        for table_name, result in validation_result['results'].items():
            if not result['valid']:
                print(f"{table_name}: {result.get('errors', [])}")
```

### 9. 执行原生SQL

```python
async def raw_sql_example():
    crud = AsyncBaseCurd()
    
    # 执行查询SQL
    result = await crud.async_execute_raw_sql(
        "SELECT COUNT(*) as user_count FROM users WHERE name LIKE :pattern",
        {"pattern": "%张%"}
    )
    
    # 执行更新SQL
    await crud.async_execute_raw_sql(
        "UPDATE users SET updated_at = NOW() WHERE created_at < :date",
        {"date": "2023-01-01"}
    )
```

## 性能优化建议

1. **连接池配置**：根据应用负载调整 `pool_size` 和 `max_overflow`
2. **批量操作**：使用 `chunk_size` 参数控制批量操作的大小
3. **会话管理**：合理使用 `async_get_session` 上下文管理器
4. **索引优化**：确保查询条件字段有适当的索引

## 错误处理

```python
from tk_db_utils import SchemaValidationError

async def error_handling_example():
    try:
        crud = AsyncBaseCurd()
        await crud.async_insert_one(User, {"invalid": "data"})
    except RuntimeError as e:
        print(f"数据库操作错误: {e}")
    except SchemaValidationError as e:
        print(f"模式验证错误: {e}")
    except Exception as e:
        print(f"未知错误: {e}")
```

## 注意事项

1. 异步功能需要在异步环境中运行（使用 `asyncio.run()` 或在异步函数中）
2. 确保安装了对应数据库的异步驱动
3. 异步和同步功能可以并存，但建议在同一个应用中保持一致
4. 异步操作的性能优势在高并发场景下更明显
5. 配置文件中的 `async_driver` 字段指定异步驱动类型

## 完整示例

```python
import asyncio
from sqlalchemy import Column, Integer, String
from tk_db_utils import (
    DbOrmBaseMixedIn,
    AsyncBaseCurd,
    async_init_db,
    configure_async_database
)

# 定义模型
class User(DbOrmBaseMixedIn):
    __tablename__ = 'async_users'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    email = Column(String(100), unique=True)

async def main():
    # 配置数据库
    await configure_async_database(
        host="localhost",
        username="root",
        password="password",
        database="test_db",
        driver="aiomysql"
    )
    
    # 初始化表
    await async_init_db()
    
    # 创建CRUD实例
    crud = AsyncBaseCurd()
    
    # 执行操作
    user_data = {"name": "异步用户", "email": "async@example.com"}
    user_id = await crud.async_insert_one(User, user_data)
    print(f"创建用户ID: {user_id}")
    
    # 查询用户
    user = await crud.async_select_by_id(User, user_id)
    print(f"查询到用户: {user.name if user else 'None'}")

if __name__ == "__main__":
    asyncio.run(main())
```

这个异步功能模块为 `tk_db_utils` 提供了完整的异步数据库操作能力，适用于需要高并发数据库访问的现代Python应用。