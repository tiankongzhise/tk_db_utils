# tk-db-utils

一个基于 SQLAlchemy 2.0 的高可复用数据库工具库，提供完整的 CRUD 操作、批量数据处理和高级数据库功能。

## 特性

### 🚀 核心功能
- **SQLAlchemy 2.0 风格**: 完全采用 SQLAlchemy 2.0 的现代化 API
- **高可复用性**: 模块化设计，易于集成到任何项目中
- **完整的 CRUD 操作**: 支持增删改查的所有基本操作
- **批量数据处理**: 支持大批量数据的高效处理
- **多数据库支持**: 支持 MySQL、PostgreSQL、SQLite 等主流数据库

### 📊 高级数据库功能
- **INSERT IGNORE**: 支持忽略重复数据的批量插入
- **REPLACE INTO**: 支持数据替换的批量操作
- **分块处理**: 可配置批量大小，避免内存溢出
- **事务管理**: 自动事务处理，确保数据一致性
- **连接池管理**: 高效的数据库连接池管理
- **模式验证**: 自动检查 ORM 模型与数据库表结构的一致性
- **冲突检测**: 智能检测和处理唯一约束冲突

### 🛠️ 开发友好
- **类型提示**: 完整的 TypeScript 风格类型提示
- **错误处理**: 详细的错误信息和异常处理
- **日志系统**: 可配置的日志记录系统
- **向后兼容**: 保持与旧版本的兼容性

## 安装

```bash
pip install tk-db-utils
```

## 快速开始

### 1. 安装

```bash
pip install tk-db-utils
```

### 2. 配置数据库连接

本项目采用分离式配置管理，将敏感信息和引擎参数分别存储：

#### 敏感信息配置 (`.env`)
创建 `.env` 文件并配置数据库连接的敏感信息：

```env
# 数据库敏感信息配置
DB_HOST=localhost
DB_PORT=3306
DB_USERNAME=your_username
DB_PASSWORD=your_password
```

#### 引擎参数配置 (`db_config.toml`)
创建 `db_config.toml` 文件并配置数据库引擎参数：

```toml
[database]
database = "your_database_name"
driver = "pymysql"
dialect = "mysql"
charset = "utf8mb4"
collation = "utf8mb4_general_ci"

[engine]
echo = false
pool_size = 5
max_overflow = 10
pool_timeout = 30
pool_recycle = 3600
pool_pre_ping = true
```

> 💡 **提示**: 可以复制 `.env.example` 和 `db_config.example.toml` 文件作为模板开始配置。
> 
> 📖 **详细配置指南**: 查看 [CONFIG_GUIDE.md](CONFIG_GUIDE.md) 了解完整的配置说明。

### 3. 基础配置

```python
from tk_db_utils import (
    configure_database,
    init_db,
    set_logger_level,
    SqlAlChemyBase,
    DbOrmBaseMixedIn,
    BaseCurd
)
from sqlalchemy import Column, Integer, String, DateTime
from datetime import datetime

# 配置日志级别
set_logger_level('INFO')

# 配置数据库连接
configure_database(
    host="localhost",
    port=3306,
    username="your_username",
    password="your_password",
    database="your_database",
    driver="mysql",
    dialect="mysql+pymysql",
    # 引擎参数
    echo=True,
    pool_size=5,
    max_overflow=10
)

# 初始化数据库
init_db()
```

### 2. 定义数据模型

```python
class User(SqlAlChemyBase, DbOrmBaseMixedIn):
    """用户表模型"""
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
```

### 3. 基础 CRUD 操作

```python
# 创建 CRUD 实例
crud = BaseCurd()

# 插入单条记录
user_data = {"username": "john", "email": "john@example.com"}
user_id = crud.insert_one(User, user_data)

# 批量插入
users_data = [
    {"username": "alice", "email": "alice@example.com"},
    {"username": "bob", "email": "bob@example.com"},
]
inserted_count = crud.bulk_insert(User, users_data, chunk_size=1000)

# 查询所有记录
all_users = crud.select_all(User, limit=10, offset=0)

# 根据ID查询
user = crud.select_by_id(User, 1)

# 根据条件查询
users = crud.select_by_conditions(User, {"username": "alice"})

# 更新记录
updated_count = crud.update_by_id(User, 1, {"email": "new@example.com"})

# 删除记录
deleted_count = crud.delete_by_id(User, 1)

# 统计记录数
total_count = crud.count(User)
```

## 高级功能

### 模式验证功能

```python
from tk_db_utils import (
    SchemaValidator,
    validate_schema_consistency,
    SchemaValidationError
)

# 方法1: 使用便捷函数进行验证
with get_session() as session:
    try:
        is_valid = validate_schema_consistency(
            model=User,
            engine=get_engine(),
            session=session,
            strict_mode=False,  # 非严格模式
            halt_on_error=True  # 发现错误时暂停等待用户确认
        )
        
        if is_valid:
            print("✅ 模式验证通过")
        else:
            print("❌ 模式验证失败")
            
    except SchemaValidationError as e:
        print(f"模式验证错误: {e}")

# 方法2: 使用 SchemaValidator 类进行详细验证
with get_session() as session:
    validator = SchemaValidator(get_engine(), session)
    
    result = validator.validate_model_schema(
        model=User,
        strict_mode=False
    )
    
    if not result['valid']:
        print("发现的问题:")
        for error in result['errors']:
            print(f"  - {error}")
```

### INSERT IGNORE 批量操作

```python
# 批量插入，忽略重复数据
duplicate_users = [
    {"username": "alice", "email": "alice@example.com"},  # 可能重复
    {"username": "david", "email": "david@example.com"},  # 新数据
]

# 使用 INSERT IGNORE，重复数据会被忽略
inserted_count = crud.bulk_insert_ignore(User, duplicate_users, chunk_size=1000)
print(f"实际插入了 {inserted_count} 条记录")
```

### REPLACE INTO 批量操作

```python
# 批量替换数据
replace_users = [
    {"id": 1, "username": "john_updated", "email": "john.new@example.com"},
    {"id": 100, "username": "new_user", "email": "new@example.com"},  # 新记录
]

# 使用 REPLACE INTO，存在则更新，不存在则插入
processed_count = crud.bulk_replace_into(User, replace_users, chunk_size=1000)
print(f"处理了 {processed_count} 条记录")
```

### 会话管理

```python
from tk_db_utils import get_session

# 使用上下文管理器
with get_session() as session:
    # 在会话中进行复杂查询
    users = session.query(User).filter(User.username.like('%admin%')).all()
    
    # 创建新记录
    new_user = User(username="admin", email="admin@example.com")
    session.add(new_user)
    # 会话结束时自动提交
```

### 原生 SQL 执行

```python
# 执行原生 SQL
result = crud.execute_raw_sql(
    "SELECT COUNT(*) as total FROM users WHERE created_at > :date",
    params={"date": "2024-01-01"}
)
```

## 数据库支持

### MySQL
```python
configure_database(
    host="localhost",
    port=3306,
    username="root",
    password="password",
    database="mydb",
    driver="mysql",
    dialect="mysql+pymysql"
)
```

### PostgreSQL
```python
configure_database(
    host="localhost",
    port=5432,
    username="postgres",
    password="password",
    database="mydb",
    driver="postgresql",
    dialect="postgresql+psycopg2"
)
```

### SQLite
```python
configure_database(
    host="",
    port=0,
    username="",
    password="",
    database="mydb.db",
    driver="sqlite",
    dialect="sqlite"
)
```

## 环境变量配置

你也可以通过环境变量来配置数据库连接：

```bash
# 数据库连接配置
export DB_HOST=localhost
export DB_PORT=3306
export DB_USERNAME=root
export DB_PASSWORD=password
export DB_DATABASE=mydb
export DB_DRIVER=mysql
export DB_DIALECT=mysql+pymysql

# 引擎参数配置
export DB_ECHO=true
export DB_POOL_SIZE=5
export DB_MAX_OVERFLOW=10
export DB_POOL_TIMEOUT=30
export DB_POOL_RECYCLE=3600
```

## 日志配置

```python
from tk_db_tool import set_logger_level, set_message_handler, set_message_config
import logging

# 设置日志级别
set_logger_level('DEBUG')

# 设置自定义日志处理器
handler = logging.FileHandler('app.log')
set_message_handler(handler)

# 设置日志配置
logger = logging.getLogger('my_app')
set_message_config(logger)
```

## 性能优化

### 批量操作建议

```python
# 对于大量数据，建议使用适当的批量大小
# 一般建议 1000-5000 条记录为一批

# 小数据量
crud.bulk_insert(User, small_data, chunk_size=1000)

# 大数据量
crud.bulk_insert(User, large_data, chunk_size=3000)

# 超大数据量
crud.bulk_insert(User, huge_data, chunk_size=5000)
```

### 连接池配置

```python
configure_database(
    # ... 其他配置
    pool_size=10,        # 连接池大小
    max_overflow=20,     # 最大溢出连接数
    pool_timeout=30,     # 获取连接超时时间
    pool_recycle=3600,   # 连接回收时间
)
```

## 错误处理

```python
try:
    crud.bulk_insert(User, invalid_data)
except ValueError as e:
    print(f"数据验证错误: {e}")
except RuntimeError as e:
    print(f"数据库操作错误: {e}")
except Exception as e:
    print(f"未知错误: {e}")
```

## 迁移指南

### 从旧版本迁移

如果你正在使用旧版本的 `bulk_insert_ignore_in_chunks` 方法：

```python
# 旧方法（仍然支持，但会显示警告）
crud.bulk_insert_ignore_in_chunks(User, data, chunk_size=1000)

# 新方法（推荐）
crud.bulk_insert_ignore(User, data, chunk_size=1000)
```

## 完整示例

查看 `example.py` 文件获取完整的使用示例，包括：
- 数据库配置
- 模型定义
- CRUD 操作
- INSERT IGNORE 和 REPLACE INTO
- 会话管理
- 错误处理

## 依赖要求

- Python >= 3.11
- SQLAlchemy >= 2.0
- PyMySQL (用于 MySQL)
- psycopg2 (用于 PostgreSQL)
- python-dotenv (用于环境变量)
- pydantic (用于数据验证)

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！

## 更新日志

### v0.1.2
- ✨ 重构为 SQLAlchemy 2.0 风格
- ✨ 新增 INSERT IGNORE 和 REPLACE INTO 支持
- ✨ 新增完整的 CRUD 操作方法
- ✨ 新增灵活的数据库配置系统
- ✨ 修复 message 模块在 pip 安装后的使用问题
- ✨ 新增批量操作的分块处理
- ✨ 新增多数据库支持
- ✨ 新增完整的类型提示
- ✨ 新增详细的错误处理和日志记录
