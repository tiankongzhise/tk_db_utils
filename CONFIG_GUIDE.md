# 数据库配置指南

本项目采用分离式配置管理，将敏感信息和引擎参数分别存储在不同的文件中，以提高安全性和可维护性。

## 配置文件结构

### 1. 敏感信息配置 (`.env`)

包含数据库连接的敏感信息，如主机地址、用户名、密码等。这些信息不应提交到版本控制系统。

```env
# 数据库敏感信息配置
DB_HOST=localhost
DB_PORT=3306
DB_USERNAME=your_username
DB_PASSWORD=your_password
```

### 2. 引擎参数配置 (`db_config.toml`)

包含数据库引擎的非敏感配置参数，如连接池大小、超时设置等。这些配置可以提交到版本控制系统。

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

[connection]
default_port = 3306
connection_timeout = 30
read_timeout = 30
write_timeout = 30
```

## 快速开始

### 1. 设置敏感信息

复制 `.env.example` 为 `.env` 并填入真实的数据库连接信息：

```bash
cp .env.example .env
```

编辑 `.env` 文件：
```env
DB_HOST=your_database_host
DB_PORT=3306
DB_USERNAME=your_username
DB_PASSWORD=your_password
```

### 2. 设置引擎配置

复制 `db_config.example.toml` 为 `db_config.toml` 并根据需要调整配置：

```bash
cp db_config.example.toml db_config.toml
```

编辑 `db_config.toml` 文件，调整数据库名称和引擎参数。

### 3. 使用数据库

```python
from tk_db_utils import init_db, get_session

# 初始化数据库
init_db()

# 使用数据库会话
with get_session() as session:
    # 执行数据库操作
    pass
```

## 高级配置

### 动态配置

可以通过 `configure_database` 函数动态配置数据库连接：

```python
from tk_db_utils import configure_database

# 使用自定义配置文件
engine = configure_database(
    config_file="/path/to/custom_config.toml",
    host="custom_host",
    database="custom_db"
)
```

### 多环境配置

可以为不同环境创建不同的配置文件：

```
db_config.dev.toml    # 开发环境
db_config.test.toml   # 测试环境
db_config.prod.toml   # 生产环境
```

然后在代码中根据环境变量选择配置文件：

```python
import os
from tk_db_utils import configure_database

env = os.getenv("APP_ENV", "dev")
config_file = f"db_config.{env}.toml"

engine = configure_database(config_file=config_file)
```

## 支持的数据库

### MySQL
```toml
[database]
database = "myapp_db"
driver = "pymysql"
dialect = "mysql"
charset = "utf8mb4"
collation = "utf8mb4_unicode_ci"
```

### PostgreSQL
```toml
[database]
database = "myapp_db"
driver = "psycopg2"
dialect = "postgresql"
charset = "utf8"
```

### SQLite
```toml
[database]
database = "./data/myapp.db"  # 使用文件路径
driver = "pysqlite"
dialect = "sqlite"
```

## 安全注意事项

1. **永远不要将 `.env` 文件提交到版本控制系统**
2. **确保 `db_config.toml` 中不包含敏感信息**
3. **在生产环境中使用强密码和安全的连接配置**
4. **定期轮换数据库密码**
5. **使用环境变量或密钥管理服务管理敏感信息**

## 迁移指南

如果你正在从旧版本迁移，请按以下步骤操作：

1. 备份现有的 `.env` 文件
2. 创建新的 `db_config.toml` 文件
3. 将引擎参数从 `.env` 移动到 `db_config.toml`
4. 更新 `.env` 文件，只保留敏感信息
5. 测试新配置是否正常工作

## 故障排除

### 配置文件未找到
如果看到 "配置文件不存在" 的警告，请确保：
- `db_config.toml` 文件存在于项目根目录
- 文件路径正确
- 文件权限允许读取

### 连接失败
如果数据库连接失败，请检查：
- `.env` 文件中的连接信息是否正确
- 数据库服务是否正在运行
- 网络连接是否正常
- 用户权限是否足够

### 配置加载错误
如果配置加载失败，请检查：
- TOML 文件语法是否正确
- 配置项名称是否拼写正确
- 数据类型是否匹配