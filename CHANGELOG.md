# 更新日志

## [0.1.0] - 2025-06-03

### 新增功能
- ✨ **模式验证功能**: 新增 `SchemaValidator` 类和 `validate_schema_consistency` 函数
  - 自动检查 ORM 模型与数据库表结构的一致性
  - 支持检查表名、列名、表结构、索引、默认值、字段类型
  - 提供严格模式和非严格模式
  - 发现不一致时可选择暂停流程等待用户确认
  - 详细的错误日志记录

- 🔧 **优化的工具函数**: 改进 `utils` 模块
  - 升级到 SQLAlchemy 2.0 风格的查询语法
  - 使用 `select()` 替代传统的 `session.query()`
  - 改进类型提示和代码风格
  - 增强的唯一约束检测功能

### 改进
- 📦 **打包配置优化**: 修复 PyPI 打包失败问题
  - 添加完整的 `build-system` 配置
  - 添加项目元数据：作者、许可证、关键词、分类器
  - 添加项目 URL 链接
  - 支持 Python 3.9+ (之前要求 3.11+)

- 🐛 **兼容性修复**:
  - 修复 Python 3.9 类型注解兼容性问题
  - 使用 `Optional[List[str]]` 替代 `list[str]|None` 语法
  - 改进导入语句和类型提示

### 技术改进
- 🔄 **SQLAlchemy 2.0 风格**: 全面采用现代化的 SQLAlchemy 2.0 API
  - 使用 `select()` 构建查询
  - 使用 `session.execute()` 执行查询
  - 使用 `result.scalars()` 获取结果

- 📝 **文档更新**:
  - 更新 README 文档，添加模式验证功能说明
  - 添加完整的使用示例
  - 创建示例代码文件

### 新增模块
- `schema_validator.py`: 模式验证核心功能
- `examples/schema_validation_example.py`: 使用示例

### API 变更
- 新增导出函数:
  - `SchemaValidator`
  - `SchemaValidationError`
  - `validate_schema_consistency`
  - `get_unique_constraints`
  - `filter_unique_conflicts`

### 依赖更新
- 保持现有依赖版本不变
- 确保与 SQLAlchemy 2.0.40+ 兼容
- 支持 Python 3.9-3.12