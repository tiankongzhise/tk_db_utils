#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
模式验证功能使用示例

本示例展示如何使用 tk-db-utils 的模式验证功能来检查 ORM 模型与数据库表结构的一致性。
"""

from sqlalchemy import Column, Integer, String, DateTime, Boolean, Index
from datetime import datetime

from tk_db_utils import (
    configure_database,
    init_db,
    get_session,
    get_engine,
    SqlAlChemyBase,
    DbOrmBaseMixedIn,
    SchemaValidator,
    validate_schema_consistency,
    SchemaValidationError
)


# 定义示例模型
class User(SqlAlChemyBase, DbOrmBaseMixedIn):
    """用户表模型"""
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    
    # 添加索引
    __table_args__ = (
        Index('idx_username_email', 'username', 'email'),
    )


def main():
    """主函数"""
    
    # 1. 配置数据库连接
    print("配置数据库连接...")
    configure_database(
        host="localhost",
        port=3306,
        username="root",
        password="password",
        database="test_db",
        driver="pymysql",
        dialect="mysql"
    )
    
    # 2. 初始化数据库
    print("初始化数据库...")
    init_db()
    
    # 3. 获取引擎和会话
    engine = get_engine()
    
    # 4. 使用上下文管理器进行模式验证
    print("\n开始模式验证...")
    
    with get_session() as session:
        try:
            # 方法1: 使用便捷函数进行验证
            print("\n=== 方法1: 使用便捷函数 ===")
            is_valid = validate_schema_consistency(
                model=User,
                engine=engine,
                session=session,
                strict_mode=False,  # 非严格模式，不会抛出异常
                halt_on_error=True  # 发现错误时暂停等待用户确认
            )
            
            if is_valid:
                print("✅ 模式验证通过")
            else:
                print("❌ 模式验证失败")
            
            # 方法2: 使用 SchemaValidator 类进行详细验证
            print("\n=== 方法2: 使用 SchemaValidator 类 ===")
            validator = SchemaValidator(engine, session)
            
            result = validator.validate_model_schema(
                model=User,
                strict_mode=False
            )
            
            print(f"验证结果: {'通过' if result['valid'] else '失败'}")
            print(f"表是否存在: {result['table_exists']}")
            
            if not result['valid']:
                print("\n发现的问题:")
                for i, error in enumerate(result['errors'], 1):
                    print(f"  {i}. {error}")
                
                # 显示详细的结构信息
                print("\n=== ORM 模型结构 ===")
                orm_info = result['orm_info']
                print(f"表名: {orm_info['name']}")
                print("列信息:")
                for col_name, col_info in orm_info['columns'].items():
                    print(f"  - {col_name}: {col_info['type']} (nullable={col_info['nullable']}, pk={col_info['primary_key']})")
                
                print("\n=== 数据库表结构 ===")
                db_info = result['db_info']
                print(f"表名: {db_info['name']}")
                print("列信息:")
                for col_name, col_info in db_info['columns'].items():
                    print(f"  - {col_name}: {col_info['type']} (nullable={col_info['nullable']}, pk={col_info['primary_key']})")
            
        except SchemaValidationError as e:
            print(f"❌ 模式验证错误: {e}")
        except Exception as e:
            print(f"❌ 发生未预期的错误: {e}")


def example_with_strict_mode():
    """严格模式示例"""
    print("\n=== 严格模式示例 ===")
    
    engine = get_engine()
    
    with get_session() as session:
        try:
            # 严格模式会在发现不一致时抛出异常
            validate_schema_consistency(
                model=User,
                engine=engine,
                session=session,
                strict_mode=True,
                halt_on_error=False  # 不暂停，直接抛出异常
            )
            print("✅ 严格模式验证通过")
            
        except SchemaValidationError as e:
            print(f"❌ 严格模式验证失败: {e}")


if __name__ == "__main__":
    print("tk-db-utils 模式验证功能示例")
    print("=" * 50)
    
    try:
        main()
        example_with_strict_mode()
        
    except Exception as e:
        print(f"示例执行失败: {e}")
    
    print("\n示例执行完成")