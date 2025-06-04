#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
模式验证功能使用示例

本示例展示如何使用 tk-db-utils 的模式验证功能来检查 ORM 模型与数据库表结构的一致性。
"""

from sqlalchemy import Integer, String, DateTime, Boolean, Index
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import JSON, UniqueConstraint
from typing import Optional
from datetime import datetime

from tk_db_utils import (
    configure_database,
    init_db,
    get_session,
    get_engine,
    DbOrmBaseMixedIn,
    SchemaValidator,
    validate_schema_consistency,
    SchemaValidationError,
)


# 定义示例模型
class SchoolObject(DbOrmBaseMixedIn):
    __tablename__ = "school_object"

    # 主键字段
    key_id: Mapped[int] = mapped_column(
        primary_key=True, autoincrement=True, comment="主键"
    )

    # 唯一约束字段
    school_name: Mapped[str] = mapped_column(
        String(255), unique=True, nullable=False, comment="学校名称"
    )

    # 普通字符串字段
    school_type: Mapped[str] = mapped_column(
        String(255), nullable=False, comment="学校性质"
    )
    school_level: Mapped[str] = mapped_column(
        String(255), nullable=False, comment="办学层次"
    )
    is_sfx_school: Mapped[str] = mapped_column(
        String(255), nullable=False, comment="示范性院校"
    )
    is_gz_school: Mapped[str] = mapped_column(
        String(255), nullable=False, comment="骨干院校"
    )
    is_zy_school: Mapped[str] = mapped_column(
        String(255), nullable=False, comment="卓越院校"
    )
    is_cy_school: Mapped[str] = mapped_column(
        String(255), nullable=False, comment="楚怡高水平院校"
    )

    # JSON 类型字段
    school_address: Mapped[dict] = mapped_column(
        JSON, nullable=False, comment="办学地点"
    )
    major: Mapped[list] = mapped_column(JSON, nullable=False, comment="优势专业")

    # 时间戳字段
    create_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, default=datetime.now, comment="创建时间"
    )
    update_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime, default=datetime.now, onupdate=datetime.now, comment="更新时间"
    )

    # 表级配置
    __table_args__ = (
        UniqueConstraint("school_name", name="unique_school_name"),
        {
            "comment": "学校信息表",
            "mysql_engine": "InnoDB",
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_0900_ai_ci",
            "schema": "test_db",
        },
    )


def main():
    """主函数"""

    # 1. 配置数据库连接
    print("配置数据库连接...")
    configure_database(
        # host="localhost",
        # port=3306,
        # username="root",
        # password="password",
        # database="test_db",
        # driver="pymysql",
        # dialect="mysql"
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
                model=SchoolObject,
                engine=engine,
                session=session,
                strict_mode=False,  # 非严格模式，不会抛出异常
                halt_on_error=True,  # 发现错误时暂停等待用户确认
            )

            if is_valid:
                print("✅ 模式验证通过")
            else:
                print("❌ 模式验证失败")

            # 方法2: 使用 SchemaValidator 类进行详细验证
            print("\n=== 方法2: 使用 SchemaValidator 类 ===")
            validator = SchemaValidator(engine, session)

            result = validator.validate_model_schema(model=SchoolObject, strict_mode=False)

            print(f"验证结果: {'通过' if result['valid'] else '失败'}")
            print(f"表是否存在: {result['table_exists']}")

            if not result["valid"]:
                print("\n发现的问题:")
                for i, error in enumerate(result["errors"], 1):
                    print(f"  {i}. {error}")

                # 显示详细的结构信息
                print("\n=== ORM 模型结构 ===")
                orm_info = result["orm_info"]
                print(f"表名: {orm_info['name']}")
                print("列信息:")
                for col_name, col_info in orm_info["columns"].items():
                    print(
                        f"  - {col_name}: {col_info['type']} (nullable={col_info['nullable']}, pk={col_info['primary_key']})"
                    )

                print("\n=== 数据库表结构 ===")
                db_info = result["db_info"]
                print(f"表名: {db_info['name']}")
                print("列信息:")
                for col_name, col_info in db_info["columns"].items():
                    print(
                        f"  - {col_name}: {col_info['type']} (nullable={col_info['nullable']}, pk={col_info['primary_key']})"
                    )

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
                model=SchoolObject,
                engine=engine,
                session=session,
                strict_mode=True,
                halt_on_error=False,  # 不暂停，直接抛出异常
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
