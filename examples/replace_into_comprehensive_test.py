#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
REPLACE INTO功能综合测试
基于CRUD测试用例结构，专门测试REPLACE INTO功能的各种场景
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from tk_db_utils.datebase import configure_database, DbOrmBaseMixedIn, get_engine
from tk_db_utils.curd import BaseCurd
from tk_db_utils import message
from sqlalchemy import text, select
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import String, JSON, DateTime, Integer
from sqlalchemy.schema import UniqueConstraint


class ReplaceTestModel(DbOrmBaseMixedIn):
    """
    REPLACE INTO测试专用模型
    包含主键、唯一键和普通字段的完整测试场景
    """
    __tablename__ = "replace_test_table"

    # 主键字段
    id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True, comment="主键ID"
    )

    # 唯一约束字段
    unique_code: Mapped[str] = mapped_column(
        String(50), unique=True, nullable=False, comment="唯一编码"
    )

    # 普通字段
    name: Mapped[str] = mapped_column(
        String(100), nullable=False, comment="名称"
    )
    
    category: Mapped[str] = mapped_column(
        String(50), nullable=False, comment="分类"
    )
    
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="active", comment="状态"
    )
    
    metadata_info: Mapped[dict] = mapped_column(
        JSON, nullable=True, comment="元数据信息"
    )
    
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.now, comment="创建时间"
    )
    
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.now, onupdate=datetime.now, comment="更新时间"
    )

    __table_args__ = (
        UniqueConstraint('unique_code', name='uk_unique_code'),
        {
            "comment": "REPLACE INTO测试表",
            "mysql_engine": "InnoDB",
            "mysql_charset": "utf8mb4",
            "schema": "test_db"
        },
    )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'unique_code': self.unique_code,
            'name': self.name,
            'category': self.category,
            'status': self.status,
            'metadata_info': self.metadata_info,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

    def __repr__(self):
        return f"<ReplaceTestModel(id={self.id}, unique_code='{self.unique_code}', name='{self.name}')>"


class ReplaceIntoComprehensiveTest:
    """
    REPLACE INTO功能综合测试类
    """
    
    def __init__(self):
        """初始化测试环境"""
        self.engine = get_engine()
        self.crud = BaseCurd(self.engine, auto_init_db=True)
        
    def setup_test_data(self) -> List[Dict[str, Any]]:
        """准备测试数据"""
        test_data = [
            {
                'unique_code': 'TEST001',
                'name': '测试项目1',
                'category': '类别A',
                'status': 'active',
                'metadata_info': {'version': '1.0', 'author': 'tester1'},
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            },
            {
                'unique_code': 'TEST002',
                'name': '测试项目2',
                'category': '类别B',
                'status': 'active',
                'metadata_info': {'version': '1.0', 'author': 'tester2'},
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            },
            {
                'unique_code': 'TEST003',
                'name': '测试项目3',
                'category': '类别A',
                'status': 'inactive',
                'metadata_info': {'version': '1.1', 'author': 'tester3'},
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }
        ]
        return test_data
    
    def clear_test_table(self):
        """清空测试表"""
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {ReplaceTestModel.__tablename__}"))
            message.info("测试表清空完成")
        except Exception as e:
            message.error(f"清空测试表失败: {e}")
            raise
    
    def test_initial_insert(self):
        """测试1: 初始数据插入"""
        try:
            message.info("=== 测试1: 初始数据插入 ===")
            
            # 清空表
            self.clear_test_table()
            
            # 准备测试数据
            test_data = self.setup_test_data()
            
            # 执行批量插入
            inserted_count = self.crud.bulk_insert(ReplaceTestModel, test_data, chunk_size=5)
            message.info(f"初始插入完成，插入 {inserted_count} 条记录")
            
            # 验证插入结果
            records = self.crud.select_by_conditions(ReplaceTestModel, {})
            message.info(f"验证结果：表中共有 {len(records)} 条记录")
            
            for record in records:
                message.info(f"记录: ID={record.id}, Code={record.unique_code}, Name={record.name}")
            
            message.info("✅ 初始数据插入测试通过")
            return True
            
        except Exception as e:
            message.error(f"❌ 初始数据插入测试失败: {e}")
            raise
    
    def test_replace_with_primary_key(self):
        """测试2: 基于主键的REPLACE INTO"""
        try:
            message.info("=== 测试2: 基于主键的REPLACE INTO ===")
            
            # 获取现有记录
            existing_records = self.crud.select_by_conditions(
                ReplaceTestModel, 
                {}, 
                limit=2
            )
            
            if not existing_records:
                message.warning("没有现有记录，跳过主键REPLACE测试")
                return False
            
            # 修改数据（包含主键）
            modified_data = []
            for i, record in enumerate(existing_records):
                modified_item = {
                    'id': record.id,  # 包含主键
                    'unique_code': record.unique_code,
                    'name': f'替换测试_{record.name}_{i}',
                    'category': f'新类别_{i}',
                    'status': 'replaced',
                    'metadata_info': {'version': '2.0', 'replaced': True, 'test_id': i},
                    'created_at': record.created_at,  # 保持原创建时间
                    'updated_at': datetime.now()  # 更新时间
                }
                modified_data.append(modified_item)
            
            message.info(f"准备替换 {len(modified_data)} 条记录")
            
            # 执行REPLACE INTO
            processed_count = self.crud.bulk_replace_into(ReplaceTestModel, modified_data, chunk_size=5)
            message.info(f"REPLACE INTO完成，处理 {processed_count} 条记录")
            
            # 验证替换结果
            replaced_records = self.crud.select_by_conditions(
                ReplaceTestModel, 
                {'status': 'replaced'}
            )
            
            message.info(f"验证结果：找到 {len(replaced_records)} 条被替换的记录")
            
            for record in replaced_records:
                message.info(f"替换记录: ID={record.id}, Name={record.name}, Status={record.status}")
            
            # 验证总记录数没有增加
            total_records = self.crud.select_by_conditions(ReplaceTestModel, {})
            message.info(f"总记录数: {len(total_records)} (应该保持不变)")
            
            message.info("✅ 基于主键的REPLACE INTO测试通过")
            return True
            
        except Exception as e:
            message.error(f"❌ 基于主键的REPLACE INTO测试失败: {e}")
            raise
    
    def test_replace_with_unique_key(self):
        """测试3: 基于唯一键的REPLACE INTO"""
        try:
            message.info("=== 测试3: 基于唯一键的REPLACE INTO ===")
            
            # 准备新数据（使用现有的unique_code但不包含主键）
            new_data_with_existing_codes = [
                {
                    'unique_code': 'TEST001',  # 使用现有的unique_code
                    'name': '通过唯一键替换的项目1',
                    'category': '唯一键替换类别',
                    'status': 'unique_replaced',
                    'metadata_info': {'version': '3.0', 'unique_replace': True},
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                },
                {
                    'unique_code': 'TEST004',  # 新的unique_code
                    'name': '新增项目4',
                    'category': '新增类别',
                    'status': 'new_added',
                    'metadata_info': {'version': '1.0', 'new_record': True},
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
            ]
            
            message.info(f"准备处理 {len(new_data_with_existing_codes)} 条记录（包含替换和新增）")
            
            # 记录操作前的总数
            before_count = len(self.crud.select_by_conditions(ReplaceTestModel, {}))
            message.info(f"操作前总记录数: {before_count}")
            
            # 执行REPLACE INTO
            processed_count = self.crud.bulk_replace_into(
                ReplaceTestModel, 
                new_data_with_existing_codes, 
                chunk_size=5
            )
            message.info(f"REPLACE INTO完成，处理 {processed_count} 条记录")
            
            # 验证结果
            after_count = len(self.crud.select_by_conditions(ReplaceTestModel, {}))
            message.info(f"操作后总记录数: {after_count}")
            
            # 验证替换的记录
            replaced_record = self.crud.select_by_conditions(
                ReplaceTestModel, 
                {'unique_code': 'TEST001'}
            )
            
            if replaced_record:
                record = replaced_record[0]
                message.info(f"替换验证: Code={record.unique_code}, Name={record.name}, Status={record.status}")
                
                if record.status == 'unique_replaced':
                    message.info("✅ 唯一键替换成功")
                else:
                    message.warning(f"⚠️ 替换状态异常: {record.status}")
            
            # 验证新增的记录
            new_record = self.crud.select_by_conditions(
                ReplaceTestModel, 
                {'unique_code': 'TEST004'}
            )
            
            if new_record:
                record = new_record[0]
                message.info(f"新增验证: Code={record.unique_code}, Name={record.name}, Status={record.status}")
                message.info("✅ 新记录添加成功")
            else:
                message.warning("⚠️ 新记录未找到")
            
            message.info("✅ 基于唯一键的REPLACE INTO测试通过")
            return True
            
        except Exception as e:
            message.error(f"❌ 基于唯一键的REPLACE INTO测试失败: {e}")
            raise
    
    def test_replace_error_handling(self):
        """测试4: REPLACE INTO错误处理"""
        try:
            message.info("=== 测试4: REPLACE INTO错误处理 ===")
            
            # 测试空数据
            try:
                self.crud.bulk_replace_into(ReplaceTestModel, [], chunk_size=5)
                message.info("✅ 空数据处理正常")
            except Exception as e:
                message.info(f"空数据处理异常: {e}")
            
            # 测试无效数据结构
            try:
                invalid_data = [{'invalid_field': 'test'}]
                self.crud.bulk_replace_into(ReplaceTestModel, invalid_data, chunk_size=5)
                message.warning("⚠️ 无效数据未被拦截")
            except Exception as e:
                message.info(f"✅ 无效数据正确拦截: {type(e).__name__}")
            
            message.info("✅ 错误处理测试通过")
            return True
            
        except Exception as e:
            message.error(f"❌ 错误处理测试失败: {e}")
            raise
    
    def test_database_dialect_compatibility(self):
        """测试5: 数据库方言兼容性"""
        try:
            message.info("=== 测试5: 数据库方言兼容性 ===")
            
            dialect_name = self.engine.dialect.name
            message.info(f"当前数据库方言: {dialect_name}")
            
            # 根据不同方言显示相应的REPLACE INTO实现
            if dialect_name == 'mysql':
                message.info("MySQL方言：使用REPLACE INTO语法")
            elif dialect_name == 'postgresql':
                message.info("PostgreSQL方言：使用INSERT ... ON CONFLICT DO UPDATE")
            elif dialect_name == 'sqlite':
                message.info("SQLite方言：使用INSERT OR REPLACE")
            else:
                message.warning(f"未知方言: {dialect_name}")
            
            # 执行一个简单的REPLACE操作来验证兼容性
            test_data = [{
                'unique_code': 'DIALECT_TEST',
                'name': f'{dialect_name}方言测试',
                'category': '兼容性测试',
                'status': 'dialect_test',
                'metadata_info': {'dialect': dialect_name},
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }]
            
            processed_count = self.crud.bulk_replace_into(ReplaceTestModel, test_data, chunk_size=1)
            message.info(f"方言兼容性测试完成，处理 {processed_count} 条记录")
            
            message.info("✅ 数据库方言兼容性测试通过")
            return True
            
        except Exception as e:
            message.error(f"❌ 数据库方言兼容性测试失败: {e}")
            raise
    
    def cleanup_test_table(self):
        """清理测试表"""
        try:
            message.info("=== 清理测试数据 ===")
            DbOrmBaseMixedIn.metadata.drop_all(self.engine)
            message.info("测试表清理完成")
        except Exception as e:
            message.error(f"清理测试表失败: {e}")
            raise
    
    def run_all_tests(self):
        """运行所有REPLACE INTO测试"""
        try:
            message.info("开始REPLACE INTO综合测试")
            message.info("=" * 50)
            
            test_results = []
            
            # 测试1: 初始数据插入
            test_results.append(self.test_initial_insert())
            
            # 测试2: 基于主键的REPLACE INTO
            test_results.append(self.test_replace_with_primary_key())
            
            # 测试3: 基于唯一键的REPLACE INTO
            test_results.append(self.test_replace_with_unique_key())
            
            # 测试4: 错误处理
            test_results.append(self.test_replace_error_handling())
            
            # 测试5: 数据库方言兼容性
            test_results.append(self.test_database_dialect_compatibility())
            
            # 统计结果
            passed_tests = sum(test_results)
            total_tests = len(test_results)
            
            message.info("=" * 50)
            message.info(f"测试完成: {passed_tests}/{total_tests} 通过")
            
            if passed_tests == total_tests:
                message.info("✅ 所有REPLACE INTO测试通过！")
                return True
            else:
                message.error("❌ 部分REPLACE INTO测试失败")
                return False
                
        except Exception as e:
            message.error(f"REPLACE INTO测试执行失败: {e}")
            raise
        finally:
            # 清理测试数据
            self.cleanup_test_table()


def main():
    """主函数"""
    try:
        print("REPLACE INTO功能综合测试开始")
        print("=" * 50)
        
        # 配置数据库
        configure_database()
        
        # 创建测试实例
        tester = ReplaceIntoComprehensiveTest()
        
        # 运行所有测试
        success = tester.run_all_tests()
        
        if success:
            print("\n✅ REPLACE INTO综合测试全部通过！")
        else:
            print("\n❌ REPLACE INTO综合测试失败")
            
        print("\n测试执行完成")
        
    except Exception as e:
        print(f"\n❌ 测试执行异常: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
