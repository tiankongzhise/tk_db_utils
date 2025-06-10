#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CRUD模块测试案例
从education.school_object表获取数据并写入SchoolObject模型，测试CRUD模块各项功能
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from tk_db_utils.datebase import configure_database, DbOrmBaseMixedIn,get_engine
from tk_db_utils.curd import BaseCurd
from tk_db_utils import message
from sqlalchemy import text, select,func
from sqlalchemy.orm import  Mapped, mapped_column
from sqlalchemy.types import String, JSON, DateTime, Integer
from sqlalchemy.schema import UniqueConstraint



class SchoolObject(DbOrmBaseMixedIn):
    """
    学校信息表模型
    """
    __tablename__ = "school_object"

    # 主键字段
    key_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True, comment="主键"
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
    create_at: Mapped[datetime] = mapped_column(
        DateTime, default=func.now(), comment="创建时间"
    )
    update_at: Mapped[datetime] = mapped_column(
        DateTime, onupdate=func.now(),nullable=True, comment="更新时间"
    )

    # 表级配置
    __table_args__ = (
        UniqueConstraint("school_name", name="unique_school_name"),
        {
            "comment": "学校信息表",
            "mysql_engine": "InnoDB",
            "mysql_charset": "utf8mb4",
            "schema": "test_db"
        },
    )


    def __repr__(self):
        return f"<SchoolObject(key_id={self.key_id}, school_name='{self.school_name}')>"


class CurdTestCase:
    """
    CRUD模块测试案例
    """
    
    def __init__(self):

        self.engine = get_engine()
        self.crud = BaseCurd(self.engine,auto_init_db=True)
        
    
    def fetch_source_data(self) -> List[Dict[str, Any]]:
        """从education.school_object表获取源数据"""
        try:
            with self.engine.begin() as conn:
                # 查询源数据
                sql = """
                SELECT 
                    school_name,
                    school_type,
                    school_level,
                    is_sfx_school,
                    is_gz_school,
                    is_zy_school,
                    is_cy_school,
                    school_address,
                    major
                FROM education.school_object
                LIMIT 10
                """
                
                result = conn.execute(text(sql))
                rows = result.fetchall()
                
                # 转换为字典列表
                data = []
                for row in rows:
                    data.append({
                        'school_name': row.school_name,
                        'school_type': row.school_type,
                        'school_level': row.school_level,
                        'is_sfx_school': row.is_sfx_school,
                        'is_gz_school': row.is_gz_school,
                        'is_zy_school': row.is_zy_school,
                        'is_cy_school': row.is_cy_school,
                        'school_address': row.school_address if row.school_address else {},
                        'major': row.major if row.major else [],
                    })
                
                message.info(f"从源表获取到 {len(data)} 条数据")
                return data
                
        except Exception as e:
            message.error(f"获取源数据失败: {e}")
            raise
    
    def test_bulk_insert(self, data: List[Dict[str, Any]]):
        """测试批量插入功能"""
        try:
            message.info("=== 测试批量插入功能 ===")
            
            # 清空测试表
            self.crud.execute_raw_sql("DELETE FROM school_object")
            
            # 批量插入
            inserted_count = self.crud.bulk_insert(SchoolObject, data, chunk_size=5)
            message.info(f"批量插入完成，插入 {inserted_count} 条记录")
            
            # 验证插入结果
            total_count = self.crud.count(SchoolObject)
            assert total_count == len(data), f"插入数据数量不匹配: 期望 {len(data)}, 实际 {total_count}"
            message.info("批量插入测试通过")
            
        except Exception as e:
            message.error(f"批量插入测试失败: {e}")
            raise
    
    def test_bulk_insert_ignore(self, data: List[Dict[str, Any]]):
        """测试批量INSERT IGNORE功能"""
        try:
            message.info("=== 测试批量INSERT IGNORE功能 ===")
            
            # 再次插入相同数据（应该被忽略）
            inserted_count = self.crud.bulk_insert_ignore(SchoolObject, data, chunk_size=5)
            message.info(f"INSERT IGNORE完成，插入 {inserted_count} 条记录")
            
            # 验证数据没有重复
            total_count = self.crud.count(SchoolObject)
            assert total_count == len(data), f"数据重复插入: 期望 {len(data)}, 实际 {total_count}"
            message.info("批量INSERT IGNORE测试通过")
            
        except Exception as e:
            message.error(f"批量INSERT IGNORE测试失败: {e}")
            raise
    
    def test_select_operations(self):
        """测试查询操作"""
        try:
            message.info("=== 测试查询操作 ===")
            
            # 测试根据ID查询
            record = self.crud.select_by_id(SchoolObject, 1)
            if record:
                message.info(f"根据ID查询成功: {record.school_name}")
            
            # 测试根据条件查询
            records = self.crud.select_by_conditions(
                SchoolObject, 
                {'school_type': '公办'}, 
                limit=3
            )
            message.info(f"根据条件查询到 {len(records)} 条记录")
            
            # 测试统计功能
            total_count = self.crud.count(SchoolObject)
            public_count = self.crud.count(SchoolObject, {'school_type': '公办'})
            message.info(f"总记录数: {total_count}, 公办学校数: {public_count}")
            
            message.info("查询操作测试通过")
            
        except Exception as e:
            message.error(f"查询操作测试失败: {e}")
            raise
    
    def test_update_operations(self):
        """测试更新操作"""
        try:
            message.info("=== 测试更新操作 ===")
            
            # 测试根据ID更新
            update_data = {'school_type': '测试更新'}
            updated_count = self.crud.update_by_id(SchoolObject, 1, update_data)
            message.info(f"根据ID更新 {updated_count} 条记录")
            
            # 验证更新结果
            record = self.crud.select_by_id(SchoolObject, 1)
            if record and record.school_type == '测试更新':
                message.info("ID更新验证成功")
            
            # 测试根据条件更新
            update_data = {'school_level': '测试层次'}
            updated_count = self.crud.update_by_conditions(
                SchoolObject, 
                {'school_type': '测试更新'}, 
                update_data
            )
            message.info(f"根据条件更新 {updated_count} 条记录")
            
            message.info("更新操作测试通过")
            
        except Exception as e:
            message.error(f"更新操作测试失败: {e}")
            raise
    
    def test_delete_operations(self):
        """测试删除操作"""
        try:
            message.info("=== 测试删除操作 ===")
            
            # 获取删除前的总数
            before_count = self.crud.count(SchoolObject)
            
            # 测试根据条件删除
            deleted_count = self.crud.delete_by_conditions(
                SchoolObject, 
                {'school_type': '测试更新'}
            )
            message.info(f"根据条件删除 {deleted_count} 条记录")
            
            # 验证删除结果
            after_count = self.crud.count(SchoolObject)
            assert after_count == before_count - deleted_count, "删除数量不匹配"
            
            # 测试根据ID删除（如果还有记录的话）
            if after_count > 0:
                deleted_count = self.crud.delete_by_id(SchoolObject, 2)
                message.info(f"根据ID删除 {deleted_count} 条记录")
            
            message.info("删除操作测试通过")
            
        except Exception as e:
            message.error(f"删除操作测试失败: {e}")
            raise
    
    def test_bulk_replace_into(self, data: List[Dict[str, Any]]):
        """测试批量REPLACE INTO功能"""
        try:
            message.info("=== 测试批量REPLACE INTO功能 ===")
            
            # 先查询现有记录获取主键
            existing_records = self.crud.select_by_conditions(
                SchoolObject, 
                {}, 
                limit=3
            )
            
            if not existing_records:
                message.warning("没有现有记录，跳过REPLACE INTO测试")
                return
            
            # 修改部分数据，包含主键
            modified_data = []
            for i, record in enumerate(existing_records):
                modified_item = {
                    'key_id': record.key_id,  # 包含主键
                    'school_name': record.school_name,
                    'school_type': f'替换测试_{i}',
                    'school_level': record.school_level,
                    'is_sfx_school': record.is_sfx_school,
                    'is_gz_school': record.is_gz_school,
                    'is_zy_school': record.is_zy_school,
                    'is_cy_school': record.is_cy_school,
                    'school_address': record.school_address,
                    'major': record.major,
                    'create_at': record.create_at,
                    'update_at': datetime.now()
                }
                modified_data.append(modified_item)
            
            # 执行REPLACE INTO
            processed_count = self.crud.bulk_replace_into(SchoolObject, modified_data, chunk_size=5)
            message.info(f"REPLACE INTO完成，处理 {processed_count} 条记录")
            
            # 验证替换结果
            records = self.crud.select_by_conditions(
                SchoolObject, 
                {}, 
                limit=3
            )
            
            replaced_count = sum(1 for r in records if '替换测试' in r.school_type)
            message.info(f"验证到 {replaced_count} 条记录被替换")
            
            message.info("批量REPLACE INTO测试通过")
            
        except Exception as e:
            message.error(f"批量REPLACE INTO测试失败: {e}")
            raise
    
    def cleanup_test_table(self):
        """清理测试表"""
        try:
            # 删除测试表
            DbOrmBaseMixedIn.metadata.drop_all(self.engine)
            message.info("测试表清理完成")
        except Exception as e:
            message.error(f"清理测试表失败: {e}")
    
    def run_all_tests(self):
        """运行所有测试"""
        try:
            message.info("开始CRUD模块功能测试")
            
            # 1. 创建测试表
            # self.setup_test_table()
            
            # 2. 获取源数据
            source_data = self.fetch_source_data()
            
            if not source_data:
                message.warning("没有获取到源数据，跳过测试")
                return
            
            # 3. 测试批量插入
            self.test_bulk_insert(source_data)
            
            # 4. 测试INSERT IGNORE
            self.test_bulk_insert_ignore(source_data)
            
            # 5. 测试查询操作
            self.test_select_operations()
            
            # 6. 测试更新操作
            self.test_update_operations()
            
            # 7. 测试REPLACE INTO
            self.test_bulk_replace_into(source_data)
            
            # 8. 测试删除操作
            self.test_delete_operations()
            
            message.info("所有CRUD测试完成")
            
        except Exception as e:
            message.error(f"CRUD测试失败: {e}")
            raise
        finally:
            # 清理测试表
            self.cleanup_test_table()
            pass


def main():
    """主函数"""
    print("CRUD模块功能测试开始")
    print("=" * 50)
    
    try:
        # 创建测试实例
        test_case = CurdTestCase()
        
        # 运行所有测试
        test_case.run_all_tests()
        
        print("✅ CRUD模块测试全部通过！")
        
    except Exception as e:
        print(f"❌ 测试执行失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
    print("\n测试执行完成")
