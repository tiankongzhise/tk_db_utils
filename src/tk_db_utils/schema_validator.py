from datetime import datetime
from decimal import Decimal
from typing import Type, List, Dict, Union, Any, Optional, Set
from pydantic import BaseModel
from sqlalchemy import inspect, and_, or_, text, MetaData, Table, Column
from sqlalchemy.orm import Session
from sqlalchemy.sql.schema import UniqueConstraint, Index, ForeignKey
from sqlalchemy.engine import Engine
from collections import defaultdict
import logging

from .models import SqlAlChemyBase
from .message import message


class SchemaValidationError(Exception):
    """模式验证错误"""
    pass


class SchemaValidator:
    """ORM模型与数据库表结构一致性检查器 (SQLAlchemy 2.0风格)"""
    
    def __init__(self, engine: Engine, session: Session):
        """
        初始化模式验证器
        
        Args:
            engine: SQLAlchemy引擎
            session: SQLAlchemy会话
        """
        self.engine = engine
        self.session = session
        self.logger = logging.getLogger(__name__)
        
    def validate_model_schema(self, model: Type[SqlAlChemyBase], 
                            strict_mode: bool = True) -> Dict[str, Any]:
        """
        验证ORM模型与数据库表结构的一致性
        
        Args:
            model: SQLAlchemy ORM模型类
            strict_mode: 严格模式，如果为True则在发现不一致时抛出异常
            
        Returns:
            Dict包含验证结果和不一致的详细信息
            
        Raises:
            SchemaValidationError: 当strict_mode=True且发现不一致时
        """
        table_name = model.__tablename__
        db_scahema = self.session.get_bind().url.database
        # 检查表是否存在
        if not self._table_exists(model):
            error_msg = f"表 '{table_name}' 在数据库:{db_scahema}中不存在"
            message.error(error_msg)
            if strict_mode:
                raise SchemaValidationError(error_msg)
            return {
                'valid': False,
                'table_exists': False,
                'errors': [error_msg]
            }
        
        # 获取数据库表结构
        db_table_info = self._get_database_table_info(table_name)
        
        # 获取ORM模型结构
        orm_table_info = self._get_orm_table_info(model)
        
        # 比较结构
        validation_result = self._compare_table_structures(
            orm_table_info, db_table_info, table_name
        )
        
        # 记录验证结果
        if validation_result['valid']:
            message.info(f"表 '{table_name}' 结构验证通过")
        else:
            error_msg = f"表 '{table_name}' 结构验证失败，发现 {len(validation_result['errors'])} 个不一致项"
            message.error(error_msg)
            
            # 详细记录每个错误
            for error in validation_result['errors']:
                self.logger.error(f"Schema validation error: {error}")
            
            if strict_mode:
                raise SchemaValidationError(
                    f"{error_msg}\n详细错误:\n" + "\n".join(validation_result['errors'])
                )
        
        return validation_result
    
    def _table_exists(self, table_model: Type[SqlAlChemyBase]) -> bool:
        """检查表是否存在"""
        try:
            table_name = table_model.__tablename__
            table_schema = table_model.__table__.schema
            if table_schema:
                result = self.session.execute(
                    text("SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = :table_schema AND table_name = :table_name"),
                    {"table_schema":table_schema,"table_name": table_name}
                )
            else:
                result = self.session.execute(
                    text("SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = DATABASE() AND table_name = :table_name"),
                    {"table_name": table_name}
                )
            return result.fetchone() is not None
        except Exception as e:
            message.error(f"检查表存在性时出错: {str(e)}")
            return False
    
    def _get_database_table_info(self, table_name: str) -> Dict[str, Any]:
        """获取数据库中表的实际结构信息"""
        # 使用反射获取表结构
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=self.engine)
        
        # 获取唯一索引信息，用于判断列的唯一性
        unique_columns = set()
        for idx in table.indexes:
            if idx.unique and len(idx.columns) == 1:
                unique_columns.add(list(idx.columns)[0].name)
        
        # 提取列信息
        columns = {}
        for col in table.columns:
            # 检查列是否有唯一约束（通过唯一索引判断）
            is_unique = col.name in unique_columns or col.unique
            columns[col.name] = {
                'type': str(col.type),
                'nullable': col.nullable,
                'default': self._get_column_default(col),
                'primary_key': col.primary_key,
                'unique': is_unique,
                'autoincrement': getattr(col, 'autoincrement', False)
            }
        
        # 提取索引信息
        indexes = []
        for idx in table.indexes:
            indexes.append({
                'name': idx.name,
                'columns': [col.name for col in idx.columns],
                'unique': idx.unique
            })
        
        # 提取约束信息 - 直接查询数据库获取更准确的约束信息
        constraints = []
        try:
            # 查询唯一约束
            unique_constraints_query = text("""
                SELECT CONSTRAINT_NAME, COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = DATABASE() 
                AND TABLE_NAME = :table_name
                AND CONSTRAINT_NAME != 'PRIMARY'
                AND CONSTRAINT_NAME IN (
                    SELECT CONSTRAINT_NAME 
                    FROM information_schema.TABLE_CONSTRAINTS 
                    WHERE CONSTRAINT_TYPE = 'UNIQUE'
                    AND TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = :table_name
                )
                ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
            """)
            
            result = self.session.execute(unique_constraints_query, {"table_name": table_name})
            constraint_columns = {}
            
            for row in result:
                constraint_name = row[0]
                column_name = row[1]
                if constraint_name not in constraint_columns:
                    constraint_columns[constraint_name] = []
                constraint_columns[constraint_name].append(column_name)
            
            for constraint_name, columns_list in constraint_columns.items():
                constraints.append({
                    'type': 'unique',
                    'name': constraint_name,
                    'columns': columns_list
                })
                
        except Exception as e:
            # 如果查询失败，回退到反射机制
            for constraint in table.constraints:
                if isinstance(constraint, UniqueConstraint):
                    constraints.append({
                        'type': 'unique',
                        'name': constraint.name,
                        'columns': [col.name for col in constraint.columns]
                    })
        
        # 提取外键信息
        foreign_keys = []
        for fk in table.foreign_keys:
            foreign_keys.append({
                'column': fk.parent.name,
                'referenced_table': fk.column.table.name,
                'referenced_column': fk.column.name
            })
        
        return {
            'name': table_name,
            'columns': columns,
            'indexes': indexes,
            'constraints': constraints,
            'foreign_keys': foreign_keys
        }
    
    def _get_orm_table_info(self, model: Type[SqlAlChemyBase]) -> Dict[str, Any]:
        """获取ORM模型的表结构信息"""
        table = model.__table__
        
        # 提取列信息
        columns = {}
        for col in table.columns:
            columns[col.name] = {
                'type': str(col.type),
                'nullable': col.nullable,
                'default': self._get_column_default(col),
                'primary_key': col.primary_key,
                'unique': col.unique,
                'autoincrement': getattr(col, 'autoincrement', False)
            }
        
        # 提取索引信息
        indexes = []
        for idx in table.indexes:
            indexes.append({
                'name': idx.name,
                'columns': [col.name for col in idx.columns],
                'unique': idx.unique
            })
        
        # 提取约束信息
        constraints = []
        
        # 1. 提取表级约束
        for constraint in table.constraints:
            if isinstance(constraint, UniqueConstraint):
                constraints.append({
                    'type': 'unique',
                    'name': constraint.name,
                    'columns': [col.name for col in constraint.columns]
                })
        
        # 2. 提取字段级unique约束
        for col in table.columns:
            if col.unique and not col.primary_key:
                # 检查是否已经有表级约束覆盖了这个列
                column_covered = False
                for constraint in constraints:
                    if constraint['columns'] == [col.name]:
                        column_covered = True
                        break
                
                # 如果没有被表级约束覆盖，则添加字段级约束
                if not column_covered:
                    constraints.append({
                        'type': 'unique',
                        'name': f'uq_{table.name}_{col.name}',  # 生成约束名
                        'columns': [col.name]
                    })
        
        # 提取外键信息
        foreign_keys = []
        for col in table.columns:
            for fk in col.foreign_keys:
                foreign_keys.append({
                    'column': col.name,
                    'referenced_table': fk.column.table.name,
                    'referenced_column': fk.column.name
                })
        
        return {
            'name': table.name,
            'columns': columns,
            'indexes': indexes,
            'constraints': constraints,
            'foreign_keys': foreign_keys
        }
    
    def _get_column_default(self, column: Column) -> Any:
        """获取列的默认值"""
        if column.default is None:
            return None
        
        if hasattr(column.default, 'arg'):
            return column.default.arg
        elif hasattr(column.default, 'name'):
            return f"FUNCTION: {column.default.name}"
        else:
            return str(column.default)
    
    def _compare_table_structures(self, orm_info: Dict[str, Any], 
                                db_info: Dict[str, Any], 
                                table_name: str) -> Dict[str, Any]:
        """比较ORM模型和数据库表结构"""
        errors = []
        
        # 比较表名
        if orm_info['name'] != db_info['name']:
            errors.append(f"表名不一致: ORM='{orm_info['name']}', DB='{db_info['name']}'")
        
        # 比较列
        orm_columns = set(orm_info['columns'].keys())
        db_columns = set(db_info['columns'].keys())
        
        # 检查缺失的列
        missing_in_db = orm_columns - db_columns
        if missing_in_db:
            errors.append(f"数据库中缺失列: {', '.join(missing_in_db)}")
        
        missing_in_orm = db_columns - orm_columns
        if missing_in_orm:
            errors.append(f"ORM模型中缺失列: {', '.join(missing_in_orm)}")
        
        # 比较共同列的属性
        common_columns = orm_columns & db_columns
        for col_name in common_columns:
            orm_col = orm_info['columns'][col_name]
            db_col = db_info['columns'][col_name]
            
            # 比较数据类型（简化比较）
            if not self._types_compatible(orm_col['type'], db_col['type']):
                errors.append(
                    f"列 '{col_name}' 类型不一致: ORM='{orm_col['type']}', DB='{db_col['type']}'"
                )
            
            # 比较可空性
            if orm_col['nullable'] != db_col['nullable']:
                errors.append(
                    f"列 '{col_name}' 可空性不一致: ORM={orm_col['nullable']}, DB={db_col['nullable']}"
                )
            
            # 比较主键
            if orm_col['primary_key'] != db_col['primary_key']:
                errors.append(
                    f"列 '{col_name}' 主键属性不一致: ORM={orm_col['primary_key']}, DB={db_col['primary_key']}"
                )
            
            # 比较唯一性
            if orm_col['unique'] != db_col['unique']:
                errors.append(
                    f"列 '{col_name}' 唯一性不一致: ORM={orm_col['unique']}, DB={db_col['unique']}"
                )
        
        # 比较索引
        self._compare_indexes(orm_info['indexes'], db_info['indexes'], errors)
        
        # 比较约束
        self._compare_constraints(orm_info['constraints'], db_info['constraints'], errors)
        
        # 比较外键
        self._compare_foreign_keys(orm_info['foreign_keys'], db_info['foreign_keys'], errors)
        
        return {
            'valid': len(errors) == 0,
            'table_exists': True,
            'errors': errors,
            'orm_info': orm_info,
            'db_info': db_info
        }
    
    def _types_compatible(self, orm_type: str, db_type: str) -> bool:
        """检查ORM类型和数据库类型是否兼容"""
        # 标准化类型字符串
        orm_type = orm_type.upper().replace(' ', '')
        db_type = db_type.upper().replace(' ', '')
        
        # 精确类型映射 - 区分DATETIME和TIMESTAMP
        exact_type_mappings = {
            'DATETIME': ['DATETIME'],
            'TIMESTAMP': ['TIMESTAMP'],
            'INTEGER': ['INT', 'INTEGER', 'BIGINT'],
            'VARCHAR': ['VARCHAR'],
            'TEXT': ['TEXT'],
            'DECIMAL': ['DECIMAL', 'NUMERIC'],
            'BOOLEAN': ['BOOLEAN', 'BOOL', 'TINYINT', 'TINYINT(1)']
        }
        
        # 首先尝试精确匹配
        for base_type, exact_types in exact_type_mappings.items():
            if any(t in orm_type for t in exact_types) and any(t in db_type for t in exact_types):
                return True
        
        # 兼容类型映射 - 用于向后兼容
        compatible_type_mappings = {
            'VARCHAR_TEXT': ['VARCHAR', 'TEXT', 'STRING'],  # VARCHAR和TEXT可以互相兼容
        }
        
        for group_name, compatible_types in compatible_type_mappings.items():
            if any(t in orm_type for t in compatible_types) and any(t in db_type for t in compatible_types):
                return True
        
        # 如果找不到映射，进行字符串相似性检查
        return orm_type == db_type
    
    def _compare_indexes(self, orm_indexes: List[Dict], db_indexes: List[Dict], errors: List[str]):
        """比较索引"""
        # 创建索引签名集合（基于列组合而不是名称）
        def create_index_signature(idx):
            columns = tuple(sorted(idx['columns']))
            return f"{columns}_{idx['unique']}"
        
        orm_signatures = {create_index_signature(idx): idx for idx in orm_indexes}
        db_signatures = {create_index_signature(idx): idx for idx in db_indexes}
        
        # 检查缺失的索引（基于列组合）
        missing_in_db = set(orm_signatures.keys()) - set(db_signatures.keys())
        if missing_in_db:
            missing_names = [orm_signatures[sig]['name'] or f"unnamed({orm_signatures[sig]['columns']})" for sig in missing_in_db]
            errors.append(f"数据库中缺失索引: {', '.join(missing_names)}")
        
        missing_in_orm = set(db_signatures.keys()) - set(orm_signatures.keys())
        if missing_in_orm:
            # 过滤掉自动生成的唯一索引（这些通常对应unique=True的列）
            filtered_missing = []
            for sig in missing_in_orm:
                idx = db_signatures[sig]
                # 如果是单列唯一索引，可能是由unique=True自动生成的，不报错
                if not (idx['unique'] and len(idx['columns']) == 1):
                    filtered_missing.append(idx['name'] or f"unnamed({idx['columns']})")
            
            if filtered_missing:
                errors.append(f"ORM模型中缺失索引: {', '.join(filtered_missing)}")
    
    def _compare_constraints(self, orm_constraints: List[Dict], db_constraints: List[Dict], errors: List[str]):
        """比较约束 - 基于约束覆盖的列进行智能比较"""
        # 创建约束签名：基于约束类型和覆盖的列
        def create_constraint_signature(constraint):
            columns = tuple(sorted(constraint['columns']))
            return f"{constraint['type']}:{','.join(columns)}"
        
        # 获取ORM和数据库约束的签名
        orm_signatures = {create_constraint_signature(c) for c in orm_constraints}
        db_signatures = {create_constraint_signature(c) for c in db_constraints}
        
        # 比较约束覆盖范围，而不是约束名称
        missing_in_db = orm_signatures - db_signatures
        if missing_in_db:
            missing_details = []
            for sig in missing_in_db:
                constraint_type, columns = sig.split(':', 1)
                missing_details.append(f"{constraint_type}({columns})")
            errors.append(f"数据库中缺失约束: {', '.join(missing_details)}")
        
        missing_in_orm = db_signatures - orm_signatures
        if missing_in_orm:
            missing_details = []
            for sig in missing_in_orm:
                constraint_type, columns = sig.split(':', 1)
                missing_details.append(f"{constraint_type}({columns})")
            errors.append(f"ORM模型中缺失约束: {', '.join(missing_details)}")
    
    def _compare_foreign_keys(self, orm_fks: List[Dict], db_fks: List[Dict], errors: List[str]):
        """比较外键"""
        orm_fk_sigs = {f"{fk['column']}->{fk['referenced_table']}.{fk['referenced_column']}" for fk in orm_fks}
        db_fk_sigs = {f"{fk['column']}->{fk['referenced_table']}.{fk['referenced_column']}" for fk in db_fks}
        
        missing_in_db = orm_fk_sigs - db_fk_sigs
        if missing_in_db:
            errors.append(f"数据库中缺失外键: {', '.join(missing_in_db)}")
        
        missing_in_orm = db_fk_sigs - orm_fk_sigs
        if missing_in_orm:
            errors.append(f"ORM模型中缺失外键: {', '.join(missing_in_orm)}")


def validate_schema_consistency(model: Type[SqlAlChemyBase], 
                              engine: Engine, 
                              session: Session,
                              strict_mode: bool = True,
                              halt_on_error: bool = True) -> bool:
    """
    验证ORM模型与数据库表结构的一致性
    
    Args:
        model: SQLAlchemy ORM模型类
        engine: SQLAlchemy引擎
        session: SQLAlchemy会话
        strict_mode: 严格模式
        halt_on_error: 发现错误时是否暂停流程等待用户确认
        
    Returns:
        bool: 验证是否通过
        
    Raises:
        SchemaValidationError: 当发现不一致且halt_on_error=True时
    """
    validator = SchemaValidator(engine, session)
    
    try:
        result = validator.validate_model_schema(model, strict_mode=False)
        
        if not result['valid']:
            error_msg = f"模型 {model.__name__} 与数据库表结构不一致"
            message.error(error_msg)
            
            # 记录详细错误到日志
            for error in result['errors']:
                logging.error(f"Schema validation: {error}")
            
            if halt_on_error:
                print("\n" + "="*60)
                print("⚠️  数据库模式验证失败")
                print("="*60)
                print(f"模型: {model.__name__}")
                print(f"表名: {model.__tablename__}")
                print("\n发现的不一致项:")
                for i, error in enumerate(result['errors'], 1):
                    print(f"  {i}. {error}")
                
                print("\n" + "="*60)
                user_input = input("是否继续执行？(y/N): ").strip().lower()
                
                if user_input not in ['y', 'yes']:
                    raise SchemaValidationError(
                        f"用户选择停止执行。模式验证失败: {error_msg}"
                    )
                else:
                    message.info("用户选择继续执行，忽略模式验证错误")
            
            return False
        
        return True
        
    except Exception as e:
        error_msg = f"模式验证过程中发生错误: {str(e)}"
        message.error(error_msg)
        if halt_on_error:
            raise SchemaValidationError(error_msg) from e
        return False
