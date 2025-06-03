    
from datetime import datetime
from decimal import Decimal
from typing import Type,List,Dict,Union,Any
from pydantic import BaseModel
from sqlalchemy import inspect,and_,or_
from sqlalchemy.orm import Session
from sqlalchemy.sql.schema import Table, UniqueConstraint, Index
from collections import defaultdict

from .models import SqlAlChemyBase
from .message import message

import json


class TransDictToPydantic(object):
    def __init__(self,model: Type[BaseModel]):
        self.model = model

    def parse_datetime(self, time_str):
        """统一处理时间格式转换"""
        if not time_str:
            return None
        try:
            # 处理可能的格式: "2023-10-2316:09:47" 或 "2023-10-23 16:09:47"
            time_str = str(time_str).replace(' ', '')
            return datetime.strptime(time_str, '%Y-%m-%d%H:%M:%S')
        except ValueError as e:
            message.error(f"时间格式解析错误: {time_str}, 错误: {str(e)}")
            return None
    def set_mapping_fields(self, mapping_fields: dict):
        """设置映射字段"""
        self.mapping_fields = mapping_fields or None

    def trans(self, obj_dict: dict):
        """设置对象属性"""
        if self.mapping_fields is None:
            raise ValueError("mapping_fields未设置")
        temp_dict = {}
        for obj_field, (attr_name, field_type, required) in self.mapping_fields.items():
            raw_value = obj_dict.get(obj_field)
            
            # 处理 null 值
            value = None if isinstance(raw_value, str) and raw_value.lower() == 'null' else raw_value
            
            # 必填字段检查
            if required and value is None:
                raise ValueError(f"必填字段缺失: {obj_field}")
            
            # 类型转换
            if value is not None:
                try:
                    if field_type == 'int':
                        # 检查是否是浮点数
                        if  '.' in str(value):
                            raise  ValueError(f"字段[{obj_field}]类型错误: {field_type},值: {[value]}")
                        #  检查是否是数字
                        if not str(value).strip().isdigit():
                            raise  ValueError(f"字段[{obj_field}]类型错误: {field_type},值: {[value]}")
                        value = int(value)
                    elif field_type == 'datetime':
                        value = self._parse_datetime(value)
                    elif field_type == 'str':
                        value = str(value).strip() if value else ''
                    elif field_type == 'float':
                        value = float(value)
                    elif field_type == 'bool':
                        value = bool(value)
                    elif field_type == 'json':
                        value = value if isinstance(value, dict) else json.loads(value)
                    elif field_type == 'decimal':
                        value = Decimal(value)
                    else:
                        raise ValueError(f"字段[{obj_field}]类型错误: {field_type},值: {[value]}")
                except (ValueError, TypeError, AttributeError) as e:
                    message.error(f"字段[{obj_field}]转换错误: {str(e)}")
                    value = None
            temp_dict[attr_name] = value
        try:
            return self.model(**temp_dict)
        except Exception as e:
            raise ValueError(f"对象属性设置错误: {str(e)}") from e

    

# def get_unique_constraints(model:type[SqlAlChemyBase]):
#     """正确获取模型的所有唯一约束"""
#     # 方法1：通过表对象获取
#     table = model.__table__
#     constraints = []
    
#     # 获取显式定义的 UniqueConstraint
#     for constraint in table.constraints:
#         if isinstance(constraint, UniqueConstraint):
#             constraints.append({
#                 'name': constraint.name,
#                 'columns': [col.name for col in constraint.columns]
#             })
    
#     # 获取隐式唯一索引 (unique=True 的列或索引)
#     for index in table.indexes:
#         if index.unique:
#             constraints.append({
#                 'name': index.name,
#                 'columns': [col.name for col in index.columns]
#             })
    
#     return constraints        

def get_unique_constraints(model: Type[SqlAlChemyBase]) -> List[Dict[str, Union[str, List[str]]]]:
    """获取模型的所有唯一约束（兼容 SQLAlchemy 1.x 和 2.x）
    
    Args:
        model: SQLAlchemy 模型类
        
    Returns:
        List of dicts with 'name' and 'columns' keys for each unique constraint
    """
    # 使用 inspect 获取表对象，兼容新旧版本
    table: Table = inspect(model).local_table
    
    constraints = []
    
    # 获取显式定义的 UniqueConstraint
    for constraint in table.constraints:
        if isinstance(constraint, UniqueConstraint):
            constraints.append({
                'name': constraint.name or f"uq_{'_'.join(col.name for col in constraint.columns)}",
                'columns': [get_column_name(col) for col in constraint.columns]
            })
    
    # 获取隐式唯一索引 (unique=True 的列或索引)
    for index in table.indexes:
        if index.unique:
            constraints.append({
                'name': index.name or f"ix_{'_'.join(get_column_name(col) for col in index.columns)}",
                'columns': [get_column_name(col) for col in index.columns]
            })
    
    # 获取列级唯一约束 (Column(unique=True))
    mapper = inspect(model)
    for column in mapper.columns:
        if column.unique and not any(
            column.name in constraint['columns'] 
            for constraint in constraints
        ):
            constraints.append({
                'name': f"uq_{column.table.name}_{column.name}",
                'columns': [column.name]
            })
    
    return constraints

def get_column_name(column) -> str:
    """兼容获取列名（处理不同SQLAlchemy版本的列对象）"""
    if hasattr(column, 'name'):  # 常规列对象
        return column.name
    elif hasattr(column, 'key'):  # 某些版本的列对象
        return column.key
    return str(column)
def filter_unique_conflicts(session:Session, model:Type[SqlAlChemyBase], object_list:list[Any]):
    """
    优化后的去重方法，批量处理唯一约束冲突检查
    
    :param session: SQLAlchemy session
    :param model: ORM 模型类
    :param object_list: 待检查的对象列表,必须有get方法
    :return: (保留的对象列表, 冲突的对象列表)
    """
    # 获取模型的所有唯一约束
    unique_constraints = get_unique_constraints(model)
    
    # 如果没有唯一约束，直接返回原始列表
    if not unique_constraints:
        return object_list, []
    
    # 用于存储已存在的唯一键组合（内存中和数据库中的）
    seen_keys = defaultdict(lambda: defaultdict(set))
    kept_objects = []
    conflict_objects = []
    
    # 先收集所有需要检查的唯一键组合
    all_constraint_values = defaultdict(list)
    
    for obj in object_list:
        for constraint in unique_constraints:
            key_values = tuple(getattr(obj, col_name) for col_name in constraint['columns'])
            all_constraint_values[constraint['name']].append((obj, key_values))
    
    # 批量查询数据库检查已存在的记录
    db_existing_keys = defaultdict(set)
    
    for constraint in unique_constraints:
        # 收集所有需要检查的值组合
        value_combinations = set()
        for obj, key_values in all_constraint_values[constraint['name']]:
            value_combinations.add(key_values)
        
        if not value_combinations:
            continue
            
        # 构建批量查询条件
        conditions = []
        for values in value_combinations:
            condition_parts = []
            for col_name, value in zip(constraint['columns'], values):
                if value is None:
                    condition_parts.append(getattr(model, col_name).is_(None))
                else:
                    condition_parts.append(getattr(model, col_name) == value)
            conditions.append(and_(*condition_parts))
        
        # 执行批量查询
        query = session.query(model)
        if len(conditions) == 1:
            query = query.filter(conditions[0])
        else:
            query = query.filter(or_(*conditions))
        
        # 获取数据库中已存在的键组合
        for record in query:
            key = tuple(getattr(record, col_name) for col_name in constraint['columns'])
            db_existing_keys[constraint['name']].add(key)
    
    # 第二次遍历检查冲突
    for obj in object_list:
        is_conflict = False
        
        for constraint in unique_constraints:
            # 获取当前对象的约束键值组合
            key_values = tuple(getattr(obj, col_name) for col_name in constraint['columns'])
            
            # 检查内存中是否已存在
            if key_values in seen_keys[constraint['name']]['memory']:
                is_conflict = True
                break
                
            # 检查数据库中是否已存在
            if key_values in db_existing_keys[constraint['name']]:
                is_conflict = True
                break
            
            # 标记为已存在（内存中）
            seen_keys[constraint['name']]['memory'].add(key_values)
        
        if is_conflict:
            conflict_objects.append(obj)
        else:
            kept_objects.append(obj)
    
    return kept_objects, conflict_objects

# 使用示例
def process_objects_with_conflicts(session, model, objects):
    print('正在对数据进行预处理,去除冲突对象')
    kept, conflicts = filter_unique_conflicts(session, model, objects)
    
    # 打印冲突警告
    for obj in conflicts:
        print(f"WARNING: 发现冲突对象 - {obj}")
    print('数据预处理完成')
    # 返回保留的对象
    return kept