from typing import Type, Iterable, Optional, List, Dict, Any, Union
from .models import SqlAlChemyBase
from .message import message
from .datebase import init_db, get_session, get_engine
from .utlis import get_unique_constraints
from sqlalchemy import Engine, Insert, select, update, delete, text,func,CursorResult
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from pydantic import BaseModel


class BaseCurd:
    """基础CRUD操作类 (SQLAlchemy 2.0风格)"""
    
    def __init__(self, db_engine: Optional[Engine] = None, auto_init_db: bool = True):
        """
        初始化CRUD操作类
        
        Args:
            db_engine: 数据库引擎，如果为None则使用全局引擎
            auto_init_db: 是否自动初始化数据库
        """
        self.engine = db_engine or get_engine()
        if not self.engine:
            raise RuntimeError("数据库引擎未配置，请先配置数据库连接")
            
        if auto_init_db:
            init_db()
    
    def _get_insert_ignore_stmt(self, table: Type[SqlAlChemyBase], data: List[Dict[str, Any]]):
        """
        获取INSERT IGNORE语句 (SQLAlchemy 2.0风格)
        
        Args:
            table: SQLAlchemy 表模型类
            data: 要插入的数据列表
            
        Returns:
            INSERT IGNORE 语句
            
        Raises:
            NotImplementedError: 不支持的数据库类型
        """
        dialect_name = self.engine.dialect.name
        
        if dialect_name == 'mysql':
            stmt = mysql_insert(table).values(data)
            return stmt.prefix_with("IGNORE")
        elif dialect_name == 'postgresql':
            stmt = postgresql_insert(table).values(data)
            return stmt.on_conflict_do_nothing()
        elif dialect_name == 'sqlite':
            stmt = sqlite_insert(table).values(data)
            return stmt.prefix_with("OR IGNORE")
        else:
            raise NotImplementedError(f"不支持的数据库类型: {dialect_name}")
    def _get_unique_and_primary_keys(self, table: Type[SqlAlChemyBase]) -> List[str]:
        """
        获取表的唯一约束和主键列名称,无序,去重

        Args:
            table: SQLAlchemy 表模型类

        Returns:
            唯一约束和主键列名称列表
        """
        result_set = set()
        # 获取唯一约束
        unique_constraints = get_unique_constraints(table)
        for constraint in unique_constraints:
            result_set.update(constraint['columns'])
        # 获取主键列
        primary_keys = [key.name for key in table.__table__.primary_key]
        result_set.update(primary_keys)

        return list(result_set)
    
    
    def _get_replace_into_stmt(self, table: Type[SqlAlChemyBase], data: List[Dict[str, Any]]):
        """
        获取REPLACE INTO语句 (SQLAlchemy 2.0风格)
        
        Args:
            table: SQLAlchemy 表模型类
            data: 要插入的数据列表
            
        Returns:
            REPLACE INTO 语句
            
        Raises:
            NotImplementedError: 不支持的数据库类型
        """
        dialect_name = self.engine.dialect.name
        
        if dialect_name == 'mysql':
            insert_stmt = mysql_insert(table).values(data)
            # 获取所有列名
            all_columns = [col.name for col in table.__table__.columns]
            
            # 获取唯一约束和主键列名称
            unique_and_primary_keys = self._get_unique_and_primary_keys(table)
            # 确定要更新的列（排除唯一键和主键）
            columns_to_update = [col for col in all_columns if col not in unique_and_primary_keys]
            # 构建更新字典
            update_dict = {}
            for col in columns_to_update:
                # 使用 VALUES() 函数引用新值
                update_dict[col] = text(f"VALUES({col})")
            # 特殊处理更新时间
            if 'updated_at' in columns_to_update:
                update_dict['updated_at'] = func.now()    
            
            update_stmt = insert_stmt.on_duplicate_key_update(update_dict)
            return update_stmt
        elif dialect_name == 'postgresql':
            # PostgreSQL使用ON CONFLICT DO UPDATE
            stmt = postgresql_insert(table).values(data)
            # 获取主键列
            primary_keys = [key.name for key in table.__table__.primary_key]
            if not primary_keys:
                raise ValueError(f"表 {table.__tablename__} 没有定义主键，无法执行REPLACE操作")
            
            # 构建更新字典，排除主键
            update_dict = {c.name: stmt.excluded[c.name] 
                          for c in table.__table__.columns 
                          if c.name not in primary_keys}
            
            return stmt.on_conflict_do_update(
                index_elements=primary_keys,
                set_=update_dict
            )
        elif dialect_name == 'sqlite':
            stmt = sqlite_insert(table).values(data)
            return stmt.prefix_with("OR REPLACE")
        else:
            raise NotImplementedError(f"不支持的数据库类型: {dialect_name}")
    
    def _convert_objects_to_dict(self, objects: List[Union[Dict[str, Any], SqlAlChemyBase, BaseModel]]) -> List[Dict[str, Any]]:
        """
        将对象列表转换为字典列表
        
        Args:
            objects: 待处理的对象列表
            
        Returns:
            转换后的字典列表
            
        Raises:
            TypeError: 不支持的对象类型
            ValueError: 空对象列表
        """
        if not objects:
            raise ValueError("对象列表不能为空")
        
        first_obj = objects[0]
        
        if isinstance(first_obj, dict):
            return objects
        elif hasattr(first_obj, '__table__'):  # SQLAlchemy模型
            result = []
            for obj in objects:
                if hasattr(obj, 'to_dict'):
                    result.append(obj.to_dict())
                elif hasattr(obj, 'special_fields'):
                    result.append({
                        c.name: getattr(obj, c.name)
                        for c in obj.__table__.columns 
                        if c.name not in getattr(obj, 'special_fields', [])
                    })
                else:
                    result.append({
                        c.name: getattr(obj, c.name)
                        for c in obj.__table__.columns
                    })
            return result
        elif isinstance(first_obj, BaseModel):  # Pydantic模型
            return [obj.model_dump() for obj in objects]
        else:
            raise TypeError(f"不支持的对象类型: {type(first_obj).__name__}，请传入字典、SQLAlchemy模型或Pydantic模型")

    def bulk_insert_ignore(self, table: Type[SqlAlChemyBase], objects: Iterable, chunk_size: int = 3000) -> int:
        """
        分块批量插入数据，支持 INSERT IGNORE (SQLAlchemy 2.0风格)
        
        Args:
            table: SQLAlchemy 表模型类
            objects: 可迭代对象，每个元素代表一行数据(可以是字典或模型实例)
            chunk_size: 每批插入的数据量，默认为3000
            
        Returns:
            实际插入的记录数
            
        Raises:
            ValueError: 参数错误
            RuntimeError: 插入失败
        """
        try:
            # 检查objects是否为空
            objects_list = list(objects)  # 转换为列表以确保可多次迭代
            if not objects_list:
                message.warning('没有需要插入的数据')
                return 0
            
            # 确保chunk_size合理
            if chunk_size <= 0:
                raise ValueError("chunk_size必须大于0")
                
            total = len(objects_list)
            inserted_count = 0
            
            with self.engine.begin() as conn:
                for i in range(0, total, chunk_size):
                    # 获取当前批次的数据
                    chunk = objects_list[i:i + chunk_size]
                    chunk_dict = self._convert_objects_to_dict(chunk)
                    
                    # 构建并执行 INSERT IGNORE 语句
                    stmt = self._get_insert_ignore_stmt(table, chunk_dict)
                    result = conn.execute(stmt)
                    inserted_count += result.rowcount
                    
                    message.info(f"已处理: {min(i + chunk_size, total)}/{total} 条记录")
            
            message.info(f"批量INSERT IGNORE完成，共插入 {inserted_count} 条记录")
            return inserted_count
            
        except Exception as e:
            message.error(f"批量INSERT IGNORE失败: {str(e)}")
            raise RuntimeError(f"批量INSERT IGNORE失败: {str(e)}") from e
    
    def bulk_replace_into(self, table: Type[SqlAlChemyBase], objects: Iterable, chunk_size: int = 3000) -> int:
        """
        分块批量替换数据，支持 REPLACE INTO (SQLAlchemy 2.0风格)
        
        Args:
            table: SQLAlchemy 表模型类
            objects: 可迭代对象，每个元素代表一行数据(可以是字典或模型实例)
            chunk_size: 每批插入的数据量，默认为3000
            
        Returns:
            实际处理的记录数
            
        Raises:
            ValueError: 参数错误
            RuntimeError: 替换失败
        """
        try:
            # 检查objects是否为空
            objects_list = list(objects)
            if not objects_list:
                message.warning('没有需要替换的数据')
                return 0
            
            # 确保chunk_size合理
            if chunk_size <= 0:
                raise ValueError("chunk_size必须大于0")
                
            total = len(objects_list)
            processed_count = 0
            
            with self.engine.begin() as conn:
                for i in range(0, total, chunk_size):
                    # 获取当前批次的数据
                    chunk = objects_list[i:i + chunk_size]
                    chunk_dict = self._convert_objects_to_dict(chunk)
                    
                    # 构建并执行 REPLACE INTO 语句
                    stmt = self._get_replace_into_stmt(table, chunk_dict)
                    result = conn.execute(stmt)
                    processed_count += result.rowcount
                    
                    message.info(f"已处理: {min(i + chunk_size, total)}/{total} 条记录")
            
            message.info(f"批量REPLACE INTO完成，共处理 {processed_count} 条记录")
            return processed_count
            
        except Exception as e:
            message.error(f"批量REPLACE INTO失败: {str(e)}")
            raise RuntimeError(f"批量REPLACE INTO失败: {str(e)}") from e
    
    def bulk_insert(self, table: Type[SqlAlChemyBase], objects: Iterable, chunk_size: int = 3000) -> int:
        """
        分块批量插入数据 (SQLAlchemy 2.0风格)
        
        Args:
            table: SQLAlchemy 表模型类
            objects: 可迭代对象，每个元素代表一行数据(可以是字典或模型实例)
            chunk_size: 每批插入的数据量，默认为3000
            
        Returns:
            实际插入的记录数
            
        Raises:
            ValueError: 参数错误
            RuntimeError: 插入失败
        """
        try:
            objects_list = list(objects)
            if not objects_list:
                message.warning('没有需要插入的数据')
                return 0
            
            if chunk_size <= 0:
                raise ValueError("chunk_size必须大于0")
                
            total = len(objects_list)
            inserted_count = 0
            
            with self.engine.begin() as conn:
                for i in range(0, total, chunk_size):
                    chunk = objects_list[i:i + chunk_size]
                    chunk_dict = self._convert_objects_to_dict(chunk)
                    
                    # 使用标准INSERT语句
                    stmt = Insert(table).values(chunk_dict)
                    result = conn.execute(stmt)
                    inserted_count += result.rowcount
                    
                    message.info(f"已处理: {min(i + chunk_size, total)}/{total} 条记录")
            
            message.info(f"批量INSERT完成，共插入 {inserted_count} 条记录")
            return inserted_count
            
        except Exception as e:
            message.error(f"批量INSERT失败: {str(e)}")
            raise RuntimeError(f"批量INSERT失败: {str(e)}") from e
    
    def insert_one(self, table: Type[SqlAlChemyBase], data: Union[Dict[str, Any], SqlAlChemyBase, BaseModel]) -> int:
        """
        插入单条记录
        
        Args:
            table: SQLAlchemy 表模型类
            data: 要插入的数据
            
        Returns:
            插入的记录ID（如果有自增主键）
            
        Raises:
            RuntimeError: 插入失败
        """
        try:
            data_dict = self._convert_objects_to_dict([data])[0]
            
            with self.engine.begin() as conn:
                stmt = Insert(table).values(data_dict)
                result = conn.execute(stmt)
                
                message.info(f"成功插入1条记录到表 {table.__tablename__}")
                return result.lastrowid or result.rowcount
                
        except Exception as e:
            message.error(f"插入记录失败: {str(e)}")
            raise RuntimeError(f"插入记录失败: {str(e)}") from e
    
    def select_all(self, table: Type[SqlAlChemyBase], limit: Optional[int] = None, offset: Optional[int] = None) -> List[SqlAlChemyBase]:
        """
        查询所有记录
        
        Args:
            table: SQLAlchemy 表模型类
            limit: 限制返回记录数
            offset: 偏移量
            
        Returns:
            查询结果列表
        """
        try:
            with get_session(auto_commit=False) as session:
                stmt = select(table)
                if offset is not None:
                    stmt = stmt.offset(offset)
                if limit is not None:
                    stmt = stmt.limit(limit)
                    
                result = session.execute(stmt)
                records = result.scalars().all()
                
                message.info(f"从表 {table.__tablename__} 查询到 {len(records)} 条记录")
                return records
                
        except Exception as e:
            message.error(f"查询记录失败: {str(e)}")
            raise RuntimeError(f"查询记录失败: {str(e)}") from e
    
    def select_by_id(self, table: Type[SqlAlChemyBase], record_id: Any) -> Optional[SqlAlChemyBase]:
        """
        根据ID查询单条记录
        
        Args:
            table: SQLAlchemy 表模型类
            record_id: 记录ID
            
        Returns:
            查询结果，如果不存在则返回None
        """
        try:
            with get_session(auto_commit=False) as session:
                record = session.get(table, record_id)
                
                if record:
                    message.info(f"从表 {table.__tablename__} 查询到ID为 {record_id} 的记录")
                else:
                    message.warning(f"表 {table.__tablename__} 中不存在ID为 {record_id} 的记录")
                    
                return record
                
        except Exception as e:
            message.error(f"根据ID查询记录失败: {str(e)}")
            raise RuntimeError(f"根据ID查询记录失败: {str(e)}") from e
    
    def select_by_conditions(self, table: Type[SqlAlChemyBase], conditions: Dict[str, Any], 
                           limit: Optional[int] = None, offset: Optional[int] = None) -> List[SqlAlChemyBase]:
        """
        根据条件查询记录
        
        Args:
            table: SQLAlchemy 表模型类
            conditions: 查询条件字典
            limit: 限制返回记录数
            offset: 偏移量
            
        Returns:
            查询结果列表
        """
        try:
            with get_session(auto_commit=False) as session:
                stmt = select(table)
                
                # 添加查询条件
                for column_name, value in conditions.items():
                    if hasattr(table, column_name):
                        column = getattr(table, column_name)
                        stmt = stmt.where(column == value)
                    else:
                        raise ValueError(f"表 {table.__tablename__} 不存在列 {column_name}")
                
                if offset is not None:
                    stmt = stmt.offset(offset)
                if limit is not None:
                    stmt = stmt.limit(limit)
                    
                result = session.execute(stmt)
                records = result.scalars().all()
                
                message.info(f"从表 {table.__tablename__} 根据条件查询到 {len(records)} 条记录")
                return records
                
        except Exception as e:
            message.error(f"根据条件查询记录失败: {str(e)}")
            raise RuntimeError(f"根据条件查询记录失败: {str(e)}") from e
    
    def update_by_id(self, table: Type[SqlAlChemyBase], record_id: Any, data: Dict[str, Any]) -> int:
        """
        根据ID更新记录
        
        Args:
            table: SQLAlchemy 表模型类
            record_id: 记录ID
            data: 要更新的数据
            
        Returns:
            更新的记录数
        """
        try:
            # 获取主键列名
            primary_key_columns = [key.name for key in table.__table__.primary_key]
            if not primary_key_columns:
                raise ValueError(f"表 {table.__tablename__} 没有定义主键")
            
            primary_key_column = primary_key_columns[0]  # 假设只有一个主键
            
            with self.engine.begin() as conn:
                stmt = update(table).where(getattr(table, primary_key_column) == record_id).values(**data)
                result = conn.execute(stmt)
                
                message.info(f"成功更新表 {table.__tablename__} 中 {result.rowcount} 条记录")
                return result.rowcount
                
        except Exception as e:
            message.error(f"根据ID更新记录失败: {str(e)}")
            raise RuntimeError(f"根据ID更新记录失败: {str(e)}") from e
    
    def update_by_conditions(self, table: Type[SqlAlChemyBase], conditions: Dict[str, Any], data: Dict[str, Any]) -> int:
        """
        根据条件更新记录
        
        Args:
            table: SQLAlchemy 表模型类
            conditions: 更新条件
            data: 要更新的数据
            
        Returns:
            更新的记录数
        """
        try:
            with self.engine.begin() as conn:
                stmt = update(table)
                
                # 添加更新条件
                for column_name, value in conditions.items():
                    if hasattr(table, column_name):
                        column = getattr(table, column_name)
                        stmt = stmt.where(column == value)
                    else:
                        raise ValueError(f"表 {table.__tablename__} 不存在列 {column_name}")
                
                stmt = stmt.values(**data)
                result = conn.execute(stmt)
                
                message.info(f"成功更新表 {table.__tablename__} 中 {result.rowcount} 条记录")
                return result.rowcount
                
        except Exception as e:
            message.error(f"根据条件更新记录失败: {str(e)}")
            raise RuntimeError(f"根据条件更新记录失败: {str(e)}") from e
    
    def delete_by_id(self, table: Type[SqlAlChemyBase], record_id: Any) -> int:
        """
        根据ID删除记录
        
        Args:
            table: SQLAlchemy 表模型类
            record_id: 记录ID
            
        Returns:
            删除的记录数
        """
        try:
            # 获取主键列名
            primary_key_columns = [key.name for key in table.__table__.primary_key]
            if not primary_key_columns:
                raise ValueError(f"表 {table.__tablename__} 没有定义主键")
            
            primary_key_column = primary_key_columns[0]  # 假设只有一个主键
            
            with self.engine.begin() as conn:
                stmt = delete(table).where(getattr(table, primary_key_column) == record_id)
                result = conn.execute(stmt)
                
                message.info(f"成功删除表 {table.__tablename__} 中 {result.rowcount} 条记录")
                return result.rowcount
                
        except Exception as e:
            message.error(f"根据ID删除记录失败: {str(e)}")
            raise RuntimeError(f"根据ID删除记录失败: {str(e)}") from e
    
    def delete_by_conditions(self, table: Type[SqlAlChemyBase], conditions: Dict[str, Any]) -> int:
        """
        根据条件删除记录
        
        Args:
            table: SQLAlchemy 表模型类
            conditions: 删除条件
            
        Returns:
            删除的记录数
        """
        try:
            with self.engine.begin() as conn:
                stmt = delete(table)
                
                # 添加删除条件
                for column_name, value in conditions.items():
                    if hasattr(table, column_name):
                        column = getattr(table, column_name)
                        stmt = stmt.where(column == value)
                    else:
                        raise ValueError(f"表 {table.__tablename__} 不存在列 {column_name}")
                
                result = conn.execute(stmt)
                
                message.info(f"成功删除表 {table.__tablename__} 中 {result.rowcount} 条记录")
                return result.rowcount
                
        except Exception as e:
            message.error(f"根据条件删除记录失败: {str(e)}")
            raise RuntimeError(f"根据条件删除记录失败: {str(e)}") from e
    
    def count(self, table: Type[SqlAlChemyBase], conditions: Optional[Dict[str, Any]] = None) -> int:
        """
        统计记录数
        
        Args:
            table: SQLAlchemy 表模型类
            conditions: 统计条件，如果为None则统计所有记录
            
        Returns:
            记录总数
        """
        try:
            with get_session(auto_commit=False) as session:
                stmt = select(table)
                
                # 添加统计条件
                if conditions:
                    for column_name, value in conditions.items():
                        if hasattr(table, column_name):
                            column = getattr(table, column_name)
                            stmt = stmt.where(column == value)
                        else:
                            raise ValueError(f"表 {table.__tablename__} 不存在列 {column_name}")
                
                result = session.execute(stmt)
                count = len(result.scalars().all())
                
                message.info(f"表 {table.__tablename__} 统计结果: {count} 条记录")
                return count
                
        except Exception as e:
            message.error(f"统计记录失败: {str(e)}")
            raise RuntimeError(f"统计记录失败: {str(e)}") from e
    
    def execute_raw_sql(self, sql: str, params: Optional[Dict[str, Any]] = None) -> CursorResult[Any]:
        """
        执行原生SQL语句
        
        Args:
            sql: SQL语句
            params: SQL参数
            
        Returns:
            执行结果
        """
        try:
            with self.engine.begin() as conn:
                if params:
                    result = conn.execute(text(sql), params)
                else:
                    result = conn.execute(text(sql))
                
                message.info(f"成功执行原生SQL语句")
                return result
                
        except Exception as e:
            message.error(f"执行原生SQL失败: {str(e)}")
            raise RuntimeError(f"执行原生SQL失败: {str(e)}") from e
    
    # 向后兼容的方法名
    def bulk_insert_ignore_in_chunks(self, table: Type[SqlAlChemyBase], objects: Iterable, chunk_size: int = 3000) -> int:
        """
        向后兼容的方法名，调用新的bulk_insert_ignore方法
        """
        message.warning("bulk_insert_ignore_in_chunks方法已废弃，请使用bulk_insert_ignore方法")
        return self.bulk_insert_ignore(table, objects, chunk_size)
