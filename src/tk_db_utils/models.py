from sqlalchemy.orm import DeclarativeBase
from typing import List, Optional

class SqlAlChemyBase(DeclarativeBase):
    """
    sqlalchemy 基类,所有自定义的ORM表,都应该继承自这个基类
    """
    __abstract__ = True
    pass


class MixIn(DeclarativeBase):
    """
    sqlalchemy的MixIn增强,包含特别字段的处理和转换为字典的方法
    包含的字段:
        - special_fields: 特殊字段列表,这些字段不会被转换为字典

    包含的方法:
        - set_special_fields: 设置特殊字段列表
        - to_dict: 将对象转换为字典
    """
    __abstract__ = True

    def set_special_fields(self, special_fields: Optional[List[str]] = None):
        self.special_fields = special_fields or []

    def to_dict(self):
        if hasattr(self, "special_fields"):
            return {c.name: getattr(self, c.name) for c in self.__table__.columns if c.name not in self.special_fields}
        else:
            return {c.name: getattr(self, c.name) for c in self.__table__.columns}
class DbOrmBaseMixedIn(SqlAlChemyBase, MixIn):
    """
    被mixin增强的sqlalchemy基类,如果没有自定义的需求,应当使用这个基类
    默认会为模型设置以下特殊字段:
        - id/key_id: 主键
        - create_at/created_at: 创建时间
        - update_at/updated_at: 更新时间
    """
    __abstract__ = True
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        default_special_field_names = ["id","key_id","create_at","update_at",'created_at','updated_at']
        special_fields = [field for field in default_special_field_names if hasattr(self, field)]
        self.set_special_fields(special_fields)
