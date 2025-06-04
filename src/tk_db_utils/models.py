from sqlalchemy.orm import DeclarativeBase
from typing import List, Optional

class SqlAlChemyBase(DeclarativeBase):
    pass


class MixIn(DeclarativeBase):
    __abstract__ = True

    def set_special_fields(self, special_fields: Optional[List[str]] = None):
        self.special_fields = special_fields or []

    def to_dict(self):
        if hasattr(self, "special_fields"):
            return {c.name: getattr(self, c.name) for c in self.__table__.columns if c.name not in self.special_fields}
        else:
            return {c.name: getattr(self, c.name) for c in self.__table__.columns}
class DbOrmBaseMixedIn(SqlAlChemyBase, MixIn):
    __abstract__ = True
    pass
