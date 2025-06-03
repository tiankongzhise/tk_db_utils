from sqlalchemy.orm import DeclarativeBase

class SqlAlChemyBase(DeclarativeBase):
    pass


class MixIn(DeclarativeBase):
    __abstract__ = True

    def set_special_fields(self, special_fields: list[str]|None  = None):
        self.special_fields = special_fields or []

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns if c.name not in self.special_fields}

class DbOrmBaseMixedIn(SqlAlChemyBase, MixIn):
    __abstract__ = True
    pass