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
from sqlalchemy import text, select
from sqlalchemy.orm import  Mapped, mapped_column
from sqlalchemy.types import String, JSON, DateTime, Integer
from sqlalchemy.schema import UniqueConstraint
from tk_db_utils.utlis import get_unique_constraints
from replace_into_comprehensive_test import ReplaceTestModel

print(get_unique_constraints(ReplaceTestModel))
print("primary_key:")
print([key.name for key in ReplaceTestModel.__table__.primary_key])
