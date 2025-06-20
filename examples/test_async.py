import asyncio
from sqlalchemy import Column, Integer, String
from tk_db_utils import (
    DbOrmBaseMixedIn,
    AsyncBaseCurd,
    async_init_db,
    run_async
)

# 定义模型
class User(DbOrmBaseMixedIn):
    __tablename__ = 'async_users'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    email = Column(String(100), unique=True)


async def main():
    
    
    # 创建CRUD实例
    crud = AsyncBaseCurd()

    
    # 先清理可能存在的测试数据
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 执行操作
    user_data = {"name": "异步用户", "email": f"async_{timestamp}@example.com"}
    user_id = await crud.async_insert_one(User, user_data)
    print(f"创建用户ID: {user_id}")
    
    # 查询用户
    user = await crud.async_select_by_id(User, user_id)
    print(f"查询到用户: {user.name if user else 'None'}")
    


if __name__ == "__main__":
    run_async(main())
