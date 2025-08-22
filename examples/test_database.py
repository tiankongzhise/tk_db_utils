from tk_db_utils import get_db_client
from tk_base_utils import find_file
from sqlalchemy import text
from tk_base_utils.tk_logger import set_logger_config_path, get_logger

def test_database(config_path,secret_path):
    logger = get_logger()
    logger.info("开始测试数据库连接")
    db_client = get_db_client(env_file_path=secret_path,db_config_path=config_path,db_logger_config_path=config_path)
    logger.info("数据库客户端创建完成")
    with db_client.session_scope as session:
        logger.info("开始执行SQL查询")
        result = session.execute(text("select 1;"))
        logger.info("SQL查询执行完成")
        print(result.fetchone())

if __name__ == '__main__':
    config_path = find_file("test_config.toml")
    print(config_path)
    secret_path = find_file(".env")
    set_logger_config_path(config_path)
    test_database(config_path,secret_path)
