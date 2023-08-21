from financial_data.config import MYSQL_DATA_DATABASE, MYSQL_DATA_HOST, MYSQL_DATA_PASSWORD, MYSQL_DATA_PORT, MYSQL_DATA_USER
from sqlalchemy import create_engine, engine


def get_mysql_financial_data_conn() -> engine.base.Connection:
    address = (
        f'mysql+pymysql://{MYSQL_DATA_USER}:{MYSQL_DATA_PASSWORD}@{MYSQL_DATA_HOST}:{MYSQL_DATA_PORT}/{MYSQL_DATA_DATABASE}'
    )
    engine = create_engine(address)
    conn = engine.connect()
    return conn
