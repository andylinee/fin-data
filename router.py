import time
import typing
from client import get_mysql_financial_data_conn

from loguru import logger
from sqlalchemy import engine

def check_alive(connect: engine.base.Connection):
    connect.execute('SELECT 1 + 1')


def reconnect(connect_function: typing.Callable) -> engine.base.Connection:
    try:
        connect = connect_function()
    except Exception as e:
        logger.info(f'{connect_function.__name__} reconnect error: {e}')
    return connect


def check_connect_alive(connect: engine.base.Connection, connect_function: typing.Callable):
    if connect:
        try:
            check_alive(connect=connect)
            return connect
        except Exception as e:
            logger.info(f'{connect_function.__name__} connect error: {e}')
            time.sleep(1)
            connect = reconnect(connect_function=connect_function)
            return check_connect_alive(connect=connect, connect_function=connect_function)
    else:
        connect = reconnect(connect_function=connect_function)
        return check_connect_alive(connect=connect, connect_function=connect_function)
    

class Router:
    def __init__(self) -> None:
        self._mysql_financial_data_conn = get_mysql_financial_data_conn()

    def check_mysql_financial_data_conn_alive(self):
        self._mysql_financial_data_conn = check_connect_alive(connect=self._mysql_financial_data_conn, connect_function=get_mysql_financial_data_conn())
        return self._mysql_financial_data_conn
    
    @property
    def mysql_financial_data_conn(self):
        return self.check_mysql_financial_data_conn_alive()