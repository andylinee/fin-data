from sqlalchemy import create_engine, engine


def get_mysql_financial_data_conn() -> engine.base.Connection:
    address = 'mysql+pymysql://root:test@localhost:3306/FinancialData'
    engine = create_engine(address)
    conn = engine.connect()
    return conn
