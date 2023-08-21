from loguru import logger
import pandas as pd
import pymysql
from typing import Union
from sqlalchemy import engine

def update_mysql_by_pandas(df: pd.DataFrame, table: str, mysql_conn: engine.base.Connection):
    if len(df) > 0:
        try:
            df.to_sql(
                name=table,
                con=mysql_conn,
                if_exists='append',
                index=False,
                chunksize=1000,
            )
        except Exception as e:
            pass

def build_update_sql(colname: list[str], value: list[str]):
    update_sql = ','.join(
        [
            ' `{}` = "{}" '.format(colname[i], str(value[i]))
            for i in range(len(colname))
            if str(value[i])
        ]
    )
    return update_sql

def build_df_update_sql(table: str, df: pd.DataFrame) -> list[str]:
    logger.info('build_df_update_sql')
    df_columns = list(df.columns)
    sql_list = []
    for i in range(len(df)):
        temp = list(df.iloc[i])
        value = [
            pymysql.converters.escape_string(str(v))
            for v in temp
        ]
        sub_df_columns = [
            df_columns[j]
            for j in range(len(temp))
        ]
        update_sql = build_update_sql(
            colname=sub_df_columns, value=value
        )
        sql = '''INSERT INTO `{}`({})VALUES ({}) ON DUPLICATE KEY UPDATE {}
            '''.format(
            table,
            '`{}`'.format(
                '`,`'.join(sub_df_columns)
            ),
            '"{}"'.format(
                '"{}"'.join(value)
            ),
            update_sql,
            )
        sql_list.append(sql)
    return sql_list

def update_mysql_by_sql(df: pd.DataFrame, table: str, mysql_conn: engine.base.Connection):
    sql = build_df_update_sql(table=table, df=df)
    commit(sql=sql, mysql_conn=mysql_conn)

def commit(sql: Union[str, list[str]], mysql_conn: engine.base.Connection = None):
    logger.info('commit')
    try:
        trans = mysql_conn.begin()
        if isinstance(sql, list):
            for s in sql:
                try:
                    mysql_conn.execution_options(autocommit=False).execute(s)
                except Exception as e:
                    logger.info(e)
                    logger.info(s)
                    break
        elif isinstance(sql, str):
            mysql_conn.execution_options(autocommit=False).execute(sql)
        trans.commit()
    except Exception as e:
        trans.rollback()
        logger.info(e)

def upload_data(df: pd.DataFrame, table: str, mysql_conn: engine.base.Connection):
    if len(df) > 0:
        if update_mysql_by_pandas(df=df, table=table, mysql_conn=mysql_conn):
            pass
        else:
            update_mysql_by_sql(df=df, table=table, mysql_conn=mysql_conn)