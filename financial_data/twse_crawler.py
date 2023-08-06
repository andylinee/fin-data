import datetime
import imp
import sys
import time
import typing

import pandas as pd
import requests
from loguru import logger
from pydantic import BaseModel
from tqdm import tqdm

from .router import Router

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df['Dir'] = ( 
        df['Dir']
        .str.split('>')
        .str[1]
        .str.split('<')
        .str[0]
    )
    df['Change'] = (
        df['Dir'] + df['Change']
    )
    df['Change'] = (
        df['Change']
        .str.replace(' ', '')
        .str.replace('X', '')
        .astype(float)
    )
    df = df.fillna('')
    df = df.drop(['Dir'], axis=1)
    for col in [
        'TradeVolume',
        'Transaction',
        'TradeValue',
        'Open',
        'Max',
        'Min',
        'Close',
        'Change',
    ]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(',', '')
            .str.replace('X', '')
            .str.replace('+', '')
            .str.replace('----', '0')
            .str.replace('---', '0')
            .str.replace('--', '0')
        )
    return df

def transfer_colname_zh2en(df: pd.DataFrame, colname: list[str]) -> pd.DataFrame:
    taiwan_stock_price = {
        '證券代號': 'StockID', 
        '證券名稱': '',
        '成交股數': 'TradeVolume',
        '成交筆數': 'Transaction',
        '成交金額': 'TradeValue',
        '開盤價': 'Open',
        '最高價': 'Max',
        '最低價': 'Min',
        '收盤價': 'Close',
        '漲跌(+/-)': 'Dir',
        '漲跌價差': 'Change',
        '最後揭示買價': '',
        '最後揭示買量': '',
        '最後揭示賣價': '',
        '最後揭示賣量': '',
        '本益比': '',
    }
    df.columns = [
        taiwan_stock_price[col]
        for col in colname
    ]
    df = df.drop([''], axis=1)
    return df

def get_twse_headers():
    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6",
        "Connection": "keep-alive",
        "Host": "www.twse.com.tw",
        "Referer": "https://www.twse.com.tw/zh/trading/historical/mi-index.html",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

def crawler_twse(date: str) -> pd.DataFrame:
    _date = date.replace('-', '')
    url = f'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={_date}&type=ALL&response=json'
    time.sleep(5)
    res = requests.get(
        url=url, 
        headers=get_twse_headers()
    )
    
    try:
        if res.json()['stat'] in ['查詢日期小於93年2月11日，請重新查詢!', '很抱歉，沒有符合條件的資料!']:
            return pd.DataFrame
        else:
            df = pd.DataFrame(
                res.json()['tables'][8]['data']
            )
            colname = res.json()['tables'][8]['fields']
    except BaseException:
        return pd.DataFrame
    
    if len(df) == 0:
        return pd.DataFrame
    
    df = transfer_colname_zh2en(df.copy(), colname)
    df['date'] = date
    return df

class TaiwanStockPrice(BaseModel):
    StockID: str
    TradeVolume: int
    Transaction: int
    TradeValue: int
    Open: float
    Max: float
    Min: float
    Close: float
    Change: float
    date: str

def check_schema(df: pd.DataFrame) -> pd.DataFrame:
    df_dict = df.to_dict('records')
    df_schema = [
        TaiwanStockPrice(**dd).__dict__
        for dd in df_dict
    ]
    df = pd.DataFrame(df_schema)
    return df

def gen_date_list(start_date: str, end_date: str) -> list[str]:
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
    days = (end_date - start_date).days + 1
    date_list = [
        str(start_date + datetime.timedelta(days=day))
        for day in range(days)
    ]
    return date_list

def main(start_date: str, end_date: str):
    date_list = gen_date_list(start_date=start_date, end_date=end_date)
    db_router = Router()
    for date in date_list:
        logger.info(date)
        df = crawler_twse(date=date)
        if len(df) > 0:
            df = clean_data(df)
            df = check_schema(df)
            try:
                df.to_sql(
                    name='TaiwanStockPrice',
                    con=db_router.mysql_financial_data_conn,
                    if_exists='append',
                    index=False,
                    chunksize=1000,
                )
            except Exception as e:
                logger.info(e)

if __name__ == '__main__':
    start_date, end_date = sys.argv[1:]
    main(start_date=start_date, end_date=end_date)
