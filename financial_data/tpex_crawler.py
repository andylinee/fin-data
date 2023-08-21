import datetime
import sys
import time
import pandas as pd
import requests
from financial_data.backend.db.router import Router
from loguru import logger
from pydantic import BaseModel

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
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
            .str.replace('除權息', '0')
            .str.replace('除息', '0')
            .str.replace('除權', '0')
        )
    return df

def set_column(df: pd.DataFrame) -> pd.DataFrame:

    df.columns = [
        'StockID',
        'Close',
        'Change',
        'Open',
        'Max',
        'Min',
        'TradeVolume',
        'TradeValue',
        'Transaction',
    ]
    return df

def get_tpex_headers():
    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6",
        "Connection": "keep-alive",
        "Host": "www.tpex.org.tw",
        "Referer": "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430.php?l=zh-tw",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

def convert_date(date: str) -> str:
    year, month, day = date.split('-')
    year = int(year) - 1911
    return f'{year}/{month}/{day}'

def crawler_tpex(date: str) -> pd.DataFrame:
    _date = convert_date(date=date)
    url = f'https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&d={_date}&se=AL'
    time.sleep(5)
    res = requests.get(
        url=url,
        headers=get_tpex_headers()
    )
    data = res.json().get('aaData', '')
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)

    if len(df) == 0:
        return pd.DataFrame()
    
    df = df[[0, 2, 3, 4, 5, 6, 7, 8, 9]]
    df = set_column(df.copy())
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
        df = crawler_tpex(date=date)
        if len(df) > 0:
            df = clean_data(df.copy())
            df = check_schema(df.copy())
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