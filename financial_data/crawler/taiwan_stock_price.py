from typing import Union
import datetime
from financial_data.schema.dataset import check_schema
from loguru import logger
import pandas as pd
import time
import requests

def is_weekend(day: int) -> bool:
    return day in [0, 6]

def gen_date_list(start_date: str, end_date: str) -> list[str]:
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()    
    days = (end_date - start_date).days + 1
    date_list = [
        str(start_date + datetime.timedelta(days=day))
        for day in range(days)
    ]
    date_list = [
        dict(date=str(d), data_source=data_source)
        for d in date_list
        for data_source in [
            'twse', 'tpex'
        ]
        if not is_weekend(d.weekday())
    ]
    return date_list

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

def convert_change(df: pd.DataFrame) -> pd.DataFrame:
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
    return df


def crawler(parameter: dict[str, list[Union[str, int, float]]]) -> pd.DataFrame:
    logger.info(f'{parameter=}')
    date = parameter.get('date', '')
    data_source = parameter.get('data_source', '')
    if data_source == 'twse':
        df = crawler_twse(date=date)
    elif data_source == 'tpex':
        df = crawler_tpex(date=date)
    df = check_schema(df=df, dataset='TaiwanStockPrice')
    return df


