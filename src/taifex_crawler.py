import datetime
import sys
import io
from loguru import logger
import pandas as pd
import time
import datetime
import requests

from pydantic import BaseModel

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df['date'] = df['date'].str.replace('/', '-')
    df['ChangePer'] = df['ChangePer'].str.replace('%', '')
    df['ContractDate'] = df['ContractDate'].astype(str).replace(' ', '')
    if 'TradingSession' in df.columns:
        df['TradingSession'] = df['TradingSession'].map(
            {
                '一般': 'Position',
                '盤後': 'AfterMarket'
            }
        )
    else:
        df['TradingSession'] = 'Position'

    for col in [
        'Open',
        'Max',
        'Min',
        'Close',
        'Change',
        'ChangePer',
        'Volume',
        'SettlementPrice',
        'OpenInterest',
    ]:
        df[col] = (
            df[col]
            .str.replace('-', '0')
            .astype(float)
        )
    df = df.fillna(0)
    return df

def transfer_colname_zh2en(df: pd.DataFrame) -> pd.DataFrame:
    colname_dict = {
        '交易日期': 'date',
        '契約': 'FuturesID',
        '到期月份(週別)': 'ContractDate',
        '開盤價': 'Open',
        '最高價': 'Max',
        '最低價': 'Min',
        '收盤價': 'Close',
        '漲跌價': 'Change',
        '漲跌%': 'ChangePer',
        '成交量': 'Volume',
        '結算價': 'SettlementPrice',
        '未沖銷契約數': 'OpenInterest',
        '交易時段': 'TradingSession',
    }
    df = df.drop(
        [
            '最後最佳買價',
            '最後最佳賣價',
            '歷史最高價',
            '歷史最低價',
            '是否因訊息面暫停交易',
            '價差對單式委託成交量',
        ], 
        axis=1
    )
    df.columns = [
        colname_dict[col]
        for col in df.columns
    ]
    return df

def get_taifex_headers():
    return {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6",
        "Connection": "keep-alive",
        "Host": "www.taifex.com.tw",
        "Referer": "https://www.taifex.com.tw/cht/3/futDailyMarketView",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }

def crawler_futures(date: str) -> pd.DataFrame:
    _date = date.replace('-', '')
    url = f'https://www.taifex.com.tw/cht/3/getFutcontractDl?queryStartDate={_date}&queryEndDate={_date}'
    time.sleep(5)
    res = requests.get(
        url=url, 
        headers=get_taifex_headers()
    )
    
    if res.ok:
        if res.content:
            df = pd.read_csv(io.StringIO(res.content.decode('big5')), index_col=False)
    else:
        return pd.DataFrame()
    return df

class TaiwanFuturesDaily(BaseModel):
    FuturesID: str
    ContractDate: str
    Open: float
    Max: float
    Min: float
    Close: float
    Change: float
    ChangePer: float
    Volume: float
    SettlementPrice: float
    OpenInterest: int
    TradingSession: str
    date: str

def check_schema(df: pd.DataFrame) -> pd.DataFrame:
    df_dict = df.to_dict('records')
    df_schema = [
        TaiwanFuturesDaily(**dd).__dict__
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
    for date in date_list:
        logger.info(date)
        df = crawler_futures(date=date)
        if len(df) > 0:
            df = transfer_colname_zh2en(df.copy())
            df = clean_data(df)
            df = check_schema(df)
            df.to_csv(
                f'taiwan_futures_price_{date}.csv',
                index=False
            )

if __name__ == '__main__':
    start_date, end_date = sys.argv[1:]
    main(start_date=start_date, end_date=end_date)