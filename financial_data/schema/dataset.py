
import importlib
from pydantic import BaseModel
import pandas as pd

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


def check_schema(df: pd.DataFrame, dataset: str) -> pd.DataFrame:
    df_dict = df.to_dict('records')
    schema = getattr(importlib.import_module('financial_data.schema.dataset'), dataset)
    df_schema = [
        schema(**dd).__dict__
        for dd in df_dict
    ]
    df = pd.DataFrame(df_schema)
    return df