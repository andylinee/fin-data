import importlib
from financial_data.backend import db
from financial_data.tasks.worker import app

@app.task()
def crawler(dataset: str, parameter: dict[str, str]):
    df = getattr(
        importlib.import_module(f'financial_data.crawler.{dataset}'),
        'crawler'
    )(parameter=parameter)
    db.upload_data(df, dataset, db.router.mysql_financial_data_conn)