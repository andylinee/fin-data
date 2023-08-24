import sys
import importlib
from financial_data.backend import db
from financial_data.tasks.task import crawler
from loguru import logger

def Update(dataset: str, start_date: str, end_date: str):
    parameter_list = getattr(
        importlib.import_module(f'financial_data.crawler.{dataset}'),
        'gen_task_parameter_list',
    )(start_date=start_date, end_date=end_date)
    for parameter in parameter_list:
        logger.info(f'{dataset}, {parameter}')
        task = crawler.s(dataset, parameter)
        task.apply_async(queue=parameter.get('data_source', ''))

    db.router.close_connection()

if __name__ == '__main__':
    dataset, start_date, end_date = sys.argv[1:]
    Update(dataset=dataset, start_date=start_date, end_date=end_date)