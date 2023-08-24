from celery import Celery
from financial_data.config import MESSAGE_QUEUE_HOST, MESSAGE_QUEUE_PORT, WORKER_ACCOUNT, WORKER_PASSWORD

broker = f'pyamqp://{WORKER_ACCOUNT}:{WORKER_PASSWORD}@{MESSAGE_QUEUE_HOST}:{MESSAGE_QUEUE_PORT}/'

app = Celery(
    "task",
    include=["financial_data.tasks.tasks"],
    broker=broker,
)