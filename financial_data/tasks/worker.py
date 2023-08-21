from celery import Celery

app = Celery(
    "task",
    include=["tasks"],
    broker="pyamqp://worker:worker@localhost:5672/",
)