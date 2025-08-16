from celery import Celery
import dash

REDIS_URL = 'redis://localhost:6379/0'

queue = Celery(__name__, broker=REDIS_URL, backend=REDIS_URL)
manager = dash.CeleryManager(queue)
