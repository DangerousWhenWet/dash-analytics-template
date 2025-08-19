from celery import Celery
import dash

REDIS_URL = 'redis://localhost:6379/0'

queue = Celery(__name__, broker=REDIS_URL, backend=REDIS_URL)
manager = dash.CeleryManager(queue, cache_by=[])
#HACK -- disable background-callback caching or else get "race conditions" where fast-returned background-callbacks are cached and take priority over longer-returned executions of the same method
