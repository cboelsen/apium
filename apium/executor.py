import concurrent.futures

from .client import sendmsg, DEFAULT_PORT
from .future import Future


class TaskExecutor(concurrent.futures.Executor):

    def __init__(self, server='localhost', port=DEFAULT_PORT, polling_interval=1):
        self._address = (server, port)
        self._polling_interval = polling_interval

    def submit(self, fn, *args, **kwargs):
        task = sendmsg(self._address, {'op': 'submit', 'task': {'name': fn, 'args': args, 'kwargs': kwargs}})
        return Future(self._address, task, self._polling_interval)
