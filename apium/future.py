import concurrent.futures
import threading
import time
import weakref

from datetime import datetime

from .client import sendmsg


class Condition(threading.Condition):

    def __init__(self, future, *args, **kwargs):
        super(Condition, self).__init__(*args, **kwargs)
        self._future_ref = weakref.ref(future)

    def __enter__(self):
        self._future_ref()._update_remotely()
        return super(Condition, self).__enter__()

    def wait(self, timeout):
        self._future_ref()._wait_remotely(timeout)


class Future(concurrent.futures.Future):

    def __init__(self, address, task, polling_interval):
        super(Future, self).__init__()
        self._condition = Condition(self)
        self._address = address
        self._task = task
        self._polling_interval = polling_interval

    def _update_details(self, details):
        try:
            self._state = details['state']
            self._result = details['result']
            self._exception = details['exception']
        except KeyError:
            pass

    def _update_remotely(self):
        details = sendmsg(self._address, {'op': 'poll', 'id': self._task['id']})
        self._update_details(details)

    def _wait_remotely(self, timeout):
        while True:
            self._update_remotely()
            if self._state not in [concurrent.futures._base.PENDING, concurrent.futures._base.RUNNING]:
                break
            time.sleep(self._polling_interval)
            if timeout and (datetime.now() - self._start_time).total_seconds() > timeout:
                return

    def cancel(self):
        details = sendmsg(self._address, {'op': 'cancel', 'id': self._task['id']})
        self._update_details(details)
        super(Future, self).cancel()
        return details['response']

    def then(self, fn, *args, **kwargs):
        task = sendmsg(self._address, {
            'op': 'chain',
            'task': {'name': fn, 'args': args, 'kwargs': kwargs},
            'parent': self._task['id']
        })
        return Future(self._address, task, self._polling_interval)
