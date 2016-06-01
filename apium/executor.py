import concurrent.futures
import time
import weakref

from .client import sendmsg, DEFAULT_PORT
from .exceptions import DeadExecutor


class Future(concurrent.futures.Future):

    def __init__(self, task_id, executor, *args, **kwargs):
        self._executor_ref = weakref.ref(executor)
        self._id = task_id
        super(Future, self).__init__(*args, **kwargs)

    def _executor(self):
        executor = self._executor_ref()
        if not executor or not executor._running:
            raise DeadExecutor()
        return executor

    def cancel(self):
        self._executor()._cancel_task(self)
        return super(Future, self).cancel()

    def then(self, fn, *args, **kwargs):
        return self._executor()._chain(self, "then", fn, *args, **kwargs)

    def catch(self, fn, *args, **kwargs):
        return self._executor()._chain(self, "catch", fn, *args, **kwargs)


class TaskExecutor(concurrent.futures.Executor):

    def __init__(self, server='localhost', port=DEFAULT_PORT, polling_interval=1):
        self._address = (server, port)
        self._polling_interval = polling_interval
        self._tasks = {}
        self._running = True

        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self._executor.submit(self._poll)

    def shutdown(self, wait=True):
        self._running = False

    def _update_details(self, future, details):
        try:
            state = details['state']
            notifiable_states = [
                concurrent.futures._base.CANCELLED_AND_NOTIFIED,
                concurrent.futures._base.RUNNING
            ]
            if future._state != state and state in notifiable_states:
                # TODO: Need to delete this task from self._tasks if cancelled.
                future.set_running_or_notify_cancel()
        except KeyError:
            return
        result = details['result']
        if future._result != result:
            future.set_result(result)
            del self._tasks[future._id]
            return
        exc = details['exception']
        if future._exception != exc:
            future.set_exception(exc)
            del self._tasks[future._id]
            return
        future._state = state

    def _poll(self):
        while self._running:
            time.sleep(self._polling_interval)
            for task_id, future in self._tasks.copy().items():
                details = sendmsg(self._address, {'op': 'poll', 'id': task_id})
                self._update_details(future, details)

    def _cancel_task(self, future):
        details = sendmsg(self._address, {'op': 'cancel', 'id': future._id})
        self._update_details(future, details)

    def _new_future(self, details):
        future = Future(details['id'], self)
        self._tasks[details['id']] = future
        return future

    def _chain(self, future, chain_type, fn, *args, **kwargs):
        task = sendmsg(self._address, {
            'op': 'chain',
            'task': {'name': fn, 'args': args, 'kwargs': kwargs, 'type': chain_type},
            'parent': future._id,
        })
        return self._new_future(task)

    def submit(self, fn, *args, **kwargs):
        task = sendmsg(self._address, {'op': 'submit', 'task': {'name': fn, 'args': args, 'kwargs': kwargs}})
        return self._new_future(task)
