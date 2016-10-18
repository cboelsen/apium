import concurrent.futures
import time
import weakref

from .client import sendmsg, DEFAULT_PORT
from .exceptions import DeadExecutor


class Future(concurrent.futures.Future):
    """Encapsulates the asynchronous execution of a task. Future instances
    are created by :py:meth:`apium.TaskExecutor.submit`,
    :py:meth:`~apium.Future.then` and :py:meth:`~apium.Future.catch`, and
    should not be created directly except for testing.
    """

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
        """Cancel the Future if possible.

        Returns True if the future was cancelled, False otherwise. A future
        cannot be cancelled if it is running or has already completed.
        """
        self._executor()._cancel_task(self)
        result = super(Future, self).cancel()
        if result:
            del self._executor()._tasks[self._id]
        return result

    def then(self, fn, *args, **kwargs):
        """Chain a new task to the result of this task.

        Once the task encapsulated by this :py:class:`~apium.Future` finishes,
        the result will be passed as the first argument to the new task. If
        instead of a result, an exception is raised, the exception will
        propagate to the new task (ie. instead of running the task, it will
        simply take the parent task's exception).

        ::

            >>> import apium
            >>> with apium.TaskExecutor() as executor:
            ...     t1 = executor.submit('add', 1, 2)
            ...     t2 = t1.then('add', 3)
            ...     print(t1.result())
            ...     print(t2.result())
            3
            6

        :param task_name: The name of the task to chain.
        :type task_name: str
        :param args: The arguments to pass to the new task. Note that these
            arguments will be passed after and in addition to the result of
            this Future's task.
        :type args: tuple
        :param kwargs: The keyword arguments to pass to the task.
        :type kwargs: dict
        :returns: A Future encapsulating this task
        :rtype: :py:class:`~apium.Future`
        :raises RuntimeError: if the Executor has already shut down.
        :raises TaskDoesNotExist: if a task with the given name doesn't exist.
        """
        return self._executor()._chain(self, "then", fn, *args, **kwargs)

    def catch(self, task_name, *args, **kwargs):
        """Chain a new task to any exception raised by this task.

        Once the task encapsulated by this :py:class:`~apium.Future` finishes,
        any exception raised by this Future's task will be passed as the first
        argument to the new task. If this task doesn't raise an exception, the
        result will propagate to the result of the new task (ie. instead of
        running the task, it will simply take the parent task's result).

        ::

            >>> import apium
            >>> with apium.TaskExecutor() as executor:
            ...     t1 = executor.submit('exc_raise')
            ...     t2 = t1.catch('exc_to_string')
            ...     print(t2.result())
            "Exception('exc_raise')"

        :param task_name: The name of the task to chain.
        :type task_name: str
        :param args: The arguments to pass to the new task. Note that these
            arguments will be passed after and in addition to the exception
            raised by this Future's task.
        :type args: tuple
        :param kwargs: The keyword arguments to pass to the task.
        :type kwargs: dict
        :returns: A Future encapsulating this task
        :rtype: :py:class:`~apium.Future`
        :raises RuntimeError: if the Executor has already shut down.
        :raises TaskDoesNotExist: if a task with the given name doesn't exist.
        """
        return self._executor()._chain(self, "catch", task_name, *args, **kwargs)


class TaskExecutor(concurrent.futures.Executor):
    """An Executor subclass that executes tasks asynchronously using a pool of
    remote workers. The polling_interval dictates how often the TaskExecutor
    should poll the workers for the current state of incomplete tasks.
    """

    def __init__(self, server='localhost', port=DEFAULT_PORT, polling_interval=1):
        self._address = (server, port)
        self._polling_interval = polling_interval
        self._tasks = {}
        self._running = True
        self._shutting_down = False

        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self._executor.submit(self._poll)

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

    def submit(self, task_name, *args, **kwargs):
        """Submits a task to be executed with the given arguments.

        Schedules the task to be executed as fn(\*args, \*\*kwargs) and returns
        a Future instance representing the execution of the remote task.

        :param task_name: The name of the task to run.
        :type task_name: str
        :param args: The arguments to pass to the task.
        :type args: tuple
        :param kwargs: The keyword arguments to pass to the task.
        :type kwargs: dict
        :returns: A Future encapsulating this task
        :rtype: :py:class:`~apium.Future`
        :raises RuntimeError: if the Executor has already shut down.
        :raises TaskDoesNotExist: if a task with the given name doesn't exist.
        """
        if self._shutting_down:
            raise RuntimeError('cannot schedule new tasks after shutdown')
        task = sendmsg(self._address, {'op': 'submit', 'task': {'name': task_name, 'args': args, 'kwargs': kwargs}})
        return self._new_future(task)

    def shutdown(self, wait=True):
        """Clean-up the resources associated with the Executor.

        It is safe to call this method several times. Otherwise, no other
        methods can be called after this one.

        You can avoid having to call this method explicitly if you use the
        `with` statement, which will shutdown the Executor (waiting as if
        shutdown() were called with wait set to True)

        :param wait: If True then shutdown will not return until all running
            futures have finished executing and the resources used by the
            executor have been reclaimed.
        :type wait: bool
        """
        self._shutting_down = True
        if wait:
            while self._tasks:
                time.sleep(self._polling_interval)
        self._running = False
