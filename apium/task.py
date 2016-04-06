import functools
import pickle
import socket
import time

from datetime import datetime
from enum import Enum

from .exceptions import ApiumWorkerException, TaskTimeoutException, TaskRaisedException


__all__ = ['Task', 'TaskQueue', 'TaskStatus']


DEFAULT_PORT = 9737


def sendmsg(data, server):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(server)
        sock.sendall(pickle.dumps(data))
        received = sock.recv(1024)
    finally:
        sock.close()
    response = pickle.loads(received)
    if isinstance(response, ApiumWorkerException):
        raise response
    return response


def create_task_on_queue(server, polling_interval, private, credentials, data, task_name, *args, **kwargs):
    data['task'] = {'name': task_name, 'args': args, 'kwargs': kwargs}
    data['private'] = private
    data['credentials'] = credentials
    task_id = sendmsg(data, server)
    return Task(task_id, server, polling_interval, private, credentials)


class TaskStatus(Enum):
    finished = 'finished'
    pending = 'pending'
    error = 'error'
    cancelled = 'cancelled'


class Task:
    """A Task represents a task sent to the queue.

    :param task_id: The unique ID for this task.
    :type task_id: bytes
    :param server: The address and port of the TaskQueue.
    :type server: tuple
    :param polling_interval: How often to check that a task finished.
    :type polling_interval: int
    :param private: Whether a Task's results should only be accessible by this
        client.
    :type private: bool
    """

    def __init__(self, task_id, server, polling_interval, private, credentials):
        self._task_id = task_id
        self._server = server
        self._polling_interval = polling_interval
        self._start_time = datetime.now()
        self._private = private
        self._credentials = credentials

        self._result = None
        self._exception = None
        self._task_status = TaskStatus.pending

    def __repr__(self):
        return "{}('{}', {})".format(self.__class__.__name__, self._task_id.decode(), self._server)

    def __str__(self):
        return "<{} {}>".format(self.__class__.__name__, self._task_id.decode())

    def _raise_if_timed_out(self, timeout):
        if timeout and (datetime.now() - self._start_time).total_seconds() > timeout:
            raise TaskTimeoutException('{} timed out after {} seconds'.format(self, timeout))

    def _wait(self, timeout):
        while True:
            if self.done():
                break
            time.sleep(self._polling_interval)
            self._raise_if_timed_out(timeout)

    def _status(self):
        if self._task_status == TaskStatus.pending:
            data = {'task_id': self._task_id, 'op': 'poll', 'credentials': self._credentials}
            task_state = sendmsg(data, self._server)
            self._result = task_state.get('result')
            self._exception = task_state.get('exception')
            self._task_status = task_state['status']
        return self._task_status

    # TODO: Implement
    def cancel(self):
        """Attempt to cancel the task.

        If the task is currently being executed and cannot be cancelled then the
        method will return False, otherwise the task will be cancelled and the
        method will return True.

        :returns: Success
        :rtype: bool
        """
        raise NotImplementedError()

    # TODO: Implement
    def cancelled(self):
        """Returns whether the task has been cancelled.

        :returns: Whether the task has been cancelled.
        :rtype: bool
        """
        raise NotImplementedError()

    def running(self):
        """Returns whether the task is still running.

        :returns: Whether the task is still running.
        :rtype: bool
        """
        return self._status() == TaskStatus.pending

    def done(self):
        """Returns whether the task has finished running.

        :returns: Whether the task has finished running.
        :rtype: bool
        """
        return self._status() != TaskStatus.pending

    def result(self, timeout=None):
        """Pause execution until the task is complete and return the result.

        Return the value returned by the task. If the call hasn’t yet completed
        then this method will wait up to timeout seconds.  If the call hasn’t
        completed in timeout seconds, then an apium.TaskTimeoutException will be
        raised. If timeout is not specified or None, there is no limit to the
        wait time.

        If the task is cancelled before completing then apium.CancelledError
        will be raised.

        If the task raised, this method will raise an apium.TaskRaisedException
        containing the stack trace from the exception raised by the task.

        :param timeout: How long, in seconds, to wait for the result.
        :type timeout: float
        :returns: The result of the task.
        :rtype: any
        :raises: TaskTimeoutException, TaskRaisedException
        """
        exc = self.exception(timeout)
        if exc:
            raise exc
        return self._result

    def exception(self, timeout=None):
        """Pause execution until the task is complete and return the exception.

        Return the exception raised by the task. If the call hasn’t yet
        completed then this method will wait up to timeout seconds. If the call
        hasn’t completed in timeout seconds, then an apium.TaskTimeoutException
        will be raised. If timeout is not specified or None, there is no limit
        to the wait time.

        If the task is cancelled before completing then apium.CancelledError
        will be raised.

        If the call completed without raising, None is returned.

        :param timeout: How long, in seconds, to wait for the exception.
        :type timeout: float
        :returns: An exception containing the stack trace of the Exception
            raised by the task, or None if no exception was raised.
        :rtype: TaskRaisedException
        """
        self._wait(timeout)
        if self._exception:
            return TaskRaisedException('Task raised the following exception:\n{}'.format(self._exception))
        return None

    def add_done_callback(self, fn):
        """Attaches the callable fn to the future. fn will be called, with the
        future as its only argument, when the future is cancelled or finishes
        running.

        Added callables are called in the order that they were added and are
        always called in a thread belonging to the process that added them. If
        the callable raises an Exception subclass, it will be logged and
        ignored. If the callable raises a BaseException subclass, the behavior
        is undefined.

        If the future has already completed or been cancelled, fn will be called
        immediately.
        """
        # TODO: Pay attention to the "always called in a thread belonging to the process that added them."
        pass

    def chain(self, task_name, *args, **kwargs):
        """Add a new Task to the queue, passing the result of this Task on.

        The result of this Task will be passed to the new Task as the first
        argument, with the remainder of the arguments being passed after that.

        >>> from apium import TaskQueue
        >>> last_task_in_chain = TaskQueue().submit('sub', 8, 3).chain('sub', 2)   # doctest: +SKIP
        >>> last_task_in_chain.wait()                                              # doctest: +SKIP
        3

        :param task_name: The name of the task to add to the queue.
        :type task_name: str
        :param args: The extra arguments to pass to the function.
        :type args: tuple
        :param kwargs: The keyword arguments to pass to the function.
        :type kwargs: dict
        :returns: A Task representing the task added to the queue.
        :rtype: Task
        """
        data = {'op': 'chain', 'parent': self._task_id}
        return create_task_on_queue(
            self._server, self._polling_interval, self._private, self._credentials,
            data, task_name, *args, **kwargs
        )


# TODO: Allow SSL connections.
class TaskQueue:
    """TaskQueue objects represent a queue of tasks that are fed to workers.

    The constructor defaults to pointing at the localhost on the default port.
    The polling_interval can be adjusted so that polling for results happens
    more frequently. Setting the TaskQueue as "private" means that only this
    client can retrieve the results of tasks originating from this TaskQueue.

    :param address: The IP address of the machine running the server.
    :type address: str
    :param port: The port on which the machine is serving.
    :type port: int
    :param polling_interval: How often to poll for results.
    :type polling_interval: float
    :param private: Whether to allow access to the results of tasks originating
        from here.
    :type private: bool
    :param username: If provided, the username required to authenticate.
    :type username: str
    :param password: If provided, the password required to authenticate.
    :type password: str
    """

    def __init__(
            self, address='localhost', port=DEFAULT_PORT,
            polling_interval=1.0, private=False,
            username='', password=''
    ):
        self._server = (address, port)
        self._polling_interval = polling_interval
        self._private = private
        self._credentials = (username, password)

    def __repr__(self):
        return "{}('{}', {})".format(self.__class__.__name__, *self._server)

    def __str__(self):
        return "<{} @ {}:{}>".format(self.__class__.__name__, *self._server)

    def submit(self, task_name, *args, **kwargs):
        """Submit a task to the queue to be processed by the workers.

        .. doctest::

            >>> from apium import TaskQueue
            >>> task = TaskQueue().submit('add', 2, 3, 4)  # doctest: +SKIP
            >>> task.wait()                                # doctest: +SKIP
            9

        :param task_name: The name of the task to process.
        :type task_name: str
        :param args: The arguments to pass to the function.
        :type args: tuple
        :param kwargs: The keyword arguments to pass to the function.
        :type kwargs: dict
        :returns: A Task representing the task added to the queue.
        :rtype: Task
        """
        return create_task_on_queue(
            self._server, self._polling_interval, self._private, self._credentials,
            {'op': 'add'}, task_name, *args, **kwargs
        )

    def map(self, task_name, iterable, timeout=None):
        """Run an instance of task_name for each item in iterable.

        The tasks are added to the queue at the same time and the results of the
        tasks will be yielded as they are completed. This functions similarly to
        concurrent.futures.Executor.map().

        .. doctest::

            >>> from apium import TaskQueue
            >>> for value in TaskQueue().map('multiply_by_2', [1, 2, 3, 4]):    # doctest: +SKIP
            ...     print(value)                                                # doctest: +SKIP
            6
            4
            8
            2

        :param task_name: The name of the task to process.
        :type task_name: str
        :param iterable: The items to pass to each task as the lone argument.
        :type iterable: iterable
        :param timeout: How long to wait for the tasks to complete before
            raising an exception.
        :type timeout: int
        :returns: The results of the tasks.
        :rtype: any
        :raises: TaskTimeoutException
        """
        tasks = list(map(functools.partial(self.submit, task_name), iterable))
        while tasks:
            for task in tasks[:]:
                if task.done():
                    tasks.remove(task)
                    yield task.result()
                    continue
                task._raise_if_timed_out(timeout)
            time.sleep(self._polling_interval)
