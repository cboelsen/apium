import functools
import pickle
import socket
import time

from datetime import datetime
from enum import Enum


__all__ = ['Task', 'TaskQueue', 'TaskTimeoutException', 'TaskStatus']


DEFAULT_PORT = 9737


def sendmsg(data, server):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(server)
        sock.sendall(pickle.dumps(data))
        received = sock.recv(1024)
    finally:
        sock.close()
    return pickle.loads(received)


def create_task_on_queue(server, polling_interval, private, data, task_name, *args, **kwargs):
    data['task'] = {'name': task_name, 'args': args, 'kwargs': kwargs}
    data['private'] = private
    task_id = sendmsg(data, server)
    return Task(task_id, server, polling_interval, private)


class TaskStatus(Enum):
    finished = 'finished'
    pending = 'pending'
    error = 'error'


class TaskTimeoutException(Exception):
    """The Exception to raise when a task takes longer than expected."""
    pass


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

    def __init__(self, task_id, server, polling_interval, private):
        self._task_id = task_id
        self._server = server
        self._result = None
        self._polling_interval = polling_interval
        self._start_time = datetime.now()
        self._private = private

        self.timeout = 60

    def __repr__(self):
        return "{}('{}', {})".format(self.__class__.__name__, self._task_id.decode(), self._server)

    def __str__(self):
        return "<{} {}>".format(self.__class__.__name__, self._task_id.decode())

    def status(self):
        """Returns the current state of the task.

        Returns either:

         - TaskStatus.finished,
         - TaskStatus.pending
         - TaskStatus.error.

        :returns: The state of the task.
        :rtype: str
        """
        data = {'task_id': self._task_id, 'op': 'poll'}
        task_state = sendmsg(data, self._server)
        self._result = task_state['result']
        return task_state['status']

    def wait(self):
        """Pause execution until the task is complete and return the result.

        If the Task takes longer than self.timeout, this raises a
        TaskTimeoutException.

        :returns: The result of the task.
        :rtype: any
        :raises: TaskTimeoutException
        """
        while True:
            if self.is_finished():
                return self.result()
            time.sleep(self._polling_interval)
            if (datetime.now() - self._start_time).total_seconds() > self.timeout:
                raise TaskTimeoutException('{} timed out after {} seconds'.format(self, self.timeout))

    def result(self):
        """Returns the result of the task, or None if it's yet to finish."""
        return self._result

    def is_finished(self):
        """Returns whether the task has finished."""
        return self.status() == TaskStatus.finished

    def chain(self, task_name, *args, **kwargs):
        """Add a new Task to the queue, passing the result of this Task on.

        The result of this Task will be passed to the new Task as the first
        argument, with the remainder of the arguments being passed after that.

        >>> from apium import TaskQueue
        >>> last_task_in_chain = TaskQueue().add('sub', 8, 3).chain('sub', 2)   # doctest: +SKIP
        >>> last_task_in_chain.wait()                                           # doctest: +SKIP
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
            self._server, self._polling_interval, self._private,
            data, task_name, *args, **kwargs
        )


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
    """

    # TODO: Authentication
    def __init__(
            self, address='localhost', port=DEFAULT_PORT,
            polling_interval=1.0, private=False
    ):
        self._server = (address, port)
        self._polling_interval = polling_interval
        self._private = private

    def __repr__(self):
        return "{}('{}', {})".format(self.__class__.__name__, *self._server)

    def __str__(self):
        return "<{} @ {}:{}>".format(self.__class__.__name__, *self._server)

    def add(self, task_name, *args, **kwargs):
        """Add a task to the queue to be processed by the workers.

        .. doctest::

            >>> from apium import TaskQueue
            >>> task = TaskQueue().add('add', 2, 3, 4)  # doctest: +SKIP
            >>> task.wait()                             # doctest: +SKIP
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
            self._server, self._polling_interval, self._private,
            {'op': 'add'}, task_name, *args, **kwargs
        )

    def map(self, task_name, iterable, timeout=60):
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
        tasks = list(map(functools.partial(self.add, task_name), iterable))
        while tasks:
            for task in tasks[:]:
                if task.is_finished():
                    tasks.remove(task)
                    yield task.result()
                    continue
                time.sleep(self._polling_interval)
                if (datetime.now() - task._start_time).total_seconds() > timeout:
                    raise TaskTimeoutException('{} timed out after {} seconds'.format(tasks, timeout))
