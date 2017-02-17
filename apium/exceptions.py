# -*- coding: utf-8 -*-

"""
.. module: exceptions
    :synopsis: The collection of common exceptions.
"""


from concurrent.futures import (   # pylint: disable=W0611
    CancelledError,
    TimeoutError as TaskTimeoutError,
)


class RemoteException(Exception):
    """Raised in place of an Exception raised by the remote task, and contains a
    string representation of the original Exception and its stack trace.
    """
    pass


class TaskDoesNotExist(Exception):
    """Raised when the given task name has not been registered with the remote
    worker.
    """
    pass


class TaskWasNotSubmitted(Exception):
    """Raised when the given task ID is not found on the remote worker. This
    usually indicates the wrong UUID was passed, or the task was submitted
    from a different client.
    """
    pass


class UnknownMessage(Exception):
    """Raised by a worker when it receives a message it can't process."""
    pass


class DeadExecutor(Exception):
    """Raised when a Future's TaskExecutor has shut down, and a method is called
    on a Future that requires communication with the remote worker (eg.
    :py:meth:`~apium.Future.cancel`).
    """
    pass
