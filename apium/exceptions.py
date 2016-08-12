from concurrent.futures import (
    CancelledError,
    TimeoutError,
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


class UnknownMessage(Exception):
    """Raised by a worker when it receives a message it can't process."""
    pass


class DeadExecutor(Exception):
    """Raised when a Future's TaskExecutor has shut down, and a method is called
    on a Future that requires communication with the remote worker (eg.
    :py:meth:`~apium.Future.cancel`).
    """
    pass
