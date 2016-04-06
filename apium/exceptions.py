class ApiumWorkerException(Exception):
    pass


class WrongCredentials(ApiumWorkerException):
    pass


class BrokenWorker(ApiumWorkerException):
    pass


class TaskTimeoutException(Exception):
    """The Exception to raise when a task takes longer than expected."""
    pass


class TaskRaisedException(Exception):
    pass
