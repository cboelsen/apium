from concurrent.futures import (
    CancelledError,
    TimeoutError,
)


class RemoteException(Exception):
    pass


class TaskDoesNotExist(Exception):
    pass


class UnknownMessage(Exception):
    pass
