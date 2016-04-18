from concurrent.futures import (
    CancelledError,
    TimeoutError,
)


class RemoteException(Exception):
    pass
