# -*- coding: utf-8 -*-

"""
.. module: apium
    :synopsis: User facing imports.
"""


from concurrent.futures import (
    wait,
    as_completed,
    FIRST_COMPLETED,
    FIRST_EXCEPTION,
    ALL_COMPLETED,
)
from .exceptions import *   # pylint: disable=W0401
from .executor import TaskExecutor, Future
from .worker import register_task, schedule_task

# TODO:
# - Authentication
# - Encryption
# - More error checking:
#   - Problems unpickling task on the server.
#   - Problems unpickling results on the client.
#   - Chaining task to non existent parent.
