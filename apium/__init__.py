from .exceptions import *
from .executor import TaskExecutor
from .worker import register_task, schedule_task

# TODO:
# - Authentication
# - Encryption
# - Import concurrent.futures module level functions (should just work with these Futures).
# - More error checking:
#   - Non existent task.
#   - Problems unpickling task on the server.
#   - Problems unpickling results on the client.
#   - Chaining task to non existent parent.
# - Currently attaching remote exception - what about when it doesn't exist locally?
# - Possible to backport a better chaining implementation to concurrent.futures??
#   - Probably not. Would need a reference to the executor in the future.
