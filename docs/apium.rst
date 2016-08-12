API Documentation
=================

Decorators
----------

.. automodule:: apium
    :members: register_task, schedule_task
    :undoc-members:

TaskExecutor
------------

An Executor subclass that executes tasks asynchronously using a pool of
remote workers.

The workers need to be started separately, and the details of their
SockerServer passed in to the TaskExecutor. If no details are passed, it will
attempt to connect to the default port on localhost.

It's recommended that you use the `with` statement to avoid having to explicitly
use the :py:meth:`~apium.TaskExecutor.shutdown`::

    with apium.TaskExecutor() as executor:
        task = executor.submit('do_something', 2, 3, blah=1)
        print(task.result())

        for result in executor.map('add', [1, 2, 3, 4], [2, 3, 4, 5]):
            print(result)

.. autoclass:: apium.TaskExecutor
    :members:

    .. method:: map(task_name, \*iterables, timeout=None, chunksize=1)

        Returns an iterator equivalent to map(task, iter).

        :param task_name: The name of the task to execute, that will take as
            many arguments as there are passed iterables.
        :type task_name: str
        :param iterables: The iterables to iterate over, passing a value from
            each to a task.
        :type iterables: iter
        :param timeout: The maximum number of seconds to wait. If None, then
            there is no limit on the wait time.
        :type timeout: float
        :param chunksize: The size of the chunks the iterable will be broken
            into before being passed to a child process. This argument is only
            used by ProcessPoolExecutor; it is ignored by ThreadPoolExecutor
            and TaskExecutor.
        :returns: An iterator equivalent to: map(task, \*iterables) but the
            calls may be evaluated out-of-order.
        :rtype: iter
        :raises TimeoutError: If the entire result iterator could not be
            generated before the given timeout.
        :raises RemoteException: If task(\*args) raises for any values.

Future
------

.. autoclass:: apium.Future
    :members:
    :inherited-members:

Exceptions
----------

.. automodule:: apium.exceptions
    :members: CancelledError, TimeoutError, RemoteException, TaskDoesNotExist, UnknownMessage, DeadExecutor
    :undoc-members:
