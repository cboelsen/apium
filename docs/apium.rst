API Documentation
=================

Decorators
----------

.. automodule:: apium
    :members: register_task, schedule_task
    :undoc-members:

TaskQueue
---------

.. autoclass:: apium.TaskQueue
    :members: add, map
    :member-order: groupwise

Task
----

.. autoclass:: apium.Task
    :members: status, wait, result, is_finished, chain
    :member-order: groupwise

TaskTimeoutException
--------------------

.. autoclass:: apium.TaskTimeoutException

TaskStatus
----------

.. autoclass:: apium.TaskStatus
