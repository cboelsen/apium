import apium
import pytest


def test_basic_task_run___state_is_consistent(port_num, running_worker):
    executor = apium.TaskExecutor(port=port_num, polling_interval=0.1)
    task = executor.submit('add', 1, 2, 3)
    assert task.done() is False
    assert task.running() is True
    assert task.result() is 6
    assert task.done() is True
    assert task.running() is False


def test_mapping_tasks___values_are_returned_as_tasks_complete(port_num, running_worker):
    executor = apium.TaskExecutor(port=port_num, polling_interval=0.1)
    results = list(executor.map('add', (2, 4, 6), (3, 6, 9)))
    assert 5 in results
    assert 10 in results
    assert 15 in results
    assert len(results) is 3


def test_cancelling_task___functions_like_a_standard_future(port_num, running_worker):
    executor = apium.TaskExecutor(port=port_num, polling_interval=0.1)
    values = list(range(6))
    tasks = [executor.submit('add', value) for value in values]
    assert tasks[-1].cancel() is True
    assert tasks[-1].done() is True
    assert tasks[0].result() in values
    assert tasks[0].cancel() is False
    with pytest.raises(apium.CancelledError):
        tasks[-1].result()


def test_chaining_futures___results_of_previous_tasks_are_passed_forward(port_num, running_worker):
    executor = apium.TaskExecutor(port=port_num, polling_interval=0.1)
    task = executor.submit('add', 1, 1).then('add', 2).then('add', 4).then('add', 8)
    assert task.done() is False
    assert task.running() is False
    assert task.result() is 16
    assert task.done() is True
    assert task.running() is False


def test_chaining_multiple_times_from_one_future___functions_like_a_single_chain(port_num, running_worker):
    executor = apium.TaskExecutor(port=port_num, polling_interval=0.1)
    initial_task = executor.submit('add', 1, 1).then('add', 2)
    chain1 = initial_task.then('add', 5)
    chain2 = initial_task.then('add', 7)
    assert chain1.done() is chain2.done() is False
    assert chain1.running() is chain2.running() is False
    assert chain1.result() is 9
    assert chain2.result() is 11
    assert chain1.done() is chain2.done() is True
    assert chain1.running() is chain2.running() is False


def test_cancelling_chained_task___functions_as_a_standard_future(port_num, running_worker):
    executor = apium.TaskExecutor(port=port_num, polling_interval=0.1)
    task = executor.submit('add', 1, 1).then('add', 2).then('add', 4).then('add', 8)
    assert task.done() is False
    assert task.running() is False
    assert task.cancel() is True
    assert task.done() is True
    with pytest.raises(apium.CancelledError):
        task.result()


def test_tasks_raising_exception___exception_raised_locally_with_stacktrace(port_num, running_worker):
    executor = apium.TaskExecutor(port=port_num, polling_interval=0.1)
    task = executor.submit('raiser')
    with pytest.raises(apium.RemoteException):
        try:
            task.result()
        except Exception as err:
            assert 'ValueError' in str(err)
            raise


def test_tasks_raising_exception_in_chain___exception_propagated(port_num, running_worker):
    executor = apium.TaskExecutor(port=port_num, polling_interval=0.1)
    task = executor.submit('add', 1, 1).then('add', 2).then('raiser')
    with pytest.raises(apium.RemoteException):
        task.result()


def test_tasks_catching_exception_in_chain___exception_not_propagated(port_num, running_worker):
    executor = apium.TaskExecutor(port=port_num, polling_interval=0.1)
    task = executor.submit('raiser').catch('format_exc')
    assert 'ValueError' in task.result()


def test_task_timing_out___exception_raised(port_num, running_worker):
    executor = apium.TaskExecutor(port=port_num, polling_interval=0.1)
    task = executor.submit('add')
    with pytest.raises(apium.TimeoutError):
        task.result(timeout=0.01)
