import functools
import pytest
import tempfile
import time

from datetime import datetime, timedelta

import apium

from apium.inspect import inspect_worker, print_inspected_worker


def send_msg_from(address, port_num, bind_addr, msg):
    import socket
    import pickle
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((bind_addr, 0))
    try:
        sock.connect((address, port_num))
        sock.sendall(pickle.dumps(msg))
        received = sock.recv(10240)
        result = pickle.loads(received)
        if isinstance(result, Exception):
            raise result
        return result
    finally:
        sock.close()


def test_basic_task_run___state_is_consistent(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        task = executor.submit('add', 1, 2, 3)
        time.sleep(0.1)
        assert task.done() is False
        assert task.running() is True
        assert task.result() is 6
        assert task.done() is True
        assert task.running() is False


def test_mapping_tasks___values_are_returned_as_tasks_complete(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        results = list(executor.map('add', (2, 4, 6), (3, 6, 9)))
        assert 5 in results
        assert 10 in results
        assert 15 in results
        assert len(results) is 3


def test_cancelling_task___functions_like_a_standard_future(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        values = list(range(8))
        tasks = [executor.submit('add', value) for value in values]
        assert tasks[-1].cancel() is True
        assert tasks[-1].done() is True
        assert tasks[0].result() in values
        assert tasks[0].cancel() is False
        with pytest.raises(apium.CancelledError):
            tasks[-1].result()


def test_chaining_futures___results_of_previous_tasks_are_passed_forward(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        task = executor.submit('add', 1, 1).then('add', 2).then('add', 4).then('add', 8)
        assert task.done() is False
        assert task.running() is False
        assert task.result() is 16
        assert task.done() is True
        assert task.running() is False


def test_chaining_multiple_times_from_one_future___functions_like_a_single_chain(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
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
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        task = executor.submit('add', 1, 1).then('add', 2).then('add', 4).then('add', 8)
        assert task.done() is False
        assert task.running() is False
        assert task.cancel() is True
        assert task.done() is True
        with pytest.raises(apium.CancelledError):
            task.result()


def test_tasks_raising_exception___exception_raised_locally_with_stacktrace(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        task = executor.submit('raiser')
        with pytest.raises(apium.RemoteException):
            try:
                task.result()
            except Exception as err:
                assert 'ValueError' in str(err)
                raise


def test_tasks_raising_exception_in_chain___exception_propagated(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        task = executor.submit('add', 1, 1).then('add', 2).then('raiser')
        with pytest.raises(apium.RemoteException):
            task.result()


def test_tasks_catching_exception_in_chain___exception_not_propagated(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        task = executor.submit('raiser').catch('format_exc')
        assert 'ValueError' in task.result()


def test_tasks_exceptions_falling_through_tasks___exception_propagated(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        task = executor.submit('raiser').then('add', 2)
        with pytest.raises(apium.RemoteException):
            task.result()


def test_tasks_results_falling_through_catch___result_propagated(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        task = executor.submit('add', 2, 3).catch('format_exc')
        assert task.result() is 5


def test_tasks_chaining_on_a_finished_task___chaining_as_per_normal(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        task = executor.submit('add', 2, 3)
        task.result()
        chain = task.then('add', 5)
        assert chain.result() is 10


def test_importing_tasks_from_module___tasks_can_be_run(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        task = executor.submit('chain', [1, 2, 3], [4, 5, 6])
        assert list(task.result()) == [1, 2, 3, 4, 5, 6]


def test_calling_non_existant_task___exception_raised(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        with pytest.raises(apium.TaskDoesNotExist):
            executor.submit('non-existant')


def test_chaining_non_existant_task___exception_raised(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        with pytest.raises(apium.TaskDoesNotExist):
            executor.submit('add', 2, 3).then('non-existant')


def test_chaining_task_that_is_yet_to_be_queued___chaining_as_per_normal(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        values = list(range(6))
        tasks = [executor.submit('add', value) for value in values]
        chained_task = tasks[-1].then('add', 5)
        assert chained_task.result() is 10


def test_task_timing_out___exception_raised(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        task = executor.submit('add')
        with pytest.raises(apium.TaskTimeoutError):
            task.result(timeout=0.01)


def test_scheduling_tasks___tasks_called_on_schedule(port_num, running_worker):
    with tempfile.NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name

        wait = 0.1
        import task_import
        apium.schedule_task(
            datetime.now() + timedelta(seconds=wait),
            args=(filename, )
        )(task_import.scheduled_fn)
        time.sleep(3 * wait)
        assert len(open(filename).read()) is 1


def test_scheduling_repeating_tasks___tasks_called_on_repeating_schedule(port_num, running_worker):
    with tempfile.NamedTemporaryFile() as tmpfile:
        filename = tmpfile.name

        wait = 0.1
        import task_import
        apium.schedule_task(
            datetime.now(),
            timedelta(seconds=wait),
            args=(filename, )
        )(task_import.scheduled_fn)
        time.sleep(5 * wait)
        assert len(open(filename).read()) > 3


def test_sending_a_garbage_message___exception_raised1(port_num, running_worker):
    with pytest.raises(apium.UnknownMessage):
        apium.client.sendmsg(('localhost', port_num), {'op': 'bob'})


def test_sending_a_garbage_message___exception_raised2(port_num, running_worker):
    with pytest.raises(apium.UnknownMessage):
        apium.client.sendmsg(('localhost', port_num), 'bob')


def test_cancelling_task_after_executor_dies___exception_raised(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        task = executor.submit('add', 1, 2, 3)
    with pytest.raises(apium.DeadExecutor):
        task.cancel()


def test_chaininging_task_after_executor_dies___exception_raised(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        task = executor.submit('add', 1, 2, 3)
    with pytest.raises(apium.DeadExecutor):
        task.then('add', 4)


def test_submitting_task_after_executor_shutdown___exception_raised(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        executor._shutting_down = True
        with pytest.raises(RuntimeError):
            executor.submit('add', 1, 2, 3)


def test_cancelling_task___executor_doesnt_wait_for_cancelled_task_on_shutdown(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        values = list(range(8))
        tasks = [executor.submit('add', value) for value in values]
        assert tasks[-1].cancel() is True


def test_shutting_down_executor_without_waiting___executor_shuts_down_cleanly(port_num, running_worker):
    executor = apium.TaskExecutor(port=port_num, polling_interval=0.1)
    values = list(range(8))
    for value in values:
        executor.submit('add', value)
    executor.shutdown(wait=False)


def test_wait_all_completed___all_futures_done(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        values = list(range(6))
        tasks = [executor.submit('add', value) for value in values]
        results = apium.wait(tasks)
        assert len(results.done) == len(values)
        assert len(results.not_done) == 0


def test_wait_first_completed___one_future_done(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        values = list(range(6))
        tasks = [executor.submit('add', value) for value in values]
        results = apium.wait(tasks, return_when=apium.FIRST_COMPLETED)
        assert len(results.done) >= 1
        assert len(results.not_done) <= len(values) - 1


def test_wait_first_exception___all_futures_done(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.02) as executor:
        tasks = [executor.submit('add', 1)]
        tasks.append(executor.submit('raiser'))
        tasks += [executor.submit('add', 2)]
        results = apium.wait(tasks, return_when=apium.FIRST_EXCEPTION)
        assert len(results.done) == 1
        assert len(results.not_done) == 2


def test_as_completed___futures_returned_as_sompleted(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.1) as executor:
        values = list(range(6))
        tasks = list(apium.as_completed([executor.submit('add', value) for value in values]))
        assert len(tasks) == len(values)
        assert functools.reduce(lambda d1, d2: d1 and d2, map(lambda f: f.done(), tasks))
        assert sum(map(lambda f: f.result(), tasks)) == sum(values)


def test_inspect___registered_task_names_returned(port_num, running_worker):
    details = inspect_worker(('localhost', port_num))
    tasks = details['tasks']
    assert len(tasks) == 5
    task_names = tasks.keys()
    assert 'chain' in task_names
    assert 'add' in task_names
    assert 'raiser' in task_names
    assert 'scheduled_fn' in task_names
    assert 'format_exc' in task_names


def test_inspect___registered_signature_returned(port_num, running_worker):
    details = inspect_worker(('localhost', port_num))
    tasks = details['tasks']
    assert len(tasks) == 5
    assert len([t for t in tasks.values() if isinstance(t, str)]) == 5


def test_inspect___schedules_of_tasks_returned_with_correct_repeat(port_num, running_worker):
    details = inspect_worker(('localhost', port_num))
    schedules = details['schedules']
    assert len(schedules) == 1
    assert len(schedules['scheduled_fn']) == 1
    assert schedules['scheduled_fn'][0][1] == timedelta(seconds=0.1)


def test_inspect___schedules_of_tasks_returned_with_correct_initial(port_num, running_worker):
    future_initial = datetime.now() + timedelta(seconds=3600)
    import task_import
    apium.schedule_task(
        future_initial,
        args=('/tmp/a/b/c/d/e/f', ),
        kwargs={'fake': 'arg'},
    )(task_import.scheduled_fn)
    time.sleep(0.2)

    details = inspect_worker(('localhost', port_num))
    schedules = details['schedules']
    assert len(schedules) == 1
    assert len(schedules['scheduled_fn']) == 2
    assert schedules['scheduled_fn'][1][0] == future_initial
    assert schedules['scheduled_fn'][1][2] == ('/tmp/a/b/c/d/e/f', )
    assert schedules['scheduled_fn'][1][3] == {'fake': 'arg'}


def test_print_inspect___runs_without_exception(port_num, running_worker):
    with apium.TaskExecutor(port=port_num, polling_interval=0.02) as executor:
        executor.submit('add', 1)
        print_inspected_worker(inspect_worker(('localhost', port_num)))


def test_framework_setup_without_extra_frameworks___happily_continues(port_num, running_worker):
    import apium.frameworks
    apium.frameworks.setup()


def test_tasks_are_segregated_by_address___cant_check_task_from_different_ip(port_num, running_worker):
    with apium.TaskExecutor(server='127.0.0.1', port=port_num, polling_interval=0.1) as executor:
        future1 = executor.submit('add', 2, 3)
        send_msg_from('127.0.0.1', port_num, '127.0.0.1', {'op': 'poll', 'id': future1._id})
        future2 = executor.submit('add', 2, 3)
        with pytest.raises(apium.TaskWasNotSubmitted):
            send_msg_from('127.0.0.1', port_num, '127.0.0.3', {'op': 'poll', 'id': future2._id})


def test_inspect_from_other_client___cant_see_running_tasks(port_num, running_worker):
    with apium.TaskExecutor(server='127.0.0.1', port=port_num, polling_interval=0.1) as executor:
        while len(inspect_worker(('localhost', port_num))['running']) > 0:
            time.sleep(0.1)
        executor.submit('add', 2, 3)
        executor.submit('add', 2, 3)
        details = send_msg_from('127.0.0.1', port_num, '127.0.0.1', {'op': 'inspect'})
        assert len(details['running']) == 2
        assert details['running'][0] is not None
        assert details['running'][1] is not None
        details = send_msg_from('127.0.0.1', port_num, '127.1.1.3', {'op': 'inspect'})
        assert len(details['running']) == 2
        assert details['running'][0] is None
        assert details['running'][1] is None
