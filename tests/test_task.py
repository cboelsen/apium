import pytest
import apium
import unittest

from apium import Task, TaskStatus


TASK_ID = b'123'
SERVER = 'server'
POLLINT = 0.001


def response(result=None, status=TaskStatus.pending):
    return {'result': result, 'status': status}


def poll_msg(task_id=TASK_ID, op='poll'):
    return {'task_id': task_id, 'op': op}


@unittest.mock.patch('apium.task.sendmsg')
def test_task_status_after_creation(sendmsg):
    t = Task(TASK_ID, SERVER, POLLINT, False)
    assert t.result() is None
    sendmsg.side_effect = [response()]
    assert t.status() is TaskStatus.pending
    apium.task.sendmsg.assert_called_once_with(poll_msg(), SERVER)


@unittest.mock.patch('apium.task.sendmsg')
def test_task_is_finished_polls_queue(sendmsg):
    t = Task(TASK_ID, SERVER, POLLINT, False)
    sendmsg.side_effect = [response()]
    assert not t.is_finished()
    apium.task.sendmsg.assert_called_once_with(poll_msg(), SERVER)


@unittest.mock.patch('apium.task.sendmsg')
def test_task_wait_returns_the_result_when_a_task_has_finished(sendmsg):
    t = Task(TASK_ID, SERVER, POLLINT, False)
    sendmsg.side_effect = [response(result=5, status=TaskStatus.finished)]
    assert t.wait() is 5
    apium.task.sendmsg.assert_called_once_with(poll_msg(), SERVER)


@unittest.mock.patch('apium.task.sendmsg')
def test_task_wait_waits_until_a_task_has_finished(sendmsg):
    t = Task(TASK_ID, SERVER, POLLINT, False)
    sendmsg.side_effect = [response()] * 3 + [response(result=5, status=TaskStatus.finished)]
    assert t.wait() is 5
    assert apium.task.sendmsg.call_count is 4


@unittest.mock.patch('apium.task.sendmsg')
def test_task_wait_raises_exception_if_timeout_exceeded(sendmsg):
    t = Task(TASK_ID, SERVER, POLLINT, False)
    t.timeout = 0.002
    sendmsg.side_effect = [response()] * 10
    with pytest.raises(apium.TaskTimeoutException):
        t.wait()
