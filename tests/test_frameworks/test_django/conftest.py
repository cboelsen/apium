import apium
import pytest
import time


def add(*args):
    time.sleep(0.1)
    return sum(args)


def raiser(*args):
    raise ValueError(args)


def format_exc(exc):
    return str(exc)


def is_port_open(port):
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('127.0.0.1', port))
    return result == 0


@pytest.fixture(scope="session")
def port_num():
    import random

    def random_port():
        return random.randint(1025, 65535)

    port = random_port()
    while is_port_open(port):
        port = random_port()
    return port


@pytest.yield_fixture(scope="session")
def task_add():
    fn = apium.register_task(add)
    try:
        yield fn
    finally:
        del apium.worker.tasks['add']


@pytest.yield_fixture(scope="session")
def task_raise():
    fn = apium.register_task(raiser)
    try:
        yield fn
    finally:
        del apium.worker.tasks['raiser']


@pytest.yield_fixture(scope="session")
def task_format_exc():
    fn = apium.register_task(format_exc)
    try:
        yield fn
    finally:
        del apium.worker.tasks['format_exc']


@pytest.yield_fixture(scope="session")
def running_worker(port_num, task_add, task_raise, task_format_exc):
    import threading
    with apium.worker.create_workers(('localhost', port_num), ['task_import'], 2, 0.01) as workers:
        thread = threading.Thread(target=workers.serve_forever)
        thread.start()
        while not is_port_open(port_num):
            time.sleep(0.1)
        yield
