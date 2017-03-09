# -*- coding: utf-8 -*-

"""
.. module: worker
    :synopsis: Spawns the workers, scheduler thread, and creates the TCP server.
"""


__all__ = (
    'register_task',
    'schedule_task',
    'create_workers',
    'setup_scheduler',
    'setup_tcp_server',
)


import collections
import concurrent.futures
import contextlib
import functools
import importlib
import logging
import pickle
import threading
import time
import traceback
import uuid

from datetime import datetime
from multiprocessing import Queue, Lock

from .client import sendmsg
from .exceptions import (
    RemoteException,
    TaskDoesNotExist,
    TaskWasNotSubmitted,
    UnknownMessage,
)
from .utils import format_fn


try:
    from inspect import signature
except ImportError:
    from funcsigs import signature


try:
    import socketserver
except ImportError:
    import SocketServer as socketserver


class WorkersState(object):
    """Namespace in which to keep the module's state."""
    # TODO: Can we please make this thread-safe?!?!?
    tasks = {}
    futures = {}
    chains = collections.defaultdict(list)
    chains_lock = Lock()
    schedule_queue = Queue()
    schedules = collections.defaultdict(list)


def register_task(fn):
    """A decorator to register a function as a task, to be later run by a
    Worker.

    .. doctest::

        >>> import apium
        >>> @apium.register_task
        ... def task1(arg1, kwarg1=1):
        ...     return arg1 + kwarg1

    :param fn: The function to register.
    :type fn: function
    :returns: The given function.
    :rtype: function
    """
    fn_name = fn.__name__
    logging.debug('Registering task %s', fn_name)
    WorkersState.tasks[fn_name] = fn
    return fn


def schedule_task(start_when=None, repeat_every=None, args=None, kwargs=None):
    """A decorator to register and schedule the given function at a time and/or
    interval.

    It's possible to apply this decorator multiple times to the same function in
    order to have multiple schedules for the same function.

    .. testsetup::

        from datetime import datetime, timedelta

    .. doctest::

        >>> import apium
        >>> @apium.schedule_task(datetime.now() + timedelta(seconds=30), args=(2, ))
        ... @apium.schedule_task(repeat_every=timedelta(seconds=60), args=(3, ), kwargs={'kwarg1': 3})
        ... @apium.schedule_task(args=(4, ), kwargs={'kwarg1': 5})
        ... def task1(arg1, kwarg1=1):
        ...     return arg1 + kwarg1

    In the example above, task1 will be added to the worker's queue:

     - as task1(2, kwarg1=1), 30 seconds after the workers start.

     - as task1(3, kwarg1=3), immediately after the workers start and every 60 seconds thereafter.

     - as task1(4, kwarg1=5), immediately after the workers start.

    :param start_when: When to run the function for the first time. A value of
        None results in the function being run immediately after the workers
        start.
    :type start_when: datetime.datetime
    :param repeat_every: The interval at which the function should repeatedly
        run. A value of None results in the function only being called once.
    :type repeat_every: datetime.timedelta
    :param args: The arguments to pass to the scheduled function.
    :type args: tuple
    :param kwargs: The keyword arguments to pass to the scheduled function.
    :type kwargs: dict
    :returns: The given function
    :rtype: function
    """
    def _schedule_task(fn):
        fn_name = fn.__name__
        logging.debug(
            'Scheduling task %s for %s, repeating every %s',
            format_task({'name': fn_name, 'args': args, 'kwargs': kwargs}),
            start_when or datetime.now(),
            repeat_every
        )
        schedule = (start_when or datetime.now(), repeat_every, args or (), kwargs or {})
        WorkersState.schedule_queue.put((fn_name, schedule))
        return register_task(fn)
    return _schedule_task


def format_task(task, chain=False):
    """Return a string representation of a task, for logging."""
    if chain:
        task = task.copy()
        task['args'] = ('X', ) + task['args']

    return '{}{}{}'.format(
        format_fn(task['name'], task['args'], task['kwargs']),
        ' [{}]'.format(task['id'].decode()) if task.get('id') else '',
        ' from {}'.format(task['client']) if task.get('client') else '',
    )


def run_task(task):
    """A wrapper around a task's function, for handling logging and exceptions."""
    formatted_task = format_task(task)
    logging.info('Running task %s...', formatted_task)
    task_start = datetime.now()
    try:
        result = WorkersState.tasks[task['name']](*task['args'], **task['kwargs'])
        logging.info(
            'Task %s took %s seconds, and returned %s',
            formatted_task,
            (datetime.now() - task_start).total_seconds(),
            result,
        )
        return result
    except Exception:
        logging.info(
            'Task %s took %s seconds, and raised an exception:\n%s',
            formatted_task,
            (datetime.now() - task_start).total_seconds(),
            traceback.format_exc(),
        )
        remote_exc = RemoteException(traceback.format_exc())
        raise remote_exc


def scheduler_process(address, interval, new_schedule_queue):
    """Responsible for polling the tasks' schedules."""
    try:
        while True:
            start_time = datetime.now()
            while not new_schedule_queue.empty():
                new_schedule = new_schedule_queue.get(False)
                if new_schedule is None:
                    return
                fn_name, schedule = new_schedule
                WorkersState.schedules[fn_name].append(schedule)
            for fn_name, fn_schedules in WorkersState.schedules.copy().items():
                for loc, schedule in enumerate(fn_schedules[:]):
                    next_run, repeat_every, args, kwargs = schedule
                    if next_run < start_time:
                        if repeat_every:
                            fn_schedules[loc] = (next_run + repeat_every, repeat_every, args, kwargs)
                        else:
                            fn_schedules.pop(loc)
                        sendmsg(address, {'op': 'submit', 'task': {'name': fn_name, 'args': args, 'kwargs': kwargs}})
            sleep_duration = max(interval - (datetime.now() - start_time).total_seconds(), 0.1)
            time.sleep(sleep_duration)
    except KeyboardInterrupt:
        pass


def get_future_state(future):
    """Serialises the state of a concurrent.futures.Future"""
    if future is None:
        return {}
    with future._condition:
        return {
            'state': future._state,
            'result': future._result,
            'exception': future._exception,
        }


def check_for_chain(parent_id, executor, parent_future):
    """If there are tasks chained to the given parent, add them now."""
    with WorkersState.chains_lock:
        for task in WorkersState.chains[parent_id]:
            add_task(executor, task, parent_future)
        del WorkersState.chains[parent_id]


def future_values_fall_through(task, parent_future):
    """Pass the parent's state onto the child."""
    future = concurrent.futures.Future()
    future._state = parent_future._state
    future._result = parent_future._result
    future._exception = parent_future._exception
    future._task = task
    WorkersState.futures[task['id']] = future


def submit_new_task(executor, task):
    """Submit a new task to the given executor"""
    future = executor.submit(run_task, task)
    future.add_done_callback(functools.partial(check_for_chain, task['id'], executor))
    future._task = task
    WorkersState.futures[task['id']] = future


def add_task(executor, task, parent_future):
    """Add a new task to the executor, based on the result of the parent."""
    try:
        task['args'] = (parent_future.result(), ) + task['args']
    except BaseException as err:    # pylint: disable=W0703
        if task['type'] == 'catch':
            task['args'] = (err, ) + task['args']
            submit_new_task(executor, task)
        else:
            future_values_fall_through(task, parent_future)
    else:
        if task['type'] == 'then':
            submit_new_task(executor, task)
        else:
            future_values_fall_through(task, parent_future)


@contextlib.contextmanager
def create_workers(modules, num_workers):
    """Create the workers and yield the executor.

    .. doctest::

        >>> with create_workers([], 2) as workers:
        ...     pass

    :param modules: Strings representing the python paths of modules from which
        to load tasks.
    :type modules: list
    :param num_workers: The number of worker processes to spawn.
    :type num_workers: int
    :returns: The worker's executor.
    :rtype: concurrent.futures.Executor
    """

    logging.debug('Setting up process pool with %s workers', num_workers)
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=num_workers)

    for mod in modules:
        logging.debug('Importing %s', mod)
        importlib.import_module(mod)

    try:
        yield executor
    except KeyboardInterrupt:
        print('KeyboardInterrupt')
    finally:
        logging.debug('Shutting down process pool and waiting for currently running tasks to finish')
        executor.shutdown(wait=True)


@contextlib.contextmanager
def setup_scheduler(address, interval):
    """Set up and start the scheduler, then yield.

    .. doctest::

        >>> with setup_scheduler(('localhost', 12378), 1.0):
        ...     pass

    :param address: The address on which to listen for connections.
    :type address: tuple
    :param interval: How often to poll the tasks' schedules, to see if a task
        should be run.
    :type interval: float
    """

    logging.debug('Setting up task scheduler thread')
    scheduler = threading.Thread(
        target=scheduler_process,
        args=(address, interval, WorkersState.schedule_queue),
        name='Scheduler',
    )
    scheduler.daemon = True

    logging.debug('Starting task scheduler')
    scheduler.start()
    try:
        yield
    except KeyboardInterrupt:
        print('KeyboardInterrupt')
    finally:
        logging.debug('Stopping task scheduler')
        WorkersState.schedule_queue.put(None)
        scheduler.join()


def get_future(task_id, client_address):
    """Get the future associated with the given task ID, and check that it was
    originally submitted by the same client.
    """
    future = WorkersState.futures[task_id]
    if future is not None:
        task = future._task
        if task['client'] != client_address:
            raise TaskWasNotSubmitted(task_id)
    return future


@contextlib.contextmanager
def setup_tcp_server(address, workers):
    """Set up and then yield the TCP server.

    :param address: The address on which to listen for connections.
    :type address: tuple
    :param wokrers: The workers with which to run the requested tasks.
    :type workers: concurrent.futures.Executor
    :returns: The TCP server.
    :rtype: socketserver.BaseRequestHandler
    """

    class TCPHandler(socketserver.BaseRequestHandler):
        """Handle requests for running tasks and getting results."""

        def _handle_task_submission(self, task):
            if task['name'] not in WorkersState.tasks:
                return TaskDoesNotExist(task['name'])
            task['client'] = self.client_address[0]
            task_id = str(uuid.uuid4()).encode()
            task['id'] = task_id
            logging.debug('Adding task %s to queue', format_task(task))
            submit_new_task(workers, task)
            return task

        def _handle_task_cancellation(self, task_id):
            future = get_future(task_id, self.client_address[0])
            if future is not None:
                response = future.cancel()
            else:
                response = False
                with WorkersState.chains_lock:
                    for children in WorkersState.chains.values():
                        for child in children[:]:
                            if child['id'] == task_id:
                                response = True
                                children.remove(child)
            if response:
                logging.debug('Cancelled task [%s]', task_id.decode())
            else:
                logging.debug('Failed to cancel task [%s]', task_id.decode())
            state = get_future_state(future)
            state['response'] = response
            return state

        def _handle_task_poll(self, task_id):
            future = get_future(task_id, self.client_address[0])
            state = get_future_state(future)
            logging.debug('Polled for task [%s]: %s', task_id.decode(), state)
            return state

        def _handle_task_chaining(self, task, parent_id):
            if task['name'] not in WorkersState.tasks:
                return TaskDoesNotExist(task['name'])
            task['client'] = self.client_address[0]
            task_id = str(uuid.uuid4()).encode()
            task['id'] = task_id
            future = get_future(parent_id, self.client_address[0])
            with WorkersState.chains_lock:
                if future is not None and future.done():
                    add_task(workers, task, future)
                else:
                    WorkersState.chains[parent_id].append(task)
                    WorkersState.futures[task_id] = None
            logging.debug('Chained task %s to [%s]', format_task(task, chain=True), parent_id.decode())
            return task

        def _handle_task_queue_inspection(self):
            task_list = {}
            for task_name, task_fn in WorkersState.tasks.items():
                try:
                    sig = signature(task_fn)
                except ValueError as err:
                    sig = err
                task_list[task_name] = str(sig)

            def _get_task_if_from_client(future):
                task = future._task
                addr = self.client_address[0]
                if task['client'] != addr and not addr.startswith('127.0.0'):
                    return None
                return task

            response = {
                'tasks': task_list,
                'schedules': WorkersState.schedules,
                'running': [_get_task_if_from_client(f) for f in WorkersState.futures.values() if f and f.running()],
            }
            return response

        def handle(self):
            self.data = self.request.recv(10240).strip()
            request = pickle.loads(self.data)
            logging.debug('Received request from %s: %s', self.client_address[0], request)
            # TODO: Check credentials
            # TODO: Encrypt credentials
            try:
                if request['op'] == 'submit':
                    task = request['task']
                    task = self._handle_task_submission(task)
                    self.request.sendall(pickle.dumps(task))
                elif request['op'] == 'cancel':
                    task_id = request['id']
                    state = self._handle_task_cancellation(task_id)
                    self.request.sendall(pickle.dumps(state))
                elif request['op'] == 'poll':
                    task_id = request['id']
                    state = self._handle_task_poll(task_id)
                    self.request.sendall(pickle.dumps(state))
                elif request['op'] == 'chain':
                    task = request['task']
                    parent_id = request['parent']
                    task = self._handle_task_chaining(task, parent_id)
                    self.request.sendall(pickle.dumps(task))
                elif request['op'] == 'inspect':
                    inspect_response = self._handle_task_queue_inspection()
                    self.request.sendall(pickle.dumps(inspect_response))
                else:
                    self.request.sendall(pickle.dumps(UnknownMessage(request)))
            except TaskWasNotSubmitted as error:
                self.request.sendall(pickle.dumps(error))
            except (KeyError, TypeError):
                self.request.sendall(pickle.dumps(UnknownMessage(request)))
            except Exception as error:
                logging.warning('Exception raised when processing request:\n%s', traceback.format_exc())
                self.request.sendall(pickle.dumps(error))

    logging.debug('Setting up TCP server')
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    server = socketserver.ThreadingTCPServer(address, TCPHandler)

    try:
        yield server
    except KeyboardInterrupt:
        print('KeyboardInterrupt')
    finally:
        logging.debug('Shutting down TCP server')
        server.shutdown()
        server.server_close()
