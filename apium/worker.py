import collections
import importlib
import logging
import pickle
import queue
import socket
import socketserver
import time
import traceback
import uuid

from datetime import datetime
from multiprocessing import Process, Queue

from .exceptions import WrongCredentials  # , BrokenWorker
from .task import TaskStatus, sendmsg


__all__ = ['register_task', 'schedule_task']


tasks = {}
task_queue = Queue()
task_runs = {}
schedule_queue = Queue()


class TaskRun:

    def __init__(self, details, client):
        try:
            self._id = details['task_id']
        except KeyError:
            self._id = str(uuid.uuid4()).encode()
        self.details = details
        self.details.update({'task_id': self._id, 'client': client, 'status': TaskStatus.pending})
        self.client = client

    def __getitem__(self, key):
        return self.details[key]

    def __setitem__(self, key, value):
        self.details[key] = value

    def queue(self):
        logging.debug(
            'Adding task %s(%s%s%s) [%s]%s to queue',
            self.details['name'],
            format_args(self.details['args']),
            ', ' if self.details['args'] and self.details['kwargs'] else '',
            format_kwargs(self.details['kwargs']),
            self.details['task_id'].decode(),
            ' from {}'.format(self.client) if self.client else ''
        )
        task_queue.put(self.details)

    def id(self):
        return self._id

    def update(self, new_status):
        self['status'] = new_status
        sendmsg({'op': 'update', 'credentials': ('', ''), 'task': self.details}, ('localhost', 9737))

    def run(self):
        self.update(TaskStatus.starting)
        args = self['args']
        fn_name = self['name']
        kwargs = self['kwargs']
        logging.info(
            'Running task %s(%s%s%s) [%s]...',
            fn_name,
            format_args(args),
            ', ' if args and kwargs else '',
            format_kwargs(kwargs),
            self['task_id'].decode()
        )
        self.update(TaskStatus.running)
        task_start = datetime.now()
        try:
            self['result'] = tasks[fn_name](*args, **kwargs)
            self.update(TaskStatus.finished)
            logging.info(
                'Task %s [%s] took %s seconds, and returned %s',
                fn_name,
                self['task_id'].decode(),
                (datetime.now() - task_start).total_seconds(),
                self['result'],
            )
        except Exception:
            self['exception'] = traceback.format_exc()
            self.update(TaskStatus.error)
            logging.info(
                'Task %s [%s] took %s seconds, and raised an exception:\n%s',
                fn_name,
                self['task_id'].decode(),
                (datetime.now() - task_start).total_seconds(),
                traceback.format_exc(),
            )

    @classmethod
    def get_queued(cls):
        details = cls._queue.get()
        return TaskRun(details, details['client'])


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
    tasks[fn_name] = fn
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
            'Scheduling task %s(%s%s%s) for %s, repeating every %s',
            fn_name,
            format_args(args),
            ', ' if args and kwargs else '',
            format_kwargs(kwargs),
            start_when or datetime.now(),
            repeat_every
        )
        schedule = (start_when or datetime.now(), repeat_every, args or (), kwargs or {})
        schedule_queue.put((fn_name, schedule))
        return register_task(fn)
    return _schedule_task


def format_args(args):
    return ', '.join([str(a) for a in (args or ())])


def format_kwargs(kwargs):
    return ', '.join([str(k) + '=' + str(v) for k, v in (kwargs or {}).items()])


def add_task_to_queue(task, client):
    t = TaskRun(task, client)
    t.queue()
    return t.details


def scheduler_process(interval):
    schedules = collections.defaultdict(list)
    try:
        while True:
            start_time = datetime.now()
            while not schedule_queue.empty():
                fn_name, schedule = schedule_queue.get(False)
                schedules[fn_name].append(schedule)
            for fn_name, fn_schedules in schedules.copy().items():
                for loc, schedule in enumerate(fn_schedules[:]):
                    next_run, repeat_every, args, kwargs = schedule
                    if next_run < start_time:
                        if repeat_every:
                            fn_schedules[loc] = (next_run + repeat_every, repeat_every, args, kwargs)
                        else:
                            fn_schedules.pop(loc)
                        task = {'name': fn_name, 'args': args, 'kwargs': kwargs}
                        add_task_to_queue(task, None)
            sleep_duration = max(interval - (datetime.now() - start_time).total_seconds(), 0.1)
            time.sleep(sleep_duration)
    except KeyboardInterrupt:
        pass


def start_scheduler_process(interval, modules):
    for mod in modules:
        importlib.import_module(mod)
    Process(target=scheduler_process, args=(interval, ), name='Scheduler').start()


def worker_process():
    try:
        while True:
            task = sendmsg({'op': 'get', 'credentials': ('', '')}, ('localhost', 9737))
            if task is None:
                time.sleep(0.1)
                continue
            task = TaskRun(task, task['client'])
            try:
                task.run()
            except Exception as err:
                logging.exception('Exception caught while retrieveing task from queue: %s', err)
                task['exception'] = traceback.format_exc()
                task.update(TaskStatus.broken)
    except KeyboardInterrupt:
        pass


def start_worker_processes(num_workers, modules):
    for mod in modules:
        importlib.import_module(mod)
    for i in range(num_workers):
        Process(target=worker_process, name='Worker-' + str(i)).start()


def start_tcp_server(server_details, username, password):

    class TCPHandler(socketserver.BaseRequestHandler):

        def handle(self):
            self.data = self.request.recv(10240).strip()
            request = pickle.loads(self.data)
            logging.debug('Received request: %s', request)
            creds = request['credentials']
            # TODO: Encrypt the creds.
            if username != creds[0] or password != creds[0]:
                logging.info('Incorrect credentials received from "%s"', self.client_address[0])
                self.request.sendall(pickle.dumps(WrongCredentials('creds')))
                return
            if request['op'] == 'add':
                # TODO: Use the client address if the client says task is "private".
                task = add_task_to_queue(request['task'], self.client_address[0])
                task_runs[task['task_id']] = task
                self.request.sendall(pickle.dumps(task))
            elif request['op'] == 'get':
                try:
                    task = task_queue.get(False)
                    if 'parent' in task:
                        parent = task_runs[task['parent']]
                        if parent['status'] == TaskStatus.finished:
                            args = (parent['result'], ) + task['args']
                            task['args'] = args
                        elif parent['status'] == TaskStatus.error:
                            task['status'] = TaskStatus.error
                            task['exception'] = parent['exception']
                            task_runs[task['task_id']] = task
                            self.request.sendall(pickle.dumps(None))
                            return
                        else:
                            task_queue.put(task)
                            self.request.sendall(pickle.dumps(None))
                            return
                    self.request.sendall(pickle.dumps(task))
                except queue.Empty:
                    self.request.sendall(pickle.dumps(None))
            elif request['op'] == 'update':
                task = request['task']
                task_runs[task['task_id']] = task
                self.request.sendall(pickle.dumps(None))
            elif request['op'] == 'poll':
                task_id = request['task_id']
                task = task_runs[task_id]
                logging.debug('Responding to poll with: %s', task)
                self.request.sendall(pickle.dumps(task))
            elif request['op'] == 'chain':
                # TODO: Ugly!!! Fix on the client side.
                request['task']['parent'] = request['parent']
                task = add_task_to_queue(request['task'], self.client_address[0])
                task_runs[task['task_id']] = task
                logging.info('Chaining task [%s] to [%s]', task['task_id'], request['task']['parent'])
                self.request.sendall(pickle.dumps(task))

    server = socketserver.ThreadingTCPServer(server_details, TCPHandler)
    server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('Shutting down')
    finally:
        server.shutdown()
