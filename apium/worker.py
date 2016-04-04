import collections
import importlib
import logging
import pickle
import socketserver
import time
import uuid

from datetime import datetime
from multiprocessing import Process, Manager, Queue

from .task import TaskStatus


__all__ = ['register_task', 'schedule_task']


_mgr = Manager()
results = _mgr.dict()
task_queue = Queue()
tasks = {}
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
        schedules[fn_name].append((start_when or datetime.now(), repeat_every, args or (), kwargs or {}))
        return register_task(fn)
    return _schedule_task


def format_args(args):
    return ', '.join([str(a) for a in (args or ())])


def format_kwargs(kwargs):
    return ', '.join([str(k) + '=' + str(v) for k, v in (kwargs or {}).items()])


def add_task_to_queue(task, client):
    task.update({'task_id': str(uuid.uuid4()).encode(), 'client': client})
    logging.debug(
        'Adding task %s(%s%s%s) [%s]%s to queue',
        task['name'],
        format_args(task['args']),
        ', ' if task['args'] and task['kwargs'] else '',
        format_kwargs(task['kwargs']),
        task['task_id'].decode(),
        ' from {}'.format(client) if client else ''
    )
    task_queue.put(task)
    return task['task_id']


def scheduler_process(interval):
    try:
        while True:
            start_time = datetime.now()
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
            task = task_queue.get()
            try:
                try:
                    parent_id = task['parent']
                    try:
                        args = (results[parent_id]['result'], *task['args'])
                    except KeyError:
                        time.sleep(0.1)
                        task_queue.put(task)
                        continue
                except KeyError:
                    args = task['args']
                fn_name = task['name']
                kwargs = task['kwargs']
                logging.info(
                    'Running task %s(%s%s%s) [%s]...',
                    fn_name,
                    format_args(args),
                    ', ' if args and kwargs else '',
                    format_kwargs(kwargs),
                    task['task_id'].decode()
                )
                task_start = datetime.now()
                task['result'] = tasks[fn_name](*args, **kwargs)
                logging.info(
                    'Task %s [%s] returned %s, in %s seconds',
                    fn_name,
                    task['task_id'].decode(),
                    task['result'],
                    (datetime.now() - task_start).total_seconds(),
                )
            except Exception as err:
                pass # TODO: Log the error and return it... Somehow.
            results[task['task_id']] = task
    except KeyboardInterrupt:
        pass


def start_worker_processes(num_workers, modules):
    for mod in modules:
        importlib.import_module(mod)
    for i in range(num_workers):
        Process(target=worker_process, name='Worker-' + str(i)).start()


def start_tcp_server(server_details):

    class TCPHandler(socketserver.BaseRequestHandler):

        def handle(self):
            self.data = self.request.recv(10240).strip()
            request = pickle.loads(self.data)
            logging.debug('Received request: %s', request)
            if request['op'] == 'add':
                # TODO: Use the client address if the client says task is "private".
                task_id = add_task_to_queue(request['task'], self.client_address[0])
                self.request.sendall(pickle.dumps(task_id))
            elif request['op'] == 'poll':
                task_id = request['task_id']
                result = {'task_id': task_id}
                try:
                    task = results[task_id]
                    result_state = {'status': TaskStatus.finished, 'result': task['result']}
                except KeyError:
                    result_state = {'status': TaskStatus.pending, 'result': None}
                result.update(result_state)
                self.request.sendall(pickle.dumps(result))
            elif request['op'] == 'chain':
                # TODO: Ugly!!! Fix on the client side.
                request['task']['parent'] = request['parent']
                task_id = add_task_to_queue(request['task'], self.client_address[0])
                logging.info('Chaining task [%s] to [%s]', task_id, request['task']['parent'])
                self.request.sendall(pickle.dumps(task_id))

    server = socketserver.TCPServer(server_details, TCPHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('Shutting down')
    finally:
        server.shutdown()
