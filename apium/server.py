import collections
import concurrent.futures
import importlib
import logging
import pickle
import socketserver
import time
import traceback
import uuid

from datetime import datetime
from multiprocessing import Process, Queue

from .client import sendmsg


tasks = {}
futures = {}
schedule_queue = Queue()


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


def run_task(task_id, name, *args, **kwargs):
    logging.info(
        'Running task %s(%s%s%s) [%s]...',
        name,
        format_args(args),
        ', ' if args and kwargs else '',
        format_kwargs(kwargs),
        task_id.decode()
    )
    task_start = datetime.now()
    try:
        result = tasks[name](*args, **kwargs)
        logging.info(
            'Task %s [%s] took %s seconds, and returned %s',
            name,
            task_id.decode(),
            (datetime.now() - task_start).total_seconds(),
            result,
        )
        return result
    except:
        logging.info(
            'Task %s [%s] took %s seconds, and raised an exception:\n%s',
            name,
            task_id.decode(),
            (datetime.now() - task_start).total_seconds(),
            traceback.format_exc(),
        )
        raise


def scheduler_process(address, interval, schedule_queue):
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
                        sendmsg(address, {'op': 'submit', 'task': {'name': fn_name, 'args': args, 'kwargs': kwargs}})
            sleep_duration = max(interval - (datetime.now() - start_time).total_seconds(), 0.1)
            time.sleep(sleep_duration)
    except KeyboardInterrupt:
        pass


def get_future_state(future):
    with future._condition:
        return {
            'state': future._state,
            'result': future._result,
            'exception': future._exception,
        }


def run_workers(address, modules, num_workers, interval):

    executor = concurrent.futures.ProcessPoolExecutor(max_workers=num_workers)

    for mod in modules:
        importlib.import_module(mod)

    scheduler = Process(target=scheduler_process, args=(address, interval, schedule_queue), name='Scheduler')

    class TCPHandler(socketserver.BaseRequestHandler):

        def handle(self):
            self.data = self.request.recv(10240).strip()
            request = pickle.loads(self.data)
            logging.debug('Received request: %s', request)
            # TODO: Check credentials
            # TODO: Encrypt credentials
            if request['op'] == 'submit':
                # TODO: Use the client address if the client says task is "private".
                task = request['task']
                if task['name'] not in tasks:
                    pass  # TODO: Return exception.
                task['client'] = self.client_address[0]
                task_id = str(uuid.uuid4()).encode()
                task['id'] = task_id
                logging.debug(
                    'Adding task %s(%s%s%s) [%s]%s to queue',
                    task['name'],
                    format_args(task['args']),
                    ', ' if task['args'] and task['kwargs'] else '',
                    format_kwargs(task['kwargs']),
                    task_id.decode(),
                    ' from {}'.format(task['client']) if task['client'] else ''
                )
                futures[task_id] = executor.submit(run_task, task_id, task['name'], *task['args'], **task['kwargs'])
                self.request.sendall(pickle.dumps(task))
            elif request['op'] == 'cancel':
                task_id = request['id']
                future = futures[task_id]
                response = future.cancel()
                if response:
                    logging.debug('Cancelled task [%s]', task_id)
                else:
                    logging.debug('Failed to cancel task [%s]', task_id)
                state = get_future_state(future)
                state['response'] = response
                self.request.sendall(pickle.dumps(state))
            elif request['op'] == 'poll':
                task_id = request['id']
                future = futures[task_id]
                state = get_future_state(future)
                logging.debug('Responding to poll for task [%s] with: %s', task_id, state)
                self.request.sendall(pickle.dumps(state))

    server = socketserver.ThreadingTCPServer(address, TCPHandler)

    with executor:
        scheduler.start()
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print('KeyboardInterrupt')
        finally:
            scheduler.join()
            server.shutdown()
            server.server_close()
