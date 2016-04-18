import collections
import concurrent.futures
import contextlib
import functools
import importlib
import logging
import pickle
import socketserver
import time
import traceback
import uuid

from datetime import datetime
from multiprocessing import Process, Queue, Lock

from .client import sendmsg
from .exceptions import RemoteException


tasks = {}
futures = {}
chains = collections.defaultdict(list)
chains_lock = Lock()
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
            'Scheduling task %s for %s, repeating every %s',
            format_task({'name': fn_name, 'args': args, 'kwargs': kwargs}),
            start_when or datetime.now(),
            repeat_every
        )
        schedule = (start_when or datetime.now(), repeat_every, args or (), kwargs or {})
        schedule_queue.put((fn_name, schedule))
        return register_task(fn)
    return _schedule_task


def format_task(task, chain=False):
    if chain:
        task = task.copy()
        task['args'] = ('X', ) + task['args']

    def format_args(args):
        return ', '.join([repr(a) for a in (args or ())])

    def format_kwargs(kwargs):
        return ', '.join([str(k) + '=' + repr(v) for k, v in (kwargs or {}).items()])

    return '{}({}{}{}){}{}'.format(
        task['name'],
        format_args(task['args']),
        ', ' if task['args'] and task['kwargs'] else '',
        format_kwargs(task['kwargs']),
        ' [{}]'.format(task['id'].decode()) if task.get('id') else '',
        ' from {}'.format(task['client']) if task.get('client') else '',
    )


def run_task(task):
    formatted_task = format_task(task)
    logging.info('Running task %s...', formatted_task)
    task_start = datetime.now()
    try:
        result = tasks[task['name']](*task['args'], **task['kwargs'])
        logging.info(
            'Task %s took %s seconds, and returned %s',
            formatted_task,
            (datetime.now() - task_start).total_seconds(),
            result,
        )
        return result
    except Exception as err:
        logging.info(
            'Task %s took %s seconds, and raised an exception:\n%s',
            formatted_task,
            (datetime.now() - task_start).total_seconds(),
            traceback.format_exc(),
        )
        remote_exc = RemoteException(traceback.format_exc())
        # TODO: This doesn't work if the client hasn't loaded the same modules.
        # remote_exc.exception = err
        raise remote_exc


def scheduler_process(address, interval, schedule_queue):
    schedules = collections.defaultdict(list)
    try:
        while True:
            start_time = datetime.now()
            while not schedule_queue.empty():
                new_schedule = schedule_queue.get(False)
                if new_schedule is None:
                    return
                fn_name, schedule = new_schedule
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
    if future is None:
        return {}
    with future._condition:
        return {
            'state': future._state,
            'result': future._result,
            'exception': future._exception,
        }


def check_for_chain(parent_id, executor, parent_future):
    try:
        with chains_lock:
            for task in chains[parent_id]:
                add_task(executor, task, parent_future)
            del chains[parent_id]
    except KeyError:
        pass


def future_values_fall_through(task_id, parent_future):
    future = concurrent.futures.Future()
    future._state = parent_future._state
    future._result = parent_future._result
    future._exception = parent_future._exception
    futures[task_id] = future


def submit_new_task(executor, task):
    future = executor.submit(run_task, task)
    future.add_done_callback(functools.partial(check_for_chain, task['id'], executor))
    futures[task['id']] = future


def add_task(executor, task, parent_future):
    try:
        if parent_future:
            task['args'] = (parent_future.result(), ) + task['args']
    except BaseException as err:
        if task['type'] == 'catch':
            task['args'] = (err, ) + task['args']
            submit_new_task(executor, task)
        else:
            future_values_fall_through(task['id'], parent_future)
    else:
        if task['type'] == 'then':
            submit_new_task(executor, task)
        else:
            future_values_fall_through(task['id'], parent_future)


@contextlib.contextmanager
def create_workers(address, modules, num_workers, interval):

    executor = concurrent.futures.ProcessPoolExecutor(max_workers=num_workers)

    for mod in modules:
        importlib.import_module(mod)

    scheduler = Process(target=scheduler_process, args=(address, interval, schedule_queue), name='Scheduler')

    class TCPHandler(socketserver.BaseRequestHandler):

        def handle(self):
            self.data = self.request.recv(10240).strip()
            request = pickle.loads(self.data)
            logging.debug('Received request from %s: %s', self.client_address[0], request)
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
                logging.debug('Adding task %s to queue', format_task(task))
                submit_new_task(executor, task)
                self.request.sendall(pickle.dumps(task))
            elif request['op'] == 'cancel':
                task_id = request['id']
                future = futures[task_id]
                if future is not None:
                    response = future.cancel()
                else:
                    response = False
                    for children in chains.values():
                        for child in children:
                            if child['id'] == task_id:
                                response = True
                                children.remove(child)
                if response:
                    logging.debug('Cancelled task [%s]', task_id.decode())
                else:
                    logging.debug('Failed to cancel task [%s]', task_id.decode())
                state = get_future_state(future)
                state['response'] = response
                self.request.sendall(pickle.dumps(state))
            elif request['op'] == 'poll':
                task_id = request['id']
                future = futures[task_id]
                state = get_future_state(future)
                logging.debug('Responding to poll for task [%s] with: %s', task_id.decode(), state)
                self.request.sendall(pickle.dumps(state))
            elif request['op'] == 'chain':
                # TODO: Use the client address if the client says task is "private".
                task = request['task']
                parent_id = request['parent']
                if task['name'] not in tasks:
                    pass  # TODO: Return exception.
                task['client'] = self.client_address[0]
                task_id = str(uuid.uuid4()).encode()
                task['id'] = task_id
                try:
                    future = futures[parent_id]
                    if future is not None and future.done():
                        add_task(executor, task, future)
                    else:
                        with chains_lock:
                            chains[parent_id].append(task)
                        futures[task_id] = None
                except KeyError:
                    with chains_lock:
                        chains[parent_id].append(task)
                    futures[task_id] = None
                logging.debug('Chaining task %s to [%s]', format_task(task, chain=True), parent_id.decode())
                self.request.sendall(pickle.dumps(task))

    server = socketserver.ThreadingTCPServer(address, TCPHandler)

    with executor:
        scheduler.start()
        try:
            yield server
        except KeyboardInterrupt:
            print('KeyboardInterrupt')
        finally:
            schedule_queue.put(None)
            scheduler.join()
            server.shutdown()
            server.server_close()


def main(*args, **kwargs):
    with create_workers(*args, **kwargs) as workers:
        workers.serve_forever()