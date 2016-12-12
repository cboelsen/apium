# -*- coding: utf-8 -*-

"""
.. module: command
    :synopsis: Functions for use as console entry points.
"""


__all__ = ('start_workers', 'inspect')


import argparse
import logging
import multiprocessing

from . import frameworks, worker
from .client import DEFAULT_PORT
from .inspect import print_inspected_worker, inspect_worker


def _setup_logging(args):
    log_format = '[%(asctime)-15s: %(levelname)+8s/%(processName)+11s]  %(message)s'
    log_kwargs = {
        'format': log_format,
        'level': logging.DEBUG if args.debug else logging.INFO
    }
    if args.logfile:
        log_kwargs['filename'] = args.logfile
    logging.basicConfig(**log_kwargs)


def start_workers():
    """Launch workers, a scheduler and a TCP server."""
    parser = argparse.ArgumentParser(description='Start workers to run the given tasks.')
    parser.add_argument('modules', nargs='+', help='Modules containing registered tasks.')

    def _num_workers_type(num):
        num = int(num)
        if num <= 0:
            raise argparse.ArgumentTypeError("The number of workers must be greater than 0")
        return num

    default_num_workers = multiprocessing.cpu_count()
    parser.add_argument(
        '-n', '--num-workers',
        dest='num_workers', type=_num_workers_type, default=default_num_workers,
        help='The number of worker processes to start (default {})'.format(default_num_workers),
    )

    default_bind = 'localhost:{}'.format(DEFAULT_PORT)
    parser.add_argument(
        '-b', '--bind',
        dest='bind', default=default_bind,
        help='The address and port to bind the TCP server to (default {})'.format(default_bind),
    )

    default_interval = 1
    parser.add_argument(
        '-i', '--interval',
        dest='interval', type=float, default=default_interval,
        help='How often the scheduler polls for scheduled tasks in seconds (default {})'.format(default_interval),
    )

    parser.add_argument(
        '-l', '--logfile',
        dest='logfile', default=None,
        help='A filename to write the logs to (default stdout)',
    )

    parser.add_argument(
        '-u', '--username',
        dest='username', default='',
        help='The username to use for authentication (default "")',
    )

    parser.add_argument(
        '-p', '--password',
        dest='password', default='',
        help='The password to use for authentication (default "")',
    )

    parser.add_argument(
        '--debug',
        dest='debug', action='store_const', default=False, const=True,
        help='Show debug logs',
    )

    args = parser.parse_args()
    _setup_logging(args)
    server, port = args.bind.rsplit(':', 1)
    address = (server, int(port))

    frameworks.setup()

    with worker.create_workers(args.modules, args.num_workers) as workers:
        with worker.setup_tcp_server(address, workers) as tcp_server:
            with worker.setup_scheduler(address, args.interval):
                logging.debug('Starting TCP server')
                tcp_server.serve_forever()


def inspect():
    """Print the state of the tasks and their schedules to the console."""
    parser = argparse.ArgumentParser(description='Inspect the task queue of the given workers.')

    default_bind = 'localhost:{}'.format(DEFAULT_PORT)
    parser.add_argument(
        '-c', '--connect',
        dest='connect', default=default_bind,
        help='The address and port to bind the TCP server to (default {})'.format(default_bind),
    )

    parser.add_argument(
        '-u', '--username',
        dest='username', default='',
        help='The username to use for authentication (default "")',
    )

    parser.add_argument(
        '-p', '--password',
        dest='password', default='',
        help='The password to use for authentication (default "")',
    )

    args = parser.parse_args()
    server, port = args.connect.rsplit(':', 1)
    address = (server, int(port))

    print_inspected_worker(inspect_worker(address))
