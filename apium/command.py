import argparse
import logging
import multiprocessing

from . import server
from .client import DEFAULT_PORT


def setup_logging(args):
    FORMAT = '[%(asctime)-15s: %(levelname)+8s/%(processName)+11s]  %(message)s'
    log_kwargs = {
        'format': FORMAT,
        'level': logging.DEBUG if args.debug else logging.INFO
    }
    if args.logfile:
        log_kwargs['filename'] = args.logfile
    logging.basicConfig(**log_kwargs)


def start_workers():
    parser = argparse.ArgumentParser(description='Start workers to run the given tasks.')
    parser.add_argument('modules', nargs='+', help='Modules containing registered tasks.')

    def num_workers_type(x):
        x = int(x)
        if x <= 0:
            raise argparse.ArgumentTypeError("The number of workers must be greater than 0")
        return x

    DEFAULT_NUM_WORKERS = multiprocessing.cpu_count()
    parser.add_argument(
        '-n', '--num-workers',
        dest='num_workers', type=num_workers_type, default=DEFAULT_NUM_WORKERS,
        help='The number of worker processes to start (default {})'.format(DEFAULT_NUM_WORKERS),
    )

    DEFAULT_BIND = 'localhost:{}'.format(DEFAULT_PORT)
    parser.add_argument(
        '-b', '--bind',
        dest='bind', default=DEFAULT_BIND,
        help='The address and port to bind the TCP server to (default {})'.format(DEFAULT_BIND),
    )

    DEFAULT_INTERVAL = 1
    parser.add_argument(
        '-i', '--interval',
        dest='interval', type=float, default=DEFAULT_INTERVAL,
        help='How often the scheduler should poll for scheduled tasks (default {})'.format(DEFAULT_INTERVAL),
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
    setup_logging(args)
    server_ip, port = args.bind.rsplit(':', 1)
    address = (server_ip, int(port))

    server.run_workers(address, args.modules, args.num_workers, args.interval)
