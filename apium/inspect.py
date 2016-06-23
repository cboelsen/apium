from .client import sendmsg
from .utils import format_fn


def inspect_worker(address):
    return sendmsg(address, {'op': 'inspect'})


def print_inspected_worker(details):
    if details['tasks']:
        print('Registered tasks:')
        for task_name, task_sig in details['tasks'].items():
            print('  * {}{}'.format(task_name, task_sig))
        print()

    if details['running']:
        print('Running tasks:')
        for task in details['running']:
            print('  * {}:   {}'.format(task['id'].decode(), format_fn(task['name'], task['args'], task['kwargs'])))
        print()

    if details['schedules']:
        single_sched = {t: s for t, s in details['schedules'].items() if [sch for sch in s if not sch[1]]}
        repeat_sched = {t: s for t, s in details['schedules'].items() if [sch for sch in s if sch[1]]}

        if single_sched:
            print('Scheduled tasks:')
            for task_name, schedules in single_sched.items():
                for s in schedules:
                    if not s[1]:
                        print('  * {}:\tScheduled for {}'.format(
                            format_fn(task_name, s[2], s[3]),
                            s[0].isoformat(' '),
                        ))
            print()

        if repeat_sched:
            print('Recurring tasks:')
            for task_name, schedules in repeat_sched.items():
                for s in schedules:
                    if s[1]:
                        print('  * {}:\tScheduled for {}; repeating every {}'.format(
                            format_fn(task_name, s[2], s[3]),
                            s[0].isoformat(' '),
                            str(s[1]),
                        ))
            print()
