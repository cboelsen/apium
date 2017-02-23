# -*- coding: utf-8 -*-

"""
.. module: inspect
    :synopsis: Inspect the state of the tasks and their schedules.
"""


__all__ = ('inspect_worker', 'print_inspected_worker')


from .client import sendmsg
from .utils import format_fn


def inspect_worker(address):
    """Return a dict containing the state of the tasks and their schedules."""
    return sendmsg(address, {'op': 'inspect'})


def _print_singly_scheduled_tasks(tasks_and_schedules):
    print('Scheduled tasks:')
    for task_name, schedules in tasks_and_schedules.items():
        for schedule in schedules:
            if not schedule[1]:
                print('  * {}:\tScheduled for {}'.format(
                    format_fn(task_name, schedule[2], schedule[3]),
                    schedule[0].isoformat(' '),
                ))
    print('')


def _print_repeatedly_scheduled_tasks(tasks_and_schedules):
    print('Recurring tasks:')
    for task_name, schedules in tasks_and_schedules.items():
        for schedule in schedules:
            if schedule[1]:
                print('  * {}:\tScheduled for {}; repeating every {}'.format(
                    format_fn(task_name, schedule[2], schedule[3]),
                    schedule[0].isoformat(' '),
                    str(schedule[1]),
                ))
    print('')


def print_inspected_worker(details):
    """Print the state of the tasks and their schedules to the console."""
    if details['tasks']:
        print('Registered tasks:')
        for task_name, task_sig in details['tasks'].items():
            print('  * {}{}'.format(task_name, task_sig))
        print('')

    if details['running']:
        print('Running tasks:')
        for task in details['running']:
            if task is not None:
                task_id = task['id'].decode()
                print('  * {}:   {}'.format(task_id, format_fn(task['name'], task['args'], task['kwargs'])))
        print('')

    if details['schedules']:
        single_sched = {t: s for t, s in details['schedules'].items() if [sch for sch in s if not sch[1]]}
        repeat_sched = {t: s for t, s in details['schedules'].items() if [sch for sch in s if sch[1]]}

        if single_sched:
            _print_singly_scheduled_tasks(single_sched)

        if repeat_sched:
            _print_repeatedly_scheduled_tasks(repeat_sched)
