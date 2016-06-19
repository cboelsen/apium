from .client import sendmsg


def inspect_worker(address):
    return sendmsg(address, {'op': 'inspect'})


def print_inspected_worker(details):
    if details['tasks']:
        print('Tasks:')
        for task in details['tasks']:
            print('    {}{}'.format(task[0], task[1]))
    if details['schedules']:
        print('Schedules:')
        for schedule in details['schedules']:
            print(schedule)
