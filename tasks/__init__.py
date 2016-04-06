import apium
import time

from datetime import datetime, timedelta


@apium.register_task
def add(*args):
    time.sleep(2)
    return sum(args)


@apium.schedule_task(repeat_every=timedelta(seconds=10), args=(1, 2, 3))
def mul(*args):
    val = 1
    for arg in args:
        val *= arg
    return val


@apium.schedule_task()
@apium.schedule_task(datetime.now() + timedelta(seconds=30), timedelta(seconds=2))
def printer1():
    return 'printer1'


@apium.register_task
def raiser():
    raise Exception('Example exception!!')
