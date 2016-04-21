import apium
import itertools


apium.register_task(itertools.chain)


@apium.register_task
def scheduled_fn(filename):
    with open(filename, 'a') as f:
        f.write('1')
