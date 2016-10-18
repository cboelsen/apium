import apium


def chain(*iterables):
    result = []
    for it in iterables:
        for element in it:
            result.append(element)
    return result


apium.register_task(chain)


@apium.register_task
def scheduled_fn(filename):
    with open(filename, 'a') as f:
        f.write('1')
