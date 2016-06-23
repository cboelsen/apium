def format_fn(name, args, kwargs):

    def format_args():
        return ', '.join([repr(a) for a in (args or ())])

    def format_kwargs():
        return ', '.join([str(k) + '=' + repr(v) for k, v in (kwargs or {}).items()])

    return '{}({}{}{})'.format(
        name,
        format_args(),
        ', ' if args and kwargs else '',
        format_kwargs(),
    )
