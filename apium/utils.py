# -*- coding: utf-8 -*-

"""
.. module: utils
    :synopsis: A collection of useful, cross-module functions.
"""


__all__ = ('format_fn', )


def format_fn(name, args, kwargs):
    """Return a string formatting the given function name, args and kwargs.

    Outputted string resembles a function call in python:

    .. doctest::

        >>> format_fn('fn_name', (1, 2, 'c'), {'some': 24, 'another': 'blah'})
        "fn_name(1, 2, 'c', some=24, another='blah')"

    :param name: The name of the function.
    :type name: str
    :param args: The arguments to pass to the function.
    :type args: tuple
    :param kwargs: The keyword arguments to pass to the function.
    :type kwargs: dict
    :returns: A string representation of a function call.
    :rtype: str
    """

    def _format_args():
        return ', '.join([repr(a) for a in args or ()])

    def _format_kwargs():
        return ', '.join([str(k) + '=' + repr(v) for k, v in (kwargs or {}).items()])

    return '{}({}{}{})'.format(
        name,
        _format_args(),
        ', ' if args and kwargs else '',
        _format_kwargs(),
    )
