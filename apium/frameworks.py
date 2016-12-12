# -*- coding: utf-8 -*-

"""
.. module: frameworks
    :synopsis: Functions to ease integration with various frameworks.
"""


__all__ = ('setup', )


import logging
import os


DJANGO_SETTINGS_ENV_VAR = 'DJANGO_SETTINGS_MODULE'


def _setup_django():
    try:
        import django
        django.setup()
        from django.core.checks import run_checks
        run_checks()
    except ImportError as error:
        logging.error('Found %s in environment, but couldn\'t import django: %s', DJANGO_SETTINGS_ENV_VAR, error)
        raise


def setup():
    """Check if any frameworks appear to be in use, and set them up."""
    if DJANGO_SETTINGS_ENV_VAR in os.environ:
        _setup_django()
