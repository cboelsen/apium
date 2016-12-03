import logging
import os


DJANGO_SETTINGS_ENV_VAR = 'DJANGO_SETTINGS_MODULE'


def setup_django():
    try:
        import django
        django.setup()
        from django.core.checks import run_checks
        run_checks()
    except ImportError as error:
        logging.error(
            'Found {} in environment, but couldn\'t import django: {}'.format(DJANGO_SETTINGS_ENV_VAR, error)
        )
        raise


def setup():
    if DJANGO_SETTINGS_ENV_VAR in os.environ:
        setup_django()
