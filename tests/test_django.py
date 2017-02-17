import os

import apium.frameworks


def test_basic_task_run___state_is_consistent(port_num, running_worker):
    os.environ['DJANGO_SETTINGS_MODULE'] = 'django_test_project.settings'
    apium.frameworks.setup()
