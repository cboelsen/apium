import sys

from setuptools import setup


requirements = []
if sys.version_info.major < 3:
    requirements += [
        'futures',
        'funcsigs',
    ]


setup(
    entry_points={
        'console_scripts': [
            'apium-worker = apium.command:start_workers',
            'apium-inspect = apium.command:inspect',
        ],
    },
    setup_requires=['pbr>=1.9', 'setuptools>=17.1'],
    install_requires=requirements,
    pbr=True,
)
