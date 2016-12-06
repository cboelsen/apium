import sys

from setuptools import setup, find_packages


requirements = []
if sys.version_info.major < 3:
    requirements += [
        'futures',
        'funcsigs',
    ]


setup(
    name='apium',
    version='0.0.3',
    author='Christian Boelsen',
    author_email='christianboelsen+github@gmail.com',
    packages=find_packages(exclude=["tests"]),
    entry_points={
        'console_scripts': [
            'apium-worker = apium.command:start_workers',
            'apium-inspect = apium.command:inspect',
        ],
    },
    license='LGPLv3+',
    description=open('README.rst').read(),
    install_requires=requirements,
    keywords=['celery', 'task', 'queue', 'job', 'async'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Lesser General Public License v3 or later (LGPLv3+)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)
