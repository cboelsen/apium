from setuptools import setup, find_packages


setup(
    name='apium',
    version='0.0.1',
    author='Christian Boelsen',
    author_email='christianboelsen+github@gmail.com',
    packages = find_packages(exclude=["tests"]),
    entry_points = {
        'console_scripts': [
            'apium-worker = apium.command:start_workers',
             # TODO: Add script to inspect queue state.
        ],
    },
    license='LICENSE',
    description=open('README').read(),
    install_requires=[],
)
