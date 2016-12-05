from setuptools import setup


setup(
    entry_points={
        'console_scripts': [
            'apium-worker = apium.command:start_workers',
            'apium-inspect = apium.command:inspect',
        ],
    },
    setup_requires=['pbr>=1.9', 'setuptools>=17.1'],
    pbr=True,
)
