from setuptools import setup

setup(
    name='remote-runner',
    maintainer="Sergei Izmailov",
    maintainer_email="sergei.a.izmailov@gmail.com",
    description="Batch remote execution of python scripts",
    url="https://github.com/sizmailov/remote-runner",
    version="0.1.0",
    long_description=open("README.rst").read(),
    license="BSD",
    install_requires=[
        'dill',
        'paramiko'
    ],
    tests_require=[
        'pytest'
    ],
    packages=[
        'remote_runner'
    ]
)
