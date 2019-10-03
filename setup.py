from setuptools import setup

setup(
    name='remote-runner',
    maintainer="Sergei Izmailov",
    maintainer_email="sergei.a.izmailov@gmail.com",
    description="Batch remote execution of python scripts",
    url="https://github.com/sizmailov/pybind11-stubgen",
    version="0.0.0",
    long_description=open("README.rst").read(),
    license="BSD",
    install_requires=[
        'dill',
        'paramiko'
    ]
)