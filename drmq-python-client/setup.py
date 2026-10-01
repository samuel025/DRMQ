from setuptools import setup, find_packages

setup(
    name='drmq_client',
    version='1.0.0',
    description='Python SDK for DRMQ',
    author='DRMQ',
    py_modules=['drmq_client', 'messages_pb2'],
    install_requires=[
        'protobuf>=4.21.0'
    ],
)
