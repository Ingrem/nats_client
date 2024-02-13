from setuptools import setup

setup(
    name='nats_contractor',
    version="1.0.0",
    packages=[
        'nats_contractor',
    ],
    install_requires=[
        'asyncio-nats-client',
        'asyncio-nats-streaming',
    ]
)
