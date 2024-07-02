#!/usr/bin/env python

"""The setup script."""

from setuptools import find_packages, setup

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('requirements.txt') as req_file:
    requirements = [req.strip() for req in req_file.readlines()]

with open('requirements_dev.txt') as req_dev_file:
    lines = req_dev_file.readlines()[1:]  # skip -r requirements.txt
    requirements_dev = requirements + [req.strip() for req in lines]

setup(
    author='Sergey Dubovitsky',
    author_email='sergey.dubovitsky@piklema.com',
    python_requires='>=3.10',
    classifiers=[
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    description='MQTT Kafka Connector',
    entry_points={
        'console_scripts': [
            'mqtt_kafka_connector=mqtt_kafka_connector.connector.main:main',
        ],
    },
    install_requires=requirements,
    long_description=readme,
    include_package_data=True,
    keywords='mqtt_kafka_connector',
    name='mqtt_kafka_connector',
    packages=find_packages(
        include=['src'],
    ),
    package_dir={'': 'src'},
    extras_require={
        'develop': requirements_dev,
    },
    url='https://github.com/piklema/mqtt-kafka-connector',
    version='0.0.1',
    zip_safe=False,
)
