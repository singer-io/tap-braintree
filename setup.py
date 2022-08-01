#!/usr/bin/env python

from setuptools import setup

setup(name='tap-braintree',
      version='0.9.1',
      description='Singer.io tap for extracting data from the Braintree API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_braintree'],
      install_requires=[
          'singer-python==5.12.2',
          'requests==2.20.0',
          'braintree==4.16.0',
      ],
      extras_require={
          'dev': [
              'pylint',
              'ipdb',
              'nose',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-braintree=tap_braintree:main
      ''',
      packages=['tap_braintree'],
      package_data = {
          "schemas": ["tap_braintree/schemas/*.json"],
      },
      include_package_data=True,
)
