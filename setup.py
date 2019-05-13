#!/usr/bin/env python

from setuptools import setup

setup(name='tap-braintree',
      version='0.8.3',
      description='Singer.io tap for extracting data from the Braintree API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_braintree'],
      install_requires=[
          'singer-python==5.5.0',
          'requests==2.20.0',
          'braintree==3.53.0',
      ],
      entry_points='''
          [console_scripts]
          tap-braintree=tap_braintree:main
      ''',
      packages=['tap_braintree'],
      package_data = {
          'tap_braintree/schemas': [
              'transactions.json',
          ],
      },
      include_package_data=True,
)
