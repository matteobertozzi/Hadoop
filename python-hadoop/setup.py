#!/usr/bin/env python

from distutils.core import setup

setup(name='Hadoop',
      version='0.1',
      description='Python Hadoop I/O Utilities',
      license="Apache Software License 2.0 (ASF)",
      author='Matteo Bertozzi',
      author_email='theo.bertozzi@gmail.com',
      url='http://hadoop.apache.org',
      packages=["hadoop", 'hadoop.util', 'hadoop.io', 'hadoop.io.compress']
     )

