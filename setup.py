#!/usr/bin/env python
from distutils.core import setup

setup(name='redstreak',
      version='0.1',
      description="""
an example database
Written for Bradfield CS database course
Spring 2018 by Lady Red
""",
      author='Chris Beacham/Lady Red',
      author_email='mcscope@gmail.com',
      url='https://github.com/mcscope/liteup/',
      packages=['redstreak'],
      install_requires=[
          "attrs",
      ]
      )
