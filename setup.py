#!/usr/bin/env python

from distutils.core import setup
from os import chdir

requirements = open('requirements.txt').read().splitlines()
chdir('ers-local')
setup(name='ERS',
      version='0.12',
      description='Entity Registry System',
      url='https://github.com/ers-devs/ers/',
      packages=['ers'],
      requires = requirements,
      test_suite = "ers.ers.test"
     )
