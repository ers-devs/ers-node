#!/usr/bin/env python

from distutils.core import setup

requirements = open('requirements.txt').read().splitlines()
setup(name='ERS',
      version='0.12',
      description='Entity Registry System',
      url='https://github.com/ers-devs/ers-node/',
      packages=['ers'],
      requires = requirements
     )
