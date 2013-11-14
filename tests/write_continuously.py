#!/usr/bin/env python

import sys, os; sys.path.extend(['../../ers-local', '/Users/marat/projects/wikireg/site-packages'])
import random, platform
import ers
from time import time, sleep

registry = ers.ERSLocal(reset_database=True)
node = platform.node()

def random_urn():
    return "urn:ers:testdata:{0}".format(random.randint(0, 10e10))

def loop_write(run_time=1, delay=0):
	t0 = time()
	while time() - t0 < run_time:
		registry.add_data(random_urn(), random_urn(), "test", node)
		sleep(delay)

if __name__ == '__main__':
	loop_write(*map(float, sys.argv[1:]))