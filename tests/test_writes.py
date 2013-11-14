import argparse
import random
import time
from ers import ERSLocal


def rand_digits(count):
    return ''.join((random.choice('0123456789') for _ in xrange(count)))


parser = argparse.ArgumentParser()
parser.add_argument("-i", "--interval", help="average interval between writes (seconds)", type=float, default=10.0)
parser.add_argument("-s", "--stddev", help="standard deviation for above", type=float, default=1.0)
parser.add_argument("-b", "--batch_size", help="number of tuples in a batch", type=int, default=10)
parser.add_argument("-e", "--entities", help="entities in a batch", type=int, default=2)
args = parser.parse_args()

ers = ERSLocal()

print "Doing writes approx every {0} second(s), use CTRL+C to abort".format(args.interval)

while True:
    graph_name = 'urn:ers:testData:graph:' + rand_digits(32)

    entities = ['urn:ers:testData:entity:' + rand_digits(32) for _ in xrange(args.entities)]

    tuples = []
    for _ in xrange(args.batch_size):
        tuples.append((random.choice(entities),
                       'urn:ers:testData:property:' + rand_digits(32),
                       'urn:ers:testData:value:' + rand_digits(32)))

    for e, p, v in tuples:
        ers.add_data(e, p, v, graph_name)

    time.sleep(random.normalvariate(args.interval, args.stddev))
