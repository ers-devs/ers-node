from ers import ERS
import time
import uuid
from ers import store
import unittest


class APITestCase(unittest.TestCase):
    def setUp(self):
        self.ers = ERS()
    def tearDown(self):
        self.ers.reset()
    def testEntityLoad(self):
        test_range = 10
        start_time = time.time()

        for i in xrange(0, test_range):
            test_entity = str(uuid.uuid4())
            entity = self.ers.get(test_entity)
            predicate = "rdf:type"
            value = "ers:TestCase"
            private = False
            entity.add(predicate, value, private)
            self.ers.persist_entity(entity)

        end_time = time.time()
        print("Loaded %d entities in public store. time was %g seconds" % (test_range, end_time - start_time))

    def testValLoad(self):
        test_range = 10
        test_entity = 'urn:ers:test'
        entity = self.ers.get(test_entity)
        start_time = time.time()
        for i in xrange(0, test_range):
            predicate = "rdf:type"
            value = str(uuid.uuid4())
            private = False
            entity.add(predicate, value, private)
            self.ers.persist_entity(entity)

        end_time = time.time()
        print("Loaded %d values to entity in public store. time was %g seconds" % (test_range, end_time - start_time))

    def testPredLoad(self):
        test_range = 10
        test_entity = 'urn:ers:test'
        entity = self.ers.get(test_entity)
        start_time = time.time()
        for i in xrange(0, test_range):
            predicate = str(uuid.uuid4())
            value = "ers:random"
            private = False
            entity.add(predicate, value, private)
            self.ers.persist_entity(entity)

        end_time = time.time()
        print("Loaded %d properties to entity in public store. time was %g seconds" % (test_range, end_time - start_time))



if __name__ == '__main__':
    unittest.main()
