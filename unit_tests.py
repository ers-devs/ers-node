from ers import ERS
from ers import store
import unittest

TEST_ENTITY = "urn:ers:test"

class APITestCase(unittest.TestCase):
    """
        Basic high-level testing of ERS. Verifies basic functionalities.
    """
    def setUp(self):
        self.ers = ERS()
    def tearDown(self):
        self.ers.reset()

    def testInsertion(self):
        entity = self.ers.get(TEST_ENTITY)
        predicate = "rdf:type"
        value = "ers:TestCase"
        private = False
        entity.add(predicate, value, private)
        self.ers.persist_entity(entity)

        #check if it's persisted
        result = self.ers.entity_exist(TEST_ENTITY)
        self.assertTrue(result)
        #query and verify it is the right one
        #nothing should be in private
        result = entity.get_documents('private')
        self.assertEqual(result, None)

        result = entity.get_documents('public')
        tuples = result.to_tuples()
        self.assertEqual(len(tuples), 1)
        inserted_tuple = tuples[0]

        self.assertEqual(inserted_tuple[0], predicate)
        self.assertEqual(inserted_tuple[1], value)

    def testEntityDeletion(self):
        entity = self.ers.get(TEST_ENTITY)
        predicate = "rdf:type"
        value = "ers:TestCase"
        private = False
        entity.add(predicate, value, private)
        self.ers.persist_entity(entity)

        self.ers.delete_entity(TEST_ENTITY)
        #check if it's deleted
        result = self.ers.entity_exist(TEST_ENTITY)
        self.assertFalse(result)

    def testPropertyDeletion(self):
        entity = self.ers.get(TEST_ENTITY)
        predicate = "rdf:type"
        private = False
        value2 = "ers:SpecialTestCase"
        entity.add(predicate, value2, private)
        self.ers.persist_entity(entity)

        result = entity.get_documents('public')
        tuples = result.to_tuples()
        self.assertEqual(len(tuples), 1)

        entity.delete(predicate, value2)

        result = entity.get_documents('public')
        tuples = result.to_tuples()
        self.assertEqual(len(tuples), 0)


    def testPropertyDeletionTwoValues(self):
        entity = self.ers.get(TEST_ENTITY)
        predicate = "rdf:type"
        value = "ers:TestCase"
        private = False
        entity.add(predicate, value, private)
        value2 = "ers:SpecialTestCase"
        entity.add(predicate, value2, private)
        self.ers.persist_entity(entity)

        result = entity.get_documents('public')
        tuples = result.to_tuples()
        self.assertEqual(len(tuples), 2)

        values = map(lambda x:x[1], tuples)
        self.assertEqual(sorted(values), sorted([value, value2]))

        entity.delete(predicate, value2)

        result = entity.get_documents('public')
        tuples = result.to_tuples()
        self.assertEqual(len(tuples), 1)
        self.assertEqual(tuples[0][1], value)


    def testSearchMatchingMultipleItems(self):
        entity = self.ers.get(TEST_ENTITY)
        predicate = "rdf:type"
        value = "ers:TestCase"
        private = False
        entity.add(predicate, value, private)

        test_entity_2 = "urn:ers:test2"
        entity2 = self.ers.get(test_entity_2)
        entity2.add(predicate,value, private)
        self.ers.persist_entity(entity)
        self.ers.persist_entity(entity2)

        list_of_entities = self.ers.search(predicate, value)
        self.assertEqual(sorted(list_of_entities), sorted([TEST_ENTITY, test_entity_2]))

    def testBadSearch(self):
        entity = self.ers.get(TEST_ENTITY)
        predicate = "rdf:type"
        value = "ers:TestCase"
        private = False
        entity.add(predicate, value, private)

        test_entity_2 = "urn:ers:test2"
        entity2 = self.ers.get(test_entity_2)
        entity2.add(predicate,value, private)
        self.ers.persist_entity(entity)
        self.ers.persist_entity(entity2)

        list_of_entities = self.ers.search(predicate, "random_value")
        self.assertEqual(len(list_of_entities), 0)

    def testSearchSingleItem(self):
        entity = self.ers.get(TEST_ENTITY)
        predicate = "rdf:type"
        value = "ers:TestCase"
        private = False
        entity.add(predicate, value, private)

        test_entity_2 = "urn:ers:test2"
        predicate2 = "rdf:random"
        value2 = "rdf:random_value"

        entity2 = self.ers.get(test_entity_2)
        entity2.add(predicate2, value2, private)
        self.ers.persist_entity(entity)
        self.ers.persist_entity(entity2)

        list_of_entities = self.ers.search(predicate, value)
        self.assertEqual(len(list_of_entities), 1)
        self.assertEqual(list_of_entities[0], TEST_ENTITY)


    def testEntityTwoValues(self):
        entity = self.ers.get(TEST_ENTITY)
        predicate = "rdf:type"
        value = "ers:TestCase"
        private = False
        entity.add(predicate, value, private)

        predicate2 = "rdf:type"
        value2  = "ers:TestCase2"

        entity.add(predicate2, value2, private)

        self.ers.persist_entity(entity)

        list_of_entities = self.ers.search(predicate, value)
        self.assertEqual(len(list_of_entities), 1)
        self.assertEqual(list_of_entities[0], TEST_ENTITY)

        list_of_entities = self.ers.search(predicate2, value2)
        self.assertEqual(len(list_of_entities), 1)
        self.assertEqual(list_of_entities[0], TEST_ENTITY)

    def testMultipleInsertion(self):
        predicate = "rdf:type"
        value = "ers:TestCase"
        private = False
        for i in range(0, 10):
            entity = self.ers.get(TEST_ENTITY + str(i))
            entity.add(predicate, value, private)

            self.ers.persist_entity(entity)

        list_of_entities = self.ers.search(predicate, value)
        self.assertEqual(len(list_of_entities), 10)


if __name__ == '__main__':
    unittest.main()
