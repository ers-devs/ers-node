from ers import store
from couchdb.client import Database, Server

from copy import deepcopy
import unittest

class StorageTests(unittest.TestCase):
    def setUp(self):
        self.store = store.ServiceStore()

    def tearDown(self):
        self.store.reset()
        del self.store._server['_replicator']
        self.store._server.create('_replicator')

    def testGetReplicationDocsNoDocs(self):
        result = self.store.replicator_docs()
        self.assertEqual(result, [])

    def testGetReplicationDocsWithoutDocIds(self):
        docs = []
        doc1 = {"_id":"id1", "continous": True, "source":"ers-public", "target":"ers-cache"}
        doc2 = {"_id":"id2", "continous": True, "source":"ers-public", "target":"ers-cache"}
        docs.append(doc1)
        docs.append(doc2)
        self.store.replicator.update(docs)

        result = self.store.replicator_docs()
        sorted_by_id = sorted(result, key=lambda x:x[0])
        self.assertEqual(len(sorted_by_id), 2)
        self.assertEqual(sorted_by_id[0][0], 'id1')
        self.assertEqual(sorted_by_id[0][2], None)
        self.assertEqual(sorted_by_id[1][0], 'id2')
        self.assertEqual(sorted_by_id[1][2], None)

    def testGetReplicationDocsWithDocIds(self):
        docs = []
        doc1 = {"_id":"id1", "continous": True, "source":"ers-public", "target":"ers-cache", "doc_ids":["i1","i2"]}
        doc2 = {"_id":"id2", "continous": True, "source":"ers-public", "target":"ers-cache", "doc_ids":["i3","i4"]}
        doc3 = {"_id":"id3", "continous": True, "source":"ers-public", "target":"ers-cache"}

        docs.append(doc1)
        docs.append(doc2)
        docs.append(doc3)

        self.store.replicator.update(docs)

        result = self.store.replicator_docs()

        sorted_by_id = sorted(result, key=lambda x:x[0])
        self.assertEqual(len(sorted_by_id), 3)
        self.assertEqual(sorted_by_id[0][0], 'id1')
        self.assertEqual(sorted_by_id[0][2], ["i1", "i2"])
        self.assertEqual(sorted_by_id[1][0], 'id2')
        self.assertEqual(sorted_by_id[1][2], ["i3", "i4"])
        self.assertEqual(sorted_by_id[2][0], 'id3')
        self.assertEqual(sorted_by_id[2][2], None)

    def generate_replication_docs(self):
        docs = []
        doc1 = {"_id":"id1", "continous": True, "source":"ers-public", "target":"ers-cache", "doc_ids":["i1","i2"]}
        doc2 = {"_id":"id2", "continous": True, "source":"ers-public", "target":"ers-cache", "doc_ids":["i3","i4"]}
        doc3 = {"_id":"id3", "continous": True, "source":"ers-public", "target":"ers-cache"}

        docs.append(doc1)
        docs.append(doc2)
        docs.append(doc3)
        return docs



    def testUpdateReplicationDocsDeleteOldDocs(self):
        docs = self.generate_replication_docs()

        self.store.replicator.update(deepcopy(docs))

        self.store.update_replicator_docs({})
        result = self.store.replicator_docs()
        self.assertEqual(result, [])

    def testUpdateReplicationDocsAddNewDocs(self):

        doc1 = {"_id":"id1", "continous": True, "source":"ers-public", "target":"ers-cache", "doc_ids":["i1","i2"]}

        doc2 = {"_id":"id2", "continous": True, "source":"ers-public", "target":"ers-cache", "doc_ids":["i3","i4"]}

        docs = {"id1":doc1, "id2":doc2}
        self.store.update_replicator_docs(deepcopy(docs))
        result = self.store.replicator_docs()
        self.assertEqual(len(result), len(docs.values()))

        sorted_by_id = sorted(result, key=lambda x:x[0])

        for i in range(0, len(sorted_by_id)):
            current_doc = sorted_by_id[i]
            self.assertTrue(current_doc[0] in docs)

    def testUpdateReplicationDocsWithoutDocIds(self):
        doc1 = {"_id":"id1", "continous": True, "source":"ers-public", "target":"ers-cache"}

        doc2 = {"_id":"id2", "continous": True, "source":"ers-public", "target":"ers-cache"}
        self.store.replicator.update([doc1, doc2])

        docs = {"id1": doc1, "id2": doc2}
        self.store.update_replicator_docs(deepcopy(docs))

        result = self.store.replicator_docs()
        self.assertEqual(len(result), len(docs.values()))

        sorted_by_id = sorted(result, key=lambda x:x[0])


        for i in range(0, len(sorted_by_id)):
            current_doc = sorted_by_id[i]
            self.assertTrue(current_doc[0] in docs)

    def testUpdateReplicationDocsWithDocIds(self):
        import time
        doc1 = {"_id":"id1", "continous": True, "source":"ers-public", "target":"ers-cache", "doc_ids":["i1","i2"]}

        doc2 = {"_id":"id2", "continous": True, "source":"ers-public", "target":"ers-cache", "doc_ids":["i3","i4"]}
        self.store.replicator.update([doc1, doc2])

        doc1["doc_ids"] = ["id5", "id6"]
        doc2["doc_ids"] = ["id7", "id8"]

        docs = {"id1": doc1, "id2": doc2}
        self.store.update_replicator_docs(deepcopy(docs))

        result = self.store.replicator_docs()
        self.assertEqual(len(result), len(docs.values()))

        sorted_by_id = sorted(result, key=lambda x:x[0])


        for i in range(0, len(sorted_by_id)):
            current_doc = sorted_by_id[i]
            self.assertTrue(current_doc[0] in docs)
            #check that doc ids are updated
            self.assertEqual(current_doc[2], docs[current_doc[0]]['doc_ids'])

    def testUpdateReplicationDocsSameDocIds(self):
        doc1 = {"_id":"id1", "continous": True, "source":"ers-public", "target":"ers-cache", "doc_ids":["i1","i2"]}

        doc2 = {"_id":"id2", "continous": True, "source":"ers-public", "target":"ers-cache", "doc_ids":["i3","i4"]}
        self.store.replicator.update([doc1, doc2])


        docs = {"id1": doc1, "id2": doc2}
        self.store.update_replicator_docs(deepcopy(docs))

        result = self.store.replicator_docs()
        self.assertEqual(len(result), len(docs.values()))

        sorted_by_id = sorted(result, key=lambda x:x[0])


        for i in range(0, len(sorted_by_id)):
            current_doc = sorted_by_id[i]
            self.assertTrue(current_doc[0] in docs)
            #check that doc ids are updated
            self.assertEqual(current_doc[2], docs[current_doc[0]]['doc_ids'])




if __name__ == '__main__':
    unittest.main()
