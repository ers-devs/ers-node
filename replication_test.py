import requests
import time
import json
import unittest

webserver_port = "5000"

node1_url = "http://172.28.128.165" + ':' + webserver_port
node2_url = "http://172.28.128.166" + ':' + webserver_port
test_entity = "urn:ers:test"



class ReplicationTestCase(unittest.TestCase):
    """
        Testing of replication performance of ERS
    """

    def setUp(self):
        pass

    def tearDown(self):
        #clean db, but without "peers"
        pass

    def testSameDocPropertyReplication(self):
        #check to see that the web apis are running on the nodes
        resp1 = requests.get(node1_url + '/')
        self.assertEqual(resp1.status_code, 200)
        self.assertTrue(resp1.content.startswith('ERS web interface running'))

        resp2 = requests.get(node2_url + '/')
        self.assertEqual(resp2.status_code, 200)
        self.assertTrue(resp2.content.startswith('ERS web interface running'))

        #clean_db
        resp1 = requests.get(node1_url + '/ResetDb')
        resp1 = requests.get(node2_url + '/ResetDb')

        #add statements
        resp1 = requests.get(node1_url + '/AddStatement/' + test_entity + '/rdf:type/foaf:LocalAgent')
        resp2 = requests.get(node2_url + '/AddStatement/' + test_entity + '/rdf:type/foaf:RemoteAgent')

        #update replication links to link the doc on node 2 public to doc on node1 cache
        #resp = requests.get(node1_url + '/Get/' + test_entity)
        #resp = requests.get(node1_url + '/CacheEntity/' + test_entity)



        # !!! important: initial replication is slow. After the first one, things go pretty smoothly.
        # figure out a way to test ^
        initial_statements = 2
        replication_statements = 3
        req_start = time.time()
        for i in range(0, replication_statements):
            requests.get(node2_url + '/AddStatement/' + test_entity + '/rdf:type/foaf:ReplicationAgent' + str(i))

        resp = requests.get(node1_url + '/ShowEntity/' + test_entity)
        print resp.content

        #the public statements on node2 should be in node1's cache
        while True:
            # query node1's cache
            cache_contents = requests.get(node1_url + '/ShowDBDocument/ers-cache/' + test_entity)
            result = json.loads(cache_contents.content)
            query_time = time.time()
            print "Result after " + str(query_time - req_start)
            print len(result)
            print ""

            if len(result) == initial_statements + replication_statements:
                print "Done"
                break


if __name__ == '__main__':
        unittest.main()

