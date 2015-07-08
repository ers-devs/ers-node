import requests
import concurrent.futures
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
        # start daemons
        resp1 = requests.get(node1_url + '/StartDaemon')
        self.assertEqual(resp1.status_code, 200)
        d1_pid = resp1.content
        print "daemon started with pid " + d1_pid

        resp2 = requests.get(node2_url + '/StartDaemon')
        self.assertEqual(resp2.status_code, 200)
        d2_pid = resp2.content
        print "daemon started with pid " + d2_pid

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
        document_id = resp2.content.split()[-2]

        #update replication links to link the doc on node 2 public to doc on node1 cache
        #resp = requests.get(node1_url + '/Get/' + test_entity)
        #resp = requests.get(node1_url + '/CacheEntity/' + test_entity)

        # !!! important: initial replication is slow. After the first one, things go pretty smoothly.
        initial_statements = 2
        replication_statements = 100

        both_working = 10
        node2_alone = 25
        total_time = 30

        executor = concurrent.futures.ProcessPoolExecutor(max_workers=1)
        req_start = time.time()
        pred =[]
        vals = []
        for i in range(0, replication_statements):
            pred.append('rdf:type')
            vals.append('foaf:RemoteAgent' + str(i))

        url = node2_url + '/BatchAddStatement/' + test_entity +'/'
        data = json.dumps({'predicates':pred, 'values':vals})

        future = executor.submit(requests.post,url,data)


        #the public statements on node2 should be in node1's cache
        #run for 10 seconds
        while time.time() - req_start < both_working:
            url = node2_url + '/ShowDoc/ers-public/' + document_id + '/rdf:type'
            resp = requests.get(url)
            nr_values = int( resp.content)
            print "node 2 public after {} nr_values:{} ".format(time.time() - req_start, nr_values)

            url = node1_url + '/ShowDoc/ers-cache/' + document_id + '/rdf:type'
            resp = requests.get(url)
            nr_values = int( resp.content)
            print "node 1 cache after {} nr_values:{} ".format(time.time() - req_start, nr_values)
            time.sleep(0.5)

        #stop node 1
        resp = requests.get(node1_url + '/StopDaemon/' + d1_pid)

        #wait 4 seconds
        #start node 1
        while time.time() - req_start < node2_alone:
            url = node2_url + '/ShowDoc/ers-public/' + document_id + '/rdf:type'
            resp = requests.get(url)
            nr_values = int( resp.content)
            print "node 2 public after {} nr_values:{} ".format(time.time() - req_start, nr_values)

            url = node1_url + '/ShowDoc/ers-cache/' + document_id + '/rdf:type'
            resp = requests.get(url)
            nr_values = int( resp.content)
            print "node 1 cache after {} nr_values:{} ".format(time.time() - req_start, nr_values)
            time.sleep(0.5)


        resp1 = requests.get(node1_url + '/StartDaemon')
        self.assertEqual(resp1.status_code, 200)
        d1_pid = resp1.content

        #stop after 30 seconds
        while time.time() - req_start < total_time:
            url = node2_url + '/ShowDoc/ers-public/' + document_id + '/rdf:type'
            resp = requests.get(url)
            nr_values = int( resp.content)
            print "node 2 public after {} nr_values:{} ".format(time.time() - req_start, nr_values)

            url = node1_url + '/ShowDoc/ers-cache/' + document_id + '/rdf:type'
            resp = requests.get(url)
            nr_values = int( resp.content)
            print "node 1 cache after {} nr_values:{} ".format(time.time() - req_start, nr_values)
            time.sleep(0.5)

            # query node1's cache
            #cache_contents = requests.get(node1_url + '/ShowDBDocument/ers-cache/' + test_entity)
            #result = json.loads(cache_contents.content)
            #query_time = time.time()
            #print "Result cache after " + str(query_time - req_start)
            #print len(result)
            #print ""

            if nr_values == replication_statements + 1:
                print "Done"
                break

        resp = requests.get(node1_url + '/StopDaemon/' + d1_pid)
        resp = requests.get(node2_url + '/StopDaemon/' + d2_pid)

if __name__ == '__main__':
        unittest.main()

