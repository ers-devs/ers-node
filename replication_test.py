import requests
from multiprocessing import Process
import concurrent.futures
import time
import json
import unittest

webserver_port = "5000"

node1_url = "http://172.28.128.165" + ':' + webserver_port
node2_url = "http://172.28.128.166" + ':' + webserver_port
bridge_url = "http://172.28.128.170" + ':' + webserver_port
test_entity = "urn:ers:test"

def get_nr_documents(entity_id, predicate, base_url, database_name):
    # helper function
    url = base_url + '/ShowDoc/' + database_name + '/' +entity_id + '/' + predicate
    resp = requests.get(url)
    nr_values = int(resp.content)
    return nr_values
def service_store_access(base_url):
    url = base_url + '/ServiceStoreAccess'
    resp = requests.get(url)
    return True


class ReplicationTestCase(unittest.TestCase):
    """
        Testing of replication performance of ERS
    """

    def setUp(self):
        pass

    def tearDown(self):
        #clean db, but without "peers"
        pass

    def testBridgeReplication(self):
        resp3 = requests.get(bridge_url + '/StartDaemon')
        self.assertEqual(resp3.status_code, 200)
        d3_pid = resp3.content
        print "bridge daemon started with pid " + d3_pid

        resp1 = requests.get(node1_url + '/StartDaemon')
        self.assertEqual(resp1.status_code, 200)
        d1_pid = resp1.content
        print "daemon started with pid " + d1_pid

        resp2 = requests.get(node2_url + '/StartDaemon')
        self.assertEqual(resp2.status_code, 200)
        d2_pid = resp2.content
        print "daemon started with pid " + d2_pid

        time.sleep(2)
        #clean_db
        #resp1 = requests.get(node1_url + '/ResetDb')
        #time.sleep(1)
        #resp1 = requests.get(node2_url + '/ResetDb')
        #time.sleep(1)
        #resp1 = requests.get(bridge_url+ '/ResetDb')

        #time.sleep(3)
        bridge_entity = "urn:ers:bridge_entity"

        test_pred = 'rdf:comment'
        #add statements
        resp1 = requests.get(node1_url + '/AddStatement/' + bridge_entity+ '/' +test_pred + '/random:node1')
        document_id_node1 = resp1.content.split()[-2]
        print "node1 public document id : " + str(document_id_node1)

        resp2 = requests.get(node2_url + '/AddStatement/' + bridge_entity+ '/' + test_pred  + '/random:node2')
        document_id_node2 = resp2.content.split()[-2]
        print "node2 public document id : " + str(document_id_node2)

        #update replication links to link the doc on node 2 public to doc on node1 cache
        #resp = requests.get(node1_url + '/Get/' + bridge_entity)
        resp = requests.get(node1_url + '/CacheEntity/' + bridge_entity)


        #resp = requests.get(node2_url + '/Get/' + bridge_entity)
        resp = requests.get(node2_url + '/CacheEntity/' + bridge_entity)

        replication_statements = 20
        pred_node1 = []
        val_node1 = []
        pred_node2 = []
        val_node2 = []
        for i in range(0, replication_statements):
            pred_node1.append(test_pred)
            val_node1.append('replication:node1_' + str(i))

            pred_node2.append(test_pred)
            val_node2.append('replication:node2_' + str(i))

        url = node2_url + '/BatchAddStatement/' + bridge_entity+'/'
        data = json.dumps({'predicates':pred_node2, 'values':val_node2})

        executor1 = concurrent.futures.ProcessPoolExecutor(max_workers=1)
        future = executor1.submit(requests.post,url,data)

        url = node1_url + '/BatchAddStatement/' + bridge_entity +'/'
        data = json.dumps({'predicates':pred_node1, 'values':val_node1})

        executor2 = concurrent.futures.ProcessPoolExecutor(max_workers=1)
        future = executor2.submit(requests.post,url,data)

        req_start = time.time()
        # at this point, node1 and node2 are inserting statements in their public stores.
        # they should be replicated to the bridge's cache, and then to the caches of the nodes
        # we want to see how many are in node2's cache from those sent to node1 and vice-versa

        total_time = 10
        while time.time() - req_start < total_time:
            print '-------------------------------------------'
            nr_values = get_nr_documents(document_id_node2, test_pred, node2_url, 'ers-public')
            print "node 2 public after {} nr_values:{} ".format(time.time() - req_start, nr_values)

            nr_values = get_nr_documents(document_id_node2, test_pred, node1_url, 'ers-cache')
            print "node 1 cache after {} nr_values:{} ".format(time.time() - req_start, nr_values)

            print "\n"

            nr_values = get_nr_documents(document_id_node1, test_pred, node1_url, 'ers-public')
            print "node 1 public after {} nr_values:{} ".format(time.time() - req_start, nr_values)

            nr_values = get_nr_documents(document_id_node1, test_pred, node2_url, 'ers-cache')
            print "node 2 cache after {} nr_values:{} ".format(time.time() - req_start, nr_values)
            print '-------------------------------------------'
            print "\n"
            time.sleep(0.5)

        #clean_db
        resp1 = requests.get(node1_url + '/ResetDb')
        resp1 = requests.get(node2_url + '/ResetDb')
        resp1 = requests.get(bridge_url+ '/ResetDb')


        #stop everything
        resp = requests.get(node1_url + '/StopDaemon/' + d1_pid)
        resp = requests.get(node2_url + '/StopDaemon/' + d2_pid)
        resp = requests.get(bridge_url+ '/StopDaemon/' + d3_pid)

    def test2NodesWrite(self):
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


        time.sleep(3)

        #add statements
        resp1 = requests.get(node1_url + '/AddStatement/' + test_entity + '/rdf:type/foaf:LocalAgent')
        document_id_node1 = resp1.content.split()[-2]
        resp2 = requests.get(node2_url + '/AddStatement/' + test_entity + '/rdf:type/foaf:RemoteAgent')
        document_id_node2 = resp2.content.split()[-2]

        #update replication links to link the doc on node 2 public to doc on node1 cache

        resp = requests.get(node1_url + '/CacheEntity/' + test_entity)
        resp = requests.get(node2_url + '/CacheEntity/' + test_entity)

        initial_statements = 2
        replication_statements = 50

        both_working = 7
        node2_alone = 15
        total_time = 20

        pred =[]
        vals_node1 = []
        vals_node2 = []
        for i in range(0, replication_statements):
            pred.append('rdf:type')
            vals_node1.append('foaf:RemoteAgent_node1_' + str(i))
            vals_node2.append('foaf:RemoteAgent_node2_' + str(i))


        executor = concurrent.futures.ProcessPoolExecutor(max_workers=1)
        url = node1_url + '/BatchAddStatement/' + test_entity +'/'
        data = json.dumps({'predicates':pred, 'values':vals_node1})


        p = Process(target=requests.post, args=(url, data))
        p.start()

        url = node2_url + '/BatchAddStatement/' + test_entity +'/'
        data = json.dumps({'predicates':pred, 'values':vals_node2})

        p = Process(target=requests.post, args=(url, data))
        p.start()

        req_start = time.time()


        #the public statements on node2 should be in node1's cache
        #run for 10 seconds
        test_pred = "rdf:type"
        while time.time() - req_start < both_working:
            nr_values = get_nr_documents(document_id_node2, test_pred, node2_url, 'ers-public')
            print "node 2 public after {} nr_values:{} ".format(time.time() - req_start, nr_values)

            nr_values = get_nr_documents(document_id_node1, test_pred, node2_url, 'ers-cache')
            print "node 2 cache after {} nr_values:{} ".format(time.time() - req_start, nr_values)
            nr_values = get_nr_documents(document_id_node1, test_pred, node1_url, 'ers-public')
            print "node 1 public after {} nr_values:{} ".format(time.time() - req_start, nr_values)
            nr_values = get_nr_documents(document_id_node2, test_pred, node1_url, 'ers-cache')
            print "node 1 cache after {} nr_values:{} ".format(time.time() - req_start, nr_values)
            time.sleep(0.5)


        #clean_db
        resp1 = requests.get(node1_url + '/ResetDb')
        resp1 = requests.get(node2_url + '/ResetDb')

        resp = requests.get(node1_url + '/StopDaemon/' + d1_pid)
        resp = requests.get(node2_url + '/StopDaemon/' + d2_pid)


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


        #wait for resetting to finish
        time.sleep(2)

        #add statements
        resp1 = requests.get(node1_url + '/AddStatement/' + test_entity + '/rdf:type/foaf:LocalAgent')
        resp2 = requests.get(node2_url + '/AddStatement/' + test_entity + '/rdf:type/foaf:RemoteAgent')
        document_id = resp2.content.split()[-2]

        #update replication links to link the doc on node 2 public to doc on node1 cache
        resp = requests.get(node1_url + '/Get/' + test_entity)
        resp = requests.get(node1_url + '/CacheEntity/' + test_entity)

        resp = requests.get(node1_url + '/ShowEntity/' + test_entity)

        # !!! important: initial replication is slow. After the first one, things go pretty smoothly.
        initial_statements = 2
        replication_statements = 50

        both_working = 7

        node2_alone = 12
        total_time = 15

        pred =[]
        vals_node1 = []
        vals_node2 = []
        for i in range(0, replication_statements):
            pred.append('rdf:type')
            vals_node1.append('foaf:RemoteAgent_node1_' + str(i))
            vals_node2.append('foaf:RemoteAgent_node2_' + str(i))

        url = node2_url + '/BatchAddStatement/' + test_entity +'/'
        data = json.dumps({'predicates':pred, 'values':vals_node1})

        p = Process(target=requests.post, args=(url, data))
        p.start()


        req_start = time.time()


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

        resp = requests.get(node2_url + '/Delete/' + test_entity)
        deletion_start = time.time()

        while time.time() - deletion_start < 2:
            url = node2_url + '/ShowDoc/ers-public/' + document_id + '/rdf:type'
            resp = requests.get(url)
            nr_values = int( resp.content)
            print "node 2 public after {} nr_values:{} ".format(time.time() - req_start, nr_values)

            url = node1_url + '/ShowDoc/ers-cache/' + document_id + '/rdf:type'
            resp = requests.get(url)
            nr_values = int( resp.content)
            print "node 1 cache after {} nr_values:{} ".format(time.time() - req_start, nr_values)
            time.sleep(0.5)


        #clean_db
        resp1 = requests.get(node1_url + '/ResetDb')
        resp1 = requests.get(node2_url + '/ResetDb')

        resp = requests.get(node1_url + '/StopDaemon/' + d1_pid)
        resp = requests.get(node2_url + '/StopDaemon/' + d2_pid)

if __name__ == '__main__':
        unittest.main()

