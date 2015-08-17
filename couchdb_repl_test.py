import requests
import threading
import time
from couchdb.client import Database, Server
DEFAULT_STORE_ADMIN_URI = 'http://admin:admin@127.0.0.1:5984'
other_node = 'http://admin:admin@192.168.1.200:5984'

NR_DOCS = 10000
def populate():
    # insert nr_docs
    # replicate to another node
    docs = []

    server = Server(DEFAULT_STORE_ADMIN_URI)
    test_db = server.create('test-db')
    replicator = server['_replicator']
    for i in range(NR_DOCS):
        doc = {'_id': 'RandomDoc' + str(i), 'v1':'v1', 'v2':'v2'}
        test_db.save(doc)
        #docs.append(doc)

    ip = '192.168.1.200'
    port = '5984'

    repl_doc = {
        '_id': 'test_repl_doc',
        'source': r'http://{0}:{1}/{2}'.format(ip, port, 'test-db'),
        'target': 'test_db',
        'continuous': True,
    }
    replicator.save(repl_doc)


def completion_query():
    nr_entries = 0
    start = time.time()
    while nr_entries < NR_DOCS:
        resp = json.loads(requests.get(other_node + '/test-db/_all_docs'))
        nr_entries = resp['total_rows']
        print "found {} after {}".format(str(nr_entries), time.time()-start)
        time.sleep(0.5)

def delete_dbs():
    s1 = Server(DEFAULT_STORE_ADMIN_URI)
    s2 = Server(other_node)
    s1.delete('test-db')
    s2.delete('test-db')

def main():
    t = threading.Thread(target = completion_query)
    t.start()
    populate()
    import pdb; pdb.set_trace()
    delete_dbs()


if __name__ == "__main__":
    main()

