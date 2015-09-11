import requests
import threading
import time
import json
from couchdb.client import Database, Server
DEFAULT_STORE_ADMIN_URI = 'http://admin:admin@127.0.0.1:5984'
other_node = 'http://admin:admin@192.168.1.200:5984'

NR_DOCS = 10
def populate():
    # insert nr_docs
    # replicate to another node
    docs = []

    server = Server(DEFAULT_STORE_ADMIN_URI)
    requests.put(other_node + '/testdb')
    requests.put(DEFAULT_STORE_ADMIN_URI+ '/testdb')
    replicator = server['_replicator']
    for i in range(NR_DOCS):
        doc = {'v1':'v1', 'v2':'v2'}
        resp = requests.put(DEFAULT_STORE_ADMIN_URI + '/testdb/RandomDoc'+str(i),
                      data = json.dumps(doc))
        print resp.content

        #docs.append(doc)

    print 'done creating'
    ip = '192.168.1.200'
    port = '5984'

    repl_doc = {
        '_id': 'test_repl_doc',
        'source': r'http://{0}:{1}/{2}'.format(ip, port, 'testdb'),
        'target': 'test_db',
        'continuous': True,
    }
    try:
      r_doc = replicator['test_repl_doc']
      del replicator[r_doc]
      replicator.save(repl_doc)
    except:
      pass
    print 'repl started'


def completion_query():
    nr_entries = 0
    start = time.time()
    while nr_entries < NR_DOCS:
        resp = requests.get(other_node + '/testdb/_all_docs')
        resp = resp.content
        resp = json.loads(resp)
        if 'total_rows' in resp:
            nr_entries = resp['total_rows']
            print "found {} after {}".format(str(nr_entries), time.time()-start)
        print 'nothing ' + str(resp)
        time.sleep(0.5)

def delete_dbs():
    s1 = Server(DEFAULT_STORE_ADMIN_URI)
    s2 = Server(other_node)
    s1.delete('testdb')
    s2.delete('testdb')

def main():
    try:
      delete_dbs()
    except:
      pass
    t = threading.Thread(target = completion_query)
    t.start()
    import pdb; pdb.set_trace()
    populate()
    import pdb; pdb.set_trace()
    delete_dbs()


if __name__ == "__main__":
    main()
