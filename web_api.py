'''
    Web api that can be used to interact from the exterior with the ERS nodes.
    ! preserves state
'''
from flask import Flask, request
app = Flask(__name__)
import time
import concurrent.futures

from ers import ERS
from ers import store
from cli_interface import *
import json
import os
import subprocess
import signal
from ers.daemon import FLASK_PORT
from couchdb.client import Server

interface = Interface()
TEST_ENTITY = "urn:ers:test"

@app.route('/')
def welcome():
    return 'ERS web interface running with host uri %s' % interface.ers.get_machine_uri()

@app.route('/DaemonRunning/')
def check_daemon_running():
    if os.path.isfile('/vagrant/ers_daemon.pid'):
        f = open('/vagrant/ers_daemon.pid')
        lines = f.readlines()
        pid = lines[0].split()[0]
        return str(pid)
    else:
        return '0'

@app.route('/StartDaemon')
def start_daemon():
    command = 'python /vagrant/ers-node/ers/daemon.py --config /etc/ers-node/ers-node.ini'
    pid = subprocess.Popen(command.split()).pid
    return str(pid)

@app.route('/StopDaemon/<pid>')
def stop_daemon(pid):
    # linux, mac
    os.kill(int(pid), signal.SIGTERM)
    try:
        killedpid, stat = os.waitpid(pid, os.WNOHANG)
        if killedpid == 0:
            return 'daemon failed to kill'
    except:
        pass
    requests.get('http://localhost:'+str(FLASK_PORT)+"/Stop")
    return 'daemon killed'


@app.route('/ServiceStoreAccess')
def service_store_access():
    DEFAULT_STORE_ADMIN_URI = 'http://admin:admin@127.0.0.1:5984'
    server = Server(DEFAULT_STORE_ADMIN_URI)
    import pdb; pdb.set_trace()
    return 'OK'

@app.route('/ResetDb')
def reset_db():
    interface.clean_db()
    return 'Databases reset'

@app.route('/Get/<entity>')
def get_entity(entity):
    created_entity = interface.ers.get(entity, include_remote = True)
    return 'got %s !\n' % entity

@app.route('/Delete/<entity_name>')
def delete_entity(entity_name):
    status = interface.ers.delete_entity(entity_name)
    return 'operation status : ' + str(status)

@app.route('/BatchAddStatement/<entity>/', methods = ['POST'])
def batch_add_statement(entity):
    request_data = json.loads(request.data)
    predicates = request_data['predicates']
    values = request_data['values']


    for i in range(len(predicates)):
        pred = predicates[i]
        val = values[i]
        add_statement(entity, pred, val)
        #time.sleep(0.3)
    return 'batch added'

@app.route('/AddStatement/<entity>/<predicate>/<value>')
def add_statement(entity, predicate, value):
    ers_entity = interface.ers.get(entity, include_remote = False)
    ers_entity.add(predicate, value)
    interface.ers.persist_entity(ers_entity)

    document_id = ers_entity._documents['public']._doc['_id']

    return 'added public %s:%s:%s to ERS with id %s !\n' %(entity, predicate, value, str(document_id))

@app.route('/CacheEntity/<entity>')
def cache_entity(entity):
    ers_entity = interface.ers.get(entity, include_remote = True)
    interface.ers.cache_entity(ers_entity)

    return 'the remote docs of %s have been moved to cache' % entity

@app.route('/ShowEntity/<entity>')
def show_entity(entity):
    ers_entity = interface.ers.get(entity, include_remote = True)
    table = interface._table(["Predicate", "Value", "Scope"])
    for statement in ers_entity.to_tuples():
        (p, o, scope) = statement
        table.add_row([p, o, scope])
    return table.get_string()

@app.route('/ShowPeers')
def list_peers():
    peers = interface.ers.get_peers()
    return str(peers)


@app.route('/ShowDB/<db_name>')
def show_cache(db_name):
    entries = interface.ers.store[db_name].view('_all_docs').rows
    return str(entries)

@app.route('/ShowDoc/<db_name>/<doc_id>/<property_id>')
def show_doc(db_name, doc_id, property_id):
    try:
        entries = interface.ers.store[db_name][doc_id][property_id]
        if type(entries) is list:
            return str(len(entries))
        else:
            #only have 1 element
            return '1'
    except:
        return '0'

def decode_value(encoded_value, encoded_type):
    value = encoded_value
    if encoded_type == 'xsd:hexBinary':
        value = dbus.ByteArray(binascii.unhexlify(encoded_value))
    return value


@app.route('/ShowDBDocument/<db_name>/<entity_name>')
def show_documents(db_name, entity_name):
    db = interface.ers.store[db_name]
    db_rows = db.view('index/by_entity', key = entity_name).rows
    document_ids = map(lambda x : x['id'], db_rows)
    doc_list = []
    results = {}
    for doc_id in document_ids:

        doc = db[doc_id]
        for key, values in doc.iteritems():
            # Don't return meta-elements
            if key[0] == '_' or key[0] == '@':
                continue

            # Get the type of that key if known
            t = None
            if '@context' in doc:
                if key in doc['@context']:
                    t = doc['@context'][key]['@type']

            # Decode the values
            if isinstance(values, list):
                for value in values:
                    results[key] = values
            else:
                results[key] = [values]


    return json.dumps(results)

@app.route('/Search/<prop>/<val>')
def search(prop, val):
    list_of_entities = interface.search_for_entity(prop, val)
    return str(list_of_entities)

if __name__ == '__main__':
    #app.debug = True

    app.run(host='0.0.0.0', threaded=True)
