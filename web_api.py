'''
    Web api that can be used to interact from the exterior with the ERS nodes.
    ! preserves state
'''
from flask import Flask
app = Flask(__name__)

from ers import ERS
from ers import store
from cli_interface import *
import json

interface = Interface()
TEST_ENTITY = "urn:ers:test"

@app.route('/')
def welcome():
    return 'ERS web interface running with host uri %s' % interface.ers.get_machine_uri()

@app.route('/ServiceStoreAccess')
def service_store_access():
    from couchdb.client import Server
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

@app.route('/AddStatement/<entity>/<predicate>/<value>')
def add_statement(entity, predicate, value):
    ers_entity = interface.ers.get(entity, include_remote = False)
    ers_entity.add(predicate, value)
    interface.ers.persist_entity(ers_entity)
    return 'added public %s:%s:%s to ERS!\n' %(entity, predicate, value)

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
    results = []
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
                    results.append((key, decode_value(value, t)))
            else:
                results.append((key, decode_value(values, t)))


    return json.dumps(results)

@app.route('/Search/<prop>/<val>')
def search(prop, val):
    list_of_entities = interface.search_for_entity(prop, val)
    return str(list_of_entities)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
