""" 
Helper module for interaction with CouchDB. 

Example CouchDB document representing partial data about 
entity<http://www.w3.org/People/Berners-Lee/card#i>:

{
    "_id": "3d00b0cc1ea95dbaa806dbf5b96c4d5b",
    "_rev": "1-e004c4ac4b5f7923892ad417d364a85e",
    "@id: "http://www.w3.org/People/Berners-Lee/card#i",
    "@owner : "urn:ers:host:a1fd2202aa76693d3e74ba657ce932f0",
    "http://www.w3.org/2000/01/rdf-schema#label": [
       "Tim Berners-Lee"
    ],
    "http://xmlns.com/foaf/0.1/nick": [
       "TimBL",
       "timbl"
    ]
}

"""

import couchdbkit
import restkit

from functools import partial
from itertools import chain

REMOTE_SERVER_TIMEOUT = 300

DEFAULT_STORE_URI = 'http://127.0.0.1:5984'
DEFAULT_AUTH = ['admin', 'admin']

LOCAL_DBS = ['public', 'private', 'cache']
REMOTE_DBS = ['public', 'cache']
OWN_DBS = ['public', 'private']
DB_PREFIX = 'ers-'

def db_name(db):
    return DB_PREFIX + db

def index_doc():
    return  {
        "_id": "_design/index",
        "views": {
            "by_entity": {
                "map": "function(doc) {if ('@id' in doc) {emit(doc['@id'], {'rev': doc._rev, 'g': doc._id})}}"
            },
            "by_property_value": {
                "map": """
                    function(doc) {
                        if ('@id' in doc) {
                            var entity = doc['@id'];
                            for (property in doc) {
                                if (property[0] != '_' && property[0] != '@') {
                                    doc[property].forEach(
                                      function(value) {emit([property, value], entity)}
                                    );
                                }
                            }
                        }
                    } """
            }
        }
    }

def state_doc():
    return {
        "_id": "_local/state",
        "peers": {}
    }


class ERSDatabase(couchdbkit.Database):
    """docstring for ERSDatabase"""
    def docs_by_entity(self, entity_name):
        return self.view('index/by_entity',
                        wrapper = lambda r: r['doc'],
                        key=entity_name,
                        include_docs=True)

    def by_entity(self, entity_name):
        return self.view('index/by_entity',
                        key=entity_name)

    def entity_exist(self, entity_name):
        return self.view('index/by_entity', key=entity_name).first() is not None

    def by_property(self, prop):
        return self.view('index/by_property_value',
                        startkey=[prop],
                        endkey=[prop, {}],
                        wrapper = lambda r: r['value'])

    def by_property_value(self, prop, value=None):
        if value is None:
            return self.by_property(prop)
        return self.view('index/by_property_value',
                        key=[prop, value],
                        wrapper = lambda r: r['value'])

    def delete_entity(self, entity_name):
        """ Delete an entity <entity_name>
        
            :param entity_name: name of the entity to delete
            :type entity: str
            :returns: success status
            :rtype: bool
        """
        docs = [{'_id': r['id'], '_rev': r['value']['rev'], "_deleted": True} 
                for r in self.by_entity(entity_name)]
        return self.save_docs(docs)


class Store(couchdbkit.Server):
    """ERS store"""
    def __init__(self, uri, databases, **client_opts):
        self.db_names = dict([(db, db_name(db)) for db in databases])
        super(Store, self).__init__(uri, **client_opts)
        for method_name in ('docs_by_entity', 'by_property', 'by_property_value'):
            self.add_aggregate(method_name)

    def __getattr__(self, attr):
        if attr in self.db_names:
            return self[self.db_names[attr]]
        raise AttributeError("{} object has no attribute {}".format(
                                self.__class__, attr))

    def __getitem__(self, dbname): 
        return ERSDatabase(self._db_uri(dbname), server=self) 

    def __iter__(self): 
        for dbname in self.all_dbs(): 
            yield ERSDatabase(self._db_uri(dbname), server=self)

    @classmethod
    def add_aggregate(cls, method_name):
        """
        """
        def aggregate(self, *args, **kwargs):
            return chain(*(getattr(self[db_name], method_name)(*args, **kwargs).iterator()
                            for db_name in self.all_dbs()))
        aggregate.__doc__ = """Calls method {}() of all databases in the store and returns an iterator over combined results""".format(method_name)
        aggregate.__name__ = method_name
        setattr(cls, method_name, aggregate)
 
    def get_db(self, dbname, **params): 
        """ 
        Try to return an ERSDatabase object for dbname. 
        """ 
        return ERSDatabase(self._db_uri(dbname), server=self, **params) 

    def all_dbs(self):
        return self.db_names.values()


LocalStore = partial(Store, uri=DEFAULT_STORE_URI, databases=LOCAL_DBS, timeout=REMOTE_SERVER_TIMEOUT)                                                        
RemoteStore = partial(Store, databases=REMOTE_DBS)                                                        

def reset_local_store(auth=DEFAULT_AUTH):
    user, password = auth
    store = LocalStore(filters=[restkit.BasicAuth(user, password)])
    for dbname in store.all_dbs():
        # Recreate database
        try:
            store.delete_db(dbname)
        except couchdbkit.ResourceNotFound:
            pass
        db = store.create_db(dbname)

        # Create index design doc
        db.save_doc(index_doc())

    # Create state doc in the public database
    store.public.save_doc(state_doc())

