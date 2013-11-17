import couchdbkit
import restkit

from functools import partial
from itertools import chain

REMOTE_SERVER_TIMEOUT = 300

DEFAULT_STORE_URI = 'http://127.0.0.1:5984'
DEFAULT_AUTH = ['admin', 'admin']

LOCAL_DBS = ['public', 'private', 'cache']
REMOTE_DBS = ['public', 'cache']
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
        

class Store(couchdbkit.Server):
    """ERS store"""
    def __init__(self, uri, databases, **client_opts):
        self.db_names = dict([(db, db_name(db)) for db in databases])
        super(Store, self).__init__(uri, **client_opts)
        self.add_aggregate('docs_by_entity')

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
        def aggregate(self, *args, **kwargs):
            return chain(*(getattr(self[db_name], method_name)(*args, **kwargs).iterator()
                            for db_name in self.all_dbs()))
        aggregate.__doc__ = """Calls method {}() of all databases in the store and returns an iterator over combined results""".format(method_name)
        aggregate.__name__ = method_name
        setattr(cls, method_name, aggregate)

    def _create_aggregate_method(self, method_name):
        """
        Call method ERSDatabase.method_name for all dbs in the store
        Return chained results.
        """
        methods = [getattr(self[db_name], method_name) for db_name in self.all_dbs()]
        def new_method(self=self, *args, **kwargs):
            return chain(m(*args, **kwargs) for m in methods)
        setattr(self, method_name, new_method)
 
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



