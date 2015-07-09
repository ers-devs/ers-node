"""
ers.store

Routines for interaction with CouchDB.

"""
# http://stackoverflow.com/questions/1776434/couchdb-python-and-authentication

from functools import partial
from itertools import chain
from timeout import timeout
import logging
from couchdb.client import Database, Server
from couchdb import http
import copy
from operator import getitem

REMOTE_SERVER_TIMEOUT = 0.3

DEFAULT_STORE_URI = 'http://127.0.0.1:5984'
DEFAULT_STORE_ADMIN_URI = 'http://admin:admin@127.0.0.1:5984'
DEFAULT_AUTH = ['admin', 'admin']

ERS_PRIVATE_DB = 'ers-private'
ERS_PUBLIC_DB = 'ers-public'
ERS_CACHE_DB = 'ers-cache'
ERS_STATE_DB = 'ers-state'
ALL_DBS = [ERS_PRIVATE_DB, ERS_PUBLIC_DB, ERS_CACHE_DB]
REMOTE_DBS = [ERS_PUBLIC_DB, ERS_CACHE_DB]
OWN_DBS = [ERS_PRIVATE_DB, ERS_PUBLIC_DB]

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
                                    var values = doc[property];
                                    if (typeof(values) ==  'string') {
                                        emit([property, doc[property]], entity);
                                    } else {
                                        doc[property].forEach(
                                            function(value) {emit([property, value], entity);}
                                        )
                                    }
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


class ERSDatabase(Database):
    def __new__(cls, other):
        if isinstance(other, Database):
            other = copy.copy(other)
            other.__class__ = ERSDatabase
            return other
        return object.__new__(cls)

    def __init__(self, other):
        self.child_init = True

        # user, password = auth
        # filters = client_opts.pop('filters', [])
        # FIXME filters.append(restkit.BasicAuth(user, password))

    """docstring for ERSDatabase"""
    def docs_by_entity(self, entity_name):
        return self.view('index/by_entity',
                        wrapper=lambda r: r['doc'],
                        key=entity_name,
                        include_docs=True)

    def by_entity(self, entity_name):
        return self.view('index/by_entity',
                        key=entity_name)

    def entity_exist(self, entity_name):
        return len(self.view('index/by_entity', key=entity_name)) > 0

    def by_property(self, prop):
        # TODO add offset and limit
        return self.view('index/by_property_value',
                        startkey=[prop],
                        endkey=[prop, {}],
                        wrapper=lambda r: r['value'])

    def by_property_value(self, prop, value=None):
        # TODO add offset and limit
        # TODO add detect * + replace next char
        if value is None:
            return self.by_property(prop)
        return self.view('index/by_property_value',
                        key=[prop, value],
                        wrapper=lambda r: r['value'])

    def delete_entity(self, entity_name):
        """ Delete an entity <entity_name>

            :param entity_name: name of the entity to delete
            :type entity: str
            :returns: success status
            :rtype: bool
        """
        # !! this method of deletion is necessary for correct cache replication behavior
        # https://wiki.apache.org/couchdb/Replication
        # """Note: When using filtered replication you should not use the DELETE method to remove documents,
        #    but instead use PUT and add a _deleted:true field to the document, preserving the
        #    fields required for the filter. Your Document Update Handler should make sure these fields
        #    are always present. This will ensure that the filter will propagate deletions properly.
        # """
        docs = [{'_id': r['id'], '_rev': r['value']['rev'], "_deleted": True}
                for r in self.by_entity(entity_name)]
        #return self.save_docs(docs)
        #docs = self.docs_by_entity(entity_name).rows
        result = True
        for doc in docs:
            try:
                self.save(doc)
            except ResourceNotFound:
                result = False
                continue

        return result



class Store(object):
    """
        ERS store
    """
    def __init__(self, url=DEFAULT_STORE_ADMIN_URI, **client_opts):
        self.logger = logging.getLogger('ers-store')
        self._server = Server(url=url, **client_opts)

        self.db_names = {'public': ERS_PUBLIC_DB,
                'private': ERS_PRIVATE_DB,
                'cache': ERS_CACHE_DB,}

        # Add aggregate functions
        # for method_name in ('docs_by_entity', 'by_property', 'by_property_value'):
        #    self.add_aggregate(method_name)

        # Check the status of the databases
        self._ers_dbs = {}
        self._repair()

    def __getitem__(self, dbname):
        return self._ers_dbs[dbname]
        # return ERSDatabase(self._db_uri(dbname), server=self)

    def __iter__(self):
        for dbname in self.all_dbs():
            yield self._ers_dbs[dbname]
            # yield ERSDatabase(self._db_uri(dbname), server=self)

    @classmethod
    def add_aggregate(cls, method_name):
        """
        """
        def aggregate(self, *args, **kwargs):
            return chain(*[getitem(self[db_name], method_name)(*args, **kwargs).iterator()
                            for db_name in ALL_DBS])
        aggregate.__doc__ = """Calls method {}() of all databases in the store and returns an iterator over combined results""".format(method_name)
        aggregate.__name__ = method_name
        setattr(cls, method_name, aggregate)

    def reset(self):
        for db_name in ALL_DBS:
            try:
                del self._server[db_name]
            except http.ResourceNotFound:
                pass
        self._repair()

    def by_property_value(self, property, value=None):
        results = []
        for db in self._ers_dbs.itervalues():
            for res in db.by_property_value(property, value):
                results.append(res)
        return results

    def get_ers_db(self, dbname, **params):
        """
        Try to return an ERSDatabase object for dbname.
        """
        return self._ers_dbs[dbname]

    def info(self):
        return self._server.config()['couchdb']

    def _repair(self):
        # Authenticate with the local store
        # user, password = auth
        try:
            state_db = self._server[ERS_STATE_DB]
        except http.ResourceNotFound:
            state_db = self._server.create(ERS_STATE_DB)
        if not '_local/state' in state_db:
            state_db.save(state_doc())
        if not '_design/index' in state_db:
            state_db.save(index_doc())
        self._ers_dbs[ERS_STATE_DB] = ERSDatabase(state_db)

        for dbname in ALL_DBS:
            # Recreate database if needed
            try:
                db = self._server[dbname]
            except http.ResourceNotFound:
                db = self._server.create(dbname)

            # Create index design doc if needed
            if not '_design/index' in db:
                db.save(index_doc())

            ## Create state doc in the public database if needed
            #if dbname == ERS_PUBLIC_DB:
            #    if not '_local/state' in db:
            #        db.save(state_doc())

            # Save the ERSDatabase object
            self._ers_dbs[dbname] = ERSDatabase(db)

class ServiceStore(Store):
    """
        ServiceStore is used by ERS daemon
    """
    def __init__(self, url=DEFAULT_STORE_ADMIN_URI, **client_opts):
        # user, password = auth
        # filters = client_opts.pop('filters', [])
        # FIXME filters.append(restkit.BasicAuth(user, password))
        super(ServiceStore, self).__init__(url=url, **client_opts)
        self.replicator = self._server['_replicator']
        self.cache = self._server[ERS_CACHE_DB]

    def cache_contents(self):
        return self.cache.view('index/by_entity', wrapper= lambda x:x['id']).rows
        # TODO
        # not sure whether this was the intended functionality
        #return list(self.cache.view('_all_docs', startkey=u"_\ufff0",
        #                                include_docs = True,
        #                                wrapper=lambda r: r['id']).rows)
        #return list(self.cache.all_docs(startkey=u"_\ufff0",
        #                                wrapper=lambda r: r['id']))

    def replicator_docs(self):
        #map_fun = '''function(r) {emit (r['id'], r['value']['rev'])}'''

        return map(lambda x: [x['id'], x['value']['rev']], self.replicator.view('_all_docs', startkey = 'ers-').rows)
        #return self.replicator.view('_all_docs', startkey = 'ers-')
        #return self.replicator.query(map_fun, startkey=u"ers-", endkey=u"ers-\ufff0")
#        return self.replicator.all_docs(
#                        startkey=u"ers-auto-",
#                        endkey=u"ers-auto-\ufff0",
#                        wrapper = lambda r: (r['id'], r['value']['rev']))

    def update_replicator_docs(self, repl_docs):
        for doc_id, doc_rev in self.replicator_docs():
            if doc_id in repl_docs:
                repl_docs[doc_id]['_rev'] = doc_rev
            else:
                repl_docs[doc_id] = {"_id": doc_id, "_rev": doc_rev, "_deleted": True}
        try:
            self.replicator.update(repl_docs.values())
        except TypeError as e:
            print "Error while trying to update replicator docs: {}".format(str(e))


RemoteStore = partial(Store)#, databases=REMOTE_DBS), timeout=REMOTE_SERVER_TIMEOUT)


#@timeout(REMOTE_SERVER_TIMEOUT)
def query_remote(uri, method_name, *args, **kwargs):
    remote_store = RemoteStore(uri)
    # TODO do we want to query both?
    remote_public = remote_store[ERS_PUBLIC_DB]
    remote_cache = remote_store[ERS_CACHE_DB]
    public_docs = getattr(remote_public, method_name)(*args, **kwargs).rows
    cache_docs  = getattr(remote_cache , method_name)(*args, **kwargs).rows
    return public_docs + cache_docs


