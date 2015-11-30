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

from copy import deepcopy

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
                        include_docs=True).rows

    def by_entity(self, entity_name):
        return self.view('index/by_entity',
                        key=entity_name).rows

    def entity_exist(self, entity_name):
        return len(self.view('index/by_entity', key=entity_name).rows) > 0

    def by_property(self, prop):
        # TODO add offset and limit
        return self.view('index/by_property_value',
                        startkey=[prop],
                        endkey=[prop, {}],
                        wrapper=lambda r: r['value']).rows

    def by_property_value(self, prop, value=None):
        # TODO add offset and limit
        # TODO add detect * + replace next char
        if value is None:
            return self.by_property(prop)
        return self.view('index/by_property_value',
                        key=[prop, value],
                        wrapper=lambda r: r['value']).rows

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
        repl_docs = []
        db_docs = self.replicator.view('_all_docs', include_docs = True, startkey = 'ers-').rows
        for doc in db_docs:
            doc_fields = []
            full_doc = doc['doc']
            doc_fields.append(full_doc['_id'])
            doc_fields.append(full_doc['_rev'])
            if 'doc_ids' in full_doc:
                doc_fields.append(full_doc['doc_ids'])
            else:
                doc_fields.append(None)

            repl_docs.append(doc_fields)

        return repl_docs


        #return map(lambda x: [x['id'], x['value']['rev']], self.replicator.view('_all_docs', startkey = 'ers-').rows)
#        return self.replicator.all_docs(
#                        startkey=u"ers-auto-",
#                        endkey=u"ers-auto-\ufff0",
#                        wrapper = lambda r: (r['id'], r['value']['rev']))

    def update_replicator_docs(self, repl_docs):
        '''
            Since it is impossible to change the value of replication docs, but cancelling
            deleting replication documents cancels a replication, we need a bit of extra
            logic in managing them.

            first, always delete replication docs to nodes that are no longer connected.

            if there is a repl doc for a node still connected, leave it if no "doc_ids"
            have changed.

            if the "doc_ids" have changed, delete the old repl_doc and insert the new one

            # TODO now it's "all or nothing". Perhaps should be changed to individual doc
            # also, maybe add "last resort" : clear all replication documents and set new ones
        '''
        nr_tries = 4

        new_docs = {}

        for doc_id, doc_rev, replicated_docs_ids in self.replicator_docs():
            if doc_id in repl_docs:
                # if it is a replication document that is present both in the
                # new set and in the database
                if 'doc_ids' in repl_docs[doc_id]:
                    #we only care about filtered replication
                    new_doc_ids = repl_docs[doc_id]['doc_ids']
                    if new_doc_ids != replicated_docs_ids:
                        # different doc ids, we have to stop the old replication
                        # delete old one, insert new one
                        new_docs[doc_id] = deepcopy(repl_docs[doc_id])

                        if '_rev' in new_docs[doc_id]:
                            del new_docs[doc_id]['_rev']

                        repl_docs[doc_id] = {"_id": doc_id, "_rev": doc_rev, "_deleted": True}
                    else:
                        # do nothing to the old document(i.e. remove from repl_docs)
                        del repl_docs[doc_id]
                else:
                    #if it's not filtered replication no need to change the document
                    del repl_docs[doc_id]
            else:
                #if an old replication doc, remove it
                repl_docs[doc_id] = {"_id": doc_id, "_rev": doc_rev, "_deleted": True}

        #out with the old, in with the new!
        save_with_retries(repl_docs, self.replicator, nr_tries)
        save_with_retries(new_docs, self.replicator, nr_tries)


def save_with_retries(doc_dict, database, nr_tries):
    # we could first try a batch update, and if that fails, retry those that failed
    result = database.update(doc_dict.values())
    all_ok = all(map(lambda x: x[0], result))
    if all_ok:
        return
    #the structure of the tuples in result is (SuccceededBoolean, id, revision)
    #if it failed, revision instead shows the reason
    for individual_result in result:
        if individual_result[0] == False:
            #it failed in the batch
            document_id = individual_result[1]
            for i in range(nr_tries):
                #try to save nr_tries times
                new_rev = database[document_id]['_rev']
                new_doc = doc_dict[document_id]
                new_doc['_rev'] = new_rev
                try:
                    database[document_id] = new_doc
                except Exception as e:
                    continue
                except TypeError as e:
                    print "Error while trying to update replicator docs: {}".format(str(e))

                #if we get here, we have succeeded or failed due to a type error.
                #either way, no point in trying again so break out of for loop
                break




RemoteStore = partial(Store)#, databases=REMOTE_DBS), timeout=REMOTE_SERVER_TIMEOUT)


#@timeout(REMOTE_SERVER_TIMEOUT)
def query_remote(uri, method_name, *args, **kwargs):
    remote_store = RemoteStore(uri)
    # TODO do we want to query both?
    remote_public = remote_store[ERS_PUBLIC_DB]
    #remote_cache = remote_store[ERS_CACHE_DB]
    public_docs = getattr(remote_public, method_name)(*args, **kwargs)
    #cache_docs  = getattr(remote_cache , method_name)(*args, **kwargs).rows
    return public_docs
