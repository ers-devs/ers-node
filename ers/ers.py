#!/usr/bin/python

import couchdbkit
import re
import sys
import uuid

from models import ModelS
from utils import EntityCache

DEFAULT_MODEL = ModelS()

class ERSReadOnly(object):
    def __init__(self,
                 server_url=r'http://admin:admin@127.0.0.1:5984/',
                 dbname='ers',
                 model=DEFAULT_MODEL,
                 fixed_peers=(),
                 local_only=False):
        self._local_only = local_only
        self.fixed_peers = [] if self._local_only else list(fixed_peers)
        self.server = couchdbkit.Server(server_url)
        self._init_model(model)
        self._init_databases()

    def _init_databases(self):
        self.public_db = self.server.get_db('ers-public')
        self.private_db = self.server.get_db('ers-private')
        self.cache_db = self.server.get_db('ers-cache')

    def _init_model(self, model):
        """Connect to model."""
        self.model = model

    def get_annotation(self, entity):
        result = self.get_data(entity)

        for peer in self.get_peers():
            try:
                remote = ERSReadOnly(peer['server_url'], peer['dbname'], local_only=True)
                remote_result = remote.get_data(entity)
            except:
                sys.stderr.write("Warning: failed to query remote peer {0}".format(peer))
                continue
            self._merge_annotations(result, remote_result)

        return result

    def get_data(self, subject, graph=None):
        """get all property+values for an identifier"""
        result = {}
        if graph is None:
            docs = [d['doc'] for d in self.public_db.view('index/by_entity', include_docs=True, key=subject)]
        else:
            docs = [self.get_doc(subject, graph)]
        for doc in docs:
            self._merge_annotations(result, self.model.get_data(doc, subject, graph))
        return result

    def get_doc(self, subject, graph):
        try:
            return self.public_db.get(self.model.couch_key(subject, graph))
        except couchdbkit.exceptions.ResourceNotFound: 
            return None

    def get_entity(self, entity_name):
        '''
        Create an entity object, fill it will all the relevant documents
        '''
        
        # Create the entity
        entity = Entity(entity_name)
        
        # Search for related documents in public
        for doc in self.public_db.all_docs().all():
            document = self.public_db.get(doc['id'])
            if '@id' in document and document['@id'] == entity_name:
                entity.add_document(document, 'public')
                
        # Do the same in the cache
        for doc in self.cache_db.all_docs().all():
            document = self.cache_db.get(doc['id'])
            if '@id' in document and document['@id'] == entity_name:
                entity.add_document(document, 'cache')
         
        # TODO : Get documents out of public/cache of connected peers
        
        # Return the entity
        return entity
    

    def get_values(self, entity, prop):
        """ Get the value for a identifier+property (return null or a special value if it does not exist)
            Return a list of values or an empty list
        """
        entity_data = self.get_annotation(entity)
        return entity_data.get(prop, [])

    # def search(self, prop, value=None):
    #     '''
    #     Search local entities by property or property+value
    #     @return: a list of identifiers.
    #     '''
    #     # TODO Fix index
    #     results = []
    #     for doc in self.public_db.all_docs().all():
    #         document = self.public_db.get(doc['id'])
    #         if prop in document:
    #             if value == None or value in document[prop]:
    #                 results.append(document['@id'])
    #     return results
    
    def search(self, prop, value=None):
        """ Search entities by property or property+value
            Return a list of unique (entity, graph) pairs.
        """
        if value is None:
            view_range = {'startkey': [prop], 'endkey': [prop, {}]}
        else:
            view_range = {'key': [prop, value]}

        result = set([r['value'] for r in self.public_db.view('index/by_property_value', **view_range)])
        for peer in self.get_peers():
            try:
                remote = ERSReadOnly(peer['server_url'], peer['dbname'], local_only=True)
                remote_result = remote.search(prop, value)
            except:
                sys.stderr.write("Warning: failed to query remote peer {0}".format(peer))
                continue
            result.update(remote_result)
        return list(result)

    def exist(self, subject, graph):
        return self.public_db.doc_exist(self.model.couch_key(subject, graph))

    def get_peers(self):
        result = []
        if self._local_only:
            return result
        for peer_info in self.fixed_peers:
            if 'url' in peer_info:
                server_url = peer_info['url']
            elif 'host' in peer_info and 'port' in peer_info:
                server_url = r'http://admin:admin@' + peer_info['host'] + ':' + str(peer_info['port']) + '/'
            else:
                raise RuntimeError("Must include either 'url' or 'host' and 'port' in fixed peer specification")

            dbname = peer_info['dbname'] if 'dbname' in peer_info else 'ers'

            result.append({'server_url': server_url, 'dbname': dbname})
        state_doc = self.public_db.open_doc('_local/state')
        for peer in state_doc['peers']:
            result.append({
                'server_url': r'http://admin:admin@' + peer['ip'] + ':' + str(peer['port']) + '/',
                'dbname': peer['dbname']
            })
        return result

    def contains_entity(self, entity_name):
        '''
        Search in the public DB is there is already a document for describing that entity
        '''
        # TODO Use an index
        for doc in self.public_db.all_docs().all():
            document = self.public_db.get(doc['id'])
            if '@id' in document and document['@id'] == entity_name:
                return True
        return False
        
    def _merge_annotations(self, a, b):
        for key, values in b.iteritems():
            unique_values = set(values)
            unique_values.update(a.get(key,[]))
            a[key] = list(unique_values)


class ERSLocal(ERSReadOnly):
    def __init__(self,
                 server_url=r'http://admin:admin@127.0.0.1:5984/',
                 dbname='ers',
                 model=DEFAULT_MODEL,
                 fixed_peers=(),
                 local_only=False,
                 reset_database=False):
        self._local_only = local_only
        self.fixed_peers = [] if self._local_only else list(fixed_peers)

        # Connect to CouchDB
        self.server = couchdbkit.Server(server_url)

        # Connect databases
        self._init_databases(reset_database)

        # Connect to the internal model used to store the triples
        self._init_model(model)

    def _init_databases(self, reset_database):
        if reset_database:
            try:
                self.server.delete_db('ers-public')
                self.server.delete_db('ers-private')
                self.server.delete_db('ers-cache')
            except couchdbkit.ResourceNotFound:
                pass
        self.public_db = self.server.get_or_create_db('ers-public')
        self.private_db = self.server.get_or_create_db('ers-private')
        self.cache_db = self.server.get_or_create_db('ers-cache')

    def _init_model(self, model):
        """Connect to model and create required docs."""
        self.model = model
        for doc in self.model.initial_docs_public():
            if not self.public_db.doc_exist(doc['_id']):
                self.public_db.save_doc(doc)
        for doc in self.model.initial_docs_cache():
            if not self.cache_db.doc_exist(doc['_id']):
                self.cache_db.save_doc(doc)
        for doc in self.model.initial_docs_private():
            if not self.private_db.doc_exist(doc['_id']):
                self.private_db.save_doc(doc)

    def add_data(self, s, p, o, g):
        """Adds the value for the given property in the given entity. Create the entity if it does not exist yet)"""
        triples = EntityCache()
        triples.add(s, p, o)
        self.write_cache(triples, g)

    def delete_entity(self, entity, graph=None):
        """Deletes the entity."""
        # Assumes there is only one entity per doc.
        if graph is None:
            docs = [{'_id': r['id'], '_rev': r['value']['rev'], "_deleted": True} 
                    for r in self.public_db.view('index/by_entity', key=entity)]
        else:
            docs = [{'_id': r['id'], '_rev': r['value']['rev'], "_deleted": True} 
                    for r in self.public_db.view('index/by_entity', key=entity)
                    if r['value']['g'] == graph]
        return self.public_db.save_docs(docs)

    def delete_value(self, entity, prop, graph=None):
        """Deletes all of the user's values for the given property in the given entity."""
        if graph is None:
            docs = [r['doc'] for r in self.public_db.view('index/by_entity', key=entity, include_docs=True)]
        else:
            docs = [r['doc'] for r in self.public_db.view('index/by_entity', key=entity, include_docs=True)
                             if r['value']['g'] == graph]
        for doc in docs:
            self.model.delete_property(doc, prop)
        return self.public_db.save_docs(docs)        

    def write_cache(self, cache, graph):
        docs = []
        # TODO: check if sorting keys makes it faster
        couch_docs = self.public_db.view(self.model.view_name, include_docs=True,
                                  keys=[self.model.couch_key(k, graph) for k in cache])
        for doc in couch_docs:
            couch_doc = doc.get('doc', {'_id': doc['key']})
            self.model.add_data(couch_doc, cache)
            docs.append(couch_doc)
        self.public_db.save_docs(docs)

    def update_value(self, subject, object, graph=None):
        """update a value for an identifier+property (create it if it does not exist yet)"""
        raise NotImplementedError

    def create_entity(self, entity_name):
        '''
        Create a new entity, return it and store in as a new document in the public store
        '''
        # Create a new document
        document = {'@id' : entity_name}
        self.public_db.save_doc(document)
        
        # Create the entity
        entity = Entity(entity_name)
        entity.add_document(document, 'public')
         
        # Return the entity
        return entity

    def persist_entity(self, entity):
        '''
        Save an updated entity
        '''
        for doc in entity.get_documents():
            if doc['source'] == 'public':
                self.public_db.save_doc(doc['document'])

    def is_cached(self, entity_name):
        '''
        Check if an entity is listed as being cached
        '''
        entity_names_doc = self.cache_db.open_doc('_local/content')
        return entity_name in entity_names_doc['entity_name']

    def cache_entity(self, entity):
        '''
        Place an entity in the cache. This mark the entity as being
        cached and store the currently loaded documents in the cache.
        Later on, ERS will automatically update the description of
        this entity with new / updated documents
        @param entity An entity object
        '''
        # No point in caching it again
        if self.is_cached(entity.get_name()):
            return
        
        # Add the entity to the list
        entity_names_doc = self.cache_db.open_doc('_local/content')
        entity_names_doc['entity_name'].append(entity.get_name())
        self.cache_db.save_doc(entity_names_doc)
        
        # Save all its current documents in the cache
        for doc in entity.get_raw_documents():
            self.cache_db.save_doc(doc)
    
    def get_machine_uuid(self):
        '''
        @return a unique identifier for this ERS node
        '''
        return str(uuid.getnode())


class Entity():
    '''
    An entity description is contained in various CouchDB documents
    '''
    def __init__(self, entity_name):
        # Save the name
        self._entity_name = entity_name
        
        # Create an empty list of documents
        self._documents = []
        
        # Get an instance of the serialisation model
        self._model = DEFAULT_MODEL
        
    def add_document(self, document, source):
        '''
        Add a document to the list of documents that compose this entity
        '''
        self._documents.append({'document': document, 'source': source})
            
    def add_property(self, property, value):
        '''
        Add a property to the description of the entity
        TODO: we can only edit the local or private documents
        '''
        document = None
        for doc in self._documents:
            if doc['source'] == 'public':
                document = doc['document']
        if document == None:
            return
        self._model.add_property(document, property, value)
    
    def delete_property(self, property, value):
        '''
        Delete a property to the description of the entity
        TODO: we can only edit the local or private documents
        '''
        document = None
        for doc in self._documents:
            if doc['source'] == 'public':
                document = doc['document']
        if document == None:
            return
        self._model.delete_property(document, property, value)
        
    def get_properties(self):
        '''
        Get the aggregated properties out of all the individual documents
        '''
        result = {}
        for doc in self._documents:
            for key, value in doc['document'].iteritems():
                if key[0] != '_' and key[0] != '@':
                    if key not in result:
                        result[key] = []
                    for v in value:
                        result[key].append(v)
        return result
        
    def get_documents(self):
        '''
        Return all the documents associated to this entity and their source
        '''
        return self._documents
    
    def get_documents_ids(self):
        '''
        Get the identifiers of all the documents associated to the entity
        '''
        results = []
        for doc in self._documents:
            results.append(doc['document']['_id'])
        return results
         
    def get_raw_documents(self):
        '''
        Get the content of all the documents associated to the entity
        '''
        results = []
        for doc in self._documents:
            results.append(doc['document'])
        return results
    
    def get_name(self):
        '''
        @return the name of that entity
        '''
        return self._entity_name

if __name__ == '__main__':
    print "To test this module use 'python ../../tests/test_ers.py'."
