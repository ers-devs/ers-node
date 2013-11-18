#!/usr/bin/python

import re
import sys
import uuid

import couchdbkit
import restkit

from hashlib import md5
from socket import gethostname

from store import LocalStore, reset_local_store
from models import ModelS
from utils import EntityCache

DEFAULT_MODEL = ModelS()

class ERSReadOnly(object):
    """ The read-only class for an ERS peer.
    
        :param server_url: CouchDB server URL
        :type server_url: str
        :param auth: authentication filter used to connect to CouchDB
        :type restkit.BasicAuth
        :param fixed_peers: known peers
        :type fixed_peers: tuple
        :param local_only: whether or not the peer is local-only
        :type local_only: bool.
    """
    def __init__(self,
                 fixed_peers=(),
                 local_only=False):
        self._local_only = local_only
        self.fixed_peers = [] if self._local_only else list(fixed_peers)
        self.store = LocalStore()
        self._init_host_urn()

    def _init_host_urn(self):
        fingerprint = md5(gethostname()).hexdigest()
        self.host_urn = "urn:ers:host:{}".format(fingerprint)

    def get_entity(self, entity_name):
        '''
        Create an entity object, fill it will all the relevant documents
        '''
        
        # Create the entity
        entity = Entity(entity_name)
        
        # Add matching documents from the local store
        for source, db_name in self.store.db_names.iteritems():
            docs = self.store[db_name].docs_by_entity(entity_name)
            for doc in docs:
                entity.add_document(doc, source)
         
        # Get documents out of public/cache of connected peers
        for peer in self.get_peers():
            remote_docs = []
            try:
                remote_store = RemoteStore(peer['server_url'])
                remote_docs.extend(remote_store.docs_by_entity(entity_name))
            except:
                sys.stderr.write("Warning: failed to query remote peer {0}".format(peer))
                continue
            else:
                for doc in remote_docs:
                    entity.add_document(doc, 'remote')
        return entity

    def search(self, prop, value=None):
        """ Search entities by property or property + value pair.
        
            :param prop: property to search for
            :type prop: str.
            :param value: value to search for
            :type value: str.
            :returns: list of unique (entity, graph) pairs
        """
        result = set(self.store.by_property_value(prop, value))
        for peer in self.get_peers():
            for dbname in ('ers-public', 'ers-cache'):
                remote_result = []
                try:
                    remote_store = RemoteStore(peer['server_url'])
                    remote_result = set(remote_store.by_property_value(prop, value))
                except:
                    sys.stderr.write("Warning: failed to query remote peer {0}".format(peer))
                    continue
                result.update(remote_result)
        return list(result)

    def exist(self, subject, graph):
        """ Check whether a subject exists in a given graph.

            :param subject: subject to check for
            :type subject: str.
            :param graph: graph to use for checking
            :type graph: str.
            :rtype: bool.
        """
        return self.store.public.doc_exist(self.model.couch_key(subject, graph))

    def get_peers(self):
        """ Get the known peers.
        
            :rtype: array 
        """
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
        state_doc = self.store.public.open_doc('_local/state')
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
        for doc in self.store.public.all_docs().all():
            document = self.store.public.get(doc['id'])
            if '@id' in document and document['@id'] == entity_name:
                return True
        return False
        
class ERSLocal(ERSReadOnly):
    """ The read-write local class for an ERS peer.
    
        :param server_url: CouchDB server URL
        :type server_url: str
        :param auth: authentication filter used to connect to CouchDB
        :type restkit.BasicAuth
        :param fixed_peers: known peers
        :type fixed_peers: tuple
        :param local_only: if True ERS will not attempt to connect to remote peers
        :type local_only: bool
        :param reset_store: whether or not to reset the CouchDB database on the given server
        :type reset_database: bool
    """
    def __init__(self,
                 fixed_peers=(),
                 local_only=False,
                 reset_database=False):
        if reset_database:
            reset_local_store()
        super(ERSLocal, self).__init__(fixed_peers, local_only)

    def delete_entity(self, entity):
        """ Delete an entity from the given graph.
        
            :param entity: entity to delete
            :type entity: str.
            :param graph: graph to delete from
            :type graph: str.
            :returns: success status
            :rtype: bool.
        """
        docs = [{'_id': r['id'], '_rev': r['value']['rev'], "_deleted": True} 
                for r in self.store.public.view('index/by_entity', key=entity)]
        return self.store.public.save_docs(docs)

    def delete_value(self, entity, prop):
        """ Delete all of the peer's values for a property in an entity from the given graph.
        
            :param entity: entity to delete for
            :type entity: str.
            :param prop: property to delete for
            :type prop: str.
            :param graph: graph to delete from
            :type graph: str.
            :returns: success status
            :rtype: bool.
        """
        docs = [r['doc'] for r in self.store.public.view('index/by_entity',
            key=entity, include_docs=True)]
        for doc in docs:
            self.model.delete_property(doc, prop)
        return self.store.public.save_docs(docs)        

    def create_entity(self, entity_name):
        '''
        Create a new entity, return it and store in as a new document in the public store
        '''
        # Create a new document
        document =  {
                        '@id' : entity_name,
                        '@owner' : self.host_urn
                    }
        self.store.public.save_doc(document)
        
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
                self.store.public.save_doc(doc['document'])

    def is_cached(self, entity_name):
        '''
        Check if an entity exists in the cache
        '''
        return self.store.cache.entity_exist(entity_name)

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
        
        # Save all its current documents in the cache
        for doc in entity.get_raw_documents():
            self.store.cache.save_doc(doc)
    
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
