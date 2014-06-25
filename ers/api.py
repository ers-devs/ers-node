"""
ers.api

Provides class ERS implementing API to Entity Registry System.

"""
import sys

from hashlib import md5
from socket import gethostname
from collections import Counter
from random import randrange

import store
from timeout import TimeoutError

class ERSReadOnly(object):
    """ ERS version with read-only methods.
    
        :param fixed_peers: URL's of known peers
        :type fixed_peers: list
        :param local_only: whether or not the peer is local-only
        :type local_only: bool.
    """
    def __init__(self,
                 fixed_peers=(),
                 local_only=False):
        self._local_only = local_only
        self.fixed_peers = [] if self._local_only else list(fixed_peers)
        self._timeout_count = Counter()
        self.store = store.LocalStore()
        self._init_host_urn()

    def _init_host_urn(self):
        # Use uuid provided by CouchDB 1.3+, fallback to hostname fingerprint
        try:
            uid = self.store.info()['uuid']
        except KeyError:
            uid = md5(gethostname()).hexdigest()
        self.host_urn = "urn:ers:host:{}".format(uid)

    def get_machine_uuid(self):
        '''
        @return a unique identifier for this ERS node
        '''
        return self.host_urn.split(':')[-1]

    def _is_failing(self, url):
        """
        Returns True for url's which failed to respond with increasing probability.
        Returns False for url's which did not fail.
        """
        return randrange(self._timeout_count[url] + 1) != 0

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
        # TODO parallelize 
        for peer in self.get_peers():
            url = peer['server_url']
            if self._is_failing(url):
                continue

            remote_docs = []
            try:
                remote_docs = store.query_remote(url, 'docs_by_entity', entity_name)
            except TimeoutError:
                self._timeout_count[url] += 1
                sys.stderr.write("Incremented timeout count for {0}: {1}\n".format(
                    url, self._timeout_count[url]))
            except Exception as e:
                sys.stderr.write("Warning: failed to query remote peer {0}. Error: {1}\n".format(peer, e))
            else:
                self._timeout_count.pop(url, 0)
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
        # Search in the local store
        result = set(self.store.by_property_value(prop, value))

        # Search peers
        for peer in self.get_peers():
            url = peer['server_url']
            if self._is_failing(url):
                continue

            remote_result = []
            try:
                remote_result = store.query_remote(url, 'by_property_value', prop, value)
            except TimeoutError:
                self._timeout_count[url] += 1
                sys.stderr.write("Incremented timeout count for {0}: {1}\n".format(
                    url, self._timeout_count[url]))
            except Exception as e:
                sys.stderr.write("Warning: failed to query remote peer {0}. Error: {1}\n".format(peer, e))
            else:
                self._timeout_count.pop(url, 0)
                result.update(remote_result)

        return list(result)

    def entity_exist(self, entity_name):
        """ Check whether an entity exists in the local store.

            :param entity_name
            :type subject: str
            :rtype: bool
        """
        return any([getattr(self.store, db).exist(entity_name)
            for db in self.store.db_names])

    def get_peers(self):
        """ Get the known peers.
        
            :rtype: array 
        """
        if self._local_only:
            return []

        result = [{'server_url': server_url} for server_url in self.fixed_peers] 

        state_doc = self.store.public.open_doc('_local/state')
        for peer in state_doc['peers']:
            result.append({
                'server_url': r'http://' + peer['ip'] + ':' + str(peer['port']) + '/',
            })
        return result

    def is_cached(self, entity_name):
        '''
        Check if an entity exists in the cache
        FIXME Check if *all* the remote documents are in the cache store
        '''
        return self.store.cache.entity_exist(entity_name)

    def contains_entity(self, entity_name):
        """
        Check if the entity exists in the public store
        """
        # QUESTION Other dbs?
        return self.store.public.entity_exist(entity_name)
        
class ERS(ERSReadOnly):
    """ The read-write local class for an ERS peer.
    
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
            store.reset_local_store()
        super(ERS, self).__init__(fixed_peers, local_only)

    def delete_entity(self, entity_name):
        """ Delete an entity from the public store.
        
            :param entity: entity to delete
            :type entity: str.
            :param graph: graph to delete from
            :type graph: str.
            :returns: success status
            :rtype: bool.
        """
        return self.store.public.delete_entity(entity_name)

    def create_entity(self, entity_name):
        '''
        Create a new entity
        '''
        # Create and return the entity
        return Entity(entity_name)

    def persist_entity(self, entity):
        '''
        Persist the description of an entity in the private and public stores
        '''
        for scope in ['public', 'private']:
            document = entity.get_documents(scope)
            
            # Skip the document if empty or not right scope
            if entity.get_documents(scope) == None:
                continue
            
            # Update the author, last modif date and other meta-data
            if '@owner' not in document:
                document['@owner'] = self.host_urn

            # Write the document
            if scope == 'public':
                self.store.public.save_doc(document)
            elif scope == 'private':
                self.store.private.save_doc(document)
                
    def cache_entity(self, entity):
        '''
        Place an entity in the cache. This mark the entity as being
        cached and store the currently loaded documents in the cache.
        Later on, ERS will automatically update the description of
        this entity with new / updated documents
        @param entity An entity object
        '''
        # No point in caching it again
        if self.is_cached(entity.get_entity_name()):
            return
        
        # Save all its current documents in the cache
        for document in entity.get_documents('remote'):
            self.store.cache.save_doc(document)

    def delete_from_cache(self, entity_name):
        """ Delete an entity from the cache.
        
            :param entity_name: name of the entity to delete
            :type entity: str
            :returns: success status
            :rtype: bool
        """
        return self.store.cache.delete_entity(entity_name)


class Entity():
    '''
    An entity description is contained in various CouchDB documents
    '''
    def __init__(self, entity_name):
        # Name of the entity
        self._entity_name = entity_name
        
        # List of documents, there can be one private, one public and several 
        # coming from the cache or from other peers
        self._documents = {
            'public' : None, 
            'private' : None,
            'remote' : [],
            'cache' : []
        }
        
    def add_property_value(self, prop, value, private=False):
        '''
        Add a property to the description of the entity
        '''
        # Set the scope to write in the right document
        scope = 'private' if private else 'public'
        
        # If the document does not exist yet we need to create it
        if self._documents[scope] == None:
            self._documents[scope] =  {'@id' : self._entity_name}
            
        # Add the value to those associated to this property
        if prop not in self._documents[scope]:
            self._documents[scope][prop] = []
        self._documents[scope][prop].append(value)
        
    def set_property_value(self, prop, value, private=False):
        '''
        Set a property/value pair for the description of the entity
        '''
        # Set the scope to write in the right document
        scope = 'private' if private else 'public'
        
        # Delete all previous association to that property
        self.delete_property(prop, None)
        
        # If the document does not exist yet we need to create it
        if self._documents[scope] == None:
            self._documents[scope] =  {'@id' : self._entity_name}
            
        # Add the value to those associated to this property
        self._documents[scope][prop] = [value]
        
    def delete_property(self, prop, value=None):
        '''
        Delete a property from the description of the entity
        '''
        # We delete the property from the public and private documents
        for scope in ['public', 'private']:
            if self._documents[scope] != None and prop in self._documents[scope]:
                # If a value is specified delete only this value, otherwise
                # remove the key completely
                if value == None:
                    del self._documents[scope][prop]
                else:
                    self._documents[scope][prop].pop(value, None)

    def get_properties(self):
        '''
        Get the aggregated properties out of all the individual documents
        TODO additional parameters (filter='public', flatten=False)
        '''
        result = {}

        # Set the documents to pick data from
        documents = []
        documents.append(self._documents['private'])
        documents.append(self._documents['public'])
        documents.append(self._documents['cache'].values())
        documents.append(self._documents['remote'].values())
                
        # Add properties from the target documents
        for document in documents:
            for key, values in document.iteritems():
                if key[0] != '_' and key[0] != '@':
                    result.setdefault(key, set())
                    for value in values:
                        result[key].append(value)
                        
        return result
    
    def add_document(self, document, scope):
        '''
        Add a new document to the list of documents that compose this entity
        '''
        if scope == 'public' or scope == 'private':
            self._documents[scope] = document 
        elif scope == 'cache':
            self._documents[scope].append(document)
        elif scope == 'remote':
            # TODO check that we don't append twice the same document
            self._documents[scope].append(document) 
            
    def get_documents(self, scope):
        '''
        Return all the documents associated to this entity
        '''
        return self._documents[scope]
    
    def get_entity_name(self):
        '''
        Get the name of the entity
        @return the name of that entity
        '''
        return self._entity_name

if __name__ == '__main__':
    print "To test this module use 'python ../../tests/test_ers.py'."
