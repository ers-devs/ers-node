#!/usr/bin/python

import couchdbkit
import re
import sys

from models import ModelS, ModelT
from utils import EntityCache

DEFAULT_MODEL = ModelS()

class ERSReadOnly(object):
    """ The read-only class for an ERS peer.
    
        :param server_url: peer's server URL
        :type server_url: str.
        :param dbname: CouchDB database name
        :type dbname: str.
        :param model: document model
        :type model: LocalModelBase instance
        :param fixed_peers: known peers
        :type fixed_peers: tuple
        :param local_only: whether or not the peer is local-only
        :type local_only: bool.
    """
    def __init__(self,
                 server_url=r'http://admin:admin@127.0.0.1:5984/',
                 dbname='ers',
                 model=DEFAULT_MODEL,
                 fixed_peers=(),
                 local_only=False):
        self.local_only = local_only
        self.server = couchdbkit.Server(server_url)
        self.model = model
        self.fixed_peers = list(fixed_peers)
        self.db = self.server.get_db(dbname)

    def get_annotation(self, entity):
        """ Get data from self and known peers for a given subject.
        
            :param entity: subject to get data for
            :type entity: str.
            :rtype: dict.
        """
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
        """ Get all property + value pairs for a given subject and graph.
            
            :param subject: subject to get data for
            :type subject: str.
            :param graph: graph to get data for
            :type graph: str.
            :rtype: property-value dictionary
        """
        result = {}
        if graph is None:
            docs = [d['doc'] for d in self.db.view('index/by_entity', include_docs=True, key=subject)]
        else:
            docs = [self.get_doc(subject, graph)]
        for doc in docs:
            self._merge_annotations(result, self.model.get_data(doc, subject, graph))
        return result

    def get_doc(self, subject, graph):
        """ Get the documents for a given subject and graph.
            
            :param subject: subject to get data for
            :type subject: str.
            :param graph: graph to get data for
            :type graph: str.
            :rtype: array
        """
        try:
            return self.db.get(self.model.couch_key(subject, graph))
        except couchdbkit.exceptions.ResourceNotFound: 
            return None

    def get_values(self, entity, prop):
        """ Get the value for an identifier + property pair (return null or a special value if it does not exist).
            
            :param entity: entity to get data for
            :type entity: str.
            :param prop: property to get data for
            :type prop: str.
            :returns: list of values or an empty list
        """
        entity_data = self.get_annotation(entity)
        return entity_data.get(prop, [])

    def search(self, prop, value=None):
        """ Search entities by property or property + value pair.
        
            :param prop: property to search for
            :type prop: str.
            :param value: value to search for
            :type value: str.
            :returns: list of unique (entity, graph) pairs
        """
        if value is None:
            view_range = {'startkey': [prop], 'endkey': [prop, {}]}
        else:
            view_range = {'key': [prop, value]}
        result = set([tuple(r['value']) for r in self.db.view('index/by_property_value', **view_range)])
        for peer in self.get_peers():
            try:
                remote = ERSReadOnly(peer['server_url'], peer['dbname'])
                remote_result = remote.search(prop, value)
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
        return self.db.doc_exist(self.model.couch_key(subject, graph))

    def get_peers(self):
        """ Get the known peers.
        
            :rtype: array 
        """
        result = []
        if self.local_only:
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
        state_doc = self.db.open_doc('_local/state')
        for peer in state_doc['peers']:
            result.append({
                'server_url': r'http://admin:admin@' + peer['ip'] + ':' + str(peer['port']) + '/',
                'dbname': peer['dbname']
            })
        return result

    def _merge_annotations(self, a, b):
        for key, values in b.iteritems():
            unique_values = set(values)
            unique_values.update(a.get(key,[]))
            a[key] = list(unique_values)


class ERSLocal(ERSReadOnly):
    """ The read-write local class for an ERS peer.
    
        :param server_url: peer's server URL
        :type server_url: str.
        :param dbname: CouchDB database name
        :type dbname: str.
        :param model: document model
        :type model: LocalModelBase instance
        :param fixed_peers: known peers
        :type fixed_peers: tuple
        :param local_only: whether or not the peer is local-only
        :type local_only: bool.
        :param reset_database: whether or not to reset the CouchDB database on the given server
        :type reset_databasae: bool.
    """
    def __init__(self,
                 server_url=r'http://admin:admin@127.0.0.1:5984/',
                 dbname='ers',
                 model=DEFAULT_MODEL,
                 fixed_peers=(),
                 local_only=False,
                 reset_database=False):
        self.local_only = local_only
        self.server = couchdbkit.Server(server_url)
        if reset_database and dbname in self.server:
            self.server.delete_db(dbname)
        self.db = self.server.get_or_create_db(dbname)
        self.model = model
        for doc in self.model.initial_docs():
            if not self.db.doc_exist(doc['_id']):
                self.db.save_doc(doc)
        self.fixed_peers = list(fixed_peers)

    def add_data(self, s, p, o, g):
        """ Add a value for a property in an entity in the given graph (create the entity if it does not exist yet).
        
            :param s: RDF subject
            :type s: str.
            :param p: RDF property
            :type p: str.
            :param o: RDF object
            :type o: str.
            :param g: RDF graph
            :type g: str.
        """
        triples = EntityCache()
        triples.add(s, p, o)
        self.write_cache(triples, g)

    def delete_entity(self, entity, graph=None):
        """ Delete an entity from the given graph.
        
            :param entity: entity to delete
            :type entity: str.
            :param graph: graph to delete from
            :type graph: str.
            :returns: success status
            :rtype: bool.
        """
        # Assumes there is only one entity per doc.
        if graph is None:
            docs = [{'_id': r['id'], '_rev': r['value']['rev'], "_deleted": True} 
                    for r in self.db.view('index/by_entity', key=entity)]
        else:
            docs = [{'_id': r['id'], '_rev': r['value']['rev'], "_deleted": True} 
                    for r in self.db.view('index/by_entity', key=entity)
                    if r['value']['g'] == graph]
        return self.db.save_docs(docs)

    def delete_value(self, entity, prop, graph=None):
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
        if graph is None:
            docs = [r['doc'] for r in self.db.view('index/by_entity', key=entity, include_docs=True)]
        else:
            docs = [r['doc'] for r in self.db.view('index/by_entity', key=entity, include_docs=True)
                             if r['value']['g'] == graph]
        for doc in docs:
            self.model.delete_property(doc, prop)
        return self.db.save_docs(docs)        

    def write_cache(self, cache, graph):
        """ Write cache to the given graph.
        
            :param cache: cache to write
            :type cache: array
            :param graph: graph to write to
            :type graph: str.
        """
        docs = []
        # TODO: check if sorting keys makes it faster
        couch_docs = self.db.view(self.model.view_name, include_docs=True,
                                  keys=[self.model.couch_key(k, graph) for k in cache])
        for doc in couch_docs:
            couch_doc = doc.get('doc', {'_id': doc['key']})
            self.model.add_data(couch_doc, cache)
            docs.append(couch_doc)
        self.db.save_docs(docs)

    def update_value(self, subject, object, graph=None):
        """ Update a value for an identifier + property pair (create it if it does not exist yet).
            [Not yet implemented]
        
            :param subject: subject to update for
            :type subject: str.
            :param object: object to update
            :type object: str.
            :param graph: graph to use for updating
            :type graph: str.
        """
        raise NotImplementedError


if __name__ == '__main__':
    print "To test this module use 'python ../../tests/test_ers.py'."
