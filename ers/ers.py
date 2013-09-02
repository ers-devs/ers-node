#!/usr/bin/python

import couchdbkit
import re
import sys

from models import ModelS, ModelT
from zeroconf import ServicePeer
from utils import EntityCache

ERS_AVAHI_SERVICE_TYPE = '_ers._tcp'

ERS_PEER_TYPE_CONTRIB = 'contrib'
ERS_PEER_TYPE_BRIDGE = 'bridge'
ERS_PEER_TYPES = [ERS_PEER_TYPE_CONTRIB, ERS_PEER_TYPE_BRIDGE]

ERS_DEFAULT_DBNAME = 'ers'
ERS_DEFAULT_PEER_TYPE = ERS_PEER_TYPE_CONTRIB

# Document model is used to store data in CouchDB. The API is independent from the choice of model.
DEFAULT_MODEL = ModelS()


def merge_annotations(a, b):
    for key, values in b.iteritems():
        unique_values = set(values)
        unique_values.update(a.get(key,[]))
        a[key] = list(unique_values)

class ERSPeerInfo(ServicePeer):
    """
    This class contains information on an ERS peer.
    """
    dbname = None
    peer_type = None

    def __init__(self, service_name, host, ip, port, dbname=ERS_DEFAULT_DBNAME, peer_type=ERS_DEFAULT_PEER_TYPE):
        ServicePeer.__init__(self, service_name, ERS_AVAHI_SERVICE_TYPE, host, ip, port)
        self.dbname = dbname
        self.peer_type = peer_type

    def __str__(self):
        return "ERS peer on {0.host}(={0.ip}):{0.port} (dbname={0.dbname}, type={0.peer_type})".format(self)

    def to_json(self):
        return {
            'name': self.service_name,
            'host': self.host,
            'ip': self.ip,
            'port': self.port,
            'dbname': self.dbname,
            'type': self.peer_type
        }

    @staticmethod
    def from_service_peer(svc_peer):
        dbname = ERS_DEFAULT_DBNAME
        peer_type = ERS_DEFAULT_PEER_TYPE

        match = re.match(r'ERS on .* [(](.*)[)]$', svc_peer.service_name)
        if match is None:
            return None

        for item in match.group(1).split(','):
            param, sep, value = item.partition('=')

            if param == 'dbname':
                dbname = value
            if param == 'type':
                peer_type = value

        return ERSPeerInfo(svc_peer.service_name, svc_peer.host, svc_peer.ip, svc_peer.port, dbname, peer_type)


class ERSReadOnly(object):
    def __init__(self, server_url=r'http://admin:admin@127.0.0.1:5984/', dbname='ers', model=DEFAULT_MODEL):
        self.server = couchdbkit.Server(server_url)
        self.db = self.server.get_db(dbname)
        self.model = model

    def get_data(self, subject, graph=None):
        """get all property+values for an identifier"""
        result = {}
        if graph is None:
            docs = [d['doc'] for d in self.db.view('index/by_entity', include_docs=True, key=subject)]
        else:
            docs = [self.get_doc(subject, graph)]
        for doc in docs:
            merge_annotations(result, self.model.get_data(doc, subject, graph))
        return result

    def get_doc(self, subject, graph):
        try:
            return self.db.get(self.model.couch_key(subject, graph))
        except couchdbkit.exceptions.ResourceNotFound: 
            return None

    def get_values(self, subject, predicate, graph=None):
        """ Get the value for a identifier+property (return null or a special value if it does not exist)
            Return a list of values or an empty list
        """
        data = self.get_data(subject, graph)
        return data.get(predicate, [])

    def search(self, prop, value=None):
        """ Search local entities by property or property+value
            Return a list of (entity, graph) pairs.
        """
        if value is None:
            result = [tuple(r['value']) for r in self.db.view('index/by_property_value', startkey=[prop], endkey=[prop, {}])]
        else:
            result = [tuple(r['value']) for r in self.db.view('index/by_property_value', key=[prop, value])]
        return result      

    def exist(self, subject, graph):
        return self.db.doc_exist(self.model.couch_key(subject, graph))


class ERSLocal(ERSReadOnly):
    def __init__(self, server_url=r'http://admin:admin@127.0.0.1:5984/', dbname='ers',
                 model=DEFAULT_MODEL, fixed_peers=(), local_only=False, reset_database=False):
        self.server = couchdbkit.Server(server_url)
        if reset_database and dbname in self.server:
            self.server.delete_db(dbname)
        self.db = self.server.get_or_create_db(dbname)
        self.model = model
        self.local_only = local_only
        for doc in self.model.initial_docs():
            if not self.db.doc_exist(doc['_id']):
                self.db.save_doc(doc)
        self.fixed_peers = list(fixed_peers)

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
                    for r in self.db.view('index/by_entity', key=entity)]
        else:
            docs = [{'_id': r['id'], '_rev': r['value']['rev'], "_deleted": True} 
                    for r in self.db.view('index/by_entity', key=entity)
                    if r['value']['g'] == graph]
        return self.db.save_docs(docs)

    def delete_value(self, entity, prop, graph=None):
        """Deletes all of the user's values for the given property in the given entity."""
        if graph is None:
            docs = [r['doc'] for r in self.db.view('index/by_entity', key=entity, include_docs=True)]
        else:
            docs = [r['doc'] for r in self.db.view('index/by_entity', key=entity, include_docs=True)
                             if r['value']['g'] == graph]
        for doc in docs:
            self.model.delete_property(doc, prop)
        return self.db.save_docs(docs)        

    def write_cache(self, cache, graph):
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
        """update a value for an identifier+property (create it if it does not exist yet)"""
        raise NotImplementedError

    def get_annotation(self, entity):
        result = self.get_data(entity)
        for peer in self.get_peers():
            try:
                remote = ERSReadOnly(peer['server_url'], peer['dbname'])
                remote_result = remote.get_data(entity)
            except:
                sys.stderr.write("Warning: failed to query remote peer {0}".format(peer))
                continue

            merge_annotations(result, remote_result)

        return result

    def search(self, prop, value=None):
        """ Search entities by property or property+value
            Return a list of unique (entity, graph) pairs.
        """
        result = set(super(ERSLocal, self).search(prop, value))
        for peer in self.get_peers():
            try:
                remote = ERSReadOnly(peer['server_url'], peer['dbname'])
                remote_result = remote.search(prop, value)
            except:
                sys.stderr.write("Warning: failed to query remote peer {0}".format(peer))
                continue

            result.update(remote_result)

        return list(result)

    def get_values(self, entity, prop):
        entity_data = self.get_annotation(entity)
        return entity_data.get(prop, [])

    def get_peers(self):
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

        state_doc = self.db.open_doc('_design/state')
        for peer in state_doc['peers']:
            result.append({
                'server_url': r'http://admin:admin@' + peer['ip'] + ':' + str(peer['port']) + '/',
                'dbname': peer['dbname']
            })

        return result


if __name__ == '__main__':
    print "To test this module use 'python ../../tests/test_ers.py'."
