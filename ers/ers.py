#!/usr/bin/python

import couchdbkit
import rdflib
import peer_monitor
from collections import defaultdict
from models import ModelS, ModelT
                                                        
# Document model is used to store data in CouchDB. The API is independent from the choice of model.
DEFAULT_MODEL = ModelS()


def merge_annotations(a, b):
    for key in set(a.keys() + b.keys()):
        a.setdefault(key, []).extend(b.get(key, []))


class EntityCache(defaultdict):
    """Equivalent to defaultdict(lambda: defaultdict(set))."""
    def __init__(self):
        super(EntityCache, self).__init__(lambda: defaultdict(set))

    def add(self, s, p, o):
        """Add <s, p, o> to cache."""
        self[s][p].add(o)


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

    def exist(self, subject, graph):
        return self.db.doc_exist(self.model.couch_key(subject, graph))


class ERSReadWrite(ERSReadOnly):
    def __init__(self, server_url=r'http://admin:admin@127.0.0.1:5984/',
                 dbname='ers', model=DEFAULT_MODEL):
        self.server = couchdbkit.Server(server_url)
        self.db = self.server.get_or_create_db(dbname)
        self.model = model

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

    def import_nt(self, file_name, target_graph):
        """Import N-Triples file."""
        cache = EntityCache()
        input_doc = open(file_name, "r")
        for input_line in input_doc:
            triple = input_line.split(None, 2) # assumes SPO is separated by any whitespace string with leading and trailing spaces ignored
            s = triple[0][1:-1] # get rid of the <>, naively assumes no bNodes for now
            p = triple[1][1:-1] # get rid of the <>
            o = triple[2][1:-1] # get rid of the <> or "", naively assumes no bNodes for now
            oquote = triple[2][0]
            if oquote == '"':
                o = triple[2][1:].rsplit('"')[0]
            elif oquote == '<':
                o = triple[2][1:].rsplit('>')[0]
            else:
                o = triple[2].split(' ')[0] # might be a named node
            cache.add(s, p, o)
        self.write_cache(cache, target_graph)

    def import_nt_rdflib(self, file_name, target_graph):
        """Import N-Triples file using rdflib."""
        # TODO: get rid of the intermediate cache?
        cache = EntityCache()
        graph = rdflib.Graph()
        graph.parse(file_name, format='nt')
        for s, p, o in graph:
            cache.add(str(s), str(p), str(o))
        self.write_cache(cache, target_graph)

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
        pass


class ERSLocal(ERSReadWrite):
    def __init__(self, server_url=r'http://admin:admin@127.0.0.1:5984/', dbname='ers', model=DEFAULT_MODEL,
                fixed_peers=()):
        super(ERSLocal, self).__init__(server_url, dbname, model)
        self.fixed_peers = list(fixed_peers)

    def get_annotation(self, entity):
        result = self.get_data(entity)
        for remote in self.get_peer_ers_interfaces():
            merge_annotations(result, remote.get_data(entity))
        return result

    def get_values(self, entity, prop):
        entity_data = self.get_annotation(entity)
        return entity_data.get(prop, [])

    def get_peer_ers_interfaces(self):
        result = []

        for peer_info in self.fixed_peers: # + peer_monitor.get_peers():
            if 'url' in peer_info:
                server_url = peer_info['url']
            elif 'host' in peer_info and 'port' in peer_info:
                server_url = r'http://admin:admin@' + peer_info['host'] + ':' + str(peer_info['port']) + '/'
            else:
                continue

            dbname = peer_info['dbname'] if 'dbname' in peer_info else self.dbname

            peer_ers = ERSReadOnly(server_url, dbname)
            result.append(peer_ers)

        return result


def test():
    server = couchdbkit.Server(r'http://admin:admin@127.0.0.1:5984/')

    def create_ers(dbname, model=DEFAULT_MODEL):
        if dbname in server:
            server.delete_db(dbname)           
        ers_new = ERSLocal(dbname=dbname, model=model)
        view = ers_new.model.views_doc.copy()  # avoid writing _rev to the view_doc
        ers_new.db.save_doc(view)
        return ers_new
 
    def test_ers():
        """Model independent tests"""
        ers.import_nt('../../tests/data/timbl.nt', 'timbl')
        assert ers.db.doc_exist('_design/index')
        assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'bad_graph') == False
        assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl') == True
        ers.delete_entity('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl')
        assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl') == False
        for o in objects:
            ers.add_data(s, p, o, g)
            ers.add_data(s, p2, o, g)
        for o in objects2:
            ers.add_data(s, p, o, g2)
            ers.add_data(s, p2, o, g2)
        data = ers.get_data(s, g)
        assert set(data[p]) == objects
        data2 = ers.get_data(s) # get data from all graphs
        assert set(data2[p]) == local_objects
        ers.delete_value(entity, p2)
        assert p2 not in ers.get_annotation(entity)


    # Test data
    s = entity = 'urn:ers:meta:testEntity'
    p = 'urn:ers:meta:predicates:hasValue'
    p2 = 'urn:ers:meta:predicates:property'
    g = 'urn:ers:meta:testGraph'
    g2 = 'urn:ers:meta:testGraph2'
    g3 = 'urn:ers:meta:testGraph3'
    objects = set(['value 1', 'value 2'])
    objects2 = set(['value 3', 'value 4'])
    local_objects = objects | objects2
    remote_objects = set(['value 5', 'value 6'])
    all_objects = local_objects | remote_objects

    # Test local ers using differend document models
    for model in [ModelS(), ModelT()]:
        dbname = 'ers_' + model.__class__.__name__.lower()
        ers = create_ers(dbname, model)
        test_ers()

    # Prepare remote ers
    ers_remote = create_ers('ers_remote')
    for o in remote_objects:
        ers_remote.add_data(entity, p, o, g3)

    # Query remote
    ers_local = ERSLocal(dbname='ers_models', fixed_peers=[{'url': r'http://admin:admin@127.0.0.1:5984/',
                                                            'dbname': 'ers_remote'}])
    assert set(ers_local.get_annotation(entity)[p]) == all_objects
    assert set(ers_local.get_values(entity, p)) == all_objects
    ers_local.delete_entity(entity)
    assert set(ers_local.get_annotation(entity)[p]) == remote_objects

    print "Tests pass"


if __name__ == '__main__':
    test()

