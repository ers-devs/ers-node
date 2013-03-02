#!/usr/bin/python

import couchdbkit
import rdflib
from collections import defaultdict
from models import ModelS, ModelT
                                                        
# Document model is used to store data in CouchDB. The API is independent from the choice of model.
DEFAULT_MODEL = ModelS()

class EntityCache(defaultdict):
    """Equivalent to defaultdict(lambda: defaultdict(set))."""
    def __init__(self):
        super(EntityCache, self).__init__(lambda: defaultdict(set))

    def add(self, s, p, o):
        """Add <s, p, o> to cache."""
        self[s][p].add(o)

class ERSReadOnly(object):
    def __init__(self, serverURL=r'http://admin:admin@127.0.0.1:5984/', dbname='ers', model = DEFAULT_MODEL):
        self.server = couchdbkit.Server(serverURL)
        self.db = self.server.get_db(dbname)
        self.model = model

    def get_data(self, subject, graph=None):
        """get all property+values for an identifier"""
        # FIXME: overwrites instead of merging values from different graphs
        result = {}
        if graph is None:
            docs = [d['doc'] for d in self.db.view('index/by_entity', include_docs=True, key=subject)]
        else:
            docs = [self.get_doc(subject, graph)]
        for doc in docs:
            result.update(self.model.get_data(doc, subject, graph))
        return result

    def get_annotation(self, entity):
        # preferred terminology for user API is "entity, property, value"
        pass

    def get_doc(self, subject, graph):
        try:
            return self.db.get(self.model.couch_key(subject, graph))
        except couchdbkit.exceptions.ResourceNotFound: 
            return None
        raise Exception()

    def get_values(self, subject, predicate, graph):
        """ Get the value for a identifier+property (return null or a special value if it does not exist)
            Return a list of values or an empty list
        """
        data = self.get_data(subject, graph)
        return data.get(predicate, [])        

    def exist(self, subject, graph):
        return self.db.doc_exist(self.model.couch_key(subject, graph))


class ERSLocal(ERSReadOnly):
    def __init__(self, serverURL=r'http://admin:admin@127.0.0.1:5984/', dbname='ers', model = DEFAULT_MODEL):
        self.server = couchdbkit.Server(serverURL)
        self.db = self.server.get_or_create_db(dbname)
        self.model = model

    def add_data(self, s, p, o, g):
        """add a property+value to an identifier (create it if it does not exist yet)"""
        triples = EntityCache()
        triples.add(s,p,o)
        self.write_cache(triples, g)

    def delete_entity(self, subject, graph):
        """delete ids"""
        return self.db.delete_doc(self.model.couch_key(subject, graph))

    def delete_value(self, subject, graph):
        """delete value"""
        pass

    def import_nt(self, file_name, target_graph):
        """Import N-Triples file."""
        cache = EntityCache()
        input_doc = open(file_name, "r")
        for input_line in input_doc:
             # parsing a triple @@FIXME: employ real NTriples parser here!
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
            cache.add(s,p,o)
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
            self.model.refresh_doc(couch_doc, cache)
            docs.append(couch_doc)
        self.db.save_docs(docs)

    def update_value(self, subject, object, graph=None):
        """update a value for an identifier+property (create it if it does not exist yet)"""
        pass

def test():
    server = couchdbkit.Server(r'http://admin:admin@127.0.0.1:5984/')
    def prepare_ers(model, dbname='ers_test'):
        if dbname in server:
            server.delete_db(dbname)
        ers = ERSLocal(dbname=dbname, model=model)
        ers.import_nt('../tests/data/timbl.nt', 'timbl')
        ers.db.save_doc(ers.model.views_doc)
        return ers

    def test_ers():
        """Model independent tests"""
        assert ers.db.doc_exist('_design/index')
        assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'bad_graph') == False
        assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl') == True
        assert ers.delete_entity('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl')['ok'] == True
        assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl') == False
        s = 'urn:ers:meta:testEntity'
        p = 'urn:ers:meta:predicates:hasValue'
        g = 'urn:ers:meta:testGraph'
        g2 = 'urn:ers:meta:testGraph2'
        objects = set(['value 1', 'value 2'])
        objects2 = set(['value 3', 'value 4'])
        for o in objects:
            ers.add_data(s, p, o, g)
        for o in objects2:
            ers.add_data(s, "predicate:temp", o, g2)
        data = ers.get_data(s, g)
        assert set(data[p]) == objects
        data2 = ers.get_data(s) # get data from all graphs
        assert set(data2[p]) == objects
        assert set(data2["predicate:temp"]) == objects2       
        assert set(ers.get_values(s, p, g)) == objects

    for model in [ModelS(), ModelT()]:
#    for model in [ModelT()]:
        dbname = 'ers_' + model.__class__.__name__.lower()
        ers = prepare_ers(model, dbname)
        test_ers()
 
    print "Tests pass"


if __name__ == '__main__':
    test()

