#!/usr/bin/python

import couchdbkit
import requests # used by tests only
from collections import defaultdict
from models import ModelS, ModelT
                                                        
class EntityCache(defaultdict):
    """Equivalent to defaultdict(lambda: defaultdict(set))."""
    def __init__(self):
        super(EntityCache, self).__init__(lambda: defaultdict(set))

    def add(self, s, p, o):
        """Add <s, p, o> to cache."""
        self[s][p].add(o)


class ERSLocal(object):
    def __init__(self, serverURL=r'http://admin:admin@127.0.0.1:5984/', dbname='ers', model = ModelS()):
        self.server = couchdbkit.Server(serverURL)
        self.db = self.server.get_or_create_db(dbname)
        self.model = model

    def add_data(self):
        pass

    def delete_entity(self, subject, graph):
        return self.db.delete_doc(self.model.couch_key(subject, graph))

    def get_data(self, subject, graph):
        pass

    def exist(self, subject, graph):
        return self.db.doc_exist(self.model.couch_key(subject, graph))

    def import_nt(self, file_name, target_graph):
        """Import N-Triples file."""
        cache = EntityCache()
        input_doc = open(file_name, "r")
        for input_line in input_doc:
             # parsing a triple @@FIXME: employ real NTriples parser here!
            triple = input_line.split(' ', 2) # naively assumes SPO is separated by a single whitespace
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


def test():
    server = couchdbkit.Server(r'http://admin:admin@127.0.0.1:5984/')
    def prepare_ers(model, dbname='ers_test'):
        if dbname in server:
            server.delete_db(dbname)
        ers = ERSLocal(dbname=dbname, model=model)
        ers.import_nt('../test_dataset/timbl.nt', 'timbl')
        return ers

    def test_ers():
        """Model independent tests"""
        assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'bad_graph') == False
        assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl') == True
        assert ers.delete_entity('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl')['ok'] == True
        assert ers.exist('http://www4.wiwiss.fu-berlin.de/booksMeshup/books/006251587X', 'timbl') == False
 
    dbname = 'ers_s'
    ers = prepare_ers(ModelS(), dbname)
    test_ers()
    doc = requests.get(r'http://127.0.0.1:5984/{0}/timbl%20http%3A%2F%2Fwww.w3.org%2FPeople%2FBerners-Lee%2Fcard%23i'.format(dbname)).json()
    assert set(doc['http://xmlns.com/foaf/0.1/nick']) == set([u'TimBL', u'timbl'])

    dbname = 'ers_t'
    ers = prepare_ers(ModelT(), dbname)
    test_ers()
    doc = requests.get(r'http://127.0.0.1:5984/{0}/http%3A%2F%2Fwww.w3.org%2FPeople%2FBerners-Lee%2Fcard%23i%23timbl'.format(dbname)).json()
    assert set([o for p, o in zip(doc['p'], doc['o']) if p=='http://xmlns.com/foaf/0.1/nick']) == set([u'TimBL', u'timbl'])

    print "Tests pass"


if __name__ == '__main__':
    test()

