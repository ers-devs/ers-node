class LocalModelBase(object):
    """ """
    view_name = '_all_docs'

    def cache_key(self, couch_key):
        return couch_key

    def couch_key(self, cache_key, graph):
        return cache_key

    def refresh_doc(self, couch_doc, cache):
        pass


class ModelS(LocalModelBase):
    """

    Example document:

    {
        "_id": "http://graph.name http://www.w3.org/People/Berners-Lee/card#i",
        "_rev": "1-e004c4ac4b5f7923892ad417d364a85e",
        "http://www.w3.org/2000/01/rdf-schema#label": [
           "Tim Berners-Lee"
        ],
        "http://xmlns.com/foaf/0.1/nick": [
           "TimBL",
           "timbl"
        ]
    }

    """
    def cache_key(self, couch_key):
        return couch_key.split(' ')[1]

    def couch_key(self, cache_key, graph):
        return "{0} {1}".format(graph, cache_key)

    def get_data(self, doc):
        data_dict = doc.copy()
        data_dict.pop('_id', None)
        data_dict.pop('_rev', None)
        return data_dict

    def refresh_doc(self, couch_doc, cache):
        cache_data = cache[self.cache_key(couch_doc['_id'])]
        for p, oo in cache_data.iteritems():
            couch_doc[p] = list(oo.union(couch_doc.get(p, [])))

class ModelT(LocalModelBase):
    """

    Example document:

    {
        "_id": "http://www.w3.org/People/Berners-Lee/card#i#http://graph.name",
        "_rev": "1-d7841397d995f90480553919cff49bd8",
        "p": [
           "http://xmlns.com/foaf/0.1/nick",
           "http://xmlns.com/foaf/0.1/nick",
           "http://www.w3.org/2000/01/rdf-schema#label",
        ],
        "s": "http://www.w3.org/People/Berners-Lee/card#i",
        "g": "http://graph.name",
        "o": [
           "TimBL",
           "timbl",
           "Tim Berners-Lee",
        ]
    }
    """
    def cache_key(self, couch_key):
        return couch_key.rsplit('#', 1)[0]

    def couch_key(self, cache_key, graph):
        return "{0}#{1}".format(cache_key, graph)

    def get_data(self, doc):
        data_dict = {}
        for p, o in zip(doc['p'], doc['o']):
            data_dict.setdefault(p, []).append(o)
        return data_dict

    def refresh_doc(self, couch_doc, cache):
        s, g = couch_doc['_id'].rsplit('#', 1)
        couch_doc['g'] = g
        couch_doc['s'] = s
        pp, oo = zip(*[(p, o) for p, oo in cache[s].iteritems() for o in oo])
        couch_doc['p'] = tuple(couch_doc.get('p', ())) + pp
        couch_doc['o'] = tuple(couch_doc.get('o', ())) + oo


class ModelH(ModelT):
    view_name = 'by_graph_subject'

    def cache_key(self, couch_key):
        pass

    def couch_key(self, cache_key, graph):
        pass

    def refresh_doc(self, couch_doc, cache):
        pass
