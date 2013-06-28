class LocalModelBase(object):
    """ """
    view_name = '_all_docs'

    def index_doc(self):
        return {"_id": "_design/index"}

    def cache_key(self, couch_key):
        return couch_key

    def couch_key(self, cache_key, graph):
        return cache_key

    def get_data(self, doc, subject, graph):
        pass
        
    def add_data(self, couch_doc, cache):
        pass

    def delete_property(self, couch_doc, prop):
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

    @classmethod
    def index_doc(cls):
        return  {
                    "_id": "_design/index",
                    "views": {
                        "by_entity": {
                            "map": "function(doc) {var a = doc._id.split(' '); if (a.length == 2 && a[1].length > 0) {emit(a[1], {'rev': doc._rev, 'g': a[0]})}}"
                        },
                        "by_property_value": {
                            "map": """function(doc) {
                                        var a = doc._id.split(' '); 
                                        if (a.length == 2 && a[1].length > 0) {
                                          for (property in doc) {
                                            if (property[0] != '_') {
                                              doc[property].forEach(function(value) {emit([property, value], [a[1], a[0]])});
                                            }
                                          }
                                        }
                                      }
                                    """
                        }
                    }
                }

    def cache_key(self, couch_key):
        return couch_key.split(' ')[1]

    def couch_key(self, cache_key, graph):
        return "{0} {1}".format(graph, cache_key)

    def delete_property(self, couch_doc, prop):
        couch_doc.pop(prop, [])

    def get_data(self, doc, subject, graph):
        """
        Return property-value dictionary. Call with subject=None, graph=None to get data from a doc without an _id.
        """
        data_dict = doc.copy()
        data_dict.pop('_rev', None)
        doc_id = data_dict.pop('_id', None)
        if doc_id:
            g, s = doc_id.split(' ')
        else:
            g = s = None
        if (subject == s) and ((graph is None) or (graph == g)):
            return data_dict
        return {}

    def add_data(self, couch_doc, cache):
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

    @classmethod
    def index_doc(self):
        return  {
                    "_id": "_design/index",
                    "views": {
                        "by_entity": {
                            "map": "function(doc) {var separatorPosition = doc._id.lastIndexOf('#'); if (separatorPosition > 0 && separatorPosition < doc._id.length - 1) {emit(doc._id.slice(0, separatorPosition), {'rev': doc._rev, 'g': doc._id.slice(separatorPosition+1)})}}"
                        }
                    }
                }

    def cache_key(self, couch_key):
        return couch_key.rsplit('#', 1)[0]

    def couch_key(self, cache_key, graph):
        return "{0}#{1}".format(cache_key, graph)

    def delete_property(self, couch_doc, prop):
        couch_doc['p'], couch_doc['o'] = zip(*[(p, o) for p, o in zip(couch_doc['p'], couch_doc['o']) if p != prop])

    def get_data(self, doc, subject, graph):
        data_dict = {}
        if (subject != None) and ((doc['s'] != subject) or (graph != None and (doc['g'] != graph))):
            return {}
        for p, o in zip(doc['p'], doc['o']):
            data_dict.setdefault(p, []).append(o)
        return data_dict

    def add_data(self, couch_doc, cache):
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

    def add_data(self, couch_doc, cache):
        pass
