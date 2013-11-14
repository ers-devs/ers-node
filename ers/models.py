class LocalModelBase(object):
    """ 
    Base class for all the serialisations
    """
    view_name = '_all_docs'

    def index_doc(self):
        """ Get the default CouchDB index design document.
        
            :rtype: JSON object
        """
        return {"_id": "_design/index"}

    def state_doc(self):
        """ Get the default CouchDB state document.
        
            :rtype: JSON object
        """
        return {
            "_id": "_local/state",
            "peers": {}
        }

    def cache_key(self, couch_key):
        """ Get the default CouchDB cache key.
        
            :rtype: str.
        """
        return couch_key

    def couch_key(self, cache_key, graph):
        """ Get the default CouchDB key.
        
            :rtype: str.
        """
        return cache_key

    def get_data(self, doc, subject, graph):
        pass
        
    def add_data(self, couch_doc, cache):
        pass

    def delete_property(self, couch_doc, prop, value):
        pass

    def initial_docs(self):
        """ Get the default initial CouchDB documents (index design document and state document).
        
            :rtype: array
        """
        return [ self.index_doc(), self.state_doc() ]

    def content_doc(self):
        # TODO: check if the method is used
        return {"_id": "_local/content", 'entity_name': []}
    
    def initial_docs_public(self):
        return [ self.index_doc(), self.state_doc() ]
    
    def initial_docs_private(self):
        return [ self.index_doc() ]
    
    def initial_docs_cache(self):
        return [ self.index_doc(), self.content_doc() ]


class ModelS(LocalModelBase):
    """ The model class for document model type S.

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
        """ Get the CouchDB index design document (containing two views).
        
            :rtype: JSON object 
        """
        return  {
            "_id": "_design/index",
            "views": {
                "by_entity": {
                    "map": "function(doc) {if ('@id' in doc) {emit(doc['@id'], {'rev': doc._rev, 'g': doc._id})}}"
                },
                "by_property_value": {
                    "map": """
                        function(doc) {
                            if ('@id' in doc) {
                                var entity = doc['@id'];
                                for (property in doc) {
                                    if (property[0] != '_' && property[0] != '@') {
                                        doc[property].forEach(
                                          function(value) {emit([property, value], entity)}
                                        );
                                    }
                                }
                            }
                        }  """
                }
            }
        }

    def cache_key(self, couch_key):
        """ Get the CouchDB cache key.
        
            :param couch_key: the CouchDB key
            :type couch_key: str.
            :rtype: str.
        """
        return couch_key.split(' ')[1]

    def couch_key(self, cache_key, graph):
        """ Get the CouchDB key.
        
            :param cache_key: the CouchDB cache key
            :type cache_key: str.
            :param graph: the RDF graph
            :type graph: str.
            :rtype: str.
        """
        return "{0} {1}".format(graph, cache_key)

    def delete_property(self, couch_doc, prop, value):
        """ Deletes a property from the given CouchDB document.
            
            :param couch_doc: the CouchDB document
            :type couch_doc: CouchDB document object
            :param prop: the property to delete
            :type prop: str.
        """
        if prop not in couch_doc:
            return
        
        couch_doc[prop] = filter(lambda a: a != value, couch_doc[prop])

    def add_property(self, couch_doc, prop, value):
        if prop not in couch_doc:
            couch_doc[prop] = []
        couch_doc[prop].append(value)
    
    #      
    # def delete_property(self, couch_doc, prop):
    #     """"
    #      Deletes all values while the new function 
    #      deletes only the given prop value pair
    #     """
    #     couch_doc.pop(prop, [])

    def get_data(self, doc, subject, graph):
        """ Get data from a CouchDB document given a subject and graph (call with subject=None and graph=None to get data from a document without an _id).
        
            :param doc: the CouchDB document from which to get data
            :type doc: CouchDB document object
            :param subject: the subject to get data for
            :type subject: str.
            :param graph: the graph to get data for
            :type graph: str.
            :rtype: property-value dictionary
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
        """ Add cache data to a CouchDB document.
            
            :param couch_doc: the CouchDB document to add data to
            :type couch_doc: CouchDB document object
            :param cache: the cache data to add
            :type cache: dict.
        """
        cache_data = cache[self.cache_key(couch_doc['_id'])]
        for p, oo in cache_data.iteritems():
            couch_doc[p] = couch_doc.get(p, []) + list(oo)
