
from collections import defaultdict
from StringIO import StringIO
import rdflib

# Document model is used to store data in CouchDB. The API is independent from the choice of model.

def import_nt(registry, file_name, target_graph):
    """ Import N-Triples file.
    
        :param registry: registry used for writing the data
        :type registry: ERSLocal instance
        :param file_name: file name
        :type file_name: str.
        :param target_graph: graph to write to
        :type target_graph: str.
    """
    cache = EntityCache().parse_nt(filename=file_name)
    registry.write_cache(cache, target_graph)

def import_nt_rdflib(registry, file_name, target_graph):
    """ Import N-Triples file using rdflib.
    
        :param registry: registry used for writing the data
        :type registry: ERSLocal instance
        :param file_name: file name
        :type file_name: str.
        :param target_graph: graph to write to
        :type target_graph: str.
    """
    # TODO: get rid of the intermediate cache?
    cache = EntityCache().parse_nt_rdflib(filename=file_name)
    registry.write_cache(cache, target_graph)

class EntityCache(defaultdict):
    """ Equivalent to defaultdict(lambda: defaultdict(set)).
    """
    def __init__(self):
        super(EntityCache, self).__init__(lambda: defaultdict(set))

    def add(self, s, p, o):
        """ Add <s, p, o> to cache.
        
            :param s: RDF subject
            :type s: str.
            :param p: RDF property
            :type p: str.
            :param o: RDF object
            :type o: str.
        """
        self[s][p].add(o)

    def parse_nt(self, **kwargs):
        """ Parse N-Triples data.
        
            :returns: self
        """
        if 'filename' in kwargs:
            lines = open(kwargs['filename'], 'r')
        elif 'data' in kwargs:
            lines = StringIO(kwargs['data'])
        else:
            raise RuntimeError("Must specify filename= or data= for parse_nt")

        for input_line in lines:
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

            self.add(s, p, o)

        return self

    def parse_nt_rdflib(self, **kwargs):
        """ Parse N-Triples data using rdflib.
        
            :returns: self
        """
        graph = rdflib.Graph()

        if 'filename' in kwargs:
            graph.parse(location=kwargs['filename'], format='nt')
        elif 'data' in kwargs:
            graph.parse(data=kwargs['data'], format='nt')
        else:
            raise RuntimeError("Must specify filename= or data= for parse_nt_rdflib")

        for s, p, o in graph:
            self.add(str(s), str(p), str(o))

        return self

if __name__ == '__main__':
    print "To test this module use 'python ../../tests/test_utils.py'."
