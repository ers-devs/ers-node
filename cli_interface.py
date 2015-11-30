import os
import sys
CURRENT_PATH = os.path.dirname(os.path.realpath(__file__))
ERS_PATH = os.path.join(os.path.dirname(CURRENT_PATH), '../ers-node')
sys.path.insert(0, ERS_PATH)
from ers import ERS
from prettytable import PrettyTable
from rdflib import Graph

TEST_ENTITY = "urn:ers:test"

class Interface(object):
    def __init__(self):
        self.ers = ERS()

    def clean_db(self):
        '''
        Reset all the content of the DB
        '''
        self.ers.reset()

    # List all the peers

    # List the local statements about an entity (groupe them by status)

    # Delete all statements about an entity

    # List the peers that have statements about an entity
    def search_for_entity(self, prop, val=None):
        list_of_entities = self.ers.search(prop,val)
        return list_of_entities

    def show_peers(self):
        list_of_peers = self.ers.get_peers()
        return list_of_peers

    # Cache remote documents

    # Add a public|private p/o to an entity
    def add_statement(self, uri, predicate, value, private=False):
        '''
        Add a new predicate,value pair to the description.
        Eventually make this private
        '''
        entity = self.ers.get(uri)
        entity.add(predicate, value, private)
        self.ers.persist_entity(entity)
        print "Added <%s, %s, %s> (private=%s)" % (uri, predicate, value, private)

    # Set a public|private p/o from an entity
    def set_statement(self, uri, predicate, value, private=False):
        entity = self.ers.get(uri)
        entity.set(predicate, value, private)
        self.ers.persist_entity(entity)
        print "Set <%s, %s, %s> (private=%s)" % (uri, predicate, value, private)

    # Delete a public|private p/o from an entity

    def delete_statement(self, uri, predicate, value=None):
        entity = self.ers.get(uri)
        entity.delete(predicate, value)
        self.ers.persist_entity(entity)
        print "Deleted <%s, %s, %s>" % (uri, predicate, value)

    # Show internal synchronisation rules

    def show_status(self):
        '''
        Show various information concerning the current status of the node
        '''
        table = self._table(["Description", "Value"])
        table.add_row(['Instance UUID', self.ers.get_machine_uri()])
        table.add_row(['Number of peers', len(self.ers.get_peers())])
        print table

    def _table(self, columns):
        '''
        Helper function to initialise pretty table
        '''
        table = PrettyTable(columns)
        for column in columns:
            table.align[column] = 'l'
        return table


    def show_entity(self, uri):
        entity = self.ers.get(uri)
        table = self._table(["Predicate", "Value", "Scope"])
        for statement in entity.to_tuples():
            (p, o, scope) = statement
            table.add_row([p, o, scope])
        print table

    def import_entity(self, entity_uri):
        entities = {}

        # Load
        g = Graph()
        g.parse(entity_uri)
        for statement in g:
            (s, p, o) = statement
            if s not in entities:
                entity = self.ers.get(s)
                entities[s] = entity
            else:
                entity = entities[s]
            entity.add(p, o)
        for entity in entities.itervalues():
            self.ers.persist_entity(entity)

        # Display
        for entity_uri in entities.iterkeys():
            print "\n=> %s " % entity_uri
            self.show_entity(entity_uri)

if __name__ == '__main__':
    interface = Interface()
    #interface.clean_db()
    #print "# Status"
    #interface.show_status()
    #interface.add_statement(TEST_ENTITY, "rdf:type", "foaf:LocalAgent")

    import pdb; pdb.set_trace()
    interface.add_statement(TEST_ENTITY, "rdf:type", "foaf:RemoteAgent")
    mye = interface.ers.get(TEST_ENTITY)
    interface.ers.cache_entity(mye)
    interface.show_peers()
    print "# Adding statements to %s" % TEST_ENTITY
    interface.show_entity(TEST_ENTITY)
    interface.search_for_entity("rdf:type", "foaf:Agent")
    #interface.add_statement(TEST_ENTITY, "rdf:type", "foaf:Person", True)
    #interface.show_entity(TEST_ENTITY)
    #print "# Setting statements for %s" % TEST_ENTITY

    ##print "# Search for %s" % TEST_ENTITY
    #q_pred = "rdf:type"
    #q_value = "foaf:Agent"
    #print "# Search for entities which have %s as a predicate and %s as value" % (q_pred, q_value)
    #interface.search_for_entity("rdf:type", "foaf:Agent")
    #interface.set_statement(TEST_ENTITY, "rdf:type", "foaf:Agent")
    #interface.show_entity(TEST_ENTITY)
    #interface.set_statement(TEST_ENTITY, "rdf:type", "foaf:SecretAgent", True)
    #interface.show_entity(TEST_ENTITY)
    #print "# Delete statements for %s" % TEST_ENTITY
    #interface.delete_statement(TEST_ENTITY, "rdf:type", "foaf:Agent")
    #interface.show_entity(TEST_ENTITY)

    #interface.delete_statement(TEST_ENTITY, "rdf:type")
    #interface.show_entity(TEST_ENTITY)
    #interface.import_entity("http://people.csail.mit.edu/lkagal/foaf.rdf")

