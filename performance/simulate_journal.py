#!/usr/bin/python2
import csv
import pprint

# Prefer ../ers-local/ers/ers.py over installed versions
import sys
import os
import time
TESTS_PATH = os.path.dirname(os.path.realpath(__file__))
ERS_PATH = os.path.dirname(TESTS_PATH)
sys.path.insert(0, ERS_PATH)

from ers.api import ERS

NS = 'urn:ers:meta:predicates:'

# Create a pretty printer for debugging
pp = pprint.PrettyPrinter(indent=2)

def go():
    # Load the journal data
    entries = []
    header = None
    with open('data/journals_data.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='*', quotechar='"')
        for row in reader:
            if header == None:
                header = row
            else:
                dictionary = dict(zip(header, row))
                entries.append(dictionary)
    # pp.pprint(entries)
    entries = entries[:1]
    print "Loaded {} journal entries".format(len(entries))
    
    # Connect to ERS and clean the DB
    ers = ERS(reset_database=True)
    host_uuid = ers.get_machine_uuid()
    
    # Iterate over all the journal entries
    for entry in entries:
        
        # Insert the journal entry as private statements
        start = time.time()
        uid = 'urn:ers:{0}:journal-entry:{1}'.format(host_uuid,entry['uid'])
        entity = ers.create_entity(uid)
        for key, value in entry.items():
            entity.add_property_value(NS + key, value, private=True)
        ers.persist_entity(entity)
        print 'J,{}'.format(time.time()-start)

        
        # If the activity has a name, publish some public information about it
        activity_name = entry['act']
        if activity_name != 'NA':
            start = time.time()
            uid_stat = 'urn:ers:{0}:journal-stat:{1}'.format(host_uuid,activity_name)
            entity = None
            if ers.entity_exist(uid_stat):
                entity = ers.get_entity(uid_stat)
            else:
                entity = ers.create_entity(uid_stat)
                entity.add_property_value(NS + 'activity_name', activity_name)
            entity.add_property_value(NS + 'journal_entry', uid)
            ers.persist_entity(entity)
            print 'S,{}'.format(time.time()-start)
        
if __name__ == '__main__':
    go()