#!/bin/bash

python ld-in-couch.py -i ~/rdf_ntriples/repo10k.nt -g repo10k_test -d rdf10k &> /home/teodor/couchdb/loading_10k.log

python ld-in-couch.py -i ~/rdf_ntriples/repo100k.nt -g repo100k_test -d rdf100k &> /home/teodor/couchdb/loading_100k.log

python ld-in-couch.py -i ~/rdf_ntriples/repo1mil.nt -g repo1mil_test -d rdf1mil &> /home/teodor/couchdb/loading_1mil.log
