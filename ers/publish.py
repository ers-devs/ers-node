#!/usr/bin/python

"""
ers.publish

Pushes local changes to Aggregator

Python script to be launched: python publish.py

Variables that may be changed accordingly:
1. SYNC_PERIOD_SEC -> number of seconds between to consecutive synchronizations; maybe
a decent value between 10s and 30s would be fine
2. GLOBAL_SERVER_HOST -> the IP of the global server where ERS is running
3. GLOBAL_SERVER_PORT -> the PORT of the global server where ERS is running
4. GLOBAL_SERVER_HTTP_SEQ -> the path URL of ERS's servlet for accesing the sequence number;
the important bit here is the first part of the path as ERS may be deployed as root app or not;
e.g. if deployed as root app, the value MUST be 'last_sync_seq'
5. GLOBAL_SERVER_HTTP_BULKRUN -> same as above
e.g. if deployed as root app, the value MUST be 'bulkrun'

If the script is intended to be tested or used as it is (i.e. without an integration
with the ERS local stuff), there is already in place a test function.
First line creates the ERS object that needs to get the CouchDB url. Therefore, that should be set
accordingly. The database to be synchronized is 'ers-public'.

Next step is to use the SynchronizationManager object you just created. There are two possible
scenarios as follows:
1. to use one thread per each graph to be synchronized (this is the initial version when the 'id' of a
document contains 'graph_name URN'); if needed, the thread can be stopped
2. to start synchronization of the entire database (i.e. 'ers-public') - this is adapted to the current
data model; however, the problem is that whenever a delete occurs, _changes feed gives us the 'id' of
such a document, but not the fields it contains - therefore, '@id' field is missing and the global
cannot delete this document; as before, this synchronization thread can be stopped or the entire
program killed
"""

__author__ = 'Teodor Macicas'


import time
import os.path
import requests

from threading import Thread
from couchdbkit.changes import ChangesStream
from string import Template

import functools
from api import ERS
ERSReadWrite = functools.partial(ERS, local_only=True)

# Maximum this number of changes are retrieved once. It can be an issue if the DB has never been synchronized
LIMIT_MAXSEQ_PER_OP = 1000
# A timeout of this amount of secs is run inbetween two consecutive synchronizations.
SYNC_PERIOD_SEC=30
# the global keyspace where all the synched data is stored
GLOBAL_TARGET_KEYSPACE = "sync_bridges"
# HTTP address of the global server
GLOBAL_SERVER_HOST = "127.0.0.1"
GLOBAL_SERVER_PORT = 8080
# The RESTful endpoint for handling the last synchronized sequence
#GLOBAL_SERVER_HTTP_SEQ = "/ers/last_sync_seq"
GLOBAL_SERVER_HTTP_SEQ = "/ers/last_sync_seq"
# RESTful endpoint for uploading a file containing document changes
#GLOBAL_SERVER_HTTP_BULKRUN = "/ers/bulkrun"
GLOBAL_SERVER_HTTP_BULKRUN = "/ers/bulkrun"
# The filename used to dump the changes
OUTPUT_FILENAME = Template("/www-data/changes_$graph.log")


class GraphSynch(Thread):
    """ Synchronizes a peer's graph data with the global aggregator (creates a synch thread for a given graph using _changes feed).

        :param ERSReadWrite: ERSReadWrite to used for synchronization
        :type ERSReadWrite: ERSReadWrite instance
        :param name: thread name
        :type name: str.
        :param graph: graph to use for synchronization
        :type graph: str.
    """
    def __init__(self, ERSReadWrite, name, graph):
        Thread.__init__(self)
        self.ers = ERSReadWrite
        self.name = name
        self.graph = graph
        self.finished = False
        if not self.ers.public_db.doc_exist(self.filter_by_graph_doc()['_id']):
            self.ers.public_db.save_doc(self.filter_by_graph_doc())
        self.output_filename = OUTPUT_FILENAME.substitute(graph = self.graph)


    def get_previous_seq_num(self):
        """ Ask global server what was the last synchronized sequence number for this graph.

            :returns: previous sequence number (-1 if server request didn't succeed)
            :rtype: int.
        """
        r = requests.get("http://"+GLOBAL_SERVER_HOST+":"+str(GLOBAL_SERVER_PORT)+GLOBAL_SERVER_HTTP_SEQ+
                            "?g=<"+self.graph+">")
        if r.status_code == 200:
            return r.text
        else:
            print 'Error getting the previous sequence number'
            print 'Maybe there is no graph ', self.graph, ' on the global server'
            print r.status_code, r.reason
            return -1

    def set_new_seq_num(self, new_seq):
        """ Update the global last synchronized sequence number for this graph.

            :param new_seq: new sequence number
            :type new_seq: int.
            :returns: success status
            :rtype: bool.
        """
        r = requests.post("http://"+GLOBAL_SERVER_HOST+":"+str(GLOBAL_SERVER_PORT)+GLOBAL_SERVER_HTTP_SEQ,
                            data={"g" : "<"+self.graph+">", "seq" : new_seq})
        if r.status_code == 200:
            return True
        else:
            print r.status_code, r.reason
            return False

    def filter_by_graph_doc(self):
        """ Defines the filter document by graph. One DB stores multiple graphs.

            :returns: a CouchDB filter design document
            :rtype: JSON object
        """
        return  {
            "_id": "_design/filter",
            "filters": {
                    "by_graph": "function(doc, req) { if(doc._id.substring(0, req.query.g.length) === req.query.g && doc._id[req.query.g.length] === ' '  )  return true;  else return false; }"
                    }
               }


    def dump_changes_to_file(self, prev_seq, stream):
        """ Save the changes made to a log file.

            :param prev_seq: sequence number of the previous successfully synchronized sequence from the global aggregator
            :type prev_seq: int.
            :param stream: stream of made changes
            :type stream: ChangesStream instance
            :returns: last synchronized sequence number
        """
        if os.path.exists(self.output_filename):
            os.remove(self.output_filename)
        o_file = open(self.output_filename, "w")

        last_seq_n = prev_seq
        for c in stream:
            last_seq_n = c['seq']
            # Has the document been deleted?
            if 'deleted' in c and c['deleted'] == True:
		# if we synch everything (i.e. not a thread per graph and not id='graph id' data model)
		if self.graph == GLOBAL_TARGET_KEYSPACE:
                    doc_id = "<"+c['id']+">"
		else:
                    doc_id = "<"+c['id'][len(self.graph)+1:]+">"
                o_file.write(doc_id + " <NULL> <NULL> \"4\" . \n")
            else:
		# if we synch everything (i.e. not a thread per graph and not id='graph id' data model)
		if self.graph == GLOBAL_TARGET_KEYSPACE:
		    # do not synch design docs
		    if c['doc']['_id'].startswith("_design"):
			continue
	            doc_id = str("<"+c['doc']['@id']+">")
		else:
	            doc_id = "<"+c['doc']['_id'][len(self.graph)+1:]+">"

                add_delete = False
                for param in c['doc'].keys():
                    if param == '_rev' or param == '_id' or param == '@id':
                        continue
                    if param.startswith('http'):
                        param2 = "<" + param + ">"
                    else:
                        param2 = "\"" + param + "\""
                    if not add_delete:
                        o_file.write(doc_id + " <NULL> <NULL> \"4\" . \n")
                        add_delete = True

                    if c['doc'][param] == None:
                        value = ""
                        o_file.write(doc_id + " " + param2 + " " + value + " \"1\" . \n")
                    else:
                        for list_value in c['doc'][param]:
                            list_value = str(list_value)
                            if list_value.startswith('http'):
                                value = "<" + list_value + ">"
                            else:
                                value = "\"" + list_value + "\""
                            o_file.write(doc_id + " " + param2 + " " + value + " \"1\" . \n")
        o_file.close()
        return last_seq_n


    def set_finished(self):
        """ Set the synchronization status to finished.
        """
        self.finished = True


    def run(self):
        """ Start the synchronization process
        """
        while True:
            if self.finished:
                break
            time.sleep(SYNC_PERIOD_SEC)

            # get previous successfully synchronized sequence from global server
            prev_seq_n = int(self.get_previous_seq_num())
	    """ COMMENT THIS LINE (used for tests)
	    prev_seq_n = 2 """
            if prev_seq_n == -1:
                continue

            #print "Previous sequence: " + str(prev_seq_n)
	    # if synch everything, then do not use the filter
	    if self.graph == GLOBAL_TARGET_KEYSPACE:
            	stream = ChangesStream(self.ers.public_db, feed="normal", since=prev_seq_n,
                        include_docs=True, limit=LIMIT_MAXSEQ_PER_OP)
	    else:
                stream = ChangesStream(self.ers.public_db, feed="normal", since=prev_seq_n,
                        include_docs=True, limit=LIMIT_MAXSEQ_PER_OP, filter="filter/by_graph", g=self.graph)

            # dump to file the changes and post it to global server is not empty
            last_seq_n = self.dump_changes_to_file(prev_seq_n, stream)
            # are there entities to be synchronized?
            if last_seq_n == prev_seq_n:
                print 'Nothing new ...'
                continue

            """ Now upload the changes file to global server. """
            r = requests.post("http://"+GLOBAL_SERVER_HOST+":"+str(GLOBAL_SERVER_PORT)
                    +GLOBAL_SERVER_HTTP_BULKRUN, files={self.output_filename: open(self.output_filename, 'rb')},
                    data={"g" : "<"+self.graph+">"})

            if r.status_code == 200:
                # it means that this step of synchronization is working
		""" UNCOMMENT NEXT LINE (not used in case of testing) """
                self.set_new_seq_num(last_seq_n)
                print 'Last synchronization sequence number is ' + str(last_seq_n)


class SynchronizationManager(object):
    """ Manages a synchronization process.

        :param ERSReadWrite: ERSReadWrite to used for synchronization
        :type ERSReadWrite: ERSReadWrite instance
    """
    def __init__(self, ERSReadWrite):
        self.active_repl = dict()
        self.ers = ERSReadWrite

    def get_thread_name(self, graph):
        """ Get the tread name of the synchronization process.

            :param graph: graph used for synchronization
            :type graph: str.
            :rtype: str.
        """
        return 'synch_'+graph

    def exists_synch_thread(self, graph):
        """ Check whether a synchronization process exists for the given graph.

            :param graph: graph to use for synchronization
            :type graph: str.
            :rtype: bool.
        """
        return self.get_thread_name(graph) in self.active_repl

    def start_synch(self, graph):
        """ Start the synchronization process.

            :param graph: graph to use for synchronization
            :type graph: str.
        """
        if self.exists_synch_thread(graph):
            print 'Another thread that synchronize graph ' + graph + ' already runs!'
        new_synch = GraphSynch(self.ers, self.get_thread_name(graph), graph)
        new_synch.start()
        # add the new thread into the dictionary
        self.active_repl[self.get_thread_name(graph)] = new_synch

    def start_synch_everything(self):
        new_synch = GraphSynch(self.ers, GLOBAL_TARGET_KEYSPACE, GLOBAL_TARGET_KEYSPACE)
        new_synch.start()
        # add the new thread into the dictionary
        self.active_repl[GLOBAL_TARGET_KEYSPACE] = new_synch


    def stop_synch(self, graph):
        """ Stop the synchronization process.

            :param graph: graph used for synchronization
            :type graph: str.
        """
        if self.get_thread_name(graph) in self.active_repl:
            thread = self.active_repl[self.get_thread_name(graph)]
            if thread.isAlive():
               thread.set_finished()
        else:
            print 'Synchronization thread for graph ' + graph + ' does not exist.'

    def stop_synch_everything(self):
	thread = self.active_repl[GLOBAL_TARGET_KEYSPACE]
	if thread.isAlive():
	   thread.set_finished()



def test():
    synch_manager = SynchronizationManager(ERSReadWrite(server_url=r'http://admin:admin@127.0.0.1:5984/'))
    synch_manager.start_synch_everything()
    time.sleep(6)
    synch_manager.stop_synch_everything()

if __name__ == '__main__':
    """test()"""
    synch_manager = SynchronizationManager(ERSReadWrite(server_url=r'http://admin:admin@127.0.0.1:5984/'))
    synch_manager.start_synch_everything()
