#!/usr/bin/python

"""
ers.daemon

Publishes ERS service, monitors ERS peers, manages peer-to-peer replications.

If a bridge is present in the network the daemon initiates continuous
replication from the local public store to the database advertised by the
bridge. The daemon also makes a single attempt to get a newer
version of documents in the local cache store when a new peer is
discovered.
"""

import argparse
import atexit
import os
import signal
import socket
import sys
import time
import logging.handlers
import zeroconf
import uuid

from store import ServiceStore
from ConfigParser import SafeConfigParser
from defaults import ERS_AVAHI_SERVICE_TYPE, ERS_PEER_TYPE_BRIDGE, ERS_PEER_TYPE_CONTRIB
from defaults import set_logging
import gobject
from zeroconf import ERSPeerInfo
from store import ERS_PUBLIC_DB, ERS_CACHE_DB, ERS_STATE_DB

import requests
from flask import Flask, request
import threading
from copy import deepcopy

log = logging.getLogger('ers')
FLASK_PORT = 5678

class Configuration(object):
    """
    Wrapper for the configuration file
    """
    def __init__(self, file_name):
        # Read the configuration file
        self._config = SafeConfigParser()
        self._config.read(file_name)

        # Get the logger
        #self._config.get('log','level')

        self._setup_logging()

    def _setup_logging(self):
        """
        Configure a logger for the ERS daemon
        """
        # Set the output handler
        handler = None
        if self._config.get('log','output') == 'syslog':
            handler = logging.handlers.SysLogHandler(address='/dev/log')
            handler.setFormatter(logging.Formatter("%(message)s"))
        elif self._config.get('log','output').startswith('/'):
            handler = logging.FileHandler(self._config.get('log','output'))
            handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
        else:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))

        set_logging(self._config.get('log','level'), handler=handler)

    def logger(self):
        """
        Return the logger
        """
        return self._logger

    def tries(self):
        tries =  int(self._config.get('couchdb','tries'))
        return max(tries, 1)

    def pidfile(self):
        pidfile = self._config.get('node','pid_file')
        return pidfile if pidfile is not None and pidfile.lower() != 'none' else None

    def prefix(self):
        return self._config.get('couchdb','prefix')

    def port(self):
        return int(self._config.get('couchdb','port'))

    def node_type(self):
        return self._config.get('node','type')

class ERSDaemon(object):
    """
        The daemon class for the ERS daemon.

        :param config: configuration object
        :type config: Configuration
    """
    peer_type = None
    port = None
    prefix = None
    pidfile = None
    tries = None
    logger = None

    _active = False
    _service = None
    _monitor = None
    #this maintains some basic state. 
    #we don't want to do too many updates to the replicator database
    #because it might stop current replications
    _old_replication_docs = {}

    # List of all peers, contributor and bridges
    _peers = {
        ERS_PEER_TYPE_CONTRIB : {},
        ERS_PEER_TYPE_BRIDGE : {}
    }

    def __init__(self, config):
        """
        Constructor
        """
        self.config = config
        self.peer_type = config.node_type()
        self.port = config.port()
        self.prefix = config.prefix()
        self.pidfile = config.pidfile()
        self.tries = config.tries()

    def start(self):
        """
        Starting up an ERS daemon.
        """
        log.info("Starting ERS daemon")
        self._check_already_running()

        log.debug("Initialise CouchDB")
        self._init_db_connection()

        log.debug("Publish service on ZeroConf")
        # if the hostname is the same as another node, avahi will not pick up the service
        # so we must add some unique identifier
        # uuid4 guarantees unique identifiers, but we cannot fit the whole 32 characters otherwise the name becomes too long
        # thus, we only choose the first 20 and hope there will be no collisions
        service_name = 'ERS on {0} (prefix={1},type={2})'.format(socket.gethostname() + str(uuid.uuid4())[:20], self.prefix, self.peer_type)
        self._service = zeroconf.PublishedService(service_name, ERS_AVAHI_SERVICE_TYPE, self.port)
        self._service.publish()

        self._update_peers_in_couchdb()
        self._clear_replication_documents()
        self._monitor = zeroconf.ServiceMonitor(ERS_AVAHI_SERVICE_TYPE, self._on_join, self._on_leave)
        self._monitor.start()

        self._init_pidfile()
        self._active = True

        atexit.register(self.stop)

        log.info("ERS daemon started")

    def _init_db_connection(self):
        log.debug("Initialise databases")
        for i in range(self.tries):  # @UnusedVariable
            try:
                self._store = ServiceStore()
                return
            except Exception as e:
                #raise RuntimeError("Error connecting to CouchDB: {0}".format(str(e)))
                log.info("Error connecting to Couchdb on try {0} with exception {1}".format(str(i), str(e)))
        raise RuntimeError("Error connecting to CouchDB. Please make sure CouchDB is running.")

    def _init_pidfile(self):
        if self.pidfile is not None:
            with file(self.pidfile, 'w+') as f:
                f.write("{0}\n".format(os.getpid()))

    def _remove_pidfile(self):
        if self.pidfile is not None:
            try:
                os.remove(self.pidfile)
            except IOError:
                pass

    def stop(self):
        """
        Stopping an ERS daemon.
        """
        if not self._active:
            return

        log.info("Stopping ERS daemon")

        if self._monitor is not None:
            self._monitor.shutdown()

        if self._service is not None:
            self._service.unpublish()

        self._peers['contributors'] = {}
        self._peers['bridges'] = {}
        self._update_peers_in_couchdb()
        self._clear_replication_documents()

        self._remove_pidfile()

        self._active = False

        log.info("ERS daemon stopped")

    def _on_join(self, peer):
        ers_peer = zeroconf.ERSPeerInfo.from_service_peer(peer)
        if ers_peer is None:
            return

        log.debug("Peer joined: " + str(ers_peer))
        try:
            socket.inet_pton(socket.AF_INET, ers_peer.ip)
        except socket.error:
            log.debug("Peer ignored: {} does not appear to be a valid IPv4 address".format(ers_peer.ip))
            return

        self._peers[ers_peer.peer_type][ers_peer.service_name] = ers_peer

        self._update_peers_in_couchdb()
        self._update_replication_links()
        self._update_cache()

    def _on_leave(self, peer):
        """
        Called when a peer leaved the neighbourhood
        """
        peer_info = ERSPeerInfo.from_service_peer(peer)

        if peer.service_name not in self._peers[peer_info.peer_type]:
            return

        ex_peer = self._peers[peer_info.peer_type][peer.service_name]
        log.debug("Peer left: " + str(ex_peer))

        del self._peers[peer_info.peer_type][peer.service_name]

        self._update_peers_in_couchdb()
        self._update_replication_links()

    def _update_peers_in_couchdb(self):
        log.debug("Update peers in CouchDB")
        state_doc = self._store[ERS_STATE_DB]['_local/state']
        ok = False
        while ok == False:
            # If there are bridges, do not record other peers in the state_doc.
            visible_peers = None
            if len(self._peers[ERS_PEER_TYPE_BRIDGE]) != 0:
                visible_peers = self._peers[ERS_PEER_TYPE_BRIDGE]
            else:
                visible_peers = self._peers[ERS_PEER_TYPE_CONTRIB]
            state_doc['peers'] = [peer.to_json() for peer in visible_peers.values()]
            try:
                self._store[ERS_STATE_DB].save(state_doc)
                ok = True
            except Exception as e:
                pass


    def _update_replication_links(self):
        '''
        Update the links to replicate documents with other contributors and bridges
        '''
        log.debug("Update replication documents")

        # Clear all the previous replicator documents
        self._clear_replication_documents()

        # List of replication rules
        docs = {}

        cache_contents = self._store.cache_contents()
        # If there is at least one bridge in the neighbourhood focus on it
        # otherwise establish rules with all the contributors
        if len(self._peers[ERS_PEER_TYPE_BRIDGE]) != 0:
            # Publish all the public documents to the cache of the bridges
            for peer in self._peers[ERS_PEER_TYPE_BRIDGE].values():
                doc_id = 'ers-{2}-auto-local-to-{0}:{1}'.format(peer.ip, peer.port, socket.gethostname())
                #docs[doc_id] = {
                #    '_id': doc_id,
                #    'source': 'ers-public',
                #    'target': r'http://{0}:{1}/{2}'.format(peer.ip, peer.port, 'ers-cache'),
                #    'continuous': True
                #    # TODO add the option to not do it too often
                #}
                # ALSO cache
                doc_id = 'ers-{2}-pull-from-bridge-{0}:{1}'.format(peer.ip, peer.port, socket.gethostname())
                docs[doc_id] = {
                    '_id': doc_id,
                    'source': r'http://{0}:{1}/{2}'.format(peer.ip, peer.port, 'ers-cache'),
                    'target':self._store[ERS_CACHE_DB].name,
                    'continuous': True,
                    'doc_ids': cache_contents,
                    # TODO add the option to not do it too often
                }


            # Synchronise local cache and bridge cache
            # TODO
        else:
            #if cache_contents:
            # Synchronise all the cached documents with the peers
            for peer in self._peers[ERS_PEER_TYPE_CONTRIB].values():
                doc_id = 'ers-{2}-get-from-cache-of-{0}:{1}'.format(peer.ip, peer.port,socket.gethostname())
                docs[doc_id] = {
                    '_id': doc_id,
                    'source': r'http://{0}:{1}/{2}'.format(peer.ip, peer.port, 'ers-cache'),
                    'target':self._store[ERS_CACHE_DB].name,
                    'continuous': True,
                    #'doc_ids' : cache_contents
                }
                #if a regular peer, only sync cached documents.
                #as a bridge, we pull evertyhing
                if self.peer_type == ERS_PEER_TYPE_CONTRIB:
                    docs[doc_id]['doc_ids'] = cache_contents

            # Get update from their public documents we have cached
            for peer in self._peers[ERS_PEER_TYPE_CONTRIB].values():
                doc_id = 'ers-{2}-auto-get-from-public-of-{0}:{1}'.format(peer.ip, peer.port, socket.gethostname())
                docs[doc_id] = {
                    '_id': doc_id,
                    'source': r'http://{0}:{1}/{2}'.format(peer.ip, peer.port, 'ers-public'),
                    'target': self._store[ERS_CACHE_DB].name,
                    'continuous': True,
                    #'doc_ids' : cache_contents
                }
                #if a regular peer, only sync cached documents.
                #as a bridge, we pull evertyhing
                if self.peer_type == ERS_PEER_TYPE_CONTRIB:
                    docs[doc_id]['doc_ids'] = cache_contents


        log.debug(docs)

        # If this node is a bridge configured to push data to an aggregator
        # add one more rule to do that

        # Apply sync rules
        if self._old_replication_docs != docs:
            self._old_replication_docs = deepcopy(docs)

            self._set_replication_documents(docs)

    def _clear_replication_documents(self):
        self._set_replication_documents({})

    def _set_replication_documents(self, documents):
        self._store.update_replicator_docs(documents)

    def _update_cache(self):
        cache_contents = self._store.cache_contents()
        if not cache_contents:
            return
        for peer in self._peers[ERS_PEER_TYPE_CONTRIB].values():
            for dbname in ('ers-public', 'ers-cache'):
                source_db = r'http://{0}:{1}/{2}'.format(peer.ip, peer.port, dbname)
                repl_doc = {
                    'target': self._store[ERS_CACHE_DB].name,
                    'source': source_db,
                    'continuous': False,
                    'doc_ids' : cache_contents
                }
                self._store.replicator.save(repl_doc)

    def _check_already_running(self):
        log.debug("Check if already running")
        if self.pidfile is not None and os.path.exists(self.pidfile):
            raise RuntimeError("The ERS daemon seems to be already running. If this is not the case, " +
                               "delete " + self.pidfile + " and try again.")

daemon = None

app = Flask(__name__)

@app.route('/ReplicationLinksUpdate')
def update_replication_links_api():
    global daemon
    daemon._update_replication_links()
    return 'Replication links updated'


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()

@app.route('/Stop')
def stop_server():
    shutdown_server()
    return 'Server shutting down'

def run_flask():
    app.run(port=FLASK_PORT, threaded = True)

def run():
    """
    Parse the given arguments for setting up the daemon and run it.
    """
    # Parse the command line
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="Configuration file", type=str, default='ers-node.ini')
    args = parser.parse_args()

    # Create the configuration object
    config = Configuration(args.config)

    global daemon
    failed = False
    mainloop = None
    try:
        daemon = ERSDaemon(config)
        daemon.start()

        def sig_handler(sig, frame):
            mainloop.quit()
        try:
            requests.get('http://localhost:'+str(FLASK_PORT)+'/Stop')
        except Exception as e:
            pass


        signal.signal(signal.SIGQUIT, sig_handler)
        signal.signal(signal.SIGTERM, sig_handler)

        # Start the main loop
        thread = threading.Thread(target = run_flask)
        thread.start()
        mainloop = gobject.MainLoop()
        gobject.threads_init()

        mainloop.run()

    except (KeyboardInterrupt, SystemExit):
        if mainloop is not None:
            mainloop.quit()
        requests.get('http://localhost:'+str(FLASK_PORT)+'/Stop')
    except RuntimeError as e:
        log.critical(str(e))
        failed = True

    if daemon is not None:
        daemon.stop()
        #maybe daemon has been stopped some other way ?
        try:
            requests.get('http://localhost:'+str(FLASK_PORT)+'/Stop')
        except Exception as e:
            pass

    sys.exit(os.EX_SOFTWARE if failed else os.EX_OK)


if __name__ == '__main__':
    run()

