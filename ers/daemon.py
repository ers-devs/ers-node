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
import restkit
import zeroconf

from store import ServiceStore
from ConfigParser import SafeConfigParser
from defaults import ERS_AVAHI_SERVICE_TYPE, ERS_PEER_TYPE_BRIDGE, ERS_PEER_TYPE_CONTRIB
from defaults import set_logging
import gobject
from zeroconf import ERSPeerInfo

log = logging.getLogger('ers')

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
        self._init_db_connection()

        service_name = 'ERS on {0} (prefix={1},type={2})'.format(socket.gethostname(), self.prefix, self.peer_type)
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
        for i in range(self.tries):  # @UnusedVariable
            try:
                self._store = ServiceStore()
                return
            except restkit.RequestError:
                time.sleep(1)
            except Exception as e:
                raise RuntimeError("Error connecting to CouchDB: {0}".format(str(e)))
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
        state_doc = self._store.public.open_doc('_local/state')

        # If there are bridges, do not record other peers in the state_doc.
        visible_peers = None
        if len(self._peers[ERS_PEER_TYPE_BRIDGE]) != 0:
            visible_peers = self._peers[ERS_PEER_TYPE_BRIDGE] 
        else:
            visible_peers = self._peers[ERS_PEER_TYPE_CONTRIB]
        state_doc['peers'] = [peer.to_json() for peer in visible_peers.values()]
        self._store.public.save_doc(state_doc)

    def _update_replication_links(self):
        '''
        Update the links to replicate documents with other contributors and bridges
        '''
        log.debug("Update replication documents")
        
        # Clear all the previous replicator documents
        self._clear_replication_documents()
        
        # List of replication rules
        docs = {}
        
        # If there is at least one bridge in the neighbourhood focus on it
        # otherwise establish rules with all the contributors
        if len(self._peers[ERS_PEER_TYPE_BRIDGE]) != 0:
            # Publish all the public documents to the cache of the bridges
            for peer in self._peers[ERS_PEER_TYPE_BRIDGE].values():
                doc_id = 'ers-auto-local-to-{0}:{1}'.format(peer.ip, peer.port)
                docs[doc_id] = {
                    '_id': doc_id,
                    'source': 'ers-public',
                    'target': r'http://{0}:{1}/{2}'.format(peer.ip, peer.port, 'ers-cache'),
                    'continuous': True
                    # TODO add the option to not do it too often
                }
                
            # Synchronise local cache and bridge cache
            # TODO
        else:
            cache_contents = self._store.cache_contents()
            if cache_contents:
                # Synchronise all the cached documents with the peers
                for peer in self._peers[ERS_PEER_TYPE_CONTRIB].values():
                    doc_id = 'ers-auto-cache-to-{0}:{1}'.format(peer.ip, peer.port)
                    docs[doc_id] = {
                        '_id': doc_id,
                        'source': self._store.cache.dbname,
                        'target': r'http://{0}:{1}/{2}'.format(peer.ip, peer.port, 'ers-cache'),
                        'continuous': True,
                        'doc_ids' : cache_contents
                    }
                    
                # Get update from their public documents we have cached
                for peer in self._peers[ERS_PEER_TYPE_CONTRIB].values():
                    doc_id = 'ers-auto-cache-to-{0}:{1}'.format(peer.ip, peer.port)
                    docs[doc_id] = {
                        '_id': doc_id,
                        'source': r'http://{0}:{1}/{2}'.format(peer.ip, peer.port, 'ers-public'),
                        'target': self._store.cache.dbname,
                        'continuous': True,
                        'doc_ids' : cache_contents
                    }
            
            
        log.debug(docs)
        
        # If this node is a bridge configured to push data to an aggregator
        # add one more rule to do that
        
        # Apply sync rules
        self._set_replication_documents(docs)

    def _clear_replication_documents(self):
        self._set_replication_documents({})

    def _set_replication_documents(self, documents):
        self._store.update_replicator_docs(documents)
        
    def _update_cache(self):
        cache_contents = self._store.cache_contents()
        if not cache_contents:
            return
        for peer in self._peers.values():
            for dbname in ('ers-public', 'ers-cache'):
                source_db = r'http://{0}:{1}/{2}'.format(peer.ip, peer.port, dbname)
                repl_doc = {
                    'target': self._store.cache.dbname,
                    'source': source_db,
                    'continuous': False,
                    'doc_ids' : cache_contents                
                }
                self._store.replicator.save_doc(repl_doc)

    def _check_already_running(self):
        if self.pidfile is not None and os.path.exists(self.pidfile):
            raise RuntimeError("The ERS daemon seems to be already running. If this is not the case, " +
                               "delete " + self.pidfile + " and try again.")


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
    
    daemon = None
    failed = False
    mainloop = None
    try:
        daemon = ERSDaemon(config)
        daemon.start()

        def sig_handler(sig, frame):
            mainloop.quit()
        signal.signal(signal.SIGQUIT, sig_handler)
        signal.signal(signal.SIGTERM, sig_handler)

        # Initialise the web interface handler
        #application = web.Application([
        #    (r"/api/(.*)", WebAPIHandler),
        #    (r"/(.*)", web.StaticFileHandler, {'path': 'www', 'default_filename' : 'index.html'}),
        #])
        #application.listen(8888, address="127.0.0.1")

        # Start the main loop
        mainloop = gobject.MainLoop()
        mainloop.run()
        
    except (KeyboardInterrupt, SystemExit):
        if mainloop is not None:
            mainloop.quit()
    except RuntimeError as e:
        log.critical(str(e))
        failed = True

    if daemon is not None:
        daemon.stop()

    sys.exit(os.EX_SOFTWARE if failed else os.EX_OK)


if __name__ == '__main__':
    run()
