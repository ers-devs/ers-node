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

import tornado.ioloop
import tornado.web

import restkit

import zeroconf
from store import ServiceStore
from tornado import web, ioloop

ERS_AVAHI_SERVICE_TYPE = '_ers._tcp'

ERS_PEER_TYPE_CONTRIB = 'contrib'
ERS_PEER_TYPE_BRIDGE = 'bridge'
ERS_PEER_TYPES = [ERS_PEER_TYPE_CONTRIB, ERS_PEER_TYPE_BRIDGE]

ERS_DEFAULT_DBNAME = 'ers-public'
ERS_DEFAULT_PEER_TYPE = ERS_PEER_TYPE_CONTRIB


class ERSDaemon(object):
    """ 
        The daemon class for the ERS daemon.
    
        :param peer_type: type of daemon instance
        :type peer_type: 'contrib' or 'bridge'
        :param port: TCP port number of CouchDB
        :type port: int.
        :param dbname: CouchDB database name
        :type dbname: str.
        :param pidfile: filepath to a PID file for this ERS daemon instance
        :type pidfile: str.
        :param tries: number of tries to connect to CouchDB
        :type tries: int.
        :param logger: logger for the daemon to use
        :type logger: Logger instance
    """
    peer_type = None
    port = None
    dbname = None
    pidfile = None
    tries = None
    logger = None

    _active = False
    _service = None
    _monitor = None

    # List of all peers
    _peers = None

    # List of bridges
    _bridges = None

    def __init__(self, peer_type, port, dbname, pidfile, tries, logger):
        self.peer_type = peer_type
        self.port = port
        self.dbname = dbname
        self.pidfile = pidfile if pidfile is not None and pidfile.lower() != 'none' else None
        self.tries = max(tries, 1)
        self.logger = logger if logger is not None else logging.getLogger('ers-daemon')

        self._peers = {}
        self._bridges = {}

    def start(self):
        """ 
            Starting up an ERS daemon.
        """
        self.logger.info("Starting ERS daemon")
        self._check_already_running()
        self._init_db_connection()

        service_name = 'ERS on {0} (dbname={1},type={2})'.format(socket.gethostname(), self.dbname, self.peer_type)
        self._service = zeroconf.PublishedService(service_name, ERS_AVAHI_SERVICE_TYPE, self.port)
        self._service.publish()

        self._update_peers_in_couchdb()
        self._clear_replication()
        self._monitor = zeroconf.ServiceMonitor(ERS_AVAHI_SERVICE_TYPE, self._on_join, self._on_leave)
        self._monitor.start()

        self._init_pidfile()
        self._active = True

        atexit.register(self.stop)

        self.logger.info("ERS daemon started")

    def _init_db_connection(self):
        for i in range(self.tries):
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

        self.logger.info("Stopping ERS daemon")

        if self._monitor is not None:
            self._monitor.shutdown()

        if self._service is not None:
            self._service.unpublish()

        self._peers = {}
        self._bridges = {}
        self._update_peers_in_couchdb()
        self._clear_replication()

        self._remove_pidfile()

        self._active = False

        self.logger.info("ERS daemon stopped")

    def _on_join(self, peer):
        ers_peer = zeroconf.ERSPeerInfo.from_service_peer(peer)
        if ers_peer is None:
            return

        self.logger.debug("Peer joined: " + str(ers_peer))
        try:
            socket.inet_pton(socket.AF_INET, ers_peer.ip)
        except socket.error:
            self.logger.debug("Peer ignored: {} does not appear to be a valid IPv4 address".format(ers_peer.ip))
            return

        self._peers[ers_peer.service_name] = ers_peer
        if ers_peer.peer_type == ERS_PEER_TYPE_BRIDGE:
            self._bridges[ers_peer.service_name] = ers_peer

        self._update_peers_in_couchdb()
        self._update_replication_links()
        self._update_cache()

    def _on_leave(self, peer):
        if not peer.service_name in self._peers:
            return

        ex_peer = self._peers[peer.service_name]

        self.logger.debug("Peer left: " + str(ex_peer))

        del self._peers[peer.service_name]
        if peer.service_type == ERS_PEER_TYPE_BRIDGE:
            del self._bridges[peer.service_name]

        self._update_peers_in_couchdb()
        self._update_replication_links()

    def _update_peers_in_couchdb(self):
        state_doc = self._store.public.open_doc('_local/state')

        # If there are bridges, do not record other peers in the state_doc.
        visible_peers = self._bridges or self._peers
        state_doc['peers'] = [peer.to_json() for peer in visible_peers.values()]
        self._store.public.save_doc(state_doc)

    def _desired_replication_docs(self):
        docs = {}
        for peer in self._bridges.values():
            doc_id = 'ers-auto-local-to-{0}:{1}'.format(peer.ip, peer.port)
            docs[doc_id] = {
                '_id': doc_id,
                'source': self.dbname,
                'target': r'http://{0}:{1}/{2}'.format(peer.ip, peer.port, peer.dbname),
                'continuous': True}
        return docs

    def _update_replication_links(self):
        self._store.update_replicator_docs(self._desired_replication_docs())

    def _clear_replication(self):
        self._store.update_replicator_docs({})

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


class WebAPIHandler(web.RequestHandler):
    def get(self):
        self.write("Hello, world")

    
    
LOG_LEVELS = ['debug', 'info', 'warning', 'error', 'critical']


def setup_logging(args):
    """ 
        Setup a file or system logger.
    
        :param args: has a logtype attribute (which can be 'file' or 'syslog').
        :type args: Object
        :rtype: Logger instance
    """
    logger = logging.getLogger('ers-daemon')
    logger.setLevel(10 + 10 * LOG_LEVELS.index(args.loglevel))

    if args.logtype == 'file':
        handler = logging.FileHandler(args.logfile)
        handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
    elif args.logtype == 'syslog':
        handler = logging.handlers.SysLogHandler(address='/dev/log')
        handler.setFormatter(logging.Formatter("%(message)s"))
    else:
        handler = None

    if handler is not None:
        logger.addHandler(handler)

    return logger


def run():
    """ 
        Parse the given arguments for setting up the daemon and run it.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", help="CouchDB port", type=int, default=5984)
    parser.add_argument("-d", "--dbname", help="CouchDB database name", type=str, default=ERS_DEFAULT_DBNAME)
    parser.add_argument("-t", "--type", help="Type of instance", type=str, default=ERS_DEFAULT_PEER_TYPE,
                        choices=ERS_PEER_TYPES)
    parser.add_argument("--pidfile", help="PID file for this ERS daemon instance (or 'none')",
                        type=str, default='/var/run/ers/daemon.pid')
    parser.add_argument("--tries", help="Number of tries to connect to CouchDB", type=int, default=10)
    parser.add_argument("--logtype", help="The log type (own file vs. syslog)", type=str, default='file',
                        choices=['file', 'syslog'])
    parser.add_argument("--logfile", help="The log file to use", type=str, default='/var/log/ers/daemon.log')
    parser.add_argument("--loglevel", help="Log messages of this level and above", type=str, default='info',
                        choices=LOG_LEVELS)
    args = parser.parse_args()

    logger = setup_logging(args)

    daemon = None
    failed = False
    mainloop = None
    try:
        daemon = ERSDaemon(args.type, args.port, args.dbname, args.pidfile, args.tries, logger)
        daemon.start()

        def sig_handler(sig, frame):
            mainloop.quit()
        signal.signal(signal.SIGQUIT, sig_handler)
        signal.signal(signal.SIGTERM, sig_handler)

        # Initialise the web interface handler
        application = web.Application([
            (r"/api/(.*)", WebAPIHandler),
            (r"/(.*)", web.StaticFileHandler, {'path': 'www', 'default_filename' : 'index.html'}),
        ])
        application.listen(8888, address="127.0.0.1")

        # Start the main loop
        mainloop = ioloop.IOLoop.instance()
        mainloop.start()
        
    except (KeyboardInterrupt, SystemExit):
        if mainloop is not None:
            mainloop.stop()
    except RuntimeError as e:
        logger.critical(str(e))
        failed = True

    if daemon is not None:
        daemon.stop()

    sys.exit(os.EX_SOFTWARE if failed else os.EX_OK)


if __name__ == '__main__':
    run()
