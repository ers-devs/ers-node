import argparse
import atexit
import couchdbkit
import os
import signal
import socket
import sys
import time
import zeroconf
import gobject
import logging
import logging.handlers

from ers import ERS_AVAHI_SERVICE_TYPE, ERS_PEER_TYPES, ERS_DEFAULT_DBNAME, ERS_DEFAULT_PEER_TYPE, DEFAULT_MODEL
from ers import ERS_PEER_TYPE_BRIDGE
from ers import ERSPeerInfo


class ERSDaemon:
    peer_type = None
    port = None
    dbname = None
    pidfile = None
    tries = None
    logger = None

    _active = False
    _service = None
    _monitor = None
    _db = None
    _model = None
    _repl_db = None
    _peers = None

    def __init__(self, peer_type=ERS_DEFAULT_PEER_TYPE, port=5984, dbname=ERS_DEFAULT_DBNAME,
                 pidfile='/var/run/ers_daemon.pid', tries=10, logger=None):
        self.peer_type = peer_type
        self.port = port
        self.dbname = dbname
        self.pidfile = pidfile if pidfile is not None and pidfile.lower() != 'none' else None
        self.tries = max(tries, 1)
        self.logger = logger if logger is not None else logging.getLogger('ers-daemon')

        self._peers = {}

    def start(self):
        self.logger.info("Starting ERS daemon")

        self._check_already_running()
        self._init_db_connection()

        self._update_peers_in_couchdb()
        self._clear_replication()

        service_name = 'ERS on {0} (dbname={1},type={2})'.format(socket.gethostname(), self.dbname, self.peer_type)
        self._service = zeroconf.PublishedService(service_name, ERS_AVAHI_SERVICE_TYPE, self.port)
        self._service.publish()

        self._monitor = zeroconf.ServiceMonitor(ERS_AVAHI_SERVICE_TYPE, self._on_join, self._on_leave)
        self._monitor.start()

        self._init_pidfile()

        self._active = True

        atexit.register(self.stop)

        self.logger.info("ERS daemon started")

    def _init_db_connection(self):
        try:
            server_url = "http://admin:admin@127.0.0.1:{0}/".format(self.port)

            tries_left = self.tries
            while True:
                try:
                    server = couchdbkit.Server(server_url)
                    self._db = server.get_or_create_db(self.dbname)
                    self._repl_db = server.get_db('_replicator')
                    break
                except Exception as e:
                    if 'Connection refused' in str(e) and tries_left > 0:
                        tries_left -= 1
                        time.sleep(1)
                        continue
                    else:
                        raise e

            self._model = DEFAULT_MODEL
            for doc in self._model.initial_docs():
                if not self._db.doc_exist(doc['_id']):
                    self._db.save_doc(doc)
        except Exception as e:
            raise RuntimeError("Error connecting to CouchDB: {0}".format(str(e)))

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
        if not self._active:
            return

        self.logger.info("Stopping ERS daemon")

        if self._monitor is not None:
            self._monitor.shutdown()

        if self._service is not None:
            self._service.unpublish()

        self._remove_pidfile()

        self._active = False

        self.logger.info("ERS daemon stopped")

    def _on_join(self, peer):
        ers_peer = ERSPeerInfo.from_service_peer(peer)
        if ers_peer is None:
            return

        self.logger.debug("Peer joined: " + str(ers_peer))

        self._peers[ers_peer.service_name] = ers_peer

        self._update_peers_in_couchdb()
        self._update_replication_links()

    def _on_leave(self, peer):
        if not peer.service_name in self._peers:
            return

        ex_peer = self._peers[peer.service_name]

        self.logger.debug("Peer left: " + str(ex_peer))

        del self._peers[peer.service_name]

        self._update_peers_in_couchdb()
        self._update_replication_links()

    def _update_peers_in_couchdb(self):
        state_doc = self._db.open_doc('_local/state')
        state_doc['peers'] = [peer.to_json() for peer in self._peers.values()]
        self._db.save_doc(state_doc)

    def _replication_doc_id(self, peer):
        return 'ers-auto-local-to-{0}:{1}'.format(peer.ip, peer.port)

    def _update_replication_links(self):
        desired_repl_docs = [{
            '_id': self._replication_doc_id(peer),
            'source': self.dbname,
            'target': r'http://admin:admin@{0}:{1}/{2}'.format(peer.ip, peer.port, peer.dbname),
            'continuous': True
        } for peer in self._peers.values()]

        self._apply_desired_repl_docs(desired_repl_docs)

    def _clear_replication(self):
        self._apply_desired_repl_docs([])

    def _apply_desired_repl_docs(self, desired_repl_docs):
        for desired_doc in desired_repl_docs:
            if not self._repl_db.doc_exist(desired_doc['_id']):
                self._repl_db.save_doc(desired_doc)
                self.logger.debug("Added replication doc: {0}".format(str(desired_doc)))
                continue

            doc = self._repl_db.open_doc(desired_doc['_id'])
            if any(doc[key] != desired_doc[key] for key in desired_doc):
                for key in desired_doc:
                    doc[key] = desired_doc[key]
                self._repl_db.save_doc(doc)
                self.logger.debug("Updated replication doc: {0}".format(str(desired_doc)))

        # Docs that are no longer in the desired list are deleted
        kept_doc_ids = set(doc['_id'] for doc in desired_repl_docs)
        for doc in self._replication_docs():
            if doc['_id'] not in kept_doc_ids:
                self._repl_db.delete_doc(doc)
                self.logger.debug("Deleted replication doc: {0}".format(str(doc)))

    def _replication_docs(self):
        search_view = { "map": 'function(doc) { if (doc._id.indexOf("ers-auto-") == 0) emit(doc._id, doc); }' }

        return [doc['value'] for doc in self._repl_db.temp_view(search_view)]

    def _check_already_running(self):
        if self.pidfile is not None and os.path.exists(self.pidfile):
            raise RuntimeError("The ERS daemon seems to be already running. If this is not the case, " +
                               "delete " + self.pidfile + " and try again.")

LOG_LEVELS = ['debug', 'info', 'warning', 'error', 'critical']


def setup_logging(args):
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
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", help="CouchDB port", type=int, default=5984)
    parser.add_argument("-d", "--dbname", help="CouchDB database name", type=str, default=ERS_DEFAULT_DBNAME)
    parser.add_argument("-t", "--type", help="Type of instance", type=str, default=ERS_DEFAULT_PEER_TYPE,
                        choices=ERS_PEER_TYPES)
    parser.add_argument("--pidfile", help="PID file for this ERS daemon instance (or 'none')",
                        type=str, default='/var/run/ers_daemon.pid')
    parser.add_argument("--tries", help="Number of tries to connect to CouchDB", type=int, default=10)
    parser.add_argument("--logtype", help="The log type (own file vs. syslog)", type=str, default='file',
                        choices=['file', 'syslog'])
    parser.add_argument("--logfile", help="The log file to use", type=str, default='/var/log/ers_daemon.log')
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

        mainloop = gobject.MainLoop()
        mainloop.run()
    except (KeyboardInterrupt, SystemExit):
        if mainloop is not None:
            mainloop.quit()
    except RuntimeError as e:
        logger.critical(str(e))
        failed = True

    if daemon is not None:
        daemon.stop()

    sys.exit(os.EX_SOFTWARE if failed else os.EX_OK)


if __name__ == '__main__':
    run()
