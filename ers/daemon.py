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

from ers import ERS_AVAHI_SERVICE_TYPE, ERS_PEER_TYPES, ERS_DEFAULT_DBNAME, ERS_DEFAULT_PEER_TYPE, DEFAULT_MODEL
from ers import ERSPeerInfo


class ERSDaemon:
    peer_type = None
    port = None
    dbname = None
    pidfile = None
    tries = None

    _active = False
    _service = None
    _monitor = None
    _db = None
    _model = None
    _repl_db = None
    _peers = None

    def __init__(self, peer_type=ERS_DEFAULT_PEER_TYPE, port=5984, dbname=ERS_DEFAULT_DBNAME,
                 pidfile='/var/run/ers_daemon.pid', tries=10):
        self.peer_type = peer_type
        self.port = port
        self.dbname = dbname
        self.pidfile = pidfile
        self.tries = max(tries, 1)

        self._peers = {}

    def start(self):
        self._check_already_running()
        self._init_db_connection()

        self._update_peers_in_couchdb()
        self._clear_replication()

        service_name = 'ERS on {0} (dbname={1},type={2})'.format(socket.gethostname(), self.dbname, self.peer_type)
        self._service = zeroconf.PublishedService(service_name, ERS_AVAHI_SERVICE_TYPE, self.port)
        self._service.publish()

        self._monitor = zeroconf.ServiceMonitor(ERS_AVAHI_SERVICE_TYPE, self._on_join, self._on_leave)
        self._monitor.start()

        with file(self.pidfile, 'w+') as f:
            f.write("{0}\n".format(os.getpid()))

        self._active = True

        atexit.register(self.stop)

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

    def stop(self):
        if not self._active:
            return

        if self._monitor is not None:
            self._monitor.shutdown()

        if self._service is not None:
            self._service.unpublish()

        os.remove(self.pidfile)

        self._active = False

    def _on_join(self, peer):
        ers_peer = ERSPeerInfo.from_service_peer(peer)
        if ers_peer is None:
            return

        self._peers[ers_peer.service_name] = ers_peer

        self._update_peers_in_couchdb()
        self._setup_replication(ers_peer)

    def _on_leave(self, peer):
        if not peer.service_name in self._peers:
            return

        ex_peer = self._peers[peer.service_name]
        del self._peers[peer.service_name]

        self._update_peers_in_couchdb()
        self._teardown_replication(ex_peer)

    def _update_peers_in_couchdb(self):
        state_doc = self._db.open_doc('_design/state')
        state_doc['peers'] = [peer.to_json() for peer in self._peers.values()]
        self._db.save_doc(state_doc)

    def _replication_id(self, peer):
        return 'ers-auto-local-to-{0}:{1}'.format(peer.ip, peer.port)

    def _setup_replication(self, peer):
        doc = {
            '_id': self._replication_id(peer),
            'source': self.dbname,
            'target': r'http://admin:admin@{0}:{1}/{2}'.format(peer.ip, peer.port, peer.dbname),
            'continuous': True
        }

        self._repl_db.save_doc(doc)

    def _teardown_replication(self, peer):
        self._repl_db.delete_doc(self._replication_id(peer))

    def _clear_replication(self):
        search_view = { "map": 'function(doc) { if (doc._id.indexOf("ers-auto-") == 0) emit(doc._id, doc); }' }

        self._repl_db.delete_docs([doc['value'] for doc in self._repl_db.temp_view(search_view)])

    def _check_already_running(self):
        if os.path.exists(self.pidfile):
            raise RuntimeError("The ERS daemon seems to be already running. If this is not the case, " +
                               "delete " + self.pidfile + " and try again.")


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", help="CouchDB port", type=int, default=5984)
    parser.add_argument("-d", "--dbname", help="CouchDB dbname", type=str, default=ERS_DEFAULT_DBNAME)
    parser.add_argument("-t", "--type", help="Type of instance", type=str, default=ERS_DEFAULT_PEER_TYPE,
                        choices=ERS_PEER_TYPES)
    parser.add_argument("--pidfile", help="PID file for ERS instance", type=str, default='/var/run/ers_daemon.pid')
    parser.add_argument("--tries", help="Number of tries to connect to CouchDB", type=int, default=10)
    args = parser.parse_args()

    print "Starting ERS daemon..."
    daemon = None
    try:
        daemon = ERSDaemon(args.type, args.port, args.dbname, args.pidfile, args.tries)

        daemon.start()
        print "Started ERS daemon"

        def sig_handler(sig, frame):
            mainloop.quit()
        signal.signal(signal.SIGQUIT, sig_handler)
        signal.signal(signal.SIGTERM, sig_handler)

        mainloop = gobject.MainLoop()
        mainloop.run()

        print "Stopping ERS daemon..."
    except (KeyboardInterrupt, SystemExit):
        pass
    except RuntimeError as e:
        sys.stderr.write('Error: '+str(e)+"\n")

    if daemon is not None:
        daemon.stop()


if __name__ == '__main__':
    run()
