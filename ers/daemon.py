import argparse
import atexit
import couchdbkit
import os
import signal
import socket
import sys
import zeroconf
import gobject

from ers import ERS_AVAHI_SERVICE_TYPE, ERS_PEER_TYPES, ERS_PEER_TYPE_CONTRIB, ERS_PEER_TYPE_BRIDGE, DEFAULT_MODEL


class ERSDaemon:
    peer_type = None
    port = None
    dbname = None
    pidfile = None

    _active = False
    _service = None
    _monitor = None
    _db = None
    _model = None

    def __init__(self, peer_type=ERS_PEER_TYPE_CONTRIB, port=5984, dbname='ers', pidfile='/var/run/ers_daemon.pid'):
        self.peer_type = peer_type
        self.port = port
        self.dbname = dbname
        self.pidfile = pidfile

    def start(self):
        self._check_already_running()
        self._init_db_connection()

        service_name = 'ERS on {0} (dbname={1},type={2})'.format(socket.gethostname(), self.dbname, self.peer_type)
        self._service = zeroconf.PublishedService(service_name, ERS_AVAHI_SERVICE_TYPE, self.port)
        self._monitor = zeroconf.ServiceMonitor(ERS_AVAHI_SERVICE_TYPE, self._on_join, self._on_leave)
        self._monitor.start()
        self._service.publish()

        with file(self.pidfile, 'w+') as f:
            f.write("{0}\n".format(os.getpid()))

        self._active = True

        atexit.register(self.stop)

    def _init_db_connection(self):
        try:
            server_url = "http://admin:admin@127.0.0.1:{0}/".format(self.port)
            server = couchdbkit.Server(server_url)
            self._db = server.get_or_create_db(self.dbname)
            self._model = DEFAULT_MODEL
            for doc in self._model.initial_docs():
                if not self._db.doc_exist(doc['_id']):
                    self._db.save_doc(doc)
        except Exception as e:
            raise RuntimeError("Error connecting to CouchDB: {0}".format(str(e)))

    def stop(self):
        if not self._active:
            return

        self._monitor.shutdown()
        self._service.unpublish()

        os.remove(self.pidfile)

        self._active = False

    def _on_join(self, peer):
        # TODO: setup replication etc.
        pass

    def _on_leave(self, peer):
        # TODO: tear down replication etc.
        pass

    def _check_already_running(self):
        if os.path.exists(self.pidfile):
            raise RuntimeError("The ERS daemon seems to be already running. If this is not the case, " +
                               "delete " + self.pidfile + " and try again.")


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--port", help="CouchDB port", type=int, default=5984)
    parser.add_argument("-d", "--dbname", help="CouchDB dbname", type=str, default='ers')
    parser.add_argument("-t", "--type", help="Type of instance", type=str, default=ERS_PEER_TYPE_CONTRIB,
                        choices=ERS_PEER_TYPES)
    parser.add_argument("--pidfile", help="PID file for ERS instance", type=str, default='/var/run/ers_daemon.pid')
    args = parser.parse_args()

    print "Starting ERS daemon..."
    daemon = None
    try:
        daemon = ERSDaemon(args.type, args.port, args.dbname, args.pidfile)

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
