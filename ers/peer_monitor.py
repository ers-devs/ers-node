import re
import sys
import threading
import socket


AVAHI_LOOKUP_TIMEOUT = 5.0
ERS_AVAHI_SERVICE_TYPE = '_ers._tcp'


_peers = {}
_lock = threading.Lock()


def get_peers():
    result = []
    with _lock:
        for peer_info in _peers.values():
            result.append(dict(peer_info))

    return result


def _report_error(error):
    sys.stderr.write("Peer discovery error: {0}\n".format(error))


def _on_peer_join(peer_name, host, port, dbname):
    with _lock:
        _peers[peer_name] = { 'name': peer_name, 'host': host, 'port': port, 'dbname': dbname }


def _on_peer_leave(peer_name):
    with _lock:
        del _peers[peer_name]


def _extract_dbname(service_name):
    match = re.search(r'[(]dbname=([^)]*)[)]', service_name, re.I)
    if match is None:
        return 'ers'
    else:
        return match.group(1)


def is_my_host(hostname):
    return socket.gethostname().partition('.')[0] == hostname.partition('.')[0]




try:
    import pybonjour

    pybonjour_supported = True
except ImportError:
    pybonjour_supported = False


try:
    import dbus
    from dbus.mainloop.glib import DBusGMainLoop
    import avahi
    import gobject
    import threading

    avahi_supported = True
except ImportError:
    avahi_supported = False


if avahi_supported:
    gobject.threads_init()
    dbus.mainloop.glib.threads_init()

    _avahi_lock = threading.Lock()
    _loop = DBusGMainLoop(set_as_default=True)
    _bus = dbus.SystemBus(mainloop=_loop)
    _server = dbus.Interface(_bus.get_object(avahi.DBUS_NAME, avahi.DBUS_PATH_SERVER),
                             avahi.DBUS_INTERFACE_SERVER)
    _serv_browser = _server.ServiceBrowserNew(avahi.IF_UNSPEC, avahi.PROTO_UNSPEC,
                                              ERS_AVAHI_SERVICE_TYPE, 'local', dbus.UInt32(0))
    _browser = dbus.Interface(_bus.get_object(avahi.DBUS_NAME, _serv_browser),
                              avahi.DBUS_INTERFACE_SERVICE_BROWSER)


    def on_resolved(interface, protocol, name, service, domain, host, proto, address, port, txt, flags):
        if not is_my_host(host):
            _on_peer_join(unicode(name), unicode(host), int(port), _extract_dbname(unicode(name)))


    def on_resolve_error(*args, **kwargs):
        _report_error("Error resolving service: {0}".format(args))


    def on_item_new(interface, protocol, name, stype, domain, flags):
        with _avahi_lock:
            _server.ResolveService(interface, protocol, name, stype, domain, avahi.PROTO_UNSPEC, dbus.UInt32(0),
                                   reply_handler=on_resolved, error_handler=on_resolve_error)


    def on_item_remove(interface, protocol, name, service, domain, flags):
        _on_peer_leave(unicode(name))


    def on_failure(exception):
        _report_error(exception)


    _browser.connect_to_signal("ItemNew", on_item_new)
    _browser.connect_to_signal("ItemRemove", on_item_remove)
    _browser.connect_to_signal("Failure", on_failure)

    _thread = threading.Thread(target=gobject.MainLoop().run)
    _thread.daemon = True
    _thread.start()
elif pybonjour_supported:
    def _pybonjour_monitor_main():
        import select

        _resolved = []

        def on_resolve(sd_ref, flags, if_index, error_code, full_name, host, port, txt_record):
            if error_code != pybonjour.kDNSServiceErr_NoError:
                _report_error("Error resolving service {0}".format(full_name))
                return

            _resolved.append((host, port))

        def on_browse(sd_ref, flags, if_index, error_code, service_name, regtype, reply_domain):
            if error_code != pybonjour.kDNSServiceErr_NoError:
                return

            if not (flags & pybonjour.kDNSServiceFlagsAdd):
                _on_peer_leave(service_name)
                return

            resolve_sd_ref = pybonjour.DNSServiceResolve(0, if_index, service_name, regtype, reply_domain, on_resolve)

            try:
                while not _resolved:
                    ready = select.select([resolve_sd_ref], [], [], AVAHI_LOOKUP_TIMEOUT)
                    if resolve_sd_ref not in ready[0]:
                        _report_error("Resolve timeout for {0}".format(service_name))
                        break
                    pybonjour.DNSServiceProcessResult(resolve_sd_ref)

                if _resolved:
                    host, port = _resolved[0]
                    _resolved.pop()

                    if not is_my_host(host):
                        _on_peer_join(service_name, host, port, _extract_dbname(service_name))
            finally:
                resolve_sd_ref.close()

        browse_sd_ref = pybonjour.DNSServiceBrowse(regtype=ERS_AVAHI_SERVICE_TYPE, callBack=on_browse)

        try:
            while True:
                ready = select.select([browse_sd_ref], [], [])
                if browse_sd_ref in ready[0]:
                    pybonjour.DNSServiceProcessResult(browse_sd_ref)
        finally:
            browse_sd_ref.close()


    _thread = threading.Thread(target=_pybonjour_monitor_main)
    _thread.daemon = True
    _thread.start()
else:
    _report_error("Neither avahi nor bonjour supported in Python, peer discovery will not work")


def test():
    import time
    print "This test runs continuously, use Ctrl+C to exit"
    prev_peers = None
    while True:
        time.sleep(0.5)
        peers = get_peers()
        peers.sort(key=lambda p: p['name'])

        if peers != prev_peers:
            print "Peers now:", peers
            prev_peers = peers


if __name__ == '__main__':
    test()
