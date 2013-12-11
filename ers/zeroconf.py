"""
ers.zeroconf

This module contains functions for publishing and discovering services over Zeroconf protocols. Currently only Avahi
is supported.
"""

__author__ = 'Cristian Dinu <goc9000@gmail.com>'

# Need this import because constants for ERSPeerInfo are still in daemon
import daemon

import re

try:
    import avahi
    import dbus

    from dbus.mainloop.glib import DBusGMainLoop

    AVAHI_SUPPORTED = True
except ImportError as e:
    AVAHI_SUPPORTED = False

try:
    import pybonjour
    import select
    import socket
    import threading

    PYBONJOUR_SUPPORTED = True
except ImportError as e:
    PYBONJOUR_SUPPORTED = False


def _listify(value):
    if value is None:
        return list()
    elif not isinstance(value, (list, tuple)):
        return [value]
    else:
        return list(value)


class PublishedService(object):
    """
    This class represents a service published over Zeroconf.
    """
    name = None
    port = None
    service_type = None
    domain = None
    host = None
    text = None

    _group = None

    def __init__(self, name, service_type, port, domain="", host="", text=""):
        """
        Constructor.
        """
        self.name = name
        self.service_type = service_type
        self.domain = domain
        self.host = host
        self.port = port
        self.text = text

        self._group = None

    def publish(self):
        """
        Publishes the service defined by this handle.
        """

        if AVAHI_SUPPORTED:
            loop = DBusGMainLoop(set_as_default=True)
            bus = dbus.SystemBus(mainloop=loop)
            server = dbus.Interface(bus.get_object(avahi.DBUS_NAME, avahi.DBUS_PATH_SERVER), avahi.DBUS_INTERFACE_SERVER)
            g = dbus.Interface(bus.get_object(avahi.DBUS_NAME, server.EntryGroupNew()), avahi.DBUS_INTERFACE_ENTRY_GROUP)
            g.AddService(avahi.IF_UNSPEC, avahi.PROTO_UNSPEC, dbus.UInt32(0), self.name, self.service_type, self.domain,
                         self.host, dbus.UInt16(self.port), self.text)
            g.Commit()
            self._group = g
        elif PYBONJOUR_SUPPORTED:
            def _pybonjour_service_thread():
                def _register_callback(sd_ref, flags, error_code, name, regtype, domain):
                    if error_code != pybonjour.kDNSServiceErr_NoError:
                        print "Error while registering service {0}".format(name)
                    
                    print "Registered service:\n  name    = {0}\n  regtype = {1}\n  domain  = {2}".format(name, regtype, domain)

                sd_ref = pybonjour.DNSServiceRegister(name = self.name, regtype = self.service_type,
                                                      port = self.port, callBack = _register_callback)

                try:
                    while True:
                        ready = select.select([sd_ref], [], [])
                        if sd_ref in ready[0]:
                            pybonjour.DNSServiceProcessResult(sd_ref)
                finally:
                    sd_ref.close()

            _thread = threading.Thread(target=_pybonjour_service_thread)
            _thread.daemon = True
            _thread.start()
        else:
            raise RuntimeError("Avahi nor Bonjour support installed! (Python packages 'avahi' and 'dbus', or 'pybonjour')")

    def unpublish(self):
        """
        Unpublishes the service.
        """
        if self._group is not None:
            self._group.Reset()
            self._group = None


class ServiceMonitor(object):
    """
    This class represents a monitor for services of a given type advertised over the network.

    Whenever a peer that publishes a service of type `service_type` comes into range, a series on `on_join` functions
    specified by the user will be called. They will receive as their only parameter an object describing the peer
    (its IP, port, etc.). The object also acts as a handle for recognizing a peer that has gone offline (in which case
    the `on_leave` functions are similarly called). Finally, the `on_error` functions will be called whenever an error
    occurs in the monitoring process.

    Notes:

    - In order for the monitor to work, you must have a GTK mainloop running in your application. This library does
      not automatically create one.
    - When the monitor starts up, a series of join events will be perceived for every peer that is currently online,
      even if the service was published some time in the past.
    - Note: If `see_self` is set to True, the monitor will also report services published locally.
    """
    _server = None
    _peers = None

    def __init__(self, service_type, on_join=None, on_leave=None, on_error=None, see_self=False):
        """
        Constructor.
        """
        on_join = _listify(on_join)
        on_leave = _listify(on_leave)
        on_error = _listify(on_error)

        self.service_type = service_type
        self.on_join = on_join
        self.on_leave = on_leave
        self.on_error = on_error
        self.see_self = see_self

        self._active = False
        self._inited = False
        self._peers = dict()

    def start(self):
        """
        Starts the monitor.
        """

        if self._inited:
            self._active = True
            return

        if AVAHI_SUPPORTED:
            loop = DBusGMainLoop(set_as_default=True)
            bus = dbus.SystemBus(mainloop=loop)
            server = dbus.Interface(bus.get_object(avahi.DBUS_NAME, avahi.DBUS_PATH_SERVER), avahi.DBUS_INTERFACE_SERVER)
            self._server = server
            self._active = True
            browser = dbus.Interface(bus.get_object(avahi.DBUS_NAME,
                                                    server.ServiceBrowserNew(avahi.IF_UNSPEC,
                                                                             avahi.PROTO_UNSPEC,
                                                                             self.service_type,
                                                                             'local',
                                                                             dbus.UInt32(0))),
                                     avahi.DBUS_INTERFACE_SERVICE_BROWSER)

            browser.connect_to_signal("ItemNew", _on_item_new)
            browser.connect_to_signal("ItemRemove", _on_item_remove)
            self._inited = True
            
            def _on_item_new(interface, protocol, name, service_type, domain, flags):
                if (flags & avahi.LOOKUP_RESULT_LOCAL) and not self.see_self:
                    return

                self._server.ResolveService(interface, protocol, name, service_type, domain, avahi.PROTO_UNSPEC, dbus.UInt32(0),
                                            reply_handler=_on_resolved, error_handler=_on_resolve_error)

            def _on_item_remove(interface, protocol, name, service_type, domain, flags):
                self._on_peer_leave(name)

            def _on_resolved(interface, protocol, name, service, domain, host, aproto, address, port, txt, flags):
                self._on_peer_join(name, host, address, port)

            def _on_resolve_error(*args):
                message = 'Error resolving service: ' + ' '.join(str(arg) for arg in args)

                if self._active:
                    for callback in self.on_error:
                        callback(message)
                        
        elif PYBONJOUR_SUPPORTED:
            def _pybonjour_monitor_thread():
                _queried  = []
                _resolved = []

                def _try_query(sd_ref, result_buffer):                
                    result = None

                    while result_buffer:
                        result_buffer.pop()
                
                    try:
                        while not result_buffer:
                            ready = select.select([sd_ref], [], [], 5)
                            if sd_ref not in ready[0]:
                                print 'Query timed out'
                                break
                            pybonjour.DNSServiceProcessResult(sd_ref)
                        else:
                            result = result_buffer[0]
                            result_buffer.pop()
                    finally:
                        sd_ref.close()
                
                    return result

                def _on_queried(sd_ref, flags, interface_index, error_code, fullname, rrtype, rrclass, rdata, ttl):
                    if error_code != pybonjour.kDNSServiceErr_NoError:
                        print "Error while getting IP for service {0}".format(fullname)
                        _queried.append(None)
                        return
                    
                    print "  IP         = {0}".format(socket.inet_ntoa(rdata))
                
                    _queried.append(socket.inet_ntoa(rdata))

                def _on_resolved(sd_ref, flags, interface_index, error_code, fullname, hosttarget, port, txt_record):
                    if error_code != pybonjour.kDNSServiceErr_NoError:
                        print "Error while resolving service {0}".format(fullname)
                        _resolved.append(None)
                        return

                    print "Resolved service:\n  fullname   = {0}\n  hosttarget = {1}\n  port       = {2}".format(fullname, hosttarget, port)

                    query_sd_ref = pybonjour.DNSServiceQueryRecord(interfaceIndex = interface_index, fullname = hosttarget,
                                                                  rrtype = pybonjour.kDNSServiceType_A, callBack = _on_queried)

                    address = _try_query(query_sd_ref, _queried)

                    if address is None:
                        print "Error while resolving service {0}".format(full_name)

                    _resolved.append((hosttarget, address, port))

                def _on_browsed(sd_ref, flags, interface_index, error_code, service_name, regtype, reply_domain):
                    if error_code != pybonjour.kDNSServiceErr_NoError:
                        return

                    if not (flags & pybonjour.kDNSServiceFlagsAdd):
                        self._on_peer_leave(service_name)
                        print 'Service removed'
                        return

                    print 'Service added; resolving'

                    resolve_sd_ref = pybonjour.DNSServiceResolve(0, interface_index, service_name, regtype, reply_domain, _on_resolved)

                    service = _try_query(resolve_sd_ref, _resolved)

                    if service is None:
                        print "Error resolving service {0}".format(service_name)
                        return
                
                    host, address, port = service
                    if self.see_self or not (socket.gethostname().partition('.')[0] == host.partition('.')[0]):
                        self._on_peer_join(service_name, host, address, port)

                browse_sd_ref = pybonjour.DNSServiceBrowse(regtype = self.service_type, callBack = _on_browsed)

                try:
                    while True:
                        ready = select.select([browse_sd_ref], [], [])
                        if browse_sd_ref in ready[0]:
                            pybonjour.DNSServiceProcessResult(browse_sd_ref)
                finally:
                    browse_sd_ref.close()
                
                self._inited = True

            _thread = threading.Thread(target=_pybonjour_monitor_thread)
            _thread.daemon = True
            _thread.start()
                
        else:
            raise RuntimeError("Avahi nor Bonjour support installed! (Python packages 'avahi' and 'dbus', or 'pybonjour')")

    def _on_peer_join(self, name, host, address, port):
        peer_name = unicode(name)

        if peer_name in self._peers:
            peer = self._peers[peer_name]
            peer.service_name = unicode(name)
            peer.host = unicode(host)
            peer.ip = str(address)
            peer.port = int(port)
        else:
            peer = ServicePeer(unicode(name), self.service_type, unicode(host), str(address), int(port))
            self._peers[peer_name] = peer

        if self._active:
            for callback in self.on_join:
                callback(peer)
        
    def _on_peer_leave(self, name):
        peer_name = unicode(name)

        if peer_name not in self._peers:
            return

        ex_peer = self._peers[peer_name]
        del self._peers[peer_name]

        if self._active:
            for callback in self.on_leave:
                callback(ex_peer)

    def shutdown(self):
        """
        Shuts down this monitor.
        """
        self._active = False

    def get_peers(self):
        """
        Gets a dictionary of all currently online peers for this service.
        """
        return self._peers.values()


class PeerInfo(object):
    """docstring for Peer"""
    def __init__(self, arg):
        super(Peer, self).__init__()
        self.arg = arg

    def to_string(self):
        pass
        
    def from_string(self):
        pass
        
    def to_dict(self):
        pass
        


class ServicePeer(object):
    """
    This class represents a peer over a given service published via Zeroconf on the network.
    """
    def __init__(self, service_name, service_type, host, ip, port):
        self.service_name = service_name
        self.service_type = service_type
        self.host = host
        self.ip = ip
        self.port = port

    def __str__(self):
        return "'{0}' of type {1} on {2}(={3}):{4}".format(self.service_name, self.service_type, self.host,
                                                           self.ip, self.port)

class ERSPeerInfo(ServicePeer):
    """ 
        This class contains information on an ERS peer.
    """
    def __init__(self, service_name, host, ip, port, dbname=daemon.ERS_DEFAULT_DBNAME, peer_type=daemon.ERS_DEFAULT_PEER_TYPE):
        super(ERSPeerInfo, self).__init__(service_name, daemon.ERS_AVAHI_SERVICE_TYPE, host, ip, port)
        self.dbname = dbname
        self.peer_type = peer_type

    def __str__(self):
        return "ERS peer on {0.host}(={0.ip}):{0.port} (dbname={0.dbname}, type={0.peer_type})".format(self)

    def to_json(self):
        """ Returns the ERS peer information from this instance in JSON format.
        
            :rtype: dict.
        """
        return {
            'name': self.service_name,
            'host': self.host,
            'ip': self.ip,
            'port': self.port,
            'dbname': self.dbname,
            'type': self.peer_type
        }

    @staticmethod
    def from_service_peer(svc_peer):
        """ 
            Get an ERSPeerInfo instance from a given service peer.
        
            :param svc_peer: a service peer
            :type svc_peer: ServicePeer instance
            :rtype: ERSPeerInfo instance
        """
        dbname = daemon.ERS_DEFAULT_DBNAME
        peer_type = daemon.ERS_DEFAULT_PEER_TYPE

        match = re.match(r'ERS on .* [(](.*)[)]$', svc_peer.service_name)
        if match is None:
            return None

        for item in match.group(1).split(','):
            param, sep, value = item.partition('=')

            if param == 'dbname':
                dbname = value
            if param == 'type':
                peer_type = value

        return ERSPeerInfo(svc_peer.service_name, svc_peer.host, svc_peer.ip, svc_peer.port, dbname, peer_type)



def test():
    """
    Runs a test of this module.

    Note: This test requires user interaction.
    """
    import gobject

    mainloop = gobject.MainLoop()

    def run_mainloop_for(seconds):
        def on_timer():
            mainloop.quit()
            return False

        gobject.timeout_add(int(seconds * 1000), on_timer)
        mainloop.run()

    def handle_join(peer):
        print "Monitor: JOIN  {0}".format(peer)

    def handle_leave(peer):
        print "Monitor: LEAVE {0}".format(peer)

    def handle_error(error):
        print "Monitor: ERROR {0}".format(error)

    print "Publishing service for 5 seconds..."
    service = PublishedService('Test Zeroconf service', '_etc._tcp', 9999)
    service.publish()

    print "Starting monitor..."
    mon = ServiceMonitor('_etc._tcp', on_join=handle_join, on_leave=handle_leave, on_error=handle_error, see_self=True)
    mon.start()

    run_mainloop_for(5)

    print "Unpublishing service..."
    service.unpublish()

    run_mainloop_for(3)

    print "OK"


if __name__ == '__main__':
    test()
