"""
This module contains functions for publishing and discovering services over Zeroconf protocols. Currently only Avahi
is supported.
"""

__author__ = 'Cristian Dinu <goc9000@gmail.com>'


import avahi
import dbus
import gobject

from dbus.mainloop.glib import DBusGMainLoop


def _listify(value):
    if value is None:
        return list()
    elif not isinstance(value, (list, tuple)):
        return [value]
    else:
        return list(value)


class PublishedService:
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
        loop = DBusGMainLoop(set_as_default=True)
        bus = dbus.SystemBus(mainloop=loop)
        server = dbus.Interface(bus.get_object(avahi.DBUS_NAME, avahi.DBUS_PATH_SERVER), avahi.DBUS_INTERFACE_SERVER)
        g = dbus.Interface(bus.get_object(avahi.DBUS_NAME, server.EntryGroupNew()), avahi.DBUS_INTERFACE_ENTRY_GROUP)
        g.AddService(avahi.IF_UNSPEC, avahi.PROTO_UNSPEC, dbus.UInt32(0), self.name, self.service_type, self.domain,
                     self.host, dbus.UInt16(self.port), self.text)
        g.Commit()
        self._group = g

    def unpublish(self):
        """
        Unpublishes the service.
        """
        if self._group is not None:
            self._group.Reset()
            self._group = None


class ServiceMonitor:
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
    service_type = None
    on_join = None
    on_leave = None
    on_error = None
    see_self = None

    _active = None
    _inited = None
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

        browser.connect_to_signal("ItemNew", self._on_item_new)
        browser.connect_to_signal("ItemRemove", self._on_item_remove)
        self._inited = True

    def _on_item_new(self, interface, protocol, name, service_type, domain, flags):
        if (flags & avahi.LOOKUP_RESULT_LOCAL) and not self.see_self:
            return

        self._server.ResolveService(interface, protocol, name, service_type, domain, avahi.PROTO_UNSPEC, dbus.UInt32(0),
                                    reply_handler=self._on_resolved, error_handler=self._on_resolve_error)

    def _on_item_remove(self, interface, protocol, name, service_type, domain, flags):
        peer_name = unicode(name)

        if peer_name not in self._peers:
            return

        ex_peer = self._peers[peer_name]
        del self._peers[peer_name]

        if self._active:
            for callback in self.on_leave:
                callback(ex_peer)

    def _on_resolved(self, interface, protocol, name, service, domain, host, aproto, address, port, txt, flags):
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

    def _on_resolve_error(self, *args):
        message = 'Error resolving service: ' + ' '.join(str(arg) for arg in args)

        if self._active:
            for callback in self.on_error:
                callback(message)

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


class ServicePeer:
    """
    This class represents a peer over a given service published via Zeroconf on the network.
    """
    service_name = None
    service_type = None
    host = None
    ip = None
    port = None

    def __init__(self, service_name, service_type, host, ip, port):
        self.service_name = service_name
        self.service_type = service_type
        self.host = host
        self.ip = ip
        self.port = port

    def __str__(self):
        return "'{0}' of type {1} on {2}(={3}):{4}".format(self.service_name, self.service_type, self.host,
                                                           self.ip, self.port)


def test():
    """
    Runs a test of this module.

    Note: This test requires user interaction.
    """
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
