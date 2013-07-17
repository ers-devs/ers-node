"""
This module contains functions for publishing and discovering services over Zeroconf protocols, using either the
Avahi or Bonjour implementations.

The module works by calling the avahi-browse/avahi-publish/dns-sd utilities and interacting with them in the background.
This is brittle and not very elegant, but it does avoid a number of pitfalls such as conflicts with the main loop of
a GTK application when a separate thread is using DBus to talk to Avahi.

The functions here are designed to be used in an asynchronous fashion, i.e. they return immediately and do not block.
Unless otherwise noted, the functions are also thread-safe.

Note: There is currently no Windows support.
"""

__author__ = 'Cristian Dinu <goc9000@gmail.com>'


import atexit
import re
import socket
import subprocess
import time
import threading


def _command_exists(command):
    try:
        subprocess.check_output(['which', command], stderr=subprocess.STDOUT)
        return True
    except subprocess.CalledProcessError:
        return False


def _watch_process_output(proc):
    while proc.poll() is None:
        line = proc.stdout.readline()
        if line is None:
            proc.wait()
            break

        yield line.rstrip()

    proc.wait()


def _listify(value):
    if value is None:
        return list()
    elif not isinstance(value, (list, tuple)):
        return [value]
    else:
        return list(value)


def _run_lookup_program(program_args, response_pattern, max_lookup_time=5.0):
    def in_thread():
        for line in _watch_process_output(proc):
            match = re.search(response_pattern, line)
            if match is not None:
                result.append(match.groups())
                proc.terminate()
                break

    result = []
    proc = subprocess.Popen(program_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)

    thread = threading.Thread(target=in_thread)
    thread.start()
    thread.join(max_lookup_time)

    if thread.is_alive():
        proc.terminate()
        thread.join()

    return None if len(result) == 0 else result[0]


def _is_my_host(hostname):
     return socket.gethostname().partition('.')[0] == hostname.partition('.')[0]


_published_services = list()
_monitors = list()
_lock = threading.RLock()


def avahi_supported():
    """
    Check whether Avahi is supported on this system.
    """
    return _command_exists('avahi-browse')


def bonjour_supported():
    """
    Check whether Bonjour is supported on this system.
    """
    return _command_exists('dns-sd')


def zeroconf_supported():
    """
    Check whether there is any support for Zeroconf (Avahi or Bonjour) on this system.
    """
    return avahi_supported() or bonjour_supported()


@atexit.register
def cleanup():
    """
    Kill all processes and threads spawned by this module to ensure an orderly shutdown. This implies that all services
    will be unpublished and all monitors will cease to function.
    """
    unpublish_all()
    unmonitor_all()


class PublishedService:
    """
    This class represents a handle to a currently published service. Creating an instance of this class results in the
    publication of a service over Zeroconf (Avahi/Bonjour) until either the .unpublish() method is called, or the
    Python program terminates.
    """
    name = None
    service_type = None
    port = None

    _process = None

    def __init__(self, name, service_type, port):
        """
        Constructor.
        """
        if avahi_supported():
            process_args = ['avahi-publish', '-s', name, service_type, str(port)]
            success_pattern = r'Established under name'
            error_pattern = r'(Failure.*)'
        elif bonjour_supported():
            process_args = ['dns-sd', '-R', name, service_type, 'local', str(port)]
            success_pattern = r'Name now registered'
            error_pattern = r'call failed[ ]*(.*)'
        else:
            raise RuntimeError('Cannot publish service: no support for either Avahi or Bonjour on this system')

        self._process = subprocess.Popen(process_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                         universal_newlines=True)

        success = False
        error_text = "Unknown error"
        for line in _watch_process_output(self._process):
            m = re.search(error_pattern, line)
            if m is not None:
                error_text = m.group(1)
                break
            if re.search(success_pattern, line):
                success = True
                break

        if not success:
            self._process.terminate()
            raise RuntimeError("Failure publishing service: {0}".format(error_text))

        self.name = name
        self.service_type = service_type
        self.port = port

        with _lock:
            _published_services.append(self)

    def unpublish(self):
        """
        Unpublish this service.
        """
        with _lock:
            if self not in _published_services:
                return
            _published_services.remove(self)

        self._process.terminate()


class ServiceMonitor:
    """
    This class represents a monitor for services of a given type advertised over the network.

    Whenever a peer that publishes a service of type `service_type` comes into range, a series on `on_join` functions
    specified by the user will be called. They will receive as their only parameter an object describing the peer
    (its IP, port, etc.). The object also acts as a handle for recognizing a peer that has gone offline (in which case
    the `on_leave` functions are similarly called). Finally, the `on_error` functions will be called whenever an error
    occurs in the monitoring process.

    Note that the `on_join`, etc. callbacks are invoked asynchronously from within a separate thread, so they must be
    ready to perform any necessary synchronization. It is however guaranteed that a given monitor will not attempt to
    execute more than one callback at a time.

    This function returns a handle that can subsequently be used to query or remove the monitor.

    Note: When the monitor starts up, a series of join events will be perceived for every peer that is currently
    online, even if the service was published some time in the past.

    Note: If `see_self` is set to True, the monitor will also report services published locally.
    """
    service_type = None
    on_join = None
    on_leave = None
    on_error = None
    see_self = None
    max_lookup_time = None

    _process = None
    _peers = None
    _peers_lock = None
    _thread = None
    _shutdown = None

    def __init__(self, service_type, on_join=None, on_leave=None, on_error=None, see_self=False, max_lookup_time=5.0):
        """
        Constructor.
        """
        on_join = _listify(on_join)
        on_leave = _listify(on_leave)
        on_error = _listify(on_error)

        if avahi_supported():
            process_args = ['avahi-browse', '-p', '-k', '-r', service_type]
            line_handler = self._handle_line_avahi
        elif bonjour_supported():
            process_args = ['dns-sd', '-B', service_type]
            line_handler = self._handle_line_dns_sd
        else:
            raise RuntimeError('Cannot start monitor: no support for either Avahi or Bonjour on this system')

        self.service_type = service_type
        self.on_join = on_join
        self.on_leave = on_leave
        self.on_error = on_error
        self.see_self = see_self
        self.max_lookup_time = max_lookup_time

        self._process = subprocess.Popen(process_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                         universal_newlines=True)
        self._thread = threading.Thread(target=lambda: self._do_in_thread(line_handler))
        self._thread.daemon = True
        self._peers = dict()
        self._peers_lock = threading.Lock()
        self._shutdown = False

        with _lock:
            _monitors.append(self)

        self._thread.start()

    def shutdown(self):
        """
        Shuts down this monitor.
        """
        with _lock:
            if self not in _monitors:
                return
            _monitors.remove(self)

        self._shutdown = True
        self._process.terminate()
        self._thread.join()

    def get_peers(self):
        """
        Gets a dictionary of all currently online peers for this service.
        """
        with _lock:
            return self._peers.values()

    def _do_in_thread(self, line_handler):
        error_text = "Unknown error"

        for line in _watch_process_output(self._process):
            action = line_handler(line)

            if action is None:
                continue

            if action[0] == '+':
                peer_name, service_name, host, ip, port = action[1:]
                if _is_my_host(host) and not self.see_self:
                    continue

                with self._peers_lock:
                    if peer_name in self._peers:
                        peer = self._peers[peer_name]
                        peer.service_name = service_name
                        peer.host = host
                        peer.ip = ip
                        peer.port = port
                    else:
                        peer = ServicePeer(service_name, self.service_type, host, ip, port)
                        self._peers[peer_name] = peer

                for callback in self.on_join:
                    callback(peer)
            elif action[0] == '-':
                peer_name = action[1]
                with self._peers_lock:
                    if peer_name not in self._peers:
                        continue
                    ex_peer = self._peers[peer_name]
                    del self._peers[peer_name]

                for callback in self.on_leave:
                    callback(ex_peer)

        if not self._shutdown:
            for callback in self.on_error:
                callback(error_text)

    def _handle_line_avahi(self, line):
        parts = line.split(';')
        if len(parts) < 6:
            return None

        operation = parts[0]
        service_name = re.sub(r'\\[0-9]{3}', lambda m: chr(int(m.group(0)[1:])), parts[3])

        if operation == '=' and len(parts) >= 9:
            host = parts[6]
            ip = parts[7]
            port = int(parts[8])

            return '+', service_name, service_name, host, ip, port
        elif operation == '-':
            return '-', service_name

    def _handle_line_dns_sd(self, line):
        parts = line.split(None, 6)
        if len(parts) != 7:
            return None

        operation = parts[1]
        service_name = parts[6]

        if operation == 'Add':
            result = _run_lookup_program(['dns-sd', '-L', service_name, self.service_type],
                                         r' can be reached at ([^:]+):([0-9]+)',
                                         self.max_lookup_time)
            if result is None:
                return None

            host, port = result

            result = _run_lookup_program(['dns-sd', '-q', host],
                                         r'^\s*\S+\s+Add\s+\d+\s+\d+\s+\S+\s+1\s+\d+\s+(.*)',
                                         self.max_lookup_time)

            if result is None:
                return None

            ip = result[0]

            return '+', service_name, service_name, host, ip, port
        elif operation == 'Rmv':
            return '-', service_name


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


def get_published_services(name=None, service_type=None, port=None):
    """
    Get information on all currently published services matching the given `handle`, `name` or `port` parameters.

    Returns a list of handles to the matching services.
    """
    with _lock:
        result = []

        for service in _published_services:
            if name is not None and service.name != name: continue
            if service_type is not None and service.service_type != service_type: continue
            if port is not None and service.port != port: continue

            result.append(service)

    return result


def unpublish_all():
    """
    Unpublish all services.
    """
    for service in get_published_services():
        service.unpublish()


def unmonitor_all():
    """
    Shuts down all monitors.
    """
    with _lock:
        monitors = list(_monitors)

    for monitor in monitors:
        monitor.shutdown()


def test():
    """
    Runs a test of this module.

    Note: This test requires user interaction.
    """
    def handle_join(peer):
        print "Monitor: JOIN  {0}".format(peer)

    def handle_leave(peer):
        print "Monitor: LEAVE {0}".format(peer)

    def handle_error(error):
        print "Monitor: ERROR {0}".format(error)

    print "Publishing service for 5 seconds..."
    service = PublishedService('Test Zeroconf service', '_etc._tcp', 9999)

    print "Starting monitor..."
    mon = ServiceMonitor('_etc._tcp', on_join=handle_join, on_leave=handle_leave, on_error=handle_error, see_self=True)

    time.sleep(5)
    print "Unpublishing service..."
    service.unpublish()

    time.sleep(3)

    print "OK"


if __name__ == '__main__':
    test()
