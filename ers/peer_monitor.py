import re
import time
import sys
import subprocess
import threading
import socket


AVAHI_LOOKUP_TIMEOUT = 5.0
ERS_AVAHI_SERVICE_TYPE = '_ers._tcp'


_peers = None
_lock = None
_thread = None


def get_peers():
    result = []
    with _lock:
        for peer_info in _peers.values():
            result.append(dict(peer_info))

    return result


def command_exists(command):
    try:
        subprocess.check_output(['which', command], stderr=subprocess.STDOUT)
        return True
    except subprocess.CalledProcessError:
        return False


def is_my_host(hostname):
    return socket.gethostname().partition('.')[0] == hostname.partition('.')[0]


def _main_thread():
    if command_exists('avahi-browse'):
        process_args = ['avahi-browse', '-p', '-k', '-r', ERS_AVAHI_SERVICE_TYPE]
        handle_line = _handle_line_avahi
    elif command_exists('dns-sd'):
        process_args = ['dns-sd', '-B', ERS_AVAHI_SERVICE_TYPE]
        handle_line = _handle_line_dns_sd
    else:
        _report_error('Neither avahi nor dns-sd are installed')
        return

    proc = subprocess.Popen(process_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            universal_newlines=True)

    while proc.poll() is None:
        line = proc.stdout.readline()
        if line is None:
            proc.wait()
            break

        handle_line(line.strip())

    proc.wait()

    _report_error("{0} failed with code {1}\n".format(process_args[0], proc.returncode))


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


def _handle_line_avahi(line):
    parts = line.split(';')
    if len(parts) < 6:
        return

    operation = parts[0]
    service_name = re.sub(r'\\[0-9]{3}', lambda m: chr(int(m.group(0)[1:])), parts[3])

    if operation == '=' and len(parts) >= 9:
        host = parts[6]
        port = int(parts[8])
        dbname = _extract_dbname(service_name)
        if not is_my_host(host):
            _on_peer_join(service_name, host, port, dbname)
    elif operation == '-':
        _on_peer_leave(service_name)


def _handle_line_dns_sd(line):
    parts = line.split(None, 6)
    if len(parts) != 7:
        return

    operation = parts[1]
    service_name = parts[6]

    if operation == 'Add':
        host, port, dbname = _lookup_dns_sd(service_name)
        if host is not None and not is_my_host(host):
            _on_peer_join(service_name, host, port, dbname)
    elif operation == 'Rmv':
        _on_peer_leave(service_name)


def _lookup_dns_sd(service_name):
    result = []
    proc = subprocess.Popen(['dns-sd', '-L', service_name, ERS_AVAHI_SERVICE_TYPE], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, universal_newlines=True)

    def target():
        while proc.poll() is None:
            line = proc.stdout.readline()
            if line is None:
                proc.wait()
                break

            match = re.search(r' can be reached at ([^:]+):([0-9]+)', line, re.I)
            if match is not None:
                result.append((match.group(1).rstrip('.'), match.group(2)))
                break

    thread = threading.Thread(target=target)
    thread.start()
    thread.join(AVAHI_LOOKUP_TIMEOUT)

    if thread.is_alive():
        proc.terminate()
        thread.join()

    if len(result) == 0:
        return None, None, None
    else:
        dbname = _extract_dbname(service_name)

        return result[0][0], result[0][1], dbname


def _report_error(error):
    sys.stderr.write("Peer discovery error: {0}".format(error))


def test():
    print "This test runs continuously, use Ctrl+C to exit"
    prev_peers = None

    while True:
        time.sleep(0.5)
        peers = get_peers()
        peers.sort(key=lambda p: p['name'])

        if peers != prev_peers:
            print "Peers now:", peers
            prev_peers = peers


_peers = {}
_lock = threading.Lock()

_thread = threading.Thread(target=_main_thread)
_thread.daemon = True
_thread.start()


if __name__ == '__main__':
    test()
