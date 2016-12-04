from ast import literal_eval
from collections import defaultdict
from entropy import *
import logging as LOG
import os
from random import uniform
from re import match
from socket import AF_INET, socket, SOCK_STREAM
import sys
from threading import Thread, Lock
from time import sleep
from serialization import *

address = 'localhost'
am_primary = False
first_time = True
timer = -1
vv = {}

class WorkerThread(Thread):
    def __init__(self, address, index, port, connections, global_lock, committed_log, tentative_log):
        Thread.__init__(self)
        self.address = address
        self.index = index
        self.port = port
        self.connections = connections
        self.global_lock = global_lock
        self.committed_log = committed_log
        self.tentative_log = tentative_log

        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.bind((address, 20000+index))
        self.sock.listen(1)
        LOG.debug('server.WorkerThread()')
    def run(self):
        while True:
            conn, addr = self.sock.accept()
            handler = ClientServerHandler(conn, self.index, self.connections, self.global_lock, self.committed_log, self.tentative_log)
            handler.start()


# Listens for messages from clients and servers.
class ClientServerHandler(Thread):
    def __init__(self, conn, index, connections, global_lock, committed_log, tentative_log):
        Thread.__init__(self)
        self.buffer = ''
        self.conn = conn
        self.connections = connections
        self.index = index
        self.global_lock = global_lock
        self.log = tentative_log
        self.log_com = committed_log
        self.valid = True

    # Not thread-safe!
    def accept_stamp(self):
        global timer
        timer += 1
        return (self.index, timer)

    def run(self):
        global am_primary, first_time, vv
        while self.valid:
            if '\n' in self.buffer:
                (line, rest) = self.buffer.split('\n', 1)
                self.buffer = rest
                line = ServerDeserialize(line)
                if line.sender_type == CLIENT:
                    LOG.debug('%d: server.ClientServerHandler received %s' % (self.index, line.__dict__))
                    client_knows_too_much = False
                    LOG.debug('%d: server.ClientServerHandler FOOBAR my.vv = %s, cl.vv = %s' % (self.index, vv, line.vv))
                    for server in line.vv:
                        if server not in vv or vv[server] < line.vv[server]:
                            client_knows_too_much = True
                    if client_knows_too_much:
                        sendClient(line.client_id, server_client_response(line.action_type, line.song_name, ERR_DEP, line.vv))
                    # Client doesn't know too much, so we can perform the requested action.
                    elif line.action_type == ADD:
                        op_value = '%s,%s' % (line.song_name, line.url)
                        self.log_entry('PUT', op_value)
                        sendClient(line.client_id, server_client_response(line.action_type, line.song_name, line.url, vv))
                    elif line.action_type == DELETE:
                        self.log_entry('DELETE', line.song_name)
                        sendClient(line.client_id, server_client_response(line.action_type, line.song_name, '', vv))
                    elif line.action_type == GET:
                        url = 'ERR_KEY'
                        state = self.state()
                        LOG.debug('%d: server.ClientServerHandler got GET state= %s' % (self.index, state))
                        if line.song_name in state:
                            url = state[line.song_name]
                        sendClient(line.client_id, server_client_response(line.action_type, line.song_name, url, vv))
                        LOG.debug('%d: server.ClientServerHandler got GET after send' % self.index)
                elif line.sender_type == SERVER:
                    if line.action_type == ANTI_ENTROPY:
                        with self.global_lock:
                            if am_primary:
                                primary_anti_entropy(self.log_com, self.log, line.logs['committed'], line.logs['tentative'], line.logs['vv'], vv)
                            else:
                                anti_entropy(self.log_com, self.log, line.logs['committed'], line.logs['tentative'], line.logs['vv'], vv)
                    elif line.action_type == CONNECT:
                        self.connections.add(line.sender_index)
                        LOG.debug('%d: server.ClientServerHandler first_time? line.logs = %s' % (self.index, line.logs))
                        if line.logs['first_time']:
                            # TODO: use recursive names if possible.
                            self.log_entry('CREATE', line.sender_index)
                        if first_time:
                            sendServer(line.sender_index, server_connect(self.index, self.index, first_time))
                            first_time = False
                    elif line.action_type == DISCONNECT:
                        self.connections.discard(line.sender_index)
                    elif line.action_type == UR_ELECTED:
                        with self.global_lock:
                            am_primary = True
                            # Commit everything you know.
                            for entry in self.log:
                                self.log.remove(entry)
                                self.log_com.append(entry)
                        LOG.debug('%d: server.ClientServerHandler just elected am_primary = %s, com.log = %s, ten.log = %s' % (self.index, am_primary, self.log_com, self.log))
            else:
                try:
                    data = self.conn.recv(1024)
                    self.buffer += data
                except:
                    self.valid = False
                    self.conn.close()
                    break

    # Not thread safe!
    def log_entry(self, op_type, op_value):
        global am_primary, vv
        LOG.debug('   server.ClientServerHandler.log_entry(%s,%s)' % (op_type, op_value))
        if am_primary:
            log = self.log_com
        else:
            log = self.log
        if self.index in vv:
            vv[self.index] += 1
        else:
            vv[self.index] = 0
        log.append({
            'OP_TYPE': op_type,
            'OP_VALUE': op_value,
            'ACCT_STAMP': self.accept_stamp()
        })

    # Not thread safe!
    def state(self):
        result = {}
        update(result, self.log_com)
        update(result, self.log)
        return result

def print_logs(committed_log, tentative_log):
    # correct stuff
    for entry in committed_log:
        if entry in tentative_log:
            tentative_log.remove(entry)

    result = 'log '
    for entry in committed_log:
        result += '%s:(%s):%s' % (entry['OP_TYPE'], entry['OP_VALUE'], 'TRUE')
        result += ','
    for entry in tentative_log:
        result += '%s:(%s):%s' % (entry['OP_TYPE'], entry['OP_VALUE'], 'FALSE')
        result += ','
    if result != 'log ':
        result = result[:-1]
    return result

def update(st, log):
    for log_entry in log:
        if log_entry['OP_TYPE'] == 'PUT':
            song_name, url = log_entry['OP_VALUE'].split(',',1)
            st[song_name] = url
        elif log_entry['OP_TYPE'] == 'DELETE':
            if log_entry['OP_VALUE'] in st:
                del st[log_entry['OP_VALUE']]

# listens for messages from the master
class MasterHandler(Thread):
    def __init__(self, index, address, port, connections, global_lock, log_com, log):
        Thread.__init__(self)
        self.buffer = ''
        self.connections = connections
        self.index = index
        self.global_lock = global_lock
        self.log_com = log_com
        self.log = log
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.bind((address, port))
        self.sock.listen(1)
        self.conn, self.addr = self.sock.accept()
        self.valid = True

        LOG.debug('%d: server.MasterHandler()' % self.index)

    def run(self):
        global am_primary, first_time, timer, vv
        while self.valid:
            if '\n' in self.buffer:
                (line, rest) = self.buffer.split('\n', 1)
                self.buffer = rest
                LOG.debug('%d: server.MasterHandler received \'%s\'' % (self.index, line))
                line = line.split()
                if 'createConn' == line[0]:
                    s_ids = map(int, line[1:])
                    with self.global_lock:
                        self.connections |= set(s_ids)
                    LOG.debug('%d: server.MasterHandler: connections = %s' % (self.index, self.connections))
                    for s_id in s_ids:
                        sendServer(s_id, server_connect(self.index, self.index, first_time))
                        first_time = False
                elif 'breakConn' == line[0]:
                    s_ids = map(int, line[1:])
                    with self.global_lock:
                        self.connections -= set(s_ids)
                    for s_id in s_ids:
                        sendServer(s_id, server_disconnect(self.index, self.index))
                elif 'retire' == line[0]:
                    with self.global_lock:
                        last_connection = self.connections.pop()
                        for s_id in self.connections:
                            sendServer(s_id, server_disconnect(self.index, self.index))
                        timer += 1
                        if am_primary:
                            log = self.log_com
                        else:
                            log = self.log
                        log.append({'OP_TYPE': 'RETIRE', 'OP_VALUE': self.index, 'ACCT_STAMP': (self.index, timer)})
                        if self.index in vv:
                            vv[self.index] += 1
                        else:
                            vv[self.index] = 0
                        sendServer(last_connection, server_logs(self.index, self.index, self.log_com, self.log, vv))
                        if am_primary:
                            sendServer(last_connection, server_elect(self.index, self.index))
                            am_primary = False
                        sendServer(last_connection, server_disconnect(self.index, self.index))
                        LOG.debug('%d: server.MasterHandler: after retire am_primary = %s' % (self.index, am_primary))
                        os._exit()
                elif 'printLog' == line[0]:
                    with self.global_lock:
                        msg = print_logs(self.log_com, self.log)
                    self.send(msg)
            else:
                try:
                    data = self.conn.recv(1024)
                    self.buffer += data
                except:
                    self.valid = False
                    self.conn.close()
                    break

    def send(self, message):
        self.conn.send(str(message) + '\n')


# Send a message to a client.
def sendClient(c_id, message):
    global address
    try:
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((address, 21000 + c_id))
        sock.send(str(message) + '\n')
        sock.close()
    except Exception as msg:
        LOG.debug('   server.sendClient ERROR: ' + str(msg))

# Send a message to a server.
def sendServer(s_id, message):
    global address
    try:
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((address, 20000 + s_id))
        sock.send(str(message) + '\n')
        sock.close()
    except Exception as msg:
        LOG.debug('   server.sendServer ERROR: ' + str(msg))


# Returns the version vector corresponding to a log.
def version_vector(log):
    result = {}
    for log_entry in log:
        index, tmr = log_entry['ACCT_STAMP']
        result[index] = tmr
    return result

def main():
    global address, am_primary, first_time, vv
    connections = set()
    global_lock = Lock()
    committed_log = []
    tentative_log = []
    vv = {}

    index = int(sys.argv[1])
    port = int(sys.argv[2])

    # Process with id = 0 starts out as primary.
    am_primary = (index == 0)
    if am_primary:
        first_time = False

    LOG.basicConfig(filename='LOG/%d.log' % index, level=LOG.DEBUG)
    LOG.debug('%d: server.main()' % index)

    wthread = WorkerThread(address, index, port, connections, global_lock, committed_log, tentative_log)
    mhandler = MasterHandler(index, address, port, connections, global_lock, committed_log, tentative_log)
    wthread.start()
    mhandler.start()
    LOG.debug('%d: server.main: beginning while loop' % index)

    while True:
        # Initiate anti-entropy with all connections
        with global_lock:
            for pid in connections:
                sendServer(pid, server_logs(index, index, committed_log, tentative_log, vv))
        # Wait before conducting anti-entropy again.
        #LOG.debug('%d: server.main: log_com, log = %s, %s' % (index, committed_log, tentative_log))
        sleep(uniform(.1,.3))

if __name__ == '__main__':
    main()
