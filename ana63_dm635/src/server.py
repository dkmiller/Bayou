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

# Listens for messages from clients and servers.
class ClientServerHandler(Thread):
    def __init__(self, index, address, connections, committed_log, tentative_log, am_primary):
        Thread.__init__(self)
        self.am_primary = am_primary
        self.connections = connections
        self.index = index
        self.log = tentative_log
        self.log_com = committed_log
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.bind((address, 20000+self.index))
        self.sock.listen(1)
        self.timer = -1
        self.vv = {}
        LOG.debug('%d: server.ClientHandler()' % self.index)

    def accept_stamp(self):
        self.timer += 1
        return (self.index, self.timer)

    def run(self):
        LOG.debug('%d: server.ClientHandler.run()' % self.index)
        while True:
            conn, addr = self.sock.accept()
            buff = ''
            valid = True
            while valid:
                if '\n' in buff:
                    (line, rest) = buff.split('\n', 1)
                    line = ServerDeserialize(line)
                    if line.sender_type == CLIENT:
                        client_knows_too_much = False
                        for server in line.vv:
                            if server not in self.vv or self.vv[server] < line.vv[server]:
                                client_knows_too_much = True
                        if client_knows_too_much:
                            sendClient(line.client_id, server_client_response(line.action_type, line.song_name, ERR_DEP, line.vv))
                        # Client doesn't know too much, so we can perform the requested action.
                        elif line.action_type == ADD:
                            self.log_entry(ADD, '%s,%s' % (line.song_name, line.url))
                            if self.index not in self.vv:
                                self.vv[self.index] = 0
                            else:
                                self.vv[self.index] += 1
                            sendClient(line.client_id, server_client_response(line.action_type, line.song_name, line.url, self.vv))
                        elif line.action_type == DELETE:
                            self.log_entry(DELETE, line.song_name)
                            if self.index not in self.vv:
                                self.vv[self.index] = 0
                            else:
                                self.vv[self.index] += 1
                            sendClient(line.client_id, server_client_response(line.action_type, line.song_name, '', self.vv))
                        elif line.action_type == GET:
                            # TODO: write self.state()
                            url = 'ERR_KEY'
                            state = self.state()
                            if line.song_name in state:
                                url = state[line.song_name]
                            sendClient(line.client_id, server_client_response(line.action_type, line.song_name, url, self.vv))
                    elif line.sender_type == SERVER:
                        if line.action_type == ANTI_ENTROPY:
                            if self.am_primary:
                                primary_anti_entropy(self.log_com, self.log, line.logs['committed'], line.logs['tentative'], line.logs['vv'], self.vv)
                            else:
                                anti_entropy(self.log_com, self.log, line.logs['committed'], line.logs['tentative'], line.logs['vv'], self.vv)
                        elif line.action_type == CONNECT:
                            self.connections.add(line.server_index)
                        elif line.action_type == DISCONNECT:
                            self.connections.discard(line.server_index)
                        elif line.action_type == UR_ELECTED:
                            self.am_primary = True
                else:
                    try:
                        buff += conn.recv(1024)
                    except:
                        LOG.debug('ClientServerHandler recv failed')
                        valid = False
                        conn.close()
                        break
    def log_entry(self, op_type, op_value):
        if self.am_primary:
            log = self.log_com
        else:
            log = self.log
        log.append({
            'OP_TYPE': op_type,
            'OP_VALUE': op_value,
            'ACCT_STAMP': self.accept_stamp()
        })
    def state(self):
        result = {}
        def update(st, log):
            for log_entry in log:
                if log_entry['OP_TYPE'] == 'PUT':
                    song_name, url = log_entry['OP_VALUE'].split(',',1)
                    st[song_name] = url
                elif log_entry['OP_TYPE'] == 'DELETE':
                    if log_entry['OP_VALUE'] in st:
                        del st[log_entry['OP_VALUE']]
        update(result, self.log_com)
        update(result, self.log)

# listens for messages from the master
class MasterHandler(Thread):
    def __init__(self, index, address, port, connections, log):
        Thread.__init__(self)
        self.buffer = ''
        self.connections = connections
        self.index = index
        self.log = log
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.bind((address, port))
        self.sock.listen(1)
        self.conn, self.addr = self.sock.accept()
        self.valid = True
        LOG.debug('%d: server.MasterHandler()' % self.index)

    def run(self):
        while self.valid:
            if '\n' in self.buffer:
                (line, rest) = self.buffer.split('\n', 1)
                self.buffer = rest
                LOG.debug('%d: server received \'%s\'' % (self.index, line))
                line = line.split()
                if 'createConn' == line[0]:
                    s_ids = map(int, line[1:])
                    self.connections |= set(s_ids)
                    message = '%d:connect' % self.index
                    for s_id in s_ids:
                        sendServer(s_id, message)
                elif 'breakConn' == line[0]:
                    s_ids = map(int, line[1:])
                    self.connections -= set(s_ids)
                    message = '%d:disconnect' % self.index
                    for s_id in s_ids:
                        sendServer(s_id, message)
                elif 'retire' == line[0]:
                    # TODO: retire.
                    pass
                elif 'printLog' == line[0]:
                    # TODO: print log.
                    pass
            else:
                try:
                    self.buffer += self.conn.recv(1024)
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
        LOG.debug('server send ERROR: ' + str(msg))

# Send a message to a server.
def sendServer(s_id, message):
    global address
    try:
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((address, 20000 + s_id))
        sock.send(str(message) + '\n')
        sock.close()
    except Exception as msg:
        LOG.debug('server send ERROR: ' + str(msg))


# Returns the version vector corresponding to a log.
def version_vector(log):
    result = {}
    for log_entry in log:
        index, timer = log_entry['ACCT_STAMP']
        result[index] = timer
    return result

def main():
    global address
    connections = set()
    commited_log = []
    tentative_log = []

    index = int(sys.argv[1])
    port = int(sys.argv[2])

    # Process with id = 0 starts out as primary.
    i_am_primary = (index == 0)

    LOG.basicConfig(filename='LOG/%d.log' % index, level=LOG.DEBUG)
    LOG.debug('%d: server.main()' % index)

    cshandler = ClientServerHandler(index, address, connections, committed_log, tentative_log, i_am_primary)
    mhandler = MasterHandler(index, address, port, connections, committed_log, tentative_log)
    cshandler.start()
    mhandler.start()
    LOG.debug('%d: server.main: beginning while loop' % index)

    while True:
        # Initiate anti-entropy with all connections
        for pid in connections:
            sendServer(pid, server_logs(index, index, committed_log, tentative_log, cshandler.vv))
        # Wait before conducting anti-entropy again.
        sleep(uniform(.1,.3))

if __name__ == '__main__':
    main()
