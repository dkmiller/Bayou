from ast import literal_eval
import logging as LOG
import os
from random import uniform
from re import match
from socket import AF_INET, socket, SOCK_STREAM
import sys
from threading import Thread, Lock
from time import sleep

address = 'localhost'

# Listens for messages from clients and servers.
class ClientServerHandler(Thread):
    def __init__(self, index, address, connections, log):
        Thread.__init__(self)
        self.connections = connections
        self.index = index
        self.log = log
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.bind((address, 20000+self.index))
        self.sock.listen(1)
        self.timer = -1
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
                    LOG.debug('ClientServerHandler: received \'%s\'' % line)
                    # Received anti-entropy request from another server.
                    if match('^\d+:anti-entropy:', line):
                        line = line.split(':', 2)
                        sender_id = int(line[0])
                        vv = literal_eval(line[2])
                        anti_entropy(self.connections, self.log, self.index, sender_id, vv)
                    elif match('^d+:anti-response:', line):
                        line = line.split(':',2)
                        s_id = int(line[0])
                        s_log = literal_eval(line[2]))
                        # Update log.
                        for log_entry in s_log:
                            if log_entry not in self.log:
                                self.log.append(log_entry)
                        # TODO: handle retirement.
                    elif match('^d+:connect', line):
                        s_id = int(line.split(':')[0])
                        self.connections.add(s_id)
                    elif match('^d:create', line):
                        s_id = int(line.split(':')[0])
                        self.connections.add(s_id)
                        self.log_entry('CREATE', s_id)
                    elif match('^\d+:disconnect', line):
                        s_id = int(line.split(':')[0])
                        self.connections.discard(s_id)
                    elif match('^\d+:retire', line):
                        s_id = int(line.split(':')[0])
                        self.connections.discard(s_id)
                        self.log_entry('RETIRE', s_id)
                    elif match('^d:add:', line):
                        line = line.split(':')
                        song_name = line[2]
                        URL = line[3]
                        self.log_entry('PUT', song_name + ',' + URL)
                    elif match('^d:delete', line):
                        line = line.split(':')
                        song_name = line[2]
                        self.log_entry('DELETE', song_name)
                    elif match('^d:get:', line):
                        line = line.split(':')
                        c_id = int(line[0])
                        song_name = line[2]
                        state = current_state(self.log)
                        if song_name in state:
                            URL = state[song_name]
                            message = '%d:%s:%s' % (self.index, song_name, URL)
                        else:
                            message = '%d:%s:ERR_DEP' % (self.index, song_name)
                        sendClient(c_id, message)
                else:
                    try:
                        buff += conn.recv(1024)
                    except:
                        LOG.debug('ClientServerHandler recv failed')
                        valid = False
                        conn.close()
                        break
    def log_entry(self, op_type, op_value):
        self.log.append({
            'OP_TYPE': op_type,
            'OP_VALUE': op_value,
            'ACCT_STAMP': self.accept_stamp()
        })

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

# Conducts anti-entropy with process pid.
def anti_entropy(connections, log, pid, s_id, vv):
    LOG.debug('%d:server.anti_entropy(%d, %s)' % (pid, s_id, vv))
    send_log = []
    for log_entry in log:
        index, timer = log_entry['ACCT_STAMP']
        if vv[index] < timer:
            send_log.append(log_entry)
    message = '%d:anti-response:%s' % send_log
    sendServer(s_id, message)

# Returns a song-name: URL dictionary determined by the log.
def current_state(log):
    result = {}
    for log_entry in log:
        if log_entry['OP_TYPE'] == 'PUT':
            song_name, URL = log_entry['OP_VALUE'].split(',')
            result[song_name] = URL
        elif log_entry['OP_TYPE'] == 'DELETE':
            song_name = log_entry['OP_VALUE']

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
    log = []

    index = int(sys.argv[1])
    port = int(sys.argv[2])

    # Process with id = 0 starts out as primary.
    i_am_primary = (index == 0)

    LOG.basicConfig(filename='LOG/%d.log' % index, level=LOG.DEBUG)
    LOG.debug('%d: server.main()' % index)

    cshandler = ClientServerHandler(index, address, connections, log)
    mhandler = MasterHandler(index, address, port, connections, log)
    cshandler.start()
    mhandler.start()
    LOG.debug('%d: server.main: beginning while loop' % index)

    while True:
        # Initiate anti-entropy with all connections
        message = '%d:anti-entropy:%s' % (index, version_vector(log))
        for pid in connections:
            sendServer(pid, message)
        # Wait before conducting anti-entropy again.
        sleep(uniform(.1,.3))

if __name__ == '__main__':
    main()
