from ast import literal_eval
import logging as LOG
import os
from random import uniform
from re import match
from socket import AF_INET, socket, SOCK_STREAM
import sys
from threading import Thread, Lock
from time import sleep

# Listens for messages from clients and servers.
class ClientServerHandler(Thread):
    def __init__(self, index, address, connections, log):
        Thread.__init__(self)
        self.address = address
        self.connections = connections
        self.index = index
        self.log = log
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.bind((address, 20000+self.index))
        self.sock.listen(1)
        LOG.debug('%d: server.ClientHandler()' % self.index)
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
                else:
                    try:
                        buff += conn.recv(1024)
                    except:
                        LOG.debug('ClientServerHandler recv failed')
                        valid = False
                        conn.close()
                        break

# send a message to a client
def sendClient(address, pid, message):
    try:
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((address, 21000+pid))
        sock.send(str(message) + '\n')
        sock.close()
    except Exception as msg:
        LOG.debug('server send ERROR: ' + str(msg))

# send a message to a server
def sendServer(address, pid, message):
    try:
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((address, 20000+pid))
        sock.send(str(message) + '\n')
        sock.close()
    except Exception as msg:
        LOG.debug('server send ERROR: ' + str(msg))

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
                elif 'breakConn' == line[0]:
                    s_ids = map(int, line[1:])
                    # TODO: break connections.
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
    connections.add(s_id)

def V(log):
    return {-1: 'TODO'}

def main():
    address = 'localhost'
    connections = set()
    log = []

    index = int(sys.argv[1])
    port = int(sys.argv[2])

    LOG.basicConfig(filename='LOG/%d.log' % index, level=LOG.DEBUG)
    LOG.debug('%d: server.main()' % index)

    cshandler = ClientServerHandler(index, address, connections, log)
    mhandler = MasterHandler(index, address, port, connections, log)
    cshandler.start()
    mhandler.start()
    LOG.debug('%d: server.main: beginning while loop' % index)

    while True:
        # Initiate anti-entropy with all connections
        message = '%d:anti-entropy:%s' % (index, V(log))
        LOG.debug('%d: server.main: connections = %s' % (index, connections))
        for pid in connections:
            sendServer(address, pid, message)
        # Wait a bit before conducting anti-entropy again.
        sleep(uniform(.1,.4))

if __name__ == '__main__':
  main()
