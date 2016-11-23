import logging as LOG
import os
from socket import AF_INET, socket, SOCK_STREAM
import sys
from threading import Thread, Lock

address = 'localhost'

class ClientHandler(Thread):
    def __init__(self, index, address, port):
        Thread.__init__(self)
        self.index = index
        LOG.debug('%d: server.ClientHandler()' % self.index)
    def run():
        LOG.debug('%d: server.ClientHandler.run()' % self.index)

class MasterHandler(Thread):
    def __init__(self, index, address, port):
        Thread.__init__(self)
        self.buffer = ''
        self.index = index
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
                    # TODO: create connections.
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
                    data = self.conn.recv(1024)
                    self.buffer += data
                except:
                    self.valid = False
                    self.conn.close()
                    break

class ServerHandler(Thread):
    def __init__(self, index, address, port):
        Thread.__init__(self)
        self.index = index
        LOG.debug('%d: server.ServerHandler()' % self.index)
    def run():
        LOG.debug('%d: server.ServerHandler.run()' % self.index)
        # TODO: do something.

def main():
    global address

    pid = int(sys.argv[1])
    port = int(sys.argv[2])

    LOG.basicConfig(filename='LOG/%d.log' % pid, level=LOG.DEBUG)
    LOG.debug('%d: server.main()' % pid)

    chandler = ClientHandler(pid, address, port)
    shandler = ServerHandler(pid, address, port)
    mhandler = MasterHandler(pid, address, port)
    chandler.start()
    shandler.start()
    mhandler.start()

    LOG.debug('%d: server.main ended' % pid)


if __name__ == '__main__':
  main()
