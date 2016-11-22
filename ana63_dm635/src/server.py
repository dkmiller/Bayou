import logging as LOG
import os
from socket import AF_INET, socket, SOCK_STREAM
import sys
from threading import Thread, Lock

address = 'localhost'

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

    def run(self):
        while self.valid:
            if '\n' in self.buffer:
                (line, rest) = self.buffer.split('\n', 1)
                self.buffer = rest
                # TODO: do something with line
                LOG.debug('%d: server received \'%s\'' % (self.index, line))
            else:
                try:
                    data = self.conn.recv(1024)
                    self.buffer += data
                except:
                    self.valid = False
                    self.conn.close()
                    break

def main():
    global address

    pid = int(sys.argv[1])
    port = int(sys.argv[2])

    LOG.basicConfig(filename='LOG/%d.log' % pid, level=LOG.DEBUG)
    LOG.debug('%d: server started' % pid)


    mhandler = MasterHandler(pid, address, port)
    mhandler.start()

    LOG.debug('%d: server.main ended' % pid)


if __name__ == '__main__':
  main()
