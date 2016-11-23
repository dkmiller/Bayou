import logging as LOG
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
        LOG.debug('%d: client.MasterHandler()' % self.index)

    def run(self):
        while self.valid:
            if '\n' in self.buffer:
                (line, rest) = self.buffer.split('\n', 1)
                LOG.debug('%d: client got \'%s\'' % (self.index, line))
                self.buffer = rest
                line = line.split()
                if 'add' == line[0]:
                    songName = line[1]
                    URL = line[2]
                    s_id = int(line[3])
                    # TODO: add the song
                elif 'delete' == line[0]:
                    songName = line[1]
                    s_id = int(line[2])
                    # TODO: delete the song
                elif 'get' == line[0]:
                    songName = line[1]
                    s_id = int(line[2])
                    # TODO: get the URL
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
        LOG.debug('%d: client.ServerHandler()' % self.index)
    def run():
        LOG.debug('%d: client.ServerHandler.run()' % self.index)
        # TODO: do something

def main():
    global address

    pid = int(sys.argv[1])
    port = int(sys.argv[2])

    LOG.basicConfig(filename='LOG/%d.log' % pid, level=LOG.DEBUG)
    LOG.debug('%d: client.main()' % pid)

    shandler = ServerHandler(pid, address, port)
    mhandler = MasterHandler(pid, address, port)
    shandler.start()
    mhandler.start()

    LOG.debug('%d: client.main ended' % pid)

if __name__ == '__main__':
    main()
