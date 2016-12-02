import logging as LOG
from socket import AF_INET, socket, SOCK_STREAM
import sys
import serialization
from threading import Thread, Lock

root_port21k = 21000
address = 'localhost'
mHandler = None
client_counter = 0

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
                    ## Send add song to the server
                    
                    # increment the seq_no
                    client_counter++
                    # get msg payload
                    msg = client_add(self.index, client_counter, songName, URL)
                    # send msg to server
                    send(s_id, msg)

                elif 'delete' == line[0]:
                    songName = line[1]
                    s_id = int(line[2])
                    ## Send delete song to the server
                    
                    # increment the seq_no
                    client_counter++
                    # get msg payload
                    msg = client_delete(self.index, client_counter, songName)
                    # send msg to server
                    send(s_id, msg)

                elif 'get' == line[0]:
                    songName = line[1]
                    s_id = int(line[2])
                    ## Send get song to the server
                    
                    # get msg payload
                    msg = client_read(self.index, client_counter, songName)
                    # send msg to server
                    send(s_id, msg)

            else:
                try:
                    data = self.conn.recv(1024)
                    self.buffer += data
                except:
                    self.valid = False
                    self.conn.close()
                    break

    def send(self, s):
        self.conn.send(str(s) + '\n')

    def close(self):
        try:
            self.sock.close()
        except:
            pass

def send(pid, msg):
    global mHandler, root_port21k
    if pid is -1:
        mHandler.send(msg)
        return
    try:
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((address, root_port21k + pid))
        sock.send(str(msg) + '\n')
        sock.close()
    except:
        LOG.debug('SOCKET: ERROR ' + str(msg))

class ServerHandler(Thread):
    def __init__(self, index, address, port):
        Thread.__init__(self)
        self.buffer = ''
        self.index = index
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.bind((address, port))
        self.sock.listen(1)
        self.conn, self.addr = self.sock.accept()
        self.valid = True
        LOG.debug('%d: client.ServerHandler()' % self.index)
    def run():
        LOG.debug('%d: client.ServerHandler.run()' % self.index)
        # TODO: do something

def main():
    global address, mHandler, root_port21k

    pid = int(sys.argv[1])
    port = int(sys.argv[2])

    LOG.basicConfig(filename='LOG/%d.log' % pid, level=LOG.DEBUG)
    LOG.debug('%d: client.main()' % pid)

    shandler = ServerHandler(pid, address, root_port21k + pid)
    mhandler = MasterHandler(pid, address, port)
    shandler.start()
    mhandler.start()

    LOG.debug('%d: client.main ended' % pid)

if __name__ == '__main__':
    main()
