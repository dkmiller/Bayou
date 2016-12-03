import logging as LOG
from socket import AF_INET, socket, SOCK_STREAM
import sys
from serialization import client_add, client_delete, client_get, ClientDeserialize
from threading import Thread, Lock

root_port21k = 21000
root_port20k = 20000
address = 'localhost'
mHandler = None
client_vv = dict()
global_flag = True

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
        global client_vv, global_flag
        while self.valid:
            if global_flag:
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
                        
                        # get msg payload
                        msg = client_add(self.index, client_vv, songName, URL)
                        # send msg to server
                        send(s_id, msg)
                        global_flag = False

                    elif 'delete' == line[0]:
                        songName = line[1]
                        s_id = int(line[2])
                        ## Send delete song to the server
                        
                        # get msg payload
                        msg = client_delete(self.index, client_vv, songName)
                        # send msg to server
                        send(s_id, msg)
                        global_flag = False

                    elif 'get' == line[0]:
                        songName = line[1]
                        s_id = int(line[2])
                        ## Send get song to the server
                        
                        # get msg payload
                        msg = client_read(self.index, client_vv, songName)
                        # send msg to server
                        send(s_id, msg)
                        global_flag = False
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
    global mHandler, root_port20k
    LOG.debug('client.send(%d,\'%s\')' % (pid, msg))
    if pid is -1:
        mHandler.send(msg)
        return
    try:
        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((address, root_port20k + pid))
        sock.send(str(msg) + '\n')
        sock.close()
    except:
        LOG.debug('SOCKET: ERROR ' + str(msg))

    #shandler = ServerHandler(pid, address, root_port21k + pid)
class ServerHandler(Thread):
    def __init__(self, index, address, port):
        Thread.__init__(self)
        self.index = index
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.bind((address, port))
        self.sock.listen(1)
        LOG.debug('client.ServerHandler()')

    def run():
        global client_vv, global_flag
        LOG.debug('%d: client.ServerHandler.run()' % self.index)
        while True:
            conn, addr = self.sock.accept()
            LOG.debug('%d: client.Handler accept' % self.index)
            buff = ''
            valid = True
            while valid:
                if not global_flag:
                    if '\n' in buff:
                        (line, rest) = buff.split('\n', 1)
                        LOG.debug('%d: client got \'%s\'' % (self.index, line))
                        buff = rest
                        #TODO
                        line = ClientDeserialize(line)
                        if line.url == 'ERR_DEP':
                            if line.action_type in ['PUT', 'DELETE']:
                                global_flag = True
                            elif line.action_type == 'GET':
                                # send msg to master
                                send(-1, line.url)
                                global_flag = True
                        # operation successful
                        else:
                            if line.action_type in ['PUT', 'DELETE']:
                                client_vv = line.vv
                                global_flag = True
                            elif line.action_type == 'GET':
                                # send msg to master
                                client_vv = line.vv
                                send(-1, "getResp " + str(line.songName) + ":" + str(line.url))
                                global_flag = True

                    else:
                        try:
                            data = conn.recv(1024)
                            buff += data
                        except:
                            valid = False
                            conn.close()
                            break

def main():
    global address, mHandler, root_port21k

    pid = int(sys.argv[1])
    port = int(sys.argv[2])

    LOG.basicConfig(filename='LOG/%d.log' % pid, level=LOG.DEBUG)
    LOG.debug('%d: client.main()' % pid)

    shandler = ServerHandler(pid, address, root_port21k + pid)
    mHandler = MasterHandler(pid, address, port)
    shandler.start()
    mHandler.start()

    LOG.debug('%d: client.main ended' % pid)

if __name__ == '__main__':
    main()
