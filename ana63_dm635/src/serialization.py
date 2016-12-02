from ast import literal_eval

# Client should call this to serialize an 'add song: URL' message.
def client_add(c_id, seq_no, song_name, url):
    return client_message(c_id, seq_no, 'add', song_name, url)

# Client should call this to serialize a 'delete song' message.
def client_delete(c_id, seq_no, song_name):
    return client_message(c_id, seq_no, 'delete', song_name, None)

def client_read(c_id, seq_no, song_name):
    return client_message(c_id, seq_no, 'get', song_name, None)

# Client should call this to deserialize a message from a server. 
class ClientDeserialize:
    def __init__(self, message):
        pass

# Client should not call this!
def client_message(c_id, seq_no, write_type, song_name, url):
    message = '%s:%d:%d:%s:%s' % ('client', c_id, seq_no, write_type, song_name)
    if url is not None:
        message += ':%s' % url
    return message


## SERVER CODE.

# Server should call this to serialize a 'connect to me' message.
def server_connect():
    pass

def server_disconnect():
    pass

def server_elect():
    pass

def server_log():
    pass

def server_serialize(s_id, s_index, **keywords):
    pass

# Call this to get an object representing the deserialized state of a message. 
class ServerDeserialize:
    def __init__(self, message):
        self.sender_type, rest = message.split(':',1)
        if self.sender_type == 'client':
            self.client_id, self.sequence_number, self.action_type, rest = rest.split(':',3)
            self.client_id = int(self.client_id)
            self.sequence_number = int(self.sequence_number)
            if self.action_type in ['delete', 'get']:
                self.song_name = rest
            elif self.action_type in ['add']:
                self.song_name, self.URL = rest.split(':',1)
        elif self.sender_type == 'server':
            pass

