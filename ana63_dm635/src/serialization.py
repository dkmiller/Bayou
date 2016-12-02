from ast import literal_eval

ADD = 'add'
DELETE = 'delete'
GET = 'get'

ANTI_ENTROPY = 'anti-entropy'
ANTI_ENTROPY_LOG = 'anti-entropy-log'
CONNECT = 'connect'
DISCONNECT = 'disconnect'
UR_ELECTED = 'ur-elected'

# Client should call this to serialize an 'add song: URL' message.
def client_add(c_id, seq_no, song_name, url):
    return client_message(c_id, seq_no, ADD, song_name, url)

# Client should call this to serialize a 'delete song' message.
def client_delete(c_id, seq_no, song_name):
    return client_message(c_id, seq_no, DELETE, song_name, None)

def client_get(c_id, seq_no, song_name):
    return client_message(c_id, seq_no, GET, song_name, None)

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

# Server should call this to serialize a 'initiate anti-entropy' message.
def server_anti_entropy(s_index, s_id, version_vector):
    return server_message(s_index, s_id, ANTI_ENTROPY, version_vector)

# Server should call this to serialize a 'connect to me' message.
def server_connect(s_index, s_id):
    return server_message(s_index, s_id, CONNECT, None)

# Server should call this to serialize a 'disconnect with me' message.
def server_disconnect(s_index, s_id):
    return server_message(s_index, s_id, DISCONNECT, None)

# Primary should call this to serialize a 'you are now primary' message.
def server_elect(s_index, s_id):
    return server_message(s_index, s_id, UR_ELECTED, None)

# Server should call this to serialize a 'response with log' message.
def server_log(s_index, s_id, log):
    return server_message(s_index, s_id, ANTI_ENTROPY_LOG, log)

def server_message(s_index, s_id, m_type, stuff):
    message = '%s:%d:%s:%s' % ('server', s_index, s_id, m_type)
    if stuff is not None:
        message += ':%s' % stuff
    return message

# Call this to get an object representing the deserialized state of a message. 
class ServerDeserialize:
    def __init__(self, message):
        self.sender_type, rest = message.split(':',1)
        if self.sender_type == 'client':
            self.client_id, self.sequence_number, self.action_type, rest = rest.split(':',3)
            self.client_id = int(self.client_id)
            self.sequence_number = int(self.sequence_number)
            if self.action_type in [DELETE, GET]:
                self.song_name = rest
            elif self.action_type in [ADD]:
                self.song_name, self.URL = rest.split(':',1)
        elif self.sender_type == 'server':
            self.sender_index, self.sender_id, self.message_type = rest.split(':',2)
            self.sender_index = int(self.sender_index)
            if self.message_type not in [CONNECT, DISCONNECT, UR_ELECTED]:
                self.message_type, rest = rest.split(':',3)[2:]
                if self.message_type == ANTI_ENTROPY:
                    self.version_vector = literal_eval(rest)
                elif self.message_type == ANTI_ENTROPY_LOG:
                    self.log = literal_eval(rest)

