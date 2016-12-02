from ast import literal_eval

# Client should call this to serialize an 'add song: URL' message.
def client_add(c_id, seq_no, song_name, url):
    return client_message(c_id, seq_no, 'add', song_name, url)

# Client should call this to serialize a 'delete song' message.
def client_delete(c_id, seq_no, song_name):
    return client_message(c_id, seq_no, 'delete', song_name, None)

def client_read(c_id, seq_no, song_name):
    return client_message(c_id, seq_no, 'get', song_name, None)

# Client should not call this!
def client_message(c_id, seq_no, write_type, song_name, url):
    message = '%s:%d:%d:%s:%s' % ('client', c_id, seq_no, write_type, song_name)
    if url is not None:
        message += ':%s' % url
    return message

def server_deserialize(message):
    sender_type, rest = message.split(':',1)
    if sender_type == 'client':
        c_id, seq_no, kind, rest = rest.split(':',3)
        song_name = url = None
        if kind in ['delete', 'get']:
            song_name = rest
        elif kind in ['add']:
            song_name, url = rest.split(':', 1)
        return ('client', c_id, seq_no, kind, song_name, url)
    elif sender_type == 'server':
        pass
