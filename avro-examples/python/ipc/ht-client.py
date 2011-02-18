#!/usr/bin/env python
# ==================================================
# Simple key/value storage client using Avro IPC
# put(key, value), get(key), delete(key)

from avro import protocol
from avro import ipc

_PROTO = protocol.parse(file('ht-proto.avpr').read())

class HTClient(object):
    def __init__(self, host, port):
        self._host = host
        self._port = port

    def get(self, key):
        return self._request('get', {'key': key})

    def put(self, key, value):
        return self._request('put', {'key': key, 'value': value})

    def delete(self, key):
        return self._request('delete', {'key': key})

    def _request(self, msg, args):
        transceiver = ipc.HTTPTransceiver(self._host, self._port)
        requestor = ipc.Requestor(_PROTO, transceiver)
        response = requestor.request(msg, args)
        transceiver.close()
        return response

if __name__ == '__main__':
    client = HTClient('localhost', 8080)

    def _testGet(client):
        for i in xrange(12):
            key = 'Key %d' % i
            value = client.get(key)
            print 'GET %s %r' % (key, value)


    for i in xrange(10):
        client.put('Key %d' % i, 'Value %d' % i)
    _testGet(client)

    for i in xrange(10):
        client.delete('Key %d' % i)
    _testGet(client)


