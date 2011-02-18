#!/usr/bin/env python

from avro import protocol
from avro import ipc

if __name__ == '__main__':
    proto = protocol.parse(file('test-proto.avpr').read())

    client = ipc.HTTPTransceiver('localhost', 8080)
    requestor = ipc.Requestor(proto, client)

    message = {'data': 'Hello from client'}
    result = requestor.request('xyz', {'message': message})
    print result

    client.close()
