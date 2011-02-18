#!/usr/bin/env python
# ==================================================
# Simple key/value storage server using Avro IPC
# put(key, value), get(key), delete(key)

from BaseHTTPServer import BaseHTTPRequestHandler
from BaseHTTPServer import HTTPServer

from avro.ipc import AvroRemoteException
from avro import protocol
from avro import ipc

_PROTO = protocol.parse(file('ht-proto.avpr').read())
_STORAGE = {}

class Responder(ipc.Responder):
    def __init__(self):
        super(Responder, self).__init__(_PROTO)

    def invoke(self, msg, req):
        global _STORAGE
        if msg.name == 'get':
            return _STORAGE.get(req['key'])
        elif msg.name == 'put':
            _STORAGE[req['key']] = req['value']
        elif msg.name == 'delete':
            req_key = req['key']
            if req_key in _STORAGE:
                del _STORAGE[req_key]
        else:
            raise AvroRemoteException("unexpected message: " % msg.name)

class RequestHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        request_reader = ipc.FramedReader(self.rfile)
        request = request_reader.read_framed_message()

        responder = Responder()
        response_body = responder.respond(request)

        self.send_response(200)
        self.send_header('Content-Type', 'avro/binary')
        self.end_headers()

        response_writer = ipc.FramedWriter(self.wfile)
        response_writer.write_framed_message(response_body)

if __name__ == '__main__':
    server = HTTPServer(('localhost', 8080), RequestHandler)
    server.allow_reuse_address = True
    server.serve_forever()

