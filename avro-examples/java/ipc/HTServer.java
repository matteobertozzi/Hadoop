// Simple key/value storage client using Avro IPC
// put(key, value), get(key), delete(key)

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.avro.Protocol;
import org.apache.avro.AvroRuntimeException;

import org.apache.avro.util.Utf8;
import org.apache.avro.generic.GenericData;

import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.generic.GenericResponder;

public class HTServer {
  private static ConcurrentSkipListMap<Utf8, ByteBuffer> storage = new ConcurrentSkipListMap<Utf8, ByteBuffer>();

  static class Responder extends GenericResponder {
    public Responder (Protocol protocol) {
      super(protocol);
    }

    public Object respond (Protocol.Message message, Object request) {
      GenericData.Record record = (GenericData.Record)request;
      String msgName = message.getName();

      if (msgName == "get")
        return(storage.get(record.get("key")));

      if (msgName == "put") {
        storage.put((Utf8)record.get("key"), (ByteBuffer)record.get("value"));
        return null;
      }

      if (msgName == "delete") {
        storage.remove(record.get("key"));
        return null;
      }

      throw new AvroRuntimeException("unexcepcted message: " + msgName);
    }
  }


  public static void main (String[] args) throws InterruptedException, IOException {
    Protocol protocol = Protocol.parse(new File("ht-proto.avpr"));

    HttpServer server = new HttpServer(new Responder(protocol), 8080);
    server.start();
    server.join();
  }
}

