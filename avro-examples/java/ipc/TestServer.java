import java.io.*;

import org.apache.avro.Protocol;
import org.apache.avro.AvroRuntimeException;

import org.apache.avro.util.Utf8;

import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.generic.GenericResponder;

public class TestServer {
  static class Responder extends GenericResponder {
    public Responder (Protocol protocol) {
      super(protocol);
    }

    public Object respond (Protocol.Message message, Object request) {
      String msgName = message.getName();
      if (msgName == "xyz") {
        return new Utf8("The java responder greets you.");
      } else {
        throw new AvroRuntimeException("unexcepcted message: " + msgName);
      }
    }
  }


  public static void main (String[] args) throws InterruptedException, IOException {
    Protocol protocol = Protocol.parse(new File("test-proto.avpr"));

    HttpServer server = new HttpServer(new Responder(protocol), 8080);
    server.start();
    server.join();
  }
}

