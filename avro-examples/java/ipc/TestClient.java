import java.io.*;
import java.net.*;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Protocol;
import org.apache.avro.util.Utf8;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.generic.GenericRequestor;

public class TestClient {
  public static void main (String[] args) throws IOException, MalformedURLException {
    Protocol protocol = Protocol.parse(new File("test-proto.avpr"));

    HttpTransceiver transceiver = new HttpTransceiver(new URL("http://localhost:8080"));
    GenericRequestor requestor = new GenericRequestor(protocol, transceiver);

    GenericData.Record message = new GenericData.Record(protocol.getType("Message"));
    message.put("data", new Utf8("Hello from Client"));

    Schema schema = protocol.getMessages().get("xyz").getRequest();
    GenericData.Record request = new GenericData.Record(schema);
    request.put("message", message);

    Object result = requestor.request("xyz", request);
    System.out.println(result);
  }
}

