// Simple key/value storage client using Avro IPC
// put(key, value), get(key), delete(key)

import java.io.*;
import java.net.*;
import java.util.Map;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.Protocol;
import org.apache.avro.util.Utf8;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.generic.GenericRequestor;

public class HTClient {
  private Protocol protocol;
  private URL url;

  public HTClient (URL url) throws IOException {
    this.url = url;
    protocol = Protocol.parse(new File("ht-proto.avpr"));
  }

  public ByteBuffer get (String key) throws IOException {
    Schema schema = protocol.getMessages().get("get").getRequest();
    GenericData.Record request = new GenericData.Record(schema);
    request.put("key", key);

    return((ByteBuffer)request("get", request));
  }

  public void put (String key, ByteBuffer value) throws IOException {
    Schema schema = protocol.getMessages().get("put").getRequest();
    GenericData.Record request = new GenericData.Record(schema);
    request.put("key", key);
    request.put("value", value);

    request("put", request);
  }

  public void delete (String key) throws IOException {
    Schema schema = protocol.getMessages().get("delete").getRequest();
    GenericData.Record request = new GenericData.Record(schema);
    request.put("key", key);

    request("delete", request);
  }

  private Object request (String msg, Object request) throws IOException {
    HttpTransceiver transceiver = new HttpTransceiver(url);
    GenericRequestor requestor = new GenericRequestor(protocol, transceiver);
    return(requestor.request(msg, request));
  }

  public static void main (String[] args) throws IOException, MalformedURLException {
    HTClient client = new HTClient(new URL("http://localhost:8080"));

    for (int i = 0; i < 10; ++i)
      client.put(String.format("Key %d", i), ByteBuffer.wrap(String.format("Value %d", i).getBytes()));

    for (int i = 0; i < 12; ++i) {
      String key = String.format("Key %d", i);
      ByteBuffer value = client.get(key);
      System.out.println("GET " + key + ": " + value.array());
    }

    for (int i = 0; i < 10; ++i)
      client.delete(String.format("Key %d", i));

        for (int i = 0; i < 12; ++i) {
      String key = String.format("Key %d", i);
      ByteBuffer value = client.get(key);
      System.out.println("GET " + key + ": " + value.array());
    }
  }
}

