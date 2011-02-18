import java.io.*;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericDatumReader;

public class TestFileEvo {
  public static GenericData.Record makeObjectV0 (Schema schema, String path) {
    GenericData.Record record = new GenericData.Record(schema);
    record.put("path", path);
    return(record);
  }

  public static GenericData.Record makeObjectV1 (Schema schema, String path, long length) {
    GenericData.Record record = new GenericData.Record(schema);
    record.put("path", path);
    record.put("length", length);
    return(record);
  }

  public static GenericData.Record makeObjectV2 (Schema schema, String path, long size, Map<String, String> attrs) {
    if (attrs == null)
      attrs = new TreeMap<String, String>();

    GenericData.Record record = new GenericData.Record(schema);
    record.put("path", path);
    record.put("size", size);
    record.put("attributes", attrs);
    return(record);
  }

  public static void testWriteV0 (File file, Schema schema) throws IOException {
    GenericDatumWriter<GenericData.Record> datum = new GenericDatumWriter<GenericData.Record>(schema);
    DataFileWriter<GenericData.Record> writer = new DataFileWriter<GenericData.Record>(datum);

    writer.create(schema, file);
    writer.append(makeObjectV0(schema, "/"));
    writer.append(makeObjectV0(schema, "/root"));
    writer.append(makeObjectV0(schema, "/root/myfile"));

    writer.close();
  }

  public static void testWriteV1 (File file, Schema schema) throws IOException {
    GenericDatumWriter<GenericData.Record> datum = new GenericDatumWriter<GenericData.Record>(schema);
    DataFileWriter<GenericData.Record> writer = new DataFileWriter<GenericData.Record>(datum);

    writer.create(schema, file);
    writer.append(makeObjectV1(schema, "/", 0));
    writer.append(makeObjectV1(schema, "/root", 0));
    writer.append(makeObjectV1(schema, "/root/myfile", 8192));

    writer.close();
  }

  public static void testWriteV2 (File file, Schema schema) throws IOException {
    GenericDatumWriter<GenericData.Record> datum = new GenericDatumWriter<GenericData.Record>(schema);
    DataFileWriter<GenericData.Record> writer = new DataFileWriter<GenericData.Record>(datum);

    TreeMap<String, String> attrs = new TreeMap<String, String>();
    attrs.put("author", "th30z");
    attrs.put("type", "b+tree");

    writer.create(schema, file);
    writer.append(makeObjectV2(schema, "/", 0, null));
    writer.append(makeObjectV2(schema, "/root", 0, null));
    writer.append(makeObjectV2(schema, "/root/myfile", 8192, attrs));

    writer.close();
  }

  public static void testReadV0 (File file, Schema schema) throws IOException {
    GenericDatumReader<GenericData.Record> datum = new GenericDatumReader<GenericData.Record>(null, schema);
    DataFileReader<GenericData.Record> reader = new DataFileReader<GenericData.Record>(file, datum);

    GenericData.Record record = new GenericData.Record(schema);
    while (reader.hasNext()) {
      reader.next(record);
      System.out.println("Path " + record.get("path"));
    }

    reader.close();
  }

  public static void testReadV1 (File file, Schema schema) throws IOException {
    GenericDatumReader<GenericData.Record> datum = new GenericDatumReader<GenericData.Record>(null, schema);
    DataFileReader<GenericData.Record> reader = new DataFileReader<GenericData.Record>(file, datum);

    GenericData.Record record = new GenericData.Record(schema);
    while (reader.hasNext()) {
      reader.next(record);
      System.out.println("Path " + record.get("path") +
                         " Length " + record.get("length"));
    }

    reader.close();
  }


  public static void testReadV2 (File file, Schema schema) throws IOException {
    GenericDatumReader<GenericData.Record> datum = new GenericDatumReader<GenericData.Record>(null, schema);
    DataFileReader<GenericData.Record> reader = new DataFileReader<GenericData.Record>(file, datum);

    GenericData.Record record = new GenericData.Record(schema);
    while (reader.hasNext()) {
      reader.next(record);
      System.out.println("Path " + record.get("path") +
                         " Size " + record.get("size") +
                         " Attrs " + record.get("attributes"));
    }

    reader.close();
  }

  public static void main (String[] args) throws IOException {
    Schema s0 = Schema.parse(new File("schema/FSObject-v0.json"));
    Schema s1 = Schema.parse(new File("schema/FSObject-v1.json"));
    Schema s2 = Schema.parse(new File("schema/FSObject-v2.json"));

    File f0 = new File("tevo-v0.avro");
    File f1 = new File("tevo-v1.avro");
    File f2 = new File("tevo-v2.avro");

    testWriteV0(f0, s0);
    testWriteV1(f1, s1);
    testWriteV2(f2, s2);

    System.out.println("Read File v0 with Scheme v0"); testReadV0(f0, s0);
    System.out.println("\nRead File v1 with Scheme v0"); testReadV0(f1, s0);
    System.out.println("\nRead File v2 with Scheme v0"); testReadV0(f1, s0);

    System.out.println("\n\nRead File v0 with Scheme v1"); testReadV1(f0, s1);
    System.out.println("\nRead File v1 with Scheme v1"); testReadV1(f1, s1);
    System.out.println("\nRead File v2 with Scheme v1"); testReadV1(f2, s1);

    System.out.println("\n\nRead File v0 with Scheme v2"); testReadV2(f0, s2);
    System.out.println("\nRead File v1 with Scheme v2"); testReadV2(f1, s2);
    System.out.println("\nRead File v2 with Scheme v2"); testReadV2(f2, s2);
  }
}

