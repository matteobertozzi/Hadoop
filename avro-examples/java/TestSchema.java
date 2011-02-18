import java.io.*;
import java.util.*;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;

public class TestSchema {
  public static Schema schemaFromCode() {
    List<Field> fields = new ArrayList<Field>();
    fields.add(new Field("field1", Schema.create(Type.STRING), null, null));
    fields.add(new Field("field2", Schema.create(Type.LONG), null, null));
    fields.add(new Field("field3", Schema.createArray(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("field4", Schema.createMap(Schema.create(Type.INT)), null, null));

    Schema schema = Schema.createRecord("recordName", "Record Doc String", "recordNS", false);
    schema.setFields(fields);

    schema.addProp("Property1", "Value1");
    schema.addProp("Property2", "Value2");

    return(schema);
  }

  public static void writeSchema (File file, String json) throws IOException {
    FileWriter writer = new FileWriter(file);
    writer.write(json, 0, json.length());
    writer.close();
  }

  public static void main(String[] args) throws IOException {
    Schema s1 = schemaFromCode();
    String jsonSchema = s1.toString(true);

    File file = new File("test-schema.json");
    writeSchema(file, jsonSchema);

    Schema s2 = Schema.parse(file);
    assert(s1.equals(s2));

    System.out.println(jsonSchema);
  }
}
