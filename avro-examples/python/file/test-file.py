#!/usr/bin/env python

from avro import schema
from avro.io import DatumWriter, DatumReader
from avro.datafile import DataFileWriter, DataFileReader

def makeSchema():
    json_schema = """
                    {
                     "type":"record",
                     "name":"Person",
                     "namespace":"avro.test",
                     "fields":[
                        {"name":"name","type":"string"},
                        {"name":"age","type":"int"}]
                    }
                 """
    return schema.parse(json_schema);

def makeObject(name, age):
    return {'name': name, 'age': age}

def testWrite(filename, schema):
    fd = open(filename, 'wb')

    datum = DatumWriter()
    writer = DataFileWriter(fd, datum, schema)

    writer.append(makeObject("Person A", 23))
    writer.append(makeObject("Person B", 31))
    writer.append(makeObject("Person C", 28))

    writer.close()

def testRead(filename):
    fd = open(filename, 'rb')

    datum = DatumReader()
    reader = DataFileReader(fd, datum)

    for record in reader:
        print record['name'], record['age']

    reader.close()

if __name__ == '__main__':
    filename = 'test-file.avro'
    schema = makeSchema()

    testWrite(filename, schema)
    testRead(filename)
