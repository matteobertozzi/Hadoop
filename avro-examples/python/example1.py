#!/usr/bin/env python
# =============================================================================
# Example 1 - How to read & write a record list.
#  - testWrite(): Write 10 record to a new file, and write schema)
#  - testAppend(): Append 10 record to previously created file,
#                  The schema is read from file.
#  - testRead(): Read the file and print the records.
# =============================================================================

from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader
import avro.schema

TEST_SCHEMA = """
{
    "type": "record",
    "name": "person",
    "fields": [
                { "name": "name", "type": "string" },
                { "name": "company", "type": "string" },
                { "name": "website", "type": { "type": "array", "items": "string" }}
              ]
}
"""

def _makeTestPerson(uid):
    return {'name':'Person %d' % uid,
            'company':'Company %d' % uid,
            'website': ['http://myurl%d.net' % i for i in xrange(uid % 5)],
            }

def testWrite(filename):
    schema_object = avro.schema.parse(TEST_SCHEMA)

    fd = open(filename, 'wb')
    datum_writer = DatumWriter()
    fwriter = DataFileWriter(fd, datum_writer, schema_object)
    for i in xrange(10):
        fwriter.append(_makeTestPerson(i))
    fwriter.close()

def testAppend(filename):
    fd = open(filename, 'a+b')
    datum_writer = DatumWriter()
    fwriter = DataFileWriter(fd, datum_writer)
    for i in xrange(10, 20):
        fwriter.append(_makeTestPerson(i))
    fwriter.close()

def testRead(filename):
    fd = open(filename, 'rb')
    datum_writer = DatumReader()
    freader = DataFileReader(fd, datum_writer)
    for datum in freader:
        print datum['name'], datum['company']
        print datum['website']
        print
    freader.close()

if __name__ == '__main__':
    FILENAME = 'test.db'
    testWrite(FILENAME)
    testAppend(FILENAME)
    testRead(FILENAME)

