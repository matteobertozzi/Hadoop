/*
 * gcc -Wall test-file.c -o test-file -I/usr/include/ -L/usr/local/lib/ -lavro
 * LD_LIBRARY_PATH=/usr/local/lib ./test-file
 */
#include <stdlib.h>
#include <stdio.h>

#include <avro.h>

static avro_schema_t __make_schema (void) {
    avro_schema_t field_schema;
    avro_schema_t schema;

    schema = avro_schema_record("Person", "avro.test");

    field_schema = avro_schema_string();
    avro_schema_record_field_append(schema, "name", field_schema);
    avro_schema_decref(field_schema);

    field_schema = avro_schema_int();
    avro_schema_record_field_append(schema, "age", field_schema);
    avro_schema_decref(field_schema);

    return(schema);
}

static avro_datum_t __make_object (avro_schema_t schema, const char *name, int age) {
    avro_datum_t field_datum;
    avro_datum_t datum;

    datum = avro_datum_from_schema(schema);

    field_datum = avro_string(name);
    avro_record_set(datum, "name", field_datum);
    avro_datum_decref(field_datum);

    field_datum = avro_int32(age);
    avro_record_set(datum, "age", field_datum);
    avro_datum_decref(field_datum);

    return(datum);
}

static int __test_write (const char *filename, avro_schema_t schema) {
    avro_file_writer_t writer;
    avro_datum_t datum;

    if (avro_file_writer_create(filename, schema, &writer))
        return(-1);

    datum = __make_object(schema, "Person A", 23);
    avro_file_writer_append(writer, datum);
    avro_datum_decref(datum);

    datum = __make_object(schema, "Person B", 31);
    avro_file_writer_append(writer, datum);
    avro_datum_decref(datum);

    datum = __make_object(schema, "Person C", 28);
    avro_file_writer_append(writer, datum);
    avro_datum_decref(datum);

    avro_file_writer_close(writer);
    return(0);
}

static int __test_read (const char *filename) {
    avro_file_reader_t reader;
    avro_datum_t field_datum;
    avro_datum_t datum;
    int32_t i32;
    char *p;

    if (avro_file_reader(filename, &reader))
        return(-1);

    while (!avro_file_reader_read(reader, NULL, &datum)) {
        avro_record_get(datum, "name", &field_datum);
        avro_string_get(field_datum, &p);
        printf("Name %s ", p);

        avro_record_get(datum, "age", &field_datum);
        avro_int32_get(field_datum, &i32);
        printf("Age %d\n", i32);
    }

    avro_file_reader_close(reader);
    return(0);
}

int main (int argc, char **argv) {
    const char *filename = "test-file.avro";
    avro_schema_t schema;

    schema = __make_schema();

    __test_write(filename, schema);
    __test_read(filename);

    avro_schema_decref(schema);

    return(0);
}

