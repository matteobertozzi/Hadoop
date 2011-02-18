/*
 * gcc -Wall test-schema.c -o test-schema -I/usr/include/ -L/usr/local/lib/ -lavro
 * LD_LIBRARY_PATH=/usr/local/lib ./test-schema
 */
#include <assert.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include <avro.h>

static avro_schema_t __schema_from_code (void) {
    avro_schema_t field_sub_schema;
    avro_schema_t field_schema;
    avro_schema_t schema;

    schema = avro_schema_record("recordName", "recordNS");

    field_schema = avro_schema_string();
    avro_schema_record_field_append(schema, "field1", field_schema);
    avro_schema_decref(field_schema);

    field_schema = avro_schema_long();
    avro_schema_record_field_append(schema, "field2", field_schema);
    avro_schema_decref(field_schema);

    field_sub_schema = avro_schema_string();
    field_schema = avro_schema_array(field_sub_schema);
    avro_schema_decref(field_sub_schema);
    avro_schema_record_field_append(schema, "field3", field_schema);
    avro_schema_decref(field_schema);

    field_sub_schema = avro_schema_int();
    field_schema = avro_schema_map(field_sub_schema);
    avro_schema_decref(field_sub_schema);
    avro_schema_record_field_append(schema, "field4", field_schema);
    avro_schema_decref(field_schema);

    return(schema);
}

int main (int argc, char **argv) {
    avro_schema_error_t error;
    avro_schema_t s1, s2;
    avro_writer_t writer;
    char json[1024];

    s1 = __schema_from_code();

    writer = avro_writer_memory(json, sizeof(json));
    avro_schema_to_json(s1, writer);
    avro_writer_free(writer);

    avro_schema_from_json(json, strlen(json), &s2, &error);
    assert(avro_schema_equal(s1, s2));

    printf("%s\n", json);

    avro_schema_decref(s1);
    avro_schema_decref(s2);

    return(0);
}

