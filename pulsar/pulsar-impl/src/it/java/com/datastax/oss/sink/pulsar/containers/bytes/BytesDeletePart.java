/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.sink.pulsar.containers.bytes;

import static com.datastax.oss.sink.pulsar.TestUtil.*;
import static org.assertj.core.api.Assertions.*;

import com.datastax.driver.core.Row;
import com.datastax.oss.sink.util.Tuple2;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("containers")
public class BytesDeletePart extends BytesPart {

  @Override
  protected String basicName() {
    return "bytes-delete";
  }

  @BeforeEach
  public void teardown() {
    cassandraSession.execute("truncate pk_value");
    cassandraSession.execute("truncate small_simple");
    cassandraSession.execute("truncate small_compound");
  }

  @Test
  void delete_simple_key_value_null() throws PulsarAdminException, PulsarClientException {
    String name = name("dskvn");

    // First insert a row...
    cassandraSession.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = cassandraSession.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    regSink(name, "pk_value", "my_pk=key, my_value=value.my_value");

    send(name, "1234567", null);
    unregisterSink(name);

    // Verify that the record was deleted from the database.
    results = cassandraSession.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_simple_key_value_null_json() throws PulsarAdminException, PulsarClientException {
    String name = name("dskvnj");
    // First insert a row...
    cassandraSession.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = cassandraSession.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    regSink(name, "pk_value", "my_pk=key.my_pk, my_value=value.my_value");

    send(name, "{\"my_pk\": 1234567}", null);
    unregisterSink(name);

    // Verify that the record was deleted from the database.
    results = cassandraSession.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void insert_with_nulls_when_delete_disabled() throws PulsarAdminException, PulsarClientException {
    String name = name("iwnwdd");
    regSink(
        name,
        "small_simple",
        "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int",
        Tuple2.of("topic." + name + ".testks.small_simple.deletesEnabled", "false"));

    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .requiredLong("bigint")
            .optionalBoolean("boolean")
            .optionalInt("int")
            .endRecord();
    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", 1234567L);

    send(name, null, wornBytes(value));
    unregisterSink(name);

    // Verify that the record was inserted into the database with null non-pk values.
    List<Row> results = cassandraSession.execute("SELECT * FROM small_simple").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.get("booleancol", Boolean.class)).isNull();
    assertThat(row.get("intcol", Integer.class)).isNull();
  }

  @Test
  void delete_compound_key() throws PulsarAdminException, PulsarClientException {
    String name = name("dck");
    // First insert a row...
    cassandraSession.execute(
        "INSERT INTO small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)");
    List<Row> results = cassandraSession.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(1);

    regSink(
        name,
        "small_compound",
        "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int");

    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .requiredLong("bigint")
            .optionalBoolean("boolean")
            .optionalInt("int")
            .endRecord();
    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", 1234567L);
    value.put("boolean", true);

    send(name, null, wornBytes(value));
    unregisterSink(name);

    // Verify that the record was deleted from the database.
    results = cassandraSession.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_compound_key_json() throws PulsarAdminException, PulsarClientException {
    String name = name("dckj");
    // First insert a row...
    cassandraSession.execute(
        "INSERT INTO small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)");
    List<Row> results = cassandraSession.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(1);

    regSink(
        name,
        "small_compound",
        "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int");

    String json = "{\"bigint\": 1234567, \"boolean\": true, \"int\": null}";
    send(name, null, json.getBytes());
    unregisterSink(name);

    // Verify that the record was deleted from the database.
    results = cassandraSession.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_compound_key_value_null_json() throws PulsarAdminException, PulsarClientException {
    String name = name("dckvnj");
    // First insert a row...
    cassandraSession.execute(
        "INSERT INTO small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)");
    List<Row> results = cassandraSession.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(1);

    regSink(
        name, "small_compound", "bigintcol=key.bigint, booleancol=key.boolean, intcol=value.int");

    String key = "{\"bigint\":1234567,\"boolean\":true}";
    send(name, key, null);
    unregisterSink(name);

    // Verify that the record was deleted from the database.
    results = cassandraSession.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_simple_key() throws PulsarAdminException, PulsarClientException {
    String name = name("dsk");
    // First insert a row...
    cassandraSession.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = cassandraSession.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    regSink(name, "pk_value", "my_pk=value.my_pk, my_value=value.my_value");

    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .requiredLong("my_pk")
            .optionalBoolean("my_value")
            .endRecord();
    GenericRecord value = new GenericData.Record(schema);
    value.put("my_pk", 1234567L);
    send(name, null, wornBytes(value));
    unregisterSink(name);

    // Verify that the record was deleted from the database.
    results = cassandraSession.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_simple_key_json() throws PulsarAdminException, PulsarClientException {
    String name = name("dskj");
    // First insert a row...
    cassandraSession.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = cassandraSession.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    regSink(name, "pk_value", "my_pk=value.my_pk, my_value=value.my_value");

    String json = "{\"my_pk\": 1234567, \"my_value\": null}";
    send(name, null, json.getBytes());
    unregisterSink(name);

    // Verify that the record was deleted from the database.
    results = cassandraSession.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }
}
