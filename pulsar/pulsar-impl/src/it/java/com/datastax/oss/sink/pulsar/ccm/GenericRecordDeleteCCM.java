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
package com.datastax.oss.sink.pulsar.ccm;

import static com.datastax.oss.sink.pulsar.TestUtil.*;
import static org.assertj.core.api.Assertions.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.io.core.Sink;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
public abstract class GenericRecordDeleteCCM
    extends EndToEndCCMITBase<org.apache.pulsar.client.api.schema.GenericRecord> {
  protected GenericRecordDeleteCCM(
      CCMCluster ccm,
      CqlSession session,
      Sink<org.apache.pulsar.client.api.schema.GenericRecord> sink) {
    super(ccm, session, sink);
  }

  @Test
  void delete_simple_key_value_null() {
    // First insert a row...
    session.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    initConnectorAndTask(
        makeConnectorProperties("my_pk=key, my_value=value.my_value", "pk_value", null));

    // Set up records for "mytopic"

    sendRecord(mockRecord("mytopic", "1234567", null, 1234));

    // Verify that the record was deleted from the database.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_simple_key_value_null_json() {
    // First insert a row...
    session.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    initConnectorAndTask(
        makeConnectorProperties("my_pk=key.my_pk, my_value=value.my_value", "pk_value", null));

    // Set up records for "mytopic"
    String key = "{\"my_pk\": 1234567}";
    sendRecord(mockRecord("mytopic", key, null, 1234));

    // Verify that the record was deleted from the database.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void insert_with_nulls_when_delete_disabled() {
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int",
            "small_simple",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.small_simple.deletesEnabled", keyspaceName),
                "false")));

    // Set up records for "mytopic"
    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .requiredLong("bigint")
            .optionalBoolean("boolean")
            .optionalInt("int")
            .endRecord();
    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", 1234567L);

    sendRecord(mockRecord("mytopic", null, pulsarGenericAvroRecord(value), 1234));

    // Verify that the record was inserted into the database with null non-pk values.
    List<Row> results = session.execute("SELECT * FROM small_simple").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.get("bigintcol", GenericType.LONG)).isEqualTo(1234567L);
    assertThat(row.get("booleancol", GenericType.BOOLEAN)).isNull();
    assertThat(row.get("intcol", GenericType.INTEGER)).isNull();
  }

  @Test
  void delete_compound_key() {
    // First insert a row...
    session.execute(
        "INSERT INTO small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)");
    List<Row> results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(1);

    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int",
            "small_compound",
            null));

    // Set up records for "mytopic"
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

    sendRecord(mockRecord("mytopic", null, pulsarGenericAvroRecord(value), 1234));

    // Verify that the record was deleted from the database.
    results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_compound_key_json() {
    // First insert a row...
    session.execute(
        "INSERT INTO small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)");
    List<Row> results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(1);

    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int",
            "small_compound",
            null));

    // Set up records for "mytopic"
    String json = "{\"bigint\": 1234567, \"boolean\": true, \"int\": null}";

    sendRecord(mockRecord("mytopic", null, pulsarGenericJsonRecord(json), 1234));

    // Verify that the record was deleted from the database.
    results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_compound_key_value_null_json() {
    // First insert a row...
    session.execute(
        "INSERT INTO small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)");
    List<Row> results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(1);

    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=key.bigint, booleancol=key.boolean, intcol=value.int",
            "small_compound",
            null));

    String key = "{\"bigint\":1234567,\"boolean\":true}";

    sendRecord(mockRecord("mytopic", key, null, 1234));

    // Verify that the record was deleted from the database.
    results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_simple_key() {
    // First insert a row...
    session.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    initConnectorAndTask(
        makeConnectorProperties("my_pk=value.my_pk, my_value=value.my_value", "pk_value", null));

    // Set up records for "mytopic"
    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .requiredLong("my_pk")
            .optionalBoolean("my_value")
            .endRecord();
    GenericRecord value = new GenericData.Record(schema);
    value.put("my_pk", 1234567L);

    sendRecord(mockRecord("mytopic", null, pulsarGenericAvroRecord(value), 1234));

    // Verify that the record was deleted from the database.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_simple_key_json() {
    // First insert a row...
    session.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    initConnectorAndTask(
        makeConnectorProperties("my_pk=value.my_pk, my_value=value.my_value", "pk_value", null));

    // Set up records for "mytopic"
    String json = "{\"my_pk\": 1234567, \"my_value\": null}";

    sendRecord(mockRecord("mytopic", null, pulsarGenericJsonRecord(json), 1234));

    // Verify that the record was deleted from the database.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }
}
