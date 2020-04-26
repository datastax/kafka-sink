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
package com.datastax.oss.kafka.sink.ccm;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
class DeleteCCMIT extends EndToEndCCMITBase {
  DeleteCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
  }

  @Test
  void delete_simple_key_value_null() {
    // First insert a row...
    session.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    conn.start(
        makeConnectorProperties("my_pk=key.my_pk, my_value=value.my_value", "pk_value", null));

    // Set up records for "mytopic"
    Schema keySchema =
        SchemaBuilder.struct().name("Kafka").field("my_pk", Schema.INT64_SCHEMA).build();
    Struct key = new Struct(keySchema).put("my_pk", 1234567L);
    SinkRecord record = new SinkRecord("mytopic", 0, null, key, null, null, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_simple_key_value_null_json() {
    // First insert a row...
    session.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    conn.start(
        makeConnectorProperties("my_pk=key.my_pk, my_value=value.my_value", "pk_value", null));

    // Set up records for "mytopic"
    String key = "{\"my_pk\": 1234567}";

    SinkRecord record = new SinkRecord("mytopic", 0, null, key, null, null, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void insert_with_nulls_when_delete_disabled() {
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int",
            "small_simple",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.small_simple.deletesEnabled", keyspaceName),
                "false")));

    // Set up records for "mytopic"
    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("bigint", 1234567L);
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was inserted into DSE with null non-pk values.
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

    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int",
            "small_compound",
            null));

    // Set up records for "mytopic"
    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("bigint", 1234567L).put("boolean", true);
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
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

    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int",
            "small_compound",
            null));

    // Set up records for "mytopic"
    String json = "{\"bigint\": 1234567, \"boolean\": true, \"int\": null}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_compound_key_value_null() {
    // First insert a row...
    session.execute(
        "INSERT INTO small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)");
    List<Row> results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(1);

    conn.start(
        makeConnectorProperties(
            "bigintcol=key.bigint, booleancol=key.boolean, intcol=value.int",
            "small_compound",
            null));

    // Set up records for "mytopic"
    Schema keySchema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .build();
    Struct key = new Struct(keySchema).put("bigint", 1234567L).put("boolean", true);
    SinkRecord record = new SinkRecord("mytopic", 0, null, key, null, null, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
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

    conn.start(
        makeConnectorProperties(
            "bigintcol=key.bigint, booleancol=key.boolean, intcol=value.int",
            "small_compound",
            null));

    // Set up records for "mytopic"
    String key = "{\"bigint\": 1234567, \"boolean\": true}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, key, null, null, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_simple_key() {
    // First insert a row...
    session.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    conn.start(
        makeConnectorProperties("my_pk=value.my_pk, my_value=value.my_value", "pk_value", null));

    // Set up records for "mytopic"
    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("my_pk", Schema.INT64_SCHEMA)
            .field("my_value", Schema.BOOLEAN_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("my_pk", 1234567L);
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_simple_key_json() {
    // First insert a row...
    session.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    conn.start(
        makeConnectorProperties("my_pk=value.my_pk, my_value=value.my_value", "pk_value", null));

    // Set up records for "mytopic"
    String json = "{\"my_pk\": 1234567, \"my_value\": null}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }
}
