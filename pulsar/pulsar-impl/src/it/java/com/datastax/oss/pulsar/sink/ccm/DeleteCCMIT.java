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
package com.datastax.oss.pulsar.sink.ccm;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.sink.pulsar.GenericRecordImpl;
import com.datastax.oss.sink.pulsar.PulsarRecordImpl;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
class DeleteCCMIT extends EndToEndCCMITBase {
  DeleteCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
  }

  @Test
  void delete_simple_key() {
    // First insert a row...
    session.execute("INSERT INTO pk_value (my_pk, my_value) VALUES (1234567, true)");
    List<Row> results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(1);

    taskConfigs.add(
        makeConnectorProperties("my_pk=key, my_value=value.my_value", "pk_value", null));

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic",
            "1234567",
            new GenericRecordImpl(),
            recordType);

    runTaskWithRecords(record);

    // Verify that the record was deleted from the database.
    results = session.execute("SELECT * FROM pk_value").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void insert_with_nulls_when_delete_disabled() {
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int",
            "small_simple",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.small_simple.deletesEnabled", keyspaceName),
                "false")));

    // Set up records for "mytopic"

    GenericRecordImpl value =
        new GenericRecordImpl().put("bigint", 1234567L).put("int", null).put("boolean", null);
    PulsarRecordImpl record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, value, recordType);

    runTaskWithRecords(record);

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

    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=value.bigint, booleancol=value.boolean, intcol=value.int",
            "small_compound",
            null));

    GenericRecordImpl value =
        new GenericRecordImpl().put("bigint", 1234567L).put("boolean", true).put("int", null);
    PulsarRecordImpl record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, value, recordType);

    runTaskWithRecords(record);

    // Verify that the record was deleted from the database.
    results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(0);
  }

  @Test
  void delete_compound_decode_key_json() {
    // First insert a row...
    session.execute(
        "INSERT INTO small_compound (bigintcol, booleancol, intcol) VALUES (1234567, true, 42)");
    List<Row> results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(1);

    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=key.bigint, booleancol=key.boolean, intcol=key.int",
            "small_compound",
            null));

    // Set up records for "mytopic", the key contains a json value
    String json = "{\"bigint\": 1234567, \"boolean\": true, \"int\": null}";

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", json, new GenericRecordImpl(), recordType);

    runTaskWithRecords(record);

    // Verify that the record was deleted from the database.
    results = session.execute("SELECT * FROM small_compound").all();
    assertThat(results.size()).isEqualTo(0);
  }
}
