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
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.sink.pulsar.BytesSink;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
class NowFunctionCCMIT extends EndToEndCCMITBase<byte[]> {

  NowFunctionCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session, new BytesSink(true));
  }

  @Test
  void should_insert_value_using_now_function_json() {
    // given
    initConnectorAndTask(makeConnectorProperties("bigintcol=value.bigint, loaded_at=now()"));

    // when
    String json = "{\"bigint\": 1234567}";
    Record<byte[]> record = mockRecord("mytopic", null, json.getBytes(), 1234);
    sendRecord(record);

    // then
    List<Row> results = session.execute("SELECT bigintcol, loaded_at FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.get("loaded_at", TypeCodecs.TIMEUUID)).isLessThanOrEqualTo(Uuids.timeBased());
  }

  @Test
  void should_insert_value_using_now_function_for_two_dse_columns() {
    // given
    initConnectorAndTask(
        makeConnectorProperties("bigintcol=value.bigint, loaded_at=now(), loaded_at2=now()"));

    // when
    String json = "{\"bigint\": 1234567}";
    Record<byte[]> record = mockRecord("mytopic", null, json.getBytes(), 1234);
    sendRecord(record);

    // then
    List<Row> results = session.execute("SELECT bigintcol, loaded_at, loaded_at2 FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    UUID loadedAt = row.get("loaded_at", TypeCodecs.TIMEUUID);
    UUID loadedAt2 = row.get("loaded_at2", TypeCodecs.TIMEUUID);
    // columns inserted using now() should have different TIMEUUID values
    assertThat(loadedAt).isNotEqualTo(loadedAt2);
  }

  @Test
  void should_insert_value_using_now_function_avro() {
    initConnectorAndTask(
        makeConnectorProperties("bigintcol=value.bigint, loaded_at=now(), loaded_at2=now()"));

    Schema schema = SchemaBuilder.record("pulsar").fields().requiredLong("bigint").endRecord();
    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", 1234567L);

    Record<byte[]> record =
        mockRecord("mytopic", null, wornBytes(value), 1234L, 153000987L, Collections.emptyMap());
    sendRecord(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol, loaded_at, loaded_at2 FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    UUID loadedAt = row.get("loaded_at", TypeCodecs.TIMEUUID);
    UUID loadedAt2 = row.get("loaded_at2", TypeCodecs.TIMEUUID);
    assertThat(loadedAt).isNotEqualTo(loadedAt2);
  }

  @Test
  void delete_simple_key_json_when_using_now_function_in_mapping() {
    // First insert a row...
    session.execute(
        "INSERT INTO pk_value_with_timeuuid (my_pk, my_value, loaded_at) VALUES (1234567, true, now())");
    List<Row> results = session.execute("SELECT * FROM pk_value_with_timeuuid").all();
    assertThat(results.size()).isEqualTo(1);

    // now() function call is ignored when null value is send - DELETE will be performed
    initConnectorAndTask(
        makeConnectorProperties(
            "my_pk=value.my_pk, my_value=value.my_value, loaded_at=now()",
            "pk_value_with_timeuuid",
            null));

    // Set up records for "mytopic"
    String json = "{\"my_pk\": 1234567, \"my_value\": null}";
    Record<byte[]> record = mockRecord("mytopic", null, json.getBytes(), 1234);

    sendRecord(record);

    // Verify that the record was deleted from the database.
    results = session.execute("SELECT * FROM pk_value_with_timeuuid").all();
    assertThat(results.size()).isEqualTo(0);
  }
}
