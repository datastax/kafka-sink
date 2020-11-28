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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.sink.pulsar.BytesSink;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("medium")
public class WriteTimestampAndTtlCCMIT extends EndToEndCCMITBase<byte[]> {

  WriteTimestampAndTtlCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session, new BytesSink(true));
  }

  @Test
  void timestamp() throws Exception {
    initConnectorAndTask(makeConnectorProperties("bigintcol=value.bigint, doublecol=value.double"));

    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .requiredLong("bigint")
            .requiredDouble("double")
            .endRecord();
    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", 1234567L);
    value.put("double", 42.0);

    Record<byte[]> record =
        mockRecord("mytopic", null, wornBytes(value), 1234l, 153000987L, Collections.emptyMap());
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, writetime(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertThat(row.getLong(2)).isEqualTo(153000987000L);
  }

  /** Test for KAF-107. */
  @Test
  void should_insert_record_with_ttl_provided_via_mapping() throws Exception {
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.ttlcol"));

    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .requiredLong("bigint")
            .requiredDouble("double")
            .requiredLong("ttlcol")
            .endRecord();
    Number ttlValue = 1_000_000L;
    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", 1234567L);
    value.put("double", 42.0);
    value.put("ttlcol", ttlValue.longValue());

    Record<byte[]> record =
        mockRecord("mytopic", null, wornBytes(value), 1234, 153000987L, Collections.emptyMap());
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, ttl(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertTtl(row.getInt(2), ttlValue);
  }

  /** Test for KAF-107. */
  @ParameterizedTest(name = "[{index}] schema={0}, ttlValue={1}, expectedTtlValue={2}")
  @MethodSource("ttlColProvider")
  void should_insert_record_with_ttl_provided_via_mapping_and_validate_ttl_of_table(
      Schema schema, Number ttlValue, Number expectedTtlValue) throws Exception {
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.ttlcol",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", 1234567L);
    value.put("double", 42.0);
    value.put("ttlcol", ttlValue);

    Record<byte[]> record =
        mockRecord("mytopic", null, wornBytes(value), 1234L, 153000987L, Collections.emptyMap());
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, ttl(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertTtl(row.getInt(2), expectedTtlValue);
  }

  private static Stream<? extends Arguments> ttlColProvider() {
    Supplier<SchemaBuilder.FieldAssembler> schemaBuilder =
        () ->
            SchemaBuilder.record("pulsar").fields().requiredLong("bigint").requiredDouble("double");

    return Stream.of(
        Arguments.of(schemaBuilder.get().requiredLong("ttlcol").endRecord(), 1_000_000L, 1_000),
        Arguments.of(schemaBuilder.get().requiredInt("ttlcol").endRecord(), 1_000_000, 1_000),
        Arguments.of(schemaBuilder.get().requiredInt("ttlcol").endRecord(), 1_000_000, 1_000),
        Arguments.of(schemaBuilder.get().requiredFloat("ttlcol").endRecord(), 1_000_000F, 1_000),
        Arguments.of(schemaBuilder.get().requiredDouble("ttlcol").endRecord(), 1_000_000D, 1_000),
        Arguments.of(schemaBuilder.get().requiredInt("ttlcol").endRecord(), -1_000, 0));
  }

  @Test
  void should_extract_ttl_from_json_and_use_as_ttl_column() throws Exception {
    // given
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.ttlcol",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 42.0, \"ttlcol\": 1000000}";
    Record<byte[]> record = mockRecord("mytopic", null, json.getBytes(), 1234);
    runTaskWithRecords(record);

    // then
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, ttl(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertTtl(row.getInt(2), 1000);
  }

  @Test
  void should_extract_ttl_and_timestamp_from_json_and_use_as_ttl_and_timestamp_columns()
      throws Exception {
    // given
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.ttlcol, __timestamp = value.timestampcol",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS",
                String.format("topic.mytopic.%s.%s.timestampTimeUnit", keyspaceName, "types"),
                "MICROSECONDS")));

    // when
    String json =
        "{\"bigint\": 1234567, \"double\": 42.0, \"ttlcol\": 1000000, \"timestampcol\": 1000}";
    Record<byte[]> record = mockRecord("mytopic", null, json.getBytes(), 1234);
    runTaskWithRecords(record);

    // then
    List<Row> results =
        session
            .execute("SELECT bigintcol, doublecol, ttl(doublecol), writetime(doublecol) FROM types")
            .all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertTtl(row.getInt(2), 1000);
    assertThat(row.getLong(3)).isEqualTo(1000L);
  }

  @Test
  void should_extract_ttl_from_json_and_use_existing_column_as_ttl() throws Exception {
    // given
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.double",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 1000000.0}";
    Record<byte[]> record = mockRecord("mytopic", null, json.getBytes(), 1234);
    runTaskWithRecords(record);

    // then
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, ttl(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(1000000.0);
    assertTtl(row.getInt(2), 1000);
  }

  @Test
  void should_use_ttl_from_config_and_use_as_ttl() throws Exception {
    // given
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttl", keyspaceName, "types"), "100")));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 1000.0}";
    Record<byte[]> record = mockRecord("mytopic", null, json.getBytes(), 1234);
    runTaskWithRecords(record);

    // then
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, ttl(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(1000.0);
    assertTtl(row.getInt(2), 100);
  }

  /** Test for KAF-46. */
  @Test
  void should_extract_timestamp_from_json_and_use_existing_column_as_timestamp() throws Exception {
    // given
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __timestamp = value.double",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS",
                String.format("topic.mytopic.%s.%s.timestampTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 1000.0}";
    Record<byte[]> record = mockRecord("mytopic", null, json.getBytes(), 1234);
    runTaskWithRecords(record);

    // then
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, writetime(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(1000.0);
    assertThat(row.getLong(2)).isEqualTo(1_000_000L);
  }

  /** Test for KAF-46. */
  @Test
  void should_insert_record_with_timestamp_provided_via_mapping() throws Exception {
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __timestamp = value.timestamp"));

    Schema schema =
        SchemaBuilder.record("pulsar")
            .fields()
            .requiredLong("bigint")
            .requiredDouble("double")
            .requiredLong("timestamp")
            .endRecord();
    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", 1234567L);
    value.put("double", 42.0);
    value.put("timestamp", 12314L);

    Record<byte[]> record =
        mockRecord("mytopic", null, wornBytes(value), 1234, 153000987L, Collections.emptyMap());
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, writetime(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertThat(row.getLong(2)).isEqualTo(12314L);
  }

  /** Test for KAF-46. */
  @Test
  void should_extract_write_timestamp_from_json_and_use_as_write_time_column() throws Exception {
    // given
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __timestamp = value.timestampcol"));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 42.0, \"timestampcol\": 1000}";
    Record<byte[]> record = mockRecord("mytopic", null, json.getBytes(), 1234);
    runTaskWithRecords(record);

    // then
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, writetime(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertThat(row.getLong(2)).isEqualTo(1000L);
  }

  /** Test for KAF-46. */
  @ParameterizedTest(name = "[{index}] schema={0}, timestampValue={1}, expectedTimestampValue={2}")
  @MethodSource("timestampColProvider")
  void should_insert_record_with_timestamp_provided_via_mapping_and_validate_timestamp_of_table(
      Schema schema, Number timestampValue, Number expectedTimestampValue) throws Exception {
    initConnectorAndTask(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __timestamp = value.timestampcol",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.timestampTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    GenericRecord value = new GenericData.Record(schema);
    value.put("bigint", 1234567L);
    value.put("double", 42.0);
    value.put("timestampcol", timestampValue);

    Record<byte[]> record = mockRecord("mytopic", null, wornBytes(value), 1234L, 153000987L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, writetime(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertThat(row.getLong(2)).isEqualTo(expectedTimestampValue.longValue());
  }

  private static Stream<? extends Arguments> timestampColProvider() {
    Supplier<SchemaBuilder.FieldAssembler> schemaBuilder =
        () ->
            SchemaBuilder.record("pulsar").fields().requiredLong("bigint").requiredDouble("double");

    return Stream.of(
        Arguments.of(
            schemaBuilder.get().requiredLong("timestampcol").endRecord(), 1000L, 1_000_000L),
        Arguments.of(schemaBuilder.get().requiredInt("timestampcol").endRecord(), 1000, 1_000_000L),
        Arguments.of(schemaBuilder.get().requiredInt("timestampcol").endRecord(), 1000, 1_000_000L),
        Arguments.of(
            schemaBuilder.get().requiredFloat("timestampcol").endRecord(), 1000F, 1_000_000L),
        Arguments.of(
            schemaBuilder.get().requiredDouble("timestampcol").endRecord(), 1000D, 1_000_000L),
        Arguments.of(
            schemaBuilder.get().requiredInt("timestampcol").endRecord(), -1000, -1_000_000L));
  }
}
