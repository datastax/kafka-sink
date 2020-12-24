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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.sink.pulsar.GenericRecordImpl;
import com.datastax.oss.sink.pulsar.PulsarRecordImpl;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("medium")
public class WriteTimestampAndTtlCCMIT extends EndToEndCCMITBase {

  WriteTimestampAndTtlCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
  }

  @Test
  void timestamp() {
    taskConfigs.add(makeConnectorProperties("bigintcol=value.bigint, doublecol=value.double"));

    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("bigint").type(SchemaType.INT64);
    builder.field("double").type(SchemaType.DOUBLE);

    Schema schema = org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.AVRO));

    GenericRecordImpl value = new GenericRecordImpl().put("bigint", 1234567L).put("double", 42.0);

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", null, value, schema, 153000987L);
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
  void should_insert_record_with_ttl_provided_via_mapping() {
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.ttlcol"));

    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("bigint").type(SchemaType.INT64);
    builder.field("double").type(SchemaType.DOUBLE);
    builder.field("ttlcol").type(SchemaType.INT64);

    Schema schema = org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.AVRO));

    Number ttlValue = 1_000_000L;
    GenericRecordImpl value =
        new GenericRecordImpl()
            .put("bigint", 1234567L)
            .put("double", 42.0)
            .put("ttlcol", ttlValue.longValue());

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", null, value, schema, 153000987L);
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
      Schema schema, Number ttlValue, Number expectedTtlValue) {
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.ttlcol",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    GenericRecordImpl value =
        new GenericRecordImpl().put("bigint", 1234567L).put("double", 42.0).put("ttlcol", ttlValue);

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", null, value, schema, 153000987L);
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

  private static Schema schemaFinalize(
      Supplier<RecordSchemaBuilder> builderCreator, Consumer<RecordSchemaBuilder> schemaFinalizer) {
    RecordSchemaBuilder builder = builderCreator.get();
    schemaFinalizer.accept(builder);
    return Schema.generic(builder.build(SchemaType.AVRO));
  }

  private static Stream<? extends Arguments> ttlColProvider() {
    Supplier<RecordSchemaBuilder> schemaBuilder =
        () -> {
          RecordSchemaBuilder builder =
              org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
          builder.field("bigint").type(SchemaType.INT64);
          builder.field("double").type(SchemaType.DOUBLE);
          return builder;
        };
    return Stream.of(
        Arguments.of(
            schemaFinalize(
                schemaBuilder,
                sb -> {
                  sb.field("ttlcol").type(SchemaType.INT64);
                }),
            1_000_000L,
            1_000),
        Arguments.of(
            schemaFinalize(
                schemaBuilder,
                sb -> {
                  sb.field("ttlcol").type(SchemaType.INT32);
                }),
            1_000_000,
            1_000),
        Arguments.of(
            schemaFinalize(
                schemaBuilder,
                sb -> {
                  sb.field("ttlcol").type(SchemaType.FLOAT);
                }),
            1_000_000F,
            1_000),
        Arguments.of(
            schemaFinalize(
                schemaBuilder,
                sb -> {
                  sb.field("ttlcol").type(SchemaType.DOUBLE);
                }),
            1_000_000D,
            1_000),
        Arguments.of(
            schemaFinalize(
                schemaBuilder,
                sb -> {
                  sb.field("ttlcol").type(SchemaType.INT32);
                }),
            -1_000,
            0));
  }

  @Test
  void should_extract_ttl_from_json_and_use_as_ttl_column() {
    // given
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=key.bigint, doublecol=key.double, __ttl = key.ttlcol",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    // difference with Kafka Sink: we are using the Key, as we can decode raw JSON from the string
    // when
    String json = "{\"bigint\": 1234567, \"double\": 42.0, \"ttlcol\": 1000000}";
    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", json, new GenericRecordImpl(), recordType);
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
  void should_extract_ttl_and_timestamp_from_json_and_use_as_ttl_and_timestamp_columns() {
    // given
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=key.bigint, doublecol=key.double, __ttl = key.ttlcol, __timestamp = key.timestampcol",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS",
                String.format("topic.mytopic.%s.%s.timestampTimeUnit", keyspaceName, "types"),
                "MICROSECONDS")));

    // difference with Kafka Sink: we are using the Key, as we can decode raw JSON from the string
    // when
    String json =
        "{\"bigint\": 1234567, \"double\": 42.0, \"ttlcol\": 1000000, \"timestampcol\": 1000}";
    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", json, new GenericRecordImpl(), recordType);
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
  void should_extract_ttl_from_json_and_use_existing_column_as_ttl() {
    // given
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=key.bigint, doublecol=key.double, __ttl = key.double",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    // difference with Kafka Sink: we are using the Key, as we can decode raw JSON from the string
    // when
    String json = "{\"bigint\": 1234567, \"double\": 1000000.0}";
    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", json, new GenericRecordImpl(), recordType);
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
  void should_use_ttl_from_config_and_use_as_ttl() {
    // given
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttl", keyspaceName, "types"), "100")));

    // when
    GenericRecordImpl value = new GenericRecordImpl().put("bigint", 1234567L).put("double", 1000.0);
    PulsarRecordImpl record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, value, recordType);
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
  void should_extract_timestamp_from_json_and_use_existing_column_as_timestamp() {
    // given
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=key.bigint, doublecol=key.double, __timestamp = key.double",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS",
                String.format("topic.mytopic.%s.%s.timestampTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    // difference with Kafka Sink: we are using the Key, as we can decode raw JSON from the string
    // when
    String json = "{\"bigint\": 1234567, \"double\": 1000.0}";
    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", json, new GenericRecordImpl(), recordType);
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
  void should_insert_record_with_timestamp_provided_via_mapping() {
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __timestamp = value.timestamp"));

    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("bigint").type(SchemaType.INT64);
    builder.field("double").type(SchemaType.DOUBLE);
    builder.field("timestamp").type(SchemaType.INT64);

    Schema schema = org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.AVRO));

    GenericRecordImpl value =
        new GenericRecordImpl()
            .put("bigint", 1234567L)
            .put("double", 42.0)
            .put("timestamp", 12314L);

    PulsarRecordImpl record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, value, schema);
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
  void should_extract_write_timestamp_from_json_and_use_as_write_time_column() {
    // given
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=key.bigint, doublecol=key.double, __timestamp = key.timestampcol"));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 42.0, \"timestampcol\": 1000}";
    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic", json, new GenericRecordImpl(), recordType);
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
      Schema schema, Number timestampValue, Number expectedTimestampValue) {
    taskConfigs.add(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __timestamp = value.timestampcol",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.timestampTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    GenericRecordImpl value =
        new GenericRecordImpl()
            .put("bigint", 1234567L)
            .put("double", 42.0)
            .put("timestampcol", timestampValue);

    PulsarRecordImpl record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, value, schema);
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
    Supplier<RecordSchemaBuilder> schemaBuilder =
        () -> {
          RecordSchemaBuilder builder =
              org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
          builder.field("bigint").type(SchemaType.INT64);
          builder.field("double").type(SchemaType.DOUBLE);
          return builder;
        };

    return Stream.of(
        Arguments.of(
            schemaFinalize(
                schemaBuilder,
                sb -> {
                  sb.field("timestampcol").type(SchemaType.INT64);
                }),
            1000L,
            1_000_000L),
        Arguments.of(
            schemaFinalize(
                schemaBuilder,
                sb -> {
                  sb.field("timestampcol").type(SchemaType.INT32);
                }),
            1000,
            1_000_000L),
        Arguments.of(
            schemaFinalize(
                schemaBuilder,
                sb -> {
                  sb.field("timestampcol").type(SchemaType.FLOAT);
                }),
            1000F,
            1_000_000L),
        Arguments.of(
            schemaFinalize(
                schemaBuilder,
                sb -> {
                  sb.field("timestampcol").type(SchemaType.DOUBLE);
                }),
            1000D,
            1_000_000L),
        Arguments.of(
            schemaFinalize(
                schemaBuilder,
                sb -> {
                  sb.field("timestampcol").type(SchemaType.INT32);
                }),
            -1000,
            -1_000_000L));
  }
}
