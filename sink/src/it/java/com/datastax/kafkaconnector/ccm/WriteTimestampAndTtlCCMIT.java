/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.ccm;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
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
    conn.start(makeConnectorProperties("bigintcol=value.bigint, doublecol=value.double"));

    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("bigint", 1234567L).put("double", 42.0);

    SinkRecord record =
        new SinkRecord(
            "mytopic", 0, null, null, null, value, 1234L, 153000987L, TimestampType.CREATE_TIME);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
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
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.ttlcol"));

    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .field("ttlcol", Schema.INT64_SCHEMA)
            .build();
    Number ttlValue = 1_000_000L;
    Struct value =
        new Struct(schema)
            .put("bigint", 1234567L)
            .put("double", 42.0)
            .put("ttlcol", ttlValue.longValue());

    SinkRecord record =
        new SinkRecord(
            "mytopic", 0, null, null, null, value, 1234L, 153000987L, TimestampType.CREATE_TIME);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
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
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.ttlcol",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    Struct value =
        new Struct(schema).put("bigint", 1234567L).put("double", 42.0).put("ttlcol", ttlValue);

    SinkRecord record =
        new SinkRecord(
            "mytopic", 0, null, null, null, value, 1234L, 153000987L, TimestampType.CREATE_TIME);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, ttl(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertTtl(row.getInt(2), expectedTtlValue);
  }

  private static Stream<? extends Arguments> ttlColProvider() {
    Supplier<SchemaBuilder> schemaBuilder =
        () ->
            SchemaBuilder.struct()
                .name("Kafka")
                .field("bigint", Schema.INT64_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA);

    return Stream.of(
        Arguments.of(
            schemaBuilder.get().field("ttlcol", Schema.INT64_SCHEMA).build(), 1_000_000L, 1_000),
        Arguments.of(
            schemaBuilder.get().field("ttlcol", Schema.INT32_SCHEMA).build(), 1_000_000, 1_000),
        Arguments.of(
            schemaBuilder.get().field("ttlcol", Schema.INT16_SCHEMA).build(),
            (short) 1_000_000,
            (short) 1_000),
        Arguments.of(
            schemaBuilder.get().field("ttlcol", Schema.FLOAT32_SCHEMA).build(), 1_000_000F, 1_000),
        Arguments.of(
            schemaBuilder.get().field("ttlcol", Schema.FLOAT64_SCHEMA).build(), 1_000_000D, 1_000),
        Arguments.of(schemaBuilder.get().field("ttlcol", Schema.INT32_SCHEMA).build(), -1_000, 0));
  }

  @Test
  void should_extract_ttl_from_json_and_use_as_ttl_column() {
    // given
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.ttlcol",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 42.0, \"ttlcol\": 1000000}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
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
    conn.start(
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
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
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
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __ttl = value.double",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 1000000.0}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
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
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttl", keyspaceName, "types"), "100")));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 1000.0}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
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
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __timestamp = value.double",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
                "MILLISECONDS",
                String.format("topic.mytopic.%s.%s.timestampTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 1000.0}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
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
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __timestamp = value.timestamp"));

    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("double", Schema.FLOAT64_SCHEMA)
            .field("timestamp", Schema.INT64_SCHEMA)
            .build();
    Struct value =
        new Struct(schema).put("bigint", 1234567L).put("double", 42.0).put("timestamp", 12314L);

    SinkRecord record =
        new SinkRecord(
            "mytopic", 0, null, null, null, value, 1234L, 153000987L, TimestampType.CREATE_TIME);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
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
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __timestamp = value.timestampcol"));

    // when
    String json = "{\"bigint\": 1234567, \"double\": 42.0, \"timestampcol\": 1000}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
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
    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, doublecol=value.double, __timestamp = value.timestampcol",
            ImmutableMap.of(
                String.format("topic.mytopic.%s.%s.timestampTimeUnit", keyspaceName, "types"),
                "MILLISECONDS")));

    Struct value =
        new Struct(schema)
            .put("bigint", 1234567L)
            .put("double", 42.0)
            .put("timestampcol", timestampValue);

    SinkRecord record =
        new SinkRecord(
            "mytopic", 0, null, null, null, value, 1234L, 153000987L, TimestampType.CREATE_TIME);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results =
        session.execute("SELECT bigintcol, doublecol, writetime(doublecol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getDouble("doublecol")).isEqualTo(42.0);
    assertThat(row.getLong(2)).isEqualTo(expectedTimestampValue.longValue());
  }

  private static Stream<? extends Arguments> timestampColProvider() {
    Supplier<SchemaBuilder> schemaBuilder =
        () ->
            SchemaBuilder.struct()
                .name("Kafka")
                .field("bigint", Schema.INT64_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA);

    return Stream.of(
        Arguments.of(
            schemaBuilder.get().field("timestampcol", Schema.INT64_SCHEMA).build(),
            1000L,
            1_000_000L),
        Arguments.of(
            schemaBuilder.get().field("timestampcol", Schema.INT32_SCHEMA).build(),
            1000,
            1_000_000L),
        Arguments.of(
            schemaBuilder.get().field("timestampcol", Schema.INT16_SCHEMA).build(),
            (short) 1000,
            (short) 1_000_000L),
        Arguments.of(
            schemaBuilder.get().field("timestampcol", Schema.FLOAT32_SCHEMA).build(),
            1000F,
            1_000_000L),
        Arguments.of(
            schemaBuilder.get().field("timestampcol", Schema.FLOAT64_SCHEMA).build(),
            1000D,
            1_000_000L),
        Arguments.of(
            schemaBuilder.get().field("timestampcol", Schema.INT32_SCHEMA).build(),
            -1000,
            -1_000_000L));
  }
}
