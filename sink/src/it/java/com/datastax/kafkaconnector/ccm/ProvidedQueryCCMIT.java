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

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.List;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
public class ProvidedQueryCCMIT extends EndToEndCCMITBase {
  private AttachmentPoint attachmentPoint;

  public ProvidedQueryCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
    attachmentPoint =
        new AttachmentPoint() {
          @NotNull
          @Override
          public ProtocolVersion getProtocolVersion() {
            return session.getContext().getProtocolVersion();
          }

          @NotNull
          @Override
          public CodecRegistry getCodecRegistry() {
            return session.getContext().getCodecRegistry();
          }
        };
  }

  @Test
  void should_insert_json_using_query_parameter() {
    ImmutableMap<String, String> extras =
        ImmutableMap.of(
            queryParameter(),
            String.format(
                "INSERT INTO %s.types (bigintCol, intCol) VALUES (:bigintcol, :intcol)",
                keyspaceName));

    conn.start(makeConnectorProperties("bigintcol=value.bigint, intcol=value.int", extras));

    String value = "{\"bigint\": 1234, \"int\": 10000}";

    Long recordTimestamp = 123456L;
    SinkRecord record =
        new SinkRecord(
            "mytopic",
            0,
            null,
            null,
            null,
            value,
            1234L,
            recordTimestamp,
            TimestampType.CREATE_TIME);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results =
        session.execute("SELECT bigintcol, intcol, writetime(intcol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234);
    assertThat(row.getInt("intcol")).isEqualTo(10000);

    // timestamp from record is ignored with user provided queries
    assertThat(row.getLong(2)).isGreaterThan(recordTimestamp);
  }

  @Test
  void
      should_allow_insert_json_using_query_parameter_with_bound_variables_different_than_cql_columns() {
    // when providing custom query, the connector is not validating bound variables from prepared
    // statements user needs to take care of the query requirements on their own.
    ImmutableMap<String, String> extras =
        ImmutableMap.of(
            queryParameter(),
            String.format(
                "INSERT INTO %s.types (bigintCol, intCol) VALUES (:some_name, :some_name_2)",
                keyspaceName));

    conn.start(makeConnectorProperties("some_name=value.bigint, some_name_2=value.int", extras));

    String value = "{\"bigint\": 1234, \"int\": 10000}";

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT * FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234);
    assertThat(row.getInt("intcol")).isEqualTo(10000);
  }

  @Test
  void should_update_json_using_query_parameter() {
    ImmutableMap<String, String> extras =
        ImmutableMap.of(
            queryParameter(),
            String.format(
                "UPDATE %s.types SET listCol = listCol + [1] where bigintcol = :pkey",
                keyspaceName));

    conn.start(makeConnectorProperties("pkey=value.pkey, newitem=value.newitem", extras));

    String value = "{\"pkey\": 1234, \"newitem\": 1}";

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    SinkRecord record2 = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    runTaskWithRecords(record, record2);

    // Verify that two values were append to listcol
    List<Row> results = session.execute("SELECT * FROM types where bigintcol = 1234").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234);
    assertThat(row.getList("listcol", Integer.class)).isEqualTo(ImmutableList.of(1, 1));
  }

  @Test
  void should_insert_json_using_query_parameter_and_ttl() {
    ImmutableMap<String, String> extras =
        ImmutableMap.of(
            // when user provide own query, the ttlTimeUnit is ignored
            String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"),
            "HOURS",
            queryParameter(),
            String.format(
                "INSERT INTO %s.types (bigintCol, intCol) VALUES (:bigintcol, :intcol) USING TTL :ttl",
                keyspaceName));

    conn.start(
        makeConnectorProperties("bigintcol=value.bigint, intcol=value.int, ttl=value.ttl", extras));

    String value = "{\"bigint\": 1234, \"int\": 10000, \"ttl\": 100000}";

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in FDSE.
    List<Row> results = session.execute("SELECT bigintcol, intcol, ttl(intcol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234);
    assertThat(row.getInt("intcol")).isEqualTo(10000);
    assertTtl(row.getInt(2), 100000);
  }

  @Test
  void should_insert_json_using_query_parameter_and_timestamp() {
    ImmutableMap<String, String> extras =
        ImmutableMap.of(
            // when user provide own query, the timestampTimeUnit is ignored
            String.format("topic.mytopic.%s.%s.timestampTimeUnit", keyspaceName, "types"),
            "HOURS",
            queryParameter(),
            String.format(
                "INSERT INTO %s.types (bigintCol, intCol) VALUES (:bigintcol, :intcol) USING TIMESTAMP :timestamp",
                keyspaceName));

    conn.start(
        makeConnectorProperties(
            "bigintcol=value.bigint, intcol=value.int, timestamp=value.timestamp", extras));

    String value = "{\"bigint\": 1234, \"int\": 10000, \"timestamp\": 100000}";

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in FDSE.
    List<Row> results =
        session.execute("SELECT bigintcol, intcol, writetime(intcol) FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234);
    assertThat(row.getInt("intcol")).isEqualTo(10000);
    assertThat(row.getLong(2)).isEqualTo(100000L);
  }

  @Test
  void should_insert_struct_with_query_parameter() {
    ImmutableMap<String, String> extras =
        ImmutableMap.of(
            queryParameter(),
            String.format(
                "INSERT INTO %s.types (bigintCol, intCol) VALUES (:bigint_col, :int_col) USING TIMESTAMP :timestamp and TTL 1000",
                keyspaceName));

    conn.start(
        makeConnectorProperties(
            "bigint_col=value.bigint, int_col=value.int, timestamp=value.int", extras));

    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("bigint", 1234567L).put("int", 1000);

    SinkRecord record =
        new SinkRecord(
            "mytopic", 0, null, null, null, value, 1234L, 153000987L, TimestampType.CREATE_TIME);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results =
        session
            .execute("SELECT bigintcol, intcol, writetime(intcol), ttl(intcol) FROM types")
            .all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(1234567L);
    assertThat(row.getInt("intcol")).isEqualTo(1000);
    assertThat(row.getLong(2)).isEqualTo(1000L);
    assertTtl(row.getInt(3), 1000);
  }

  private String queryParameter() {
    return String.format("topic.mytopic.%s.%s.query", keyspaceName, "types");
  }
}
