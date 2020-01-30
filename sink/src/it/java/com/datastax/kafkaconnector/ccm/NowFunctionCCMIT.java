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
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class NowFunctionCCMIT extends EndToEndCCMITBase {

  NowFunctionCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
  }

  @Test
  void should_insert_value_using_now_function_json() {
    // given
    conn.start(makeConnectorProperties("bigintcol=value.bigint, loaded_at=now()"));

    // when
    String json = "{\"bigint\": 1234567}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
    runTaskWithRecords(record);

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
    conn.start(
        makeConnectorProperties("bigintcol=value.bigint, loaded_at=now(), loaded_at2=now()"));

    // when
    String json = "{\"bigint\": 1234567}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);
    runTaskWithRecords(record);

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
    conn.start(
        makeConnectorProperties("bigintcol=value.bigint, loaded_at=now(), loaded_at2=now()"));

    Schema schema =
        SchemaBuilder.struct().name("Kafka").field("bigint", Schema.INT64_SCHEMA).build();
    Struct value = new Struct(schema).put("bigint", 1234567L);

    SinkRecord record =
        new SinkRecord(
            "mytopic", 0, null, null, null, value, 1234L, 153000987L, TimestampType.CREATE_TIME);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
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
    conn.start(
        makeConnectorProperties(
            "my_pk=value.my_pk, my_value=value.my_value, loaded_at=now()",
            "pk_value_with_timeuuid",
            null));

    // Set up records for "mytopic"
    String json = "{\"my_pk\": 1234567, \"my_value\": null}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, json, 1234L);

    runTaskWithRecords(record);

    // Verify that the record was deleted from DSE.
    results = session.execute("SELECT * FROM pk_value_with_timeuuid").all();
    assertThat(results.size()).isEqualTo(0);
  }
}
