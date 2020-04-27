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
package com.datastax.kafkaconnector.ccm;

import static com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Type.DDAC;
import static com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Type.DSE;
import static com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Type.OSS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMRequirements;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMVersionRequirement;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
@CCMRequirements(
  compatibleTypes = {DSE, DDAC, OSS},
  versionRequirements = {@CCMVersionRequirement(type = OSS, min = "3.6")}
)
// minimum version required because support of non frozen types
class ProvidedQueryCCMIT extends EndToEndCCMITBase {
  private static final Schema UDT_SCHEMA =
      SchemaBuilder.struct()
          .name("Kafka")
          .field("udtmem1", Schema.INT32_SCHEMA)
          .field("udtmem2", Schema.STRING_SCHEMA)
          .build();

  public ProvidedQueryCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
  }

  @BeforeAll
  void setup() {
    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE IF NOT EXISTS types_with_frozen ("
                    + "bigintCol bigint PRIMARY KEY, "
                    + "udtCol frozen<myudt>, "
                    + "udtColNotFrozen myudt"
                    + ")")
            .setTimeout(Duration.ofSeconds(10))
            .build());
  }

  @BeforeEach
  void cleanup() {
    session.execute("TRUNCATE types_with_frozen");
  }

  @Test
  void should_insert_json_using_query_parameter() {
    ImmutableMap<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(
                queryParameter("types"),
                String.format(
                    "INSERT INTO %s.types (bigintCol, intCol) VALUES (:bigintcol, :intcol)",
                    keyspaceName))
            .put(deletesDisabled("types"))
            .build();

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
  void should_fail_insert_json_using_query_parameter_with_deletes_enabled() {
    ImmutableMap<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(
                queryParameter("types"),
                String.format(
                    "INSERT INTO %s.types (bigintCol, intCol) VALUES (:bigintcol, :intcol)",
                    keyspaceName))
            .put(deletesEnabled())
            .build();

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

    assertThatThrownBy(() -> runTaskWithRecords(record))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("If you want to provide own query, set the deletesEnabled to false.");
  }

  @Test
  void
      should_allow_insert_json_using_query_parameter_with_bound_variables_different_than_cql_columns() {
    // when providing custom query, the connector is not validating bound variables from prepared
    // statements user needs to take care of the query requirements on their own.
    ImmutableMap<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(
                queryParameter("types"),
                String.format(
                    "INSERT INTO %s.types (bigintCol, intCol) VALUES (:some_name, :some_name_2)",
                    keyspaceName))
            .put(deletesDisabled("types"))
            .build();

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
        ImmutableMap.<String, String>builder()
            .put(
                queryParameter("types"),
                String.format(
                    "UPDATE %s.types SET listCol = listCol + [1] where bigintcol = :pkey",
                    keyspaceName))
            .put(deletesDisabled("types"))
            .build();

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
        ImmutableMap.<String, String>builder()
            .put(
                // when user provide own query, the ttlTimeUnit is ignored
                String.format("topic.mytopic.%s.%s.ttlTimeUnit", keyspaceName, "types"), "HOURS")
            .put(
                queryParameter("types"),
                String.format(
                    "INSERT INTO %s.types (bigintCol, intCol) VALUES (:bigintcol, :intcol) USING TTL :ttl",
                    keyspaceName))
            .put(deletesDisabled("types"))
            .build();

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
        ImmutableMap.<String, String>builder()
            .put(
                // when user provide own query, the timestampTimeUnit is ignored
                String.format("topic.mytopic.%s.%s.timestampTimeUnit", keyspaceName, "types"),
                "HOURS")
            .put(
                queryParameter("types"),
                String.format(
                    "INSERT INTO %s.types (bigintCol, intCol) VALUES (:bigintcol, :intcol) USING TIMESTAMP :timestamp",
                    keyspaceName))
            .put(deletesDisabled("types"))
            .build();

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
        ImmutableMap.<String, String>builder()
            .put(
                queryParameter("types"),
                String.format(
                    "INSERT INTO %s.types (bigintCol, intCol) VALUES (:bigint_col, :int_col) USING TIMESTAMP :timestamp and TTL 1000",
                    keyspaceName))
            .put(deletesDisabled("types"))
            .build();

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

  @Test
  void should_use_query_to_partially_update_non_frozen_udt_when_null_to_unset() {
    ImmutableMap<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(
                queryParameter("types_with_frozen"),
                // to make a partial update of UDT to work, the UDT column type definition must be
                // not frozen
                String.format(
                    "UPDATE %s.types_with_frozen set udtColNotFrozen.udtmem1=:udtcol1, udtColNotFrozen.udtmem2=:udtcol2 where bigintCol=:bigintcol",
                    keyspaceName))
            .put(deletesDisabled("types_with_frozen"))
            // nullToUnset = true is default but it makes this requirement explicit for the test
            .put(
                String.format("topic.mytopic.%s.types_with_frozen.nullToUnset", keyspaceName),
                "true")
            .build();

    conn.start(
        makeConnectorProperties(
            "bigintcol=key, udtcol1=value.udtmem1, udtcol2=value.udtmem2",
            "types_with_frozen",
            extras));

    Struct value = new Struct(UDT_SCHEMA).put("udtmem1", 42).put("udtmem2", "the answer");

    SinkRecord record = new SinkRecord("mytopic", 0, null, 98761234L, null, value, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results =
        session.execute("SELECT bigintcol, udtColNotFrozen FROM types_with_frozen").all();
    Row row = extractAndAssertThatOneRowInResult(results);

    UserDefinedType udt =
        new UserDefinedTypeBuilder(keyspaceName, "myudt")
            .withField("udtmem1", DataTypes.INT)
            .withField("udtmem2", DataTypes.TEXT)
            .withAttachmentPoint(session.getContext())
            .build();
    assertThat(row.getUdtValue("udtColNotFrozen")).isEqualTo(udt.newValue(42, "the answer"));

    // insert record with only one column from udt - udtmem2 is null
    value = new Struct(UDT_SCHEMA).put("udtmem1", 42);

    record = new SinkRecord("mytopic", 0, null, 98761234L, null, value, 1234L);
    runTaskWithRecords(record);

    results = session.execute("SELECT bigintcol, udtColNotFrozen FROM types_with_frozen").all();
    row = extractAndAssertThatOneRowInResult(results);

    // default for topic is nullToUnset, so the udtmem2 field was not updated, the value was not
    // overridden
    assertThat(row.getUdtValue("udtColNotFrozen")).isEqualTo(udt.newValue(42, "the answer"));
  }

  @Test
  void should_use_update_query_on_non_frozen_udt_and_override_with_null_when_null_to_unset_false() {
    ImmutableMap<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(
                queryParameter("types_with_frozen"),
                // to make a partial update of UDT to work, the UDT column type definition must be
                // not frozen
                String.format(
                    "UPDATE %s.types_with_frozen set udtColNotFrozen.udtmem1=:udtcol1, udtColNotFrozen.udtmem2=:udtcol2 where bigintCol=:bigintcol",
                    keyspaceName))
            .put(deletesDisabled("types_with_frozen"))
            .put(
                String.format("topic.mytopic.%s.types_with_frozen.nullToUnset", keyspaceName),
                "false")
            .build();

    conn.start(
        makeConnectorProperties(
            "bigintcol=key, udtcol1=value.udtmem1, udtcol2=value.udtmem2",
            "types_with_frozen",
            extras));

    Struct value = new Struct(UDT_SCHEMA).put("udtmem1", 42).put("udtmem2", "the answer");

    SinkRecord record = new SinkRecord("mytopic", 0, null, 98761234L, null, value, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results =
        session.execute("SELECT bigintcol, udtColNotFrozen FROM types_with_frozen").all();
    Row row = extractAndAssertThatOneRowInResult(results);

    UserDefinedType udt =
        new UserDefinedTypeBuilder(keyspaceName, "myudt")
            .withField("udtmem1", DataTypes.INT)
            .withField("udtmem2", DataTypes.TEXT)
            .withAttachmentPoint(session.getContext())
            .build();
    assertThat(row.getUdtValue("udtColNotFrozen")).isEqualTo(udt.newValue(42, "the answer"));

    // insert record with only one column from udt - udtmem2 is null
    value = new Struct(UDT_SCHEMA).put("udtmem1", 42);

    record = new SinkRecord("mytopic", 0, null, 98761234L, null, value, 1234L);
    runTaskWithRecords(record);

    results = session.execute("SELECT bigintcol, udtColNotFrozen FROM types_with_frozen").all();
    row = extractAndAssertThatOneRowInResult(results);

    // nullToUnset for this topic was set to false, so the udtmem2 field was updated, the value was
    // overridden with null
    assertThat(row.getUdtValue("udtColNotFrozen")).isEqualTo(udt.newValue(42, null));
  }

  @Test
  void should_fail_when_use_query_to_partially_update_frozen_udt() {
    ImmutableMap<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(
                queryParameter("types_with_frozen"),
                // to make a partial update of UDT to work, the UDT column type definition must be
                // not frozen - here we are using frozen so the execption will be thrown
                String.format(
                    "UPDATE %s.types_with_frozen set udtCol.udtmem1=:udtcol1, udtCol.udtmem2=:udtcol2 where bigintCol=:bigintcol",
                    keyspaceName))
            .put(deletesDisabled("types_with_frozen"))
            .put(
                String.format("topic.mytopic.%s.types_with_frozen.nullToUnset", keyspaceName),
                "true")
            .build();

    conn.start(
        makeConnectorProperties(
            "bigintcol=key, udtcol1=value.udtmem1, udtcol2=value.udtmem2",
            "types_with_frozen",
            extras));

    Struct value = new Struct(UDT_SCHEMA).put("udtmem1", 42).put("udtmem2", "the answer");

    SinkRecord record = new SinkRecord("mytopic", 0, null, 98761234L, null, value, 1234L);

    assertThatThrownBy(() -> runTaskWithRecords(record))
        .hasCauseInstanceOf(InvalidQueryException.class)
        .hasStackTraceContaining("for frozen UDT column udtcol");
  }

  @Test
  void should_use_query_to_partially_update_map_when_null_to_unset() {
    ImmutableMap<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(
                queryParameter("types"),
                String.format(
                    "UPDATE %s.types SET mapCol[:key]=:value where bigintcol = :pk", keyspaceName))
            .put(deletesDisabled("types"))
            .put(String.format("topic.mytopic.%s.types.nullToUnset", keyspaceName), "true")
            .build();

    conn.start(makeConnectorProperties("pk=value.pk, key=value.key, value=value.value", extras));

    String value = "{\"pk\": 98761234, \"key\": \"key_1\", \"value\": 10}}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT * FROM types").all();
    Row row = extractAndAssertThatOneRowInResult(results);
    Map<String, Integer> mapcol = row.getMap("mapcol", String.class, Integer.class);
    assert mapcol != null;
    assertThat(mapcol.size()).isEqualTo(1);
    assertThat(mapcol).containsEntry("key_1", 10);

    value = "{\"pk\": 42, \"key\": \"key_1\", \"value\": null}";
    record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    runTaskWithRecords(record);
    results = session.execute("SELECT * FROM types").all();
    row = extractAndAssertThatOneRowInResult(results);
    mapcol = row.getMap("mapcol", String.class, Integer.class);
    assert mapcol != null;
    assertThat(mapcol.size()).isEqualTo(1);
    // update will null value will be skipped because nullToUnset = true
    assertThat(mapcol).containsEntry("key_1", 10);
  }

  @Test
  void should_use_query_to_partially_update_map_and_remove_when_using_null_to_unset_false() {
    ImmutableMap<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(
                queryParameter("types"),
                String.format(
                    "UPDATE %s.types SET mapCol[:key]=:value where bigintcol = :pk", keyspaceName))
            .put(deletesDisabled("types"))
            .put(String.format("topic.mytopic.%s.types.nullToUnset", keyspaceName), "false")
            .build();

    conn.start(makeConnectorProperties("pk=value.pk, key=value.key, value=value.value", extras));

    String value = "{\"pk\": 98761234, \"key\": \"key_1\", \"value\": 10}}";
    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT * FROM types").all();
    Row row = extractAndAssertThatOneRowInResult(results);
    Map<String, Integer> mapcol = row.getMap("mapcol", String.class, Integer.class);
    assert mapcol != null;
    assertThat(mapcol.size()).isEqualTo(1);
    assertThat(mapcol).containsEntry("key_1", 10);

    value = "{\"pk\": 98761234, \"key\": \"key_1\", \"value\": null}";
    record = new SinkRecord("mytopic", 0, null, null, null, value, 1234L);
    runTaskWithRecords(record);
    results = session.execute("SELECT * FROM types").all();
    // setting value for map = null when nullToUnset = false will cause the record to be removed
    assertThat(results.size()).isEqualTo(0);
  }

  @NonNull
  private Row extractAndAssertThatOneRowInResult(List<Row> results) {
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    return row;
  }

  private String queryParameter(String topicName) {
    return String.format("topic.mytopic.%s.%s.query", keyspaceName, topicName);
  }

  private Map.Entry<? extends String, ? extends String> deletesDisabled(String topicName) {
    return new LinkedHashMap.SimpleEntry<>(
        String.format("topic.mytopic.%s.%s.deletesEnabled", keyspaceName, topicName), "false");
  }

  private Map.Entry<? extends String, ? extends String> deletesEnabled() {
    return new LinkedHashMap.SimpleEntry<>(
        String.format("topic.mytopic.%s.%s.deletesEnabled", keyspaceName, "types"), "true");
  }
}
