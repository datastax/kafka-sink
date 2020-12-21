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
package com.datastax.oss.pulsar.sink.simulacron;

import static com.datastax.oss.dsbulk.tests.logging.StreamType.STDERR;
import static com.datastax.oss.dsbulk.tests.logging.StreamType.STDOUT;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.serverError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.datastax.oss.common.sink.state.InstanceState;
import com.datastax.oss.common.sink.state.LifeCycleManager;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.logging.StreamCapture;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptor;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronExtension;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Column;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Table;
import com.datastax.oss.dsbulk.tests.simulacron.annotations.SimulacronConfig;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.sink.pulsar.CassandraSinkTask;
import com.datastax.oss.sink.pulsar.GenericRecordImpl;
import com.datastax.oss.sink.pulsar.PulsarRecordImpl;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

@SuppressWarnings({"SameParameterValue", "deprecation"})
@ExtendWith(SimulacronExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@ExtendWith(LogInterceptingExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SimulacronConfig(dseVersion = "5.0.8")
class SimpleEndToEndSimulacronIT {

  private static final String INSERT_STATEMENT =
      "INSERT INTO ks1.table1(a,b) VALUES (:a,:b) USING TIMESTAMP :kafka_internal_timestamp";
  // if user provided custom query, it does not have auto-generated :kafka_internal_timestamp
  private static final String INSERT_STATEMENT_CUSTOM_QUERY =
      "INSERT INTO ks1.table1_custom_query(col1,col2) VALUES (:some1,:some2)";
  private static final String INSERT_STATEMENT_TTL =
      "INSERT INTO ks1.table1_with_ttl(a,b) VALUES (:a,:b) USING TIMESTAMP :kafka_internal_timestamp AND TTL :kafka_internal_ttl";
  private static final String DELETE_STATEMENT = "DELETE FROM ks1.table1 WHERE a = :a AND b = :b";
  private static final ImmutableMap<String, String> PARAM_TYPES =
      ImmutableMap.<String, String>builder()
          .put("a", "int")
          .put("b", "varchar")
          .put("kafka_internal_timestamp", "bigint")
          .build();

  private static final ImmutableMap<String, String> PARAM_TYPES_TTL =
      ImmutableMap.<String, String>builder()
          .put("a", "int")
          .put("b", "varchar")
          .put("kafka_internal_timestamp", "bigint")
          .put("kafka_internal_ttl", "bigint")
          .build();

  private static final ImmutableMap<String, String> PARAM_TYPES_CUSTOM_QUERY =
      ImmutableMap.<String, String>builder().put("some1", "int").put("some2", "varchar").build();

  private static final String INSTANCE_NAME = "myinstance";
  private final BoundCluster simulacron;
  private final SimulacronUtils.Keyspace schema;
  private final List<Map<String, Object>> taskConfigs = new ArrayList<>();
  private final CassandraSinkTask task;
  private final LogInterceptor logs;
  private final Map<String, Object> connectorProperties;
  private final String hostname;
  private final String port;
  private final Schema recordType;
  private SinkContext sinkContext = mock(SinkContext.class);

  @SuppressWarnings("unused")
  SimpleEndToEndSimulacronIT(
      BoundCluster simulacron,
      @LogCapture LogInterceptor logs,
      @StreamCapture(STDOUT) StreamInterceptor stdOut,
      @StreamCapture(STDERR) StreamInterceptor stdErr) {

    this.simulacron = simulacron;
    this.logs = logs;

    InetSocketAddress node = simulacron.dc(0).node(0).inetSocketAddress();
    hostname = node.getHostName();
    port = Integer.toString(node.getPort());

    task = new CassandraSinkTask();

    schema =
        new SimulacronUtils.Keyspace(
            "ks1",
            new Table("table1", new Column("a", DataTypes.INT), new Column("b", DataTypes.TEXT)),
            new Table(
                "table1_with_ttl", new Column("a", DataTypes.INT), new Column("b", DataTypes.TEXT)),
            new Table("table2", new Column("a", DataTypes.INT), new Column("b", DataTypes.TEXT)),
            new Table(
                "mycounter",
                new Column("a", DataTypes.INT),
                new Column("b", DataTypes.TEXT),
                new Column("c", DataTypes.COUNTER)),
            new Table(
                "table1_custom_query",
                new Column("col1", DataTypes.INT),
                new Column("col2", DataTypes.TEXT)));

    connectorProperties =
        ImmutableMap.<String, Object>builder()
            .put("name", INSTANCE_NAME)
            .put("contactPoints", hostname)
            .put("port", port)
            .put("loadBalancing.localDc", "dc1")
            .put("topic.mytopic.ks1.table1.mapping", "a=key, b=value.field1")
            .put("topic.mytopic_with_ttl.ks1.table1_with_ttl.mapping", "a=key, b=value, __ttl=key")
            .put("topic.yourtopic.ks1.table2.mapping", "a=key, b=value.field1")
            .put("topic.yourtopic.ks1.table2.consistencyLevel", "QUORUM")
            .put(
                "topic.yourTopic2.ks1.table1_custom_query.mapping",
                "some1=value.some1, some2=value.some2")
            .put("topic.yourTopic2.ks1.table1_custom_query.query", INSERT_STATEMENT_CUSTOM_QUERY)
            .put("topic.yourTopic2.ks1.table1_custom_query.deletesEnabled", "false")
            .build();

    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("field1").type(SchemaType.STRING);
    this.recordType = org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.AVRO));
  }

  private Record<GenericRecord> makeTtlRecord(int key, String value, long timestamp, long offset) {
    return new PulsarRecordImpl(
        "persistent://tenant/namespace/mytopic_with_ttl",
        key + "",
        new GenericRecordImpl().put("field1", value),
        recordType,
        timestamp);
  }

  private Record<GenericRecord> makeRecord(int key, String value, long timestamp, long offset) {
    return makeRecord(0, key, value, timestamp, offset);
  }

  private Record<GenericRecord> makeRecordCustomQuery(int key, String value, long offset) {
    return new PulsarRecordImpl(
        "persistent://tenant/namespace/yourTopic2",
        key + "",
        new GenericRecordImpl().put("field1", value),
        recordType,
        null);
  }

  private Record<GenericRecord> makeRecord(
      int partition, Object key, String value, long timestamp, long offset) {
    PulsarRecordImpl res =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic",
            key != null ? key.toString() : null,
            new GenericRecordImpl().put("field1", value),
            recordType,
            timestamp);
    res.setPartitionId(partition + ""); // does not matter to the sink
    res.setRecordSequence(offset); // does not matter to the sink
    return res;
  }

  private static Query makeQuery(int a, String b, long timestamp) {
    return new Query(
        INSERT_STATEMENT, Collections.emptyList(), makeParams(a, b, timestamp), PARAM_TYPES);
  }

  private static Query makeQueryCustomInsert(int a, String b) {
    return new Query(
        INSERT_STATEMENT_CUSTOM_QUERY,
        Collections.emptyList(),
        makeParamsCustomQuery(a, b),
        PARAM_TYPES_CUSTOM_QUERY);
  }

  private static Query makeTtlQuery(int a, String b, long timestamp, long ttl) {
    return new Query(
        INSERT_STATEMENT_TTL,
        Collections.emptyList(),
        makeParamsTtl(a, b, timestamp, ttl),
        PARAM_TYPES_TTL);
  }

  private static Map<String, Object> makeParamsTtl(int a, String b, long timestamp, long ttl) {
    return ImmutableMap.<String, Object>builder()
        .put("a", a)
        .put("b", b)
        .put("kafka_internal_timestamp", timestamp)
        .put("kafka_internal_ttl", ttl)
        .build();
  }

  private static Map<String, Object> makeParams(int a, String b, long timestamp) {
    return ImmutableMap.<String, Object>builder()
        .put("a", a)
        .put("b", b)
        .put("kafka_internal_timestamp", timestamp)
        .build();
  }

  private static Map<String, Object> makeParamsCustomQuery(int a, String b) {
    return ImmutableMap.<String, Object>builder().put("some1", a).put("some2", b).build();
  }

  @BeforeEach
  void resetPrimes() {
    simulacron.clearPrimes(true);
    simulacron.node(0).acceptConnections();
    Mockito.when(sinkContext.getSinkName()).thenReturn("name");
    taskConfigs.clear();
  }

  @AfterEach
  void cleanupMetrics() {
    LifeCycleManager.cleanMetrics();
  }

  @AfterEach
  void stopConnector() {
    task.close();
  }

  @Test
  void fail_prepare() {
    SimulacronUtils.primeTables(simulacron, schema);
    Query bad1 = makeQuery(32, "fail", 153000987000L);
    simulacron.prime(when(bad1).then(serverError("bad thing")).applyToPrepare());

    assertThatThrownBy(() -> task.open(connectorProperties, sinkContext))
        .isInstanceOf(RuntimeException.class)
        .hasMessageStartingWith(
            "Prepare failed for statement: " + INSERT_STATEMENT + " or " + DELETE_STATEMENT);
  }

  @Test
  void fail_prepare_no_deletes() {
    SimulacronUtils.primeTables(simulacron, schema);
    Query bad1 = makeQuery(32, "fail", 153000987000L);
    simulacron.prime(when(bad1).then(serverError("bad thing")).applyToPrepare());
    Map<String, Object> props = new HashMap<>(connectorProperties);
    props.put("topic.mytopic.ks1.table1.deletesEnabled", "false");
    Condition<Throwable> delete =
        new Condition<Throwable>("delete statement") {
          @Override
          public boolean matches(Throwable value) {
            return value.getMessage().contains(DELETE_STATEMENT);
          }
        };
    assertThatThrownBy(() -> task.open(props, sinkContext))
        .isInstanceOf(RuntimeException.class)
        .hasMessageStartingWith("Prepare failed for statement: " + INSERT_STATEMENT)
        .doesNotHave(delete);
  }

  @Test
  void fail_prepare_counter_table() {
    SimulacronUtils.primeTables(simulacron, schema);

    ImmutableMap<String, String> paramTypes =
        ImmutableMap.<String, String>builder()
            .put("a", "int")
            .put("b", "varchar")
            .put("c", "counter")
            .build();

    String query = "UPDATE ks1.mycounter SET c = c + :c WHERE a = :a AND b = :b";
    Query bad1 = new Query(query, Collections.emptyList(), makeParams(32, "fail", 2), paramTypes);
    simulacron.prime(when(bad1).then(serverError("bad thing")).applyToPrepare());

    ImmutableMap<String, Object> props =
        ImmutableMap.<String, Object>builder()
            .put("name", INSTANCE_NAME)
            .put("contactPoints", (String) connectorProperties.get("contactPoints"))
            .put("port", (String) connectorProperties.get("port"))
            .put("loadBalancing.localDc", "dc1")
            .put("topic.mytopic.ks1.mycounter.mapping", "a=key, b=value, c=value.f2")
            .build();
    assertThatThrownBy(() -> task.open(props, sinkContext))
        .isInstanceOf(RuntimeException.class)
        .hasMessageStartingWith(
            "Prepare failed for statement: "
                + query
                + " or "
                + "DELETE FROM ks1.mycounter WHERE a = :a AND b = :b");
  }

  @Test
  void fail_delete() {
    SimulacronUtils.primeTables(simulacron, schema);
    Query bad1 =
        new Query(
            "DELETE FROM ks1.mycounter WHERE a = :a AND b = :b",
            Collections.emptyList(),
            ImmutableMap.<String, Object>builder().put("a", 37).put("b", "delete").build(),
            ImmutableMap.<String, String>builder().put("a", "int").put("b", "varchar").build());
    simulacron.prime(when(bad1).then(serverError("bad thing")));
    Map<String, Object> connProps = new HashMap<>();
    connProps.put("name", INSTANCE_NAME);
    connProps.put("contactPoints", hostname);
    connProps.put("port", port);
    connProps.put("loadBalancing.localDc", "dc1");
    connProps.put(
        "topic.mytopic.ks1.mycounter.mapping", "a=value.bigint, b=value.text, c=value.int");

    taskConfigs.add(connProps);
    RecordSchemaBuilder builder =
        org.apache.pulsar.client.api.schema.SchemaBuilder.record("MyBean");
    builder.field("bigint").type(SchemaType.INT64);
    builder.field("text").type(SchemaType.STRING);
    builder.field("int").type(SchemaType.INT32);
    Schema<GenericRecord> schema =
        org.apache.pulsar.client.api.Schema.generic(builder.build(SchemaType.AVRO));

    GenericRecord value = new GenericRecordImpl().put("bigint", 37L).put("text", "delete");
    Record<GenericRecord> record =
        new PulsarRecordImpl("persistent://tenant/namespace/mytopic", null, value, schema);
    runTaskWithRecords(record);

    // The log may need a little time to be updated with our error message.
    try {
      MILLISECONDS.sleep(500);
    } catch (InterruptedException e) {
      // swallow
    }
    assertThat(logs.getAllMessagesAsString())
        .contains(
            "Error inserting/updating row for Pulsar record PulsarSinkRecord{PulsarRecordImpl{topic=persistent://tenant/namespace/mytopic, value=GenericRecordImpl{values={bigint=37, text=delete}}")
        .contains("statement: DELETE FROM ks1.mycounter WHERE a = :a AND b = :b");
  }

  @Test
  void failure_offsets() {
    SimulacronUtils.primeTables(simulacron, schema);

    Query good1 = makeQuery(42, "the answer", 153000987000L);
    simulacron.prime(when(good1).then(noRows()));

    Query bad1 = makeQuery(32, "fail", 153000987000L);
    simulacron.prime(when(bad1).then(serverError("bad thing")).delay(500, TimeUnit.MILLISECONDS));

    Query good2 = makeQuery(22, "success", 153000987000L);
    simulacron.prime(when(good2).then(noRows()));

    Query bad2 = makeQuery(12, "fail2", 153000987000L);
    simulacron.prime(when(bad2).then(serverError("bad thing")));

    taskConfigs.add(connectorProperties);

    Record<GenericRecord> record1 = makeRecord(0, "42", "the answer", 153000987L, 1234);
    Record<GenericRecord> record2 = makeRecord(0, "32", "fail", 153000987L, 1235);
    Record<GenericRecord> record3 = makeRecord(0, "22", "success", 153000987L, 1236);
    Record<GenericRecord> record4 = makeRecord(0, "12", "fail2", 153000987L, 1237);

    // Make a bad record in a different partition.
    Record<GenericRecord> record5 = makeRecord(1, "bad key", "fail3", 153000987L, 1238);

    // bad record in the wrong topic. THis is probably not realistic but allows us to test the outer
    // try-catch block in mapAndQueueRecord().
    Record<GenericRecord> record6 =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/wrongtopic", "", new GenericRecordImpl(), recordType);

    runTaskWithRecords(record1, record2, record3, record4, record5, record6);

    assertThat(logs.getAllMessagesAsString())
        .contains(
            "Error inserting/updating row for Pulsar record PulsarSinkRecord{PulsarRecordImpl{topic=persistent://tenant/namespace/mytopic, value=GenericRecordImpl{values={field1=fail2}}")
        .contains(
            "Error decoding/mapping Pulsar record PulsarSinkRecord{PulsarRecordImpl{topic=persistent://tenant/namespace/mytopic, value=GenericRecordImpl{values={field1=fail3}}")
        .contains("Connector has no configuration for record topic 'wrongtopic'")
        .contains("Could not parse 'bad key'")
        .contains(
            "statement: INSERT INTO ks1.table1(a,b) VALUES (:a,:b) USING TIMESTAMP :kafka_internal_timestamp");
    InstanceState instanceState = task.getInstanceState();
    assertThat(instanceState.getFailedRecordCounter("mytopic", "ks1.table1").getCount())
        .isEqualTo(3);
    assertThat(instanceState.getRecordCounter("mytopic", "ks1.table1").getCount()).isEqualTo(4);
    assertThat(instanceState.getFailedWithUnknownTopicCounter().getCount()).isEqualTo(1);
  }

  @Test
  void should_not_record_failure_offsets_for_mapping_errors_if_ignore_errors_all() {
    SimulacronUtils.primeTables(simulacron, schema);

    Query good1 = makeQuery(42, "the answer", 153000987000L);
    simulacron.prime(when(good1).then(noRows()));

    Query good2 = makeQuery(43, "another answer", 153000987000L);
    simulacron.prime(when(good2).then(noRows()));

    Map<String, Object> connectorPropertiesIgnoreErrors =
        new ImmutableMap.Builder<String, Object>()
            .putAll(connectorProperties)
            .put("ignoreErrors", "All")
            .build();

    taskConfigs.add(connectorPropertiesIgnoreErrors);

    Record<GenericRecord> record1 = makeRecord(0, "42", "the answer", 153000987L, 1234);
    Record<GenericRecord> record2 = makeRecord(0, "bad key", "will fail", 153000987L, 1235);
    Record<GenericRecord> record3 = makeRecord(0, "43", "another answer", 153000987L, 1236);

    runTaskWithRecords(record1, record2, record3);

    assertThat(logs.getAllMessagesAsString())
        .contains(
            "Error decoding/mapping Pulsar record PulsarSinkRecord{PulsarRecordImpl{topic=persistent://tenant/namespace/mytopic, value=GenericRecordImpl{values={field1=will fail}}")
        .contains("Could not parse 'bad key'");
    InstanceState instanceState = task.getInstanceState();
    assertThat(instanceState.getFailedRecordCounter("mytopic", "ks1.table1").getCount())
        .isEqualTo(1);
    assertThat(instanceState.getRecordCounter("mytopic", "ks1.table1").getCount()).isEqualTo(2);
  }
  //
  //  @ParameterizedTest
  //  @CsvSource({"All", "Driver"})
  //  void should_not_record_failure_offsets_for_driver_errors_if_ignore_errors_all_or_driver(
  //      String ignoreErrors) {
  //    SimulacronUtils.primeTables(simulacron, schema);
  //
  //    Query good1 = makeQuery(42, "the answer", 153000987000L);
  //    simulacron.prime(when(good1).then(noRows()));
  //
  //    Query bad1 = makeQuery(32, "fail", 153000987000L);
  //    simulacron.prime(when(bad1).then(serverError("bad thing")).delay(500,
  // TimeUnit.MILLISECONDS));
  //
  //    Query good2 = makeQuery(22, "success", 153000987000L);
  //    simulacron.prime(when(good2).then(noRows()));
  //
  //    Query bad2 = makeQuery(12, "fail2", 153000987000L);
  //    simulacron.prime(when(bad2).then(serverError("bad thing")));
  //
  //    Query bad3 = makeQuery(2, "fail3", 153000987000L);
  //    simulacron.prime(when(bad3).then(serverError("bad thing")));
  //
  //    Map<String, String> connectorPropertiesIgnoreErrors =
  //        new ImmutableMap.Builder<String, String>()
  //            .putAll(connectorProperties)
  //            .put("ignoreErrors", ignoreErrors)
  //            .build();
  //
  //    conn.start(connectorPropertiesIgnoreErrors);
  //
  //    Record<GenericRecord> record1 = makeRecord(42, "the answer", 153000987L, 1234);
  //    Record<GenericRecord> record2 = makeRecord(32, "fail", 153000987L, 1235);
  //    Record<GenericRecord> record3 = makeRecord(22, "success", 153000987L, 1236);
  //    Record<GenericRecord> record4 = makeRecord(12, "fail2", 153000987L, 1237);
  //
  //    // Make a bad record in a different partition.
  //    Record<GenericRecord> record5 = makeRecord(1, 2, "fail3", 153000987L, 1238);
  //    runTaskWithRecords(record1, record2, record3, record4, record5);
  //
  //    // Verify that we get an error offset for the first record that failed in partition 0 (1235)
  //    // even though its failure was discovered after 1237. Also, 1238 belongs to a different
  //    // partition, so it should be included.
  //    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
  //    task.preCommit(currentOffsets);
  //    assertThat(currentOffsets).isEmpty();
  //
  //    assertThat(logs.getAllMessagesAsString())
  //        .contains("Error inserting/updating row for Kafka record
  // Record<GenericRecord>{kafkaOffset=1237")
  //        .contains("Error inserting/updating row for Kafka record
  // Record<GenericRecord>{kafkaOffset=1238")
  //        .contains(
  //            "statement: INSERT INTO ks1.table1(a,b) VALUES (:a,:b) USING TIMESTAMP
  // :kafka_internal_timestamp");
  //    InstanceState instanceState = task.getInstanceState();
  //    assertThat(instanceState.getFailedRecordCounter("mytopic", "ks1.table1").getCount())
  //        .isEqualTo(3);
  //    assertThat(instanceState.getRecordCounter("mytopic", "ks1.table1").getCount()).isEqualTo(5);
  //  }
  //
  //  @Test
  //  void success_offset() {
  //    SimulacronUtils.primeTables(simulacron, schema);
  //
  //    Query good1 = makeQuery(42, "the answer", 153000987000L);
  //    simulacron.prime(when(good1).then(noRows()));
  //
  //    Query good2 = makeQuery(22, "success", 153000987000L);
  //    simulacron.prime(when(good2).then(noRows()));
  //
  //    conn.start(connectorProperties);
  //
  //    Record<GenericRecord> record1 = makeRecord(42, "the answer", 153000987L, 1234);
  //    Record<GenericRecord> record2 = makeRecord(22, "success", 153000987L, 1235);
  //    runTaskWithRecords(record1, record2);
  //
  //    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
  //    task.preCommit(currentOffsets);
  //    assertThat(currentOffsets).isEmpty();
  //
  //    List<QueryLog> queryList =
  //        simulacron
  //            .node(0)
  //            .getLogs()
  //            .getQueryLogs()
  //            .stream()
  //            .filter(q -> q.getType().equals("EXECUTE"))
  //            .collect(Collectors.toList());
  //    assertThat(queryList.size()).isEqualTo(2);
  //    assertThat(queryList.get(0).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
  //    assertThat(queryList.get(1).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
  //  }
  //
  //  /** Test for KAF-72 */
  //  @Test
  //  void should_record_counters_per_topic_ks_table() {
  //    SimulacronUtils.primeTables(simulacron, schema);
  //
  //    Query good1topic1 = makeQuery(42, "the answer", 153000987000L);
  //    simulacron.prime(when(good1topic1).then(noRows()));
  //
  //    Query good2topic1 = makeQuery(22, "success", 153000987000L);
  //    simulacron.prime(when(good2topic1).then(noRows()));
  //
  //    Query good1topic2 = makeTtlQuery(22, "success", 153000987000L, 22L);
  //    simulacron.prime(when(good1topic2).then(noRows()));
  //
  //    Query good2topic2 = makeTtlQuery(33, "success_2", 153000987000L, 33L);
  //    simulacron.prime(when(good2topic2).then(noRows()));
  //
  //    conn.start(connectorProperties);
  //
  //    Record<GenericRecord> record1topic1 = makeRecord(42, "the answer", 153000987L, 1234);
  //    Record<GenericRecord> record2topic1 = makeRecord(22, "success", 153000987L, 1235);
  //    Record<GenericRecord> record1topic2 = makeTtlRecord(22, "success", 153000987L, 1235);
  //    Record<GenericRecord> record2topic2 = makeTtlRecord(33, "success_2", 153000987L, 1235);
  //
  //    runTaskWithRecords(record1topic1, record2topic1, record1topic2, record2topic2);
  //
  //    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
  //    task.preCommit(currentOffsets);
  //    assertThat(currentOffsets).isEmpty();
  //
  //    List<QueryLog> queryList =
  //        simulacron
  //            .node(0)
  //            .getLogs()
  //            .getQueryLogs()
  //            .stream()
  //            .filter(q -> q.getType().equals("EXECUTE"))
  //            .collect(Collectors.toList());
  //    assertThat(queryList.size()).isEqualTo(4);
  //    assertThat(queryList.get(0).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
  //    assertThat(queryList.get(1).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
  //    assertThat(queryList.get(2).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
  //    assertThat(queryList.get(3).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
  //
  //    InstanceState instanceState = task.getInstanceState();
  //    assertThat(instanceState.getRecordCounter("mytopic", "ks1.table1").getCount()).isEqualTo(2);
  //    assertThat(instanceState.getRecordCounter("mytopic_with_ttl",
  // "ks1.table1_with_ttl").getCount())
  //        .isEqualTo(2);
  //  }
  //
  //  @Test
  //  void consistency_level() {
  //    SimulacronUtils.primeTables(simulacron, schema);
  //
  //    Query good1 = makeQuery(42, "the answer", 153000987000L);
  //    simulacron.prime(when(good1).then(noRows()));
  //
  //    Query good2 =
  //        new Query(
  //            "INSERT INTO ks1.table2(a,b) VALUES (:a,:b) USING TIMESTAMP
  // :kafka_internal_timestamp",
  //            Collections.emptyList(),
  //            makeParams(22, "success", 153000987000L),
  //            PARAM_TYPES);
  //    simulacron.prime(when(good2).then(noRows()));
  //
  //    conn.start(connectorProperties);
  //
  //    Record<GenericRecord> record1 = makeRecord(42, "the answer", 153000987L, 1234);
  //
  //    // Put the second record in "yourtopic", which has QUORUM CL.
  //    Record<GenericRecord> record2 =
  //        new Record<GenericRecord>(
  //            "yourtopic",
  //            0,
  //            null,
  //            22,
  //            null,
  //            "success",
  //            1235L,
  //            153000987L,
  //            TimestampType.CREATE_TIME);
  //    runTaskWithRecords(record1, record2);
  //
  //    List<QueryLog> queryList =
  //        simulacron
  //            .node(0)
  //            .getLogs()
  //            .getQueryLogs()
  //            .stream()
  //            .filter(q -> q.getType().equals("EXECUTE"))
  //            .collect(Collectors.toList());
  //    assertThat(queryList.size()).isEqualTo(2);
  //
  //    for (QueryLog queryLog : queryList) {
  //      if (queryLog.getQuery().contains("table1")) {
  //        assertThat(queryLog.getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
  //      } else if (queryLog.getQuery().contains("table2")) {
  //        assertThat(queryLog.getConsistency()).isEqualTo(ConsistencyLevel.QUORUM);
  //      } else {
  //        fail("%s is not for table1 nor table2!", queryLog.toString());
  //      }
  //    }
  //  }
  //
  //  @Test
  //  void undefined_topic() {
  //    SimulacronUtils.primeTables(simulacron, schema);
  //
  //    Query good1 = makeQuery(42, "the answer", 153000987000L);
  //    simulacron.prime(when(good1).then(noRows()));
  //
  //    conn.start(connectorProperties);
  //
  //    Record<GenericRecord> goodRecord = makeRecord(42, "the answer", 153000987L, 1234);
  //
  //    Record<GenericRecord> badRecord = new Record<GenericRecord>("unknown", 0, null, 42L, null,
  // 42, 1234L);
  //    runTaskWithRecords(goodRecord, badRecord);
  //    assertThat(logs.getAllMessagesAsString())
  //        .contains("Error decoding/mapping Kafka record Record<GenericRecord>{kafkaOffset=1234")
  //        .contains(
  //            "Connector has no configuration for record topic 'unknown'. Please update the
  // configuration and restart.");
  //
  //    // Verify that the insert for good1 was issued.
  //    List<QueryLog> queryList =
  //        simulacron
  //            .node(0)
  //            .getLogs()
  //            .getQueryLogs()
  //            .stream()
  //            .filter(q -> q.getType().equals("EXECUTE"))
  //            .collect(Collectors.toList());
  //    byte[] secondParam = new byte[10];
  //    ((Execute) queryList.get(0).getFrame().message)
  //        .options
  //        .positionalValues
  //        .get(1)
  //        .get(secondParam);
  //    assertThat(new String(secondParam, StandardCharsets.UTF_8)).isEqualTo("the answer");
  //  }
  //
  //  @Test
  //  void batch_requests() {
  //    // Insert 5 records: 2 from mytopic, 3 from yourtopic. Verify that they batch properly and
  //    // with the correct CLs.
  //
  //    // Even though we will not be executing simple statements in this test, we must specify
  //    // that we will so that Simulacron handles preparing our statement properly.
  //    SimulacronUtils.primeTables(simulacron, schema);
  //    Query good1 = makeQuery(42, "the answer", 153000987000L);
  //    simulacron.prime(when(good1).then(noRows()));
  //    Query good2 =
  //        new Query(
  //            "INSERT INTO ks1.table2(a,b) VALUES (:a,:b) USING TIMESTAMP
  // :kafka_internal_timestamp",
  //            Collections.emptyList(),
  //            makeParams(42, "topic2 success1", 153000987000L),
  //            PARAM_TYPES);
  //    simulacron.prime(when(good2).then(noRows()));
  //
  //    conn.start(connectorProperties);
  //
  //    Record<GenericRecord> goodRecord1 = makeRecord(42, "the answer", 153000987L, 1234);
  //    Record<GenericRecord> goodRecord2 = makeRecord(42, "the second answer", 153000987L, 1234);
  //    Record<GenericRecord> goodRecord3 =
  //        new Record<GenericRecord>(
  //            "yourtopic",
  //            0,
  //            null,
  //            42,
  //            null,
  //            "topic2 success1",
  //            1235L,
  //            153000987L,
  //            TimestampType.CREATE_TIME);
  //    Record<GenericRecord> goodRecord4 =
  //        new Record<GenericRecord>(
  //            "yourtopic",
  //            0,
  //            null,
  //            42,
  //            null,
  //            "topic2 success2",
  //            1235L,
  //            153000987L,
  //            TimestampType.CREATE_TIME);
  //    Record<GenericRecord> goodRecord5 =
  //        new Record<GenericRecord>(
  //            "yourtopic",
  //            0,
  //            null,
  //            42,
  //            null,
  //            "topic2 success3",
  //            1235L,
  //            153000987L,
  //            TimestampType.CREATE_TIME);
  //
  //    // The order of records shouldn't matter here, but we try to mix things up to emulate
  //    // a real workload.
  //    runTaskWithRecords(goodRecord1, goodRecord3, goodRecord2, goodRecord4, goodRecord5);
  //
  //    // Verify that we issued two batch requests, one at LOCAL_ONE (for table1/mytopic) and
  //    // one at QUORUM (for table2/yourtopic). There's seem pretty gnarly unwrapping of request
  //    // info. We distinguish one batch from the other based on the number of statements in the
  //    // batch.
  //    List<QueryLog> queryList =
  //        simulacron
  //            .node(0)
  //            .getLogs()
  //            .getQueryLogs()
  //            .stream()
  //            .filter(q -> q.getType().equals("BATCH"))
  //            .collect(Collectors.toList());
  //    Map<ConsistencyLevel, Integer> queryInfo =
  //        queryList
  //            .stream()
  //            .map(queryLog -> (Batch) queryLog.getFrame().message)
  //            .collect(
  //                Collectors.toMap(
  //                    message -> ConsistencyLevel.fromCode(message.consistency),
  //                    message -> message.values.size()));
  //    assertThat(queryInfo)
  //        .containsOnly(entry(ConsistencyLevel.LOCAL_ONE, 2), entry(ConsistencyLevel.QUORUM, 3));
  //
  //    InstanceState instanceState = task.getInstanceState();
  //
  //    // verify that was one batch with 2 statements for mytopic
  //    verifyOneBatchWithNStatements(instanceState.getBatchSizeHistogram("mytopic", "ks1.table1"),
  // 2);
  //
  //    // verify that was one batch with 3 statements for yourtopic
  //    verifyOneBatchWithNStatements(
  //        instanceState.getBatchSizeHistogram("yourtopic", "ks1.table2"), 3);
  //
  //    // verify batchSizeInBytes updates for mytopic
  //    verifyBatchSizeInBytesUpdate(
  //        instanceState.getBatchSizeInBytesHistogram("mytopic", "ks1.table1"), 2, false);
  //
  //    // verify batchSizeInBytes updates for yourtopic
  //    verifyBatchSizeInBytesUpdate(
  //        instanceState.getBatchSizeInBytesHistogram("yourtopic", "ks1.table2"), 3, true);
  //  }
  //
  //  private void verifyOneBatchWithNStatements(Histogram histogram, long numberOfStatements) {
  //    // one batch
  //    assertThat(histogram.getCount()).isEqualTo(1);
  //    // that had numberOfStatements statements in it
  //    assertThat(histogram.getSnapshot().getMax()).isEqualTo(numberOfStatements);
  //    assertThat(histogram.getSnapshot().getMin()).isEqualTo(numberOfStatements);
  //  }
  //
  //  private void verifyBatchSizeInBytesUpdate(
  //      Histogram histogram, long numberOfUpdates, boolean allMessagesInBatchAreTheSame) {
  //    // verify that size in bytes was updated for every statement in batch
  //    assertThat(histogram.getCount()).isEqualTo(numberOfUpdates);
  //    if (allMessagesInBatchAreTheSame) {
  //      // min and max are the same because statements in batch are different
  //      assertThat(histogram.getSnapshot().getMax()).isEqualTo(histogram.getSnapshot().getMin());
  //    } else {
  //      // min and max are different because statements in batch are different
  //
  // assertThat(histogram.getSnapshot().getMax()).isNotEqualTo(histogram.getSnapshot().getMin());
  //    }
  //  }
  //
  //  @Test
  //  void fail_batch_request() {
  //    // Test single topic, multiple Kafka partitions, single C* partition, fail batch request:
  //    // single batch statement failure causes failureOffsets for all Record<GenericRecord>'s in
  // the batch.
  //    SimulacronUtils.primeTables(simulacron, schema);
  //    Query good1 = makeQuery(42, "the answer", 153000987000L);
  //    simulacron.prime(when(good1).then(noRows()));
  //
  //    Statement s1 =
  //        new Statement(
  //            "INSERT INTO ks1.table1(a,b) VALUES (:a,:b) USING TIMESTAMP
  // :kafka_internal_timestamp",
  //            PARAM_TYPES,
  //            makeParams(42, "the answer", 153000987000L));
  //    Statement s2 =
  //        new Statement(
  //            "INSERT INTO ks1.table1(a,b) VALUES (:a,:b) USING TIMESTAMP
  // :kafka_internal_timestamp",
  //            PARAM_TYPES,
  //            makeParams(42, "the second answer", 153000987000L));
  //    com.datastax.oss.simulacron.common.request.Batch batchRequest =
  //        new com.datastax.oss.simulacron.common.request.Batch(
  //            Arrays.asList(s1, s2), new String[] {"LOCAL_ONE", "LOCAL_ONE"});
  //
  //    simulacron.prime(when(batchRequest).then(serverError("bad thing")));
  //
  //    Record<GenericRecord> record1 = makeRecord(0, 42, "the answer", 153000987000L, 1234);
  //    Record<GenericRecord> record2 = makeRecord(1, 42, "the second answer", 153000987000L, 8888);
  //
  //    conn.start(connectorProperties);
  //    runTaskWithRecords(record1, record2);
  //
  //    // Verify that one Batch request was issued, and no EXECUTE requests:
  //    long batchRequestCount =
  //        simulacron
  //            .node(0)
  //            .getLogs()
  //            .getQueryLogs()
  //            .stream()
  //            .filter(q -> q.getType().equals("BATCH"))
  //            .count();
  //    long executeRequestCount =
  //        simulacron
  //            .node(0)
  //            .getLogs()
  //            .getQueryLogs()
  //            .stream()
  //            .filter(q -> q.getType().equals("EXECUTE"))
  //            .count();
  //    assertThat(batchRequestCount).isEqualTo(1L);
  //    assertThat(executeRequestCount).isEqualTo(0L);
  //
  //    // Verify that we get error offsets for both records.
  //    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
  //    task.preCommit(currentOffsets);
  //    assertThat(currentOffsets)
  //        .containsOnly(
  //            entry(new TopicPartition("mytopic", 0), new OffsetAndMetadata(1234L)),
  //            entry(new TopicPartition("mytopic", 1), new OffsetAndMetadata(8888L)));
  //
  //    assertThat(logs.getAllMessagesAsString())
  //        .contains("Error inserting/updating row for Kafka record
  // Record<GenericRecord>{kafkaOffset=1234")
  //        .contains("Error inserting/updating row for Kafka record
  // Record<GenericRecord>{kafkaOffset=8888");
  //  }
  //
  //  @Test
  //  void success_offset_custom_query() {
  //    SimulacronUtils.primeTables(simulacron, schema);
  //
  //    Query good1 = makeQueryCustomInsert(42, "abc");
  //    simulacron.prime(when(good1).then(noRows()));
  //
  //    Query good2 = makeQueryCustomInsert(22, "abcd");
  //    simulacron.prime(when(good2).then(noRows()));
  //
  //    conn.start(connectorProperties);
  //
  //    Record<GenericRecord> record1 = makeRecordCustomQuery(1, "{\"some1\" : 42, \"some2\":
  // \"abc\" }", 1234);
  //    Record<GenericRecord> record2 = makeRecordCustomQuery(2, "{\"some1\" : 22, \"some2\":
  // \"abcd\" }", 1235);
  //    runTaskWithRecords(record1, record2);
  //
  //    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
  //    task.preCommit(currentOffsets);
  //    assertThat(currentOffsets).isEmpty();
  //
  //    List<QueryLog> queryList =
  //        simulacron
  //            .node(0)
  //            .getLogs()
  //            .getQueryLogs()
  //            .stream()
  //            .filter(q -> q.getType().equals("EXECUTE"))
  //            .collect(Collectors.toList());
  //    assertThat(queryList.size()).isEqualTo(2);
  //  }

  private void runTaskWithRecords(Record<GenericRecord>... records) {
    Map<String, Object> taskProps = taskConfigs.get(0);
    task.open(taskProps, sinkContext);
    for (Record<GenericRecord> r : records) {
      try {
        task.write(r);
      } catch (Throwable ex) {
        // ignore
        ex.printStackTrace();
      }
    }
  }
}
