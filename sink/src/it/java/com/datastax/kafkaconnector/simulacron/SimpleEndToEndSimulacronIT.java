/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.simulacron;

import static com.datastax.dsbulk.commons.tests.logging.StreamType.STDERR;
import static com.datastax.dsbulk.commons.tests.logging.StreamType.STDOUT;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.serverError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.logging.StreamCapture;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptor;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronExtension;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronUtils;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronUtils.Column;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronUtils.Table;
import com.datastax.dsbulk.commons.tests.simulacron.annotations.SimulacronConfig;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.kafkaconnector.DseSinkConnector;
import com.datastax.kafkaconnector.DseSinkTask;
import com.datastax.kafkaconnector.state.InstanceState;
import com.datastax.kafkaconnector.state.LifeCycleManager;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.request.Statement;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"SameParameterValue", "deprecation"})
@ExtendWith(SimulacronExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@ExtendWith(LogInterceptingExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SimulacronConfig(dseVersion = "5.0.8")
class SimpleEndToEndSimulacronIT {

  private static final String INSERT_STATEMENT =
      "INSERT INTO ks1.table1(a,b) VALUES (:a,:b) USING TIMESTAMP :kafka_internal_timestamp";
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
  private static final String INSTANCE_NAME = "myinstance";
  private final BoundCluster simulacron;
  private final SimulacronUtils.Keyspace schema;
  private final DseSinkConnector conn;
  private final DseSinkTask task;
  private final LogInterceptor logs;
  private final Map<String, String> connectorProperties;
  private final String hostname;
  private final String port;

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

    SinkTaskContext taskContext = mock(SinkTaskContext.class);
    task = new DseSinkTask();
    task.initialize(taskContext);

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
                new Column("c", DataTypes.COUNTER)));
    conn = new DseSinkConnector();

    connectorProperties =
        ImmutableMap.<String, String>builder()
            .put("name", INSTANCE_NAME)
            .put("contactPoints", hostname)
            .put("port", port)
            .put("loadBalancing.localDc", "dc1")
            .put("topic.mytopic.ks1.table1.mapping", "a=key, b=value")
            .put("topic.mytopic_with_ttl.ks1.table1_with_ttl.mapping", "a=key, b=value, __ttl=key")
            .put("topic.yourtopic.ks1.table2.mapping", "a=key, b=value")
            .put("topic.yourtopic.ks1.table2.consistencyLevel", "QUORUM")
            .build();
  }

  private static SinkRecord makeTtlRecord(int key, String value, long timestamp, long offset) {
    return new SinkRecord(
        "mytopic_with_ttl",
        0,
        null,
        key,
        null,
        value,
        offset,
        timestamp,
        TimestampType.CREATE_TIME);
  }

  private static SinkRecord makeRecord(int key, String value, long timestamp, long offset) {
    return makeRecord(0, key, value, timestamp, offset);
  }

  private static SinkRecord makeRecord(
      int partition, int key, String value, long timestamp, long offset) {
    return new SinkRecord(
        "mytopic", partition, null, key, null, value, offset, timestamp, TimestampType.CREATE_TIME);
  }

  private static Query makeQuery(int a, String b, long timestamp) {
    return new Query(
        INSERT_STATEMENT, Collections.emptyList(), makeParams(a, b, timestamp), PARAM_TYPES);
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

  @BeforeEach
  void resetPrimes() {
    simulacron.clearPrimes(true);
    simulacron.node(0).acceptConnections();
  }

  @AfterEach
  void resetLogbackConfiguration() throws JoranException {
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(context);
    context.reset();
    configurator.doConfigure(ClassLoader.getSystemResource("logback-test.xml"));
  }

  @AfterEach
  void cleanupMetrics() {
    LifeCycleManager.cleanMetrics();
  }

  @AfterEach
  void stopConnector() {
    task.stop();
    conn.stop();
  }

  @Test
  void fail_prepare() {
    SimulacronUtils.primeTables(simulacron, schema);
    Query bad1 = makeQuery(32, "fail", 153000987000L);
    simulacron.prime(when(bad1).then(serverError("bad thing")).applyToPrepare());

    assertThatThrownBy(() -> task.start(connectorProperties))
        .isInstanceOf(RuntimeException.class)
        .hasMessageStartingWith(
            "Prepare failed for statement: " + INSERT_STATEMENT + " or " + DELETE_STATEMENT);
  }

  @Test
  void fail_prepare_no_deletes() {
    SimulacronUtils.primeTables(simulacron, schema);
    Query bad1 = makeQuery(32, "fail", 153000987000L);
    simulacron.prime(when(bad1).then(serverError("bad thing")).applyToPrepare());
    Map<String, String> props = new HashMap<>(connectorProperties);
    props.put("topic.mytopic.ks1.table1.deletesEnabled", "false");
    Condition<Throwable> delete =
        new Condition<Throwable>("delete statement") {
          @Override
          public boolean matches(Throwable value) {
            return value.getMessage().contains(DELETE_STATEMENT);
          }
        };
    assertThatThrownBy(() -> task.start(props))
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

    ImmutableMap<String, String> props =
        ImmutableMap.<String, String>builder()
            .put("name", INSTANCE_NAME)
            .put("contactPoints", connectorProperties.get("contactPoints"))
            .put("port", connectorProperties.get("port"))
            .put("loadBalancing.localDc", "dc1")
            .put("topic.mytopic.ks1.mycounter.mapping", "a=key, b=value, c=value.f2")
            .build();
    assertThatThrownBy(() -> task.start(props))
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
    Map<String, String> connProps = new HashMap<>();
    connProps.put("name", INSTANCE_NAME);
    connProps.put("contactPoints", hostname);
    connProps.put("port", port);
    connProps.put("loadBalancing.localDc", "dc1");
    connProps.put(
        "topic.mytopic.ks1.mycounter.mapping", "a=value.bigint, b=value.text, c=value.int");

    conn.start(connProps);

    Schema schema =
        SchemaBuilder.struct()
            .name("Kafka")
            .field("bigint", Schema.INT64_SCHEMA)
            .field("text", Schema.STRING_SCHEMA)
            .field("int", Schema.INT32_SCHEMA)
            .build();
    Struct value = new Struct(schema).put("bigint", 37L).put("text", "delete");
    SinkRecord record =
        new SinkRecord(
            "mytopic", 0, null, 37, null, value, 1234, 153000987L, TimestampType.CREATE_TIME);
    runTaskWithRecords(record);

    // The log may need a little time to be updated with our error message.
    try {
      MILLISECONDS.sleep(500);
    } catch (InterruptedException e) {
      // swallow
    }
    assertThat(logs.getAllMessagesAsString())
        .contains("Error inserting/updating row for Kafka record SinkRecord{kafkaOffset=1234")
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

    Query bad3 = makeQuery(2, "fail3", 153000987000L);
    simulacron.prime(when(bad3).then(serverError("bad thing")));

    conn.start(connectorProperties);

    SinkRecord record1 = makeRecord(42, "the answer", 153000987L, 1234);
    SinkRecord record2 = makeRecord(32, "fail", 153000987L, 1235);
    SinkRecord record3 = makeRecord(22, "success", 153000987L, 1236);
    SinkRecord record4 = makeRecord(12, "fail2", 153000987L, 1237);

    // Make a bad record in a different partition.
    SinkRecord record5 = makeRecord(1, 2, "fail3", 153000987L, 1238);
    runTaskWithRecords(record1, record2, record3, record4, record5);

    // Verify that we get an error offset for the first record that failed in partition 0 (1235)
    // even though its failure was discovered after 1237. Also, 1238 belongs to a different
    // partition, so it should be included.
    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    task.preCommit(currentOffsets);
    assertThat(currentOffsets)
        .containsOnly(
            Assertions.entry(new TopicPartition("mytopic", 0), new OffsetAndMetadata(1235L)),
            Assertions.entry(new TopicPartition("mytopic", 1), new OffsetAndMetadata(1238L)));

    assertThat(logs.getAllMessagesAsString())
        .contains("Error inserting/updating row for Kafka record SinkRecord{kafkaOffset=1237")
        .contains(
            "statement: INSERT INTO ks1.table1(a,b) VALUES (:a,:b) USING TIMESTAMP :kafka_internal_timestamp");
    InstanceState instanceState =
        (InstanceState) ReflectionUtils.getInternalState(task, "instanceState");
    assertThat(instanceState.getFailedRecordCounter("mytopic", "ks1.table1").getCount())
        .isEqualTo(3);
    assertThat(instanceState.getRecordCounter("mytopic", "ks1.table1").getCount()).isEqualTo(5);
  }

  @Test
  void should_progress_on_failure_if_threshold_met() {
    SimulacronUtils.primeTables(simulacron, schema);

    Query good1 = makeQuery(42, "the answer", 153000987000L);
    simulacron.prime(when(good1).then(noRows()));

    Query bad1 = makeQuery(32, "fail", 153000987000L);
    simulacron.prime(when(bad1).then(serverError("bad thing")).delay(500, TimeUnit.MILLISECONDS));

    Query good2 = makeQuery(22, "success", 153000987000L);
    simulacron.prime(when(good2).then(noRows()));

    Query bad2 = makeQuery(12, "fail2", 153000987000L);
    simulacron.prime(when(bad2).then(serverError("bad thing")));

    Query bad3 = makeQuery(2, "fail3", 153000987000L);
    simulacron.prime(when(bad3).then(serverError("bad thing")));

    conn.start(connectorProperties);

    SinkRecord record1 = makeRecord(42, "the answer", 153000987L, 1234);
    SinkRecord record2 = makeRecord(32, "fail", 153000987L, 1235);
    SinkRecord record3 = makeRecord(22, "success", 153000987L, 1236);
    SinkRecord record4 = makeRecord(12, "fail2", 153000987L, 1237);

    // Make a bad record in a different partition.
    SinkRecord record5 = makeRecord(1, 2, "fail3", 153000987L, 1238);
    runTaskWithRecords(record1, record2, record3, record4, record5, record5);

    // Verify that we get an error offset for the first record that failed in partition 0 (1235)
    // even though its failure was discovered after 1237. Also, 1238 belongs to a different
    // partition, so it should be included.
    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    task.preCommit(currentOffsets);
    assertThat(currentOffsets)
        .containsOnly(
            Assertions.entry(new TopicPartition("mytopic", 0), new OffsetAndMetadata(1235L)),
            Assertions.entry(new TopicPartition("mytopic", 1), new OffsetAndMetadata(1238L)));

    assertThat(logs.getAllMessagesAsString())
        .contains("Error inserting/updating row for Kafka record SinkRecord{kafkaOffset=1237")
        .contains(
            "statement: INSERT INTO ks1.table1(a,b) VALUES (:a,:b) USING TIMESTAMP :kafka_internal_timestamp");
    InstanceState instanceState =
        (InstanceState) ReflectionUtils.getInternalState(task, "instanceState");
    assertThat(instanceState.getFailedRecordCounter("mytopic", "ks1.table1").getCount())
        .isEqualTo(3);
    assertThat(instanceState.getRecordCounter("mytopic", "ks1.table1").getCount()).isEqualTo(5);
  }

  @Test
  void success_offset_with_ttl() {
    SimulacronUtils.primeTables(simulacron, schema);

    Query good1 = makeTtlQuery(22, "success", 153000987000L, 22L);
    simulacron.prime(when(good1).then(noRows()));

    Query good2 = makeTtlQuery(33, "success_2", 153000987000L, 33L);
    simulacron.prime(when(good2).then(noRows()));

    conn.start(connectorProperties);

    SinkRecord record1 = makeTtlRecord(22, "success", 153000987L, 1235L);

    SinkRecord record2 = makeTtlRecord(33, "success_2", 153000987L, 1235L);

    runTaskWithRecords(record1, record2);

    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    task.preCommit(currentOffsets);
    assertThat(currentOffsets).isEmpty();

    List<QueryLog> queryList =
        simulacron
            .node(0)
            .getLogs()
            .getQueryLogs()
            .stream()
            .filter(q -> q.getType().equals("EXECUTE"))
            .collect(Collectors.toList());
    assertThat(queryList.size()).isEqualTo(2);
    assertThat(queryList.get(0).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
    assertThat(queryList.get(1).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
  }

  @Test
  void success_offset() {
    SimulacronUtils.primeTables(simulacron, schema);

    Query good1 = makeQuery(42, "the answer", 153000987000L);
    simulacron.prime(when(good1).then(noRows()));

    Query good2 = makeQuery(22, "success", 153000987000L);
    simulacron.prime(when(good2).then(noRows()));

    conn.start(connectorProperties);

    SinkRecord record1 = makeRecord(42, "the answer", 153000987L, 1234);
    SinkRecord record2 = makeRecord(22, "success", 153000987L, 1235);
    runTaskWithRecords(record1, record2);

    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    task.preCommit(currentOffsets);
    assertThat(currentOffsets).isEmpty();

    List<QueryLog> queryList =
        simulacron
            .node(0)
            .getLogs()
            .getQueryLogs()
            .stream()
            .filter(q -> q.getType().equals("EXECUTE"))
            .collect(Collectors.toList());
    assertThat(queryList.size()).isEqualTo(2);
    assertThat(queryList.get(0).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
    assertThat(queryList.get(1).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
  }

  /** Test for KAF-72 */
  @Test
  void should_record_counters_per_topic_ks_table() {
    SimulacronUtils.primeTables(simulacron, schema);

    Query good1topic1 = makeQuery(42, "the answer", 153000987000L);
    simulacron.prime(when(good1topic1).then(noRows()));

    Query good2topic1 = makeQuery(22, "success", 153000987000L);
    simulacron.prime(when(good2topic1).then(noRows()));

    Query good1topic2 = makeTtlQuery(22, "success", 153000987000L, 22L);
    simulacron.prime(when(good1topic2).then(noRows()));

    Query good2topic2 = makeTtlQuery(33, "success_2", 153000987000L, 33L);
    simulacron.prime(when(good2topic2).then(noRows()));

    conn.start(connectorProperties);

    SinkRecord record1topic1 = makeRecord(42, "the answer", 153000987L, 1234);
    SinkRecord record2topic1 = makeRecord(22, "success", 153000987L, 1235);
    SinkRecord record1topic2 = makeTtlRecord(22, "success", 153000987L, 1235);
    SinkRecord record2topic2 = makeTtlRecord(33, "success_2", 153000987L, 1235);

    runTaskWithRecords(record1topic1, record2topic1, record1topic2, record2topic2);

    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    task.preCommit(currentOffsets);
    assertThat(currentOffsets).isEmpty();

    List<QueryLog> queryList =
        simulacron
            .node(0)
            .getLogs()
            .getQueryLogs()
            .stream()
            .filter(q -> q.getType().equals("EXECUTE"))
            .collect(Collectors.toList());
    assertThat(queryList.size()).isEqualTo(4);
    assertThat(queryList.get(0).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
    assertThat(queryList.get(1).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
    assertThat(queryList.get(2).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
    assertThat(queryList.get(3).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);

    InstanceState instanceState =
        (InstanceState) ReflectionUtils.getInternalState(task, "instanceState");
    assertThat(instanceState.getRecordCounter("mytopic", "ks1.table1").getCount()).isEqualTo(2);
    assertThat(instanceState.getRecordCounter("mytopic_with_ttl", "ks1.table1_with_ttl").getCount())
        .isEqualTo(2);
  }

  @Test
  void consistency_level() {
    SimulacronUtils.primeTables(simulacron, schema);

    Query good1 = makeQuery(42, "the answer", 153000987000L);
    simulacron.prime(when(good1).then(noRows()));

    Query good2 =
        new Query(
            "INSERT INTO ks1.table2(a,b) VALUES (:a,:b) USING TIMESTAMP :kafka_internal_timestamp",
            Collections.emptyList(),
            makeParams(22, "success", 153000987000L),
            PARAM_TYPES);
    simulacron.prime(when(good2).then(noRows()));

    conn.start(connectorProperties);

    SinkRecord record1 = makeRecord(42, "the answer", 153000987L, 1234);

    // Put the second record in "yourtopic", which has QUORUM CL.
    SinkRecord record2 =
        new SinkRecord(
            "yourtopic",
            0,
            null,
            22,
            null,
            "success",
            1235L,
            153000987L,
            TimestampType.CREATE_TIME);
    runTaskWithRecords(record1, record2);

    List<QueryLog> queryList =
        simulacron
            .node(0)
            .getLogs()
            .getQueryLogs()
            .stream()
            .filter(q -> q.getType().equals("EXECUTE"))
            .collect(Collectors.toList());
    assertThat(queryList.size()).isEqualTo(2);

    for (QueryLog queryLog : queryList) {
      if (queryLog.getQuery().contains("table1")) {
        assertThat(queryLog.getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
      } else if (queryLog.getQuery().contains("table2")) {
        assertThat(queryLog.getConsistency()).isEqualTo(ConsistencyLevel.QUORUM);
      } else {
        fail("%s is not for table1 nor table2!", queryLog.toString());
      }
    }
  }

  @Test
  void undefined_topic() {
    SimulacronUtils.primeTables(simulacron, schema);

    Query good1 = makeQuery(42, "the answer", 153000987000L);
    simulacron.prime(when(good1).then(noRows()));

    conn.start(connectorProperties);

    SinkRecord goodRecord = makeRecord(42, "the answer", 153000987L, 1234);

    SinkRecord badRecord = new SinkRecord("unknown", 0, null, 42L, null, 42, 1234L);
    runTaskWithRecords(goodRecord, badRecord);
    assertThat(logs.getAllMessagesAsString())
        .contains("Error inserting/updating row for Kafka record SinkRecord{kafkaOffset=1234")
        .contains(
            "Connector has no configuration for record topic 'unknown'. Please update the configuration and restart.");

    // Verify that the insert for good1 was issued.
    List<QueryLog> queryList =
        simulacron
            .node(0)
            .getLogs()
            .getQueryLogs()
            .stream()
            .filter(q -> q.getType().equals("EXECUTE"))
            .collect(Collectors.toList());
    byte[] secondParam = new byte[10];
    ((Execute) queryList.get(0).getFrame().message)
        .options
        .positionalValues
        .get(1)
        .get(secondParam);
    assertThat(new String(secondParam, StandardCharsets.UTF_8)).isEqualTo("the answer");
  }

  @Test
  void batch_requests() {
    // Insert 5 records: 2 from mytopic, 3 from yourtopic. Verify that they batch properly and
    // with the correct CLs.

    // Even though we will not be executing simple statements in this test, we must specify
    // that we will so that Simulacron handles preparing our statement properly.
    SimulacronUtils.primeTables(simulacron, schema);
    Query good1 = makeQuery(42, "the answer", 153000987000L);
    simulacron.prime(when(good1).then(noRows()));
    Query good2 =
        new Query(
            "INSERT INTO ks1.table2(a,b) VALUES (:a,:b) USING TIMESTAMP :kafka_internal_timestamp",
            Collections.emptyList(),
            makeParams(42, "topic2 success1", 153000987000L),
            PARAM_TYPES);
    simulacron.prime(when(good2).then(noRows()));

    conn.start(connectorProperties);

    SinkRecord goodRecord1 = makeRecord(42, "the answer", 153000987L, 1234);
    SinkRecord goodRecord2 = makeRecord(42, "the second answer", 153000987L, 1234);
    SinkRecord goodRecord3 =
        new SinkRecord(
            "yourtopic",
            0,
            null,
            42,
            null,
            "topic2 success1",
            1235L,
            153000987L,
            TimestampType.CREATE_TIME);
    SinkRecord goodRecord4 =
        new SinkRecord(
            "yourtopic",
            0,
            null,
            42,
            null,
            "topic2 success2",
            1235L,
            153000987L,
            TimestampType.CREATE_TIME);
    SinkRecord goodRecord5 =
        new SinkRecord(
            "yourtopic",
            0,
            null,
            42,
            null,
            "topic2 success3",
            1235L,
            153000987L,
            TimestampType.CREATE_TIME);

    // The order of records shouldn't matter here, but we try to mix things up to emulate
    // a real workload.
    runTaskWithRecords(goodRecord1, goodRecord3, goodRecord2, goodRecord4, goodRecord5);

    // Verify that we issued two batch requests, one at LOCAL_ONE (for table1/mytopic) and
    // one at QUORUM (for table2/yourtopic). There's seem pretty gnarly unwrapping of request
    // info. We distinguish one batch from the other based on the number of statements in the
    // batch.
    List<QueryLog> queryList =
        simulacron
            .node(0)
            .getLogs()
            .getQueryLogs()
            .stream()
            .filter(q -> q.getType().equals("BATCH"))
            .collect(Collectors.toList());
    Map<ConsistencyLevel, Integer> queryInfo =
        queryList
            .stream()
            .map(queryLog -> (Batch) queryLog.getFrame().message)
            .collect(
                Collectors.toMap(
                    message -> ConsistencyLevel.fromCode(message.consistency),
                    message -> message.values.size()));
    assertThat(queryInfo)
        .containsOnly(entry(ConsistencyLevel.LOCAL_ONE, 2), entry(ConsistencyLevel.QUORUM, 3));
  }

  @Test
  void fail_batch_request() {
    // Test single topic, multiple Kafka partitions, single C* partition, fail DSE batch request:
    // single batch statement failure causes failureOffsets for all SinkRecord's in the batch.
    SimulacronUtils.primeTables(simulacron, schema);
    Query good1 = makeQuery(42, "the answer", 153000987000L);
    simulacron.prime(when(good1).then(noRows()));

    Statement s1 =
        new Statement(
            "INSERT INTO ks1.table1(a,b) VALUES (:a,:b) USING TIMESTAMP :kafka_internal_timestamp",
            PARAM_TYPES,
            makeParams(42, "the answer", 153000987000L));
    Statement s2 =
        new Statement(
            "INSERT INTO ks1.table1(a,b) VALUES (:a,:b) USING TIMESTAMP :kafka_internal_timestamp",
            PARAM_TYPES,
            makeParams(42, "the second answer", 153000987000L));
    com.datastax.oss.simulacron.common.request.Batch batchRequest =
        new com.datastax.oss.simulacron.common.request.Batch(
            Arrays.asList(s1, s2), new String[] {"LOCAL_ONE", "LOCAL_ONE"});

    simulacron.prime(when(batchRequest).then(serverError("bad thing")));

    SinkRecord record1 = makeRecord(0, 42, "the answer", 153000987000L, 1234);
    SinkRecord record2 = makeRecord(1, 42, "the second answer", 153000987000L, 8888);

    conn.start(connectorProperties);
    runTaskWithRecords(record1, record2);

    // Verify that one Batch request was issued, and no EXECUTE requests:
    long batchRequestCount =
        simulacron
            .node(0)
            .getLogs()
            .getQueryLogs()
            .stream()
            .filter(q -> q.getType().equals("BATCH"))
            .count();
    long executeRequestCount =
        simulacron
            .node(0)
            .getLogs()
            .getQueryLogs()
            .stream()
            .filter(q -> q.getType().equals("EXECUTE"))
            .count();
    assertThat(batchRequestCount).isEqualTo(1L);
    assertThat(executeRequestCount).isEqualTo(0L);

    // Verify that we get error offsets for both records.
    Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    task.preCommit(currentOffsets);
    assertThat(currentOffsets)
        .containsOnly(
            Assertions.entry(new TopicPartition("mytopic", 0), new OffsetAndMetadata(1234L)),
            Assertions.entry(new TopicPartition("mytopic", 1), new OffsetAndMetadata(8888L)));

    assertThat(logs.getAllMessagesAsString())
        .contains("Error inserting/updating row for Kafka record SinkRecord{kafkaOffset=1234")
        .contains("Error inserting/updating row for Kafka record SinkRecord{kafkaOffset=8888");
  }

  private void runTaskWithRecords(SinkRecord... records) {
    List<Map<String, String>> taskProps = conn.taskConfigs(1);
    task.start(taskProps.get(0));
    task.put(Arrays.asList(records));
  }
}
