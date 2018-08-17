/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.simulacron;

import static com.datastax.dsbulk.commons.tests.logging.StreamType.STDERR;
import static com.datastax.dsbulk.commons.tests.logging.StreamType.STDOUT;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.serverError;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import com.datastax.kafkaconnector.DseSinkConnector;
import com.datastax.kafkaconnector.DseSinkTask;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.RejectScope;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.assertj.core.api.Assertions;
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
@SimulacronConfig(version = "5.0.8")
class SimpleEndToEndSimulacronIT {

  private static final String INSERT_STATEMENT =
      "INSERT INTO ks1.table1(a,b) VALUES (:a,:b) USING TIMESTAMP :kafka_internal_timestamp";
  private final BoundCluster simulacron;
  private final SimulacronUtils.Keyspace schema;
  private final DseSinkConnector conn;
  private final DseSinkTask task;
  private final LogInterceptor logs;
  private final Map<String, String> connectorProperties;
  private static final ImmutableMap<String, String> PARAM_TYPES =
      ImmutableMap.<String, String>builder()
          .put("a", "int")
          .put("b", "varchar")
          .put("kafka_internal_timestamp", "bigint")
          .build();

  @SuppressWarnings("unused")
  SimpleEndToEndSimulacronIT(
      BoundCluster simulacron,
      @LogCapture LogInterceptor logs,
      @StreamCapture(STDOUT) StreamInterceptor stdOut,
      @StreamCapture(STDERR) StreamInterceptor stdErr) {

    this.simulacron = simulacron;
    this.logs = logs;

    InetSocketAddress node = simulacron.dc(0).node(0).inetSocketAddress();
    String hostname = node.getHostName();
    String port = Integer.toString(node.getPort());

    SinkTaskContext taskContext = mock(SinkTaskContext.class);
    task = new DseSinkTask();
    task.initialize(taskContext);

    schema =
        new SimulacronUtils.Keyspace(
            "ks1",
            new Table("table1", new Column("a", DataTypes.INT), new Column("b", DataTypes.TEXT)),
            new Table("table2", new Column("a", DataTypes.INT), new Column("b", DataTypes.TEXT)));
    conn = new DseSinkConnector();

    connectorProperties =
        ImmutableMap.<String, String>builder()
            .put("name", "myinstance")
            .put("contactPoints", hostname)
            .put("port", port)
            .put("loadBalancing.localDc", "dc1")
            .put("topic.mytopic.keyspace", "ks1")
            .put("topic.mytopic.table", "table1")
            .put("topic.mytopic.mapping", "a=key, b=value")
            .put("topic.yourtopic.keyspace", "ks1")
            .put("topic.yourtopic.table", "table2")
            .put("topic.yourtopic.mapping", "a=key, b=value")
            .put("topic.yourtopic.consistencyLevel", "QUORUM")
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

  @Test
  void fail_prepare() {
    SimulacronUtils.primeTables(simulacron, schema);
    Query bad1 = makeQuery(32, "fail", 153000987000L);
    simulacron.prime(when(bad1).then(serverError("bad thing")).applyToPrepare());

    assertThatThrownBy(() -> conn.start(connectorProperties))
        .isInstanceOf(RuntimeException.class)
        .hasMessageStartingWith("Prepare failed for statement: " + INSERT_STATEMENT);
  }

  @Test
  void fail_connect() throws InterruptedException {
    SimulacronUtils.primeTables(simulacron, schema);

    // Stop the node from listening.
    simulacron.node(0).rejectConnections(0, RejectScope.UNBIND);

    Thread t = null;
    try {
      AtomicBoolean done = new AtomicBoolean(false);
      t =
          new Thread(
              () -> {
                conn.start(connectorProperties);
                done.set(true);
              });
      t.start();

      // Wait upto 1 second for the thread to complete; it shouldn't complete.
      t.join(1000);
      assertThat(done.get()).isFalse();

      // Start the node listener. The connector should connect successfully without error.
      simulacron.node(0).acceptConnections();
      t.join();
      assertThat(done.get()).isTrue();
    } finally {
      if (t != null) {
        t.interrupt();
        t.join();
      }
    }

    assertThat(logs.getAllMessagesAsString())
        .contains("DSE connection failed; retrying in 1 seconds");
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
        .contains("Error inserting row for Kafka record SinkRecord{kafkaOffset=1237")
        .contains(
            "statement: INSERT INTO ks1.table1(a,b) VALUES (:a,:b) USING TIMESTAMP :kafka_internal_timestamp");
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
    assertThat(queryList.get(0).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
    assertThat(queryList.get(1).getConsistency()).isEqualTo(ConsistencyLevel.LOCAL_ONE);
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

  private static Map<String, Object> makeParams(int a, String b, long timestamp) {
    return ImmutableMap.<String, Object>builder()
        .put("a", a)
        .put("b", b)
        .put("kafka_internal_timestamp", timestamp)
        .build();
  }

  private void runTaskWithRecords(SinkRecord... records) {
    List<Map<String, String>> taskProps = conn.taskConfigs(1);
    task.start(taskProps.get(0));
    task.put(Arrays.asList(records));
  }
}
