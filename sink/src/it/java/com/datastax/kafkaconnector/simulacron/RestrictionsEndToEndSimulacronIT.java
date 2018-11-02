/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.simulacron;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptingExtension;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronExtension;
import com.datastax.dsbulk.commons.tests.simulacron.SimulacronUtils;
import com.datastax.kafkaconnector.DseSinkConnector;
import com.datastax.kafkaconnector.DseSinkTask;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;

@ExtendWith(SimulacronExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@ExtendWith(LogInterceptingExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RestrictionsEndToEndSimulacronIT {
  private final BoundCluster simulacron;
  private final SimulacronUtils.Keyspace schema;
  private final DseSinkConnector conn;
  private final DseSinkTask task;
  private final Map<String, String> connectorProperties;

  RestrictionsEndToEndSimulacronIT(BoundCluster simulacron) {

    this.simulacron = simulacron;

    InetSocketAddress node = simulacron.dc(0).node(0).inetSocketAddress();
    String hostname = node.getHostName();
    String port = Integer.toString(node.getPort());

    SinkTaskContext taskContext = mock(SinkTaskContext.class);
    task = new DseSinkTask();
    task.initialize(taskContext);

    schema =
        new SimulacronUtils.Keyspace(
            "ks1",
            new SimulacronUtils.Table(
                "table1",
                new SimulacronUtils.Column("a", DataTypes.INT),
                new SimulacronUtils.Column("b", DataTypes.TEXT)),
            new SimulacronUtils.Table(
                "table2",
                new SimulacronUtils.Column("a", DataTypes.INT),
                new SimulacronUtils.Column("b", DataTypes.TEXT)),
            new SimulacronUtils.Table(
                "mycounter",
                new SimulacronUtils.Column("a", DataTypes.INT),
                new SimulacronUtils.Column("b", DataTypes.TEXT),
                new SimulacronUtils.Column("c", DataTypes.COUNTER)));
    conn = new DseSinkConnector();

    connectorProperties =
        ImmutableMap.<String, String>builder()
            .put("name", "myinstance")
            .put("contactPoints", hostname)
            .put("port", port)
            .put("loadBalancing.localDc", "dc1")
            .put("topic.mytopic.ks1.table1.mapping", "a=key, b=value")
            .put("topic.yourtopic.ks1.table2.mapping", "a=key, b=value")
            .put("topic.yourtopic.ks1.table2.consistencyLevel", "QUORUM")
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
  void stopConnector() {
    task.stop();
    conn.stop();
  }

  @Test
  void fail_load_to_oss_cassandra() {
    SimulacronUtils.primeTables(simulacron, schema);

    // DAT-322: absence of dse_version + absence of a DSE patch in release_version => OSS C*
    Map<String, Object> overrides = new HashMap<>();
    overrides.put("release_version", "4.0.0");
    overrides.put("dse_version", null);
    SimulacronUtils.primeSystemLocal(simulacron, overrides);

    conn.start(connectorProperties);
    assertThatThrownBy(this::runTaskWithRecords)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("offending nodes: /127.0.");
  }

  @Test
  void should_allow_load_to_ddac() {
    SimulacronUtils.primeTables(simulacron, schema);

    // DAT-322: absence of dse_version + presence of a DSE patch in release_version => DDAC
    // (DataStax Distribution of Apache Cassandra).
    Map<String, Object> overrides = new HashMap<>();
    overrides.put("release_version", "4.0.0.2284");
    overrides.put("dse_version", null);
    SimulacronUtils.primeSystemLocal(simulacron, overrides);
    conn.start(connectorProperties);

    // When we run, no exception should occur.
    runTaskWithRecords();
  }

  @Test
  void should_allow_load_to_dse() {
    SimulacronUtils.primeTables(simulacron, schema);

    // DAT-322: absence of dse_version + presence of a DSE patch in release_version => DDAC
    // (DataStax Distribution of Apache Cassandra).
    Map<String, Object> overrides = new HashMap<>();
    overrides.put("release_version", "4.0.0");
    overrides.put("dse_version", "5.0.8");
    SimulacronUtils.primeSystemLocal(simulacron, overrides);
    conn.start(connectorProperties);

    // When we run, no exception should occur.
    runTaskWithRecords();
  }

  private void runTaskWithRecords(SinkRecord... records) {
    List<Map<String, String>> taskProps = conn.taskConfigs(1);
    task.start(taskProps.get(0));
    task.put(Arrays.asList(records));
  }
}
