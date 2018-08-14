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
import static org.mockito.Mockito.mock;

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
import com.datastax.kafkaconnector.DseSinkConnector;
import com.datastax.kafkaconnector.DseSinkTask;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SimulacronExtension.class)
@ExtendWith(LogInterceptingExtension.class)
@ExtendWith(StreamInterceptingExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SimpleEndToEndSimulacronIT {

  private final BoundCluster simulacron;
  private final LogInterceptor logs;
  private final StreamInterceptor stdOut;
  private final StreamInterceptor stdErr;
  private final String hostname;
  private final String port;

  private Path unloadDir;
  private Path logDir;

  private DseSinkConnector conn = new DseSinkConnector();
  private DseSinkTask task = new DseSinkTask();
  private SinkTaskContext taskContext;

  SimpleEndToEndSimulacronIT(
      BoundCluster simulacron,
      @LogCapture LogInterceptor logs,
      @StreamCapture(STDOUT) StreamInterceptor stdOut,
      @StreamCapture(STDERR) StreamInterceptor stdErr) {
    this.simulacron = simulacron;
    this.logs = logs;
    this.stdOut = stdOut;
    this.stdErr = stdErr;
    InetSocketAddress node = simulacron.dc(0).node(0).inetSocketAddress();
    hostname = node.getHostName();
    port = Integer.toString(node.getPort());

    taskContext = mock(SinkTaskContext.class);
    task.initialize(taskContext);
  }

  @BeforeEach
  void resetPrimes() {
    simulacron.clearPrimes(true);
  }

  //  @BeforeEach
  //  void setUpDirs() throws IOException {
  //    logDir = createTempDirectory("logs");
  //    unloadDir = createTempDirectory("unload");
  //  }
  //
  //  @AfterEach
  //  void deleteDirs() {
  //    deleteDirectory(logDir);
  //    deleteDirectory(unloadDir);
  //  }
  //
  //  @AfterEach
  //  void resetLogbackConfiguration() throws JoranException {
  //    LogUtils.resetLogbackConfiguration();
  //  }
  //
  @Test
  void failure_offset() {
    SimulacronUtils.primeTables(
        simulacron,
        new SimulacronUtils.Keyspace(
            "ks1",
            new Table("table1", new Column("a", DataTypes.INT), new Column("b", DataTypes.TEXT))));

    Map<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("a", "int");
    paramTypes.put("b", "varchar");
    Query when =
        new Query(
            "INSERT INTO ks1.table1 (a, b) VALUES (:a, :b)",
            Collections.emptyList(),
            ImmutableMap.<String, Object>builder().put("a", 42).put("b", "the answer").build(),
            paramTypes);
    SuccessResult then = new SuccessResult(new ArrayList<>(), new HashMap<>());
    RequestPrime insert1 = new RequestPrime(when, then);

    simulacron.prime(new Prime(insert1));

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
  }

  //  @Test
  //  void error_load_missing_field() throws Exception {
  //
  //    SimulacronUtils.primeTables(
  //        simulacron,
  //        new SimulacronUtils.Keyspace(
  //            "ks1",
  //            new Table(
  //                "table1",
  //                new Column("a", cint()),
  //                new Column("b", varchar()),
  //                new Column("c", cboolean()),
  //                new Column("d", cint()))));
  //
  //    String[] args = {
  //      "load",
  //      "--log.directory",
  //      escapeUserInput(logDir),
  //      "--log.maxErrors",
  //      "2",
  //      "--log.verbosity",
  //      "2",
  //      "-header",
  //      "true",
  //      "--connector.csv.url",
  //      escapeUserInput(getClass().getResource("/missing-extra.csv")),
  //      "--driver.query.consistency",
  //      "ONE",
  //      "--driver.hosts",
  //      hostname,
  //      "--driver.port",
  //      port,
  //      "--driver.pooling.local.connections",
  //      "1",
  //      "--schema.query",
  //      "INSERT INTO ks1.table1 (a,b,c,d) VALUES (:a,:b,:c,:d)",
  //      "--schema.mapping",
  //      "A = a, B = b, C = c, D = d",
  //      "--schema.allowMissingFields",
  //      "false"
  //    };
  //    int status = new DataStaxBulkLoader(args).run();
  //    assertThat(status).isEqualTo(DataStaxBulkLoader.STATUS_ABORTED_TOO_MANY_ERRORS);
  //    assertThat(logs.getAllMessagesAsString())
  //        .contains("aborted: Too many errors, the maximum allowed is 2")
  //        .contains("Records: total: 3, successful: 0, failed: 3");
  //    Path logPath = Paths.get(System.getProperty(LogSettings.OPERATION_DIRECTORY_KEY));
  //    validateBadOps(3, logPath);
  //    validateExceptionsLog(
  //        3,
  //        "Required field D (mapped to column d) was missing from record",
  //        "mapping-errors.log",
  //        logPath);
  //  }

  private void runTaskWithRecords(SinkRecord... records) {
    List<Map<String, String>> taskProps = conn.taskConfigs(1);
    task.start(taskProps.get(0));
    task.put(Arrays.asList(records));
  }

  private Map<String, String> makeConnectorProperties(String myTopicMappingString) {
    return makeConnectorProperties(myTopicMappingString, null);
  }

  private Map<String, String> makeConnectorProperties(
      String myTopicMappingString, Map<String, String> extras) {
    ImmutableMap.Builder<String, String> builder =
        ImmutableMap.<String, String>builder()
            .put("name", "myinstance")
            .put("contactPoints", hostname)
            .put("port", port)
            .put("loadBalancing.localDc", "Cassandra")
            .put("topic.mytopic.keyspace", "ks1")
            .put("topic.mytopic.table", "table1")
            .put("topic.mytopic.mapping", myTopicMappingString);

    if (extras != null) {
      builder.putAll(extras);
    }
    return builder.build();
  }
}
