/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.ccm;

import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DDAC;
import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DSE;
import static org.mockito.Mockito.mock;

import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMRequirements;
import com.datastax.kafkaconnector.ConnectorSettingsProvider;
import com.datastax.kafkaconnector.DseSinkConnector;
import com.datastax.kafkaconnector.DseSinkTask;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("medium")
@CCMRequirements(compatibleTypes = {DSE, DDAC})
abstract class EndToEndCCMITBase {
  final CqlSession session;
  final String keyspaceName;

  DseSinkConnector conn = new DseSinkConnector();
  DseSinkTask task = new DseSinkTask();
  ConnectorSettingsProvider cs;

  EndToEndCCMITBase(ConnectorSettingsProvider connectorSettingsProvider, CqlSession session) {
    this.cs = connectorSettingsProvider;
    this.session = session;

    keyspaceName = session.getKeyspace().orElse(CqlIdentifier.fromInternal("unknown")).asInternal();
    SinkTaskContext taskContext = mock(SinkTaskContext.class);
    task.initialize(taskContext);
  }

  @BeforeAll
  void createTables() {
    session.execute("CREATE TYPE IF NOT EXISTS myudt (udtmem1 int, udtmem2 text)");
    session.execute("CREATE TYPE IF NOT EXISTS mycomplexudt (a int, b text, c list<int>)");
    session.execute("CREATE TYPE IF NOT EXISTS mybooleanudt (udtmem1 boolean, udtmem2 text)");
    String withDateRange = cs.hasDateRange() ? "dateRangeCol 'DateRangeType', " : "";
    String withGeoTypes =
        cs.isDse()
            ? "pointCol 'PointType', linestringCol 'LineStringType', polygonCol 'PolygonType', "
            : "";
    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE IF NOT EXISTS types ("
                    + "bigintCol bigint PRIMARY KEY, "
                    + "booleanCol boolean, "
                    + "doubleCol double, "
                    + "floatCol float, "
                    + "intCol int, "
                    + "smallintCol smallint, "
                    + "textCol text, "
                    + "tinyIntCol tinyint, "
                    + "mapCol map<text, int>, "
                    + "mapNestedCol frozen<map<text, map<int, text>>>, "
                    + "listCol list<int>, "
                    + "listNestedCol frozen<list<set<int>>>, "
                    + "setCol set<int>, "
                    + "setNestedCol frozen<set<list<int>>>, "
                    + "tupleCol tuple<smallint, int, int>, "
                    + "udtCol frozen<myudt>, "
                    + "udtFromListCol frozen<myudt>, "
                    + "booleanUdtCol frozen<mybooleanudt>, "
                    + "booleanUdtFromListCol frozen<mybooleanudt>, "
                    + "listUdtCol frozen<mycomplexudt>, "
                    + "blobCol blob, "
                    + withGeoTypes
                    + withDateRange
                    + "dateCol date, "
                    + "timeCol time, "
                    + "timestampCol timestamp, "
                    + "secondsCol timestamp"
                    + ")")
            .setTimeout(Duration.ofSeconds(10))
            .build());
  }

  @BeforeEach
  void truncateTable() {
    session.execute("TRUNCATE types");
  }

  @AfterEach
  void stopConnector() {
    task.stop();
    conn.stop();
  }

  void runTaskWithRecords(SinkRecord... records) {
    initConnectorAndTask();
    task.put(Arrays.asList(records));
  }

  void initConnectorAndTask() {
    List<Map<String, String>> taskProps = conn.taskConfigs(1);
    task.start(taskProps.get(0));
  }

  Map<String, String> makeConnectorProperties(Map<String, String> extras) {
    return makeConnectorProperties("bigintcol=value", extras);
  }

  Map<String, String> makeConnectorProperties(String mappingString, Map<String, String> extras) {
    return makeConnectorProperties(mappingString, "types", extras);
  }

  Map<String, String> makeConnectorProperties(
      String mappingString, String tableName, Map<String, String> extras) {
    return makeConnectorProperties(mappingString, tableName, extras, "mytopic");
  }

  Map<String, String> makeConnectorProperties(
      String mappingString, String tableName, Map<String, String> extras, String topicName) {
    Map<String, String> props = new HashMap<>();

    props.put("name", "myinstance");
    props.put(
        "contactPoints",
        cs.getContactPoints()
            .stream()
            .map(addr -> String.format("%s", ((InetSocketAddress) addr.resolve()).getHostString()))
            .collect(Collectors.joining(",")));
    props.put("port", String.format("%d", cs.getBinaryPort()));
    props.put("loadBalancing.localDc", cs.getLocalDc());
    props.put("topics", topicName);
    props.put(
        String.format("topic.%s.%s.%s.mapping", topicName, keyspaceName, tableName), mappingString);

    if (extras != null) {
      props.putAll(extras);
    }
    return props;
  }
}
