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

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.CCMExtension;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMRequirements;
import com.datastax.dsbulk.commons.tests.utils.Version;
import com.datastax.kafkaconnector.DseSinkConnector;
import com.datastax.kafkaconnector.DseSinkTask;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
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
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CCMExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("medium")
@CCMRequirements(compatibleTypes = {DSE, DDAC})
abstract class EndToEndCCMITBase {
  final boolean hasDateRange;
  final CCMCluster ccm;
  final CqlSession session;
  final String keyspaceName;

  DseSinkConnector conn = new DseSinkConnector();
  DseSinkTask task = new DseSinkTask();

  EndToEndCCMITBase(CCMCluster ccm, CqlSession session) {
    this.ccm = ccm;
    this.session = session;

    keyspaceName = session.getKeyspace().orElse(CqlIdentifier.fromInternal("unknown")).asInternal();

    // DSE 5.0 doesn't have the DateRange type; neither does DDAC, so we need to account for that
    // in our testing.
    hasDateRange =
        ccm.getClusterType() == DSE
            && Version.isWithinRange(Version.parse("5.1.0"), null, ccm.getVersion());

    SinkTaskContext taskContext = mock(SinkTaskContext.class);
    task.initialize(taskContext);
  }

  @BeforeAll
  void createTables() {
    session.execute("CREATE TYPE IF NOT EXISTS myudt (udtmem1 int, udtmem2 text)");
    session.execute("CREATE TYPE IF NOT EXISTS mycomplexudt (a int, b text, c list<int>)");
    session.execute("CREATE TYPE IF NOT EXISTS mybooleanudt (udtmem1 boolean, udtmem2 text)");
    String withDateRange = hasDateRange ? "dateRangeCol 'DateRangeType', " : "";
    String withGeoTypes =
        ccm.getClusterType() == DSE
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
            .withTimeout(Duration.ofSeconds(10))
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
    List<Map<String, String>> taskProps = conn.taskConfigs(1);
    task.start(taskProps.get(0));
    task.put(Arrays.asList(records));
  }

  Map<String, String> makeConnectorProperties(Map<String, String> extras) {
    return makeConnectorProperties("bigintcol=value", extras);
  }

  Map<String, String> makeConnectorProperties(String mappingString, Map<String, String> extras) {
    return makeConnectorProperties(mappingString, "types", extras);
  }

  Map<String, String> makeConnectorProperties(
      String mappingString, String tableName, Map<String, String> extras) {
    Map<String, String> props = new HashMap<>();

    props.put("name", "myinstance");
    props.put(
        "contactPoints",
        ccm.getInitialContactPoints()
            .stream()
            .map(addr -> String.format("%s", addr.getHostAddress()))
            .collect(Collectors.joining(",")));
    props.put("port", String.format("%d", ccm.getBinaryPort()));
    props.put("loadBalancing.localDc", ccm.getDC(1));
    props.put(String.format("topic.mytopic.%s.%s.mapping", keyspaceName, tableName), mappingString);

    if (extras != null) {
      props.putAll(extras);
    }
    return props;
  }
}
