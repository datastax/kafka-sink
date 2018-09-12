/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.ccm;

import static org.mockito.Mockito.mock;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.CCMExtension;
import com.datastax.dsbulk.commons.tests.utils.Version;
import com.datastax.kafkaconnector.DseSinkConnector;
import com.datastax.kafkaconnector.DseSinkTask;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.time.Duration;
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
abstract class EndToEndCCMITBase {
  final boolean dse50;
  final CCMCluster ccm;
  final CqlSession session;

  DseSinkConnector conn = new DseSinkConnector();
  DseSinkTask task = new DseSinkTask();

  EndToEndCCMITBase(CCMCluster ccm, CqlSession session) {
    this.ccm = ccm;
    this.session = session;

    // DSE 5.0 doesn't have the DateRange type, so we need to account for that in our testing.
    dse50 = Version.isWithinRange(Version.parse("5.0.0"), Version.parse("5.1.0"), ccm.getVersion());

    SinkTaskContext taskContext = mock(SinkTaskContext.class);
    task.initialize(taskContext);
  }

  @BeforeAll
  void createTables() {
    session.execute("CREATE TYPE IF NOT EXISTS myudt (udtmem1 int, udtmem2 text)");
    session.execute("CREATE TYPE IF NOT EXISTS mybooleanudt (udtmem1 boolean, udtmem2 text)");
    String withDateRange = dse50 ? "" : ", dateRangeCol 'DateRangeType'";
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
                    + "blobCol blob, "
                    + "pointCol 'PointType', "
                    + "linestringCol 'LineStringType', "
                    + "polygonCol 'PolygonType', "
                    + "dateCol date, "
                    + "timeCol time, "
                    + "timestampCol timestamp, "
                    + "secondsCol timestamp"
                    + withDateRange
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
}
