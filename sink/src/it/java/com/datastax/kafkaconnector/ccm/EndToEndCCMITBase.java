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
import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.OSS;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.CCMExtension;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMRequirements;
import com.datastax.dsbulk.commons.tests.utils.Version;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CCMExtension.class)
@CCMRequirements(compatibleTypes = {DSE, DDAC, OSS})
public abstract class EndToEndCCMITBase extends ITConnectorBase {
  protected final boolean hasDateRange;
  protected final CCMCluster ccm;
  protected final CqlSession session;

  protected EndToEndCCMITBase(CCMCluster ccm, CqlSession session) {
    super(ccm.getInitialContactPoints(), ccm.getBinaryPort(), ccm.getDC(1), session);
    this.ccm = ccm;
    this.session = session;

    // DSE 5.0 doesn't have the DateRange type; neither does DDAC, so we need to account for that
    // in our testing.
    hasDateRange =
        ccm.getClusterType() == DSE
            && Version.isWithinRange(Version.parse("5.1.0"), null, ccm.getVersion());
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
                    + "secondsCol timestamp,"
                    + "loaded_at timeuuid,"
                    + "loaded_at2 timeuuid"
                    + ")")
            .setTimeout(Duration.ofSeconds(10))
            .build());

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE IF NOT EXISTS pk_value ("
                    + "my_pk bigint PRIMARY KEY,"
                    + "my_value boolean"
                    + ")")
            .setTimeout(Duration.ofSeconds(10))
            .build());

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE IF NOT EXISTS pk_value_with_timeuuid ("
                    + "my_pk bigint PRIMARY KEY,"
                    + "my_value boolean,"
                    + "loaded_at timeuuid"
                    + ")")
            .setTimeout(Duration.ofSeconds(10))
            .build());

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE IF NOT EXISTS small_simple ("
                    + "bigintCol bigint PRIMARY KEY, "
                    + "booleanCol boolean, "
                    + "intCol int"
                    + ")")
            .setTimeout(Duration.ofSeconds(10))
            .build());

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE IF NOT EXISTS small_compound ("
                    + "bigintCol bigint, "
                    + "booleanCol boolean, "
                    + "intCol int,"
                    + "PRIMARY KEY (bigintcol, booleancol)"
                    + ")")
            .setTimeout(Duration.ofSeconds(10))
            .build());
  }

  @BeforeEach
  void truncateTable() {
    session.execute("TRUNCATE types");
    session.execute("TRUNCATE pk_value");
    session.execute("TRUNCATE pk_value_with_timeuuid");
    session.execute("TRUNCATE small_simple");
    session.execute("TRUNCATE small_compound");
  }

  protected void assertTtl(int ttlValue, Number expectedTtlValue) {
    if (expectedTtlValue.equals(0)) {
      assertThat(ttlValue).isEqualTo(expectedTtlValue.intValue());
    } else {
      // actual ttl value can be less that or equal to expectedTtlValue because some time may elapse
      // between the moment the record was inserted and retrieved from db.
      assertThat(ttlValue).isLessThanOrEqualTo(expectedTtlValue.intValue()).isGreaterThan(0);
    }
  }
}
