/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.ccm;

import static com.datastax.kafkaconnector.config.DseSinkConfig.withDriverPrefix;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.kafkaconnector.state.InstanceState;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
class ConnectorSettingsCCMIT extends EndToEndCCMITBase {

  ConnectorSettingsCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
  }

  /** Test for KAF-135 */
  @Test
  void should_load_settings_from_dse_reference_conf() {
    // given (connector mapping need to be defined)
    conn.start(makeConnectorProperties("bigintcol=value.bigint, doublecol=value.double"));
    initConnectorAndTask();

    // when
    InstanceState instanceState = task.getInstanceState();

    // then setting from dse-reference.conf should be defined
    assertThat(
            instanceState
                .getSession()
                .getContext()
                .getConfig()
                .getDefaultProfile()
                .getInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE))
        .isGreaterThan(0);
  }

  @Test
  void should_insert_when_using_java_driver_contact_points_setting() {
    Map<String, String> connectorProperties =
        makeConnectorPropertiesWithoutContactPointsAndPort("bigintcol=key, listcol=value");
    // use single datastax-java-driver prefixed property that carry host:port
    connectorProperties.put(
        withDriverPrefix(DefaultDriverOption.CONTACT_POINTS),
        getContactPoints()
            .stream()
            .map(
                a -> {
                  InetSocketAddress inetSocketAddress = (InetSocketAddress) a.resolve();
                  return String.format(
                      "%s:%d", inetSocketAddress.getHostString(), inetSocketAddress.getPort());
                })
            .collect(Collectors.joining(",")));

    conn.start(connectorProperties);

    SinkRecord record = new SinkRecord("mytopic", 0, null, 98761234L, null, "[42, 37]", 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol, listcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    assertThat(row.getList("listcol", Integer.class)).isEqualTo(Arrays.asList(42, 37));
  }
}
