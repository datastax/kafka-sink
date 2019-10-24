/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.ccm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.CCMExtension;
import com.datastax.kafkaconnector.config.DseSinkConfig;
import com.datastax.kafkaconnector.state.LifeCycleManager;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Tag("medium")
@ExtendWith(CCMExtension.class)
public class LifeCycleManagerIT {

  private final CCMCluster ccm;

  public LifeCycleManagerIT(CCMCluster ccm) {
    this.ccm = ccm;
  }

  @Test
  void should_build_dse_session_with_unresolved_contact_points() {
    // given
    String contactPointDns = "localhost";
    Map<String, String> config =
        ImmutableMap.of(
            "contactPoints", contactPointDns,
            "loadBalancing.localDc", ccm.getDC(1),
            "port", String.valueOf(ccm.getBinaryPort()));
    DseSinkConfig dseSinkConfig = new DseSinkConfig(config);

    // when
    ResultSet set;
    try (CqlSession session = LifeCycleManager.buildDseSession(dseSinkConfig)) {
      // then
      set = session.execute("select * from system.local");
      assertThat(set).isNotNull();
      // and endPoint uses unresolved DNS address
      EndPoint endPoint = getEndPoint(session);
      assertThat(endPoint.toString())
          .isEqualTo(String.format("%s:%d", contactPointDns, ccm.getBinaryPort()));
      assertTrue(((InetSocketAddress) getEndPoint(session).resolve()).isUnresolved());
    }
  }

  @Test
  void should_build_dse_session_using_contact_points_with_ip() {
    // given
    String contactPointIp =
        ((InetSocketAddress) ccm.getInitialContactPoints().get(0).resolve()).getHostString();
    Map<String, String> config =
        ImmutableMap.of(
            "contactPoints", contactPointIp,
            "loadBalancing.localDc", ccm.getDC(1),
            "port", String.valueOf(ccm.getBinaryPort()));
    DseSinkConfig dseSinkConfig = new DseSinkConfig(config);

    // when
    ResultSet set;
    try (CqlSession session = LifeCycleManager.buildDseSession(dseSinkConfig)) {
      // then
      set = session.execute("select * from system.local");
      assertThat(set).isNotNull();
      // and endPoint uses IP address
      EndPoint endPoint = getEndPoint(session);
      assertThat(endPoint.toString())
          .isEqualTo(String.format("%s:%d", contactPointIp, ccm.getBinaryPort()));
      assertTrue(((InetSocketAddress) getEndPoint(session).resolve()).isUnresolved());
    }
  }

  @NotNull
  private EndPoint getEndPoint(CqlSession session) {
    return session.getMetadata().getNodes().values().iterator().next().getEndPoint();
  }
}
