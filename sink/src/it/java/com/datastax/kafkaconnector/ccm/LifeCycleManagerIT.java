package com.datastax.kafkaconnector.ccm;

import static org.assertj.core.api.Assertions.assertThat;

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
      EndPoint endPoint = session.getMetadata().getNodes().values().iterator().next().getEndPoint();
      assertThat(endPoint.toString())
          .isEqualTo(String.format("%s:%d", contactPointDns, ccm.getBinaryPort()));
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
      // and endPoint uses unresolved DNS address
      EndPoint endPoint = session.getMetadata().getNodes().values().iterator().next().getEndPoint();
      assertThat(endPoint.toString())
          .isEqualTo(String.format("%s:%d", contactPointIp, ccm.getBinaryPort()));
    }
  }
}
