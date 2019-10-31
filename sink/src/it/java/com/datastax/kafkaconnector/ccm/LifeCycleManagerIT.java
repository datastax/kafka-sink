/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.ccm;

import static com.datastax.kafkaconnector.config.DseSinkConfig.*;
import static com.datastax.kafkaconnector.config.SslConfig.HOSTNAME_VALIDATION_OPT;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONFIG_RELOAD_INTERVAL;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.CONTACT_POINTS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_INTERVAL;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.METRICS_SESSION_ENABLED;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.PROTOCOL_COMPRESSION;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.PROTOCOL_MAX_FRAME_LENGTH;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.RECONNECTION_POLICY_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_CONSISTENCY;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.REQUEST_TIMEOUT;
import static com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader.DEFAULT_ROOT_PATH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.CCMExtension;
import com.datastax.kafkaconnector.config.DseSinkConfig;
import com.datastax.kafkaconnector.state.LifeCycleManager;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
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
  void should_build_dse_session_with_unresolved_contact_points_when_hostname_validation_disabled() {
    // given
    String contactPointDns = "localhost";
    Map<String, String> config =
        ImmutableMap.of(
            "contactPoints",
            contactPointDns,
            "loadBalancing.localDc",
            ccm.getDC(1),
            "port",
            String.valueOf(ccm.getBinaryPort()),
            HOSTNAME_VALIDATION_OPT,
            "false");
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
      assertTrue(((InetSocketAddress) endPoint.resolve()).isUnresolved());
    }
  }

  @Test
  void
      should_build_dse_session_with_unresolved_contact_points_with_ip_when_hostname_validation_disabled() {
    // given
    String contactPointIp =
        ((InetSocketAddress) ccm.getInitialContactPoints().get(0).resolve()).getHostString();
    Map<String, String> config =
        ImmutableMap.of(
            "contactPoints",
            contactPointIp,
            "loadBalancing.localDc",
            ccm.getDC(1),
            "port",
            String.valueOf(ccm.getBinaryPort()),
            HOSTNAME_VALIDATION_OPT,
            "false");
    DseSinkConfig dseSinkConfig = new DseSinkConfig(config);

    // when
    ResultSet set;
    try (CqlSession session = LifeCycleManager.buildDseSession(dseSinkConfig)) {
      // then
      set = session.execute("select * from system.local");
      assertThat(set).isNotNull();
      // and endPoint uses unresolved IP address
      EndPoint endPoint = getEndPoint(session);
      assertThat(endPoint.toString())
          .isEqualTo(String.format("%s:%d", contactPointIp, ccm.getBinaryPort()));
      assertTrue(((InetSocketAddress) endPoint.resolve()).isUnresolved());
    }
  }

  @Test
  void should_build_dse_session_with_resolved_contact_points_when_hostname_validation_enabled() {
    // given
    String contactPointDns = "localhost";
    Map<String, String> config =
        ImmutableMap.of(
            "contactPoints",
            contactPointDns,
            "loadBalancing.localDc",
            ccm.getDC(1),
            "port",
            String.valueOf(ccm.getBinaryPort()),
            HOSTNAME_VALIDATION_OPT,
            "true");
    DseSinkConfig dseSinkConfig = new DseSinkConfig(config);

    // when
    ResultSet set;
    try (CqlSession session = LifeCycleManager.buildDseSession(dseSinkConfig)) {
      // then
      set = session.execute("select * from system.local");
      assertThat(set).isNotNull();
      // and endPoint uses resolved DNS address
      assertFalse(((InetSocketAddress) getEndPoint(session).resolve()).isUnresolved());
    }
  }

  @Test
  void
      should_build_dse_session_with_resolved_contact_points_with_ip_when_hostname_validation_enabled() {
    // given
    String contactPointIp =
        ((InetSocketAddress) ccm.getInitialContactPoints().get(0).resolve()).getHostString();
    Map<String, String> config =
        ImmutableMap.of(
            "contactPoints",
            contactPointIp,
            "loadBalancing.localDc",
            ccm.getDC(1),
            "port",
            String.valueOf(ccm.getBinaryPort()),
            HOSTNAME_VALIDATION_OPT,
            "true");
    DseSinkConfig dseSinkConfig = new DseSinkConfig(config);

    // when
    ResultSet set;
    try (CqlSession session = LifeCycleManager.buildDseSession(dseSinkConfig)) {
      // then
      set = session.execute("select * from system.local");
      assertThat(set).isNotNull();
      // and endPoint uses resolved IP address
      EndPoint endPoint = getEndPoint(session);
      assertFalse(((InetSocketAddress) endPoint.resolve()).isUnresolved());
    }
  }

  @Test
  void should_build_session_with_settings_under_datastax_java_driver_prefix() {
    // given
    String contactPointIp =
        ((InetSocketAddress) ccm.getInitialContactPoints().get(0).resolve()).getHostString();
    Map<String, String> config = new HashMap<>();
    config.put("contactPoints", contactPointIp);
    config.put("loadBalancing.localDc", ccm.getDC(1));
    config.put("port", String.valueOf(ccm.getBinaryPort()));
    config.put("jmx", "true");
    config.put(withDriverPrefix(CONFIG_RELOAD_INTERVAL), "1 minutes");
    config.put(withDriverPrefix(REQUEST_CONSISTENCY), "ALL");
    config.put(withDriverPrefix(REQUEST_DEFAULT_IDEMPOTENCE), "true");
    config.put(withDriverPrefix(RECONNECTION_POLICY_CLASS), "ConstantReconnectionPolicy");
    config.put(withDriverPrefix(PROTOCOL_MAX_FRAME_LENGTH), "128 MB");
    config.put(withDriverPrefix(CONTACT_POINTS), "this should be ignored because provided directly");

    DseSinkConfig dseSinkConfig = new DseSinkConfig(config);

    // when
    ResultSet set;
    try (CqlSession session = LifeCycleManager.buildDseSession(dseSinkConfig)) {
      // then
      set = session.execute("select * from system.local");
      assertThat(set).isNotNull();

      // validate explict settings form datastax-java-driver prefix
      DriverExecutionProfile profile = session.getContext().getConfig().getDefaultProfile();
      assertThat(profile.getDuration(CONFIG_RELOAD_INTERVAL))
          .isEqualTo(Duration.of(1, ChronoUnit.MINUTES));

      assertThat(profile.getString(REQUEST_CONSISTENCY)).isEqualTo("ALL");

      assertThat(profile.getBoolean(REQUEST_DEFAULT_IDEMPOTENCE)).isEqualTo(true);

      assertThat(profile.getString(RECONNECTION_POLICY_CLASS))
          .isEqualTo("ConstantReconnectionPolicy");

      assertThat(profile.getBytes(PROTOCOL_MAX_FRAME_LENGTH)).isEqualTo(128_000_000L);

      // validate defaults
      assertThat(profile.getInt(CONNECTION_POOL_LOCAL_SIZE))
          .isEqualTo(Integer.valueOf(CONNECTION_POOL_LOCAL_SIZE_DEFAULT));

      assertThat(profile.getDuration(REQUEST_TIMEOUT)).isEqualTo(Duration.ofSeconds(30));

      assertThat(profile.getDuration(METRICS_NODE_CQL_MESSAGES_HIGHEST))
          .isEqualTo(Duration.ofSeconds(35));


      assertThat(profile.getString(PROTOCOL_COMPRESSION)).isEqualTo(COMPRESSION_DEFAULT);

      assertFalse(profile.isDefined(CLOUD_SECURE_CONNECT_BUNDLE));

      assertThat(profile.getDuration(METRICS_SESSION_CQL_REQUESTS_INTERVAL))
          .isEqualTo(Duration.ofSeconds(30));
      assertThat(profile.getStringList(METRICS_SESSION_ENABLED))
          .containsOnly("cql-client-timeouts", "cql-requests");
    }
  }

  @NotNull
  private EndPoint getEndPoint(CqlSession session) {
    return session.getMetadata().getNodes().values().iterator().next().getEndPoint();
  }
}
