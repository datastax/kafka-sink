/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.state;

import static com.datastax.kafkaconnector.config.TableConfig.MAPPING_OPT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.datastax.kafkaconnector.config.DseSinkConfig;
import com.datastax.kafkaconnector.config.TableConfig;
import com.datastax.kafkaconnector.config.TableConfigBuilder;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;

class InstanceStateTest {
  private DseSinkConfig config = mock(DseSinkConfig.class);
  private CqlSession session = mock(CqlSession.class);

  private Map<String, TopicState> topicStates = new HashMap<>();
  private InstanceState instanceState =
      new InstanceState(config, session, topicStates, new MetricRegistry());

  @Test
  void getTopicConfig_fail() {
    assertTopicNotFound(() -> instanceState.getTopicConfig("unknown"));
  }

  @Test
  void getBatchSizeHistogram_fail() {
    assertTopicNotFound(() -> instanceState.getBatchSizeHistogram("unknown", "unknown"));
  }

  @Test
  void getRecordMapper_fail() {
    TableConfig config = mock(TableConfig.class);
    when(config.getTopicName()).thenReturn("unknown");
    assertTopicNotFound(() -> instanceState.getRecordMapper(config));
  }

  private void assertTopicNotFound(ThrowableAssert.ThrowingCallable callable) {
    assertThatThrownBy(callable)
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Connector has no configuration for record topic 'unknown'. "
                + "Please update the configuration and restart.");
  }

  @Test
  void should_not_reset_counter_when_create_new_instance_state() {
    // given
    MetricRegistry metricRegistry = new MetricRegistry();
    TopicState topicState = new TopicState(null);
    topicState.initializeMetrics(metricRegistry);
    topicState.createRecordMapper(
        new TableConfigBuilder("t1", "ks", "tb", false)
            .addSimpleSetting(MAPPING_OPT, "v=key.v")
            .build(),
        ImmutableList.of(),
        null,
        null);

    Map<String, TopicState> topicStates = ImmutableMap.of("t1", topicState);
    InstanceState instanceState =
        new InstanceState(
            new DseSinkConfig(ImmutableMap.of("name", "instance-a")),
            mock(CqlSession.class),
            topicStates,
            metricRegistry);

    // when
    instanceState.incrementRecordCounter("t1", "ks.tb", 1);

    // then
    assertThat(instanceState.getRecordCounter("t1", "ks.tb").getCount()).isEqualTo(1);

    // when create new instanceState
    InstanceState instanceState2 =
        new InstanceState(
            new DseSinkConfig(ImmutableMap.of("name", "instance-b")),
            mock(CqlSession.class),
            topicStates,
            metricRegistry);

    // then metrics should not reset
    assertThat(instanceState2.getRecordCounter("t1", "ks.tb").getCount()).isEqualTo(1);
  }
}
