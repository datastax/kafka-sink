/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.common.sink.state;

import static com.datastax.oss.common.sink.config.TableConfig.MAPPING_OPT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.common.sink.config.CassandraSinkConfig;
import com.datastax.oss.common.sink.config.TableConfig;
import com.datastax.oss.common.sink.config.TableConfigBuilder;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;

class InstanceStateTest {
  private CassandraSinkConfig config = mock(CassandraSinkConfig.class);
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
            new CassandraSinkConfig(ImmutableMap.of("name", "instance-a")),
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
            new CassandraSinkConfig(ImmutableMap.of("name", "instance-b")),
            mock(CqlSession.class),
            topicStates,
            metricRegistry);

    // then metrics should not reset
    assertThat(instanceState2.getRecordCounter("t1", "ks.tb").getCount()).isEqualTo(1);
  }
}
