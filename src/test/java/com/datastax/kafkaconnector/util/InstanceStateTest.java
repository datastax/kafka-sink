/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.util;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.kafkaconnector.config.DseSinkConfig;
import com.datastax.kafkaconnector.config.TableConfig;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;

class InstanceStateTest {
  private DseSinkConfig config = mock(DseSinkConfig.class);
  private DseSession session = mock(DseSession.class);

  private Map<String, TopicState> topicStates = new HashMap<>();
  private Map<String, List<CqlIdentifier>> primaryKeys = new HashMap<>();
  private InstanceState instanceState =
      new InstanceState(config, session, primaryKeys, topicStates);

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
}
