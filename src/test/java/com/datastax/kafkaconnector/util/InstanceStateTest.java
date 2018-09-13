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

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.kafkaconnector.config.DseSinkConfig;
import java.util.Map;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;

class InstanceStateTest {
  private DseSinkConfig config = mock(DseSinkConfig.class);
  private DseSession session = mock(DseSession.class);

  @SuppressWarnings("unchecked")
  private Map<String, TopicState> topicStates = mock(Map.class);

  private InstanceState instanceState = new InstanceState(config, session, topicStates);

  @Test
  void getTopicConfig_fail() {
    assertTopicNotFound(() -> instanceState.getTopicConfig("unknown"));
  }

  @Test
  void getCodecRegistry_fail() {
    assertTopicNotFound(() -> instanceState.getCodecRegistry("unknown"));
  }

  @Test
  void getInsertStatement_fail() {
    assertTopicNotFound(() -> instanceState.getCqlStatement("unknown"));
  }

  @Test
  void getPreparedInsertStatement_fail() {
    assertTopicNotFound(() -> instanceState.getPreparedInsertStatement("unknown"));
  }

  private void assertTopicNotFound(ThrowableAssert.ThrowingCallable callable) {
    assertThatThrownBy(callable)
        .isInstanceOf(RuntimeException.class)
        .hasMessage(
            "Connector has no configuration for record topic 'unknown'. "
                + "Please update the configuration and restart.");
  }
}
