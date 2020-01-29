/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.util.UUID;
import org.junit.jupiter.api.Test;

class UUIDUtilTest {
  @Test
  public void should_create_different_UUIDs_for_the_same_name_because_they_use_timestamps() {
    // when
    UUID a1 = UUIDUtil.generateClientId("a");
    UUID a2 = UUIDUtil.generateClientId("a");

    // then
    assertThat(a1).isNotEqualTo(a2);
  }
}
