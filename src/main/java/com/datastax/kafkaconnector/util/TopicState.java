/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.util;

import com.datastax.kafkaconnector.codecs.KafkaCodecRegistry;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

/**
 * Container for a topic-scoped entities that the sink tasks need (codec-registry, prepared
 * statement, etc.)
 */
class TopicState {
  private final String insertStatement;
  private final PreparedStatement preparedStatement;
  private final KafkaCodecRegistry codecRegistry;

  TopicState(
      String insertStatement,
      PreparedStatement preparedStatement,
      KafkaCodecRegistry codecRegistry) {
    this.insertStatement = insertStatement;
    this.preparedStatement = preparedStatement;
    this.codecRegistry = codecRegistry;
  }

  String getInsertStatement() {
    return insertStatement;
  }

  PreparedStatement getPreparedStatement() {
    return preparedStatement;
  }

  KafkaCodecRegistry getCodecRegistry() {
    return codecRegistry;
  }
}
