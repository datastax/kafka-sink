/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.util;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.datastax.kafkaconnector.codecs.KafkaCodecRegistry;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;

/**
 * Container for a topic-scoped entities that the sink tasks need (codec-registry, prepared
 * statement, etc.)
 */
class TopicState {
  private final String name;
  private final String cqlStatement;
  private final PreparedStatement preparedStatement;
  private final KafkaCodecRegistry codecRegistry;
  private Histogram batchSizeHistogram;

  TopicState(
      String name,
      String cqlStatement,
      PreparedStatement preparedStatement,
      KafkaCodecRegistry codecRegistry) {
    this.name = name;
    this.cqlStatement = cqlStatement;
    this.preparedStatement = preparedStatement;
    this.codecRegistry = codecRegistry;
  }

  void initializeMetrics(MetricRegistry metricRegistry) {
    batchSizeHistogram = metricRegistry.histogram(String.format("%s/batchSize", name));
  }

  Histogram getBatchSizeHistogram() {
    return batchSizeHistogram;
  }

  String getCqlStatement() {
    return cqlStatement;
  }

  PreparedStatement getPreparedStatement() {
    return preparedStatement;
  }

  KafkaCodecRegistry getCodecRegistry() {
    return codecRegistry;
  }
}
