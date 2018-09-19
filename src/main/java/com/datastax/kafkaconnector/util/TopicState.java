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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Container for a topic-scoped entities that the sink tasks need (codec-registry, prepared
 * statement, etc.)
 */
class TopicState {
  private final String name;
  private final PreparedStatement preparedInsertUpdate;
  private final PreparedStatement preparedDelete;
  private final KafkaCodecRegistry codecRegistry;
  private Histogram batchSizeHistogram;

  TopicState(
      String name,
      PreparedStatement preparedInsertUpdate,
      PreparedStatement preparedDelete,
      KafkaCodecRegistry codecRegistry) {
    this.name = name;
    this.preparedInsertUpdate = preparedInsertUpdate;
    this.preparedDelete = preparedDelete;
    this.codecRegistry = codecRegistry;
  }

  void initializeMetrics(MetricRegistry metricRegistry) {
    batchSizeHistogram = metricRegistry.histogram(String.format("%s/batchSize", name));
  }

  @NotNull
  Histogram getBatchSizeHistogram() {
    return batchSizeHistogram;
  }

  @NotNull
  PreparedStatement getPreparedInsertUpdate() {
    return preparedInsertUpdate;
  }

  @Nullable
  PreparedStatement getPreparedDelete() {
    return preparedDelete;
  }

  @NotNull
  KafkaCodecRegistry getCodecRegistry() {
    return codecRegistry;
  }
}
