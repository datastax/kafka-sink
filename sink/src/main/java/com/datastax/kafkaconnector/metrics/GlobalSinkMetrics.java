/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.metrics;

import com.codahale.metrics.MetricRegistry;

public class GlobalSinkMetrics {
  private static final String FAILED_RECORDS_WITH_UNKNOWN_TOPIC = "failedRecordsWithUnknownTopic";
  private MetricRegistry metricRegistry;

  public GlobalSinkMetrics(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  public void incrementFailedWithUnknownTopicCounter() {
    metricRegistry.counter(FAILED_RECORDS_WITH_UNKNOWN_TOPIC).inc();
  }
}
