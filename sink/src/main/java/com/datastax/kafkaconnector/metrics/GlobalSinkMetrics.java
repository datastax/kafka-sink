/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

public class GlobalSinkMetrics {
  private static final String FAILED_RECORDS_WITH_UNKNOWN_TOPIC = "failedRecordsWithUnknownTopic";
  private final Counter failedRecordsWithUnknownTopicCounter;

  public GlobalSinkMetrics(MetricRegistry metricRegistry) {
    failedRecordsWithUnknownTopicCounter =
        metricRegistry.counter(FAILED_RECORDS_WITH_UNKNOWN_TOPIC);
  }

  public void incrementFailedWithUnknownTopicCounter() {
    failedRecordsWithUnknownTopicCounter.inc();
  }
}
