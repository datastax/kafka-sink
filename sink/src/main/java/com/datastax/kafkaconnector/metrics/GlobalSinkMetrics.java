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
  private static final String FAILED_RECORD_COUNT = "globalFailedRecordCount";
  private MetricRegistry metricRegistry;

  public GlobalSinkMetrics(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  public void incrementFailedCounter() {
    metricRegistry.counter(FAILED_RECORD_COUNT).inc();
  }
}
