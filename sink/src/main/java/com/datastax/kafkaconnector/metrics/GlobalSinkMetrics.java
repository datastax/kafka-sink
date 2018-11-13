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
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class GlobalSinkMetrics {
  private static final String RECORD_COUNT = "recordCount";
  private static final String FAILED_RECORD_COUNT = "failedRecordCount";
  private MetricRegistry metricRegistry;

  public GlobalSinkMetrics(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  public void incrementRecordCounter(int incrementBy) {
    metricRegistry.meter(RECORD_COUNT).mark(incrementBy);
  }

  public void incrementFailedCounter() {
    metricRegistry.counter(FAILED_RECORD_COUNT).inc();
  }

  public Meter getRecordCountMeter() {
    return metricRegistry.meter(RECORD_COUNT);
  }

  public Counter getFailedRecordCounter() {
    return metricRegistry.counter(FAILED_RECORD_COUNT);
  }
}
