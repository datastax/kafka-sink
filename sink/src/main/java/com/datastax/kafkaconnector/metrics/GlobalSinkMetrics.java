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
  private final Meter recordCountMeter;
  private final Counter failedRecordCounter;

  public GlobalSinkMetrics(MetricRegistry metricRegistry) {
    recordCountMeter = metricRegistry.meter("recordCount");
    failedRecordCounter = metricRegistry.counter("failedRecordCount");
  }

  public void incrementRecordCounter(int incrementBy) {
    recordCountMeter.mark(incrementBy);
  }

  public void incrementFailedCounter() {
    failedRecordCounter.inc();
  }

  public Meter getRecordCountMeter() {
    return recordCountMeter;
  }

  public Counter getFailedRecordCounter() {
    return failedRecordCounter;
  }
}
