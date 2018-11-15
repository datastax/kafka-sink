package com.datastax.kafkaconnector.metrics;

import com.datastax.oss.driver.api.core.CqlIdentifier;

public class MetricNamesCreator {

  public static String createBatchSizeMetricName(
      String taskName, CqlIdentifier keyspace, CqlIdentifier table) {
    return String.format("%s/%s/%s/batchSize", taskName, sanitize(keyspace), sanitize(table));
  }

  private static String sanitize(CqlIdentifier identifier) {
    String sanitized = identifier.asInternal();
    // remove any slashes from CQL identifier names as they will interfere with hierarchical names
    return sanitized.replace("//", "");
  }

  public static String createDriverMetricName(String name) {
    return "driver/" + name;
  }
}
