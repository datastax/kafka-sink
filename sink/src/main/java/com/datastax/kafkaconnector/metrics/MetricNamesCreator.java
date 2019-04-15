/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.metrics;

import com.datastax.oss.driver.api.core.CqlIdentifier;

public class MetricNamesCreator {

  public static String createBatchSizeMetricName(
      String taskName, CqlIdentifier keyspace, CqlIdentifier table) {
    return String.format("%s/%s/%s/batchSize", taskName, keyspace.asInternal(), table.asInternal());
  }

  public static String createRecordCountMetricName(
      String taskName, CqlIdentifier keyspace, CqlIdentifier table) {
    return String.format(
        "%s/%s/%s/recordCount", taskName, keyspace.asInternal(), table.asInternal());
  }

  public static String createFailedRecordCountMetricName(
      String taskName, CqlIdentifier keyspace, CqlIdentifier table) {
    return String.format(
        "%s/%s/%s/failedRecordCount", taskName, keyspace.asInternal(), table.asInternal());
  }

  public static String createDriverMetricName(String name) {
    return "driver/" + name;
  }
}
