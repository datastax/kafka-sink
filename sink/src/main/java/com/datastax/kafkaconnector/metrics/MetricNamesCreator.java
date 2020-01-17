/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.metrics;

import com.datastax.kafkaconnector.config.TableConfig;

public class MetricNamesCreator {

  public static String createBatchSizeMetricName(TableConfig tableConfig) {
    return topicKeyspacePrefix(tableConfig, "batchSize");
  }

  public static String createBatchSizeInBytesMetricName(TableConfig tableConfig) {
    return topicKeyspacePrefix(tableConfig, "batchSizeInBytes");
  }

  public static String createRecordCountMetricName(TableConfig tableConfig) {
    return topicKeyspacePrefix(tableConfig, "recordCount");
  }

  public static String createFailedRecordCountMetricName(TableConfig tableConfig) {
    return topicKeyspacePrefix(tableConfig, "failedRecordCount");
  }

  private static String topicKeyspacePrefix(TableConfig tableConfig, String metricName) {
    return String.format(
        "%s/%s/%s/%s",
        tableConfig.getTopicName(),
        tableConfig.getKeyspace().asInternal(),
        tableConfig.getTable().asInternal(),
        metricName);
  }

  public static String createDriverMetricName(String name) {
    return "driver/" + name;
  }
}
