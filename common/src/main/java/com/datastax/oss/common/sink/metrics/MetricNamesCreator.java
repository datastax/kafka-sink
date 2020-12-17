/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.common.sink.metrics;

import com.datastax.oss.common.sink.config.TableConfig;

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
