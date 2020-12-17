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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class GlobalSinkMetrics {
  private static final String FAILED_RECORDS_WITH_UNKNOWN_TOPIC = "failedRecordsWithUnknownTopic";
  private final Meter failedRecordsWithUnknownTopicCounter;

  public GlobalSinkMetrics(MetricRegistry metricRegistry) {
    failedRecordsWithUnknownTopicCounter = metricRegistry.meter(FAILED_RECORDS_WITH_UNKNOWN_TOPIC);
  }

  // VisibleForTesting
  public Meter getFailedRecordsWithUnknownTopicCounter() {
    return failedRecordsWithUnknownTopicCounter;
  }

  public void incrementFailedWithUnknownTopicCounter() {
    failedRecordsWithUnknownTopicCounter.mark();
  }
}
