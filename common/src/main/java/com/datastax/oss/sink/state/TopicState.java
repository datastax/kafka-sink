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
package com.datastax.oss.sink.state;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.sink.Mapping;
import com.datastax.oss.sink.RecordMapper;
import com.datastax.oss.sink.config.TableConfig;
import com.datastax.oss.sink.metrics.MetricNamesCreator;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Container for a topic-scoped entities that the sink tasks need (codec-registry, prepared
 * statement, etc.)
 */
class TopicState {
  private final ConvertingCodecFactory codecFactory;
  private final Map<TableConfig, RecordMapper> recordMappers;
  private Map<String, Histogram> batchSizeHistograms;
  private Map<String, Meter> recordCounters;
  private Map<String, Meter> failedRecordCounters;
  private Map<String, Histogram> batchSizeInBytesHistograms;

  TopicState(ConvertingCodecFactory codecFactory) {
    this.codecFactory = codecFactory;
    recordMappers = new ConcurrentHashMap<>();
  }

  void createRecordMapper(
      TableConfig tableConfig,
      List<CqlIdentifier> primaryKey,
      PreparedStatement insertUpdateStatement,
      PreparedStatement deleteStatement) {
    recordMappers.putIfAbsent(
        tableConfig,
        new RecordMapper(
            insertUpdateStatement,
            deleteStatement,
            primaryKey,
            new Mapping(tableConfig.getMapping(), codecFactory),
            true,
            false,
            tableConfig));
  }

  void initializeMetrics(MetricRegistry metricRegistry) {
    // Add batch size histograms for all topic-tables.
    batchSizeHistograms =
        constructMetrics(
            recordMappers,
            MetricNamesCreator::createBatchSizeMetricName,
            metricRegistry::histogram);

    // Add batch size in bytes histograms for all topic-tables.
    batchSizeInBytesHistograms =
        constructMetrics(
            recordMappers,
            MetricNamesCreator::createBatchSizeInBytesMetricName,
            metricRegistry::histogram);

    // Add recordCounters for all topic-tables.
    recordCounters =
        constructMetrics(
            recordMappers, MetricNamesCreator::createRecordCountMetricName, metricRegistry::meter);

    // Add failedRecordCounters for all topic-tables.
    failedRecordCounters =
        constructMetrics(
            recordMappers,
            MetricNamesCreator::createFailedRecordCountMetricName,
            metricRegistry::meter);
  }

  private <T> Map<String, T> constructMetrics(
      Map<TableConfig, RecordMapper> recordMappers,
      Function<TableConfig, String> metricNameCreator,
      Function<String, T> metricCreator) {

    return recordMappers
        .keySet()
        .stream()
        .collect(
            Collectors.toMap(
                TableConfig::getKeyspaceAndTable,
                t -> metricCreator.apply(metricNameCreator.apply(t))));
  }

  @NonNull
  Histogram getBatchSizeHistogram(String keyspaceAndTable) {
    return batchSizeHistograms.get(keyspaceAndTable);
  }

  @NonNull
  Histogram getBatchSizeInBytesHistogram(String keyspaceAndTable) {
    return batchSizeInBytesHistograms.get(keyspaceAndTable);
  }

  void incrementRecordCount(String keyspaceAndTable, int incrementBy) {
    recordCounters.get(keyspaceAndTable).mark(incrementBy);
  }

  void incrementFailedCounter(String keyspaceAndTable) {
    failedRecordCounters.get(keyspaceAndTable).mark();
  }

  @VisibleForTesting
  Meter getRecordCountMeter(String keyspaceAndTable) {
    return recordCounters.get(keyspaceAndTable);
  }

  @VisibleForTesting
  Meter getFailedRecordCounter(String keyspaceAndTable) {
    return failedRecordCounters.get(keyspaceAndTable);
  }

  @NonNull
  RecordMapper getRecordMapper(TableConfig tableConfig) {
    return recordMappers.get(tableConfig);
  }
}
