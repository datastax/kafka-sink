/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.state;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.datastax.kafkaconnector.Mapping;
import com.datastax.kafkaconnector.RecordMapper;
import com.datastax.kafkaconnector.codecs.KafkaCodecRegistry;
import com.datastax.kafkaconnector.config.TableConfig;
import com.datastax.kafkaconnector.metrics.MetricNamesCreator;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

/**
 * Container for a topic-scoped entities that the sink tasks need (codec-registry, prepared
 * statement, etc.)
 */
public class TopicState {
  private final String name;
  private final KafkaCodecRegistry codecRegistry;
  private final Map<TableConfig, RecordMapper> recordMappers;
  private Map<String, Histogram> batchSizeHistograms;

  public TopicState(String name, KafkaCodecRegistry codecRegistry) {
    this.name = name;
    this.codecRegistry = codecRegistry;
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
            new Mapping(tableConfig.getMapping(), codecRegistry),
            tableConfig.isNullToUnset(),
            true,
            false));
  }

  void initializeMetrics(MetricRegistry metricRegistry) {
    // Add histograms for all topic-tables.
    batchSizeHistograms =
        recordMappers
            .keySet()
            .stream()
            .collect(
                Collectors.toMap(
                    TableConfig::getKeyspaceAndTable,
                    t ->
                        metricRegistry.histogram(
                            MetricNamesCreator.createBatchSizeMetricName(
                                name, t.getKeyspace(), t.getTable()))));
  }

  @NotNull
  Histogram getBatchSizeHistogram(String keyspaceAndTable) {
    return batchSizeHistograms.get(keyspaceAndTable);
  }

  @NotNull
  RecordMapper getRecordMapper(TableConfig tableConfig) {
    return recordMappers.get(tableConfig);
  }
}
