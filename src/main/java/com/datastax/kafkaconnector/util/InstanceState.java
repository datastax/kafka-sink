/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.util;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.kafkaconnector.DseSinkTask;
import com.datastax.kafkaconnector.codecs.KafkaCodecRegistry;
import com.datastax.kafkaconnector.config.DseSinkConfig;
import com.datastax.kafkaconnector.config.TopicConfig;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.kafka.common.KafkaException;
import org.jetbrains.annotations.NotNull;

/** Container for a session, config, etc. */
public class InstanceState {
  private final DseSession session;
  private final DseSinkConfig config;
  private final Map<String, TopicState> topicStates;
  private final Semaphore requestBarrier;
  private final Set<DseSinkTask> tasks;
  private final Executor mappingExecutor;
  private final JmxReporter reporter;
  private final Meter recordCountMeter;
  private final Counter failedRecordCounter;

  InstanceState(
      @NotNull DseSinkConfig config,
      @NotNull DseSession session,
      @NotNull Map<String, TopicState> topicStates) {
    this.session = session;
    this.config = config;
    this.topicStates = topicStates;
    this.requestBarrier = new Semaphore(getConfig().getMaxConcurrentRequests());
    tasks = Sets.newConcurrentHashSet();
    mappingExecutor =
        Executors.newFixedThreadPool(
            8, new ThreadFactoryBuilder().setNameFormat("mapping-%d").build());
    MetricRegistry metricRegistry = new MetricRegistry();
    // Add driver metrics to our registry.
    session
        .getMetrics()
        .ifPresent(
            m ->
                m.getRegistry()
                    .getMetrics()
                    .forEach((name, metric) -> metricRegistry.register("driver/" + name, metric)));

    topicStates.values().forEach(ts -> ts.initializeMetrics(metricRegistry));
    recordCountMeter = metricRegistry.meter("recordCount");
    failedRecordCounter = metricRegistry.counter("failedRecordCount");
    reporter =
        JmxReporter.forRegistry(metricRegistry)
            .inDomain("datastax.kafkaconnector." + config.getInstanceName())
            .createsObjectNamesWith(
                (type, domain, name) -> {
                  try {
                    StringBuilder sb =
                        new StringBuilder("com.datastax.kafkaconnector:0=")
                            .append(config.getInstanceName())
                            .append(',');
                    StringTokenizer tokenizer = new StringTokenizer(name, "/");
                    int i = 1;
                    while (tokenizer.hasMoreTokens()) {
                      String token = tokenizer.nextToken();
                      if (tokenizer.hasMoreTokens()) {
                        sb.append(i++).append('=').append(token).append(',');
                      } else {
                        sb.append("name=").append(token);
                      }
                    }
                    return new ObjectName(sb.toString());
                  } catch (MalformedObjectNameException e) {
                    throw new RuntimeException(e);
                  }
                })
            .build();
    if (config.getJmx()) {
      reporter.start();
    }
  }

  void registerTask(DseSinkTask task) {
    tasks.add(task);
  }

  void unregisterTask(DseSinkTask task) {
    tasks.remove(task);
  }

  Set<DseSinkTask> getTasks() {
    return tasks;
  }

  void stopJmxReporter() {
    reporter.stop();
  }

  @NotNull
  DseSinkConfig getConfig() {
    return config;
  }

  @NotNull
  public DseSession getSession() {
    return session;
  }

  @NotNull
  public Semaphore getRequestBarrier() {
    return requestBarrier;
  }

  @NotNull
  public TopicConfig getTopicConfig(String topicName) {
    TopicConfig topicConfig = this.config.getTopicConfigs().get(topicName);
    if (topicConfig == null) {
      throw new KafkaException(
          String.format(
              "Connector has no configuration for record topic '%s'. Please update the configuration and restart.",
              topicName));
    }
    return topicConfig;
  }

  @NotNull
  public KafkaCodecRegistry getCodecRegistry(String topicName) {
    return getTopicState(topicName).getCodecRegistry();
  }

  @NotNull
  public String getCqlStatement(String topicName) {
    return getTopicState(topicName).getCqlStatement();
  }

  @NotNull
  public Histogram getBatchSizeHistogram(String topicName) {
    return getTopicState(topicName).getBatchSizeHistogram();
  }

  @NotNull
  public Executor getMappingExecutor() {
    return mappingExecutor;
  }

  @NotNull
  public PreparedStatement getPreparedInsertStatement(String topicName) {
    return getTopicState(topicName).getPreparedStatement();
  }

  @NotNull
  public Meter getRecordCountMeter() {
    return recordCountMeter;
  }

  @NotNull
  public Counter getFailedRecordCounter() {
    return failedRecordCounter;
  }

  private TopicState getTopicState(String topicName) {
    TopicState topicState = topicStates.get(topicName);
    if (topicState == null) {
      throw new KafkaException(
          String.format(
              "Connector has no configuration for record topic '%s'. Please update the configuration and restart.",
              topicName));
    }
    return topicState;
  }
}
