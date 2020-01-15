/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.state;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.kafkaconnector.DseSinkTask;
import com.datastax.kafkaconnector.RecordMapper;
import com.datastax.kafkaconnector.config.DseSinkConfig;
import com.datastax.kafkaconnector.config.TableConfig;
import com.datastax.kafkaconnector.config.TopicConfig;
import com.datastax.kafkaconnector.metrics.GlobalSinkMetrics;
import com.datastax.kafkaconnector.metrics.MetricNamesCreator;
import com.datastax.kafkaconnector.metrics.MetricsJmxReporter;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import org.apache.kafka.common.KafkaException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Container for a session, config, etc. that a connector instance requires to function. */
public class InstanceState {
  private static final Logger log = LoggerFactory.getLogger(InstanceState.class);
  private final DseSession session;
  private final DseSinkConfig config;
  private final Map<String, TopicState> topicStates;

  /** Semaphore to limit the number of concurrent DSE requests. */
  private final Semaphore requestBarrier;

  private final Set<DseSinkTask> tasks;
  private final Executor mappingExecutor;
  private final JmxReporter reporter;
  private final GlobalSinkMetrics globalSinkMetrics;

  public InstanceState(
      @NotNull DseSinkConfig config,
      @NotNull DseSession session,
      @NotNull Map<String, TopicState> topicStates,
      @NotNull MetricRegistry metricRegistry) {
    this.session = session;
    this.config = config;
    this.topicStates = topicStates;
    this.requestBarrier = new Semaphore(getConfig().getMaxConcurrentRequests());
    tasks = Sets.newConcurrentHashSet();
    mappingExecutor =
        Executors.newFixedThreadPool(
            8, new ThreadFactoryBuilder().setNameFormat("mapping-%d").build());
    // Add driver metrics to our registry.
    session
        .getMetrics()
        .ifPresent(
            m ->
                m.getRegistry()
                    .getMetrics()
                    .forEach(
                        (name, metric) ->
                            metricRegistry.register(
                                MetricNamesCreator.createDriverMetricName(name), metric)));

    topicStates.values().forEach(ts -> ts.initializeMetrics(metricRegistry));
    globalSinkMetrics = new GlobalSinkMetrics(metricRegistry);
    reporter = MetricsJmxReporter.createJmxReporter(config.getInstanceName(), metricRegistry);

    if (config.getJmx()) {
      reporter.start();
    }
  }

  void registerTask(DseSinkTask task) {
    tasks.add(task);
  }

  /**
   * Unregister the given task. This method is synchronized because it also handles cleaning up the
   * InstanceState if this is the last task. Without synchronization, two threads that unregister
   * the last tasks simultaneously may both believe they are the last, causing multiple cleanups to
   * occur.
   *
   * @param task the task
   * @return true if this is the last task to be unregistered in the InstanceState, false otherwise.
   */
  synchronized boolean unregisterTaskAndCheckIfLast(DseSinkTask task) {
    tasks.remove(task);
    if (tasks.isEmpty()) {
      log.debug("last task unregister close");
      closeQuietly(session);
      reporter.stop();
      // Indicate to the caller that this is the last task in the InstanceState.
      return true;
    }

    // Indicate to the caller that this is not the last task in the InstanceState.
    return false;
  }

  @NotNull
  public DseSinkConfig getConfig() {
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
  public int getMaxNumberOfRecordsInBatch() {
    return config.getMaxNumberOfRecordsInBatch();
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
  public Histogram getBatchSizeHistogram(String topicName, String keyspaceAndTable) {
    return getTopicState(topicName).getBatchSizeHistogram(keyspaceAndTable);
  }

  @NotNull
  public Histogram getBatchSizeInBytesHistogram(String topicName, String keyspaceAndTable) {
    return getTopicState(topicName).getBatchSizeInBytesHistogram(keyspaceAndTable);
  }

  @NotNull
  public Executor getMappingExecutor() {
    return mappingExecutor;
  }

  @NotNull
  public RecordMapper getRecordMapper(TableConfig tableConfig) {
    return getTopicState(tableConfig.getTopicName()).getRecordMapper(tableConfig);
  }

  public void incrementRecordCounter(String topicName, String keyspaceAndTable, int incrementBy) {
    getTopicState(topicName).incrementRecordCount(keyspaceAndTable, incrementBy);
  }

  public void incrementFailedCounter(String topicName, String keyspaceAndTable) {
    getTopicState(topicName).incrementFailedCounter(keyspaceAndTable);
  }

  @VisibleForTesting
  public Meter getRecordCounter(String topicName, String keyspaceAndTable) {
    return getTopicState(topicName).getRecordCountMeter(keyspaceAndTable);
  }

  @VisibleForTesting
  public Counter getFailedRecordCounter(String topicName, String keyspaceAndTable) {
    return getTopicState(topicName).getFailedRecordCounter(keyspaceAndTable);
  }

  public void incrementFailedWithUnknownTopicCounter() {
    globalSinkMetrics.incrementFailedWithUnknownTopicCounter();
  }

  @NotNull
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

  /**
   * Close the given closeable without reporting errors if any occur.
   *
   * @param closeable the object close
   */
  private static void closeQuietly(AutoCloseable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        log.debug(String.format("Failed to close %s", closeable), e);
      }
    }
  }

  public ProtocolVersion getProtocolVersion() {
    return session.getContext().getProtocolVersion();
  }

  public CodecRegistry getCodecRegistry() {
    return session.getContext().getCodecRegistry();
  }
}
