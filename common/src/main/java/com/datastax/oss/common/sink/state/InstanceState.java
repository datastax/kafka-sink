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
package com.datastax.oss.common.sink.state;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.datastax.oss.common.sink.AbstractSinkTask;
import com.datastax.oss.common.sink.ConfigException;
import com.datastax.oss.common.sink.RecordMapper;
import com.datastax.oss.common.sink.config.CassandraSinkConfig;
import com.datastax.oss.common.sink.config.TableConfig;
import com.datastax.oss.common.sink.config.TopicConfig;
import com.datastax.oss.common.sink.metrics.GlobalSinkMetrics;
import com.datastax.oss.common.sink.metrics.MetricNamesCreator;
import com.datastax.oss.common.sink.metrics.MetricsJmxReporter;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ThreadFactoryBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Container for a session, config, etc. that a connector instance requires to function. */
public class InstanceState {
  private static final Logger log = LoggerFactory.getLogger(InstanceState.class);
  private final CqlSession session;
  private final CassandraSinkConfig config;
  private final Map<String, TopicState> topicStates;

  /** Semaphore to limit the number of concurrent requests. */
  private final Semaphore requestBarrier;

  private final Set<AbstractSinkTask> tasks;
  private final Executor mappingExecutor;
  private final JmxReporter reporter;
  private final GlobalSinkMetrics globalSinkMetrics;

  public InstanceState(
      @NonNull CassandraSinkConfig config,
      @NonNull CqlSession session,
      @NonNull Map<String, TopicState> topicStates,
      @NonNull MetricRegistry metricRegistry) {
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

  void registerTask(AbstractSinkTask task) {
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
  synchronized boolean unregisterTaskAndCheckIfLast(AbstractSinkTask task) {
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

  @NonNull
  public CassandraSinkConfig getConfig() {
    return config;
  }

  @NonNull
  public CqlSession getSession() {
    return session;
  }

  @NonNull
  public Semaphore getRequestBarrier() {
    return requestBarrier;
  }

  public int getMaxNumberOfRecordsInBatch() {
    return config.getMaxNumberOfRecordsInBatch();
  }

  @NonNull
  public TopicConfig getTopicConfig(String topicName) {
    TopicConfig topicConfig = this.config.getTopicConfigs().get(topicName);
    if (topicConfig == null) {
      throw new ConfigException(
          String.format(
              "Connector has no configuration for record topic '%s'. Please update the configuration and restart.",
              topicName));
    }
    return topicConfig;
  }

  @NonNull
  public Histogram getBatchSizeHistogram(String topicName, String keyspaceAndTable) {
    return getTopicState(topicName).getBatchSizeHistogram(keyspaceAndTable);
  }

  @NonNull
  public Histogram getBatchSizeInBytesHistogram(String topicName, String keyspaceAndTable) {
    return getTopicState(topicName).getBatchSizeInBytesHistogram(keyspaceAndTable);
  }

  @NonNull
  public Executor getMappingExecutor() {
    return mappingExecutor;
  }

  @NonNull
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
  public Meter getFailedRecordCounter(String topicName, String keyspaceAndTable) {
    return getTopicState(topicName).getFailedRecordCounter(keyspaceAndTable);
  }

  public void incrementFailedWithUnknownTopicCounter() {
    globalSinkMetrics.incrementFailedWithUnknownTopicCounter();
  }

  @VisibleForTesting
  public Meter getFailedWithUnknownTopicCounter() {
    return globalSinkMetrics.getFailedRecordsWithUnknownTopicCounter();
  }

  @NonNull
  private TopicState getTopicState(String topicName) {
    TopicState topicState = topicStates.get(topicName);
    if (topicState == null) {
      throw new ConfigException(
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
