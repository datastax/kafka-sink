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
package com.datastax.oss.sink;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ThreadFactoryBuilder;
import com.datastax.oss.sink.config.CassandraSinkConfig;
import com.datastax.oss.sink.config.TableConfig;
import com.datastax.oss.sink.config.TopicConfig;
import com.datastax.oss.sink.metadata.InnerDataAndMetadata;
import com.datastax.oss.sink.metadata.MetadataCreator;
import com.datastax.oss.sink.record.HeadersDataMetadata;
import com.datastax.oss.sink.record.KeyValueRecord;
import com.datastax.oss.sink.record.KeyValueRecordMetadata;
import com.datastax.oss.sink.record.RecordAndStatement;
import com.datastax.oss.sink.state.InstanceState;
import com.datastax.oss.sink.state.LifeCycleManager;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CassandraSinkTask does the heavy lifting of processing {@link SinkRecord}s and writing them to
 * DSE.
 */
public abstract class RecordProcessor extends SinkTask {
  private static final Runnable NO_OP = () -> {};
  private static final Logger log = LoggerFactory.getLogger(RecordProcessor.class);
  private final ExecutorService boundStatementProcessorService =
      Executors.newFixedThreadPool(
          1, new ThreadFactoryBuilder().setNameFormat("bound-statement-processor-%d").build());
  private InstanceState instanceState;
  private TaskStateManager taskStateManager;

  @Override
  public final void start(Map<String, String> props) {
    log.debug("{} starting with props: {}", getClass().getName(), props);
    taskStateManager = new TaskStateManager();
    beforeStart(props);
    instanceState = LifeCycleManager.startTask(this, props);
  }

  protected abstract void beforeStart(Map<String, String> config);

  protected abstract void onProcessingStart();

  /**
   * Entry point for record processing.
   *
   * @param sinkRecords collection of Kafka {@link SinkRecord}'s to process
   */
  @Override
  public final void put(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords.isEmpty()) {
      // Nothing to process.
      return;
    }

    log.debug("Received {} records", sinkRecords.size());

    taskStateManager.waitRunTransitionLogic(
        () -> {
          onProcessingStart();

          Instant start = Instant.now();
          List<CompletableFuture<Void>> mappingFutures;
          Collection<CompletionStage<? extends AsyncResultSet>> queryFutures =
              new ConcurrentLinkedQueue<>();
          BlockingQueue<RecordAndStatement> boundStatementsQueue = new LinkedBlockingQueue<>();
          BoundStatementProcessor boundStatementProcessor =
              new BoundStatementProcessor(
                  this,
                  boundStatementsQueue,
                  queryFutures,
                  instanceState.getMaxNumberOfRecordsInBatch());
          try {
            Future<?> boundStatementProcessorTask =
                boundStatementProcessorService.submit(boundStatementProcessor);
            mappingFutures =
                sinkRecords
                    .stream()
                    .map(
                        record ->
                            CompletableFuture.runAsync(
                                () -> mapAndQueueRecord(boundStatementsQueue, record),
                                instanceState.getMappingExecutor()))
                    .collect(Collectors.toList());

            try {
              CompletableFuture.allOf(mappingFutures.toArray(new CompletableFuture[0])).join();
            } finally {
              boundStatementProcessor.stop();
            }
            try {
              boundStatementProcessorTask.get();
            } catch (ExecutionException e) {
              log.error(
                  "Problem when getting boundStatementProcessorTask. This is likely a bug in the connector, please report.",
                  e);
            }
            log.debug("Query futures: {}", queryFutures.size());
            for (CompletionStage<? extends AsyncResultSet> f : queryFutures) {
              try {
                f.toCompletableFuture().get();
              } catch (ExecutionException e) {
                log.error(
                    "Problem when getting queryFuture. This is likely a bug in the connector, please report.",
                    e);
              }
            }

            Instant end = Instant.now();
            long ms = Duration.between(start, end).toMillis();
            log.debug(
                "Completed {}/{} inserts in {} ms",
                boundStatementProcessor.getSuccessfulRecordCount(),
                sinkRecords.size(),
                ms);
          } catch (InterruptedException e) {
            boundStatementProcessor.stop();
            queryFutures.forEach(
                f -> {
                  f.toCompletableFuture().cancel(true);
                  try {
                    f.toCompletableFuture().get();
                  } catch (InterruptedException | ExecutionException | CancellationException ex) {
                    log.warn("Problem when interrupting completableFuture", ex);
                  }
                });

            throw new RetriableException("Interrupted while issuing queries");
          }
        });
  }

  protected final CassandraSinkConfig config() {
    return instanceState.getConfig();
  }

  @Override
  public final void stop() {
    taskStateManager.toStopTransitionLogic(
        NO_OP, () -> LifeCycleManager.stopTask(this.instanceState, this));
  }

  @VisibleForTesting
  public InstanceState getInstanceState() {
    return instanceState;
  }

  /**
   * Map the given Kafka record based on its topic and the table mappings. Add result {@link
   * BoundStatement}'s to the given queue for further processing.
   *
   * @param boundStatementsQueue the queue that processes {@link RecordAndStatement}'s
   * @param record the {@link SinkRecord} to map
   */
  @VisibleForTesting
  final void mapAndQueueRecord(
      BlockingQueue<RecordAndStatement> boundStatementsQueue, SinkRecord record) {
    try {
      String topicName = record.topic();
      TopicConfig topicConfig = instanceState.getTopicConfig(topicName);

      for (TableConfig tableConfig : topicConfig.getTableConfigs()) {
        Runnable failedRecordIncrement =
            () ->
                instanceState.incrementFailedCounter(topicName, tableConfig.getKeyspaceAndTable());
        try {
          InnerDataAndMetadata key = MetadataCreator.makeMeta(record.key());
          InnerDataAndMetadata value = MetadataCreator.makeMeta(record.value());
          Headers headers = record.headers();

          KeyValueRecord keyValueRecord =
              new KeyValueRecord(
                  key.getInnerData(), value.getInnerData(), record.timestamp(), headers);
          RecordMapper mapper = instanceState.getRecordMapper(tableConfig);
          boundStatementsQueue.offer(
              new RecordAndStatement(
                  record,
                  tableConfig.getKeyspaceAndTable(),
                  mapper
                      .map(
                          new KeyValueRecordMetadata(
                              key.getInnerMetadata(),
                              value.getInnerMetadata(),
                              new HeadersDataMetadata(headers)),
                          keyValueRecord)
                      .setConsistencyLevel(tableConfig.getConsistencyLevel())));
        } catch (Exception ex) {
          // An IOException can theoretically happen when processing json data. But bad json
          // won't result in this exception. We're not pulling data from a file or any other kind of
          // IO.
          // KAF-200: expand failure handling to all runtime and checked exceptions when parsing
          // and mapping records.
          handleFailure(record, ex, null, failedRecordIncrement);
        }
      }
    } catch (Exception e) {
      // A KafkaException could occur if the record references an unknown topic.
      // Most likely this error can't occur in this application...but we try to protect ourselves
      // anyway just in case.
      handleFailure(record, e, null, instanceState::incrementFailedWithUnknownTopicCounter);
    }
  }

  /**
   * Handle a failed record.
   *
   * @param record the {@link SinkRecord} that failed to process
   * @param e the exception
   * @param cql the cql statement that failed to execute
   * @param failCounter the metric that keeps track of number of failures encountered
   */
  protected abstract void handleFailure(
      SinkRecord record, Throwable e, String cql, Runnable failCounter);

  protected abstract void handleSuccess(SinkRecord record);
}
