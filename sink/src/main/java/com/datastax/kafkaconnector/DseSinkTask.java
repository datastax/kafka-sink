/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector;

import com.datastax.kafkaconnector.config.TableConfig;
import com.datastax.kafkaconnector.config.TopicConfig;
import com.datastax.kafkaconnector.metadata.InnerDataAndMetadata;
import com.datastax.kafkaconnector.metadata.MetadataCreator;
import com.datastax.kafkaconnector.record.HeadersDataMetadata;
import com.datastax.kafkaconnector.record.KeyValueRecord;
import com.datastax.kafkaconnector.record.KeyValueRecordMetadata;
import com.datastax.kafkaconnector.record.RecordAndStatement;
import com.datastax.kafkaconnector.state.InstanceState;
import com.datastax.kafkaconnector.state.LifeCycleManager;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DseSinkTask does the heavy lifting of processing {@link SinkRecord}s and writing them to DSE. */
public class DseSinkTask extends SinkTask {
  private static final Runnable NO_OP = () -> {};
  private static final Logger log = LoggerFactory.getLogger(DseSinkTask.class);
  private final ExecutorService boundStatementProcessorService =
      Executors.newFixedThreadPool(
          1, new ThreadFactoryBuilder().setNameFormat("bound-statement-processor-%d").build());
  private InstanceState instanceState;
  private Map<TopicPartition, OffsetAndMetadata> failureOffsets;
  private TaskStateManager taskStateManager;

  @Override
  public String version() {
    return new DseSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    log.debug("DseSinkTask starting with props: {}", props);
    taskStateManager = new TaskStateManager();
    failureOffsets = new ConcurrentHashMap<>();
    instanceState = LifeCycleManager.startTask(this, props);
  }

  /**
   * Invoked by the Connect infrastructure prior to committing offsets to Kafka, which is typically
   * 10 seconds. This is the task's opportunity to report failed record offsets and keeping the sink
   * from progressing on a particular topic.
   *
   * @param currentOffsets map of offsets (one offset for each topic)
   * @return the map, mutated to have failure offsets recorded in it
   */
  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // Copy all of the failures (which point to the offset that we should retrieve from next time)
    // into currentOffsets.
    currentOffsets.putAll(failureOffsets);
    return currentOffsets;
  }

  /**
   * Entry point for record processing.
   *
   * @param sinkRecords collection of Kafka {@link SinkRecord}'s to process
   */
  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords.isEmpty()) {
      // Nothing to process.
      return;
    }

    log.debug("Received {} records", sinkRecords.size());

    taskStateManager.waitRunTransitionLogic(
        () -> {
          failureOffsets.clear();

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

  @Override
  public void stop() {
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
  void mapAndQueueRecord(
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
        } catch (IOException ex) {
          // The IOException can only theoretically happen when processing json data. But bad json
          // won't result in this exception. We're not pulling data from a file or any other kind of
          // IO.
          handleFailure(record, ex, null, failedRecordIncrement);
        }
      }
    } catch (KafkaException e) {
      // The Kafka exception could occur if the record references an unknown topic.
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
  synchronized void handleFailure(
      SinkRecord record, Throwable e, String cql, Runnable failCounter) {
    // Store the topic-partition and offset that had an error. However, we want
    // to keep track of the *lowest* offset in a topic-partition that failed. Because
    // requests are sent in parallel and response ordering is non-deterministic,
    // it's possible for a failure in an insert with a higher offset be detected
    // before that of a lower offset. Thus, we only record a failure if
    // 1. There is no entry for this topic-partition, or
    // 2. There is an entry, but its offset is > our offset.
    //
    // This can happen in multiple invocations of this callback concurrently, so
    // we perform these checks/updates in a synchronized block. Presumably failures
    // don't occur that often, so we don't have to be very fancy here.

    if (!instanceState.getConfig().isIgnoreErrors()) {
      TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
      long currentOffset = Long.MAX_VALUE;
      if (failureOffsets.containsKey(topicPartition)) {
        currentOffset = failureOffsets.get(topicPartition).offset();
      }
      if (record.kafkaOffset() < currentOffset) {
        failureOffsets.put(topicPartition, new OffsetAndMetadata(record.kafkaOffset()));
        context.offset(topicPartition, record.kafkaOffset());
      }
    }

    failCounter.run();
    String statementError = cql != null ? String.format("\n   statement: %s", cql) : "";

    log.warn(
        "Error inserting/updating row for Kafka record {}: {}{}",
        record,
        e.getMessage(),
        statementError);
  }
}
