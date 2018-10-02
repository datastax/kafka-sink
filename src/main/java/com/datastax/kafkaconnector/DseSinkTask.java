/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static com.datastax.kafkaconnector.DseSinkTask.TaskState.RUN;
import static com.datastax.kafkaconnector.DseSinkTask.TaskState.STOP;
import static com.datastax.kafkaconnector.DseSinkTask.TaskState.WAIT;
import static com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS;

import com.codahale.metrics.Counter;
import com.datastax.kafkaconnector.config.TableConfig;
import com.datastax.kafkaconnector.config.TopicConfig;
import com.datastax.kafkaconnector.record.JsonData;
import com.datastax.kafkaconnector.record.KeyOrValue;
import com.datastax.kafkaconnector.record.KeyValueRecord;
import com.datastax.kafkaconnector.record.KeyValueRecordMetadata;
import com.datastax.kafkaconnector.record.RawData;
import com.datastax.kafkaconnector.record.RecordAndStatement;
import com.datastax.kafkaconnector.record.RecordMetadata;
import com.datastax.kafkaconnector.record.StructData;
import com.datastax.kafkaconnector.record.StructDataMetadata;
import com.datastax.kafkaconnector.state.InstanceState;
import com.datastax.kafkaconnector.state.LifeCycleManager;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DseSinkTask does the heavy lifting of processing {@link SinkRecord}s and writing them to DSE. */
public class DseSinkTask extends SinkTask {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final JavaType JSON_NODE_MAP_TYPE =
      OBJECT_MAPPER.constructType(new TypeReference<Map<String, JsonNode>>() {}.getType());
  private static final RecordMetadata JSON_RECORD_METADATA =
      (field, cqlType) ->
          field.equals(RawData.FIELD_NAME) ? GenericType.STRING : GenericType.of(JsonNode.class);
  private static final Logger log = LoggerFactory.getLogger(DseSinkTask.class);
  private static final RawData NULL_DATA = new RawData(null);
  private final ExecutorService boundStatementProcessorService =
      Executors.newFixedThreadPool(
          1, new ThreadFactoryBuilder().setNameFormat("bound-statement-processor-%d").build());
  private InstanceState instanceState;
  private Map<TopicPartition, OffsetAndMetadata> failureOffsets;
  private AtomicReference<TaskState> state;
  private CountDownLatch stopLatch;

  static {
    // Configure the json object mapper
    OBJECT_MAPPER.configure(USE_BIG_DECIMAL_FOR_FLOATS, true);
  }

  @Override
  public String version() {
    return new DseSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    log.debug("Task DseSinkTask starting with props: {}", props);
    state = new AtomicReference<>();
    state.set(TaskState.WAIT);
    failureOffsets = new ConcurrentHashMap<>();
    stopLatch = new CountDownLatch(1);
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

    state.compareAndSet(TaskState.WAIT, TaskState.RUN);

    failureOffsets.clear();

    Instant start = Instant.now();
    List<CompletableFuture<Void>> mappingFutures;
    Collection<CompletionStage<AsyncResultSet>> queryFutures = new ConcurrentLinkedQueue<>();
    BlockingQueue<RecordAndStatement> boundStatementsQueue = new LinkedBlockingQueue<>();
    BoundStatementProcessor boundStatementProcessor =
        new BoundStatementProcessor(this, boundStatementsQueue, queryFutures);
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

      CompletableFuture.allOf(mappingFutures.toArray(new CompletableFuture[0])).join();
      boundStatementProcessor.stop();
      try {
        boundStatementProcessorTask.get();
      } catch (ExecutionException e) {
        // No-op.
      }
      log.debug("Query futures: {}", queryFutures.size());
      for (CompletionStage<AsyncResultSet> f : queryFutures) {
        try {
          f.toCompletableFuture().get();
        } catch (ExecutionException e) {
          // If any requests failed, they were handled by the "whenComplete" of the individual
          // future, so nothing to do here.
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
            } catch (InterruptedException | ExecutionException e1) {
              // swallow
            }
          });

      throw new RetriableException("Interrupted while issuing queries");
    } finally {
      state.compareAndSet(TaskState.RUN, TaskState.WAIT);
      if (state.get() == STOP) {
        // Task is stopping. Notify the caller of stop() that we're done working.
        stopLatch.countDown();
      }
    }
  }

  @Override
  public void stop() {
    // Stopping has a few scenarios:
    // 1. We're not currently processing records (e.g. we are in the WAIT state).
    //    Just transition to the STOP state and return. Signal stopLatch
    //    since we are effectively the entity declaring that this task is stopped.
    // 2. We're currently processing records (e.g. we are in the RUN state).
    //    Transition to the STOP state and wait for the thread processing records
    //    (e.g. running put()) to signal stopLatch.
    // 3. We're currently in the STOP state. This could mean that no work is occurring
    //    (because a previous call to stop occurred when we were in the WAIT state or
    //    a previous call to put completed and signaled the latch) or that a thread
    //    is running put and hasn't completed yet. Either way, this thread waits on the
    //    latch. If the latch has been opened already, there's nothing to wait for
    //    and we immediately return.
    try {
      if (state != null) {
        if (state.compareAndSet(WAIT, STOP)) {
          // Clean stop; nothing running/in-progress.
          stopLatch.countDown();
          return;
        }
        state.compareAndSet(RUN, STOP);
        stopLatch.await();
      } else if (stopLatch != null) {
        // There is no state, so we didn't get far in starting up the task. If by some chance
        // there is a stopLatch initialized, decrement it to indicate to any callers that
        // we're done and they need not wait on us.
        stopLatch.countDown();
      }
    } catch (InterruptedException e) {
      // "put" is likely also interrupted, so we're effectively stopped.
      Thread.currentThread().interrupt();
    } finally {
      log.info("Task is stopped.");
      LifeCycleManager.stopTask(instanceState, this);
    }
  }

  InstanceState getInstanceState() {
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
        InnerDataAndMetadata key = makeMeta(record.key());
        InnerDataAndMetadata value = makeMeta(record.value());
        KeyValueRecord keyValueRecord =
            new KeyValueRecord(key.innerData, value.innerData, record.timestamp());
        RecordMapper mapper = instanceState.getRecordMapper(tableConfig);
        boundStatementsQueue.offer(
            new RecordAndStatement(
                record,
                tableConfig.getKeyspaceAndTable(),
                mapper
                    .map(
                        new KeyValueRecordMetadata(key.innerMetadata, value.innerMetadata),
                        keyValueRecord)
                    .setConsistencyLevel(tableConfig.getConsistencyLevel())));
      }
    } catch (KafkaException | IOException e) {
      // The Kafka exception could occur if the record references an unknown topic.
      // The IOException can only theoretically happen when processing json data. But bad json
      // won't result in this exception. We're not pulling data from a file or any other kind of IO.
      // Most likely this error can't occur in this application...but we try to protect ourselves
      // anyway just in case.

      handleFailure(record, e, null, instanceState.getFailedRecordCounter());
    }
  }

  /**
   * Create a metadata object desribing the structure of the given key or value (extracted from a
   * {@link SinkRecord} and a data object that homogenizes interactions with the given key/value
   * (e.g. an implementation of {@link KeyOrValue}).
   *
   * @param keyOrValue the key or value
   * @return a pair of (RecordMetadata, KeyOrValue)
   * @throws IOException if keyOrValue is a String and JSON parsing fails in some unknown way. It's
   *     unclear if this exception can ever trigger in the context of this Connector.
   */
  private static InnerDataAndMetadata makeMeta(Object keyOrValue) throws IOException {
    KeyOrValue innerData;
    RecordMetadata innerMetadata;

    if (keyOrValue instanceof Struct) {
      Struct innerRecordStruct = (Struct) keyOrValue;
      // TODO: PERF: Cache these metadata objects, keyed on schema.
      innerMetadata = new StructDataMetadata(innerRecordStruct.schema());
      innerData = new StructData(innerRecordStruct);
    } else if (keyOrValue instanceof String) {
      innerMetadata = JSON_RECORD_METADATA;
      try {
        innerData = new JsonData(OBJECT_MAPPER, JSON_NODE_MAP_TYPE, (String) keyOrValue);
      } catch (RuntimeException e) {
        // Json parsing failed. Treat as raw string.
        innerData = new RawData(keyOrValue);
        innerMetadata = (RecordMetadata) innerData;
      }
    } else if (keyOrValue != null) {
      innerData = new RawData(keyOrValue);
      innerMetadata = (RecordMetadata) innerData;
    } else {
      // The key or value is null
      innerData = NULL_DATA;
      innerMetadata = NULL_DATA;
    }
    return new InnerDataAndMetadata(innerData, innerMetadata);
  }

  /**
   * Handle a failed record.
   *
   * @param record the {@link SinkRecord} that failed to process
   * @param e the exception
   * @param cql the cql statement that failed to execute
   * @param failCounter the metric that keeps track of number of failures encountered
   */
  synchronized void handleFailure(SinkRecord record, Throwable e, String cql, Counter failCounter) {
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

    failCounter.inc();
    TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
    long currentOffset = Long.MAX_VALUE;
    if (failureOffsets.containsKey(topicPartition)) {
      currentOffset = failureOffsets.get(topicPartition).offset();
    }
    if (record.kafkaOffset() < currentOffset) {
      failureOffsets.put(topicPartition, new OffsetAndMetadata(record.kafkaOffset()));
      context.offset(topicPartition, record.kafkaOffset() - 1);
    }

    String statementError = cql != null ? String.format("\n   statement: %s", cql) : "";

    log.warn(
        "Error inserting/updating row for Kafka record {}: {}{}",
        record,
        e.getMessage(),
        statementError);
  }

  /** Simple container class to tie together a {@link SinkRecord} key/value and its metadata. */
  private static class InnerDataAndMetadata {
    final KeyOrValue innerData;
    final RecordMetadata innerMetadata;

    InnerDataAndMetadata(KeyOrValue innerData, RecordMetadata innerMetadata) {
      this.innerMetadata = innerMetadata;
      this.innerData = innerData;
    }
  }

  /**
   * The DseSinkTask can be in one of three states, and shutdown behavior varies depending on the
   * current state.
   */
  enum TaskState {
    // Task is waiting for records from the infrastructure
    WAIT,
    // Task is processing a collection of SinkRecords
    RUN,
    // Task is in the process of stopping or is stopped
    STOP
  }
}
