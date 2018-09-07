/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static com.datastax.kafkaconnector.DseSinkTask.State.RUN;
import static com.datastax.kafkaconnector.DseSinkTask.State.STOP;
import static com.datastax.kafkaconnector.DseSinkTask.State.WAIT;
import static com.datastax.kafkaconnector.util.SinkUtil.JSON_NODE_MAP_TYPE;
import static com.datastax.kafkaconnector.util.SinkUtil.JSON_RECORD_METADATA;
import static com.datastax.kafkaconnector.util.SinkUtil.OBJECT_MAPPER;

import com.datastax.kafkaconnector.codecs.KafkaCodecRegistry;
import com.datastax.kafkaconnector.config.TopicConfig;
import com.datastax.kafkaconnector.util.InstanceState;
import com.datastax.kafkaconnector.util.SinkUtil;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DseSinkTask does the heavy lifting of processing {@link SinkRecord}s and writing them to DSE. */
public class DseSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(DseSinkTask.class);
  private static final RawData NULL_DATA = new RawData(null);
  private InstanceState instanceState;
  private Cache<String, Mapping> mappingObjects;
  private Map<TopicPartition, OffsetAndMetadata> failureOffsets;
  private AtomicReference<State> state;
  private CountDownLatch stopLatch;
  private ExecutorService boundStatementProcessorService =
      Executors.newFixedThreadPool(
          1, new ThreadFactoryBuilder().setNameFormat("bound-statement-processor-%d").build());

  @Override
  public String version() {
    return new DseSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    log.debug("Task DseSinkTask starting with props: {}", props);
    state = new AtomicReference<>();
    state.set(State.WAIT);
    instanceState = SinkUtil.startTask(this, props);
    mappingObjects = Caffeine.newBuilder().build();
    failureOffsets = new ConcurrentHashMap<>();
    stopLatch = new CountDownLatch(1);
  }

  @Override
  public Map<TopicPartition, OffsetAndMetadata> preCommit(
      Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // Copy all of the failures (which point to the offset that we should retrieve from next time)
    // into currentOffsets.
    currentOffsets.putAll(failureOffsets);
    return currentOffsets;
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    if (sinkRecords.isEmpty()) {
      // Nothing to process.
      return;
    }

    log.debug("Received {} records", sinkRecords.size());

    state.compareAndSet(State.WAIT, State.RUN);

    failureOffsets.clear();

    Instant start = Instant.now();
    List<CompletableFuture<Void>> mappingFutures;
    Collection<CompletionStage<AsyncResultSet>> queryFutures = new ConcurrentLinkedQueue<>();
    BlockingQueue<RecordAndStatement> boundStatementsQueue = new LinkedBlockingQueue<>();
    BoundStatementProcessor boundStatementProcessor =
        new BoundStatementProcessor(boundStatementsQueue, queryFutures);
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
      state.compareAndSet(State.RUN, State.WAIT);
      if (state.get() == STOP) {
        // Task is stopping. Notify the caller of stop() that we're done working.
        stopLatch.countDown();
      }
    }
  }

  private void mapAndQueueRecord(
      BlockingQueue<RecordAndStatement> boundStatementsQueue, SinkRecord record) {
    try {
      boundStatementsQueue.offer(new RecordAndStatement(record, mapRecord(record)));
    } catch (KafkaException e) {
      // The Kafka exception could occur if the record references an unknown topic.
      handleFailure(record, e, null);
    } catch (IOException e) {
      // The IOException can only theoretically happen when processing json data. But bad json
      // won't result in this exception. We're not pulling data from a file or any other kind of IO.
      // Most likely this error can't occur in this application...but we try to protect ourselves
      // anyway just in case.
      String topicName = record.topic();
      handleFailure(record, e, instanceState.getInsertStatement(topicName));
    }
  }

  private BoundStatement mapRecord(SinkRecord record) throws IOException {
    String topicName = record.topic();
    KafkaCodecRegistry codecRegistry = instanceState.getCodecRegistry(topicName);
    PreparedStatement preparedStatement = instanceState.getPreparedInsertStatement(topicName);
    TopicConfig topicConfig = instanceState.getTopicConfig(topicName);
    Mapping mapping =
        mappingObjects.get(topicName, t -> new Mapping(topicConfig.getMapping(), codecRegistry));
    InnerDataAndMetadata key = makeMeta(record.key());
    InnerDataAndMetadata value = makeMeta(record.value());
    KeyValueRecord keyValueRecord =
        new KeyValueRecord(key.innerData, value.innerData, record.timestamp());
    RecordMapper mapper =
        new RecordMapper(
            preparedStatement,
            mapping,
            new KeyValueRecordMetadata(key.innerMetadata, value.innerMetadata),
            topicConfig.isNullToUnset(),
            true,
            false);
    return mapper.map(keyValueRecord).setConsistencyLevel(topicConfig.getConsistencyLevel());
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
      if (state.compareAndSet(WAIT, STOP)) {
        // Clean stop; nothing running/in-progress.
        stopLatch.countDown();
        return;
      }
      state.compareAndSet(RUN, STOP);
      stopLatch.await();
    } catch (InterruptedException e) {
      // "put" is likely also interrupted, so we're effectively stopped.
      Thread.currentThread().interrupt();
    } finally {
      log.info("Task is stopped.");
      SinkUtil.stopTask(instanceState, this);
    }
  }

  private synchronized void handleFailure(SinkRecord record, Throwable e, String cql) {
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

    TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
    long currentOffset = Long.MAX_VALUE;
    if (failureOffsets.containsKey(topicPartition)) {
      currentOffset = failureOffsets.get(topicPartition).offset();
    }
    if (record.kafkaOffset() < currentOffset) {
      failureOffsets.put(topicPartition, new OffsetAndMetadata(record.kafkaOffset()));
      context.offset(topicPartition, record.kafkaOffset());
    }

    String statementError = cql != null ? String.format("\n   statement: %s", cql) : "";

    log.warn(
        "Error inserting row for Kafka record {}: {}{}", record, e.getMessage(), statementError);
  }

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

  enum State {
    WAIT,
    RUN,
    STOP
  }

  private static class InnerDataAndMetadata {
    final KeyOrValue innerData;
    final RecordMetadata innerMetadata;

    InnerDataAndMetadata(KeyOrValue innerData, RecordMetadata innerMetadata) {
      this.innerMetadata = innerMetadata;
      this.innerData = innerData;
    }
  }

  /**
   * Runnable class that pulls [sink-record, bound-statement] pairs from a queue and groups them
   * based on topic and routing-key, and then issues batch statements when groups are large enough
   * (currently 32). Execute BoundStatement's when there is only one in a group and we know no more
   * BoundStatements will be added to the queue.
   */
  private class BoundStatementProcessor implements Runnable {
    private static final int MAX_BATCH_SIZE = 32;
    private final RecordAndStatement END_STATEMENT = new RecordAndStatement(null, null);
    private final BlockingQueue<RecordAndStatement> boundStatementsQueue;
    private final Collection<CompletionStage<AsyncResultSet>> queryFutures;
    private final AtomicInteger successfulRecordCount = new AtomicInteger();

    BoundStatementProcessor(
        BlockingQueue<RecordAndStatement> boundStatementsQueue,
        Collection<CompletionStage<AsyncResultSet>> queryFutures) {
      this.boundStatementsQueue = boundStatementsQueue;
      this.queryFutures = queryFutures;
    }

    private void queueStatements(List<RecordAndStatement> statements) {
      Statement statement;
      if (statements.size() == 1) {
        statement = statements.get(0).getStatement();
      } else {
        BatchStatementBuilder bsb = BatchStatement.builder(DefaultBatchType.UNLOGGED);
        statements.stream().map(RecordAndStatement::getStatement).forEach(bsb::addStatement);
        // Construct the batch statement; set its consistency level to that of its first
        // bound statement. All bound statements in a bucket have the same CL, so this is fine.
        statement =
            bsb.build().setConsistencyLevel(statements.get(0).getStatement().getConsistencyLevel());
      }
      @NotNull Semaphore requestBarrier = instanceState.getRequestBarrier();
      requestBarrier.acquireUninterruptibly();
      CompletionStage<AsyncResultSet> future = instanceState.getSession().executeAsync(statement);
      queryFutures.add(future);
      future.whenComplete(
          (result, ex) -> {
            requestBarrier.release();
            if (ex != null) {
              statements.forEach(
                  recordAndStatement -> {
                    SinkRecord record = recordAndStatement.getRecord();
                    handleFailure(record, ex, instanceState.getInsertStatement(record.topic()));
                  });
            } else {
              successfulRecordCount.addAndGet(statements.size());
            }
          });
    }

    int getSuccessfulRecordCount() {
      return successfulRecordCount.get();
    }

    @Override
    public void run() {
      // Map of <topic, map<partition-key, list<recordAndStatement>>
      Map<String, Map<ByteBuffer, List<RecordAndStatement>>> statementGroups = new HashMap<>();
      List<RecordAndStatement> pendingStatements = new ArrayList<>();
      boolean interrupted = false;
      try {
        //noinspection InfiniteLoopStatement
        while (true) {
          if (state.get() == STOP) {
            // If the task is stopping abandon what we're doing.
            return;
          }
          pendingStatements.clear();
          boundStatementsQueue.drainTo(pendingStatements);
          if (pendingStatements.isEmpty()) {
            try {
              pendingStatements.add(boundStatementsQueue.take());
            } catch (InterruptedException e) {
              interrupted = true;
              continue;
            }
          }

          for (RecordAndStatement recordAndStatement : pendingStatements) {
            if (recordAndStatement.equals(END_STATEMENT)) {
              // There are no more bound-statements being produced.
              // Create and execute remaining statement groups,
              // creating BatchStatement's when a group has more than
              // one BoundStatement.
              statementGroups
                  .values()
                  .stream()
                  .map(Map::values)
                  .flatMap(Collection::stream)
                  .forEach(this::queueStatements);
              return;
            }

            // Get the routing-key and add this statement to the appropriate
            // statement group.

            ByteBuffer routingKey = recordAndStatement.getStatement().getRoutingKey();
            Map<ByteBuffer, List<RecordAndStatement>> topicGroup =
                statementGroups.computeIfAbsent(
                    recordAndStatement.getRecord().topic(), t -> new HashMap<>());
            List<RecordAndStatement> recordsAndStatements =
                topicGroup.computeIfAbsent(routingKey, t -> new ArrayList<>());
            recordsAndStatements.add(recordAndStatement);
            if (recordsAndStatements.size() == MAX_BATCH_SIZE) {
              // We're ready to send out a batch request!
              queueStatements(recordsAndStatements);
              recordsAndStatements.clear();
            }
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }

    void stop() {
      boundStatementsQueue.add(END_STATEMENT);
    }
  }

  /** Simple container class to hold a SinkRecord and its associated BoundStatement. */
  private static class RecordAndStatement {
    private final SinkRecord record;
    private final BoundStatement statement;

    RecordAndStatement(SinkRecord record, BoundStatement statement) {
      this.record = record;
      this.statement = statement;
    }

    SinkRecord getRecord() {
      return record;
    }

    BoundStatement getStatement() {
      return statement;
    }
  }
}
