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
import static com.datastax.kafkaconnector.util.SinkUtil.NAME_OPT;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.kafkaconnector.codecs.KafkaCodecRegistry;
import com.datastax.kafkaconnector.config.TopicConfig;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
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

  @Override
  public String version() {
    return new DseSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    log.debug("Task DseSinkTask starting with props: {}", props);
    state = new AtomicReference<>();
    state.set(State.WAIT);
    instanceState = DseSinkConnector.getInstanceState(props.get(NAME_OPT));
    mappingObjects = Caffeine.newBuilder().build();
    failureOffsets = new ConcurrentHashMap<>();
    stopLatch = new CountDownLatch(1);
    instanceState.registerTask(this);
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
    log.debug("Received {} records", sinkRecords.size());

    state.compareAndSet(State.WAIT, State.RUN);

    failureOffsets.clear();
    DseSession session = instanceState.getSession();

    Instant start = Instant.now();
    List<CompletionStage<AsyncResultSet>> futures = new ArrayList<>(sinkRecords.size());
    try {
      Semaphore requestBarrier = instanceState.getRequestBarrier();
      for (SinkRecord record : sinkRecords) {
        if (state.get() == STOP) {
          // If the task is stopping abandon what we're doing.
          break;
        }
        String topicName = record.topic();
        TopicPartition topicPartition = new TopicPartition(topicName, record.kafkaPartition());
        if (failureOffsets.containsKey(topicPartition)) {
          // We've had a failure on this topic/partition already, so we don't want to process
          // more records for this topic/partition.
          // NB: We could lock failureOffsets before access, as we do for writes in query result
          // callbacks; but at it is, having this check is an optimization. If our timing is just
          // a bit off and we don't see an entry that is actually there, we'll process one
          // more record from the collection unnecessarily. Not really a big deal. Better to not
          // lock and be more efficient in the common case (where there is no error and thus
          // failureOffsets is empty). Besides, we use a ConcurrentHashMap, so the entry will
          // either be there or not; no inconsistent state.
          log.debug(
              "Skipping record with offset {} for topic/partition {} because a failure occurred when processing a previous record from this topic/partition",
              record.kafkaOffset(),
              topicPartition);
          continue;
        }
        KafkaCodecRegistry codecRegistry = instanceState.getCodecRegistry(topicName);
        PreparedStatement preparedStatement = instanceState.getPreparedInsertStatement(topicName);
        TopicConfig topicConfig = instanceState.getTopicConfig(topicName);
        Mapping mapping =
            mappingObjects.get(
                topicName, t -> new Mapping(topicConfig.getMapping(), codecRegistry));
        try {
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
          BoundStatement boundStatement =
              mapper.map(keyValueRecord).setConsistencyLevel(topicConfig.getConsistencyLevel());
          requestBarrier.acquire();
          CompletionStage<AsyncResultSet> future = session.executeAsync(boundStatement);
          futures.add(future);
          future.whenComplete(
              (result, ex) -> {
                requestBarrier.release();
                if (ex != null) {
                  handleFailure(
                      topicPartition, record, ex, instanceState.getInsertStatement(topicName));
                }
              });
        } catch (IOException e) {
          // This can only theoretically happen when processing json data. But bad json won't result
          // in this exception. We're not pulling data from a file or any other kind of IO.
          // Most likely this error can't occur in this application...but we try to protect
          // ourselves anyway just in case.
          handleFailure(topicPartition, record, e, instanceState.getInsertStatement(topicName));
        }
      }

      // Wait for outstanding requests to complete.
      for (CompletionStage<AsyncResultSet> f : futures) {
        try {
          f.toCompletableFuture().get();
        } catch (ExecutionException e) {
          // If any requests failed, they were handled by the "whenComplete" of the individual
          // future, so nothing to do here.
        }
      }

      Instant end = Instant.now();
      long ns = Duration.between(start, end).toNanos();
      log.debug("Completed {} inserts in {} microsecs", futures.size(), ns / 1000);

      context.requestCommit();
    } catch (InterruptedException e) {
      futures.forEach(
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
      instanceState.unregisterTask(this);
    }
  }

  private synchronized void handleFailure(
      TopicPartition topicPartition, SinkRecord record, Throwable e, String cql) {
    // Store the topic-partition and offset that had an error. However, we want
    // to keep track of the *lowest* offset in a topic-partiton that failed. Because
    // requests are sent in parallel and response ordering is non-deterministic,
    // it's possible for a failure in an insert with a higher offset be detected
    // before that of a lower offset. Thus, we only record a failure if
    // 1. There is no entry for this topic-partition, or
    // 2. There is an entry, but its offset is > our offset.
    //
    // This can happen in multiple invocations of this callback concurrently, so
    // we perform these checks/updates in a synchronized block. Presumably failures
    // don't occur that often, so we don't have to be very fancy here.
    long currentOffset = Long.MAX_VALUE;
    if (failureOffsets.containsKey(topicPartition)) {
      currentOffset = failureOffsets.get(topicPartition).offset();
    }
    if (record.kafkaOffset() < currentOffset) {
      failureOffsets.put(topicPartition, new OffsetAndMetadata(record.kafkaOffset()));
    }

    log.warn(
        "Error inserting row for Kafka record {}: {}\n   statement: {}",
        record,
        e.getMessage(),
        cql);
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
      innerMetadata = DseSinkConnector.JSON_RECORD_METADATA;
      try {
        innerData =
            new JsonData(
                DseSinkConnector.objectMapper,
                DseSinkConnector.jsonNodeMapType,
                (String) keyOrValue);
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
}
