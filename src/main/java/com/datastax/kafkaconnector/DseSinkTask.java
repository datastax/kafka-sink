/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

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
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
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
  private static final Logger log = LoggerFactory.getLogger(DseSinkTask.class);
  private static final RawData NULL_DATA = new RawData(null);
  private InstanceState instanceState;
  private Cache<String, Mapping> mappingObjects;
  private Map<TopicPartition, OffsetAndMetadata> failureOffsets;

  @Override
  public String version() {
    return new DseSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    instanceState = DseSinkConnector.getInstanceState(props.get(NAME_OPT));
    mappingObjects = Caffeine.newBuilder().build();
    failureOffsets = new ConcurrentHashMap<>();
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
    // TODO: Consider removing this logging.
    sinkRecords.forEach(
        r ->
            log.debug(
                "SANDMAN: topic={} offset={} key={} value={} timestamp={}",
                r.topic(),
                r.kafkaOffset(),
                r.key(),
                r.value(),
                r.timestamp()));

    failureOffsets.clear();
    DseSession session = instanceState.getSession();

    int concurrentRequests = instanceState.getConfig().getMaxConcurrentRequests();
    Instant start = Instant.now();
    try {
      Semaphore semaphore = new Semaphore(concurrentRequests);
      for (SinkRecord record : sinkRecords) {
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
          // failureOffsets is empty).
          log.debug(
              "Skipping record with offset {} for topic/partition {} because a failure occurred when processing a previous record from this topic/partition",
              record.kafkaOffset(),
              topicPartition);
          continue;
        }
        KafkaCodecRegistry codecRegistry = instanceState.getCodecRegistry(topicName);
        PreparedStatement preparedStatement = instanceState.getInsertStatement(topicName);
        TopicConfig topicConfig = instanceState.getConfig().getTopicConfigs().get(topicName);
        Mapping mapping =
            mappingObjects.get(
                topicName,
                t -> {
                  if (topicConfig != null) {
                    return new Mapping(topicConfig.getMapping(), codecRegistry);
                  } else {
                    throw new KafkaException(
                        String.format(
                            "Connector has no configuration for record topic '%s'. Please update the configuration and restart.",
                            topicName));
                  }
                });
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
          BoundStatement boundStatement = mapper.map(keyValueRecord);
          boundStatement.setConsistencyLevel(topicConfig.getConsistencyLevel());
          semaphore.acquire();
          CompletionStage<AsyncResultSet> future = session.executeAsync(boundStatement);
          future.whenComplete(
              (result, ex) -> {
                semaphore.release();
                if (ex != null) {
                  maybeUpdateFailureOffsets(topicPartition, record.kafkaOffset());
                  log.warn("Error inserting row for Kafka record {}: {}", record, ex.getMessage());
                }
              });
        } catch (IOException e) {
          // This can only theoretically happen when processing json data. But bad json won't result
          // in this exception. We're not pulling data from a file or any other kind of IO.
          // Most likely this error can't occur in this application...but we try to protect
          // ourselves anyway just in case.
          maybeUpdateFailureOffsets(topicPartition, record.kafkaOffset());
          log.warn("Error inserting row for Kafka record {}: {}", record, e.getMessage());
        }
      }

      // Wait for outstanding requests to complete.
      while (semaphore.availablePermits() < concurrentRequests) {
        Thread.sleep(100);
      }
      Instant end = Instant.now();
      long ns = Duration.between(start, end).toNanos();
      log.debug("Completed {} inserts in {} microsecs", sinkRecords.size(), ns / 1000);

      context.requestCommit();
    } catch (InterruptedException e) {
      throw new RetriableException("Interrupted while issuing queries");
    }
  }

  private synchronized void maybeUpdateFailureOffsets(TopicPartition topicPartition, long offset) {
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
    if (offset < currentOffset) {
      failureOffsets.put(topicPartition, new OffsetAndMetadata(offset));
    }
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

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // TODO: Implement. Probably flush pending batches.
  }

  @Override
  public void stop() {}

  private static class InnerDataAndMetadata {
    final KeyOrValue innerData;
    final RecordMetadata innerMetadata;

    InnerDataAndMetadata(KeyOrValue innerData, RecordMetadata innerMetadata) {
      this.innerMetadata = innerMetadata;
      this.innerData = innerData;
    }
  }
}
