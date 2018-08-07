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
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
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

  @Override
  public String version() {
    return new DseSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    instanceState = DseSinkConnector.getInstanceState(props.get(NAME_OPT));
    mappingObjects = Caffeine.newBuilder().build();
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
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

    DseSession session = instanceState.getSession();

    try {
      BatchStatementBuilder bsb = BatchStatement.builder(DefaultBatchType.UNLOGGED);
      for (SinkRecord record : sinkRecords) {
        String topicName = record.topic();
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
        bsb.addStatement(boundStatement);
      }
      session.execute(bsb.build());
    } catch (IOException e) {
      throw new InvalidRecordException("Could not parse record", e);
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
