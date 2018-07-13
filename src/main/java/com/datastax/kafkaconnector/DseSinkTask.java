/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static com.datastax.kafkaconnector.DseSinkConfig.parseMappingString;
import static com.datastax.kafkaconnector.DseSinkConnector.MAPPING_OPT;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.kafkaconnector.codecs.KafkaCodecRegistry;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DseSinkTask writes records to stdout or a file. */
public class DseSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(DseSinkTask.class);
  private SessionState sessionState;
  private Map<CqlIdentifier, CqlIdentifier> mapping;

  @Override
  public String version() {
    return new DseSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    mapping = parseMappingString(props.get(MAPPING_OPT));
    log.debug("Task will run with mapping: {}", mapping);

    // TODO: Use a caffeine cache keyed on "session attributes" to allow us to
    // get/create a session with particular attributes.
    sessionState = DseSinkConnector.getSessionState();
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    // TODO: Consider removing this logging.
    sinkRecords.forEach(
        r -> log.debug("SANDMAN: offset={} key={} value={}", r.kafkaOffset(), r.key(), r.value()));

    DseSession session = sessionState.getSession();
    KafkaCodecRegistry codecRegistry = sessionState.getCodecRegistry();
    PreparedStatement preparedStatement = sessionState.getInsertStatement();
    // TODO: PERF: Cache the mapping, keyed on DseSession and the table being loaded.
    Mapping mapping =
        new Mapping(
            this.mapping,
            codecRegistry,
            CqlIdentifier.fromInternal(DseSinkConfig.MappingInspector.INTERNAL_TIMESTAMP_VARNAME));
    try {
      for (SinkRecord record : sinkRecords) {
        // TODO: Make a batch

        InnerRecordAndMetadata key = makeMeta(record.key());
        InnerRecordAndMetadata value = makeMeta(record.value());
        KeyValueRecord keyValueRecord = new KeyValueRecord(key.innerRecord, value.innerRecord);
        RecordMapper mapper =
            new RecordMapper(
                preparedStatement,
                mapping,
                new KeyValueRecordMetadata(key.innerMetadata, value.innerMetadata),
                true,
                true,
                false);
        Statement boundStatement = mapper.map(keyValueRecord);
        session.execute(boundStatement);
      }
    } catch (IOException e) {
      throw new InvalidRecordException("Could not parse record", e);
    }
  }

  private static InnerRecordAndMetadata makeMeta(Object keyOrValue) throws IOException {
    Record innerRecord = null;
    RecordMetadata innerRecordMeta = null;

    if (keyOrValue instanceof Struct) {
      Struct innerRecordStruct = (Struct) keyOrValue;
      // TODO: PERF: Cache these metadata objects, keyed on schema.
      innerRecordMeta = new StructRecordMetadata(innerRecordStruct.schema());
      innerRecord = new StructData(innerRecordStruct);
    } else if (keyOrValue instanceof String) {
      // TODO: Refine analysis instead of assuming it's JSON.
      innerRecordMeta = DseSinkConnector.JSON_RECORD_METADATA;
      innerRecord =
          new JsonData(
              DseSinkConnector.objectMapper, DseSinkConnector.jsonNodeMapType, (String) keyOrValue);
    }
    return new InnerRecordAndMetadata(innerRecord, innerRecordMeta);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // TODO: Implement. Probably flush pending batches.
  }

  @Override
  public void stop() {}

  private static class InnerRecordAndMetadata {
    final Record innerRecord;
    final RecordMetadata innerMetadata;

    InnerRecordAndMetadata(Record innerRecord, RecordMetadata innerMetadata) {
      this.innerMetadata = innerMetadata;
      this.innerRecord = innerRecord;
    }
  }
}
