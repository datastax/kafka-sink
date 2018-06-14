/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static com.datastax.kafkaconnector.DseSinkConnector.MAPPING_OPT;
import static com.datastax.kafkaconnector.DseSinkConnector.parseMappingString;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.nio.ByteBuffer;
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

  private Map<String, String> mapping;

  @Override
  public String version() {
    return new DseSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    mapping = parseMappingString(props.get(MAPPING_OPT));
    log.debug("Task will run with mapping: {}", mapping);
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    // TODO: Consider removing this logging.
    sinkRecords.forEach(
        r -> log.debug("SANDMAN: offset={} key={} value={}", r.kafkaOffset(), r.key(), r.value()));

    DseSession session = DseSinkConnector.getSession();
    CodecRegistry codecRegistry = session.getContext().codecRegistry();
    PreparedStatement preparedStatement = DseSinkConnector.getStatement();

    for (SinkRecord record : sinkRecords) {
      // TODO: Make a batch

      BoundStatement boundStatement = preparedStatement.bind();
      ProtocolVersion protocolVersion = session.getContext().protocolVersion();
      for (Map.Entry<String, String> entry : mapping.entrySet()) {
        String colName = entry.getKey();
        String recordFieldFullName = entry.getValue();
        String[] recordFieldNameParts = recordFieldFullName.split("\\.", 2);
        String componentType = recordFieldNameParts[0];
        String fieldName = recordFieldNameParts[1];

        Object component;
        if ("key".equals(componentType)) {
          component = record.key();
        } else if ("value".equals(componentType)) {
          component = record.value();
        } else {
          throw new RuntimeException(
              String.format("Unrecognized record component: %s", componentType));
        }

        // TODO: Handle straight-up 'key' or 'value'.

        // TODO: What if record field doesn't exist in schema or object?

        Object fieldValue = ((Struct) component).get(fieldName);
        DataType columnType = preparedStatement.getVariableDefinitions().get(colName).getType();

        ByteBuffer bb = codecRegistry.codecFor(columnType).encode(fieldValue, protocolVersion);
        boundStatement = boundStatement.setBytesUnsafe(colName, bb);
      }
      session.execute(boundStatement);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // TODO: Implement. Probably flush pending batches.
  }

  @Override
  public void stop() {}
}
