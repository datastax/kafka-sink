package com.datastax.kafkaconnector;

import com.datastax.driver.core.BoundStatement;
import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DseSinkTask writes records to stdout or a file.
 */
public class DseSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(DseSinkTask.class);


  public DseSinkTask() {
  }

  @Override
  public String version() {
    return new DseSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
  }

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    for (SinkRecord record : sinkRecords) {
      // TODO: Make a batch
      BoundStatement bound = DseSinkConnector.getStatement().bind(record.timestamp(), record.value());
      log.trace("Writing line to {}: {}", record.timestamp(), record.value());
      DseSinkConnector.getSession().execute(bound);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // TODO: Implement. Probably flush pending batches.
  }

  @Override
  public void stop() {
  }
}
