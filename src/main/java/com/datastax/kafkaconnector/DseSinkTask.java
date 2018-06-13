/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
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

  public DseSinkTask() {}

  @Override
  public String version() {
    return new DseSinkConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {}

  @Override
  public void put(Collection<SinkRecord> sinkRecords) {
    // TODO: Remove this.
    sinkRecords.forEach(
        r -> {
          Struct parsed = (Struct) r.value();
          log.error(
              String.format(
                  "SANDMAN: offset=%d f1=%d f2=%d",
                  r.kafkaOffset(), parsed.getInt32("f1"), parsed.getInt32("f2")));
        });
    for (SinkRecord record : sinkRecords) {
      // TODO: Make a batch
      Struct parsed = (Struct) record.value();
      BoundStatement bound =
          DseSinkConnector.getStatement()
              .bind(record.timestamp(), parsed.getInt32("f1"), parsed.getInt32("f2"));
      DseSinkConnector.getSession().execute(bound);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // TODO: Implement. Probably flush pending batches.
  }

  @Override
  public void stop() {}
}
