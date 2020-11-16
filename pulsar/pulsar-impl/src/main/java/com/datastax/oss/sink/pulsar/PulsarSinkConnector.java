/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.sink.pulsar;

import com.datastax.oss.sink.config.CassandraSinkConfig;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Connector(
  name = "ds-cassandra",
  type = IOType.SINK,
  help = "PulsarSinkConnector is used for moving messages from Pulsar to Cassandra",
  configClass = CassandraSinkConfig.class
)
public class PulsarSinkConnector implements Sink<byte[]> {

  private static final Logger log = LoggerFactory.getLogger(PulsarSinkConnector.class);

  private PulsarRecordProcessor recordProcessor;
  private Converter converter;

  @Override
  public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
    // TODO instantiate converter according to config value.converter
    recordProcessor = new PulsarRecordProcessor();
    recordProcessor.start(
        config
            .entrySet()
            .stream()
            .filter(et -> Objects.nonNull(et.getValue()))
            .map(et -> new KeyValue<>(et.getKey(), String.valueOf(et.getValue())))
            .collect(HashMap::new, (m, kv) -> m.put(kv.getKey(), kv.getValue()), HashMap::putAll));
  }

  @Override
  public void write(Record<byte[]> record) throws Exception {
    String topic = record.getTopicName().orElse(null);
    SchemaAndValue schemaAndValue = converter.toConnectData(topic, record.getValue());
    ConnectHeaders headers = new ConnectHeaders();
    for (Map.Entry<String, String> prop : record.getProperties().entrySet())
      headers.addString(prop.getKey(), prop.getValue());
    SinkRecord sinkRecord =
        new SinkRecord(
            topic,
            0,
            Schema.OPTIONAL_STRING_SCHEMA,
            record.getKey().orElse(null),
            schemaAndValue.schema(),
            schemaAndValue.value(),
            record.getRecordSequence().orElse(0L),
            record.getEventTime().orElse(Instant.now().toEpochMilli()),
            TimestampType.NO_TIMESTAMP_TYPE,
            headers);
    recordProcessor.put(Collections.singleton(sinkRecord));
  }

  @Override
  public void close() throws Exception {
    recordProcessor.stop();
  }
}
