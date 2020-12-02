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

import com.datastax.oss.sink.RecordProcessor;
import com.datastax.oss.sink.pulsar.gen.GenSchema;
import com.datastax.oss.sink.pulsar.gen.GenValue;
import com.datastax.oss.sink.util.StringUtil;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Connector(
  name = "dssc-generic",
  type = IOType.SINK,
  help = "PulsarSinkConnector is used for moving messages from Pulsar to Cassandra",
  configClass = PulsarSinkConfig.class
)
public class GenSchemaGenericRecordSink implements BaseSink<GenericRecord> {

  private static final Logger log = LoggerFactory.getLogger(GenSchemaGenericRecordSink.class);
  private PulsarRecordProcessor<GenericRecord, GenValue> recordProcessor;

  @Override
  public void open(Map<String, Object> config, SinkContext sinkContext) {
    log.debug("start {}", getClass().getName());
    try {
      recordProcessor = new PulsarRecordProcessor<>(this, new GenAPIAdapter());
      recordProcessor.start(StringUtil.flatString(config));
      System.out.println("started");
    } catch (Exception ex) {
      log.error("initialization error", ex);
      ex.printStackTrace();
      throw ex;
    }
  }

  @Override
  public RecordProcessor<?, ?> processor() {
    return recordProcessor;
  }

  @Override
  public void write(Record<GenericRecord> record) throws Exception {
    log.debug("got message to process {}", record);
    // TODO process headers
    Set<Header> headers = Collections.emptySet();
    // TODO process key
    Object key = null;
    recordProcessor.process(
        Collections.singleton(
            new LocalRecord<>(record, headers, key, GenSchema.adaptValue(record.getValue()))));
  }

  @Override
  public void close() throws Exception {
    log.debug("closing sink");
    recordProcessor.stop();
    log.debug("closed");
  }
}
