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
import com.datastax.oss.sink.pulsar.record.LocalGenericRecord;
import com.datastax.oss.sink.util.StringUtil;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.SerializationException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Connector(
  name = "ds-cassandra",
  type = IOType.SINK,
  help = "PulsarSinkConnector is used for moving messages from Pulsar to Cassandra",
  configClass = PulsarSinkConfig.class
)
public class GenericRecordSink implements BaseSink<GenericRecord> {

  private static final Logger log = LoggerFactory.getLogger(GenericRecordSink.class);
  private PulsarRecordProcessor<GenericRecord, org.apache.avro.generic.GenericRecord>
      recordProcessor;

  @Override
  public void open(Map<String, Object> config, SinkContext sinkContext) {
    log.info("start {}", getClass().getName());
    try {
      recordProcessor = new PulsarRecordProcessor(this, new AvroAPIAdapter<>());
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
    log.info("got message to process {} {}", record);
    try {
      Method m = record.getValue().getClass().getMethod("getAvroRecord");
      Object avroRecord = m.invoke(record.getValue());
      m = avroRecord.getClass().getMethod("getSchema");
      Object avroSchema = m.invoke(avroRecord);

      LocalGenericRecord local = null;
      try {
        log.info("construct via parsing");
        String schemaStr = avroSchema.toString();
        String recStr = avroRecord.toString();
        Schema schema = new Schema.Parser().parse(schemaStr);
        log.info("parsed schema {}", schema);
        org.apache.avro.generic.GenericRecord rec =
            (org.apache.avro.generic.GenericRecord) jsonToAvro(recStr, schema);
        log.info("deserialized rec {}", rec);
        local = new LocalGenericRecord(record, rec);
      } catch (Throwable ex) {
        log.error("could not construct via parsing", ex);
      }

      if (local != null) {
        log.info("processing....");
        recordProcessor.process(Collections.singleton(local));
      } else {
        log.info("nothing to process");
      }
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw ex;
    }
  }

  private DecoderFactory decoderFactory = DecoderFactory.get();

  private Object jsonToAvro(String jsonString, Schema schema) {
    try {
      DatumReader<Object> reader = new GenericDatumReader<>(schema);
      Object object = reader.read(null, decoderFactory.jsonDecoder(schema, jsonString));

      if (schema.getType().equals(Schema.Type.STRING)) {
        object = ((Utf8) object).toString();
      }
      return object;
    } catch (IOException e) {
      throw new SerializationException(
          String.format("Error deserializing json %s to Avro of schema %s", jsonString, schema), e);
    }
  }

  @Override
  public void close() throws Exception {
    log.info("closing sink");
    recordProcessor.stop();
    log.info("closed");
  }
}
