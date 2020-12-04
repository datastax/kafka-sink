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

import com.datastax.oss.sink.config.TopicConfig;
import com.datastax.oss.sink.pulsar.util.DataReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Connector(
  name = "dssc-bytes",
  type = IOType.SINK,
  help = "DataStax Pulsar Sink is used for moving messages from Pulsar to Cassandra",
  configClass = PulsarSinkConfig.class
)
public class BytesSink extends BaseSink<byte[], Object> {

  private static final Logger log = LoggerFactory.getLogger(BytesSink.class);

  private PulsarAdmin admin;
  private final Map<String, DataReader> valueReaders = new HashMap<>();

  private final boolean standalone;

  public BytesSink() {
    this(false);
  }

  public BytesSink(boolean standalone) {
    this.standalone = standalone;
  }

  public void setSchema(String topic, Schema schema) {
    if (schema == null) valueReaders.remove(topic);
    else valueReaders.put(topic, DataReader.createSingleAvro(schema));
  }

  @Override
  protected APIAdapter<byte[], Object, ?, ?, ?, Header> createAPIAdapter() {
    return new AvroAPIAdapter<>();
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void beforeStart(Map<String, Object> config, SinkContext sinkContext) throws Exception {
    if (!standalone) {
      PulsarAdminBuilder builder = PulsarAdmin.builder();
      Map<String, Object> adminServiceConfig =
          (Map<String, Object>) config.get("pulsarAdminService");
      if (adminServiceConfig != null) {
        if (adminServiceConfig.containsKey("url"))
          builder = builder.serviceHttpUrl((String) adminServiceConfig.get("url"));
        if (adminServiceConfig.containsKey("username")) {
          builder =
              builder.authentication(
                  (String) adminServiceConfig.get("username"),
                  (String) adminServiceConfig.getOrDefault("password", ""));
        }
      } else {
        throw new Exception("pulsarAdminService is not configured");
      }
      admin = builder.build();
    }
  }

  @Override
  protected void onEachTopicConfig(String topic, TopicConfig topicConfig, SinkContext sinkContext) {
    if (!standalone)
      checkSchemaRegister(topic, sinkContext.getTenant(), sinkContext.getNamespace());
  }

  @Override
  protected void onValueReaderDetected(String topic, DataReader reader) {
    valueReaders.put(topic, reader);
  }

  private void checkSchemaRegister(String topic, String tenant, String namespace) {
    if (!standalone) {
      log.debug("checking for schemas in register");
      try {
        SchemaInfo info =
            admin.schemas().getSchemaInfo(String.format("%s/%s/%s", tenant, namespace, topic));
        if (info != null) {
          org.apache.pulsar.client.api.Schema<org.apache.pulsar.client.api.schema.GenericRecord>
              apiSchema = org.apache.pulsar.client.api.Schema.generic(info);
          Schema schema = ((GenericAvroSchema) apiSchema).getAvroSchema();
          valueReaders.put(topic, DataReader.createSingleAvro(schema));
          log.debug("got value schema from register for [{}] {}", topic, schema);
        }
      } catch (PulsarAdminException ex) {
        log.warn("could not get value schema from register for topic " + topic, ex);
      }
    }
  }

  @Override
  protected Object readValue(Record<byte[]> record) throws IOException {
    if (record.getValue() == null) return null;
    DataReader reader = valueReaders.get(LocalRecord.shortTopic(record));
    if (reader != null) {
      return reader.read(record.getValue());
    } else {
      try {
        if (DataReader.mayContainAvroSchema(record.getValue())) {
          return DataReader.WORN_AVRO.read(record.getValue());
        } else {
          return DataReader.WORN_JSON.read(record.getValue());
        }
      } catch (Exception ex) {
        log.error(ex.getMessage(), ex);
        return new String(record.getValue(), StandardCharsets.UTF_8);
      }
    }
  }

  @Override
  protected void release() {
    if (admin != null) admin.close();
    valueReaders.clear();
  }
}
