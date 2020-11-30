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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.sink.RecordProcessor;
import com.datastax.oss.sink.config.TableConfig;
import com.datastax.oss.sink.pulsar.util.DataReader;
import com.datastax.oss.sink.pulsar.util.JsonIsNotContainer;
import com.datastax.oss.sink.util.StringUtil;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
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
  name = "ds-cassandra-bytes",
  type = IOType.SINK,
  help = "DataStax Pulsar Sink is used for moving messages from Pulsar to Cassandra",
  configClass = PulsarSinkConfig.class
)
public class BytesSink implements BaseSink<byte[]> {

  private static final Logger log = LoggerFactory.getLogger(BytesSink.class);

  private PulsarAdmin admin;
  private final Map<String, DataReader> valueReaders = new HashMap<>();
  private final Map<String, DataReader> keyReaders = new HashMap<>();
  private final Map<String, Map<String, DataReader>> headerReaders = new HashMap<>();

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

  private PulsarRecordProcessor<byte[], Object> processor;

  @Override
  public RecordProcessor<LocalRecord<byte[], Object>, Header> processor() {
    return processor;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
    log.debug("start {}", getClass().getName());
    try {
      log.debug("starting processor");
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
      processor = new PulsarRecordProcessor<>(this, new AvroAPIAdapter<>());
      processor.start(StringUtil.flatString(config));
      processor
          .config()
          .getTopicConfigs()
          .forEach(
              (topic, topicConfig) -> {
                detectTopicReaders(topic, topicConfig.getTableConfigs());
                if (!standalone)
                  checkSchemaRegister(topic, sinkContext.getTenant(), sinkContext.getNamespace());
              });

      running.set(true);
      log.debug("started {}", getClass().getName());
    } catch (Throwable ex) {
      log.error("initialization error", ex);
      close();
      throw ex;
    }
  }

  private void detectTopicReaders(String topic, Collection<TableConfig> tableConfigs) {
    log.debug("checking column types for topic [{}]", topic);
    Metadata metadata = processor.getInstanceState().getSession().getMetadata();
    for (TableConfig tableConfig : tableConfigs) {
      for (Map.Entry<CqlIdentifier, CqlIdentifier> et : tableConfig.getMapping().entrySet()) {
        metadata
            .getKeyspace(tableConfig.getKeyspace())
            .flatMap(ksMeta -> ksMeta.getTable(tableConfig.getTable()))
            .flatMap(tableMeta -> tableMeta.getColumn(et.getKey()))
            .map(ColumnMetadata::getType)
            .flatMap(DataReader::get)
            .ifPresent(
                reader -> {
                  String path = et.getValue().asInternal();
                  if (path.equals("value.__self")) {
                    valueReaders.put(topic, reader);
                    log.debug(
                        "  chosen value reader for [{}] {}",
                        topic,
                        reader.getClass().getSimpleName());
                  } else if (path.equals("key.__self")) {
                    keyReaders.put(topic, reader);
                    log.debug(
                        "  chosen key reader for [{}] {}",
                        topic,
                        reader.getClass().getSimpleName());
                  } else if (path.startsWith("header.")) {
                    String headerKey = path.substring(path.indexOf('.') + 1);
                    headerReaders
                        .computeIfAbsent(topic, k -> new HashMap<>())
                        .put(headerKey, reader);
                    log.debug(
                        "  chosen header reader for [{}/{}] {}",
                        topic,
                        headerKey,
                        reader.getClass().getSimpleName());
                  }
                });
      }
    }
    log.debug("column types checked");
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
  public void write(Record<byte[]> record) throws Exception {
    if (!running.get()) throw new IllegalStateException("Sink is not open");

    log.debug("got record for processing {} {}", record.getValue(), record);
    String topic = LocalRecord.shortTopic(record);

    Object payload = null;
    if (record.getValue() != null) {

      DataReader reader = valueReaders.get(topic);
      if (reader != null) {
        payload = reader.read(record.getValue());
      } else {
        try {
          if (DataReader.mayContainAvroSchema(record.getValue())) {
            payload = DataReader.WORN_AVRO.read(record.getValue());
          } else {
            payload = DataReader.WORN_JSON.read(record.getValue());
          }
        } catch (Exception ex) {
          log.error(ex.getMessage(), ex);
          payload = new String(record.getValue(), StandardCharsets.UTF_8);
        }
      }
    }

    Object key =
        record
            .getKey()
            .map(keyString -> readFromString(keyReaders.get(topic), keyString))
            .orElse(null);

    Set<Header> headers =
        record
            .getProperties()
            .entrySet()
            .stream()
            .map(
                property -> {
                  Object val = null;
                  if (property.getValue() != null) {
                    val =
                        readFromString(
                            Optional.ofNullable(headerReaders.get(topic))
                                .map(m -> m.get(property.getKey()))
                                .orElse(null),
                            property.getValue());
                  }
                  return new Header(property.getKey(), val);
                })
            .collect(Collectors.toSet());

    log.debug("payload prepared {}", payload);
    log.debug("key prepared {}", key);
    log.debug(
        "headers prepared {}",
        headers.stream().map(h -> h.name + "=" + h.value).collect(Collectors.toSet()));
    if (payload != null || key != null || !headers.isEmpty())
      processor.process(Collections.singleton(new LocalRecord<>(record, headers, key, payload)));
    else log.debug("header, key and value are empty, nothing to process");
  }

  private Object readFromString(DataReader reader, String string) {
    try {
      if (reader != null) return reader.read(string);
      else
        try {
          return DataReader.WORN_JSON.read(string);
        } catch (JsonIsNotContainer ex) {
          return DataReader.primitiveValue(ex.getNode());
        }
    } catch (Exception ex) {
      return string;
    }
  }

  private AtomicBoolean running = new AtomicBoolean(false);

  @Override
  public void close() throws Exception {
    log.debug("closing {}", getClass().getName());
    if (processor != null) processor.stop();
    if (admin != null) admin.close();
    valueReaders.clear();
    keyReaders.clear();
    headerReaders.clear();
    running.set(false);
    log.debug("closed {}", getClass().getName());
  }
}
