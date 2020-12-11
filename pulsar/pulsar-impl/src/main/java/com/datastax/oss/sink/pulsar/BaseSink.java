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
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.sink.RecordProcessor;
import com.datastax.oss.sink.config.TableConfig;
import com.datastax.oss.sink.config.TopicConfig;
import com.datastax.oss.sink.pulsar.util.ConfigUtil;
import com.datastax.oss.sink.pulsar.util.DataReader;
import com.datastax.oss.sink.pulsar.util.JsonIsNotContainer;
import com.datastax.oss.sink.util.SinkUtil;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseSink<Input, Payload> implements Sink<Input> {

  private static final Logger log = LoggerFactory.getLogger(BaseSink.class);

  protected final Map<String, DataReader> keyReaders = new HashMap<>();
  protected final Map<String, Map<String, DataReader>> headerReaders = new HashMap<>();
  protected PulsarRecordProcessor<Input, Payload> processor;

  protected abstract APIAdapter<Input, Payload, ?, ?, ?, Header> createAPIAdapter();

  public void onFailure(Record<Input> record, Throwable e) {
    log.info("record failed because of", e);
    record.fail();
  }

  public void onSuccess(Record<Input> record) {
    log.info("record acked");
    record.ack();
  }

  public final RecordProcessor<?, ?> processor() {
    return processor;
  }

  protected final Object readKey(Record<Input> record) {
    return record
        .getKey()
        .map(keyString -> readFromString(keyReaders.get(LocalRecord.shortTopic(record)), keyString))
        .orElse(null);
  }

  protected final Set<Header> readHeaders(Record<Input> record) {
    String topic = LocalRecord.shortTopic(record);
    return record
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
  }

  @Override
  public final void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
    log.debug("start {}", getClass().getName());
    try {
      log.debug("starting processor");
      @SuppressWarnings("unchecked")
      Map<String, Object> cfg =
          //              config.containsKey("configs") ?
          //              (Map<String, Object>) config.get("configs") :
          config;
      beforeStart(cfg, sinkContext);
      processor = new PulsarRecordProcessor<>(this, createAPIAdapter());
      processor.start(ConfigUtil.flatString(cfg));
      processor
          .config()
          .getTopicConfigs()
          .forEach(
              (topic, topicConfig) -> {
                detectTopicReaders(topic, topicConfig.getTableConfigs());
                onEachTopicConfig(topic, topicConfig, sinkContext);
              });

      running.set(true);
      log.debug("started {}", getClass().getName());
    } catch (Throwable ex) {
      log.error("initialization error", ex);
      close();
      throw ex;
    }
  }

  protected abstract void beforeStart(Map<String, Object> config, SinkContext sinkContext)
      throws Exception;

  protected abstract void onEachTopicConfig(
      String topic, TopicConfig topicConfig, SinkContext sinkContext);

  protected abstract void onValueReaderDetected(String topic, DataReader reader);

  protected void detectTopicReaders(String topic, Collection<TableConfig> tableConfigs) {
    log.debug("checking column types for topic [{}]", topic);
    Metadata metadata = processor.getInstanceState().getSession().getMetadata();
    for (TableConfig tableConfig : tableConfigs) {
      for (Map.Entry<CqlIdentifier, CqlIdentifier> et : tableConfig.getMapping().entrySet()) {
        metadata
            .getKeyspace(tableConfig.getKeyspace())
            .flatMap(ksMeta -> ksMeta.getTable(tableConfig.getTable()))
            .flatMap(
                tableMeta -> {
                  Optional<ColumnMetadata> res = tableMeta.getColumn(et.getKey());
                  if (!res.isPresent()
                      && (et.getKey().asInternal().equals(SinkUtil.TTL_VARNAME)
                          || et.getKey().asInternal().equals(SinkUtil.TIMESTAMP_VARNAME)))
                    res = Optional.of(LONG_COLUMN);
                  return res;
                })
            .map(ColumnMetadata::getType)
            .flatMap(DataReader::get)
            .ifPresent(
                reader -> {
                  String path = et.getValue().asInternal();
                  if (path.equals("value.__self")) {
                    log.info(
                        "  chosen value reader for [{}] {}",
                        topic,
                        reader.getClass().getSimpleName());
                    onValueReaderDetected(topic, reader);
                  } else if (path.equals("key.__self")) {
                    keyReaders.put(topic, reader);
                    log.info(
                        "  chosen key reader for [{}] {}",
                        topic,
                        reader.getClass().getSimpleName());
                  } else if (path.startsWith("header.")) {
                    String headerKey = path.substring(path.indexOf('.') + 1);
                    headerReaders
                        .computeIfAbsent(topic, k -> new HashMap<>())
                        .put(headerKey, reader);
                    log.info(
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

  protected abstract Payload readValue(Record<Input> record) throws Exception;

  @Override
  public final void write(Record<Input> record) throws Exception {
    if (!running.get()) throw new IllegalStateException("Sink is not open");

    log.info("got record for processing {} {}", record.getValue(), record);

    Payload payload = readValue(record);
    Object key = readKey(record);
    Set<Header> headers = readHeaders(record);

    log.info("payload prepared {}", payload);
    log.info("key prepared {}", key);
    log.info(
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
          return structuredStringReader().read(string);
        } catch (JsonIsNotContainer ex) {
          return DataReader.primitiveValue(ex.getNode());
        }
    } catch (Exception ex) {
      return string;
    }
  }

  protected abstract DataReader structuredStringReader();

  private AtomicBoolean running = new AtomicBoolean(false);

  protected abstract void release();

  @Override
  public final void close() throws Exception {
    log.debug("closing {}", getClass().getName());
    if (processor != null) processor.stop();
    release();
    keyReaders.clear();
    headerReaders.clear();
    running.set(false);
    log.debug("closed {}", getClass().getName());
  }

  @SuppressWarnings("NullableProblems")
  private static ColumnMetadata LONG_COLUMN =
      new ColumnMetadata() {
        public CqlIdentifier getKeyspace() {
          return null;
        }

        public CqlIdentifier getParent() {
          return null;
        }

        public CqlIdentifier getName() {
          return null;
        }

        public DataType getType() {
          return DataTypes.BIGINT;
        }

        public boolean isStatic() {
          return false;
        }
      };
}
