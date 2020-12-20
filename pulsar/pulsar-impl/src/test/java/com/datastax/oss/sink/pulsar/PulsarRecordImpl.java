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

import java.util.Optional;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

/** Simple Record implementation */
public class PulsarRecordImpl implements Record<GenericRecord> {
  private final String topic;
  private final GenericRecord value;
  private final Schema<GenericRecord> schema;
  private final String key;
  private final long eventTime;

  public PulsarRecordImpl(String topic, String key, GenericRecord value, Schema schema) {
    this.value = value;
    this.schema = schema;
    this.topic = topic;
    this.key = key;
    this.eventTime = System.currentTimeMillis();
  }

  @Override
  public Optional<String> getTopicName() {
    return Optional.ofNullable(topic);
  }

  @Override
  public Optional<String> getKey() {
    return Optional.ofNullable(key);
  }

  @Override
  public Optional<Long> getEventTime() {
    return Optional.of(eventTime);
  }

  @Override
  public Schema<GenericRecord> getSchema() {
    return schema;
  }

  @Override
  public GenericRecord getValue() {
    return value;
  }
}
