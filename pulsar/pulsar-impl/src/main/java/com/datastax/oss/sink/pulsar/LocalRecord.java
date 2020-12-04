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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;

public class LocalRecord<Input, Payload> implements Record<Input> {

  private Record<Input> input;
  private Object key;
  private Payload payload;
  private Set<Header> headers;

  public LocalRecord(Record<Input> input, Set<Header> headers, Object key, Payload payload) {
    this(input, key, payload);
    this.headers = Collections.unmodifiableSet(headers);
  }

  public LocalRecord(Record<Input> input, Object key, Payload payload) {
    this(input, payload);
    this.key = key;
  }

  public LocalRecord(Record<Input> input, Payload payload) {
    this.input = input;
    this.payload = payload;
  }

  @Override
  public Optional<String> getTopicName() {
    return input.getTopicName();
  }

  public static String shortTopic(Record<?> record) {
    return record.getTopicName().map(s -> s.substring(s.lastIndexOf("/") + 1)).orElse(null);
  }

  public String topic() {
    return shortTopic(input);
  }

  @Override
  public Optional<String> getKey() {
    return input.getKey();
  }

  public String rawKey() {
    return input.getKey().orElse(null);
  }

  @Override
  public Schema<Input> getSchema() {
    return input.getSchema();
  }

  public Payload payload() {
    return payload;
  }

  public Object key() {
    return key;
  }

  @Override
  public Input getValue() {
    return input.getValue();
  }

  @Override
  public Optional<Long> getEventTime() {
    return input.getEventTime();
  }

  public Long timestamp() {
    return input.getEventTime().orElse(null);
  }

  @Override
  public Optional<String> getPartitionId() {
    return input.getPartitionId();
  }

  @Override
  public Optional<Long> getRecordSequence() {
    return input.getRecordSequence();
  }

  @Override
  public Map<String, String> getProperties() {
    return input.getProperties();
  }

  public Set<Header> headers() {
    return headers;
  }

  @Override
  public void ack() {
    input.ack();
  }

  @Override
  public void fail() {
    input.fail();
  }

  @Override
  public Optional<String> getDestinationTopic() {
    return input.getDestinationTopic();
  }

  @Override
  public Optional<Message<Input>> getMessage() {
    return input.getMessage();
  }
}
