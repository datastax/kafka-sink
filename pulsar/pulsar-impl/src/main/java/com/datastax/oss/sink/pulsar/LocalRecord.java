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

public class LocalRecord<Coat, Payload> implements Record<Coat> {

  private Record<Coat> coat;
  private Object key;
  private Payload payload;
  private Set<Header> headers;

  public LocalRecord(Record<Coat> coat, Set<Header> headers, Object key, Payload payload) {
    this(coat, key, payload);
    this.headers = Collections.unmodifiableSet(headers);
  }

  public LocalRecord(Record<Coat> coat, Object key, Payload payload) {
    this(coat, payload);
    this.key = key;
  }

  public LocalRecord(Record<Coat> coat, Payload payload) {
    this.coat = coat;
    this.payload = payload;
  }

  @Override
  public Optional<String> getTopicName() {
    return coat.getTopicName();
  }

  public static String shortTopic(Record<?> record) {
    return record.getTopicName().map(s -> s.substring(s.lastIndexOf("/") + 1)).orElse(null);
  }

  public String topic() {
    return shortTopic(coat);
  }

  @Override
  public Optional<String> getKey() {
    return coat.getKey();
  }

  public String rawKey() {
    return coat.getKey().orElse(null);
  }

  @Override
  public Schema<Coat> getSchema() {
    return coat.getSchema();
  }

  public Payload payload() {
    return payload;
  }

  public Object key() {
    return key;
  }

  @Override
  public Coat getValue() {
    return coat.getValue();
  }

  @Override
  public Optional<Long> getEventTime() {
    return coat.getEventTime();
  }

  public Long timestamp() {
    return coat.getEventTime().orElse(null);
  }

  @Override
  public Optional<String> getPartitionId() {
    return coat.getPartitionId();
  }

  @Override
  public Optional<Long> getRecordSequence() {
    return coat.getRecordSequence();
  }

  @Override
  public Map<String, String> getProperties() {
    return coat.getProperties();
  }

  public Set<Header> headers() {
    return headers;
  }

  @Override
  public void ack() {
    coat.ack();
  }

  @Override
  public void fail() {
    coat.fail();
  }

  @Override
  public Optional<String> getDestinationTopic() {
    return coat.getDestinationTopic();
  }

  @Override
  public Optional<Message<Coat>> getMessage() {
    return coat.getMessage();
  }
}
