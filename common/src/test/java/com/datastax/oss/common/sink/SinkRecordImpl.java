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
package com.datastax.oss.common.sink;

import java.util.ArrayList;
import java.util.List;

/** Implementation for tests. */
public class SinkRecordImpl implements AbstractSinkRecord {

  private String topic;
  private Object key;
  private Object value;
  private List<AbstractSinkRecordHeader> headers = new ArrayList();

  SinkRecordImpl(
      String topic,
      int partition,
      AbstractSchema keySchema,
      Object key,
      AbstractSchema valueSchema,
      Object value,
      long kafkaOffset) {
    this.topic = topic;
    this.key = key;
    this.value = value;
  }

  @Override
  public Iterable<AbstractSinkRecordHeader> headers() {
    return headers;
  }

  @Override
  public Object key() {
    return key;
  }

  @Override
  public Object value() {
    return value;
  }

  @Override
  public Long timestamp() {
    return null;
  }

  @Override
  public String topic() {
    return topic;
  }
}
