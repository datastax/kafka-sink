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
package com.datastax.oss.kafka.sink;

import com.datastax.oss.common.sink.AbstractSinkRecord;
import com.datastax.oss.common.sink.AbstractSinkRecordHeader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;

/** Adapter from Kafka to Common */
public class KafkaSinkRecordAdapter implements AbstractSinkRecord {
  private final SinkRecord record;

  public KafkaSinkRecordAdapter(SinkRecord record) {
    this.record = record;
  }

  public SinkRecord getRecord() {
    return record;
  }

  @Override
  public Iterable<AbstractSinkRecordHeader> headers() {
    Headers headers = record.headers();
    return wrapHeaders(headers);
  }

  public static Iterable<AbstractSinkRecordHeader> wrapHeaders(Headers headers) {
    int size = headers.size();
    if (size == 0) {
      return Collections.emptyList();
    }
    List<AbstractSinkRecordHeader> wrapped = new ArrayList<>(headers.size());
    headers.forEach(
        h -> {
          wrapped.add(new KafkaHeader(h));
        });
    return wrapped;
  }

  @Override
  public Object key() {
    return KafkaStruct.wrap(record.key());
  }

  @Override
  public Object value() {
    return KafkaStruct.wrap(record.value());
  }

  @Override
  public Long timestamp() {
    return record.timestamp();
  }

  @Override
  public String topic() {
    return record.topic();
  }

  @Override
  public String toString() {
    return "KafkaSinkRecordAdapter{" + "record=" + record + '}';
  }
}
