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

import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;

public class TestUtil {

  public static <T> Record<T> mockRecord(String topic, String key, T value, long offset) {
    return mockRecord(topic, key, value, offset, null, Collections.emptyMap(), null, null);
  }

  public static <T> Record<T> mockRecord(
      String topic, String key, T value, long offset, Runnable onAck, Runnable onFail) {
    return mockRecord(topic, key, value, offset, null, Collections.emptyMap(), onAck, onFail);
  }

  public static <T> Record<T> mockRecord(
      String topic,
      String key,
      T value,
      long offset,
      Long timestamp,
      Map<String, String> properties) {
    return mockRecord(topic, key, value, offset, timestamp, properties, null, null);
  }

  public static <T> Record<T> mockRecord(
      String topic, String key, T value, long offset, Long timestamp) {
    return mockRecord(topic, key, value, offset, timestamp, Collections.emptyMap(), null, null);
  }

  public static <T> Record<T> mockRecord(
      String topic,
      String key,
      T value,
      long offset,
      Long timestamp,
      Map<String, String> properties,
      Runnable onAck,
      Runnable onFail) {
    Record<T> rec = mock(Record.class);
    when(rec.getTopicName()).thenReturn(Optional.of("persistent://public/default/" + topic));
    when(rec.getRecordSequence()).thenReturn(Optional.ofNullable(offset));
    when(rec.getValue()).thenReturn(value);
    when(rec.getEventTime()).thenReturn(Optional.ofNullable(timestamp));
    //    Map<String, String> props =
    //        StreamSupport.stream(headers.spliterator(), false)
    //            .collect(HashMap::new, (m, h) -> m.put(h.name, h.value), HashMap::putAll);
    when(rec.getProperties()).thenReturn(properties);
    when(rec.getKey()).thenReturn(Optional.ofNullable(key));
    if (onAck != null)
      doAnswer(
              invocationOnMock -> {
                onAck.run();
                return null;
              })
          .when(rec)
          .ack();
    if (onFail != null)
      doAnswer(
              invocationOnMock -> {
                onFail.run();
                return null;
              })
          .when(rec)
          .fail();
    return rec;
  }

  public static GenericRecord pulsarGenericRecord(org.apache.avro.generic.GenericRecord rec) {
    SchemaInfo info =
        new SchemaInfo(
            rec.getSchema().getName(),
            rec.getSchema().toString().getBytes(),
            SchemaType.AVRO,
            Collections.emptyMap());
    GenericSchema<GenericRecord> avroSchema = DefaultImplementation.getGenericSchema(info);
    return new GenericAvroRecord(null, rec.getSchema(), avroSchema.getFields(), rec);
  }
}
