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

import com.datastax.oss.sink.pulsar.util.Utf8ToStringGenericDatumReader;
import com.datastax.oss.sink.pulsar.util.kite.JsonUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonReader;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
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

  public static Schema<GenericRecord> pulsarSchema(
      org.apache.avro.generic.GenericRecord avroRecord) {
    return Schema.generic(schemaInfo(avroRecord, SchemaType.AVRO));
  }

  public static SchemaInfo schemaInfo(
      org.apache.avro.generic.GenericRecord record, SchemaType type) {
    return new SchemaInfo(
        record.getSchema().getName(),
        record.getSchema().toString().getBytes(),
        type,
        Collections.emptyMap());
  }

  public static Schema<GenericRecord> pulsarSchema(JsonNode jsonNode) {
    return Schema.generic(
        new SchemaInfo(
            "_",
            JsonUtil.inferSchema(jsonNode, "_").toString().getBytes(),
            SchemaType.JSON,
            Collections.emptyMap()));
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
    if (value instanceof GenericAvroRecord) {
      org.apache.avro.generic.GenericRecord arec = ((GenericAvroRecord) value).getAvroRecord();
      when(rec.getSchema()).thenReturn((Schema<T>) pulsarSchema(arec));
    } else if (value instanceof GenericJsonRecord) {
      GenericJsonRecord jrec = (GenericJsonRecord) value;
      when(rec.getSchema()).thenReturn((Schema<T>) pulsarSchema(jrec.getJsonNode()));
    }
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

  public static byte[] longBytes(long l) {
    return ByteBuffer.allocate(8).putLong(l).array();
  }

  public static byte[] intBytes(int i) {
    return ByteBuffer.allocate(4).putInt(i).array();
  }

  private static RuntimeException toRuntime(Throwable t) {
    if (t instanceof RuntimeException) return (RuntimeException) t;
    return new RuntimeException(t);
  }

  public static byte[] wornBytes(GenericContainer record) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DatumWriter<GenericContainer> dwrt = new GenericDatumWriter<>(record.getSchema());
    DataFileWriter<GenericContainer> wrt = new DataFileWriter<>(dwrt);
    try {
      wrt.create(record.getSchema(), baos);
      wrt.append(record);
      wrt.close();
    } catch (Exception ex) {
      throw toRuntime(ex);
    }
    return baos.toByteArray();
  }

  public static byte[] nakedBytes(GenericContainer record) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DatumWriter<GenericContainer> dwrt = new GenericDatumWriter<>(record.getSchema());
    Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
    try {
      dwrt.write(record, encoder);
      encoder.flush();
    } catch (Exception ex) {
      throw toRuntime(ex);
    }
    return baos.toByteArray();
  }

  public static GenericRecord pulsarGenericAvroRecord(org.apache.avro.generic.GenericRecord rec) {
    SchemaInfo info =
        new SchemaInfo(
            rec.getSchema().getName(),
            rec.getSchema().toString().getBytes(),
            SchemaType.AVRO,
            Collections.emptyMap());
    GenericSchema<GenericRecord> avroSchema = DefaultImplementation.getGenericSchema(info);
    return new GenericAvroRecord(null, rec.getSchema(), avroSchema.getFields(), rec);
  }

  private static ObjectMapper mapper = new ObjectMapper();

  public static GenericRecord pulsarGenericJsonRecord(String json) {
    try {
      return pulsarGenericJsonRecord(mapper.readTree(json));
    } catch (Exception ex) {
      throw toRuntime(ex);
    }
  }

  public static GenericRecord pulsarGenericJsonRecord(JsonNode node) {
    try {
      List<Field> fields = new ArrayList<>();
      int i = 0;
      for (Iterator<String> it = node.fieldNames(); it.hasNext(); ) {
        fields.add(new Field(it.next(), i++));
      }
      return new GenericJsonReader(fields).read(mapper.writeValueAsBytes(node));
    } catch (Exception ex) {
      throw toRuntime(ex);
    }
  }

  public static GenericContainer readWorn(byte[] data) {
    DatumReader<GenericContainer> reader = new Utf8ToStringGenericDatumReader<>();
    try (DataFileReader<GenericContainer> drdr =
        new DataFileReader<>(new SeekableByteArrayInput(data), reader)) {
      return drdr.next();
    } catch (Exception ex) {
      throw toRuntime(ex);
    }
  }
}
