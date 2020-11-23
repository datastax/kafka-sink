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
package com.datastax.oss.sink.kafka;

import avro.shaded.com.google.common.collect.ImmutableMap;
import com.datastax.oss.sink.EngineAPIAdapter;
import com.datastax.oss.sink.RetriableException;
import com.datastax.oss.sink.config.ConfigException;
import com.datastax.oss.sink.record.SchemaSupport;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;

public class KafkaAPIAdapter
    implements EngineAPIAdapter<SinkRecord, Schema, Struct, Field, Header> {
  @Override
  public RuntimeException adapt(ConfigException ex) {
    return new org.apache.kafka.common.config.ConfigException(ex.getMessage());
  }

  @Override
  public RuntimeException adapt(RetriableException ex) {
    return new org.apache.kafka.connect.errors.RetriableException(ex.getMessage(), ex.getCause());
  }

  @Override
  public String topic(SinkRecord sinkRecord) {
    return sinkRecord.topic();
  }

  @Override
  public Object key(SinkRecord sinkRecord) {
    return sinkRecord.key();
  }

  @Override
  public Object value(SinkRecord sinkRecord) {
    return sinkRecord.value();
  }

  @Override
  public Set<Header> headers(SinkRecord sinkRecord) {
    return StreamSupport.stream(sinkRecord.headers().spliterator(), false)
        .collect(Collectors.toSet());
  }

  @Override
  public String headerKey(Header header) {
    return header.key();
  }

  @Override
  public Object headerValue(Header header) {
    return header.value();
  }

  @Override
  public Schema headerSchema(Header header) {
    return header.schema();
  }

  @Override
  public Long timestamp(SinkRecord sinkRecord) {
    return sinkRecord.timestamp();
  }

  @Override
  public boolean isStruct(Object object) {
    return object instanceof Struct;
  }

  @Override
  public Schema schema(Struct struct) {
    return struct.schema();
  }

  @Override
  public Set<String> fields(Struct struct) {
    return struct.schema().fields().stream().map(Field::name).collect(Collectors.toSet());
  }

  @Override
  public Object fieldValue(Struct struct, String fieldName) {
    return struct.get(fieldName);
  }

  @Override
  public Schema fieldSchema(Schema schema, String fieldSchema) {
    return schema.field(fieldSchema).schema();
  }

  private static ImmutableMap<Schema.Type, SchemaSupport.Type> types =
      ImmutableMap.<Schema.Type, SchemaSupport.Type>builder()
          .put(Schema.Type.ARRAY, SchemaSupport.Type.ARRAY)
          .put(Schema.Type.BOOLEAN, SchemaSupport.Type.BOOLEAN)
          .put(Schema.Type.BYTES, SchemaSupport.Type.BYTES)
          .put(Schema.Type.FLOAT32, SchemaSupport.Type.FLOAT32)
          .put(Schema.Type.FLOAT64, SchemaSupport.Type.FLOAT64)
          .put(Schema.Type.INT8, SchemaSupport.Type.INT8)
          .put(Schema.Type.INT16, SchemaSupport.Type.INT16)
          .put(Schema.Type.INT32, SchemaSupport.Type.INT32)
          .put(Schema.Type.INT64, SchemaSupport.Type.INT64)
          .put(Schema.Type.MAP, SchemaSupport.Type.MAP)
          .put(Schema.Type.STRING, SchemaSupport.Type.STRING)
          .put(Schema.Type.STRUCT, SchemaSupport.Type.STRUCT)
          .build();

  @Override
  public SchemaSupport.Type type(Schema schema) {
    return types.get(schema.type());
  }

  @Override
  public Schema valueSchema(Schema schema) {
    return schema.valueSchema();
  }

  @Override
  public Schema keySchema(Schema schema) {
    return schema.keySchema();
  }

  @Override
  public Class<Struct> structClass() {
    return Struct.class;
  }
}
