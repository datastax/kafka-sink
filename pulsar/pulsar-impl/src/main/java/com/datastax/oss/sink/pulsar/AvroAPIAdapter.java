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

import com.datastax.oss.sink.RetriableException;
import com.datastax.oss.sink.config.ConfigException;
import com.datastax.oss.sink.record.SchemaSupport;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroAPIAdapter<Coat>
    implements APIAdapter<
        Coat, Object, LocalRecord<Coat, Object>, Schema, GenericRecord, Schema.Field, Header> {

  public static final Logger log = LoggerFactory.getLogger(AvroAPIAdapter.class);

  @Override
  public RuntimeException adapt(ConfigException ex) {
    return ex;
  }

  @Override
  public RuntimeException adapt(RetriableException ex) {
    return ex;
  }

  @Override
  public String topic(LocalRecord<Coat, Object> record) {
    return record.topic();
  }

  @Override
  public Object key(LocalRecord<Coat, Object> record) {
    return record.key();
  }

  @Override
  public Object value(LocalRecord<Coat, Object> record) {
    return record.payload();
  }

  @Override
  public Long timestamp(LocalRecord<Coat, Object> record) {
    return record.timestamp();
  }

  @Override
  public Set<Header> headers(LocalRecord<Coat, Object> record) {
    return record.headers();
  }

  @Override
  public String headerKey(Header header) {
    return header.name;
  }

  @Override
  public Object headerValue(Header header) {
    return header.value;
  }

  @Override
  public Schema headerSchema(Header header) {
    if (header.value == null) return schemaNull;
    log.debug(
        "get header {} {} {} {}",
        header.name,
        header.value,
        header.value.getClass(),
        primitiveSchemas.get(header.value.getClass()));
    Schema schema = primitiveSchemas.get(header.value.getClass());
    if (schema == null) schema = ((GenericContainer) header.value).getSchema();
    return schema;
  }

  @Override
  public boolean isStruct(Object object) {
    return object instanceof GenericRecord;
  }

  @Override
  public Schema schema(GenericRecord struct) {
    return struct.getSchema();
  }

  @Override
  public Set<String> fields(GenericRecord struct) {
    return struct
        .getSchema()
        .getFields()
        .stream()
        .map(Schema.Field::name)
        .collect(Collectors.toSet());
  }

  @Override
  public Object fieldValue(GenericRecord struct, String fieldName) {
    log.debug("get field val {} {}", struct, fieldName);
    Object v = struct.get(fieldName);
    log.debug("got {} {}", v, v == null ? "null" : v.getClass());
    return v;
  }

  @Override
  public Schema fieldSchema(Schema schema, String fieldSchema) {
    log.debug("get field schema {} -> {}", fieldSchema, schema.getField(fieldSchema).schema());
    return schema.getField(fieldSchema).schema();
  }

  private static final Schema schemaBoolean = Schema.create(Schema.Type.BOOLEAN);
  private static final Schema schemaString = Schema.create(Schema.Type.STRING);
  private static final Schema schemaDouble = Schema.create(Schema.Type.DOUBLE);
  private static final Schema schemaFloat = Schema.create(Schema.Type.FLOAT);
  private static final Schema schemaInt = Schema.create(Schema.Type.INT);
  private static final Schema schemaLong = Schema.create(Schema.Type.LONG);
  private static final Schema schemaNull = Schema.create(Schema.Type.NULL);
  private static final Schema schemaBytes = Schema.create(Schema.Type.BYTES);

  static {
    Class hbbClass = ByteBuffer.class;
    try {
      hbbClass =
          Class.forName("java.nio.HeapByteBuffer", false, AvroAPIAdapter.class.getClassLoader());
    } catch (Exception ex) {
      log.error("cound find HeapByteBuffer class");
    }
    primitiveSchemas =
        ImmutableMap.<Class, Schema>builder()
            .put(Boolean.class, schemaBoolean)
            .put(boolean.class, schemaBoolean)
            .put(String.class, schemaString)
            .put(Utf8.class, schemaString)
            .put(Double.class, schemaDouble)
            .put(double.class, schemaDouble)
            .put(Float.class, schemaFloat)
            .put(float.class, schemaFloat)
            .put(Integer.class, schemaInt)
            .put(int.class, schemaInt)
            .put(Long.class, schemaLong)
            .put(long.class, schemaLong)
            .put(Short.class, schemaInt)
            .put(short.class, schemaInt)
            .put(Byte.class, schemaInt)
            .put(byte.class, schemaInt)
            .put(byte[].class, schemaBytes)
            .put(ByteBuffer.class, schemaBytes)
            .put(hbbClass, schemaBytes)
            .build();
  }

  private static final ImmutableMap<Class, Schema> primitiveSchemas;

  private static final ImmutableMap<Schema.Type, SchemaSupport.Type> types =
      ImmutableMap.<Schema.Type, SchemaSupport.Type>builder()
          .put(Schema.Type.BOOLEAN, SchemaSupport.Type.BOOLEAN)
          .put(Schema.Type.BYTES, SchemaSupport.Type.BYTES)
          .put(Schema.Type.FLOAT, SchemaSupport.Type.FLOAT32)
          .put(Schema.Type.DOUBLE, SchemaSupport.Type.FLOAT64)
          .put(Schema.Type.INT, SchemaSupport.Type.INT32)
          .put(Schema.Type.LONG, SchemaSupport.Type.INT64)
          .put(Schema.Type.MAP, SchemaSupport.Type.MAP)
          .put(Schema.Type.STRING, SchemaSupport.Type.STRING)
          .put(Schema.Type.RECORD, SchemaSupport.Type.STRUCT)
          .put(Schema.Type.ARRAY, SchemaSupport.Type.ARRAY)
          .build();

  @Override
  public SchemaSupport.Type type(Schema schema) {
    Schema.Type t = schema.getType();
    if (schema.getType() == Schema.Type.UNION) {
      if (schema.getTypes().size() == 2 && schema.getTypes().get(0).getType() == Schema.Type.NULL) {
        t = schema.getTypes().get(1).getType();
      } else if (schema.getTypes().size() == 2
          && schema.getTypes().get(1).getType() == Schema.Type.NULL) {
        t = schema.getTypes().get(0).getType();
      }
    }
    if (!types.containsKey(t)) {
      log.error("not found type for schema type {} {}", schema.getType(), schema);
      return SchemaSupport.Type.STRING;
    }
    return types.get(t);
  }

  @Override
  public Schema valueSchema(Schema schema) {
    if (schema.getType() == Schema.Type.ARRAY) return schema.getElementType();
    return schema.getValueType();
  }

  @Override
  public Schema keySchema(Schema schema) {
    return schemaString;
  }

  @Override
  public Class<GenericRecord> structClass() {
    return GenericRecord.class;
  }
}
