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

import com.datastax.oss.sink.EngineAPIAdapter;
import com.datastax.oss.sink.RetriableException;
import com.datastax.oss.sink.config.ConfigException;
import com.datastax.oss.sink.record.SchemaSupport;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.functions.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;

public class PulsarAPIAdapter
    implements EngineAPIAdapter<
        PulsarAPIAdapter.LocalRecord, Schema, GenericRecord, Schema.Field, Header> {

  public static final Logger log = LoggerFactory.getLogger(PulsarAPIAdapter.class);

  @Override
  public RuntimeException adapt(ConfigException ex) {
    return ex;
  }

  @Override
  public RuntimeException adapt(RetriableException ex) {
    return ex;
  }

  @Override
  public String topic(LocalRecord record) {
    return record.topic;
  }

  @Override
  public Object key(LocalRecord record) {
    return record.key;
  }

  @Override
  public Object value(LocalRecord record) {
    return record.value;
  }

  @Override
  public Long timestamp(LocalRecord record) {
    return record.timestamp;
  }

  @Override
  public Set<Header> headers(LocalRecord record) {
    return record.headers;
  }

  @Override
  public String headerKey(Header header) {
    return header.name;
  }

  @Override
  public Object headerValue(Header header) {
    return header.value;
  }

  private static Schema STRING_SCHEMA = Schema.create(Schema.Type.STRING);

  private static Schema HEADER_SCHEMA =
      SchemaBuilder.record("header")
          .fields()
          .requiredString("key")
          .optionalString("value")
          .endRecord();

  @Override
  public Schema headerSchema(Header header) {
    return HEADER_SCHEMA;
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
    log.info("get field val {} {}", struct, fieldName);
    Object v = struct.get(fieldName);
    if (v instanceof Utf8) v = ((Utf8) v).toString();
    log.info("got {} {}", v, v.getClass());
    return v;
  }

  @Override
  public Schema fieldSchema(Schema schema, String fieldSchema) {
    log.info("get field schema {} -> {}", fieldSchema, schema.getField(fieldSchema).schema());
    return schema.getField(fieldSchema).schema();
  }

  private static ImmutableMap<Schema.Type, SchemaSupport.Type> types =
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
    return STRING_SCHEMA;
  }

  @Override
  public Class<GenericRecord> structClass() {
    return GenericRecord.class;
  }

  public static class LocalRecord {
    private String key;
    private GenericRecord value;
    private String topic;
    private Long timestamp;
    private Set<Header> headers;
    private Record<org.apache.pulsar.client.api.schema.GenericRecord> actual;

    public LocalRecord(Record<org.apache.pulsar.client.api.schema.GenericRecord> actual, GenericRecord value) {
      this.actual = actual;
      headers =
          actual.getProperties()
              .entrySet()
              .stream()
              .map(et -> new Header(et.getKey(), et.getValue()))
              .collect(Collectors.toSet());
      topic = actual.getTopicName().map(s -> s.substring(s.lastIndexOf("/") + 1)).orElse(null);
      timestamp = actual.getEventTime().orElse(null);
      key = actual.getKey().orElse(null);

      this.value = value;
    }

    public Record getActual() {
      return actual;
    }
  }
}
