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
import com.datastax.oss.sink.pulsar.gen.GenSchema;
import com.datastax.oss.sink.pulsar.gen.GenValue;
import com.datastax.oss.sink.record.SchemaSupport;
import java.util.Set;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;

public class GenAPIAdapter
    implements APIAdapter<GenericRecord, GenValue, GenSchema, GenValue.GenStruct, Field, Header> {
  @Override
  public RuntimeException adapt(ConfigException ex) {
    return ex;
  }

  @Override
  public RuntimeException adapt(RetriableException ex) {
    return ex;
  }

  @Override
  public String topic(LocalRecord<GenericRecord, GenValue> localRecord) {
    return localRecord.topic();
  }

  @Override
  public Object key(LocalRecord<GenericRecord, GenValue> localGenRecord) {
    return localGenRecord.key();
  }

  @Override
  public Object value(LocalRecord<GenericRecord, GenValue> localGenRecord) {
    return localGenRecord.getValue();
  }

  @Override
  public Set<Header> headers(LocalRecord<GenericRecord, GenValue> localGenRecord) {
    return localGenRecord.headers();
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
  public GenSchema headerSchema(Header header) {
    return null;
  }

  @Override
  public Long timestamp(LocalRecord localGenRecord) {
    return localGenRecord.timestamp();
  }

  @Override
  public boolean isStruct(Object object) {
    return object instanceof GenValue.GenStruct;
  }

  @Override
  public GenSchema schema(GenValue.GenStruct genRecord) {
    return genRecord.getSchema();
  }

  @Override
  public Set<String> fields(GenValue.GenStruct genRecord) {
    return ((GenSchema.StructGenSchema) genRecord.getSchema()).fields();
  }

  @Override
  public Object fieldValue(GenValue.GenStruct genRecord, String fieldName) {
    return genRecord.value(fieldName);
  }

  @Override
  public GenSchema fieldSchema(GenSchema genSchema, String fieldName) {
    assert genSchema instanceof GenSchema.StructGenSchema;
    return ((GenSchema.StructGenSchema) genSchema).field(fieldName);
  }

  @Override
  public SchemaSupport.Type type(GenSchema genSchema) {
    return genSchema.type;
  }

  @Override
  public GenSchema valueSchema(GenSchema genSchema) {
    assert genSchema instanceof GenSchema.ArrayGenSchema
        || genSchema instanceof GenSchema.MapGenSchema;
    return ((GenSchema.CollectionGenSchema) genSchema).elementSchema();
  }

  @Override
  public GenSchema keySchema(GenSchema genSchema) {
    return GenSchema.MapGenSchema.KEY_SCHEMA;
  }

  @Override
  public Class<GenValue.GenStruct> structClass() {
    return GenValue.GenStruct.class;
  }
}
