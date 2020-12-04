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
import com.datastax.oss.sink.pulsar.gen.GenStruct;
import com.datastax.oss.sink.record.SchemaSupport;
import java.util.Set;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;

public class GenAPIAdapter
    implements APIAdapter<GenericRecord, GenStruct, GenSchema, GenStruct, Field, Header> {

  @Override
  public RuntimeException adapt(ConfigException ex) {
    return ex;
  }

  @Override
  public RuntimeException adapt(RetriableException ex) {
    return ex;
  }

  @Override
  public String topic(LocalRecord<GenericRecord, GenStruct> localRecord) {
    return localRecord.topic();
  }

  @Override
  public Object key(LocalRecord<GenericRecord, GenStruct> localRecord) {
    return localRecord.key();
  }

  @Override
  public Object value(LocalRecord<GenericRecord, GenStruct> localRecord) {
    return localRecord.payload();
  }

  @Override
  public Set<Header> headers(LocalRecord<GenericRecord, GenStruct> localRecord) {
    return localRecord.headers();
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
  public Long timestamp(LocalRecord localRecord) {
    return localRecord.timestamp();
  }

  @Override
  public boolean isStruct(Object object) {
    return object instanceof GenStruct;
  }

  @Override
  public GenSchema schema(GenStruct struct) {
    return struct.getSchema();
  }

  @Override
  public Set<String> fields(GenStruct struct) {
    return struct.getSchema().fields();
  }

  @Override
  public Object fieldValue(GenStruct struct, String fieldName) {
    return struct.value(fieldName);
  }

  @Override
  public GenSchema fieldSchema(GenSchema schema, String fieldName) {
    assert schema instanceof GenSchema.StructGenSchema;
    return ((GenSchema.StructGenSchema) schema).field(fieldName);
  }

  @Override
  public SchemaSupport.Type type(GenSchema schema) {
    return schema.type;
  }

  @Override
  public GenSchema valueSchema(GenSchema schema) {
    assert schema instanceof GenSchema.CollectionGenSchema;
    return ((GenSchema.CollectionGenSchema) schema).elementSchema();
  }

  @Override
  public GenSchema keySchema(GenSchema schema) {
    return GenSchema.MapGenSchema.KEY_SCHEMA;
  }

  @Override
  public Class<GenStruct> structClass() {
    return GenStruct.class;
  }
}
