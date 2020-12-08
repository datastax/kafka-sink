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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;

public class SchemedGenericRecordAPIAdapter
    implements APIAdapter<
        GenericRecord, SchemedGenericRecord, Schema, SchemedGenericRecord, Schema.Field, Header> {
  @Override
  public RuntimeException adapt(ConfigException ex) {
    return ex;
  }

  @Override
  public RuntimeException adapt(RetriableException ex) {
    return ex;
  }

  @Override
  public String topic(LocalRecord<GenericRecord, SchemedGenericRecord> localRecord) {
    return localRecord.topic();
  }

  @Override
  public Object key(LocalRecord<GenericRecord, SchemedGenericRecord> localRecord) {
    return localRecord.key();
  }

  @Override
  public Object value(LocalRecord<GenericRecord, SchemedGenericRecord> localRecord) {
    return localRecord.payload();
  }

  @Override
  public Set<Header> headers(LocalRecord<GenericRecord, SchemedGenericRecord> localRecord) {
    return localRecord.headers();
  }

  @Override
  public String headerKey(Header header) {
    return header.name();
  }

  @Override
  public Object headerValue(Header header) {
    return header.value();
  }

  @Override
  public Schema headerSchema(Header header) {
    return AvroAPIAdapter.headerAvroSchema(header);
  }

  @Override
  public Long timestamp(LocalRecord<GenericRecord, SchemedGenericRecord> localRecord) {
    return localRecord.timestamp();
  }

  @Override
  public boolean isStruct(Object object) {
    return object instanceof SchemedGenericRecord;
  }

  @Override
  public Schema schema(SchemedGenericRecord schemedGenericRecord) {
    return schemedGenericRecord.getSchema();
  }

  @Override
  public Set<String> fields(SchemedGenericRecord schemedGenericRecord) {
    return schemedGenericRecord
        .getSchema()
        .getFields()
        .stream()
        .map(Schema.Field::name)
        .collect(Collectors.toSet());
  }

  @Override
  public Object fieldValue(SchemedGenericRecord schemedGenericRecord, String fieldName) {
    Object val = schemedGenericRecord.getRecord().getField(fieldName);
    if (val instanceof GenericRecord)
      val =
          new SchemedGenericRecord(
              (GenericRecord) val, schemedGenericRecord.getSchema().getField(fieldName).schema());
    // workaround pulsar null->"null" bug
    if ("null".equals(val)) val = null;
    return val;
  }

  @Override
  public Schema fieldSchema(Schema schema, String fieldName) {
    return schema.getField(fieldName).schema();
  }

  @Override
  public SchemaSupport.Type type(Schema schema) {
    return AvroAPIAdapter.commonType(schema);
  }

  @Override
  public Schema valueSchema(Schema schema) {
    return AvroAPIAdapter.collectionElementSchema(schema);
  }

  @Override
  public Schema keySchema(Schema schema) {
    return AvroAPIAdapter.schemaString;
  }

  @Override
  public Class<SchemedGenericRecord> structClass() {
    return SchemedGenericRecord.class;
  }
}
