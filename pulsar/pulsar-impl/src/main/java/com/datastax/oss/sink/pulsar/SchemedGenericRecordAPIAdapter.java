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
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemedGenericRecordAPIAdapter
    implements APIAdapter<
        GenericRecord, SchemedGenericRecord, Schema, SchemedGenericRecord, Schema.Field, Header> {

  private static final Logger log = LoggerFactory.getLogger(SchemedGenericRecordAPIAdapter.class);

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
    return schemedGenericRecord.getFields();
  }

  @Override
  public Object fieldValue(SchemedGenericRecord schemedGenericRecord, String fieldName) {
    return schemedGenericRecord.getField(fieldName);
  }

  @Override
  public Schema fieldSchema(Schema schema, String fieldName) {
    Schema schema1 = schema.getField(fieldName).schema();
    log.info("field schema {} {} {}", schema, fieldName, schema1);
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
