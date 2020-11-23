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

import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;

public class GRecordBuilder {

  private Schema schema;
  private GenericRecord record;

  public GRecordBuilder(Schema schema) {
    this.schema = schema;
    record = new GenericData.Record(schema);
  }

  public GRecordBuilder put(String fieldName, Object fieldValue) {
    record.put(fieldName, fieldValue);
    return this;
  }

  public GenericAvroRecord build() {
    return new GenericAvroRecord(
        null,
        schema,
        schema
            .getFields()
            .stream()
            .map(of -> new Field(of.name(), of.pos()))
            .collect(Collectors.toList()),
        record);
  }
}
