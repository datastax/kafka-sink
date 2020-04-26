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
package com.datastax.kafkaconnector.record;

import static com.datastax.kafkaconnector.record.StructDataMetadataSupport.getGenericType;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/** Metadata associated with a {@link StructData}. */
public class StructDataMetadata implements RecordMetadata {
  private final Schema schema;

  public StructDataMetadata(@NonNull Schema schema) {
    this.schema = schema;
  }

  @Override
  public GenericType<?> getFieldType(@NonNull String field, @NonNull DataType cqlType) {
    if (field.equals(RawData.FIELD_NAME)) {
      return GenericType.of(Struct.class);
    }
    Schema fieldType = schema.field(field).schema();
    return getGenericType(fieldType);
  }
}
