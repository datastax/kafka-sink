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
package com.datastax.oss.common.sink.record;

import static com.datastax.oss.common.sink.record.StructDataMetadataSupport.getGenericType;

<<<<<<< HEAD:common/src/main/java/com/datastax/oss/common/sink/record/StructDataMetadata.java
import com.datastax.oss.common.sink.AbstractField;
=======
>>>>>>> 1.x:sink/src/main/java/com/datastax/oss/kafka/sink/record/StructDataMetadata.java
import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.AbstractStruct;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;

/** Metadata associated with a {@link StructData}. */
public class StructDataMetadata implements RecordMetadata {
  private final AbstractSchema schema;

  public StructDataMetadata(@NonNull AbstractSchema schema) {
    this.schema = schema;
  }

  @Override
  public GenericType<?> getFieldType(@NonNull String field, @NonNull DataType cqlType) {
    if (field.equals(RawData.FIELD_NAME)) {
      return GenericType.of(AbstractStruct.class);
    }
    AbstractField fieldMetadata = schema.field(field);
    if (fieldMetadata == null) {
      throw new RuntimeException("Field " + field + " is not defined in schema");
    }
    AbstractSchema fieldType = fieldMetadata.schema();
    return getGenericType(fieldType);
  }
}
