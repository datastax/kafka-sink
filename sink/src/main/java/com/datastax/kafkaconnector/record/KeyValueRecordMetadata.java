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

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/** Metadata associated with a {@link KeyValueRecord}. */
public class KeyValueRecordMetadata implements RecordMetadata {

  private final RecordMetadata keyMetadata;
  private final RecordMetadata valueMetadata;
  private RecordMetadata headersMetadata;

  public KeyValueRecordMetadata(
      @Nullable RecordMetadata keyMetadata,
      @Nullable RecordMetadata valueMetadata,
      @Nullable RecordMetadata headersMetadata) {
    this.keyMetadata = keyMetadata;
    this.valueMetadata = valueMetadata;
    this.headersMetadata = headersMetadata;
  }

  @Override
  public GenericType<?> getFieldType(@NonNull String field, @NonNull DataType cqlType) {
    if (field.startsWith("key.")) {
      return keyMetadata != null ? keyMetadata.getFieldType(field.substring(4), cqlType) : null;
    } else if (field.startsWith("value.")) {
      return valueMetadata != null ? valueMetadata.getFieldType(field.substring(6), cqlType) : null;
    } else if (field.startsWith("header.")) {
      return headersMetadata != null
          ? headersMetadata.getFieldType(field.substring(7), cqlType)
          : null;
    } else {
      throw new IllegalArgumentException(
          "field name must start with 'key.', 'value.' or 'header.'.");
    }
  }
}
