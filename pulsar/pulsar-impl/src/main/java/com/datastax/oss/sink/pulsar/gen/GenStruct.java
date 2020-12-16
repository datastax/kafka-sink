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
package com.datastax.oss.sink.pulsar.gen;

import com.datastax.oss.sink.pulsar.SchemedGenericRecord;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class GenStruct {
  private GenSchema.StructGenSchema schema;
  private Map<String, ?> values;

  public GenStruct(Map<String, ?> values, GenSchema.StructGenSchema schema) {
    this.schema = schema;
    this.values = Collections.unmodifiableMap(values);
  }

  public GenSchema.StructGenSchema getSchema() {
    return schema;
  }

  public Object value(String fieldName) {
    Object val = values.get(fieldName);
    if (val instanceof Map) return SchemedGenericRecord.castKeys((Map<?, ?>) val);
    return val;
  }

  public Set<String> fields() {
    return values.keySet();
  }

  public Map<String, ?> getValues() {
    return values;
  }
}
