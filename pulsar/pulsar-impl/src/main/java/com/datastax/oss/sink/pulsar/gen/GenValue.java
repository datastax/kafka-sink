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

import java.util.Collections;
import java.util.Map;

public class GenValue<Value> {

  protected GenSchema schema;
  protected Value value;

  public static final GenValue NULL = new GenValue<>(null, GenSchema.NULL);

  public GenValue(Value value, GenSchema schema) {
    this.value = value;
    this.schema = schema;
  }

  public GenSchema getSchema() {
    return schema;
  }

  public Value getValue() {
    return value;
  }

  public static class GenStruct extends GenValue<GenStruct> {
    private Map<String, GenValue> values;

    public GenStruct(Map<String, GenValue> values, GenSchema.StructGenSchema schema) {
      super(null, schema);
      value = this;
      this.values = Collections.unmodifiableMap(values);
    }

    public GenValue value(String fieldName) {
      return values.get(fieldName);
    }
  }
}
