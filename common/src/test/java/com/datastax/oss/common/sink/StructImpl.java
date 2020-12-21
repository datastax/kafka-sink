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
package com.datastax.oss.common.sink;

import java.util.HashMap;
import java.util.Map;

/** Implementation of AbstractStruct. */
public class StructImpl implements AbstractStruct {

  private final Map<String, Object> fields;
  private final AbstractSchema schema;

  public StructImpl(AbstractSchema schema) {
    this.fields = new HashMap<>();
    this.schema = schema;
  }

  public StructImpl put(String name, Object value) {
    fields.put(name, value);
    return this;
  }

  @Override
  public Object get(String field) {
    return fields.get(field);
  }

  @Override
  public AbstractSchema schema() {
    return schema;
  }
}
