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

import java.util.List;

/** Schema implementation for tests. */
public class SchemaImpl implements AbstractSchema {

  public static final AbstractSchema STRING_SCHEMA = new SchemaImpl(Type.STRING, null);
  public static final AbstractSchema INT32_SCHEMA = new SchemaImpl(Type.INT32, null);
  public static final AbstractSchema INT64_SCHEMA = new SchemaImpl(Type.INT64, null);
  public static final AbstractSchema BOOLEAN_SCHEMA = new SchemaImpl(Type.BOOLEAN, null);
  public static final AbstractSchema BYTES_SCHEMA = new SchemaImpl(Type.BYTES, null);

  public SchemaImpl(Type type, List<FieldImpl> fields) {
    this.type = type;
    this.fields = fields;
  }

  public static class FieldImpl implements AbstractField {

    private String name;
    private AbstractSchema schema;

    public FieldImpl(String name, AbstractSchema schema) {
      this.name = name;
      this.schema = schema;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public AbstractSchema schema() {
      return schema;
    }
  }

  private final List<? extends AbstractField> fields;
  private final Type type;

  @Override
  public AbstractSchema valueSchema() {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public AbstractSchema keySchema() {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public List<? extends AbstractField> fields() {
    return fields;
  }

  @Override
  public AbstractField field(String name) {
    return fields.stream().filter(f -> f.name().equals(name)).findAny().orElse(null);
  }
}
