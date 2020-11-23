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
package com.datastax.oss.sink.record;

import java.util.Locale;

public class SchemaSupport {

  public enum Type {
    /**
     * 8-bit signed integer
     *
     * <p>Note that if you have an unsigned 8-bit data source, {@link SchemaSupport.Type#INT16} will
     * be required to safely capture all valid values
     */
    INT8,
    /**
     * 16-bit signed integer
     *
     * <p>Note that if you have an unsigned 16-bit data source, {@link SchemaSupport.Type#INT32}
     * will be required to safely capture all valid values
     */
    INT16,
    /**
     * 32-bit signed integer
     *
     * <p>Note that if you have an unsigned 32-bit data source, {@link SchemaSupport.Type#INT64}
     * will be required to safely capture all valid values
     */
    INT32,
    /**
     * 64-bit signed integer
     *
     * <p>Note that if you have an unsigned 64-bit data source, the {@link GenDecimal} logical type
     * (encoded as {@link SchemaSupport.Type#BYTES}) will be required to safely capture all valid
     * values
     */
    INT64,
    /** 32-bit IEEE 754 floating point number */
    FLOAT32,
    /** 64-bit IEEE 754 floating point number */
    FLOAT64,
    /** Boolean value (true or false) */
    BOOLEAN,
    /**
     * Character string that supports all Unicode characters.
     *
     * <p>Note that this does not imply any specific encoding (e.g. UTF-8) as this is an in-memory
     * representation.
     */
    STRING,
    /** Sequence of unsigned 8-bit bytes */
    BYTES,
    /** An ordered sequence of elements, each of which shares the same type. */
    ARRAY,
    /**
     * A mapping from keys to values. Both keys and values can be arbitrarily complex types,
     * including complex types such as {@link GenStruct}.
     */
    MAP,
    /**
     * A structured record containing a set of named fields, each field using a fixed, independent
     * {@link GenSchema}.
     */
    STRUCT;

    private String name;

    Type() {
      this.name = this.name().toLowerCase(Locale.ROOT);
    }

    public String getName() {
      return name;
    }

    public boolean isPrimitive() {
      switch (this) {
        case INT8:
        case INT16:
        case INT32:
        case INT64:
        case FLOAT32:
        case FLOAT64:
        case BOOLEAN:
        case STRING:
        case BYTES:
          return true;
      }
      return false;
    }
  }
}
