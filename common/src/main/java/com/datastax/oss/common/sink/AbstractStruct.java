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

/** A data structure. */
public interface AbstractStruct {

  /**
   * Access a field
   *
   * @param field
   * @return the value of the field, or null
   */
  public Object get(String field);

  /**
   * The data type of the field
   *
   * @return the schema
   */
  public AbstractSchema schema();
}
