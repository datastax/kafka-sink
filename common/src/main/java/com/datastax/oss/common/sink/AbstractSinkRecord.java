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

/** A record. */
public interface AbstractSinkRecord {

  /**
   * List of headers. Each header has a schema.
   *
   * @return the list of headers
   */
  public Iterable<AbstractSinkRecordHeader> headers();

  /**
   * Key of the message, it may be null.
   *
   * @return the key
   */
  public Object key();

  /**
   * Value if the message, most of the times it is a structured data type.
   *
   * @return the value
   */
  public Object value();

  /**
   * Reference timestamp.
   *
   * @return
   */
  public Long timestamp();

  /**
   * Original topic that contained the message. It must match a topic configured in the Sink
   * configuration.
   *
   * @return the topic name.
   */
  public String topic();
}
