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
package com.datastax.oss.kafka.sink;

import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.AbstractSinkRecordHeader;
import org.apache.kafka.connect.header.Header;

/** Wrapper of a Kafka Message Header */
public class HeaderAdapter implements AbstractSinkRecordHeader {

  private final Header header;

  public HeaderAdapter(Header header) {
    this.header = header;
  }

  @Override
  public String key() {
    return header.key();
  }

  @Override
  public Object value() {
    return header.value();
  }

  @Override
  public AbstractSchema schema() {
    return KafkaSchema.of(header.schema());
  }
}
