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
import com.datastax.oss.common.sink.AbstractStruct;
import org.apache.kafka.connect.data.Struct;

/** Wrapper for Kafka Structs. */
public class KafkaStruct implements AbstractStruct {

  private final Struct struct;

  public static Object wrap(Object o) {
    if (o instanceof Struct) {
      return new KafkaStruct((Struct) o);
    }
    return o;
  }

  public KafkaStruct(Struct struct) {
    this.struct = struct;
  }

  @Override
  public Object get(String field) {
    return wrap(struct.get(field));
  }

  @Override
  public AbstractSchema schema() {
    return KafkaSchema.of(struct.schema());
  }
}
