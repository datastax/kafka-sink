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
package com.datastax.oss.sink.pulsar.record;

import com.datastax.oss.sink.pulsar.LocalRecord;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;

public class LocalGenericRecord
    extends LocalRecord<GenericRecord, org.apache.avro.generic.GenericRecord> {
  public LocalGenericRecord(
      Record<GenericRecord> raw, org.apache.avro.generic.GenericRecord record) {
    super(raw, record);
  }

  public LocalGenericRecord(
      Record<GenericRecord> coat, Object key, org.apache.avro.generic.GenericRecord record) {
    super(coat, key, record);
  }
}
