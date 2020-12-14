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
package com.datastax.oss.sink.pulsar;

import com.datastax.oss.sink.pulsar.util.DataReader;
import com.datastax.oss.sink.pulsar.util.GenericJsonRecordReconstructor;
import java.io.ByteArrayInputStream;
import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Connector(
  name = "dssc-generic-sch",
  type = IOType.SINK,
  help = "PulsarSinkConnector is used for moving messages from Pulsar to Cassandra",
  configClass = PulsarSinkConfig.class
)
public class SchemedGenericRecordSink extends GenericRecordSink<SchemedGenericRecord> {

  @Override
  protected APIAdapter<GenericRecord, SchemedGenericRecord, ?, ?, ?, Header> createAPIAdapter() {
    return new SchemedGenericRecordAPIAdapter();
  }

  @Override
  protected SchemedGenericRecord readValue(Record<GenericRecord> record) throws Exception {
    if (record.getValue() == null) return null;
    Schema schema =
        new Schema.Parser()
            .parse(new ByteArrayInputStream(record.getSchema().getSchemaInfo().getSchema()));
    return new SchemedGenericRecord(
        GenericJsonRecordReconstructor.reconstruct(record.getValue()), schema);
  }

  @Override
  protected DataReader structuredStringReader() {
    return DataReader.STRING;
  }
}
