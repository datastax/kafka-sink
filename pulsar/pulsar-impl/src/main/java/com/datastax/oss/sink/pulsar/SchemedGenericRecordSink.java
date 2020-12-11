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
import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonReader;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Connector(
  name = "dssc-generic-sch",
  type = IOType.SINK,
  help = "PulsarSinkConnector is used for moving messages from Pulsar to Cassandra",
  configClass = PulsarSinkConfig.class
)
public class SchemedGenericRecordSink extends GenericRecordSink<SchemedGenericRecord> {

  private static final Logger log = LoggerFactory.getLogger(SchemedGenericRecordSink.class);

  @Override
  protected APIAdapter<GenericRecord, SchemedGenericRecord, ?, ?, ?, Header> createAPIAdapter() {
    return new SchemedGenericRecordAPIAdapter();
  }

  private Method getJsonNode;

  @Override
  protected SchemedGenericRecord readValue(Record<GenericRecord> record) throws Exception {
    if (record.getValue() == null) return null;
    GenericRecord rec;
    if (record.getValue().getClass().getName().equals(GenericJsonRecord.class.getName())) {
      // json object in GenericJsonRecord comes from pulsar as an escaped text node whereas
      // it should be an object node
      // so unescape string representation and reconstruct the record

      if (getJsonNode == null) getJsonNode = record.getValue().getClass().getMethod("getJsonNode");
      String escaped = getJsonNode.invoke(record.getValue()).toString();
      String unescaped = StringEscapeUtils.unescapeJava(escaped);
      if (unescaped.startsWith("\"")) unescaped = unescaped.substring(1, unescaped.length() - 1);

      rec =
          new GenericJsonReader(record.getValue().getFields())
              .read(unescaped.getBytes(StandardCharsets.UTF_8));
    } else {
      rec = record.getValue();
    }
    Schema schema =
        new Schema.Parser()
            .parse(new ByteArrayInputStream(record.getSchema().getSchemaInfo().getSchema()));
    return new SchemedGenericRecord(rec, schema);
  }

  @Override
  protected DataReader structuredStringReader() {
    return DataReader.AS_IS;
  }
}
