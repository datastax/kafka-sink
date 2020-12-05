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
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Connector(
  name = "dssc-generic-ref",
  type = IOType.SINK,
  help = "PulsarSinkConnector is used for moving messages from Pulsar to Cassandra",
  configClass = PulsarSinkConfig.class
)
public class ReflectionGenericRecordSink extends GenericRecordSink<Object> {

  private GenericRecordAssimilator assimilator = new GenericRecordAssimilator();

  @Override
  protected Object readValue(Record<GenericRecord> record) throws Exception {
    if (record.getValue() == null) return null;
    return assimilator.assimilateFromEnvelope(record.getValue());
  }

  public static class GenericRecordAssimilator {
    private Constructor datumWriterConstructor;
    private Constructor fileWriterContstructor;
    private Method fileCreate;
    private Method fileAppend;
    private Method fileClose;
    private Method getSchema;

    private void init(Object alien) throws Exception {
      ClassLoader cl = alien.getClass().getClassLoader();
      getSchema = alien.getClass().getMethod("getSchema");
      Class<?> schemaClass = cl.loadClass(Schema.class.getName());
      datumWriterConstructor =
          cl.loadClass(GenericDatumWriter.class.getName()).getConstructor(schemaClass);
      Class<?> dataFileWriterClass = cl.loadClass(DataFileWriter.class.getName());
      fileWriterContstructor =
          dataFileWriterClass.getConstructor(cl.loadClass(DatumWriter.class.getName()));
      fileCreate = dataFileWriterClass.getMethod("create", schemaClass, OutputStream.class);
      fileAppend = dataFileWriterClass.getMethod("append", Object.class);
      fileClose = dataFileWriterClass.getMethod("close");
    }

    public org.apache.avro.generic.GenericRecord assimilate(Object alien) throws Exception {
      if (fileClose == null) init(alien);

      Object schema = getSchema.invoke(alien);
      Object writer = datumWriterConstructor.newInstance(schema);
      Object fileWriter = fileWriterContstructor.newInstance(writer);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      fileCreate.invoke(fileWriter, schema, baos);
      fileAppend.invoke(fileWriter, alien);
      fileClose.invoke(fileWriter);
      return (org.apache.avro.generic.GenericRecord) DataReader.WORN_AVRO.read(baos.toByteArray());
    }

    private Method getAvroRecord;
    private Method getJsonNode;

    public org.apache.avro.generic.GenericRecord assimilateFromEnvelope(GenericRecord envelope)
        throws Exception {
      if (envelope.getClass().getName().equals(GenericAvroRecord.class.getName())) {
        if (getAvroRecord == null) getAvroRecord = envelope.getClass().getMethod("getAvroRecord");
        return assimilate(getAvroRecord.invoke(envelope));
      } else {
        if (getJsonNode == null) getJsonNode = envelope.getClass().getMethod("getJsonNode");
        return (org.apache.avro.generic.GenericRecord)
            DataReader.WORN_JSON.read(getJsonNode.invoke(envelope).toString());
      }
    }
  }

  @Override
  protected DataReader structuredStringReader() {
    return DataReader.AS_IS;
  }

  @Override
  protected APIAdapter<GenericRecord, Object, ?, ?, ?, Header> createAPIAdapter() {
    return new AvroAPIAdapter<>();
  }
}
