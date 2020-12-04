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
import com.datastax.oss.sink.pulsar.util.Utf8ToStringGenericDatumReader;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
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

  @Override
  protected Object readValue(Record<GenericRecord> record) throws Exception {
    if (record.getValue() == null) return null;

    if (record.getValue().getClass().getName().equals(GenericAvroRecord.class.getName())) {
      Method m = record.getValue().getClass().getMethod("getAvroRecord");
      Object avroRecord = m.invoke(record.getValue());
      m = avroRecord.getClass().getMethod("getSchema");
      Object avroSchema = m.invoke(avroRecord);
      String schemaStr = avroSchema.toString();

      String recStr = recToString(avroRecord, avroSchema);

      Schema schema = new Schema.Parser().parse(schemaStr);
      DatumReader<org.apache.avro.generic.GenericRecord> reader =
          new Utf8ToStringGenericDatumReader<>(schema);
      return reader.read(null, DecoderFactory.get().jsonDecoder(schema, recStr));
    } else {
      Object jsonNode =
          record.getValue().getClass().getMethod("getJsonNode").invoke(record.getValue());
      return DataReader.WORN_JSON.read(jsonNode.toString());
    }
  }

  private ObjectMapper mapper = new ObjectMapper();

  /**
   * Produces JSON string with field types from avro <code>GenericRecord</code>. Executes the
   * following code via reflection <code><pre>
   * GenericDatumWriter<org.apache.avro.generic.GenericRecord> writer = new GenericDatumWriter<>((Schema) avroSchema);
   * ByteArrayOutputStream baos = new ByteArrayOutputStream();
   * JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder((Schema) avroSchema, baos);
   * writer.write((org.apache.avro.generic.GenericRecord) avroRecord, jsonEncoder);
   * jsonEncoder.flush();
   * recStr = new String(baos.toByteArray(), StandardCharsets.UTF_8);
   *
   * </pre></code>
   *
   * @param avroRecord
   * @param avroSchema
   * @return
   * @throws Exception
   */
  private String recToString(Object avroRecord, Object avroSchema) throws Exception {
    ClassLoader cl = avroRecord.getClass().getClassLoader();
    Class<?> schemaClass = cl.loadClass(Schema.class.getName());
    Object writer =
        cl.loadClass(GenericDatumWriter.class.getName())
            .getConstructor(schemaClass)
            .newInstance(avroSchema);
    Class<?> encoderFactoryClass = cl.loadClass(EncoderFactory.class.getName());
    Object encoderFactory =
        encoderFactoryClass.getDeclaredMethod("get").invoke(encoderFactoryClass);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Class<?> outputStreamClass = cl.loadClass(OutputStream.class.getName());
    Class<?> encoderClass = cl.loadClass(Encoder.class.getName());
    Object encoder =
        encoderFactory
            .getClass()
            .getMethod("jsonEncoder", schemaClass, outputStreamClass)
            .invoke(encoderFactory, avroSchema, baos);
    writer
        .getClass()
        .getMethod("write", Object.class, encoderClass)
        .invoke(writer, avroRecord, encoder);
    encoder.getClass().getMethod("flush").invoke(encoder);

    return new String(baos.toByteArray(), StandardCharsets.UTF_8);
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
