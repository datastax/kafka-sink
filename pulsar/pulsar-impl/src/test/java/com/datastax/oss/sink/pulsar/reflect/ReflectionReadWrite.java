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
package com.datastax.oss.sink.pulsar.reflect;

import static org.junit.jupiter.api.Assertions.*;

import com.datastax.oss.sink.pulsar.ReflectionGenericRecordSink;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

public class ReflectionReadWrite {

  @Test
  void readWrite() throws Exception {
    GenericRecord record =
        new GenericData.Record(
            SchemaBuilder.record("test")
                .fields()
                .requiredLong("long")
                .requiredString("string")
                .requiredBoolean("boolean")
                .endRecord());
    record.put("long", 1234L);
    record.put("string", "hello world");
    record.put("boolean", true);

    ReflectionGenericRecordSink.GenericRecordAssimilator assimilator =
        new ReflectionGenericRecordSink.GenericRecordAssimilator();

    GenericRecord assimilated1 = assimilator.assimilate(record);
    GenericRecord assimilated2 = assimilator.assimilate(record);
    GenericRecord assimilated3 = assimilator.assimilate(record);

    assertNotSame(assimilated1, record);
    assertNotSame(assimilated2, record);
    assertNotSame(assimilated3, record);
    assertNotSame(assimilated1, assimilated2);

    assertEquals(assimilated1, record);
    assertEquals(assimilated2, record);
    assertEquals(assimilated3, record);
  }

  private byte[] wornBytes(GenericContainer record) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DatumWriter<GenericContainer> dwrt = new GenericDatumWriter<>(record.getSchema());
    DataFileWriter<GenericContainer> wrt = new DataFileWriter<>(dwrt);
    try {
      wrt.create(record.getSchema(), baos);
      wrt.append(record);
      wrt.close();
    } catch (Exception ex) {
      throw ex instanceof RuntimeException ? (RuntimeException) ex : new RuntimeException(ex);
    }
    return baos.toByteArray();
  }

  private byte[] wornBytes2(GenericRecord record) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<>();
    Encoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
    writer.write(record, encoder);
    encoder.flush();
    return baos.toByteArray();
  }
}
