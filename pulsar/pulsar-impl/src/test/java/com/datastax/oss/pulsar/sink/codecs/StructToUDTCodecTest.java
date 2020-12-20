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
package com.datastax.oss.pulsar.sink.codecs;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import com.datastax.oss.sink.pulsar.GenericRecordImpl;
import com.datastax.oss.sink.pulsar.LocalSchemaRegistry;
import com.datastax.oss.sink.pulsar.PulsarRecordImpl;
import com.datastax.oss.sink.pulsar.PulsarStruct;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;

class StructToUDTCodecTest {
  private final UserDefinedType udt1 =
      new UserDefinedTypeBuilder("ks", "udt")
          .withField("f1a", DataTypes.INT)
          .withField("f1b", DataTypes.DOUBLE)
          .build();

  private final UdtValue udt1Value = udt1.newValue().setInt("f1a", 42).setDouble("f1b", 0.12d);

  private final StructToUDTCodec udtCodec1 =
      (StructToUDTCodec)
          new ConvertingCodecFactory(new TextConversionContext())
              .<PulsarStruct, UdtValue>createConvertingCodec(
                  udt1, GenericType.of(PulsarStruct.class), true);

  private Schema schema;
  private Record<GenericRecord> record;
  private GenericRecordImpl struct;

  {
    RecordSchemaBuilder builder = SchemaBuilder.record("MyBean");
    builder.field("f1a").type(SchemaType.INT32);
    builder.field("f1b").type(SchemaType.DOUBLE);
    schema = Schema.generic(builder.build(SchemaType.AVRO));

    struct = new GenericRecordImpl().put("f1a", 42).put("f1b", 0.12d);
    record = new PulsarRecordImpl(null, null, struct, schema);
  }

  private final LocalSchemaRegistry registry = new LocalSchemaRegistry();

  @Test
  void should_convert_from_valid_external() {
    assertThat(udtCodec1)
        .convertsFromExternal(PulsarStruct.ofRecord(record, registry))
        .toInternal(udt1Value)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    RecordSchemaBuilder builder = SchemaBuilder.record("MyBean");
    builder.field("a1").type(SchemaType.INT32);
    Schema schemaOther = Schema.generic(builder.build(SchemaType.AVRO));
    Record<GenericRecord> other =
        new PulsarRecordImpl(null, null, new GenericRecordImpl().put("a1", 32), schema);

    RecordSchemaBuilder builder2 = SchemaBuilder.record("MyBean");
    builder2.field("a1").type(SchemaType.INT32);
    builder2.field("a2").type(SchemaType.INT32);
    Schema schemaOther2 = Schema.generic(builder2.build(SchemaType.AVRO));

    Record<GenericRecord> other2 =
        new PulsarRecordImpl(
            null, null, new GenericRecordImpl().put("a1", 32).put("a2", 40), schemaOther2);

    assertThat(udtCodec1)
        .cannotConvertFromExternal(PulsarStruct.ofRecord(other, registry))
        .cannotConvertFromExternal(PulsarStruct.ofRecord(other2, registry));
  }
}
