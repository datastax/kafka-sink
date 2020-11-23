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
package com.datastax.oss.sink.pulsar.codecs;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.*;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import com.datastax.oss.sink.pulsar.GRecordBuilder;
import com.datastax.oss.sink.pulsar.codec.StructToUDTCodec;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
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
              .<GenericAvroRecord, UdtValue>createConvertingCodec(
                  udt1, GenericType.of(GenericAvroRecord.class), true);

  private final Schema schema =
      SchemaBuilder.record("test").fields().requiredInt("f1a").requiredDouble("f1b").endRecord();

  private final GenericAvroRecord struct =
      new GRecordBuilder(schema).put("f1a", 42).put("f1b", 0.12d).build();

  @Test
  void should_convert_from_valid_external() {
    System.out.println(udtCodec1);
    System.out.println(struct);
    assertThat(udtCodec1)
        .convertsFromExternal(struct)
        .toInternal(udt1Value)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    GenericAvroRecord other =
        new GRecordBuilder(SchemaBuilder.record("other").fields().optionalInt("a1").endRecord())
            .put("a1", 32)
            .build();
    GenericAvroRecord other2 =
        new GRecordBuilder(
                SchemaBuilder.record("other2")
                    .fields()
                    .optionalInt("a1")
                    .optionalInt("a2")
                    .endRecord())
            .put("a1", 32)
            .put("a2", 40)
            .build();
    assertThat(udtCodec1).cannotConvertFromExternal(other).cannotConvertFromExternal(other2);
  }
}
