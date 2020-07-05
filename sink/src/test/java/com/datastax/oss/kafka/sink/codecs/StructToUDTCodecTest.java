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
package com.datastax.oss.kafka.sink.codecs;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
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
              .<Struct, UdtValue>createConvertingCodec(udt1, GenericType.of(Struct.class), true);

  private final Schema schema =
      SchemaBuilder.struct()
          .field("f1a", Schema.INT32_SCHEMA)
          .field("f1b", Schema.FLOAT64_SCHEMA)
          .build();

  private final Struct struct = new Struct(schema).put("f1a", 42).put("f1b", 0.12d);

  @Test
  void should_convert_from_valid_external() {
    assertThat(udtCodec1)
        .convertsFromExternal(struct)
        .toInternal(udt1Value)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    Struct other =
        new Struct(SchemaBuilder.struct().field("a1", Schema.INT32_SCHEMA).build()).put("a1", 32);
    Struct other2 =
        new Struct(
                SchemaBuilder.struct()
                    .field("a1", Schema.INT32_SCHEMA)
                    .field("a2", Schema.INT32_SCHEMA)
                    .build())
            .put("a1", 32)
            .put("a2", 40);
    assertThat(udtCodec1).cannotConvertFromExternal(other).cannotConvertFromExternal(other2);
  }
}
