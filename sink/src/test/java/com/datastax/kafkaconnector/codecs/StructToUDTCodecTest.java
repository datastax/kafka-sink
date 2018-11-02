/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.codecs;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.typesafe.config.ConfigFactory;
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
      (StructToUDTCodec) newCodecRegistry().codecFor(udt1, GenericType.of(Struct.class));

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

  private KafkaCodecRegistry newCodecRegistry() {
    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("kafka.codec"));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    return settings.createCodecRegistry(new DefaultCodecRegistry("test"));
  }
}
