package com.datastax.kafkaconnector.codecs;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.dsbulk.commons.codecs.CustomCodecFactory;
import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.NotNull;

public class CodecFactory implements CustomCodecFactory {
  @Override
  public ConvertingCodec<?, ?> codecFor(@NotNull ExtendedCodecRegistry codecRegistry, @NotNull DataType cqlType, @NotNull GenericType<?> javaType) {
    // TODO: PERF: Consider caching codecs. It's highly likely that this will get expensive if it
    // has to create a new codec for every sink-record that has a Struct field.
    if (cqlType instanceof UserDefinedType && javaType.equals(GenericType.of(Struct.class))) {
      return new StructToUDTCodec(codecRegistry, (UserDefinedType) cqlType);
    }
    return null;
  }
}
