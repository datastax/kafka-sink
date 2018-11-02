/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.codecs;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.commons.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.commons.codecs.util.TemporalFormat;
import com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.util.concurrent.FastThreadLocal;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.NotNull;

/** Converting codec registry that handles processing Kafka {@link Struct} objects. */
public class KafkaCodecRegistry extends ExtendedCodecRegistry {
  KafkaCodecRegistry(
      CodecRegistry codecRegistry,
      List<String> nullStrings,
      Map<String, Boolean> booleanInputWords,
      Map<Boolean, String> booleanOutputWords,
      List<BigDecimal> booleanNumbers,
      FastThreadLocal<NumberFormat> numberFormat,
      OverflowStrategy overflowStrategy,
      RoundingMode roundingMode,
      TemporalFormat localDateFormat,
      TemporalFormat localTimeFormat,
      TemporalFormat timestampFormat,
      ZoneId timeZone,
      TimeUnit timeUnit,
      ZonedDateTime epoch,
      TimeUUIDGenerator generator,
      ObjectMapper objectMapper) {
    super(
        codecRegistry,
        nullStrings,
        booleanInputWords,
        booleanOutputWords,
        booleanNumbers,
        numberFormat,
        overflowStrategy,
        roundingMode,
        localDateFormat,
        localTimeFormat,
        timestampFormat,
        timeZone,
        timeUnit,
        epoch,
        generator,
        objectMapper);
  }

  @Override
  protected ConvertingCodec<?, ?> maybeCreateConvertingCodec(
      @NotNull DataType cqlType, @NotNull GenericType<?> javaType) {
    if (cqlType instanceof UserDefinedType && javaType.equals(GenericType.of(Struct.class))) {
      return new StructToUDTCodec(this, (UserDefinedType) cqlType);
    }
    return super.maybeCreateConvertingCodec(cqlType, javaType);
  }
}
