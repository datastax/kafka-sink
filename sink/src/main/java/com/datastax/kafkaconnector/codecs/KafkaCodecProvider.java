/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.codecs;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecProvider;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;

/** Converting codec registry that handles processing Kafka {@link Struct} objects. */
public class KafkaCodecProvider implements ConvertingCodecProvider {

  @NonNull
  @Override
  public Optional<ConvertingCodec<?, ?>> maybeProvide(
      @NonNull DataType cqlType,
      @NonNull GenericType<?> externalJavaType,
      @NonNull ConvertingCodecFactory codecFactory,
      boolean rootCodec) {
    if (cqlType instanceof UserDefinedType
        && externalJavaType.equals(GenericType.of(Struct.class))) {
      return Optional.of(new StructToUDTCodec(codecFactory, (UserDefinedType) cqlType));
    }
    return Optional.empty();
  }
}
