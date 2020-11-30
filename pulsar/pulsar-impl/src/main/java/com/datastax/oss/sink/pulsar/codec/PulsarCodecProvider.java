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
package com.datastax.oss.sink.pulsar.codec;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecProvider;
import com.datastax.oss.dsbulk.codecs.jdk.number.NumberToNumberCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;

/** Converting codec registry that handles processing Avro {@link GenericRecord} objects. */
public class PulsarCodecProvider implements ConvertingCodecProvider {

  @NonNull
  @Override
  public Optional<ConvertingCodec<?, ?>> maybeProvide(
      @NonNull DataType cqlType,
      @NonNull GenericType<?> externalJavaType,
      @NonNull ConvertingCodecFactory codecFactory,
      boolean rootCodec) {
    if (cqlType instanceof UserDefinedType
        && externalJavaType.equals(GenericType.of(GenericRecord.class))) {
      return Optional.of(new StructToUDTCodec(codecFactory, (UserDefinedType) cqlType));
    } else if (cqlType instanceof MapType
        && externalJavaType.equals(GenericType.of(GenericRecord.class))) {
      return Optional.of(new StructToMapCodec(codecFactory, (MapType) cqlType));
      //    } else if (cqlType.getProtocolCode() == ProtocolConstants.DataType.TIME
      //        && externalJavaType.equals(GenericType.of(Integer.class))) {
      //      return Optional.of(
      //          new NumberToNumberCodec<>(
      //              Integer.class, codecFactory.getCodecRegistry().codecFor(DataTypes.BIGINT)));
    } else if (cqlType.getProtocolCode() == ProtocolConstants.DataType.COUNTER
        && externalJavaType.equals(GenericType.of(Integer.class))) {
      return Optional.of(
          new NumberToNumberCodec<>(
              Integer.class, codecFactory.getCodecRegistry().codecFor(DataTypes.COUNTER)));
    }
    return Optional.empty();
  }

  private boolean isCounterOrTime(DataType type) {
    return type.getProtocolCode() == ProtocolConstants.DataType.COUNTER
        || type.getProtocolCode() == ProtocolConstants.DataType.TIME;
  }
}
