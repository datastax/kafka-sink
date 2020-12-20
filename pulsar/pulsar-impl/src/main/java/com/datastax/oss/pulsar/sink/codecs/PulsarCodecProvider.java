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

import com.datastax.oss.common.sink.AbstractStruct;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecProvider;
import com.datastax.oss.sink.pulsar.PulsarStruct;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;

/** Converting codec registry that handles processing Pulsar objects. */
public class PulsarCodecProvider implements ConvertingCodecProvider {

  @NonNull
  @Override
  public Optional<ConvertingCodec<?, ?>> maybeProvide(
      @NonNull DataType cqlType,
      @NonNull GenericType<?> externalJavaType,
      @NonNull ConvertingCodecFactory codecFactory,
      boolean rootCodec) {
    if (cqlType instanceof UserDefinedType
        && (externalJavaType.equals(GenericType.of(PulsarStruct.class))
            || externalJavaType.equals(GenericType.of(AbstractStruct.class)))) {
      return Optional.of(new StructToUDTCodec(codecFactory, (UserDefinedType) cqlType));
    }
    return Optional.empty();
  }
}
