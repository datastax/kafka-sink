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
package com.datastax.oss.kafka.sink;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.kafka.sink.util.FunctionMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Modeled after the DefaultMapping class in DSBulk. A few key diffs: 1. "variable" in dsbulk =&gt;
 * "column" here 2. the mapping in dsbulk is 1:1, whereas here we support many (columns) to 1 field.
 */
public class Mapping {

  private final Map<CqlIdentifier, CqlIdentifier> columnsToKafkaFields;
  private final Multimap<CqlIdentifier, CqlIdentifier> kafkaFieldsToDseColumns;
  private final ConvertingCodecFactory codecFactory;
  private final Cache<CqlIdentifier, TypeCodec<?>> columnsToCodecs;
  private final List<CqlIdentifier> functions;

  public Mapping(
      Map<CqlIdentifier, CqlIdentifier> columnsToKafkaFields, ConvertingCodecFactory codecFactory) {
    this.columnsToKafkaFields = columnsToKafkaFields;
    this.codecFactory = codecFactory;
    columnsToCodecs = Caffeine.newBuilder().build();
    ImmutableMultimap.Builder<CqlIdentifier, CqlIdentifier> builder = ImmutableMultimap.builder();
    columnsToKafkaFields.forEach((c, f) -> builder.put(f, c));
    kafkaFieldsToDseColumns = builder.build();
    functions =
        columnsToKafkaFields
            .values()
            .stream()
            .filter(FunctionMapper.SUPPORTED_FUNCTIONS_IN_MAPPING::contains)
            .collect(ImmutableList.toImmutableList());
  }

  @Nullable
  CqlIdentifier columnToField(@NonNull CqlIdentifier column) {
    return columnsToKafkaFields.get(column);
  }

  @NonNull
  Collection<CqlIdentifier> getMappedColumns() {
    return columnsToKafkaFields.keySet();
  }

  @Nullable
  Collection<CqlIdentifier> fieldToColumns(@NonNull CqlIdentifier field) {
    return kafkaFieldsToDseColumns.get(field);
  }

  @NonNull
  <T> TypeCodec<T> codec(
      @NonNull CqlIdentifier column,
      @NonNull DataType cqlType,
      @NonNull GenericType<? extends T> javaType) {
    @SuppressWarnings("unchecked")
    TypeCodec<T> codec =
        (TypeCodec<T>)
            columnsToCodecs.get(
                column, n -> codecFactory.createConvertingCodec(cqlType, javaType, true));
    assert codec != null;
    return codec;
  }

  @NonNull
  public List<CqlIdentifier> functions() {
    return functions;
  }
}
