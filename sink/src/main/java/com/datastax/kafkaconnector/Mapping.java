/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector;

import static com.datastax.kafkaconnector.util.FunctionMapper.SUPPORTED_FUNCTIONS_IN_MAPPING;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
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

  private final Map<CqlIdentifier, CqlIdentifier> dseColumnsToKafkaFields;
  private final Multimap<CqlIdentifier, CqlIdentifier> kafkaFieldsToDseColumns;
  private final ConvertingCodecFactory codecFactory;
  private final Cache<CqlIdentifier, TypeCodec<?>> dseColumnsToCodecs;
  private final List<CqlIdentifier> functions;

  public Mapping(
      Map<CqlIdentifier, CqlIdentifier> dseColumnsToKafkaFields,
      ConvertingCodecFactory codecFactory) {
    this.dseColumnsToKafkaFields = dseColumnsToKafkaFields;
    this.codecFactory = codecFactory;
    dseColumnsToCodecs = Caffeine.newBuilder().build();
    ImmutableMultimap.Builder<CqlIdentifier, CqlIdentifier> builder = ImmutableMultimap.builder();
    dseColumnsToKafkaFields.forEach((c, f) -> builder.put(f, c));
    kafkaFieldsToDseColumns = builder.build();
    functions =
        dseColumnsToKafkaFields
            .values()
            .stream()
            .filter(SUPPORTED_FUNCTIONS_IN_MAPPING::contains)
            .collect(ImmutableList.toImmutableList());
  }

  @Nullable
  CqlIdentifier columnToField(@NonNull CqlIdentifier column) {
    return dseColumnsToKafkaFields.get(column);
  }

  @NonNull
  Collection<CqlIdentifier> getMappedColumns() {
    return dseColumnsToKafkaFields.keySet();
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
            dseColumnsToCodecs.get(
                column, n -> codecFactory.createConvertingCodec(cqlType, javaType, true));
    assert codec != null;
    return codec;
  }

  @NonNull
  public List<CqlIdentifier> functions() {
    return functions;
  }
}
