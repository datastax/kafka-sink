/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector;

import com.datastax.kafkaconnector.codecs.KafkaCodecRegistry;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.Multimap;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Collection;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Modeled after the DefaultMapping class in DSBulk. A few key diffs: 1. "variable" in dsbulk =&gt;
 * "column" here 2. the mapping in dsbulk is 1:1, whereas here we support many (columns) to 1 field.
 */
public class Mapping {

  private final Map<CqlIdentifier, CqlIdentifier> dseColumnsToKafkaFields;
  private final Multimap<CqlIdentifier, CqlIdentifier> kafkaFieldsToDseColumns;
  private final KafkaCodecRegistry codecRegistry;
  private final Cache<CqlIdentifier, TypeCodec<?>> dseColumnsToCodecs;

  public Mapping(
      Map<CqlIdentifier, CqlIdentifier> dseColumnsToKafkaFields, KafkaCodecRegistry codecRegistry) {
    this.dseColumnsToKafkaFields = dseColumnsToKafkaFields;
    this.codecRegistry = codecRegistry;
    dseColumnsToCodecs = Caffeine.newBuilder().build();
    ImmutableMultimap.Builder<CqlIdentifier, CqlIdentifier> builder = ImmutableMultimap.builder();
    dseColumnsToKafkaFields.forEach((c, f) -> builder.put(f, c));
    kafkaFieldsToDseColumns = builder.build();
  }

  @Nullable
  CqlIdentifier columnToField(@NotNull CqlIdentifier column) {
    return dseColumnsToKafkaFields.get(column);
  }

  @NotNull
  Collection<CqlIdentifier> getMappedColumns() {
    return dseColumnsToKafkaFields.keySet();
  }

  @Nullable
  Collection<CqlIdentifier> fieldToColumns(@NotNull CqlIdentifier field) {
    return kafkaFieldsToDseColumns.get(field);
  }

  @NotNull
  <T> TypeCodec<T> codec(
      @NotNull CqlIdentifier column,
      @NotNull DataType cqlType,
      @NotNull GenericType<? extends T> javaType) {
    @SuppressWarnings("unchecked")
    TypeCodec<T> codec =
        (TypeCodec<T>)
            dseColumnsToCodecs.get(column, n -> codecRegistry.codecFor(cqlType, javaType));
    assert codec != null;
    return codec;
  }
}
