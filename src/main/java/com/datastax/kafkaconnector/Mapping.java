/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.commons.codecs.writetime.WriteTimeCodec;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

/**
 * Modeled after the DefaultMapping class in DSBulk. A few key diffs: "variable" in dsbulk =>
 * "column" here the mapping in dsbulk is 1:1, whereas here we support many (columns) to 1 field.
 */
public class Mapping {

  private final Map<CqlIdentifier, CqlIdentifier> columnsToFields;
  private final Multimap<CqlIdentifier, CqlIdentifier> fieldsToColumns;
  private final ExtendedCodecRegistry codecRegistry;
  private final Cache<CqlIdentifier, TypeCodec<?>> columnsToCodecs;
  private final CqlIdentifier writeTimeVariable;

  public Mapping(
      Map<CqlIdentifier, CqlIdentifier> columnsToFields,
      ExtendedCodecRegistry codecRegistry,
      CqlIdentifier writeTimeVariable) {
    this.columnsToFields = columnsToFields;
    this.codecRegistry = codecRegistry;
    this.writeTimeVariable = writeTimeVariable;
    columnsToCodecs = Caffeine.newBuilder().build();
    ImmutableMultimap.Builder<CqlIdentifier, CqlIdentifier> builder = ImmutableMultimap.builder();
    columnsToFields.forEach((c, f) -> builder.put(f, c));
    fieldsToColumns = builder.build();
  }

  CqlIdentifier columnToField(@NotNull CqlIdentifier column) {
    return columnsToFields.get(column);
  }

  Collection<CqlIdentifier> fieldToColumns(@NotNull CqlIdentifier field) {
    return fieldsToColumns.get(field);
  }

  @NotNull
  <T> TypeCodec<T> codec(
      @NotNull CqlIdentifier column,
      @NotNull DataType cqlType,
      @NotNull GenericType<? extends T> javaType) {
    @SuppressWarnings("unchecked")
    TypeCodec<T> codec =
        (TypeCodec<T>)
            columnsToCodecs.get(
                column,
                n -> {
                  if (column.equals(writeTimeVariable)) {
                    if (!cqlType.equals(DataTypes.BIGINT)) {
                      throw new IllegalArgumentException(
                          "Cannot create a WriteTimeCodec for " + cqlType);
                    }
                    ConvertingCodec<T, Instant> innerCodec =
                        codecRegistry.convertingCodecFor(DataTypes.TIMESTAMP, javaType);
                    return new WriteTimeCodec<>(innerCodec);
                  }
                  return codecRegistry.codecFor(cqlType, javaType);
                });
    assert codec != null;
    return codec;
  }
}
