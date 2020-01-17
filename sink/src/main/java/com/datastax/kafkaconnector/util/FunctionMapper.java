/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.util;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;

public class FunctionMapper {
  private static final CqlIdentifier NOW_FUNCTION = CqlIdentifier.fromInternal("now()");
  public static final Set<CqlIdentifier> SUPPORTED_FUNCTIONS_IN_MAPPING =
      ImmutableSet.of(NOW_FUNCTION);

  private static final Map<CqlIdentifier, GenericType<?>> FUNCTION_TYPES =
      ImmutableMap.of(NOW_FUNCTION, GenericType.UUID);

  private static final Map<CqlIdentifier, Supplier<?>> FUNCTION_VALUE_PROVIDER =
      ImmutableMap.of(NOW_FUNCTION, Uuids::timeBased);

  @Nullable
  public static GenericType<?> typeForFunction(String function) {
    return FUNCTION_TYPES.get(CqlIdentifier.fromInternal(function));
  }

  public static Object valueForFunction(String function) {
    return FUNCTION_VALUE_PROVIDER.get(CqlIdentifier.fromInternal(function)).get();
  }
}
