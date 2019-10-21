/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector;

import com.datastax.kafkaconnector.record.RecordMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;

public class TestRecordMetadata implements RecordMetadata {
  private final ImmutableMap<Object, GenericType<?>> fieldsToTypes;

  TestRecordMetadata(ImmutableMap<Object, GenericType<?>> fieldsToTypes) {
    this.fieldsToTypes = fieldsToTypes;
  }

  @Override
  public GenericType<?> getFieldType(@NotNull String field, @NotNull DataType cqlType) {
    return fieldsToTypes.get(field);
  }
}
