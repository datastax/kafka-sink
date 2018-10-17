/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.record;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapData implements KeyOrValue, RecordMetadata {
  private static final Logger log = LoggerFactory.getLogger(MapData.class);
  private final Map<String, Object> data;
  private final Set<String> fields;

  private MapData(Map<String, Object> data, Set<String> fields) {
    this.data = data;
    this.fields = fields;
  }

  @SuppressWarnings("unchecked")
  public static KeyOrValue fromMap(Map data) {
    return new MapData(ImmutableMap.copyOf(data), ImmutableSet.copyOf(data.keySet()));
  }

  @Override
  public Set<String> fields() {
    return fields;
  }

  @Override
  public Object getFieldValue(String field) {
    return data.get(field);
  }

  @Override
  public GenericType<?> getFieldType(@NotNull String field, @NotNull DataType cqlType) {
    log.info("getFieldType for: {} and cqlType: {}", field, cqlType);
    return GenericType.STRING;
  }

  @Override
  public String toString() {
    return "MapData{" + "data=" + data + ", fields=" + fields + '}';
  }
}
