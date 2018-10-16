package com.datastax.kafkaconnector.record;

import java.util.Map;
import java.util.Set;

public class MapData implements KeyOrValue{
  private final Map<String, Object> data;
  private final Set<String> fields;

  public MapData(Map<String, Object> data, Set<String> fields) {
    this.data = data;
    this.fields = fields;
  }

  public static KeyOrValue fromMap(Map data) {
    return
  }

  @Override
  public Set<String> fields() {
    return null;
  }

  @Override
  public Object getFieldValue(String field) {
    return null;
  }
}
