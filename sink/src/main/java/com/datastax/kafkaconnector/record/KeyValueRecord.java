/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.record;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The fully parsed {@link SinkRecord} in a form where we can apply mappings of fields to DSE
 * columns.
 */
public class KeyValueRecord implements Record {
  @Nullable private final KeyOrValue key;
  @Nullable private final KeyOrValue value;
  @NotNull private final Set<String> fields;
  @Nullable private final Long timestamp;
  @Nullable private final Headers headers;

  public KeyValueRecord(
      @Nullable KeyOrValue key,
      @Nullable KeyOrValue value,
      @Nullable Long timestamp,
      @Nullable Headers headers) {
    this.key = key;
    this.value = value;
    this.headers = headers;
    fields = new HashSet<>();
    if (key != null) {
      fields.addAll(key.fields().stream().map(f -> "key." + f).collect(Collectors.toList()));
    }
    if (value != null) {
      fields.addAll(value.fields().stream().map(f -> "value." + f).collect(Collectors.toList()));
    }
    if (headers != null) {
      headers.forEach(h -> fields.add("header." + h.key()));
    }
    this.timestamp = timestamp;
  }

  @Override
  @NotNull
  public Set<String> fields() {
    return fields;
  }

  @Override
  @Nullable
  public Object getFieldValue(@NotNull String field) {
    if (field.startsWith("key.")) {
      return key != null ? key.getFieldValue(field.substring(4)) : null;
    } else if (field.startsWith("value.")) {
      return value != null ? value.getFieldValue(field.substring(6)) : null;
    } else if (field.startsWith("header.")) {
      return headers != null ? findHeaderValue(field.substring(7), headers) : null;
    } else {
      throw new IllegalArgumentException(
          "field name must start with 'key.', 'value.' or 'header.'.");
    }
  }

  @Nullable
  private Object findHeaderValue(@NotNull String field, @NotNull Headers headers) {
    for (Header h : headers) {
      if (h.key().equals(field)) {
        return h.value();
      }
    }
    return null;
  }

  @Override
  @Nullable
  public Long getTimestamp() {
    return timestamp;
  }
}
