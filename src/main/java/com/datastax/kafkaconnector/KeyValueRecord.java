/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class KeyValueRecord implements Record {
  @Nullable private final Record key;
  @Nullable private final Record value;
  private final Set<String> fields;

  KeyValueRecord(@Nullable Record key, @Nullable Record value) {
    this.key = key;
    this.value = value;
    fields = new HashSet<>();
    if (key != null) {
      fields.addAll(key.fields().stream().map(f -> "key." + f).collect(Collectors.toList()));
    }
    if (value != null) {
      fields.addAll(value.fields().stream().map(f -> "value." + f).collect(Collectors.toList()));
    }
  }

  @Override
  public Set<String> fields() {
    return fields;
  }

  @Override
  public Object getFieldValue(@NotNull String field) {
    if (field.startsWith("key.")) {
      return key != null ? key.getFieldValue(field.substring(4)) : null;
    } else if (field.startsWith("value.")) {
      return value != null ? value.getFieldValue(field.substring(6)) : null;
    } else {
      assert false : "field name must start with 'key.' or 'value.'.";
    }
    return null;
  }
}
