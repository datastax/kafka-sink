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
package com.datastax.oss.sink.record;

import com.datastax.oss.sink.EngineAPIAdapter;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/** The fully parsed record in a form where we can apply mappings of fields to columns. */
public class KeyValueRecord<EngineHeader> implements Record {
  @Nullable private final KeyOrValue key;
  @Nullable private final KeyOrValue value;
  @NonNull private final Set<String> fields;
  @Nullable private final Long timestamp;
  @Nullable private final Set<EngineHeader> headers;
  private final EngineAPIAdapter<?, ?, ?, ?, EngineHeader> adapter;

  public KeyValueRecord(
      @Nullable KeyOrValue key,
      @Nullable KeyOrValue value,
      @Nullable Long timestamp,
      @Nullable Set<EngineHeader> headers,
      EngineAPIAdapter<?, ?, ?, ?, EngineHeader> adapter) {
    this.adapter = adapter;
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
      headers.forEach(h -> fields.add("header." + adapter.headerKey(h)));
    }
    this.timestamp = timestamp;
  }

  @Override
  @NonNull
  public Set<String> fields() {
    return fields;
  }

  @Override
  @Nullable
  public Object getFieldValue(@NonNull String field) {
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
  private Object findHeaderValue(@NonNull String field, @NonNull Set<EngineHeader> headers) {
    for (EngineHeader h : headers) {
      if (adapter.headerKey(h).equals(field)) {
        return adapter.headerValue(h);
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
