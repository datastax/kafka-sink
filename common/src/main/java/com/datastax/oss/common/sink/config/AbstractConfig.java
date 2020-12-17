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
package com.datastax.oss.common.sink.config;

import com.datastax.oss.common.sink.ConfigException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Abstract Configuration. */
public class AbstractConfig {

  private Map<String, Object> values;

  protected AbstractConfig(Map<?, ?> values) {
    this.values = new HashMap<>();
    for (Map.Entry<?, ?> entry : values.entrySet()) {
      if (!(entry.getKey() instanceof String)) {
        throw new ConfigException(
            entry.getKey().toString(), entry.getValue(), "Key must be a string.");
      }
      this.values.put((String) entry.getKey(), entry.getValue());
    }
  }

  public Object get(String key) {
    return values.get(key);
  }

  public Boolean getBoolean(String key) {
    return (Boolean) get(key);
  }

  public String getString(String key) {
    return (String) get(key);
  }

  public Map<String, Object> values() {
    return values;
  }

  public List<String> getList(String key) {
    return (List<String>) get(key);
  }

  public String getPassword(String key) {
    return getString(key);
  }

  public Integer getInt(String key) {
    return (Integer) get(key);
  }
}
