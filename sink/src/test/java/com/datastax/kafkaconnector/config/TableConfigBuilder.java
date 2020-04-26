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
package com.datastax.kafkaconnector.config;

import static com.datastax.kafkaconnector.config.TableConfig.getTableSettingPath;

public class TableConfigBuilder extends TableConfig.Builder {
  private final String topic;
  private final String keyspace;
  private final String table;

  public TableConfigBuilder(String topic, String keyspace, String table, boolean cloud) {
    super(topic, keyspace, table, cloud);
    this.topic = topic;
    this.keyspace = keyspace;
    this.table = table;
  }

  public TableConfigBuilder addSimpleSetting(String name, String value) {
    addSetting(getTableSettingPath(topic, keyspace, table, name), value);
    return this;
  }
}
