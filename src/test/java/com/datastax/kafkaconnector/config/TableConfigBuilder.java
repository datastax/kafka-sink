/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.config;

import static com.datastax.kafkaconnector.config.TableConfig.getTableSettingPath;

public class TableConfigBuilder extends TableConfig.Builder {
  private final String topic;
  private final String keyspace;
  private final String table;

  public TableConfigBuilder(String topic, String keyspace, String table) {
    super(topic, keyspace, table);
    this.topic = topic;
    this.keyspace = keyspace;
    this.table = table;
  }

  public TableConfigBuilder addSimpleSetting(String name, String value) {
    addSetting(getTableSettingPath(topic, keyspace, table, name), value);
    return this;
  }
}
