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
package com.datastax.oss.kafka.sink.config;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.datastax.oss.kafka.sink.util.SinkUtil;
import com.datastax.oss.kafka.sink.util.StringUtil;
import com.datastax.oss.kafka.sink.util.TimeUnitConverter;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Table-specific connector configuration. */
public class TableConfig extends AbstractConfig {
  private static final Logger log = LoggerFactory.getLogger(TableConfig.class);
  public static final String MAPPING_OPT = "mapping";
  public static final String TTL_OPT = "ttl";
  public static final String TTL_TIME_UNIT_OPT = "ttlTimeUnit";
  static final String TIMESTAMP_TIME_UNIT_OPT = "timestampTimeUnit";
  static final String CL_OPT = "consistencyLevel";
  static final String QUERY_OPT = "query";

  static final String DELETES_ENABLED_OPT = "deletesEnabled";
  private static final String NULL_TO_UNSET_OPT = "nullToUnset";
  private static final Pattern DELIM_PAT = Pattern.compile(", *");

  private final String topicName;
  private final CqlIdentifier keyspace;
  private final CqlIdentifier table;
  private final String mappingString;
  private final Map<CqlIdentifier, CqlIdentifier> mapping;
  private final ConsistencyLevel consistencyLevel;
  private final int ttl;
  private final TimeUnit ttlTimeUnit;
  private final TimeUnit timestampTimeUnit;
  private final boolean nullToUnset;
  private final boolean deletesEnabled;
  private final String query;

  private TableConfig(
      @NonNull String topicName,
      @NonNull String keyspace,
      @NonNull String table,
      @NonNull Map<String, String> settings,
      boolean cloud) {
    super(makeTableConfigDef(topicName, keyspace, table), settings, false);

    this.topicName = topicName;
    this.keyspace = parseLoosely(keyspace);
    this.table = parseLoosely(table);
    mappingString = getString(getTableSettingPath(topicName, keyspace, table, MAPPING_OPT));
    mapping = parseMappingString(mappingString);
    String clOptName = getTableSettingPath(topicName, keyspace, table, CL_OPT);
    String clString = getString(clOptName);
    try {
      consistencyLevel =
          convertToCloudCLIfNeeded(cloud, DefaultConsistencyLevel.valueOf(clString.toUpperCase()));
    } catch (IllegalArgumentException e) {
      // Must be a non-existing enum value.
      throw new ConfigException(
          clOptName,
          StringUtil.singleQuote(clString),
          String.format(
              "valid values include: %s",
              Arrays.stream(DefaultConsistencyLevel.values())
                  .map(DefaultConsistencyLevel::name)
                  .collect(Collectors.joining(", "))));
    }
    ttl = getInt(getTableSettingPath(topicName, keyspace, table, TTL_OPT));
    ttlTimeUnit =
        TimeUnit.valueOf(
            getString(getTableSettingPath(topicName, keyspace, table, TTL_TIME_UNIT_OPT)));
    timestampTimeUnit =
        TimeUnit.valueOf(
            getString(getTableSettingPath(topicName, keyspace, table, TIMESTAMP_TIME_UNIT_OPT)));

    nullToUnset = getBoolean(getTableSettingPath(topicName, keyspace, table, NULL_TO_UNSET_OPT));
    deletesEnabled =
        getBoolean(getTableSettingPath(topicName, keyspace, table, DELETES_ENABLED_OPT));
    query = getString(getTableSettingPath(topicName, keyspace, table, QUERY_OPT));
    validateQuery();
  }

  private void validateQuery() {
    if (isQueryProvided() && deletesEnabled) {
      throw new ConfigException(
          String.format(
              "You cannot provide both %s and %s. If you want to provide own %s, set the %s to false.",
              getTableSettingPath(topicName, keyspace.asInternal(), table.asInternal(), QUERY_OPT),
              getTableSettingPath(
                  topicName, keyspace.asInternal(), table.asInternal(), DELETES_ENABLED_OPT),
              QUERY_OPT,
              DELETES_ENABLED_OPT));
    }
  }

  private ConsistencyLevel convertToCloudCLIfNeeded(boolean cloud, ConsistencyLevel cl) {
    if (cloud && !isCloudCompatible(cl)) {
      log.info(
          "Cloud deployments reject consistency level {} when writing; forcing LOCAL_QUORUM", cl);
      return DefaultConsistencyLevel.LOCAL_QUORUM;
    }
    return cl;
  }

  private boolean isCloudCompatible(ConsistencyLevel cl) {
    int protocolCode = cl.getProtocolCode();
    return protocolCode != ProtocolConstants.ConsistencyLevel.ANY
        && protocolCode != ProtocolConstants.ConsistencyLevel.ONE
        && protocolCode != ProtocolConstants.ConsistencyLevel.LOCAL_ONE;
  }

  /**
   * Given the attributes of a setting, compute its full name/path.
   *
   * @param topicName name of topic
   * @param keyspace name of keyspace
   * @param table name of table
   * @param setting base name of setting
   * @return full path of the setting in the form "topic.[topicname].[keyspace].[table].[setting]".
   */
  @NonNull
  public static String getTableSettingPath(
      @NonNull String topicName,
      @NonNull String keyspace,
      @NonNull String table,
      @NonNull String setting) {
    return String.format("topic.%s.%s.%s.%s", topicName, keyspace, table, setting);
  }

  @NonNull
  public String getSettingPath(@NonNull String settingName) {
    return getTableSettingPath(topicName, keyspace.asInternal(), table.asInternal(), settingName);
  }

  @NonNull
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NonNull
  public CqlIdentifier getTable() {
    return table;
  }

  @NonNull
  public String getTopicName() {
    return topicName;
  }

  @NonNull
  public String getKeyspaceAndTable() {
    return String.format("%s.%s", keyspace.asCql(true), table.asCql(true));
  }

  @NonNull
  public Map<CqlIdentifier, CqlIdentifier> getMapping() {
    return mapping;
  }

  @NonNull
  public String getMappingString() {
    return mappingString;
  }

  @NonNull
  public Optional<String> getQuery() {
    return Optional.ofNullable(query);
  }

  public boolean isQueryProvided() {
    return query != null;
  }

  @NonNull
  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  public int getTtl() {
    return ttl;
  }

  public boolean hasTtlMappingColumn() {
    return mapping.get(CqlIdentifier.fromInternal(SinkUtil.TTL_VARNAME)) != null;
  }

  public boolean isNullToUnset() {
    return nullToUnset;
  }

  public boolean isDeletesEnabled() {
    return deletesEnabled;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TableConfig other = (TableConfig) o;

    return topicName.equals(other.topicName)
        && keyspace.equals(other.keyspace)
        && table.equals(other.table);
  }

  public TimeUnit getTtlTimeUnit() {
    return ttlTimeUnit;
  }

  public TimeUnit getTimestampTimeUnit() {
    return timestampTimeUnit;
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicName, keyspace, table);
  }

  @Override
  @NonNull
  public String toString() {
    return String.format(
        "{keyspace: %s, table: %s, cl: %s, ttl: %d, nullToUnset: %b, "
            + "deletesEnabled: %b, mapping:\n%s\n"
            + "}",
        keyspace,
        table,
        consistencyLevel,
        ttl,
        nullToUnset,
        deletesEnabled,
        Splitter.on(DELIM_PAT)
            .splitToList(mappingString)
            .stream()
            .map(m -> "      " + m)
            .collect(Collectors.joining("\n")));
  }

  /**
   * Build up a {@link ConfigDef} for the given table specification.
   *
   * @param topicName name of topic
   * @param keyspace name of keyspace
   * @param table name of table
   * @return a ConfigDef of table-settings, where each setting name is the full setting path (e.g.
   *     topic.[topicname].[keyspace].[table].[setting]).
   */
  @NonNull
  private static ConfigDef makeTableConfigDef(
      @NonNull String topicName, @NonNull String keyspace, @NonNull String table) {
    return new ConfigDef()
        .define(
            getTableSettingPath(topicName, keyspace, table, MAPPING_OPT),
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "Mapping of record fields to dse columns, in the form of 'col1=value.f1, col2=key.f1'")
        .define(
            getTableSettingPath(topicName, keyspace, table, DELETES_ENABLED_OPT),
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.HIGH,
            "Whether to delete rows where only the primary key is non-null")
        .define(
            getTableSettingPath(topicName, keyspace, table, CL_OPT),
            ConfigDef.Type.STRING,
            "LOCAL_ONE",
            ConfigDef.Importance.HIGH,
            "Query consistency level")
        .define(
            getTableSettingPath(topicName, keyspace, table, TTL_OPT),
            ConfigDef.Type.INT,
            -1,
            ConfigDef.Range.atLeast(-1),
            ConfigDef.Importance.HIGH,
            "TTL of rows inserted in the database")
        .define(
            getTableSettingPath(topicName, keyspace, table, NULL_TO_UNSET_OPT),
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.HIGH,
            "Whether nulls in Kafka should be treated as UNSET in the database")
        .define(
            getTableSettingPath(topicName, keyspace, table, TTL_TIME_UNIT_OPT),
            ConfigDef.Type.STRING,
            "SECONDS",
            ConfigDef.Importance.HIGH,
            "TimeUnit of provided ttl mapping field.")
        .define(
            getTableSettingPath(topicName, keyspace, table, TIMESTAMP_TIME_UNIT_OPT),
            ConfigDef.Type.STRING,
            "MICROSECONDS",
            ConfigDef.Importance.HIGH,
            "TimeUnit of provided timestamp mapping field.")
        .define(
            getTableSettingPath(topicName, keyspace, table, QUERY_OPT),
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            "Custom query to use as a Prepared Statement for insert to this table.");
  }

  @NonNull
  private static CqlIdentifier parseLoosely(@NonNull String value) {
    // If the value is unquoted, treat it as a literal (no real parsing).
    // Otherwise parse it as cql. The idea is that users should be able to specify
    // case-sensitive identifiers in the mapping spec without quotes.

    return value.startsWith("\"")
        ? CqlIdentifier.fromCql(value)
        : CqlIdentifier.fromInternal(value);
  }

  @NonNull
  private Map<CqlIdentifier, CqlIdentifier> parseMappingString(String mappingString) {
    MappingInspector inspector =
        new MappingInspector(
            mappingString,
            getTableSettingPath(topicName, keyspace.asInternal(), table.asInternal(), MAPPING_OPT));
    List<String> errors = inspector.getErrors();
    if (!errors.isEmpty()) {
      throw new ConfigException(
          getTableSettingPath(topicName, keyspace.asInternal(), table.asInternal(), MAPPING_OPT),
          StringUtil.singleQuote(mappingString),
          String.format(
              "Encountered the following errors:%n%s",
              errors.stream().collect(Collectors.joining(String.format("%n  ")))));
    }

    return inspector.getMapping();
  }

  public long convertTtlToSeconds(Number ttl) {
    return TimeUnitConverter.convertToSeconds(ttlTimeUnit, ttl);
  }

  public static class Builder {
    private final String topic;
    private final String keyspace;
    private final String table;
    private final Map<String, String> settings;
    private final boolean cloud;

    Builder(String topic, String keyspace, String table, boolean cloud) {
      this.topic = topic;
      this.keyspace = keyspace;
      this.table = table;
      this.cloud = cloud;
      settings = new HashMap<>();
    }

    void addSetting(String key, String value) {
      settings.put(key, value);
    }

    public TableConfig build() {
      return new TableConfig(topic, keyspace, table, settings, cloud);
    }
  }
}
