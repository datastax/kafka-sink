/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.config;

import static com.datastax.kafkaconnector.util.StringUtil.singleQuote;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.common.base.Splitter;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class TopicConfig extends AbstractConfig {
  public static final String KEYSPACE_OPT = "keyspace";
  public static final String TABLE_OPT = "table";
  public static final String MAPPING_OPT = "mapping";
  public static final String TTL_OPT = "ttl";

  private static final Pattern DELIM_PAT = Pattern.compile(", *");

  private final String topicName;
  private final CqlIdentifier keyspace;
  private final CqlIdentifier table;
  private final String mappingString;
  private final Map<CqlIdentifier, CqlIdentifier> mapping;
  private final int ttl;

  TopicConfig(String topicName, Map<String, String> settings) {
    super(makeTopicConfigDef(topicName), settings, false);

    this.topicName = topicName;
    keyspace = parseLoosely(getString(getTopicSettingName(topicName, KEYSPACE_OPT)));
    table = parseLoosely(getString(getTopicSettingName(topicName, TABLE_OPT)));
    mappingString = getString(getTopicSettingName(topicName, MAPPING_OPT));
    mapping = parseMappingString(mappingString);
    ttl = getInt(getTopicSettingName(topicName, TTL_OPT));
  }

  private Map<CqlIdentifier, CqlIdentifier> parseMappingString(String mappingString) {
    MappingInspector inspector =
        new MappingInspector(mappingString, getTopicSettingName(topicName, MAPPING_OPT));
    List<String> errors = inspector.getErrors();
    if (!errors.isEmpty()) {
      throw new ConfigException(
          getTopicSettingName(topicName, MAPPING_OPT),
          singleQuote(mappingString),
          String.format(
              "Encountered the following errors:%n%s",
              errors.stream().collect(Collectors.joining(String.format("%n  ")))));
    }

    return inspector.getMapping();
  }

  private static CqlIdentifier parseLoosely(String value) {
    // If the value is unquoted, treat it as a literal (no real parsing).
    // Otherwise parse it as cql. The idea is that users should be able to specify
    // case-sensitive identifiers in the mapping spec without quotes.

    return value.startsWith("\"")
        ? CqlIdentifier.fromCql(value)
        : CqlIdentifier.fromInternal(value);
  }

  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  public CqlIdentifier getTable() {
    return table;
  }

  public Map<CqlIdentifier, CqlIdentifier> getMapping() {
    return mapping;
  }

  public String getMappingString() {
    return mappingString;
  }

  public int getTtl() {
    return ttl;
  }

  @Override
  public String toString() {
    return String.format(
        "{name: %s, keyspace: %s, table: %s, ttl: %d, mapping:\n%s}",
        topicName,
        keyspace,
        table,
        ttl,
        Splitter.on(DELIM_PAT)
            .splitToList(mappingString)
            .stream()
            .map(m -> "      " + m)
            .collect(Collectors.joining("\n")));
  }

  public static String getTopicSettingName(String topicName, String setting) {
    return String.format("topic.%s.%s", topicName, setting);
  }

  private static ConfigDef makeTopicConfigDef(String topicName) {
    return new ConfigDef()
        .define(
            getTopicSettingName(topicName, KEYSPACE_OPT),
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "Keyspace to which to load messages")
        .define(
            getTopicSettingName(topicName, TABLE_OPT),
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "Table to which to load messages")
        .define(
            getTopicSettingName(topicName, MAPPING_OPT),
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            "Mapping of record fields to dse columns, in the form of 'col1=value.f1, col2=key.f1'")
        .define(
            getTopicSettingName(topicName, TTL_OPT),
            ConfigDef.Type.INT,
            -1,
            ConfigDef.Range.atLeast(-1),
            ConfigDef.Importance.HIGH,
            "TTL of rows inserted in DSE nodes");
  }
}
