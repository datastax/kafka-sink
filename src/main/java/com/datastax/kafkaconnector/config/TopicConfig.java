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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.google.common.base.Splitter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.jetbrains.annotations.NotNull;

/** Topic-specific connector configuration. */
public class TopicConfig extends AbstractConfig {
  public static final String KEYSPACE_OPT = "keyspace";
  public static final String TABLE_OPT = "table";
  public static final String MAPPING_OPT = "mapping";
  public static final String TTL_OPT = "ttl";
  static final String CL_OPT = "consistencyLevel";

  static final String TIME_PAT_OPT = "codec.time";
  static final String LOCALE_OPT = "codec.locale";
  static final String TIMEZONE_OPT = "codec.timeZone";
  static final String TIMESTAMP_PAT_OPT = "codec.timestamp";
  static final String DATE_PAT_OPT = "codec.date";
  static final String TIME_UNIT_OPT = "codec.unit";

  private static final String DELETES_ENABLED_OPT = "deletesEnabled";
  private static final String NULL_TO_UNSET_OPT = "nullToUnset";
  private static final Pattern DELIM_PAT = Pattern.compile(", *");

  private final String topicName;
  private final CqlIdentifier keyspace;
  private final CqlIdentifier table;
  private final String mappingString;
  private final Map<CqlIdentifier, CqlIdentifier> mapping;
  private final ConsistencyLevel consistencyLevel;
  private final int ttl;
  private final boolean nullToUnset;
  private final boolean deletesEnabled;

  public static String getTopicSettingName(String topicName, String setting) {
    return String.format("topic.%s.%s", topicName, setting);
  }

  TopicConfig(String topicName, Map<String, String> settings) {
    super(makeTopicConfigDef(topicName), settings, false);

    this.topicName = topicName;
    keyspace = parseLoosely(getString(getTopicSettingName(topicName, KEYSPACE_OPT)));
    table = parseLoosely(getString(getTopicSettingName(topicName, TABLE_OPT)));
    mappingString = getString(getTopicSettingName(topicName, MAPPING_OPT));
    mapping = parseMappingString(mappingString);
    String clOptName = getTopicSettingName(topicName, CL_OPT);
    String clString = getString(clOptName);
    try {
      consistencyLevel = DefaultConsistencyLevel.valueOf(clString.toUpperCase());
    } catch (IllegalArgumentException e) {
      // Must be a non-existing enum value.
      throw new ConfigException(
          clOptName,
          singleQuote(clString),
          String.format(
              "valid values include: %s",
              Arrays.stream(DefaultConsistencyLevel.values())
                  .map(DefaultConsistencyLevel::name)
                  .collect(Collectors.joining(", "))));
    }
    ttl = getInt(getTopicSettingName(topicName, TTL_OPT));
    nullToUnset = getBoolean(getTopicSettingName(topicName, NULL_TO_UNSET_OPT));
    deletesEnabled = getBoolean(getTopicSettingName(topicName, DELETES_ENABLED_OPT));
  }

  @NotNull
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NotNull
  public CqlIdentifier getTable() {
    return table;
  }

  @NotNull
  public String getKeyspaceAndTable() {
    return String.format("%s.%s", keyspace.asCql(true), table.asCql(true));
  }

  @NotNull
  public Map<CqlIdentifier, CqlIdentifier> getMapping() {
    return mapping;
  }

  @NotNull
  public String getMappingString() {
    return mappingString;
  }

  @NotNull
  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  @NotNull
  public String getTopicName() {
    return topicName;
  }

  public int getTtl() {
    return ttl;
  }

  public boolean isNullToUnset() {
    return nullToUnset;
  }

  public boolean isDeletesEnabled() {
    return deletesEnabled;
  }

  @Override
  @NotNull
  public String toString() {
    String[] codecSettings = {
      LOCALE_OPT, TIMEZONE_OPT, TIMESTAMP_PAT_OPT, DATE_PAT_OPT, TIME_PAT_OPT, TIME_UNIT_OPT
    };
    String codecString =
        Arrays.stream(codecSettings)
            .map(
                s ->
                    String.format(
                        "%s: %s",
                        s.substring("codec.".length()),
                        getString(getTopicSettingName(topicName, s))))
            .collect(Collectors.joining(", "));

    return String.format(
        "{name: %s, keyspace: %s, table: %s, cl: %s, ttl: %d, nullToUnset: %b, "
            + "deletesEnabled: %b, mapping:\n%s\n"
            + " codec settings: %s}",
        topicName,
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
            .collect(Collectors.joining("\n")),
        codecString);
  }

  @NotNull
  public Config getCodecConfigOverrides() {
    String[] settingNames = {
      LOCALE_OPT, TIMEZONE_OPT, TIMESTAMP_PAT_OPT, DATE_PAT_OPT, TIME_PAT_OPT, TIME_UNIT_OPT
    };
    String config =
        Arrays.stream(settingNames)
            .map(
                s ->
                    String.format(
                        "%s=\"%s\"",
                        s.substring("codec.".length()),
                        getString(getTopicSettingName(topicName, s))))
            .collect(Collectors.joining("\n"));
    return ConfigFactory.parseString(config);
  }

  @NotNull
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
            getTopicSettingName(topicName, DELETES_ENABLED_OPT),
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.HIGH,
            "Whether to delete rows where only the primary key is non-null")
        .define(
            getTopicSettingName(topicName, CL_OPT),
            ConfigDef.Type.STRING,
            "LOCAL_ONE",
            ConfigDef.Importance.HIGH,
            "Query consistency level")
        .define(
            getTopicSettingName(topicName, TTL_OPT),
            ConfigDef.Type.INT,
            -1,
            ConfigDef.Range.atLeast(-1),
            ConfigDef.Importance.HIGH,
            "TTL of rows inserted in DSE nodes")
        .define(
            getTopicSettingName(topicName, NULL_TO_UNSET_OPT),
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.HIGH,
            "Whether nulls in Kafka should be treated as UNSET in DSE")
        .define(
            getTopicSettingName(topicName, LOCALE_OPT),
            ConfigDef.Type.STRING,
            "en_US",
            ConfigDef.Importance.HIGH,
            "The locale to use for locale-sensitive conversions.")
        .define(
            getTopicSettingName(topicName, TIMEZONE_OPT),
            ConfigDef.Type.STRING,
            "UTC",
            ConfigDef.Importance.HIGH,
            "The time zone to use for temporal conversions that do not convey any explicit time zone information")
        .define(
            getTopicSettingName(topicName, TIMESTAMP_PAT_OPT),
            ConfigDef.Type.STRING,
            "CQL_TIMESTAMP",
            ConfigDef.Importance.HIGH,
            "The temporal pattern to use for `String` to CQL `timestamp` conversion")
        .define(
            getTopicSettingName(topicName, DATE_PAT_OPT),
            ConfigDef.Type.STRING,
            "ISO_LOCAL_DATE",
            ConfigDef.Importance.HIGH,
            "The temporal pattern to use for `String` to CQL `date` conversion")
        .define(
            getTopicSettingName(topicName, TIME_PAT_OPT),
            ConfigDef.Type.STRING,
            "ISO_LOCAL_TIME",
            ConfigDef.Importance.HIGH,
            "The temporal pattern to use for `String` to CQL `time` conversion")
        .define(
            getTopicSettingName(topicName, TIME_UNIT_OPT),
            ConfigDef.Type.STRING,
            "MILLISECONDS",
            ConfigDef.Importance.HIGH,
            "If the input is a string containing only digits that cannot be parsed using the `codec.timestamp` format, the specified time unit is applied to the parsed value. All `TimeUnit` enum constants are valid choices.");
  }

  @NotNull
  private static CqlIdentifier parseLoosely(String value) {
    // If the value is unquoted, treat it as a literal (no real parsing).
    // Otherwise parse it as cql. The idea is that users should be able to specify
    // case-sensitive identifiers in the mapping spec without quotes.

    return value.startsWith("\"")
        ? CqlIdentifier.fromCql(value)
        : CqlIdentifier.fromInternal(value);
  }

  @NotNull
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
}
