/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.config;

import com.google.common.base.Splitter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.jetbrains.annotations.NotNull;

/** Topic-specific connector configuration. */
public class TopicConfig extends AbstractConfig {
  static final String TIME_PAT_OPT = "codec.time";
  static final String LOCALE_OPT = "codec.locale";
  static final String TIMEZONE_OPT = "codec.timeZone";
  static final String TIMESTAMP_PAT_OPT = "codec.timestamp";
  static final String DATE_PAT_OPT = "codec.date";
  static final String TIME_UNIT_OPT = "codec.unit";

  // Table settings are of the form "topic.mytopic.ks1.table1.setting"
  private static final Pattern TABLE_KS_PATTERN =
      Pattern.compile("^topic\\.[a-zA-Z0-9._-]+\\.([^.]+)\\.([^.]+)\\.");

  private final String topicName;
  private final Collection<TableConfig> tableConfigs;

  static String getTopicSettingPath(String topicName, String setting) {
    return String.format("topic.%s.%s", topicName, setting);
  }

  public TopicConfig(String topicName, Map<String, String> settings, boolean cloud) {
    super(makeTopicConfigDef(topicName), settings, false);

    Map<String, TableConfig.Builder> tableConfigBuilders = new LinkedHashMap<>();

    // Walk through the settings and separate out the table settings. Other settings
    // are effectively handled by our AbstractConfig super-class.
    settings.forEach(
        (name, value) -> {
          Matcher codecSettingPattern = DseSinkConfig.TOPIC_CODEC_PATTERN.matcher(name);
          Matcher tableKsSettingMatcher = TABLE_KS_PATTERN.matcher(name);

          // using codecSettingPattern to prevent including of
          // global topic level settings (under .codec prefix)
          if (!codecSettingPattern.matches() && tableKsSettingMatcher.lookingAt()) {
            TableConfig.Builder builder =
                tableConfigBuilders.computeIfAbsent(
                    tableKsSettingMatcher.group(),
                    t ->
                        new TableConfig.Builder(
                            topicName,
                            tableKsSettingMatcher.group(1),
                            tableKsSettingMatcher.group(2),
                            cloud));
            builder.addSetting(name, value);
          }
        });

    if (tableConfigBuilders.isEmpty()) {
      throw new ConfigException(
          String.format("Topic %s must have at least one table configuration", topicName));
    }

    tableConfigs =
        tableConfigBuilders
            .values()
            .stream()
            .map(TableConfig.Builder::build)
            .collect(Collectors.toList());
    this.topicName = topicName;
  }

  @NotNull
  public String getTopicName() {
    return topicName;
  }

  @NotNull
  public Collection<TableConfig> getTableConfigs() {
    return tableConfigs;
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
                        getString(getTopicSettingPath(topicName, s))))
            .collect(Collectors.joining(", "));

    return String.format(
        "name: %s, codec settings: %s%nTable configurations:%n%s",
        topicName,
        codecString,
        tableConfigs
            .stream()
            .map(
                t ->
                    Splitter.on("\n")
                        .splitToList(t.toString())
                        .stream()
                        .map(line -> "        " + line)
                        .collect(Collectors.joining("\n")))
            .collect(Collectors.joining("\n")));
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
                        getString(getTopicSettingPath(topicName, s))))
            .collect(Collectors.joining("\n"));
    return ConfigFactory.parseString(config);
  }

  /**
   * Build up a {@link ConfigDef} for the given topic specification.
   *
   * @param topicName name of topic
   * @return a ConfigDef of topic settings, where each setting name is the full setting path (e.g.
   *     topic.[topicname]).
   */
  @NotNull
  private static ConfigDef makeTopicConfigDef(String topicName) {
    return new ConfigDef()
        .define(
            getTopicSettingPath(topicName, LOCALE_OPT),
            ConfigDef.Type.STRING,
            "en_US",
            ConfigDef.Importance.HIGH,
            "The locale to use for locale-sensitive conversions.")
        .define(
            getTopicSettingPath(topicName, TIMEZONE_OPT),
            ConfigDef.Type.STRING,
            "UTC",
            ConfigDef.Importance.HIGH,
            "The time zone to use for temporal conversions that do not convey any explicit time zone information")
        .define(
            getTopicSettingPath(topicName, TIMESTAMP_PAT_OPT),
            ConfigDef.Type.STRING,
            "CQL_TIMESTAMP",
            ConfigDef.Importance.HIGH,
            "The temporal pattern to use for `String` to CQL `timestamp` conversion")
        .define(
            getTopicSettingPath(topicName, DATE_PAT_OPT),
            ConfigDef.Type.STRING,
            "ISO_LOCAL_DATE",
            ConfigDef.Importance.HIGH,
            "The temporal pattern to use for `String` to CQL `date` conversion")
        .define(
            getTopicSettingPath(topicName, TIME_PAT_OPT),
            ConfigDef.Type.STRING,
            "ISO_LOCAL_TIME",
            ConfigDef.Importance.HIGH,
            "The temporal pattern to use for `String` to CQL `time` conversion")
        .define(
            getTopicSettingPath(topicName, TIME_UNIT_OPT),
            ConfigDef.Type.STRING,
            "MILLISECONDS",
            ConfigDef.Importance.HIGH,
            "If the input is a string containing only digits that cannot be parsed using the `codec.timestamp` format, the specified time unit is applied to the parsed value. All `TimeUnit` enum constants are valid choices.");
  }
}
