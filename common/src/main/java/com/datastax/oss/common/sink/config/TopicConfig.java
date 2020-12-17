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
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.datastax.oss.dsbulk.codecs.api.ConversionContext;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.api.util.CodecUtils;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
    super(settings);

    Map<String, TableConfig.Builder> tableConfigBuilders = new LinkedHashMap<>();

    // Walk through the settings and separate out the table settings. Other settings
    // are effectively handled by our AbstractConfig super-class.
    settings.forEach(
        (name, value) -> {
          Matcher codecSettingPattern = CassandraSinkConfig.TOPIC_CODEC_PATTERN.matcher(name);
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

  @NonNull
  public String getTopicName() {
    return topicName;
  }

  @NonNull
  public Collection<TableConfig> getTableConfigs() {
    return tableConfigs;
  }

  @Override
  @NonNull
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

  @NonNull
  public ConvertingCodecFactory createCodecFactory() {
    ConversionContext context =
        new TextConversionContext()
            .setLocale(
                CodecUtils.parseLocale(getString(getTopicSettingPath(topicName, LOCALE_OPT))))
            .setTimestampFormat(getString(getTopicSettingPath(topicName, TIMESTAMP_PAT_OPT)))
            .setDateFormat(getString(getTopicSettingPath(topicName, DATE_PAT_OPT)))
            .setTimeFormat(getString(getTopicSettingPath(topicName, TIME_PAT_OPT)))
            .setTimeZone(ZoneId.of(getString(getTopicSettingPath(topicName, TIMEZONE_OPT))))
            .setTimeUnit(
                TimeUnit.valueOf(getString(getTopicSettingPath(topicName, TIME_UNIT_OPT))));
    return new ConvertingCodecFactory(context);
  }
}
