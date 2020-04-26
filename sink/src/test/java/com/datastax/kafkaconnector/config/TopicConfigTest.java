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

import static com.datastax.kafkaconnector.config.TopicConfig.DATE_PAT_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.LOCALE_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.TIMESTAMP_PAT_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.TIMEZONE_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.TIME_PAT_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.TIME_UNIT_OPT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.slf4j.event.Level.INFO;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.typesafe.config.Config;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith(LogInterceptingExtension.class)
class TopicConfigTest {
  @Test
  void should_produce_config_overrides() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(TopicConfig.getTopicSettingPath("mytopic", TIME_PAT_OPT), "time-pat")
            .put(TopicConfig.getTopicSettingPath("mytopic", LOCALE_OPT), "locale")
            .put(TopicConfig.getTopicSettingPath("mytopic", TIMEZONE_OPT), "timezone")
            .put(TopicConfig.getTopicSettingPath("mytopic", TIMESTAMP_PAT_OPT), "timestamp-pat")
            .put(TopicConfig.getTopicSettingPath("mytopic", DATE_PAT_OPT), "date-pat")
            .put(TopicConfig.getTopicSettingPath("mytopic", TIME_UNIT_OPT), "time-unit")
            .put(
                TableConfig.getTableSettingPath("mytopic", "ks", "table1", TableConfig.MAPPING_OPT),
                "c1=value.f1")
            .build();

    TopicConfig config = new TopicConfig("mytopic", props, false);
    Config configOverrides = config.getCodecConfigOverrides();
    assertThat(configOverrides.getString("locale")).isEqualTo("locale");
    assertThat(configOverrides.getString("timeZone")).isEqualTo("timezone");
    assertThat(configOverrides.getString("timestamp")).isEqualTo("timestamp-pat");
    assertThat(configOverrides.getString("date")).isEqualTo("date-pat");
    assertThat(configOverrides.getString("time")).isEqualTo("time-pat");
    assertThat(configOverrides.getString("unit")).isEqualTo("time-unit");
  }

  @Test
  void should_error_if_no_tables_provided() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(TopicConfig.getTopicSettingPath("mytopic", TIME_PAT_OPT), "time-pat")
            .put(TopicConfig.getTopicSettingPath("mytopic", LOCALE_OPT), "locale")
            .put(TopicConfig.getTopicSettingPath("mytopic", TIMEZONE_OPT), "timezone")
            .put(TopicConfig.getTopicSettingPath("mytopic", TIMESTAMP_PAT_OPT), "timestamp-pat")
            .put(TopicConfig.getTopicSettingPath("mytopic", DATE_PAT_OPT), "date-pat")
            .put(TopicConfig.getTopicSettingPath("mytopic", TIME_UNIT_OPT), "time-unit")
            .build();

    assertThatThrownBy(() -> new TopicConfig("mytopic", props, false))
        .isInstanceOf(ConfigException.class)
        .hasMessage("Topic mytopic must have at least one table configuration");
  }

  @Test
  void should_produce_table_configs() {
    Map<String, String> props = new LinkedHashMap<>();
    props.put(
        TableConfig.getTableSettingPath("mytopic", "ks", "table1", TableConfig.MAPPING_OPT),
        "c1=value.f1");
    props.put(
        TableConfig.getTableSettingPath("mytopic", "ks2", "table2", TableConfig.MAPPING_OPT),
        "c2=value.f2");
    TopicConfig config = new TopicConfig("mytopic", props, false);
    TableConfig[] tableConfigs = config.getTableConfigs().toArray(new TableConfig[0]);
    assertThat(tableConfigs.length).isEqualTo(2);
    assertThat(tableConfigs[0].getKeyspace().asInternal()).isEqualTo("ks");
    assertThat(tableConfigs[0].getTable().asInternal()).isEqualTo("table1");
    assertThat(tableConfigs[0].getMappingString()).isEqualTo("c1=value.f1");
    assertThat(tableConfigs[1].getKeyspace().asInternal()).isEqualTo("ks2");
    assertThat(tableConfigs[1].getTable().asInternal()).isEqualTo("table2");
    assertThat(tableConfigs[1].getMappingString()).isEqualTo("c2=value.f2");
  }

  @ParameterizedTest
  @CsvSource({"ANY", "LOCAL_ONE", "ONE"})
  void should_log_info_when_cloud_and_cl_is_not_proper_and_set_LOCAL_QUORUM(
      DefaultConsistencyLevel cl,
      @LogCapture(level = INFO, value = TableConfig.class) LogInterceptor logs) {
    // given
    Map<String, String> props = new LinkedHashMap<>();
    props.put(
        TableConfig.getTableSettingPath("mytopic", "ks", "table1", TableConfig.MAPPING_OPT),
        "c1=value.f1");
    props.put(
        TableConfig.getTableSettingPath("mytopic", "ks", "table1", TableConfig.CL_OPT), cl.name());

    // when
    TopicConfig config = new TopicConfig("mytopic", props, true);
    TableConfig[] tableConfigs = config.getTableConfigs().toArray(new TableConfig[0]);
    assertThat(tableConfigs.length).isEqualTo(1);

    // then
    assertThat(tableConfigs[0].getConsistencyLevel())
        .isEqualTo(DefaultConsistencyLevel.LOCAL_QUORUM);
    assertThat(logs.getLoggedMessages())
        .contains(
            String.format(
                "Cloud deployments reject consistency level %s when writing; forcing LOCAL_QUORUM",
                cl.name()));
  }

  @ParameterizedTest
  @CsvSource({"ANY", "LOCAL_ONE", "ONE"})
  void should_not_log_info_when_not_cloud_and_cl_is_not_proper_and_not_set_LOCAL_QUORUM(
      DefaultConsistencyLevel cl,
      @LogCapture(level = INFO, value = TableConfig.class) LogInterceptor logs) {
    // given
    Map<String, String> props = new LinkedHashMap<>();
    props.put(
        TableConfig.getTableSettingPath("mytopic", "ks", "table1", TableConfig.MAPPING_OPT),
        "c1=value.f1");
    props.put(
        TableConfig.getTableSettingPath("mytopic", "ks", "table1", TableConfig.CL_OPT), cl.name());

    // when
    TopicConfig config = new TopicConfig("mytopic", props, false);
    TableConfig[] tableConfigs = config.getTableConfigs().toArray(new TableConfig[0]);
    assertThat(tableConfigs.length).isEqualTo(1);

    // then
    assertThat(tableConfigs[0].getConsistencyLevel()).isEqualTo(cl);
    assertThat(logs.getLoggedMessages())
        .doesNotContain(
            String.format(
                "Cloud deployments reject consistency level %s when writing; forcing LOCAL_QUORUM",
                cl.name()));
  }

  @ParameterizedTest
  @CsvSource({"TWO", "THREE", "LOCAL_QUORUM", "QUORUM", "EACH_QUORUM", "ALL"})
  void should_not_log_warning_when_cloud_and_compatible_CL_explicitly_set(
      DefaultConsistencyLevel cl,
      @LogCapture(level = INFO, value = TableConfig.class) LogInterceptor logs) {
    // given
    Map<String, String> props = new LinkedHashMap<>();
    props.put(
        TableConfig.getTableSettingPath("mytopic", "ks", "table1", TableConfig.MAPPING_OPT),
        "c1=value.f1");
    props.put(
        TableConfig.getTableSettingPath("mytopic", "ks", "table1", TableConfig.CL_OPT), cl.name());

    // when
    TopicConfig config = new TopicConfig("mytopic", props, true);
    TableConfig[] tableConfigs = config.getTableConfigs().toArray(new TableConfig[0]);
    assertThat(tableConfigs.length).isEqualTo(1);

    // then
    assertThat(tableConfigs[0].getConsistencyLevel()).isEqualTo(cl);
    assertThat(logs.getLoggedMessages())
        .doesNotContain(
            String.format(
                "Cloud deployments reject consistency level %s when writing; forcing LOCAL_QUORUM",
                cl.name()));
  }
}
