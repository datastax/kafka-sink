/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.config;

import static com.datastax.kafkaconnector.config.DseSinkConfig.COMPRESSION_OPT;
import static com.datastax.kafkaconnector.config.DseSinkConfig.CONCURRENT_REQUESTS_OPT;
import static com.datastax.kafkaconnector.config.DseSinkConfig.CONNECTION_POOL_LOCAL_SIZE;
import static com.datastax.kafkaconnector.config.DseSinkConfig.CONNECTION_POOL_LOCAL_SIZE_DEFAULT;
import static com.datastax.kafkaconnector.config.DseSinkConfig.CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING;
import static com.datastax.kafkaconnector.config.DseSinkConfig.CONTACT_POINTS_OPT;
import static com.datastax.kafkaconnector.config.DseSinkConfig.DC_OPT;
import static com.datastax.kafkaconnector.config.DseSinkConfig.LOCAL_DC_DRIVER_SETTING;
import static com.datastax.kafkaconnector.config.DseSinkConfig.METRICS_HIGHEST_LATENCY_OPT;
import static com.datastax.kafkaconnector.config.DseSinkConfig.PORT_OPT;
import static com.datastax.kafkaconnector.config.DseSinkConfig.QUERY_EXECUTION_TIMEOUT_OPT;
import static com.datastax.kafkaconnector.config.DseSinkConfig.SECURE_CONNECT_BUNDLE_OPT;
import static com.datastax.kafkaconnector.config.DseSinkConfig.SSL_OPT_PREFIX;
import static com.datastax.kafkaconnector.config.SslConfig.PROVIDER_OPT;
import static com.datastax.kafkaconnector.config.TableConfig.MAPPING_OPT;
import static com.datastax.kafkaconnector.config.TableConfig.getTableSettingPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.slf4j.event.Level.WARN;

import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.kafkaconnector.util.SinkUtil;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(LogInterceptingExtension.class)
class DseSinkConfigTest {
  @Test
  void should_error_invalid_port() {
    Map<String, String> props =
        Maps.newHashMap(ImmutableMap.<String, String>builder().put(PORT_OPT, "foo").build());
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Invalid value foo for configuration port");

    props.put(PORT_OPT, "0");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Value must be at least 1");

    props.put(PORT_OPT, "-1");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Value must be at least 1");
  }

  @Test
  void should_error_invalid_queryExecutionTimeout() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder().put(QUERY_EXECUTION_TIMEOUT_OPT, "foo").build());
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Invalid value foo for configuration queryExecutionTimeout");

    props.put(QUERY_EXECUTION_TIMEOUT_OPT, "0");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Value must be at least 1");

    props.put(QUERY_EXECUTION_TIMEOUT_OPT, "-1");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Value must be at least 1");
  }

  @Test
  void should_error_invalid_metricsHighestLatency() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder().put(METRICS_HIGHEST_LATENCY_OPT, "foo").build());
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Invalid value foo for configuration metricsHighestLatency");

    props.put(METRICS_HIGHEST_LATENCY_OPT, "0");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Value must be at least 1");

    props.put(METRICS_HIGHEST_LATENCY_OPT, "-1");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Value must be at least 1");
  }

  // todo should we handle validation in the same way for settings with datastax-java-driver prefix?
  @Test
  void should_error_invalid_connectionPoolLocalSize() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder().put(CONNECTION_POOL_LOCAL_SIZE, "foo").build());
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Invalid value foo for configuration connectionPoolLocalSize");

    props.put(CONNECTION_POOL_LOCAL_SIZE, "0");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Value must be at least 1");

    props.put(CONNECTION_POOL_LOCAL_SIZE, "-1");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Value must be at least 1");
  }

  @Test
  void should_favor_deprecated_setting_over_java_driver_connectionPoolLocalSize(
      @LogCapture(level = WARN, value = DseSinkConfig.class) LogInterceptor logs) {
    // given
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(CONNECTION_POOL_LOCAL_SIZE, "10")
                .put(CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING, "100")
                .build());

    // when
    DseSinkConfig dseSinkConfig = new DseSinkConfig(props);

    // then
    assertThat(dseSinkConfig.getJavaDriverSettings().get(CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING))
        .isEqualTo("10");
    assertThat(logs.getLoggedMessages())
        .contains(
            String.format(
                "The %s setting is deprecated. You should use %s setting instead.",
                CONNECTION_POOL_LOCAL_SIZE, CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING));
  }

  @Test
  void should_use_deprecated_setting_as_a_new_java_driver_setting_connectionPoolLocalSize(
      @LogCapture(level = WARN, value = DseSinkConfig.class) LogInterceptor logs) {
    // given
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder().put(CONNECTION_POOL_LOCAL_SIZE, "10").build());

    // when
    DseSinkConfig dseSinkConfig = new DseSinkConfig(props);

    // then
    assertThat(dseSinkConfig.getJavaDriverSettings().get(CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING))
        .isEqualTo("10");
    assertThat(logs.getLoggedMessages())
        .contains(
            String.format(
                "The %s setting is deprecated. You should use %s setting instead.",
                CONNECTION_POOL_LOCAL_SIZE, CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING));
  }

  @Test
  void should_use_java_driver_setting_connectionPoolLocalSize(
      @LogCapture(level = WARN, value = DseSinkConfig.class) LogInterceptor logs) {
    // given
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING, "100")
                .build());

    // when
    DseSinkConfig dseSinkConfig = new DseSinkConfig(props);

    // then
    assertThat(dseSinkConfig.getJavaDriverSettings().get(CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING))
        .isEqualTo("100");
    assertThat(logs.getLoggedMessages())
        .doesNotContain(
            String.format(
                "The %s setting is deprecated. You should use %s setting instead.",
                CONNECTION_POOL_LOCAL_SIZE, CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING));
  }

  @Test
  void should_set_default_for_connectionPoolLocalSize(
      @LogCapture(level = WARN, value = DseSinkConfig.class) LogInterceptor logs) {
    // when
    DseSinkConfig dseSinkConfig = new DseSinkConfig(Collections.emptyMap());

    // then
    assertThat(dseSinkConfig.getJavaDriverSettings().get(CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING))
        .isEqualTo(CONNECTION_POOL_LOCAL_SIZE_DEFAULT);
    assertThat(logs.getLoggedMessages())
        .doesNotContain(
            String.format(
                "The %s setting is deprecated. You should use %s setting instead.",
                CONNECTION_POOL_LOCAL_SIZE, CONNECTION_POOL_LOCAL_SIZE_DRIVER_SETTING));
  }

  @Test
  void should_error_invalid_maxConcurrentRequests() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder().put(CONCURRENT_REQUESTS_OPT, "foo").build());
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Invalid value foo for configuration maxConcurrentRequests");

    props.put(CONCURRENT_REQUESTS_OPT, "0");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Value must be at least 1");

    props.put(CONCURRENT_REQUESTS_OPT, "-1");
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Value must be at least 1");
  }

  @Test
  void should_error_invalid_compression_type() {
    Map<String, String> props =
        Maps.newHashMap(ImmutableMap.<String, String>builder().put(COMPRESSION_OPT, "foo").build());
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            String.format(
                "Invalid value foo for configuration %s: valid values are None, Snappy, LZ4",
                COMPRESSION_OPT));
  }

  @Test
  void should_error_missing_dc_with_contactPoints() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder().put(CONTACT_POINTS_OPT, "127.0.0.1").build();
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            String.format(
                "When contact points is provided, %s must also be specified",
                LOCAL_DC_DRIVER_SETTING));
  }

  @Test
  void should_handle_dc_with_contactPoints() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(CONTACT_POINTS_OPT, "127.0.0.1, 127.0.1.1")
            .put(DC_OPT, "local")
            .build();

    DseSinkConfig d = new DseSinkConfig(props);
    assertThat(d.getContactPoints()).containsExactly("127.0.0.1", "127.0.1.1");
    assertThat(d.getLocalDc()).isEqualTo("local");
  }

  @Test
  void should_handle_dc_with_contactPoints_driver_prefix() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(CONTACT_POINTS_OPT, "127.0.0.1, 127.0.1.1")
            .put(LOCAL_DC_DRIVER_SETTING, "local")
            .build();

    DseSinkConfig d = new DseSinkConfig(props);
    assertThat(d.getContactPoints()).containsExactly("127.0.0.1", "127.0.1.1");
    assertThat(d.getLocalDc()).isEqualTo("local");
  }

  @Test
  void should_handle_port() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder().put(PORT_OPT, "5725").build();

    DseSinkConfig d = new DseSinkConfig(props);
    assertThat(d.getPort()).isEqualTo(5725);
  }

  @Test
  void should_handle_maxConcurrentRequests() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder().put(CONCURRENT_REQUESTS_OPT, "129").build();

    DseSinkConfig d = new DseSinkConfig(props);
    assertThat(d.getMaxConcurrentRequests()).isEqualTo(129);
  }

  @Test
  void should_handle_instance_name() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder().put(SinkUtil.NAME_OPT, "myinst").build();

    DseSinkConfig d = new DseSinkConfig(props);
    assertThat(d.getInstanceName()).isEqualTo("myinst");
  }

  @Test
  void should_handle_secure_connect_bundle() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(SECURE_CONNECT_BUNDLE_OPT, "/location/to/bundle")
            .build();

    DseSinkConfig d = new DseSinkConfig(props);
    assertThat(d.getSecureConnectBundle()).isEqualTo("/location/to/bundle");
  }

  @Test
  void should_throw_when_secure_connect_bundle_and_contact_points_provided() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(SECURE_CONNECT_BUNDLE_OPT, "/location/to/bundle")
            .put(CONTACT_POINTS_OPT, "127.0.0.1")
            .build();

    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            String.format(
                "When %s parameter is specified you should not provide %s.",
                SECURE_CONNECT_BUNDLE_OPT, CONTACT_POINTS_OPT));
  }

  @Test
  void should_throw_when_secure_connect_bundle_and_local_dc_provided() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(SECURE_CONNECT_BUNDLE_OPT, "/location/to/bundle")
            .put(DC_OPT, "dc1")
            .build();

    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            String.format(
                "When %s parameter is specified you should not provide %s.",
                SECURE_CONNECT_BUNDLE_OPT, LOCAL_DC_DRIVER_SETTING));
  }

  @Test
  void should_throw_when_secure_connect_bundle_and_local_dc_driver_setting_provided() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(SECURE_CONNECT_BUNDLE_OPT, "/location/to/bundle")
            .put(LOCAL_DC_DRIVER_SETTING, "dc1")
            .build();

    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            String.format(
                "When %s parameter is specified you should not provide %s.",
                SECURE_CONNECT_BUNDLE_OPT, LOCAL_DC_DRIVER_SETTING));
  }

  @Test
  void should_throw_when_secure_connect_bundle_and_ssl_setting_provided() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(SECURE_CONNECT_BUNDLE_OPT, "/location/to/bundle")
            .put(PROVIDER_OPT, "JDK")
            .build();

    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            String.format(
                "When %s parameter is specified you should not provide any setting under %s.",
                SECURE_CONNECT_BUNDLE_OPT, SSL_OPT_PREFIX));
  }

  @Test
  void should_parse_multiple_topic_configs() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(getTableSettingPath("mytopic", "MyKs", "MyTable", MAPPING_OPT), "c1=value.f1")
                .put(
                    getTableSettingPath("yourtopic", "MyKs2", "MyTable2", MAPPING_OPT),
                    "d1=value.f1")
                .build());
    DseSinkConfig d = new DseSinkConfig(props);
    Map<String, TopicConfig> topicConfigs = d.getTopicConfigs();
    assertThat(topicConfigs.size()).isEqualTo(2);
    assertTopic(
        "MyKs",
        "MyTable",
        ImmutableMap.<CqlIdentifier, CqlIdentifier>builder()
            .put(CqlIdentifier.fromInternal("c1"), CqlIdentifier.fromInternal("value.f1"))
            .build(),
        topicConfigs.get("mytopic"));
    assertTopic(
        "MyKs2",
        "MyTable2",
        ImmutableMap.<CqlIdentifier, CqlIdentifier>builder()
            .put(CqlIdentifier.fromInternal("d1"), CqlIdentifier.fromInternal("value.f1"))
            .build(),
        topicConfigs.get("yourtopic"));
  }

  @ParameterizedTest(name = "[{index}] topicName={0}")
  @MethodSource("correctTopicNames")
  void should_parse_correct_kafka_topic_names(String topicName) {
    // given
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(
                    String.format("topic.%s.%s.%s.%s", topicName, "ks", "tb", "mapping"),
                    "c1=value.f1")
                .build());
    // when
    DseSinkConfig d = new DseSinkConfig(props);
    Map<String, TopicConfig> topicConfigs = d.getTopicConfigs();

    // then
    assertThat(topicConfigs.size()).isEqualTo(1);
    assertTopic(
        "ks",
        "tb",
        ImmutableMap.<CqlIdentifier, CqlIdentifier>builder()
            .put(CqlIdentifier.fromInternal("c1"), CqlIdentifier.fromInternal("value.f1"))
            .build(),
        topicConfigs.get(topicName));
  }

  @ParameterizedTest(name = "[{index}] topicName={0}")
  @MethodSource("incorrectTopicNames")
  void should_not_parse_incorrect_kafka_topic_names(String topicName) {
    // given
    String settingName = String.format("topic.%s.%s.%s.%s", topicName, "ks", "tb", "mapping");
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder().put(settingName, "c1=value.f1").build());
    // when then
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "The setting: "
                + settingName
                + " does not match topic.keyspace.table nor topic.codec regular expression pattern");
  }

  @Test
  void should_favor_deprecated_setting_over_java_driver_localDc(
      @LogCapture(level = WARN, value = DseSinkConfig.class) LogInterceptor logs) {
    // given
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(DC_OPT, "dc")
                .put(LOCAL_DC_DRIVER_SETTING, "dc_suppressed")
                .build());

    // when
    DseSinkConfig dseSinkConfig = new DseSinkConfig(props);

    // then
    assertThat(dseSinkConfig.getJavaDriverSettings().get(LOCAL_DC_DRIVER_SETTING)).isEqualTo("dc");
    assertThat(logs.getLoggedMessages())
        .contains(
            String.format(
                "The %s setting is deprecated. You should use %s setting instead.",
                DC_OPT, LOCAL_DC_DRIVER_SETTING));
  }

  @Test
  void should_use_deprecated_setting_as_a_new_java_driver_setting_localDc(
      @LogCapture(level = WARN, value = DseSinkConfig.class) LogInterceptor logs) {
    // given
    Map<String, String> props =
        Maps.newHashMap(ImmutableMap.<String, String>builder().put(DC_OPT, "dc").build());

    // when
    DseSinkConfig dseSinkConfig = new DseSinkConfig(props);

    // then
    assertThat(dseSinkConfig.getJavaDriverSettings().get(LOCAL_DC_DRIVER_SETTING)).isEqualTo("dc");
    assertThat(logs.getLoggedMessages())
        .contains(
            String.format(
                "The %s setting is deprecated. You should use %s setting instead.",
                DC_OPT, LOCAL_DC_DRIVER_SETTING));
  }

  @Test
  void should_use_java_driver_setting_localDc(
      @LogCapture(level = WARN, value = DseSinkConfig.class) LogInterceptor logs) {
    // given
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder().put(LOCAL_DC_DRIVER_SETTING, "dc").build());

    // when
    DseSinkConfig dseSinkConfig = new DseSinkConfig(props);

    // then
    assertThat(dseSinkConfig.getJavaDriverSettings().get(LOCAL_DC_DRIVER_SETTING)).isEqualTo("dc");
    assertThat(logs.getLoggedMessages())
        .doesNotContain(
            String.format(
                "The %s setting is deprecated. You should use %s setting instead.",
                DC_OPT, LOCAL_DC_DRIVER_SETTING));
  }

  @Test
  void should_not_set_default_for_localDc(
      @LogCapture(level = WARN, value = DseSinkConfig.class) LogInterceptor logs) {
    // when
    DseSinkConfig dseSinkConfig = new DseSinkConfig(Collections.emptyMap());

    // then
    assertThat(dseSinkConfig.getJavaDriverSettings().get(LOCAL_DC_DRIVER_SETTING)).isNull();
    assertThat(logs.getLoggedMessages())
        .doesNotContain(
            String.format(
                "The %s setting is deprecated. You should use %s setting instead.",
                DC_OPT, LOCAL_DC_DRIVER_SETTING));
  }

  private static Stream<? extends Arguments> correctTopicNames() {
    return Stream.of(
        Arguments.of("org.datastax.init.event.history"),
        Arguments.of("org_datastax_init_event_history"),
        Arguments.of("org-datastax-init-event-history"),
        Arguments.of("org.datastax-init_event.history"),
        Arguments.of("t.codec.ttl"),
        Arguments.of("1.2_3.A_z"));
  }

  private static Stream<? extends Arguments> incorrectTopicNames() {
    return Stream.of(
        Arguments.of("org,datastax"),
        Arguments.of("org&topic"),
        Arguments.of("()"),
        Arguments.of("%"));
  }

  @Test
  void should_error_when_missing_topic_settings() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put("topics", "mytopic2")
                .put(getTableSettingPath("mytopic", "MyKs", "MyTable", MAPPING_OPT), "c1=value.f1")
                .build());

    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Missing topic settings (topic.mytopic2.*) for topic mytopic2");
  }

  @Test
  void should_handle_topics_list_with_spaces() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put("topics", "mytopic, mytopic2")
                .put(getTableSettingPath("mytopic", "MyKs", "MyTable", MAPPING_OPT), "c1=value.f1")
                .put(
                    getTableSettingPath("mytopic2", "MyKs2", "MyTable2", MAPPING_OPT),
                    "c1=value.f1")
                .build());

    DseSinkConfig config = new DseSinkConfig(props);
    assertThat(config.getTopicConfigs().size()).isEqualTo(2);
  }

  @Test
  void should_handle_topics_list_with_dots() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put("topics", "org.datastax.init.event.history, org.datastax.init.event.history2")
                .put(
                    getTableSettingPath(
                        "org.datastax.init.event.history", "MyKs", "MyTable", MAPPING_OPT),
                    "c1=value.f1")
                .put(
                    getTableSettingPath(
                        "org.datastax.init.event.history2", "MyKs2", "MyTable2", MAPPING_OPT),
                    "c1=value.f1")
                .build());

    DseSinkConfig config = new DseSinkConfig(props);
    assertThat(config.getTopicConfigs().size()).isEqualTo(2);
  }

  @Test
  void should_parse_codec_setting_per_topic() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(
                    getTableSettingPath(
                        "org.datastax.init.event.history", "MyKs2", "MyTable2", MAPPING_OPT),
                    "c1=value.f1")
                .put(getTableSettingPath("t1", "MyKs2", "MyTable2", MAPPING_OPT), "c1=value.f1")
                .put("topic.org.datastax.init.event.history.codec.timeZone", "Europe/Warsaw")
                .put("topic.t1.codec.timeZone", "Europe/Warsaw")
                .build());

    DseSinkConfig config = new DseSinkConfig(props);
    assertThat(config.getTopicConfigs().size()).isEqualTo(2);
  }

  @Test
  void should_not_match_setting_with_extra_suffix() {
    // given
    String settingName = String.format("topic.%s.%s.%s.%s", "t1", "ks", "tb", "mapping.extra");
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder().put(settingName, "c1=value.f1").build());
    // when then
    assertThatThrownBy(() -> new DseSinkConfig(props))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "The setting: "
                + settingName
                + " does not match topic.keyspace.table nor topic.codec regular expression pattern");
  }

  private void assertTopic(
      String keyspace,
      String table,
      Map<CqlIdentifier, CqlIdentifier> mapping,
      TopicConfig config) {
    TableConfig tableConfig = config.getTableConfigs().iterator().next();
    assertThat(tableConfig.getKeyspace()).isEqualTo(CqlIdentifier.fromInternal(keyspace));
    assertThat(tableConfig.getTable()).isEqualTo(CqlIdentifier.fromInternal(table));
    assertThat(tableConfig.getMapping()).isEqualTo(mapping);
  }
}
