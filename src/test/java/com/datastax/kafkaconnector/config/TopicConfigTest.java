/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.config;

import static com.datastax.kafkaconnector.config.TopicConfig.KEYSPACE_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.MAPPING_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.TABLE_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.TTL_OPT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class TopicConfigTest {
  @Test
  void should_error_missing_keyspace() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(TopicConfig.getTopicSettingName("mytopic", TABLE_OPT), "mytable")
            .build();
    assertThatThrownBy(() -> new TopicConfig("mytopic", props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Missing required configuration \"topic.mytopic.keyspace\"");
  }

  @Test
  void should_error_missing_table() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(TopicConfig.getTopicSettingName("mytopic", KEYSPACE_OPT), "myks")
            .build();
    assertThatThrownBy(() -> new TopicConfig("mytopic", props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Missing required configuration \"topic.mytopic.table\"");
  }

  @Test
  void should_handle_keyspace_and_table() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(TopicConfig.getTopicSettingName("mytopic", KEYSPACE_OPT), "myks")
                .put(TopicConfig.getTopicSettingName("mytopic", TABLE_OPT), "mytable")
                .put(TopicConfig.getTopicSettingName("mytopic", MAPPING_OPT), "c1=value.f1")
                .build());

    TopicConfig config = new TopicConfig("mytopic", props);
    assertThat(config.getKeyspace().asInternal()).isEqualTo("myks");
    assertThat(config.getTable().asInternal()).isEqualTo("mytable");

    props.put(KEYSPACE_OPT, "\"myks\"");
    props.put(TABLE_OPT, "\"mytable\"");
    config = new TopicConfig("mytopic", props);
    assertThat(config.getKeyspace().asInternal()).isEqualTo("myks");
    assertThat(config.getTable().asInternal()).isEqualTo("mytable");
  }

  @Test
  void should_handle_case_sensitive_keyspace_and_table() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(TopicConfig.getTopicSettingName("mytopic", KEYSPACE_OPT), "MyKs")
                .put(TopicConfig.getTopicSettingName("mytopic", TABLE_OPT), "MyTable")
                .put(TopicConfig.getTopicSettingName("mytopic", MAPPING_OPT), "c1=value.f1")
                .build());

    TopicConfig config = new TopicConfig("mytopic", props);
    assertThat(config.getKeyspace().asInternal()).isEqualTo("MyKs");
    assertThat(config.getTable().asInternal()).isEqualTo("MyTable");

    props.put(KEYSPACE_OPT, "\"MyKs\"");
    props.put(TABLE_OPT, "\"MyTable\"");
    config = new TopicConfig("mytopic", props);
    assertThat(config.getKeyspace().asInternal()).isEqualTo("MyKs");
    assertThat(config.getTable().asInternal()).isEqualTo("MyTable");
  }

  @Test
  void should_error_missing_mapping() {
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put(TopicConfig.getTopicSettingName("mytopic", KEYSPACE_OPT), "myks")
            .put(TopicConfig.getTopicSettingName("mytopic", TABLE_OPT), "mytable")
            .build();
    assertThatThrownBy(() -> new TopicConfig("mytopic", props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Missing required configuration \"topic.mytopic.mapping\"");
  }

  @Test
  void should_error_invalid_ttl() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(TopicConfig.getTopicSettingName("mytopic", KEYSPACE_OPT), "myks")
                .put(TopicConfig.getTopicSettingName("mytopic", TABLE_OPT), "mytable")
                .put(TopicConfig.getTopicSettingName("mytopic", MAPPING_OPT), "c1=value.f1")
                .put(TopicConfig.getTopicSettingName("mytopic", TTL_OPT), "foo")
                .build());
    assertThatThrownBy(() -> new TopicConfig("mytopic", props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Invalid value foo for configuration topic.mytopic.ttl");

    props.put(TopicConfig.getTopicSettingName("mytopic", TTL_OPT), "-2");
    assertThatThrownBy(() -> new TopicConfig("mytopic", props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Value must be at least -1");
  }

  @Test
  void should_error_bad_mapping() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(TopicConfig.getTopicSettingName("mytopic", KEYSPACE_OPT), "myks")
                .put(TopicConfig.getTopicSettingName("mytopic", TABLE_OPT), "mytable")
                .put(TopicConfig.getTopicSettingName("mytopic", MAPPING_OPT), "")
                .build());
    assertThatThrownBy(() -> new TopicConfig("mytopic", props))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            "Invalid value '' for configuration topic.mytopic.mapping: Could not be parsed");

    // Key without value
    props.put(TopicConfig.getTopicSettingName("mytopic", MAPPING_OPT), "jack");
    assertThatThrownBy(() -> new TopicConfig("mytopic", props))
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith("Invalid value 'jack' for configuration topic.mytopic.mapping:")
        .hasMessageEndingWith("expecting '='");

    props.put(TopicConfig.getTopicSettingName("mytopic", MAPPING_OPT), "jack=");
    assertThatThrownBy(() -> new TopicConfig("mytopic", props))
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith("Invalid value 'jack=' for configuration topic.mytopic.mapping:");

    // Value without key
    props.put(TopicConfig.getTopicSettingName("mytopic", MAPPING_OPT), "= value");
    assertThatThrownBy(() -> new TopicConfig("mytopic", props))
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith("Invalid value '= value' for configuration topic.mytopic.mapping:");

    // Non-first key without value.
    props.put(TopicConfig.getTopicSettingName("mytopic", MAPPING_OPT), "first = value.good, jack");
    assertThatThrownBy(() -> new TopicConfig("mytopic", props))
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith(
            "Invalid value 'first = value.good, jack' for configuration topic.mytopic.mapping:");

    // Non-first value without key
    props.put(
        TopicConfig.getTopicSettingName("mytopic", MAPPING_OPT), "first = value.good, = value");
    assertThatThrownBy(() -> new TopicConfig("mytopic", props))
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith(
            "Invalid value 'first = value.good, = value' for configuration topic.mytopic.mapping:");

    // Multiple mappings for the same column
    props.put(TopicConfig.getTopicSettingName("mytopic", MAPPING_OPT), "c1=value.f1, c1=value.f2");
    assertThatThrownBy(() -> new TopicConfig("mytopic", props))
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith(
            "Invalid value 'c1=value.f1, c1=value.f2' for configuration topic.mytopic.mapping: Encountered the following errors:")
        .hasMessageContaining("Mapping already defined for column 'c1'");

    // Mapping a field whose name doesn't start with "key." or "value."
    props.put(TopicConfig.getTopicSettingName("mytopic", MAPPING_OPT), "c1=f1");
    assertThatThrownBy(() -> new TopicConfig("mytopic", props))
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith(
            "Invalid value 'c1=f1' for configuration topic.mytopic.mapping: Encountered the following errors:")
        .hasMessageContaining(
            "Invalid field name 'f1': field names in mapping must be 'key', 'value', or start with 'key.' or 'value.'.");
  }

  @Test
  void should_parse_mapping() {
    Map<String, String> props =
        Maps.newHashMap(
            ImmutableMap.<String, String>builder()
                .put(TopicConfig.getTopicSettingName("mytopic", KEYSPACE_OPT), "myks")
                .put(TopicConfig.getTopicSettingName("mytopic", TABLE_OPT), "mytable")
                .put(
                    TopicConfig.getTopicSettingName("mytopic", MAPPING_OPT),
                    "a=key.b, first = value.good, \"jack\"=\"value.bill\",third=key.great, c1=key, "
                        + "\"This has spaces, \"\", and commas\" = \"value.me, \"\" too\", d1=value")
                .build());

    TopicConfig config = new TopicConfig("mytopic", props);
    assertThat(config.getMapping())
        .containsEntry(
            CqlIdentifier.fromInternal("This has spaces, \", and commas"),
            CqlIdentifier.fromInternal("value.me, \" too"))
        .containsEntry(CqlIdentifier.fromInternal("c1"), CqlIdentifier.fromInternal("key.__self"))
        .containsEntry(CqlIdentifier.fromInternal("d1"), CqlIdentifier.fromInternal("value.__self"))
        .containsEntry(CqlIdentifier.fromInternal("jack"), CqlIdentifier.fromInternal("value.bill"))
        .containsEntry(CqlIdentifier.fromInternal("a"), CqlIdentifier.fromInternal("key.b"))
        .containsEntry(
            CqlIdentifier.fromInternal("first"), CqlIdentifier.fromInternal("value.good"));
  }
}
