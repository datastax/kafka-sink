/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.config;

import static com.datastax.kafkaconnector.config.TableConfig.CL_OPT;
import static com.datastax.kafkaconnector.config.TableConfig.MAPPING_OPT;
import static com.datastax.kafkaconnector.config.TableConfig.TIMESTAMP_TIME_UNIT_OPT;
import static com.datastax.kafkaconnector.config.TableConfig.TTL_OPT;
import static com.datastax.kafkaconnector.config.TableConfig.TTL_TIME_UNIT_OPT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TableConfigTest {
  private TableConfigBuilder configBuilder;

  @BeforeEach
  void setup() {
    configBuilder =
        new TableConfigBuilder("mytopic", "myks", "mytable", false)
            .addSimpleSetting(MAPPING_OPT, "c1=value.f1");
  }

  @Test
  void should_error_missing_mapping() {
    assertThatThrownBy(() -> new TableConfigBuilder("mytopic", "myks", "mytable", false).build())
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            "Missing required configuration \"topic.mytopic.myks.mytable.mapping\"");
  }

  @Test
  void should_error_invalid_ttl() {
    assertThatThrownBy(() -> configBuilder.addSimpleSetting(TTL_OPT, "foo").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Invalid value foo for configuration topic.mytopic.myks.mytable.ttl");

    assertThatThrownBy(() -> configBuilder.addSimpleSetting(TTL_OPT, "-2").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("Value must be at least -1");
  }

  @Test
  void should_error_invalid_consistencyLevel() {
    assertThatThrownBy(() -> configBuilder.addSimpleSetting(CL_OPT, "foo").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            "Invalid value 'foo' for configuration topic.mytopic.myks.mytable.consistencyLevel")
        .hasMessageContaining("valid values include: ANY, ONE, TWO");
  }

  @Test
  void should_error_bad_mapping() {
    assertThatThrownBy(() -> configBuilder.addSimpleSetting(MAPPING_OPT, "").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            "Invalid value '' for configuration topic.mytopic.myks.mytable.mapping: Could not be parsed");

    // Key without value
    assertThatThrownBy(() -> configBuilder.addSimpleSetting(MAPPING_OPT, "jack").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith(
            "Invalid value 'jack' for configuration topic.mytopic.myks.mytable.mapping:")
        .hasMessageEndingWith("expecting '='");

    assertThatThrownBy(() -> configBuilder.addSimpleSetting(MAPPING_OPT, "jack=").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith(
            "Invalid value 'jack=' for configuration topic.mytopic.myks.mytable.mapping:");

    // Value without key
    assertThatThrownBy(() -> configBuilder.addSimpleSetting(MAPPING_OPT, "= value").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith(
            "Invalid value '= value' for configuration topic.mytopic.myks.mytable.mapping:");

    // Non-first key without value.
    assertThatThrownBy(
            () -> configBuilder.addSimpleSetting(MAPPING_OPT, "first = value.good, jack").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith(
            "Invalid value 'first = value.good, jack' for configuration topic.mytopic.myks.mytable.mapping:");

    // Non-first value without key
    assertThatThrownBy(
            () ->
                configBuilder.addSimpleSetting(MAPPING_OPT, "first = value.good, = value").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith(
            "Invalid value 'first = value.good, = value' for configuration topic.mytopic.myks.mytable.mapping:");

    // Multiple mappings for the same column
    assertThatThrownBy(
            () -> configBuilder.addSimpleSetting(MAPPING_OPT, "c1=value.f1, c1=value.f2").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith(
            "Invalid value 'c1=value.f1, c1=value.f2' for configuration topic.mytopic.myks.mytable.mapping: Encountered the following errors:")
        .hasMessageContaining("Mapping already defined for column 'c1'");

    // Mapping a field whose name doesn't start with "key." or "value." or "header."
    assertThatThrownBy(() -> configBuilder.addSimpleSetting(MAPPING_OPT, "c1=f1").build())
        .isInstanceOf(ConfigException.class)
        .hasMessageStartingWith(
            "Invalid value 'c1=f1' for configuration topic.mytopic.myks.mytable.mapping: Encountered the following errors:")
        .hasMessageContaining(
            "Invalid field name 'f1': field names in mapping must be 'key', 'value', or start with 'key.' or 'value.' or 'header.'.");
  }

  @Test
  void should_parse_mapping() {
    TableConfig config =
        configBuilder
            .addSimpleSetting(
                MAPPING_OPT,
                "a=key.b, first = value.good, \"jack\"=\"value.bill\",third=key.great, c1=key, "
                    + "\"This has spaces, \"\", and commas\" = \"value.me, \"\" too\", d1=value")
            .build();

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
    assertThat(config.getTtlTimeUnit())
        .isEqualTo(TimeUnit.SECONDS); // default timeUnit for ttl for backward compatibility
    assertThat(config.getTimestampTimeUnit())
        .isEqualTo(TimeUnit.MICROSECONDS); // default timeUnit for ttl for backward compatibility
  }

  @Test
  void should_parse_mapping_that_contains_only_header() {
    TableConfig config = configBuilder.addSimpleSetting(MAPPING_OPT, "a=header.a").build();

    assertThat(config.getMapping())
        .containsEntry(CqlIdentifier.fromInternal("a"), CqlIdentifier.fromInternal("header.a"));
  }

  @ParameterizedTest(name = "[{index}] ttlTimestampStringParameter={0}, expectedTimeUnit={1}")
  @MethodSource("ttlTimestampTimeUnits")
  void should_create_ttl_and_timestamp_time_units(
      String ttlTimestampStringParameter, TimeUnit expectedTimeUnit) {
    TableConfig config =
        configBuilder
            .addSimpleSetting(MAPPING_OPT, "a=key.b")
            .addSimpleSetting(TTL_TIME_UNIT_OPT, ttlTimestampStringParameter)
            .addSimpleSetting(TIMESTAMP_TIME_UNIT_OPT, ttlTimestampStringParameter)
            .build();

    assertThat(config.getTtlTimeUnit()).isEqualTo(expectedTimeUnit);
    assertThat(config.getTimestampTimeUnit()).isEqualTo(expectedTimeUnit);
  }

  private static Stream<? extends Arguments> ttlTimestampTimeUnits() {
    return Stream.of(
        Arguments.of("MILLISECONDS", TimeUnit.MILLISECONDS),
        Arguments.of("MINUTES", TimeUnit.MINUTES),
        Arguments.of("SECONDS", TimeUnit.SECONDS),
        Arguments.of("DAYS", TimeUnit.DAYS),
        Arguments.of("HOURS", TimeUnit.HOURS),
        Arguments.of("MICROSECONDS", TimeUnit.MICROSECONDS),
        Arguments.of("NANOSECONDS", TimeUnit.NANOSECONDS));
  }
}
