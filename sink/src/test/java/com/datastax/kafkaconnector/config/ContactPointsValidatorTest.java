/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.config;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ContactPointsValidatorTest {

  @ParameterizedTest(name = "[{index}] contactPointsList={0}")
  @MethodSource("correctContactPoints")
  void should_not_throw_when_contact_point_is_correct(String contactPointsList) {
    // given
    Map<String, String> config =
        ImmutableMap.of("contactPoints", contactPointsList, "loadBalancing.localDc", "dc1");
    DseSinkConfig dseSinkConfig = new DseSinkConfig(config);

    // when
    ContactPointsValidator.validateContactPoints(dseSinkConfig.getContactPoints());

    // then no throw
  }

  @ParameterizedTest(name = "[{index}] contactPointsList={0}")
  @MethodSource("incorrectContactPoints")
  void should_throw_config_exception_for_incorrect_contact_points(String contactPointsList) {
    // given
    Map<String, String> config =
        ImmutableMap.of("contactPoints", contactPointsList, "loadBalancing.localDc", "dc1");
    DseSinkConfig dseSinkConfig = new DseSinkConfig(config);

    // when, then
    assertThatThrownBy(
            () -> ContactPointsValidator.validateContactPoints(dseSinkConfig.getContactPoints()))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("contactPoints");
  }

  private static Stream<? extends Arguments> correctContactPoints() {
    return Stream.of(
        Arguments.of("localhost"),
        Arguments.of("127.0.0.1"),
        Arguments.of("127.0.0.1,127.0.0.2"),
        Arguments.of("2001:0db8:85a3:08d3:1319:8a2e:0370:7344"),
        Arguments.of(tenIp4Addresses()),
        Arguments.of(""));
  }

  private static Stream<? extends Arguments> incorrectContactPoints() {
    return Stream.of(
        Arguments.of("\"127.0.0.1\",\"127.0.0.2\""),
        Arguments.of("\"not-an-url"),
        Arguments.of("*.0.0.1"));
  }

  private static String tenIp4Addresses() {
    return IntStream.range(1, 10).mapToObj(i -> "127.0.0." + i).collect(Collectors.joining(","));
  }
}
