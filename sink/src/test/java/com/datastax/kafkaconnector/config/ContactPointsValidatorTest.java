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
        Arguments.of("*.0.0.1"),
        Arguments.of("[]"),
        Arguments.of("[127.0.0.1,127.0.0.2]"));
  }

  private static String tenIp4Addresses() {
    return IntStream.range(1, 10).mapToObj(i -> "127.0.0." + i).collect(Collectors.joining(","));
  }
}
