/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.record;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StructTtlConverterTest {

  @ParameterizedTest(name = "[{index}] jsonNode={0}, expectedSeconds={1}")
  @MethodSource("expectedToSeconds")
  void should_convert_java_number_types_that_are_supported_in_struct(
      Number jsonNode, Number expectedSeconds) {
    Object result = StructTtlConverter.transformField(TimeUnit.MILLISECONDS, jsonNode);

    // then
    assertThat(result).isEqualTo(expectedSeconds);
  }

  private static Stream<? extends Arguments> expectedToSeconds() {
    return Stream.of(
        Arguments.of(1000F, 1.0F),
        Arguments.of(-1000F, 0F),
        Arguments.of(1000D, 1D),
        Arguments.of(-1000D, 0D),
        Arguments.of(1000L, 1L),
        Arguments.of(-1000L, 0L),
        Arguments.of(1000, 1),
        Arguments.of(-1000, 0));
  }
}
