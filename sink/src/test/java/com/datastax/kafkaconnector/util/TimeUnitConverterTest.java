/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TimeUnitConverterTest {

  @ParameterizedTest(name = "[{index}] timeUnit={0}, ttl={1}, expectedSeconds={2}")
  @MethodSource("expectedToSeconds")
  void should_convert_to_seconds(TimeUnit timeUnit, Number ttl, Long expectedSeconds) {
    long result = TimeUnitConverter.convertTtlToSeconds(timeUnit, ttl);

    // then
    assertThat(result).isEqualTo(expectedSeconds);
  }

  private static Stream<? extends Arguments> expectedToSeconds() {
    return Stream.of(
        Arguments.of(TimeUnit.SECONDS, 1, 1L),
        Arguments.of(TimeUnit.MILLISECONDS, 1000L, 1L),
        Arguments.of(TimeUnit.MICROSECONDS, 1000000D, 1L),
        Arguments.of(TimeUnit.DAYS, 1F, 86400L),
        Arguments.of(TimeUnit.HOURS, BigInteger.valueOf(1L), 3600L));
  }
}
