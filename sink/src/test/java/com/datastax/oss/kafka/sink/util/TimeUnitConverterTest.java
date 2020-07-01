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
package com.datastax.oss.kafka.sink.util;

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
    long result = TimeUnitConverter.convertToSeconds(timeUnit, ttl);

    // then
    assertThat(result).isEqualTo(expectedSeconds);
  }

  @ParameterizedTest(name = "[{index}] timeUnit={0}, timestamp={1}, expectedMicroseconds={2}")
  @MethodSource("expectedToMicroseconds")
  void should_convert_to_microseconds(
      TimeUnit timeUnit, Number timestamp, Long expectedMicroseconds) {
    long result = TimeUnitConverter.convertToMicroseconds(timeUnit, timestamp);

    // then
    assertThat(result).isEqualTo(expectedMicroseconds);
  }

  private static Stream<? extends Arguments> expectedToSeconds() {
    return Stream.of(
        Arguments.of(TimeUnit.SECONDS, 1, 1L),
        Arguments.of(TimeUnit.MILLISECONDS, 1000L, 1L),
        Arguments.of(TimeUnit.MICROSECONDS, 1000000D, 1L),
        Arguments.of(TimeUnit.DAYS, 1F, 86400L),
        Arguments.of(TimeUnit.HOURS, BigInteger.valueOf(1L), 3600L));
  }

  private static Stream<? extends Arguments> expectedToMicroseconds() {
    return Stream.of(
        Arguments.of(TimeUnit.SECONDS, 1, 1_000_000L),
        Arguments.of(TimeUnit.MILLISECONDS, 1L, 1000L),
        Arguments.of(TimeUnit.MICROSECONDS, 1000000D, 1_000_000L),
        Arguments.of(TimeUnit.DAYS, 1F, 86_400_000_000L),
        Arguments.of(TimeUnit.HOURS, BigInteger.valueOf(1L), 3_600_000_000L));
  }
}
