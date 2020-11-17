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
package com.datastax.oss.sink.record;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StructTimeUnitConverterTest {

  @ParameterizedTest(name = "[{index}] fieldValue={0}, expectedSeconds={1}")
  @MethodSource("expectedTtlToSeconds")
  void should_convert_java_number_types_that_are_supported_in_struct_for_ttl(
      Number fieldValue, Number expectedSeconds) {
    Object result = StructTimeUnitConverter.transformTtlField(TimeUnit.MILLISECONDS, fieldValue);

    // then
    assertThat(result).isEqualTo(expectedSeconds);
  }

  @ParameterizedTest(name = "[{index}] fieldValue={0}, expectedSeconds={1}")
  @MethodSource("expectedTimestampToMicroseconds")
  void should_convert_java_number_types_that_are_supported_in_struct_for_timestamp(
      Number fieldValue, Number expectedSeconds) {
    Object result =
        StructTimeUnitConverter.transformTimestampField(TimeUnit.MILLISECONDS, fieldValue);

    // then
    assertThat(result).isEqualTo(expectedSeconds);
  }

  private static Stream<? extends Arguments> expectedTtlToSeconds() {
    return Stream.of(
        Arguments.of(1000F, 1.0F),
        Arguments.of(-1000F, 0F),
        Arguments.of(1000D, 1D),
        Arguments.of(-1000D, 0D),
        Arguments.of(1000L, 1L),
        Arguments.of(-1000L, 0L),
        Arguments.of(1000, 1),
        Arguments.of(-1000, 0),
        Arguments.of((short) 1000, (short) 1),
        Arguments.of((short) -1000, (short) 0),
        Arguments.of(
            Long.valueOf(1000).byteValue(),
            Long.valueOf(0).byteValue()), // 1000L overflows byte making it < 1000L
        Arguments.of(Long.valueOf(-1000).byteValue(), Long.valueOf(0).byteValue()),
        Arguments.of(BigDecimal.valueOf(1000), BigDecimal.valueOf(1)),
        Arguments.of(BigDecimal.valueOf(-1000), BigDecimal.ZERO),
        Arguments.of(BigInteger.valueOf(1000), BigInteger.valueOf(1)),
        Arguments.of(BigInteger.valueOf(-1000), BigInteger.ZERO));
  }

  private static Stream<? extends Arguments> expectedTimestampToMicroseconds() {
    return Stream.of(
        Arguments.of(1000F, 1_000_000.0F),
        Arguments.of(-1000F, -1_000_000F),
        Arguments.of(1000D, 1_000_000D),
        Arguments.of(-1000D, -1_000_000D),
        Arguments.of(1000L, 1_000_000L),
        Arguments.of(-1000L, -1_000_000L),
        Arguments.of(1000, 1_000_000),
        Arguments.of(-1000, -1_000_000),
        Arguments.of((short) 1000, (short) 1_000_000),
        Arguments.of((short) -1000, (short) -1_000_000),
        Arguments.of(Long.valueOf(1000).byteValue(), Long.valueOf(1_000_000).byteValue()),
        Arguments.of(Long.valueOf(-1000).byteValue(), Long.valueOf(-1_000_000).byteValue()),
        Arguments.of(BigDecimal.valueOf(1000), BigDecimal.valueOf(1_000_000)),
        Arguments.of(BigDecimal.valueOf(-1000), BigDecimal.valueOf(-1_000_000)),
        Arguments.of(BigInteger.valueOf(1000), BigInteger.valueOf(1_000_000)),
        Arguments.of(BigInteger.valueOf(-1000), BigInteger.valueOf(-1_000_000)));
  }
}
