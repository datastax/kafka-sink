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

import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JsonNodeTimeUnitConverterTest {

  @ParameterizedTest(name = "[{index}] jsonNode={0}, expectedSeconds={1}")
  @MethodSource("expectedTtlToSeconds")
  void should_convert_json_number_node_for_ttl(NumericNode jsonNode, NumericNode expectedSeconds) {
    Object result = JsonNodeTimeUnitConverter.transformTtlField(TimeUnit.MILLISECONDS, jsonNode);

    // then
    assertThat(result).isEqualTo(expectedSeconds);
  }

  @ParameterizedTest(name = "[{index}] jsonNode={0}, expectedSeconds={1}")
  @MethodSource("expectedTimestampToSeconds")
  void should_convert_json_number_node_for_timestamp(
      NumericNode jsonNode, NumericNode expectedSeconds) {
    Object result =
        JsonNodeTimeUnitConverter.transformTimestampField(TimeUnit.MILLISECONDS, jsonNode);

    // then
    assertThat(result).isEqualTo(expectedSeconds);
  }

  private static Stream<? extends Arguments> expectedTtlToSeconds() {
    return Stream.of(
        Arguments.of(
            new BigIntegerNode(BigInteger.valueOf(1000)),
            new BigIntegerNode(BigInteger.valueOf(1))),
        Arguments.of(
            new BigIntegerNode(BigInteger.valueOf(-1000)),
            new BigIntegerNode(BigInteger.valueOf(0))),
        Arguments.of(
            new DecimalNode(BigDecimal.valueOf(1000)), new DecimalNode(BigDecimal.valueOf(1))),
        Arguments.of(
            new DecimalNode(BigDecimal.valueOf(-1000)), new DecimalNode(BigDecimal.valueOf(0))),
        Arguments.of(new IntNode(1000), new IntNode(1)),
        Arguments.of(new IntNode(-1000), new IntNode(0)),
        Arguments.of(new FloatNode(1000), new FloatNode(1)),
        Arguments.of(new FloatNode(-1000), new FloatNode(0)),
        Arguments.of(new DoubleNode(1000), new DoubleNode(1)),
        Arguments.of(new DoubleNode(-1000), new DoubleNode(0)),
        Arguments.of(new LongNode(1000), new LongNode(1)),
        Arguments.of(new LongNode(-1000), new LongNode(0)),
        Arguments.of(new ShortNode((short) 1000), new ShortNode((short) 1)),
        Arguments.of(new ShortNode((short) -1000), new ShortNode((short) 0)));
  }

  private static Stream<? extends Arguments> expectedTimestampToSeconds() {
    return Stream.of(
        Arguments.of(
            new BigIntegerNode(BigInteger.valueOf(1000)),
            new BigIntegerNode(BigInteger.valueOf(1_000_000))),
        Arguments.of(
            new BigIntegerNode(BigInteger.valueOf(-1000)),
            new BigIntegerNode(BigInteger.valueOf(-1_000_000))),
        Arguments.of(
            new DecimalNode(BigDecimal.valueOf(1000)),
            new DecimalNode(BigDecimal.valueOf(1_000_000))),
        Arguments.of(
            new DecimalNode(BigDecimal.valueOf(-1000)),
            new DecimalNode(BigDecimal.valueOf(-1_000_000))),
        Arguments.of(new IntNode(1000), new IntNode(1_000_000)),
        Arguments.of(new IntNode(-1000), new IntNode(-1_000_000)),
        Arguments.of(new FloatNode(1000), new FloatNode(1_000_000)),
        Arguments.of(new FloatNode(-1000), new FloatNode(-1_000_000)),
        Arguments.of(new DoubleNode(1000), new DoubleNode(1_000_000)),
        Arguments.of(new DoubleNode(-1000), new DoubleNode(-1_000_000)),
        Arguments.of(new LongNode(1000), new LongNode(1_000_000)),
        Arguments.of(new LongNode(-1000), new LongNode(-1_000_000)),
        Arguments.of(new ShortNode((short) 1000), new ShortNode((short) 1_000_000)),
        Arguments.of(new ShortNode((short) -1000), new ShortNode((short) -1_000_000)));
  }
}
