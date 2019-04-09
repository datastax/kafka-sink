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

class JsonNodeTtlConverterTest {

  @ParameterizedTest(name = "[{index}] jsonNode={0}, expectedSeconds={1}")
  @MethodSource("expectedToSeconds")
  void should_convert_json_number_node(NumericNode jsonNode, NumericNode expectedSeconds) {
    Object result = JsonNodeTtlConverter.transformField(TimeUnit.MILLISECONDS, jsonNode);

    // then
    assertThat(result).isEqualTo(expectedSeconds);
  }

  private static Stream<? extends Arguments> expectedToSeconds() {
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
}
