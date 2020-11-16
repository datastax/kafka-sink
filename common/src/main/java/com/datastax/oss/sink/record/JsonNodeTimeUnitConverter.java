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

import com.datastax.oss.sink.util.TimeUnitConverter;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ShortNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Predicate;

public class JsonNodeTimeUnitConverter {

  private static final BigIntegerNode BIG_INTEGER_NODE_ZERO = new BigIntegerNode(BigInteger.ZERO);
  private static final DecimalNode DECIMAL_NODE_ZERO = new DecimalNode(BigDecimal.ZERO);
  private static final IntNode INT_NODE_ZERO = new IntNode(0);
  private static final FloatNode FLOAT_NODE_ZERO = new FloatNode(0);
  private static final DoubleNode DOUBLE_NODE_ZERO = new DoubleNode(0);
  private static final LongNode LONG_NODE_ZERO = new LongNode(0);
  private static final ShortNode SHORT_NODE_ZERO = new ShortNode((short) 0);
  private static final Predicate<Number> SHOULD_MAP_TO_ZERO_PREDICATE =
      value -> value.longValue() <= -1;
  private static final Predicate<Number> ALWAYS_FALSE_PREDICATE = value -> false;

  public static Object transformTtlField(TimeUnit ttlTimeUnit, Object fieldValue) {
    return transformField(
        ttlTimeUnit, fieldValue, SHOULD_MAP_TO_ZERO_PREDICATE, TimeUnitConverter::convertToSeconds);
  }

  public static Object transformTimestampField(TimeUnit timestampTimeUnit, Object fieldValue) {
    return transformField(
        timestampTimeUnit,
        fieldValue,
        ALWAYS_FALSE_PREDICATE,
        TimeUnitConverter::convertToMicroseconds);
  }

  private static Object transformField(
      TimeUnit timeUnit,
      Object fieldValue,
      Predicate<Number> shouldMapToZero,
      BiFunction<TimeUnit, Number, Long> converter) {
    if (fieldValue instanceof BigIntegerNode) {
      long value = ((BigIntegerNode) fieldValue).longValue();
      if (shouldMapToZero.test(value)) {
        return BIG_INTEGER_NODE_ZERO;
      }
      return new BigIntegerNode(BigInteger.valueOf(converter.apply(timeUnit, value)));
    } else if (fieldValue instanceof DecimalNode) {
      long value = ((DecimalNode) fieldValue).longValue();
      if (shouldMapToZero.test(value)) {
        return DECIMAL_NODE_ZERO;
      }
      return new DecimalNode(BigDecimal.valueOf(converter.apply(timeUnit, value)));
    } else if (fieldValue instanceof IntNode) {
      int value = ((IntNode) fieldValue).intValue();
      if (shouldMapToZero.test(value)) {
        return INT_NODE_ZERO;
      }
      return new IntNode((int) converter.apply(timeUnit, value).longValue());
    } else if (fieldValue instanceof FloatNode) {
      float value = ((FloatNode) fieldValue).floatValue();
      if (shouldMapToZero.test(value)) {
        return FLOAT_NODE_ZERO;
      }
      return new FloatNode(converter.apply(timeUnit, value));
    } else if (fieldValue instanceof DoubleNode) {
      double value = ((DoubleNode) fieldValue).doubleValue();
      if (shouldMapToZero.test(value)) {
        return DOUBLE_NODE_ZERO;
      }
      return new DoubleNode(converter.apply(timeUnit, value));
    } else if (fieldValue instanceof LongNode) {
      long value = ((LongNode) fieldValue).longValue();
      if (shouldMapToZero.test(value)) {
        return LONG_NODE_ZERO;
      }
      return new LongNode(converter.apply(timeUnit, value));
    } else if (fieldValue instanceof ShortNode) {
      short value = ((ShortNode) fieldValue).shortValue();
      if (shouldMapToZero.test(value)) {
        return SHORT_NODE_ZERO;
      }
      return new ShortNode((short) converter.apply(timeUnit, value).longValue());
    }
    return fieldValue;
  }
}
