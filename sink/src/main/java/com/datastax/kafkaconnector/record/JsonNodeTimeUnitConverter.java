/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.record;

import com.datastax.kafkaconnector.util.TimeUnitConverter;
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
        ttlTimeUnit,
        fieldValue,
        SHOULD_MAP_TO_ZERO_PREDICATE,
        TimeUnitConverter::convertTtlToSeconds);
  }

  public static Object transformTimestampField(TimeUnit timestampTimeUnit, Object fieldValue) {
    return transformField(
        timestampTimeUnit,
        fieldValue,
        ALWAYS_FALSE_PREDICATE,
        TimeUnitConverter::convertTtlToMicroseconds);
  }

  private static Object transformField(
      TimeUnit ttlTimeUnit,
      Object fieldValue,
      Predicate<Number> shouldMapToZero,
      BiFunction<TimeUnit, Number, Long> converter) {
    if (fieldValue instanceof BigIntegerNode) {
      long ttl = ((BigIntegerNode) fieldValue).longValue();
      if (shouldMapToZero.test(ttl)) {
        return BIG_INTEGER_NODE_ZERO;
      }
      return new BigIntegerNode(BigInteger.valueOf(converter.apply(ttlTimeUnit, ttl)));
    } else if (fieldValue instanceof DecimalNode) {
      long ttl = ((DecimalNode) fieldValue).longValue();
      if (shouldMapToZero.test(ttl)) {
        return DECIMAL_NODE_ZERO;
      }
      return new DecimalNode(BigDecimal.valueOf(converter.apply(ttlTimeUnit, ttl)));
    } else if (fieldValue instanceof IntNode) {
      int ttl = ((IntNode) fieldValue).intValue();
      if (shouldMapToZero.test(ttl)) {
        return INT_NODE_ZERO;
      }
      return new IntNode((int) converter.apply(ttlTimeUnit, ttl).longValue());
    } else if (fieldValue instanceof FloatNode) {
      float ttl = ((FloatNode) fieldValue).floatValue();
      if (shouldMapToZero.test(ttl)) {
        return FLOAT_NODE_ZERO;
      }
      return new FloatNode(converter.apply(ttlTimeUnit, ttl));
    } else if (fieldValue instanceof DoubleNode) {
      double ttl = ((DoubleNode) fieldValue).doubleValue();
      if (shouldMapToZero.test(ttl)) {
        return DOUBLE_NODE_ZERO;
      }
      return new DoubleNode(converter.apply(ttlTimeUnit, ttl));
    } else if (fieldValue instanceof LongNode) {
      long ttl = ((LongNode) fieldValue).longValue();
      if (shouldMapToZero.test(ttl)) {
        return LONG_NODE_ZERO;
      }
      return new LongNode(converter.apply(ttlTimeUnit, ttl));
    } else if (fieldValue instanceof ShortNode) {
      short ttl = ((ShortNode) fieldValue).shortValue();
      if (shouldMapToZero.test(ttl)) {
        return SHORT_NODE_ZERO;
      }
      return new ShortNode((short) converter.apply(ttlTimeUnit, ttl).longValue());
    }
    return fieldValue;
  }
}
