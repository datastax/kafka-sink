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

public class JsonNodeTtlConverter {

  private static final BigIntegerNode BIG_INTEGER_NODE_ZERO = new BigIntegerNode(BigInteger.ZERO);
  private static final DecimalNode DECIMAL_NODE_ZERO = new DecimalNode(BigDecimal.ZERO);
  private static final IntNode INT_NODE_ZERO = new IntNode(0);
  private static final FloatNode FLOAT_NODE_ZERO = new FloatNode(0);
  private static final DoubleNode DOUBLE_NODE_ZERO = new DoubleNode(0);
  private static final LongNode LONG_NODE_ZERO = new LongNode(0);
  private static final ShortNode SHORT_NODE_ZERO = new ShortNode((short) 0);

  public static Object transformField(TimeUnit ttlTimeUnit, Object fieldValue) {
    if (fieldValue instanceof BigIntegerNode) {
      long ttl = ((BigIntegerNode) fieldValue).longValue();
      if (ttl <= -1) {
        return BIG_INTEGER_NODE_ZERO;
      }
      return new BigIntegerNode(
          BigInteger.valueOf(TimeUnitConverter.convertTtlToSeconds(ttlTimeUnit, ttl)));
    } else if (fieldValue instanceof DecimalNode) {
      long ttl = ((DecimalNode) fieldValue).longValue();
      if (ttl <= -1) {
        return DECIMAL_NODE_ZERO;
      }
      return new DecimalNode(
          BigDecimal.valueOf(TimeUnitConverter.convertTtlToSeconds(ttlTimeUnit, ttl)));
    } else if (fieldValue instanceof IntNode) {
      int ttl = ((IntNode) fieldValue).intValue();
      if (ttl <= -1) {
        return INT_NODE_ZERO;
      }
      return new IntNode((int) TimeUnitConverter.convertTtlToSeconds(ttlTimeUnit, ttl));
    } else if (fieldValue instanceof FloatNode) {
      float ttl = ((FloatNode) fieldValue).floatValue();
      if (ttl <= -1) {
        return FLOAT_NODE_ZERO;
      }
      return new FloatNode(TimeUnitConverter.convertTtlToSeconds(ttlTimeUnit, ttl));
    } else if (fieldValue instanceof DoubleNode) {
      double ttl = ((DoubleNode) fieldValue).doubleValue();
      if (ttl <= -1) {
        return DOUBLE_NODE_ZERO;
      }
      return new DoubleNode(TimeUnitConverter.convertTtlToSeconds(ttlTimeUnit, ttl));
    } else if (fieldValue instanceof LongNode) {
      long ttl = ((LongNode) fieldValue).longValue();
      if (ttl <= -1) {
        return LONG_NODE_ZERO;
      }
      return new LongNode(TimeUnitConverter.convertTtlToSeconds(ttlTimeUnit, ttl));
    } else if (fieldValue instanceof ShortNode) {
      short ttl = ((ShortNode) fieldValue).shortValue();
      if (ttl <= -1) {
        return SHORT_NODE_ZERO;
      }
      return new ShortNode((short) TimeUnitConverter.convertTtlToSeconds(ttlTimeUnit, ttl));
    }
    return fieldValue;
  }
}
