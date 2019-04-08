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
  public static Object transformField(TimeUnit ttlTimeUnit, Object fieldValue) {
    if (fieldValue instanceof BigIntegerNode) {
      return new BigIntegerNode(
          BigInteger.valueOf(
              TimeUnitConverter.convertTtlToSeconds(
                  ttlTimeUnit, ((BigIntegerNode) fieldValue).longValue())));
    } else if (fieldValue instanceof DecimalNode) {
      return new DecimalNode(
          BigDecimal.valueOf(
              TimeUnitConverter.convertTtlToSeconds(
                  ttlTimeUnit, ((DecimalNode) fieldValue).longValue())));
    } else if (fieldValue instanceof IntNode) {
      return new IntNode(
          (int)
              TimeUnitConverter.convertTtlToSeconds(
                  ttlTimeUnit, ((IntNode) fieldValue).intValue()));
    } else if (fieldValue instanceof FloatNode) {
      return new FloatNode(
          TimeUnitConverter.convertTtlToSeconds(
              ttlTimeUnit, ((FloatNode) fieldValue).floatValue()));
    } else if (fieldValue instanceof DoubleNode) {
      return new DoubleNode(
          TimeUnitConverter.convertTtlToSeconds(
              ttlTimeUnit, ((DoubleNode) fieldValue).doubleValue()));
    } else if (fieldValue instanceof LongNode) {
      return new LongNode(
          TimeUnitConverter.convertTtlToSeconds(ttlTimeUnit, ((LongNode) fieldValue).longValue()));
    } else if (fieldValue instanceof ShortNode) {
      return new ShortNode(
          (short)
              TimeUnitConverter.convertTtlToSeconds(
                  ttlTimeUnit, ((ShortNode) fieldValue).shortValue()));
    }
    return fieldValue;
  }
}
