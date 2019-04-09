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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

public class StructTtlConverter {
  public static Number transformField(TimeUnit ttlTimeUnit, Number fieldValue) {
    long resultInSeconds = TimeUnitConverter.convertTtlToSeconds(ttlTimeUnit, fieldValue);
    if (fieldValue instanceof Integer) {
      if (resultInSeconds <= -1) {
        return 0;
      }
      return (int) resultInSeconds;
    } else if (fieldValue instanceof Double) {
      if (resultInSeconds <= -1) {
        return 0D;
      }
      return (double) resultInSeconds;
    } else if (fieldValue instanceof Float) {
      if (resultInSeconds <= -1) {
        return 0F;
      }
      return (float) resultInSeconds;
    } else if (fieldValue instanceof Short) {
      if (resultInSeconds <= -1) {
        return (short) 0;
      }
      return (short) resultInSeconds;
    } else if (fieldValue instanceof Byte) {
      if (resultInSeconds <= -1) {
        return (byte) 0;
      }
      return (byte) resultInSeconds;
    } else if (fieldValue instanceof Long) {
      if (resultInSeconds <= -1) {
        return 0L;
      }
      return resultInSeconds;
    } else if (fieldValue instanceof BigDecimal) {
      if (resultInSeconds <= -1) {
        return BigDecimal.ZERO;
      }
      return BigDecimal.valueOf(resultInSeconds);
    } else if (fieldValue instanceof BigInteger) {
      if (resultInSeconds <= -1) {
        return BigInteger.ZERO;
      }
      return BigInteger.valueOf(resultInSeconds);
    }

    return resultInSeconds;
  }
}
