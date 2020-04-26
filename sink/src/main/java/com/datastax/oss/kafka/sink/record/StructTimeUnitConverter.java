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
package com.datastax.oss.kafka.sink.record;

import com.datastax.oss.kafka.sink.util.TimeUnitConverter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class StructTimeUnitConverter {
  private static final Predicate<Number> SHOULD_MAP_TO_ZERO_PREDICATE =
      value -> value.longValue() <= -1;
  private static final Predicate<Number> ALWAYS_FALSE_PREDICATE = value -> false;

  public static Object transformTtlField(TimeUnit ttlTimeUnit, Number fieldValue) {
    long resultInSeconds = TimeUnitConverter.convertToSeconds(ttlTimeUnit, fieldValue);
    return transformField(resultInSeconds, fieldValue, SHOULD_MAP_TO_ZERO_PREDICATE);
  }

  public static Object transformTimestampField(TimeUnit timestampTimeUnit, Number fieldValue) {
    long resultInMicroseconds =
        TimeUnitConverter.convertToMicroseconds(timestampTimeUnit, fieldValue);
    return transformField(resultInMicroseconds, fieldValue, ALWAYS_FALSE_PREDICATE);
  }

  private static Number transformField(
      long resultInTimeUnit, Number fieldValue, Predicate<Number> shouldMapToZero) {
    if (fieldValue instanceof Integer) {
      if (shouldMapToZero.test(resultInTimeUnit)) {
        return 0;
      }
      return (int) resultInTimeUnit;
    } else if (fieldValue instanceof Double) {
      if (shouldMapToZero.test(resultInTimeUnit)) {
        return 0D;
      }
      return (double) resultInTimeUnit;
    } else if (fieldValue instanceof Float) {
      if (shouldMapToZero.test(resultInTimeUnit)) {
        return 0F;
      }
      return (float) resultInTimeUnit;
    } else if (fieldValue instanceof Short) {
      if (shouldMapToZero.test(resultInTimeUnit)) {
        return (short) 0;
      }
      return (short) resultInTimeUnit;
    } else if (fieldValue instanceof Byte) {
      if (shouldMapToZero.test(resultInTimeUnit)) {
        return (byte) 0;
      }
      return (byte) resultInTimeUnit;
    } else if (fieldValue instanceof Long) {
      if (shouldMapToZero.test(resultInTimeUnit)) {
        return 0L;
      }
      return resultInTimeUnit;
    } else if (fieldValue instanceof BigDecimal) {
      if (shouldMapToZero.test(resultInTimeUnit)) {
        return BigDecimal.ZERO;
      }
      return BigDecimal.valueOf(resultInTimeUnit);
    } else if (fieldValue instanceof BigInteger) {
      if (shouldMapToZero.test(resultInTimeUnit)) {
        return BigInteger.ZERO;
      }
      return BigInteger.valueOf(resultInTimeUnit);
    }

    return resultInTimeUnit;
  }
}
