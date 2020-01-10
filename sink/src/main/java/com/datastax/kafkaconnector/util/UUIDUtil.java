/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.util;

import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class UUIDUtil {
  private static final UUID KAFKA_CONNECTOR_NAMESPACE =
      UUID.fromString("bcfd612f-15fd-4b74-af81-95b04d9e756c");

  private static final DateTimeFormatter DEFAULT_TIMESTAMP_PATTERN =
      DateTimeFormatter.ofPattern("uuuuMMdd-HHmmss-SSSSSS");

  public static UUID generateClientId(String instanceName) {
    return clientId(newExecutionId(instanceName));
  }

  private static String newExecutionId(String name) {
    return name + "_" + DEFAULT_TIMESTAMP_PATTERN.format(PlatformUtils.now());
  }

  private static UUID clientId(String executionId) {
    byte[] executionIdBytes = executionId.getBytes(StandardCharsets.UTF_8);
    byte[] concat = new byte[16 + executionIdBytes.length];
    System.arraycopy(
        ByteBuffer.allocate(Long.BYTES)
            .putLong(KAFKA_CONNECTOR_NAMESPACE.getMostSignificantBits())
            .array(),
        0,
        concat,
        0,
        8);
    System.arraycopy(
        ByteBuffer.allocate(Long.BYTES)
            .putLong(KAFKA_CONNECTOR_NAMESPACE.getLeastSignificantBits())
            .array(),
        0,
        concat,
        8,
        8);
    System.arraycopy(executionIdBytes, 0, concat, 16, executionIdBytes.length);
    return UUID.nameUUIDFromBytes(concat);
  }
}
