package com.datastax.kafkaconnector.util;

import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class UUIDUtil {
  private static final UUID KAFKA_CONNECTOR_NAMESPACE =
      // todo how to generate it?
      UUID.fromString("2505c745-cedf-4714-bcab-0d580270ed95");

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
