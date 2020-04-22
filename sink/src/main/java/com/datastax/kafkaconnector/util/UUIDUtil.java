/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.util;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.dsbulk.commons.utils.PlatformUtils;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class UUIDUtil {
  private static final UUID KAFKA_CONNECTOR_NAMESPACE =
      UUID.fromString("bcfd612f-15fd-4b74-af81-95b04d9e756c");

  private static final DateTimeFormatter DEFAULT_TIMESTAMP_PATTERN =
      DateTimeFormatter.ofPattern("uuuuMMdd-HHmmss-SSSSSS");

  public static UUID generateClientId(String instanceName) {
    return Uuids.nameBased(KAFKA_CONNECTOR_NAMESPACE, newExecutionId(instanceName));
  }

  private static String newExecutionId(String name) {
    return name + "_" + DEFAULT_TIMESTAMP_PATTERN.format(PlatformUtils.now());
  }
}
