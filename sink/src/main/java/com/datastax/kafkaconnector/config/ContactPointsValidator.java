/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.config;

import com.datastax.oss.driver.shaded.guava.common.net.InetAddresses;
import com.google.common.net.InternetDomainName;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.jetbrains.annotations.NotNull;

public class ContactPointsValidator {

  public static void validateContactPoints(List<String> contactPoints) {
    String errorMsg =
        contactPoints.stream().filter(isCorrectIpAddressOrDns()).collect(Collectors.joining(","));
    if (!errorMsg.isEmpty()) {
      throw new ConfigException(
          String.format("Incorrect %s: %s", DseSinkConfig.CONTACT_POINTS_OPT, errorMsg));
    }
  }

  @NotNull
  private static Predicate<String> isCorrectIpAddressOrDns() {
    return cp -> !(InetAddresses.isInetAddress(cp) || InternetDomainName.isValid(cp));
  }
}
