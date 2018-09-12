/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.config;

import static com.datastax.kafkaconnector.util.SinkUtil.NAME_OPT;

import com.datastax.kafkaconnector.util.StringUtil;
import com.google.common.base.Splitter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/** Connector configuration and validation. */
@SuppressWarnings("WeakerAccess")
public class DseSinkConfig {
  static final String CONTACT_POINTS_OPT = "contactPoints";
  static final String PORT_OPT = "port";
  static final String DC_OPT = "loadBalancing.localDc";
  static final String CONCURRENT_REQUESTS_OPT = "maxConcurrentRequests";
  public static final ConfigDef GLOBAL_CONFIG_DEF =
      new ConfigDef()
          .define(
              CONTACT_POINTS_OPT,
              ConfigDef.Type.LIST,
              Collections.EMPTY_LIST,
              ConfigDef.Importance.HIGH,
              "Initial DSE node contact points")
          .define(
              PORT_OPT,
              ConfigDef.Type.INT,
              9042,
              ConfigDef.Range.atLeast(1),
              ConfigDef.Importance.HIGH,
              "Port to connect to on DSE nodes")
          .define(
              DC_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "The datacenter name (commonly dc1, dc2, etc.) local to the machine on which the connector is running")
          .define(
              CONCURRENT_REQUESTS_OPT,
              ConfigDef.Type.INT,
              500,
              ConfigDef.Range.atLeast(1),
              ConfigDef.Importance.HIGH,
              "The maximum number of requests to send to DSE at once");

  private static final Pattern TOPIC_PAT = Pattern.compile("topic\\.([^.]+)");
  private final String instanceName;
  private final AbstractConfig globalConfig;
  private final Map<String, TopicConfig> topicConfigs;
  private final SslConfig sslConfig;
  private final AuthenticatorConfig authConfig;

  public DseSinkConfig(Map<String, String> settings) {
    instanceName = settings.get(NAME_OPT);
    // Walk through the settings and separate out "globals" from "topics", "ssl", and "auth".
    Map<String, String> globalSettings = new HashMap<>();
    Map<String, String> sslSettings = new HashMap<>();
    Map<String, String> authSettings = new HashMap<>();
    Map<String, Map<String, String>> topicSettings = new HashMap<>();
    for (Map.Entry<String, String> entry : settings.entrySet()) {
      String name = entry.getKey();
      if (name.startsWith("topic.")) {
        Matcher m = TOPIC_PAT.matcher(name);
        //noinspection ResultOfMethodCallIgnored
        m.lookingAt();
        String topicName = m.group(1);
        Map<String, String> topicMap =
            topicSettings.computeIfAbsent(topicName, t -> new HashMap<>());
        topicMap.put(name, entry.getValue());
      } else if (name.startsWith("ssl.")) {
        sslSettings.put(name, entry.getValue());
      } else if (name.startsWith("auth.")) {
        authSettings.put(name, entry.getValue());
      } else {
        globalSettings.put(name, entry.getValue());
      }
    }

    // Put the global settings in an AbstractConfig and make/store a TopicConfig for every
    // topic settings map.
    globalConfig = new AbstractConfig(GLOBAL_CONFIG_DEF, globalSettings, false);
    sslConfig = new SslConfig(sslSettings);
    authConfig = new AuthenticatorConfig(authSettings);
    topicConfigs = new HashMap<>();
    topicSettings.forEach(
        (name, topicConfigMap) -> topicConfigs.put(name, new TopicConfig(name, topicConfigMap)));

    // Verify that we have a topic section for every topic we're subscribing to, if 'topics'
    // was provided. A user may use topics.regex to subscribe by pattern, in which case,
    // they're on their own.
    String topicsString = globalSettings.get("topics");
    if (topicsString != null) {
      List<String> topics = Splitter.on(",").splitToList(topicsString);
      for (String topic : topics) {
        if (!topicConfigs.containsKey(topic)) {
          throw new ConfigException(
              "topics",
              topicsString,
              String.format("Missing topic settings (topic.%s.*) for topic %s", topic, topic));
        }
      }
    }

    // Verify that if contact-points are provided, local dc is also specified.
    List<String> contactPoints = getContactPoints();
    if (!contactPoints.isEmpty() && StringUtil.isEmpty(getLocalDc())) {
      throw new ConfigException(
          CONTACT_POINTS_OPT,
          contactPoints,
          String.format("When contact points is provided, %s must also be specified", DC_OPT));
    }
  }

  public String getInstanceName() {
    return instanceName;
  }

  public int getPort() {
    return globalConfig.getInt(PORT_OPT);
  }

  public int getMaxConcurrentRequests() {
    return globalConfig.getInt(CONCURRENT_REQUESTS_OPT);
  }

  public List<String> getContactPoints() {
    return globalConfig.getList(CONTACT_POINTS_OPT);
  }

  public String getLocalDc() {
    return globalConfig.getString(DC_OPT);
  }

  public Map<String, TopicConfig> getTopicConfigs() {
    return topicConfigs;
  }

  public AuthenticatorConfig getAuthenticatorConfig() {
    return authConfig;
  }

  public SslConfig getSslConfig() {
    return sslConfig;
  }

  @Override
  public String toString() {
    return String.format(
        "Global configuration:%n"
            + "        contactPoints: %s%n"
            + "        port: %d%n"
            + "        localDc: %s%n"
            + "        maxConcurrentRequests: %d%n"
            + "SSL configuration:%n%s%n"
            + "Authentication configuration:%n%s%n"
            + "Topic configurations:%n%s",
        getContactPoints(),
        getPort(),
        getLocalDc(),
        getMaxConcurrentRequests(),
        Splitter.on("\n")
            .splitToList(sslConfig.toString())
            .stream()
            .map(line -> "        " + line)
            .collect(Collectors.joining("\n")),
        Splitter.on("\n")
            .splitToList(authConfig.toString())
            .stream()
            .map(line -> "        " + line)
            .collect(Collectors.joining("\n")),
        topicConfigs
            .values()
            .stream()
            .map(
                t ->
                    Splitter.on("\n")
                        .splitToList(t.toString())
                        .stream()
                        .map(line -> "        " + line)
                        .collect(Collectors.joining("\n")))
            .collect(Collectors.joining("\n")));
  }
}
