/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class DseSinkConfig extends AbstractConfig {
  private static final String KEYSPACE_OPT = "keyspace";
  private static final String TABLE_OPT = "table";
  private static final String CONTACT_POINTS_OPT = "contactPoints";
  private static final String PORT_OPT = "port";
  private static final String DC_OPT = "loadBalancing.localDc";

  static ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              KEYSPACE_OPT,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.HIGH,
              "Keyspace to which to load messages")
          .define(
              TABLE_OPT,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.HIGH,
              "Table to which to load messages")
          .define(
              CONTACT_POINTS_OPT,
              ConfigDef.Type.LIST,
              Collections.singletonList("127.0.0.1"),
              ConfigDef.Importance.HIGH,
              "Initial DSE node contact points")
          .define(
              PORT_OPT,
              ConfigDef.Type.INT,
              9042,
              ConfigDef.Importance.HIGH,
              "Port to connect to on DSE nodes")
          .define(
              DC_OPT,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.HIGH,
              "The datacenter name (commonly dc1, dc2, etc.) local to the machine on which the connector is running.");

  private final String keyspace;
  private final String table;
  private final int port;
  private final List<String> contactPoints;
  private final String localDc;

  DseSinkConfig(final Map<?, ?> settings) {
    super(CONFIG_DEF, settings, false);

    keyspace = getString(KEYSPACE_OPT);
    table = getString(TABLE_OPT);
    port = getInt(PORT_OPT);
    contactPoints = getList(CONTACT_POINTS_OPT);
    localDc = getString(DC_OPT);
  }

  String getKeyspace() {
    return keyspace;
  }

  String getTable() {
    return table;
  }

  int getPort() {
    return port;
  }

  List<String> getContactPoints() {
    return contactPoints;
  }

  String getLocalDc() {
    return localDc;
  }

  @Override
  public String toString() {
    return String.format(
        "Configuration options:%n"
            + "        keyspace = %s%n"
            + "        table = %s%n"
            + "        contactPoints = %s%n"
            + "        port = %d%n"
            + "        localDc = %s%n",
        keyspace, table, contactPoints, port, localDc);
  }
}
