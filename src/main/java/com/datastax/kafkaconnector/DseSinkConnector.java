/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.DseSessionBuilder;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sink connector to insert Kafka records into DSE. */
public class DseSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(DseSinkConnector.class);

  // TODO: Handle multiple clusters, sessions, and prepared statements (one set for
  // each instance of the connector).
  private static CountDownLatch statementReady = new CountDownLatch(1);
  private static DseSession session;
  private static PreparedStatement statement;
  private static String version;

  @Override
  public String version() {
    if (version != null) {
      return version;
    }
    synchronized (this) {
      if (version != null) {
        return version;
      }

      // Get the version from version.txt.
      version = "UNKNOWN";
      try (InputStream versionStream = DseSinkConnector.class.getResourceAsStream("/version.txt")) {
        if (versionStream != null) {
          BufferedReader reader =
              new BufferedReader(new InputStreamReader(versionStream, StandardCharsets.UTF_8));
          version = reader.readLine();
        }
      } catch (Exception e) {
        // swallow
      }
      return version;
    }
  }

  @Override
  public void start(Map<String, String> props) {
    DseSinkConfig config = new DseSinkConfig(props);
    log.info(config.toString());
    try {
      DseSessionBuilder builder = DseSession.builder();
      // TODO: Configure the cluster...
      config
          .getContactPoints()
          .forEach(
              hostStr -> builder.addContactPoint(new InetSocketAddress(hostStr, config.getPort())));
      DriverConfigLoader configLoader =
          new DefaultDriverConfigLoader(
              () -> {
                ConfigFactory.invalidateCaches();
                Config dseConfig = ConfigFactory.load().getConfig("datastax-dse-java-driver");

                String overrides =
                    String.format(
                        "basic.load-balancing-policy.local-datacenter=\"%s\"", config.getLocalDc());
                return ConfigFactory.parseString(overrides).withFallback(dseConfig);
              },
              DefaultDriverOption.values(),
              DseDriverOption.values());
      session = builder.withConfigLoader(configLoader).build();

      // TODO: Multiple sink connectors (say for different topics/tables) may be created at the same
      // time. They can all share the same session, but we must make sure only one connector
      // creates the session.
      statement =
          session.prepare(
              String.format(
                  "INSERT INTO %s.%s (timestamp, f1, f2) VALUES (?,?,?)",
                  config.getKeyspace(), config.getTable()));
      statementReady.countDown();
    } catch (RuntimeException e) {
      throw new ConnectException("Couldn't connect to DSE", e);
    }
  }

  @Override
  public Class<? extends Task> taskClass() {
    return DseSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      Map<String, String> config = new HashMap<>();
      // TODO: Pass the field mapping info to the task.
      configs.add(config);
    }
    return configs;
  }

  @Override
  public void stop() {
    // TODO: When is it safe to close the (shared) session and cluster?
    //    closeQuietly(session);
    //    closeQuietly(cluster);
  }

  @Override
  public ConfigDef config() {
    return DseSinkConfig.CONFIG_DEF;
  }

  static DseSession getSession() {
    if (session != null) {
      return session;
    }

    try {
      statementReady.await();
    } catch (InterruptedException e) {
      log.error("getSession got excp", e);
    }
    return session;
  }

  static PreparedStatement getStatement() {
    if (statement != null) {
      return statement;
    }

    try {
      statementReady.await();
    } catch (InterruptedException e) {
      log.error("getStatement got excp", e);
    }
    return statement;
  }

  private static void closeQuietly(AutoCloseable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        log.debug(String.format("Failed to close %s", closeable), e);
      }
    }
  }

  /**
   * This isn't used in the connector really; it's primarily useful for debugging logic in IntelliJ.
   */
  public static void main(String[] args) {
    DseSessionBuilder builder = DseSession.builder();
    session = builder.build();

    statement =
        session.prepare(
            String.format(
                "INSERT INTO %s.%s (timestamp, f1, f2) VALUES (?,?,?)", "simplex", "kafka2"));
    BoundStatement bound = statement.bind(987651234L, 424, 848);
    session.execute(bound);
    session.close();
  }
}
