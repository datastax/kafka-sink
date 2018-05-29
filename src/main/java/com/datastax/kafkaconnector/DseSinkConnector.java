package com.datastax.kafkaconnector;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Very simple connector that works with the console. This connector supports both source and
 * sink modes via its 'mode' setting.
 */
public class DseSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(DseSinkConnector.class);

  private static final String KEYSPACE_OPT = "keyspace";
  private static final String TABLE_OPT = "table";
  private static final String CONTACT_POINTS_OPT = "contactPoints";

  private static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(KEYSPACE_OPT, Type.STRING, null, Importance.HIGH, "Keyspace to which to load messages")
      .define(TABLE_OPT, Type.STRING, null, Importance.HIGH, "Table to which to load messages")
      .define(CONTACT_POINTS_OPT, Type.LIST, Collections.singletonList("127.0.0.1"),
          Importance.HIGH, "Initial DSE node contact points");

  // TODO: Handle multiple clusters, sessions, and prepared statements (one set for
  // each instance of the connector).
  private static AtomicReference<DseCluster> cluster = new AtomicReference<>();
  private static DseSession session;
  private static PreparedStatement statement;

  @Override
  public String version() {
    // TODO: Get this from a property provided by Maven.
    return "1.0";
  }

  @Override
  public void start(Map<String, String> props) {
    // TODO: Verify that sensitive settings don't get logged by AbstractConfig.
    AbstractConfig parsedConfig = new AbstractConfig(CONFIG_DEF, props);
    String keyspace = parsedConfig.getString(KEYSPACE_OPT);
    String table = parsedConfig.getString(TABLE_OPT);
    List<String> contactPoints = parsedConfig.getList(CONTACT_POINTS_OPT);

    try {
      DseCluster.Builder builder = DseCluster.builder().withClusterName("kafka-connector");
      // TODO: Configure the cluster...
      contactPoints.forEach(builder::addContactPoint);
      DseCluster cluster = builder.build();

      // Multiple sink connectors (say for different topics/tables) may be created at the same
      // time. They can all share the same session, but we must make sure only one connector
      // creates the session.
      if (DseSinkConnector.cluster.compareAndSet(null, cluster)) {
        session = cluster.connect();
        statement = session.prepare(String.format("INSERT INTO %s.%s (timestamp, msg) VALUES (?,?)", keyspace, table));
      }
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
    return CONFIG_DEF;
  }

  static DseSession getSession()
  {
    return session;
  }

  static PreparedStatement getStatement()
  {
    return statement;
  }

  private static void closeQuietly(AutoCloseable closeable)
  {
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
}
