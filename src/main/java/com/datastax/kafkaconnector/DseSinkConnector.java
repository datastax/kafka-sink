/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static com.datastax.kafkaconnector.DseSinkConfig.KEYSPACE_OPT;
import static com.datastax.kafkaconnector.DseSinkConfig.TABLE_OPT;

import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.DseSessionBuilder;
import com.datastax.kafkaconnector.util.StringUtil;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.util.Strings;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sink connector to insert Kafka records into DSE. */
public class DseSinkConnector extends SinkConnector {
  static final String MAPPING_OPT = "mapping";
  private static final Logger log = LoggerFactory.getLogger(DseSinkConnector.class);
  // TODO: Handle multiple clusters, sessions, and prepared statements (one set for
  // each instance of the connector).
  private static CountDownLatch statementReady = new CountDownLatch(1);
  private static DseSession session;
  private static PreparedStatement statement;
  private String version;

  private DseSinkConfig config;

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
   * This isn't used in the connector really; it's primarily useful for trying things out and
   * debugging logic in IntelliJ.
   */
  public static void main(String[] args) throws IOException {
    DseSinkConnector conn = new DseSinkConnector();
    // Table: create table types (bigintCol bigint PRIMARY KEY, booleanCol boolean,
    // doubleCol double, floatCol float, intCol int, smallintCol smallint, textCol text,
    // tinyIntCol tinyint);
    String mappingString =
        "bigintcol=value.bigint, booleancol=value.boolean, doublecol=value.double, floatcol=value.float, "
            + "intcol=value.int, smallintcol=value.smallint, textcol=value.text, tinyintcol=value.tinyint";
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put("mapping", mappingString)
            .put("keyspace", "simplex")
            .put("table", "caseSensitive")
            .build();
    try {
      conn.start(props);

      //    // Create a record (emulating what the sink will do)
      //    Schema schema =
      //        SchemaBuilder.struct()
      //            .name("Kafka")
      //            .field("bigint", Schema.INT64_SCHEMA)
      //            .field("boolean", Schema.BOOLEAN_SCHEMA)
      //            .field("double", Schema.FLOAT64_SCHEMA)
      //            .field("float", Schema.FLOAT32_SCHEMA)
      //            .field("int", Schema.INT32_SCHEMA)
      //            .field("smallint", Schema.INT16_SCHEMA)
      //            .field("text", Schema.STRING_SCHEMA)
      //            .field("tinyint", Schema.INT8_SCHEMA);
      //    Long baseValue = 98761234L;
      //    Struct value =
      //        new Struct(schema)
      //            .put("bigint", baseValue)
      //            .put("boolean", (baseValue.intValue() & 1) == 1)
      //            .put("double", (double) baseValue + 0.123)
      //            .put("float", baseValue.floatValue() + 0.987f)
      //            .put("int", baseValue.intValue())
      //            .put("smallint", baseValue.shortValue())
      //            .put("text", baseValue.toString())
      //            .put("tinyint", baseValue.byteValue());
      //    SinkRecord record = new SinkRecord("mytopic", 0, null, null, schema, value, 1234L);
      //
      //    BoundStatement boundStatement = getStatement().bind();
      //    CodecRegistry codecRegistry = session.getContext().codecRegistry();
      //
      //    ProtocolVersion protocolVersion = session.getContext().protocolVersion();
      //    for (Map.Entry<String, String> entry : mapping.entrySet()) {
      //      String colName = entry.getKey();
      //      String recordFieldFullName = entry.getValue();
      //      String[] recordFieldNameParts = recordFieldFullName.split("\\.", 2);
      //      Object component;
      //      if ("key".equals(recordFieldNameParts[0])) {
      //        component = record.key();
      //      } else if ("value".equals(recordFieldNameParts[0])) {
      //        component = record.value();
      //      } else {
      //        throw new RuntimeException(
      //            String.format("Unrecognized record component: %s", recordFieldNameParts[0]));
      //      }
      //
      //      // TODO: Handle straight-up 'key' or 'value'.
      //
      //      // TODO: What if record field doesn't exist in schema or object?
      //      String fieldName = recordFieldNameParts[1];
      //
      //      Struct componentStruct = (Struct) component;
      //      Object fieldValue = componentStruct.get(fieldName);
      //      DataType columnType = getStatement().getVariableDefinitions().get(colName).getType();
      //
      //      ByteBuffer bb = codecRegistry.codecFor(columnType).encode(fieldValue,
      // protocolVersion);
      //      boundStatement = boundStatement.setBytesUnsafe(colName, bb);
      //    }
      //        getSession().execute(boundStatement);
    } finally {
      closeQuietly(session);
    }
  }

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
    config = new DseSinkConfig(props);
    log.info(config.toString());
    DseSessionBuilder builder = DseSession.builder();
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
                  config.getLocalDc().isEmpty()
                      ? ""
                      : String.format(
                          "basic.load-balancing-policy.local-datacenter=\"%s\"",
                          config.getLocalDc());
              return ConfigFactory.parseString(overrides).withFallback(dseConfig);
            });

    Config dsbulkConfig = ConfigFactory.load().getConfig("dsbulk");
    CodecSettings codecSettings =
        new CodecSettings(new DefaultLoaderConfig(dsbulkConfig.getConfig("codecSettings")));
    codecSettings.init();
    ExtendedCodecRegistry registry =
        codecSettings.createCodecRegistry(session.getContext().codecRegistry());
    session = builder.withConfigLoader(configLoader).build();

    validateKeyspaceAndTable(session, config);
    validateMappingColumns(session, config);

    // TODO: Multiple sink connectors (say for different topics/tables) may be created at the same
    // time. They can all share the same session, but we must make sure only one connector
    // creates the session.
    statement = session.prepare(makeInsertStatement(config));
    statementReady.countDown();
  }

  static void validateKeyspaceAndTable(DseSession session, DseSinkConfig config) {
    String keyspaceName = config.getKeyspace();
    String tableName = config.getTable();
    Metadata metadata = session.getMetadata();
    KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName);
    if (keyspace == null) {
      String lowerCaseKeyspaceName = Strings.unDoubleQuote(keyspaceName).toLowerCase();
      if (metadata.getKeyspace(lowerCaseKeyspaceName) != null) {
        throw new ConfigException(
            KEYSPACE_OPT,
            keyspaceName,
            String.format(
                "Keyspace does not exist, however a keyspace %s was found. Update the config to use %s if desired.",
                lowerCaseKeyspaceName, lowerCaseKeyspaceName));
      } else {
        throw new ConfigException(KEYSPACE_OPT, keyspaceName, "Not found");
      }
    }
    TableMetadata table = keyspace.getTable(tableName);
    if (table == null) {
      String lowerCaseTableName = Strings.unDoubleQuote(tableName).toLowerCase();
      if (keyspace.getTable(lowerCaseTableName) != null) {
        throw new ConfigException(
            TABLE_OPT,
            tableName,
            String.format(
                "Table does not exist, however a table %s was found. Update the config to use %s if desired.",
                lowerCaseTableName, lowerCaseTableName));
      } else {
        throw new ConfigException(TABLE_OPT, tableName, "Not found");
      }
    }
  }

  static void validateMappingColumns(DseSession session, DseSinkConfig config) {
    String keyspaceName = config.getKeyspace();
    String tableName = config.getTable();
    Metadata metadata = session.getMetadata();
    KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName);
    assert keyspace != null;
    TableMetadata table = keyspace.getTable(tableName);
    assert table != null;

    Map<String, String> mapping = config.getMapping();

    // The columns in the mapping are the keys. Check that each exists in the table.
    String nonExistentCols =
        mapping
            .keySet()
            .stream()
            .filter(col -> table.getColumn(Strings.unDoubleQuote(col)) == null)
            .collect(Collectors.joining(", "));
    if (!StringUtil.isEmpty(nonExistentCols)) {
      throw new ConfigException(
          MAPPING_OPT,
          config.getMappingString(),
          String.format(
              "The following columns do not exist in table %s: %s", tableName, nonExistentCols));
    }

    // Now verify that each column that makes up the partition-key in the table has a reference
    // in the mapping.
    String nonExistentKeyCols =
        table
            .getPartitionKey()
            .stream()
            .filter(col -> !mapping.containsKey(col.getName().toString()))
            .map(col -> col.getName().toString())
            .collect(Collectors.joining(", "));
    if (!StringUtil.isEmpty(nonExistentKeyCols)) {
      throw new ConfigException(
          MAPPING_OPT,
          config.getMappingString(),
          String.format(
              "The following columns are part of the partition key but are not mapped: %s",
              nonExistentKeyCols));
    }
  }

  static String makeInsertStatement(DseSinkConfig config) {
    Map<String, String> mapping = config.getMapping();
    StringBuilder statementBuilder = new StringBuilder("INSERT INTO ");
    statementBuilder.append(config.getKeyspace()).append('.').append(config.getTable()).append('(');

    // Add the column names, which are the keys in the mapping. As we do so, collect the
    // bind variable names (e.g. :col) in a buffer (to achieve consistent order).
    StringBuilder valuesBuilder = new StringBuilder();
    boolean isFirst = true;
    for (String col : mapping.keySet()) {
      if (!isFirst) {
        statementBuilder.append(',');
        valuesBuilder.append(',');
      }
      isFirst = false;
      statementBuilder.append(col);
      valuesBuilder.append(':').append(col);
    }
    statementBuilder.append(") VALUES (");
    statementBuilder.append(valuesBuilder.toString());
    statementBuilder.append(')');

    return statementBuilder.toString();
  }

  @Override
  public Class<? extends Task> taskClass() {
    return DseSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    Map<String, String> taskConfig =
        ImmutableMap.<String, String>builder().put(MAPPING_OPT, config.getMappingString()).build();
    for (int i = 0; i < maxTasks; i++) {
      configs.add(taskConfig);
    }
    return configs;
  }

  @Override
  public void stop() {
    // TODO: When is it safe to close the (shared) session?
    closeQuietly(session);
  }

  @Override
  public ConfigDef config() {
    return DseSinkConfig.CONFIG_DEF;
  }
}
