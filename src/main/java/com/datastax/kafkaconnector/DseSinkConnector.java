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
import static com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS;

import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.DseSessionBuilder;
import com.datastax.kafkaconnector.util.StringUtil;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sink connector to insert Kafka records into DSE. */
public class DseSinkConnector extends SinkConnector {
  static final String MAPPING_OPT = "mapping";
  static final RecordMetadata JSON_RECORD_METADATA =
      (field, cqlType) -> GenericType.of(JsonNode.class);
  static final ObjectMapper objectMapper = new ObjectMapper();
  static final JavaType jsonNodeMapType =
      objectMapper.constructType(new TypeReference<Map<String, JsonNode>>() {}.getType());
  private static final Logger log = LoggerFactory.getLogger(DseSinkConnector.class);

  // TODO: Handle multiple clusters, sessions, and prepared statements (one set for
  // each instance of the connector).
  private static CountDownLatch statementReady = new CountDownLatch(1);
  private static SessionState sessionState;
  private String version;

  private DseSinkConfig config;

  static SessionState getSessionState() {
    if (sessionState != null) {
      return sessionState;
    }

    try {
      statementReady.await();
    } catch (InterruptedException e) {
      log.error("getSessionState got excp", e);
    }
    return sessionState;
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
  public static void main(String[] args) {
    DseSinkConnector conn = new DseSinkConnector();
    // Table: create table types (bigintCol bigint PRIMARY KEY, booleanCol boolean,
    // doubleCol double, floatCol float, intCol int, smallintCol smallint, textCol text,
    // tinyIntCol tinyint);

    String mappingString =
        "bigintcol=key.text, booleancol=value.boolean, doublecol=key.double, floatcol=value.float, "
            + "intcol=key.int, smallintcol=value.smallint, textcol=key.text, tinyintcol=value.tinyint";
    //    String mappingString = "f1=value.f1, f2=value.f2";
    Map<String, String> props =
        ImmutableMap.<String, String>builder()
            .put("mapping", mappingString)
            .put("keyspace", "simplex")
            //            .put("table", "mapping")
            .put("table", "types")
            .build();
    try {
      conn.start(props);

      // Create a record (emulating what the sink will do)

      // STRUCT
      Schema schema =
          SchemaBuilder.struct()
              .name("Kafka")
              .field("bigint", Schema.INT64_SCHEMA)
              .field("boolean", Schema.BOOLEAN_SCHEMA)
              .field("double", Schema.FLOAT64_SCHEMA)
              .field("float", Schema.FLOAT32_SCHEMA)
              .field("int", Schema.INT32_SCHEMA)
              .field("smallint", Schema.INT16_SCHEMA)
              .field("text", Schema.STRING_SCHEMA)
              .field("tinyint", Schema.INT8_SCHEMA);
      Long baseValue = 98761234L;
      Struct value =
          new Struct(schema)
              .put("bigint", baseValue)
              .put("boolean", (baseValue.intValue() & 1) == 1)
              .put("double", (double) baseValue + 0.123)
              .put("float", baseValue.floatValue() + 0.987f)
              .put("int", baseValue.intValue())
              .put("smallint", baseValue.shortValue())
              .put("text", baseValue.toString())
              .put("tinyint", baseValue.byteValue());
      baseValue = 1234567L;
      String jsonValue =
          String.format(
              "{\"bigint\": %d, \"boolean\": false, \"double\": %f, \"float\": %f, \"int\": %d, \"smallint\": %d, \"text\": \"%s\", \"tinyint\": %d}",
              baseValue,
              (double) baseValue + 0.123,
              baseValue.floatValue() + 0.987f,
              baseValue.intValue(),
              baseValue.shortValue(),
              baseValue.toString(),
              baseValue.byteValue());

      // JSON
      //      Schema schema = null;
      //      String value = "{\"f1\": 42, \"f2\": {\"sub1\": 37, \"sub2\": 96}}";

      SinkRecord record = new SinkRecord("mytopic", 0, null, jsonValue, null, value, 1234L);
      DseSinkTask task = new DseSinkTask();
      task.start(props);
      task.put(Collections.singletonList(record));
    } finally {
      closeQuietly(sessionState.getSession());
    }
  }

  static void validateKeyspaceAndTable(DseSession session, DseSinkConfig config) {
    CqlIdentifier keyspaceName = config.getKeyspace();
    CqlIdentifier tableName = config.getTable();
    Metadata metadata = session.getMetadata();
    KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName);
    if (keyspace == null) {
      String lowerCaseKeyspaceName = keyspaceName.asInternal().toLowerCase();
      if (metadata.getKeyspace(lowerCaseKeyspaceName) != null) {
        throw new ConfigException(
            KEYSPACE_OPT,
            keyspaceName,
            String.format(
                "Keyspace does not exist, however a keyspace %s was found. Update the config to use %s if desired.",
                lowerCaseKeyspaceName, lowerCaseKeyspaceName));
      } else {
        throw new ConfigException(KEYSPACE_OPT, keyspaceName.asCql(true), "Not found");
      }
    }
    TableMetadata table = keyspace.getTable(tableName);
    if (table == null) {
      String lowerCaseTableName = tableName.asInternal().toLowerCase();
      if (keyspace.getTable(lowerCaseTableName) != null) {
        throw new ConfigException(
            TABLE_OPT,
            tableName,
            String.format(
                "Table does not exist, however a table %s was found. Update the config to use %s if desired.",
                lowerCaseTableName, lowerCaseTableName));
      } else {
        throw new ConfigException(TABLE_OPT, tableName.asCql(true), "Not found");
      }
    }
  }

  static void validateMappingColumns(DseSession session, DseSinkConfig config) {
    CqlIdentifier keyspaceName = config.getKeyspace();
    CqlIdentifier tableName = config.getTable();
    Metadata metadata = session.getMetadata();
    KeyspaceMetadata keyspace = metadata.getKeyspace(keyspaceName);
    assert keyspace != null;
    TableMetadata table = keyspace.getTable(tableName);
    assert table != null;

    Map<CqlIdentifier, CqlIdentifier> mapping = config.getMapping();

    // The columns in the mapping are the keys. Check that each exists in the table.
    String nonExistentCols =
        mapping
            .keySet()
            .stream()
            .filter(col -> table.getColumn(col) == null)
            .map(c -> c.asCql(true))
            .collect(Collectors.joining(", "));
    if (!StringUtil.isEmpty(nonExistentCols)) {
      throw new ConfigException(
          MAPPING_OPT,
          config.getMappingString(),
          String.format(
              "The following columns do not exist in table %s: %s", tableName, nonExistentCols));
    }

    // Now verify that each column that makes up the primary key in the table has a reference
    // in the mapping.
    String nonExistentKeyCols =
        table
            .getPrimaryKey()
            .stream()
            .filter(col -> !mapping.containsKey(col.getName()))
            .map(col -> col.getName().toString())
            .collect(Collectors.joining(", "));
    if (!StringUtil.isEmpty(nonExistentKeyCols)) {
      throw new ConfigException(
          MAPPING_OPT,
          config.getMappingString(),
          String.format(
              "The following columns are part of the primary key but are not mapped: %s",
              nonExistentKeyCols));
    }
  }

  static String makeInsertStatement(DseSinkConfig config) {
    Map<CqlIdentifier, CqlIdentifier> mapping = config.getMapping();
    StringBuilder statementBuilder = new StringBuilder("INSERT INTO ");
    statementBuilder.append(config.getKeyspace()).append('.').append(config.getTable()).append('(');

    // Add the column names, which are the keys in the mapping. As we do so, collect the
    // bind variable names (e.g. :col) in a buffer (to achieve consistent order).
    StringBuilder valuesBuilder = new StringBuilder();
    boolean isFirst = true;
    for (CqlIdentifier col : mapping.keySet()) {
      if (!isFirst) {
        statementBuilder.append(',');
        valuesBuilder.append(',');
      }
      isFirst = false;
      String colCql = col.asCql(true);
      statementBuilder.append(colCql);
      valuesBuilder.append(':').append(colCql);
    }
    statementBuilder.append(") VALUES (");
    statementBuilder.append(valuesBuilder.toString());
    statementBuilder.append(')');

    return statementBuilder.toString();
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
        new CodecSettings(new DefaultLoaderConfig(dsbulkConfig.getConfig("codec")));
    codecSettings.init();

    DseSession session = builder.withConfigLoader(configLoader).build();

    ExtendedCodecRegistry codecRegistry =
        codecSettings.createCodecRegistry(session.getContext().codecRegistry());

    validateKeyspaceAndTable(session, config);
    validateMappingColumns(session, config);

    // TODO: Multiple sink connectors (say for different topics/tables) may be created at the same
    // time. They can all share the same session, but we must make sure only one connector
    // creates the session.
    PreparedStatement statement = session.prepare(makeInsertStatement(config));

    // Configure the json object mapper
    objectMapper.configure(USE_BIG_DECIMAL_FOR_FLOATS, true);

    sessionState = new SessionState(session, codecRegistry, statement);

    statementReady.countDown();
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
    closeQuietly(sessionState.getSession());
  }

  @Override
  public ConfigDef config() {
    return DseSinkConfig.CONFIG_DEF;
  }
}
