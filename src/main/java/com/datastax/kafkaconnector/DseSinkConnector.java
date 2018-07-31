/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector;

import static com.datastax.kafkaconnector.config.TopicConfig.KEYSPACE_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.MAPPING_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.TABLE_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.getTopicSettingName;
import static com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS;

import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.DseSessionBuilder;
import com.datastax.dse.driver.internal.core.config.typesafe.DefaultDseDriverConfigLoader;
import com.datastax.kafkaconnector.codecs.CodecSettings;
import com.datastax.kafkaconnector.codecs.KafkaCodecRegistry;
import com.datastax.kafkaconnector.config.DseSinkConfig;
import com.datastax.kafkaconnector.config.TopicConfig;
import com.datastax.kafkaconnector.util.SinkUtil;
import com.datastax.kafkaconnector.util.StringUtil;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
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
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Sink connector to insert Kafka records into DSE. */
public class DseSinkConnector extends SinkConnector {
  static final String MAPPINGS_OPT = "mappings";
  static final RecordMetadata JSON_RECORD_METADATA =
      (field, cqlType) ->
          field.equals(RawRecord.FIELD_NAME) ? GenericType.STRING : GenericType.of(JsonNode.class);
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

  static void validateKeyspaceAndTable(DseSession session, DseSinkConfig config) {
    config
        .getTopicConfigs()
        .forEach(
            (topicName, topicConfig) -> {
              CqlIdentifier keyspaceName = topicConfig.getKeyspace();
              CqlIdentifier tableName = topicConfig.getTable();
              Metadata metadata = session.getMetadata();
              Optional<? extends KeyspaceMetadata> keyspace = metadata.getKeyspace(keyspaceName);
              if (!keyspace.isPresent()) {
                String lowerCaseKeyspaceName = keyspaceName.asInternal().toLowerCase();
                if (metadata.getKeyspace(lowerCaseKeyspaceName).isPresent()) {
                  throw new ConfigException(
                      getTopicSettingName(topicName, KEYSPACE_OPT),
                      keyspaceName,
                      String.format(
                          "Keyspace does not exist, however a keyspace %s was found. Update the config to use %s if desired.",
                          lowerCaseKeyspaceName, lowerCaseKeyspaceName));
                } else {
                  throw new ConfigException(
                      getTopicSettingName(topicName, KEYSPACE_OPT),
                      keyspaceName.asCql(true),
                      "Not found");
                }
              }
              Optional<? extends TableMetadata> table = keyspace.get().getTable(tableName);
              if (!table.isPresent()) {
                String lowerCaseTableName = tableName.asInternal().toLowerCase();
                if (keyspace.get().getTable(lowerCaseTableName).isPresent()) {
                  throw new ConfigException(
                      getTopicSettingName(topicName, TABLE_OPT),
                      tableName,
                      String.format(
                          "Table does not exist, however a table %s was found. Update the config to use %s if desired.",
                          lowerCaseTableName, lowerCaseTableName));
                } else {
                  throw new ConfigException(
                      getTopicSettingName(topicName, TABLE_OPT),
                      tableName.asCql(true),
                      "Not found");
                }
              }
            });
  }

  static void validateMappingColumns(DseSession session, DseSinkConfig config) {
    config
        .getTopicConfigs()
        .forEach(
            (topicName, topicConfig) -> {
              CqlIdentifier keyspaceName = topicConfig.getKeyspace();
              CqlIdentifier tableName = topicConfig.getTable();
              Metadata metadata = session.getMetadata();
              Optional<? extends KeyspaceMetadata> keyspace = metadata.getKeyspace(keyspaceName);
              assert keyspace.isPresent();
              Optional<? extends TableMetadata> table = keyspace.get().getTable(tableName);
              assert table.isPresent();

              Map<CqlIdentifier, CqlIdentifier> mapping = topicConfig.getMapping();

              // The columns in the mapping are the keys. Check that each exists in the table.
              String nonExistentCols =
                  mapping
                      .keySet()
                      .stream()
                      .filter(col -> !table.get().getColumn(col).isPresent())
                      .map(c -> c.asCql(true))
                      .collect(Collectors.joining(", "));
              if (!StringUtil.isEmpty(nonExistentCols)) {
                throw new ConfigException(
                    getTopicSettingName(topicName, MAPPING_OPT),
                    topicConfig.getMappingString(),
                    String.format(
                        "The following columns do not exist in table %s: %s",
                        tableName, nonExistentCols));
              }

              // Now verify that each column that makes up the primary key in the table has a
              // reference
              // in the mapping.
              String nonExistentKeyCols =
                  table
                      .get()
                      .getPrimaryKey()
                      .stream()
                      .filter(col -> !mapping.containsKey(col.getName()))
                      .map(col -> col.getName().toString())
                      .collect(Collectors.joining(", "));
              if (!StringUtil.isEmpty(nonExistentKeyCols)) {
                throw new ConfigException(
                    getTopicSettingName(topicName, MAPPING_OPT),
                    topicConfig.getMappingString(),
                    String.format(
                        "The following columns are part of the primary key but are not mapped: %s",
                        nonExistentKeyCols));
              }
            });
  }

  static String makeInsertStatement(TopicConfig config) {
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

    if (config.getTtl() != -1) {
      statementBuilder.append(" USING TTL ").append(config.getTtl());
    }
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
    log.info(String.format("%s\n", config.toString()));
    DseSessionBuilder builder = DseSession.builder();
    config
        .getContactPoints()
        .forEach(
            hostStr -> builder.addContactPoint(new InetSocketAddress(hostStr, config.getPort())));

    DriverConfigLoader configLoader = null;
    if (!config.getLocalDc().isEmpty()) {
      configLoader =
          DefaultDseDriverConfigLoader.builder()
              .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, config.getLocalDc())
              .build();
    }

    Config dsbulkConfig = ConfigFactory.load().getConfig("dsbulk");
    CodecSettings codecSettings =
        new CodecSettings(new DefaultLoaderConfig(dsbulkConfig.getConfig("codec")));
    codecSettings.init();

    if (configLoader != null) {
      builder.withConfigLoader(configLoader);
    }
    DseSession session = builder.build();

    KafkaCodecRegistry codecRegistry =
        codecSettings.createCodecRegistry(session.getContext().codecRegistry());

    validateKeyspaceAndTable(session, config);
    validateMappingColumns(session, config);

    // TODO: Multiple sink connectors (say for different topics/tables) may be created at the same
    // time. They can all share the same session, but we must make sure only one connector
    // creates the session.

    Map<String, CompletionStage<PreparedStatement>> prepareFutures = new HashMap<>();
    config
        .getTopicConfigs()
        .forEach(
            (topicName, topicConfig) ->
                prepareFutures.put(
                    topicName, session.prepareAsync(makeInsertStatement(topicConfig))));
    Map<String, PreparedStatement> preparedStatements = new HashMap<>();
    prepareFutures.forEach(
        (topicName, future) -> {
          try {
            preparedStatements.put(
                topicName, Uninterruptibles.getUninterruptibly(future.toCompletableFuture()));
          } catch (ExecutionException e) {
            // TODO: Is this ok??
            throw new RuntimeException(e);
          }
        });

    // Configure the json object mapper
    objectMapper.configure(USE_BIG_DECIMAL_FOR_FLOATS, true);

    sessionState = new SessionState(session, codecRegistry, preparedStatements);

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
        ImmutableMap.<String, String>builder()
            .put(MAPPINGS_OPT, SinkUtil.serializeTopicMappings(config))
            .build();
    for (int i = 0; i < maxTasks; i++) {
      configs.add(taskConfig);
    }
    return configs;
  }

  @Override
  public void stop() {
    // TODO: When is it safe to close the (shared) session?
    if (sessionState != null) {
      closeQuietly(sessionState.getSession());
    }
  }

  @Override
  public ConfigDef config() {
    return DseSinkConfig.GLOBAL_CONFIG_DEF;
  }
}
