/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.kafkaconnector.util;

import static com.datastax.dse.driver.api.core.config.DseDriverOption.AUTH_PROVIDER_SASL_PROPERTIES;
import static com.datastax.dse.driver.api.core.config.DseDriverOption.AUTH_PROVIDER_SASL_PROTOCOL;
import static com.datastax.dse.driver.api.core.metadata.DseNodeProperties.DSE_VERSION;
import static com.datastax.kafkaconnector.config.TopicConfig.KEYSPACE_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.MAPPING_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.TABLE_OPT;
import static com.datastax.kafkaconnector.config.TopicConfig.getTopicSettingName;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.AUTH_PROVIDER_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.AUTH_PROVIDER_PASSWORD;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.AUTH_PROVIDER_USER_NAME;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_INTERVAL;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.METRICS_SESSION_ENABLED;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.PROTOCOL_COMPRESSION;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_CIPHER_SUITES;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_HOSTNAME_VALIDATION;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_KEYSTORE_PASSWORD;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_KEYSTORE_PATH;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.SSL_TRUSTSTORE_PATH;
import static com.fasterxml.jackson.databind.DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS;

import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.auth.DseGssApiAuthProvider;
import com.datastax.dse.driver.api.core.auth.DsePlainTextAuthProvider;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.internal.core.config.typesafe.DefaultDseDriverConfigLoader;
import com.datastax.kafkaconnector.DseSinkTask;
import com.datastax.kafkaconnector.RawData;
import com.datastax.kafkaconnector.RecordMetadata;
import com.datastax.kafkaconnector.codecs.CodecSettings;
import com.datastax.kafkaconnector.codecs.KafkaCodecRegistry;
import com.datastax.kafkaconnector.config.AuthenticatorConfig;
import com.datastax.kafkaconnector.config.DseSinkConfig;
import com.datastax.kafkaconnector.config.SslConfig;
import com.datastax.kafkaconnector.config.TopicConfig;
import com.datastax.kafkaconnector.ssl.SessionBuilder;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoaderBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to house useful methods and constants that the rest of the application may use. */
public class SinkUtil {
  public static final String TIMESTAMP_VARNAME = "kafka_internal_timestamp";
  public static final String NAME_OPT = "name";
  public static final RecordMetadata JSON_RECORD_METADATA =
      (field, cqlType) ->
          field.equals(RawData.FIELD_NAME) ? GenericType.STRING : GenericType.of(JsonNode.class);
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final JavaType JSON_NODE_MAP_TYPE =
      OBJECT_MAPPER.constructType(new TypeReference<Map<String, JsonNode>>() {}.getType());

  private static final Logger log = LoggerFactory.getLogger(SinkUtil.class);
  private static final ConcurrentMap<String, InstanceState> INSTANCE_STATES =
      new ConcurrentHashMap<>();

  /** This is a utility class that no one should instantiate. */
  private SinkUtil() {}

  public static InstanceState startTask(DseSinkTask task, Map<String, String> props) {
    InstanceState instanceState =
        INSTANCE_STATES.computeIfAbsent(props.get(NAME_OPT), (x) -> buildInstanceState(props));
    instanceState.registerTask(task);
    return instanceState;
  }

  public static void stopTask(InstanceState instanceState, DseSinkTask task) {
    log.info("Stopping task");
    if (instanceState != null) {
      instanceState.unregisterTask(task);
      if (instanceState.getTasks().isEmpty()) {
        closeQuietly(instanceState.getSession());
        instanceState.stopJmxReporter();
        INSTANCE_STATES.remove(instanceState.getConfig().getInstanceName());
      }
    }
    log.info("Task is stopped");
  }

  static Map<String, List<CqlIdentifier>> computePrimaryKeys(
      DseSession session, DseSinkConfig config) {
    return config
        .getTopicConfigs()
        .values()
        .stream()
        .collect(
            Collectors.toMap(
                TopicConfig::getKeyspaceAndTable,
                topicConfig -> {
                  CqlIdentifier keyspaceName = topicConfig.getKeyspace();
                  CqlIdentifier tableName = topicConfig.getTable();
                  Metadata metadata = session.getMetadata();
                  Optional<? extends KeyspaceMetadata> keyspace =
                      metadata.getKeyspace(keyspaceName);
                  if (!keyspace.isPresent()) {
                    String lowerCaseKeyspaceName = keyspaceName.asInternal().toLowerCase();
                    if (metadata.getKeyspace(lowerCaseKeyspaceName).isPresent()) {
                      throw new ConfigException(
                          getTopicSettingName(topicConfig.getTopicName(), KEYSPACE_OPT),
                          keyspaceName,
                          String.format(
                              "Keyspace does not exist, however a keyspace %s was found. Update the config to use %s if desired.",
                              lowerCaseKeyspaceName, lowerCaseKeyspaceName));
                    } else {
                      throw new ConfigException(
                          getTopicSettingName(topicConfig.getTopicName(), KEYSPACE_OPT),
                          keyspaceName.asCql(true),
                          "Not found");
                    }
                  }
                  Optional<? extends TableMetadata> table = keyspace.get().getTable(tableName);
                  if (!table.isPresent()) {
                    String lowerCaseTableName = tableName.asInternal().toLowerCase();
                    if (keyspace.get().getTable(lowerCaseTableName).isPresent()) {
                      throw new ConfigException(
                          getTopicSettingName(topicConfig.getTopicName(), TABLE_OPT),
                          tableName,
                          String.format(
                              "Table does not exist, however a table %s was found. Update the config to use %s if desired.",
                              lowerCaseTableName, lowerCaseTableName));
                    } else {
                      throw new ConfigException(
                          getTopicSettingName(topicConfig.getTopicName(), TABLE_OPT),
                          tableName.asCql(true),
                          "Not found");
                    }
                  }
                  return table
                      .get()
                      .getPrimaryKey()
                      .stream()
                      .map(ColumnMetadata::getName)
                      .collect(Collectors.toList());
                },
                (old, n) -> old));
  }

  /**
   * Validate that the mapping in the given topicConfig references columns that exist in the table,
   * and that every primary key column in the table has a mapping.
   *
   * @param session the DSE session
   * @param topicConfig the TopicConfig to evaluate
   * @return true if all columns in the table are mapped, false otherwise.
   */
  static boolean validateMappingColumns(DseSession session, TopicConfig topicConfig) {
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
          getTopicSettingName(topicConfig.getTopicName(), MAPPING_OPT),
          topicConfig.getMappingString(),
          String.format(
              "The following columns do not exist in table %s: %s", tableName, nonExistentCols));
    }

    // Now verify that each column that makes up the primary key in the table has a
    // reference in the mapping.
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
          getTopicSettingName(topicConfig.getTopicName(), MAPPING_OPT),
          topicConfig.getMappingString(),
          String.format(
              "The following columns are part of the primary key but are not mapped: %s",
              nonExistentKeyCols));
    }

    return mapping.keySet().size() == table.get().getColumns().size();
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
    statementBuilder
        .append(") VALUES (")
        .append(valuesBuilder.toString())
        .append(") USING TIMESTAMP :")
        .append(TIMESTAMP_VARNAME);

    if (config.getTtl() != -1) {
      statementBuilder.append(" AND TTL ").append(config.getTtl());
    }
    return statementBuilder.toString();
  }

  static String makeUpdateCounterStatement(TopicConfig config, TableMetadata table) {
    if (config.getTtl() != -1) {
      throw new ConfigException("Cannot set ttl when updating a counter table");
    }

    // Create an UPDATE statement that looks like this:
    // UPDATE ks.table SET col1 = col1 + :col1, col2 = col2 + :col2, ...
    // WHERE pk1 = :pk1 AND pk2 = :pk2 ...

    Map<CqlIdentifier, CqlIdentifier> mapping = config.getMapping();
    StringBuilder statementBuilder = new StringBuilder("UPDATE ");
    statementBuilder
        .append(config.getKeyspace())
        .append('.')
        .append(config.getTable())
        .append(" SET ");

    List<CqlIdentifier> pks =
        table.getPrimaryKey().stream().map(ColumnMetadata::getName).collect(Collectors.toList());

    // Walk through the columns and add the "col1 = col1 + :col1" fragments for
    // all non-pk columns.
    boolean isFirst = true;
    for (CqlIdentifier col : mapping.keySet()) {
      if (pks.contains(col)) {
        continue;
      }
      if (!isFirst) {
        statementBuilder.append(',');
      }
      isFirst = false;
      String colAsCql = col.asCql(true);
      statementBuilder
          .append(colAsCql)
          .append(" = ")
          .append(colAsCql)
          .append(" + :")
          .append(colAsCql);
    }

    // Add the WHERE clause, covering pk columns.
    statementBuilder.append(" WHERE ");
    isFirst = true;
    for (CqlIdentifier col : pks) {
      if (!isFirst) {
        statementBuilder.append(" AND ");
      }
      isFirst = false;
      String colAsCql = col.asCql(true);
      statementBuilder.append(colAsCql).append(" = :").append(colAsCql);
    }

    return statementBuilder.toString();
  }

  static String makeDeleteStatement(TopicConfig config, TableMetadata table) {

    // Create a DELETE statement that looks like this:
    // DELETE FROM ks.table
    // WHERE pk1 = :pk1 AND pk2 = :pk2 ...

    StringBuilder statementBuilder = new StringBuilder("DELETE FROM ");
    statementBuilder.append(config.getKeyspace()).append('.').append(config.getTable());

    List<CqlIdentifier> pks =
        table.getPrimaryKey().stream().map(ColumnMetadata::getName).collect(Collectors.toList());

    // Add the WHERE clause, covering pk columns.
    statementBuilder.append(" WHERE ");
    boolean isFirst = true;
    for (CqlIdentifier col : pks) {
      if (!isFirst) {
        statementBuilder.append(" AND ");
      }
      isFirst = false;
      String colAsCql = col.asCql(true);
      statementBuilder.append(colAsCql).append(" = :").append(colAsCql);
    }

    return statementBuilder.toString();
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

  private static boolean isCounterTable(TableMetadata table) {
    return table.getColumns().values().stream().anyMatch(c -> c.getType() == DataTypes.COUNTER);
  }

  private static void checkProductCompatibility(Session session) {
    Collection<Node> hosts = session.getMetadata().getNodes().values();
    List<Node> nonDseHosts =
        hosts
            .stream()
            .filter(
                host ->
                    host.getExtras().get(DSE_VERSION) == null
                        && (host.getCassandraVersion() == null
                            || host.getCassandraVersion().getDSEPatch() <= 0))
            .collect(Collectors.toList());
    if (!nonDseHosts.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "Unable to load data to non DSE cluster; offending nodes: %s",
              nonDseHosts.stream().map(Node::toString).collect(Collectors.joining(", "))));
    }
  }

  private static InstanceState buildInstanceState(Map<String, String> props) {
    DseSinkConfig config = new DseSinkConfig(props);
    log.info("DseSinkTask starting with config:\n{}\n", config.toString());
    SslConfig sslConfig = config.getSslConfig();
    SessionBuilder builder = new SessionBuilder(sslConfig);
    config
        .getContactPoints()
        .forEach(
            hostStr -> builder.addContactPoint(new InetSocketAddress(hostStr, config.getPort())));

    DefaultDriverConfigLoaderBuilder configLoaderBuilder = DefaultDseDriverConfigLoader.builder();
    if (!config.getLocalDc().isEmpty()) {
      configLoaderBuilder.withString(
          DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, config.getLocalDc());
    }

    if (config.getJmx()) {
      configLoaderBuilder.withStringList(
          METRICS_SESSION_ENABLED, Arrays.asList("cql-requests", "cql-client-timeouts"));
      configLoaderBuilder.withDuration(
          METRICS_SESSION_CQL_REQUESTS_INTERVAL, Duration.ofSeconds(30));
    }

    if (config.getCompressionType() != DseSinkConfig.CompressionType.None) {
      configLoaderBuilder.withString(
          PROTOCOL_COMPRESSION, config.getCompressionType().getDriverCompressionType());
    }

    AuthenticatorConfig authConfig = config.getAuthenticatorConfig();
    if (authConfig.getProvider() == AuthenticatorConfig.Provider.DSE) {
      configLoaderBuilder
          .withClass(AUTH_PROVIDER_CLASS, DsePlainTextAuthProvider.class)
          .with(AUTH_PROVIDER_USER_NAME, authConfig.getUsername())
          .with(AUTH_PROVIDER_PASSWORD, authConfig.getPassword());
    } else if (authConfig.getProvider() == AuthenticatorConfig.Provider.GSSAPI) {
      Path keyTabPath = authConfig.getKeyTabPath();
      Map<String, String> loginConfig;
      if (keyTabPath == null) {
        // Rely on the ticket cache.
        ImmutableMap.Builder<String, String> loginConfigBuilder =
            ImmutableMap.<String, String>builder()
                .put("useTicketCache", "true")
                .put("refreshKrb5Config", "true")
                .put("renewTGT", "true");
        if (!authConfig.getPrincipal().isEmpty()) {
          loginConfigBuilder.put("principal", authConfig.getPrincipal());
        }
        loginConfig = loginConfigBuilder.build();
      } else {
        // Authenticate with the keytab file
        loginConfig =
            ImmutableMap.of(
                "principal",
                authConfig.getPrincipal(),
                "useKeyTab",
                "true",
                "refreshKrb5Config",
                "true",
                "keyTab",
                authConfig.getKeyTabPath().toString());
      }
      configLoaderBuilder
          .withClass(AUTH_PROVIDER_CLASS, DseGssApiAuthProvider.class)
          .withString(AUTH_PROVIDER_SASL_PROTOCOL, authConfig.getService())
          .withStringMap(
              AUTH_PROVIDER_SASL_PROPERTIES, ImmutableMap.of("javax.security.sasl.qop", "auth"))
          .withStringMap(DseDriverOption.AUTH_PROVIDER_LOGIN_CONFIGURATION, loginConfig);
    }

    if (sslConfig.getProvider() == SslConfig.Provider.JDK) {
      configLoaderBuilder.withString(SSL_ENGINE_FACTORY_CLASS, "DefaultSslEngineFactory");
      List<String> cipherSuites = sslConfig.getCipherSuites();
      if (!cipherSuites.isEmpty()) {
        configLoaderBuilder.withStringList(SSL_CIPHER_SUITES, cipherSuites);
      }
      configLoaderBuilder
          .withBoolean(SSL_HOSTNAME_VALIDATION, sslConfig.requireHostnameValidation())
          .withString(SSL_TRUSTSTORE_PASSWORD, sslConfig.getTruststorePassword())
          .withString(SSL_KEYSTORE_PASSWORD, sslConfig.getKeystorePassword());

      Path truststorePath = sslConfig.getTruststorePath();
      if (truststorePath != null) {
        configLoaderBuilder.withString(SSL_TRUSTSTORE_PATH, truststorePath.toString());
      }
      Path keystorePath = sslConfig.getKeystorePath();
      if (keystorePath != null) {
        configLoaderBuilder.withString(SSL_KEYSTORE_PATH, keystorePath.toString());
      }
    }
    builder.withConfigLoader(configLoaderBuilder.build());

    DseSession session = builder.build();
    checkProductCompatibility(session);

    // Compute the primary keys of all tables being mapped to. This has the side effect
    // of throwing an exception if a table or keyspace doesn't exist.
    Map<String, List<CqlIdentifier>> primaryKeys = computePrimaryKeys(session, config);

    Config kafkaConfig = ConfigFactory.load().getConfig("kafka");

    // Walk through topic-configs to create TopicState's. This involves computing the following:
    // codec-registry
    // cql for insert-update statements
    // cql for delete statements
    // prepared-statement for insert/update requests
    // prepared-statement for delete requests, if deletesEnabled is true and all columns are mapped.
    Map<String, TopicState> topicStates = new ConcurrentHashMap<>();
    List<CompletionStage<Void>> futures =
        config
            .getTopicConfigs()
            .values()
            .stream()
            .map(
                topicConfig -> {
                  boolean allColumnsMapped = validateMappingColumns(session, topicConfig);
                  CqlIdentifier keyspaceName = topicConfig.getKeyspace();
                  CqlIdentifier tableName = topicConfig.getTable();
                  Metadata metadata = session.getMetadata();
                  Optional<? extends KeyspaceMetadata> keyspace =
                      metadata.getKeyspace(keyspaceName);
                  assert (keyspace.isPresent());
                  Optional<? extends TableMetadata> table = keyspace.get().getTable(tableName);
                  assert table.isPresent();

                  String insertUpdateStatement =
                      isCounterTable(table.get())
                          ? makeUpdateCounterStatement(topicConfig, table.get())
                          : makeInsertStatement(topicConfig);
                  CompletionStage<PreparedStatement> insertUpdateFuture =
                      session.prepareAsync(insertUpdateStatement);
                  CompletionStage<PreparedStatement> deleteFuture;
                  String deleteStatement = makeDeleteStatement(topicConfig, table.get());
                  if (topicConfig.isDeletesEnabled() && allColumnsMapped) {
                    deleteFuture = session.prepareAsync(deleteStatement);
                  } else {
                    // Make a dummy future that's already completed since there is no work to do
                    // here.
                    deleteFuture = new CompletableFuture<>();
                    ((CompletableFuture<PreparedStatement>) deleteFuture).complete(null);
                  }
                  return insertUpdateFuture
                      .thenAcceptBoth(
                          deleteFuture,
                          (preparedInsertUpdate, preparedDelete) -> {
                            CodecSettings codecSettings =
                                new CodecSettings(
                                    new DefaultLoaderConfig(topicConfig.getCodecConfigOverrides())
                                        .withFallback(kafkaConfig.getConfig("codec")));
                            codecSettings.init();
                            KafkaCodecRegistry codecRegistry =
                                codecSettings.createCodecRegistry(
                                    session.getContext().getCodecRegistry());
                            topicStates.put(
                                topicConfig.getTopicName(),
                                new TopicState(
                                    topicConfig.getTopicName(),
                                    preparedInsertUpdate,
                                    preparedDelete,
                                    codecRegistry));
                          })
                      .exceptionally(
                          e -> {
                            String statements =
                                deleteFuture.toCompletableFuture().join() != null
                                    ? String.format(
                                        "%s or %s", insertUpdateStatement, deleteStatement)
                                    : insertUpdateStatement;
                            throw new RuntimeException(
                                String.format("Prepare failed for statement: %s", statements),
                                e.getCause());
                          });
                })
            .collect(Collectors.toList());

    // Wait for all of the prepares to complete and topicStates to be up-to-date.
    futures.forEach(
        f -> {
          try {
            f.toCompletableFuture().join();
          } catch (CompletionException e) {
            // The exception wraps an underlying runtime exception. Throw *that*.
            throw (RuntimeException) e.getCause();
          }
        });

    // Configure the json object mapper
    OBJECT_MAPPER.configure(USE_BIG_DECIMAL_FOR_FLOATS, true);

    return new InstanceState(config, session, primaryKeys, topicStates);
  }
}
