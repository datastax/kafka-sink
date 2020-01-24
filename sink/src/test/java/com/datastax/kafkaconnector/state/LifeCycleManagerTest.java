/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.state;

import static com.datastax.kafkaconnector.config.DseSinkConfig.DC_OPT;
import static com.datastax.kafkaconnector.config.TableConfig.MAPPING_OPT;
import static com.datastax.kafkaconnector.config.TableConfig.TTL_OPT;
import static com.datastax.kafkaconnector.config.TableConfig.TTL_TIME_UNIT_OPT;
import static com.datastax.oss.driver.api.core.type.DataTypes.COUNTER;
import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.kafkaconnector.config.DseSinkConfig;
import com.datastax.kafkaconnector.config.TableConfig;
import com.datastax.kafkaconnector.config.TableConfigBuilder;
import com.datastax.kafkaconnector.util.SinkUtil;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class LifeCycleManagerTest {
  private static final String C1 = "c1";
  private static final String C2 = "This is column 2, and its name desperately needs quoting";
  private static final String C3 = "c3";

  private static final CqlIdentifier C1_IDENT = CqlIdentifier.fromInternal(C1);
  private static final CqlIdentifier C2_IDENT = CqlIdentifier.fromInternal(C2);
  private static final CqlIdentifier C3_IDENT = CqlIdentifier.fromInternal(C3);
  private ColumnMetadata col1;
  private ColumnMetadata col2;
  private ColumnMetadata col3;
  private CqlSession session;
  private Metadata metadata;
  private KeyspaceMetadata keyspace;
  private TableMetadata table;

  @BeforeEach
  void setUp() {
    session = mock(CqlSession.class);
    DriverContext context = mock(DriverContext.class);
    CodecRegistry codecRegistry = mock(CodecRegistry.class);
    metadata = mock(Metadata.class);
    keyspace = mock(KeyspaceMetadata.class);
    table = mock(TableMetadata.class);
    TableMetadata table2 = mock(TableMetadata.class);
    col1 = mock(ColumnMetadata.class);
    col2 = mock(ColumnMetadata.class);
    col3 = mock(ColumnMetadata.class);
    Map<CqlIdentifier, ColumnMetadata> columns =
        ImmutableMap.<CqlIdentifier, ColumnMetadata>builder()
            .put(C1_IDENT, col1)
            .put(C2_IDENT, col2)
            .put(C3_IDENT, col3)
            .build();
    when(session.getMetadata()).thenReturn(metadata);
    when(session.getContext()).thenReturn(context);
    when(context.getCodecRegistry()).thenReturn(codecRegistry);
    CompletionStage preparedStatementStage = mock(CompletionStage.class);
    CompletableFuture preparedStatementFuture = mock(CompletableFuture.class);
    when(session.prepareAsync(any(String.class))).thenReturn(preparedStatementStage);
    when(preparedStatementStage.thenAcceptBoth(any(CompletionStage.class), any(BiConsumer.class)))
        .thenReturn(preparedStatementStage);
    when(preparedStatementStage.exceptionally(any(Function.class)))
        .thenReturn(preparedStatementStage);
    when(preparedStatementStage.toCompletableFuture()).thenReturn(preparedStatementFuture);

    when(metadata.getKeyspace(any(CqlIdentifier.class))).thenReturn(Optional.of(keyspace));
    when(keyspace.getTable(CqlIdentifier.fromInternal("mytable"))).thenReturn(Optional.of(table));
    when(table.getColumns()).thenReturn(columns);
    when(table.getColumn(C1_IDENT)).thenReturn(Optional.of(col1));
    when(table.getColumn(C2_IDENT)).thenReturn(Optional.of(col2));
    when(table.getColumn(C3_IDENT)).thenReturn(Optional.of(col3));
    when(table.getPrimaryKey()).thenReturn(Collections.singletonList(col1));

    when(keyspace.getTable(CqlIdentifier.fromInternal("mytable2"))).thenReturn(Optional.of(table2));
    when(table2.getColumn(C1_IDENT)).thenReturn(Optional.of(col1));
    when(table2.getColumn(C2_IDENT)).thenReturn(Optional.of(col2));
    when(table2.getColumn(C3_IDENT)).thenReturn(Optional.of(col3));
    when(table2.getPrimaryKey()).thenReturn(Collections.singletonList(col2));

    when(col1.getName()).thenReturn(C1_IDENT);
    when(col2.getName()).thenReturn(C2_IDENT);
    when(col3.getName()).thenReturn(C3_IDENT);
    when(col1.getType()).thenReturn(TEXT);
    when(col2.getType()).thenReturn(TEXT);
    when(col3.getType()).thenReturn(TEXT);
  }

  @Test
  void should_error_that_keyspace_was_not_found() {
    when(metadata.getKeyspace(CqlIdentifier.fromInternal("MyKs"))).thenReturn(Optional.empty());
    when(metadata.getKeyspace("myks")).thenReturn(Optional.of(keyspace));

    assertThatThrownBy(
            () ->
                LifeCycleManager.getTableMetadata(
                    session, makeTableConfig("MyKs", "mytable", "c1=value.f1")))
        .isInstanceOf(ConfigException.class)
        .hasMessage(
            "Keyspace MyKs does not exist, however a keyspace myks was found. Update the config to use myks if desired.");
  }

  @Test
  void should_error_that_table_was_not_found() {
    when(keyspace.getTable(CqlIdentifier.fromInternal("MyTable"))).thenReturn(Optional.empty());
    when(keyspace.getTable("mytable")).thenReturn(Optional.of(table));

    assertThatThrownBy(
            () ->
                LifeCycleManager.getTableMetadata(
                    session, makeTableConfig("ks1", "MyTable", "c1=value.f1")))
        .isInstanceOf(ConfigException.class)
        .hasMessage(
            "Table MyTable does not exist, however a table mytable was found. Update the config to use mytable if desired.");
  }

  @Test
  void should_error_that_keyspace_was_not_found_2() {
    when(metadata.getKeyspace(CqlIdentifier.fromInternal("MyKs"))).thenReturn(Optional.empty());
    when(metadata.getKeyspace("myks")).thenReturn(Optional.empty());
    assertThatThrownBy(
            () ->
                LifeCycleManager.getTableMetadata(
                    session, makeTableConfig("MyKs", "t1", "c1=value.f1")))
        .isInstanceOf(ConfigException.class)
        .hasMessage("Keyspace MyKs does not exist.");
  }

  @Test
  void should_error_that_table_was_not_found_2() {
    when(keyspace.getTable(CqlIdentifier.fromInternal("MyTable"))).thenReturn(Optional.empty());
    when(keyspace.getTable("mytable")).thenReturn(Optional.empty());

    assertThatThrownBy(
            () ->
                LifeCycleManager.getTableMetadata(
                    session, makeTableConfig("ks1", "MyTable", "c1=value.f1")))
        .isInstanceOf(ConfigException.class)
        .hasMessage("Table MyTable does not exist.");
  }

  @Test
  void should_error_when_mapping_does_not_use_primary_key_columns() {
    TableConfig config = makeTableConfig("myks", "mytable", C3 + "=key.f3");
    assertThatThrownBy(() -> LifeCycleManager.validateMappingColumns(table, config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            "Invalid value c3=key.f3 for configuration topic.mytopic.myks.mytable.mapping:")
        .hasMessageContaining("but are not mapped: " + C1);
  }

  @Test
  void should_error_when_mapping_has_nonexistent_column() {
    TableConfig config = makeTableConfig("myks", "mytable", "nocol=key.f3");
    assertThatThrownBy(() -> LifeCycleManager.validateMappingColumns(table, config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(
            "Invalid value nocol=key.f3 for configuration topic.mytopic.myks.mytable.mapping:")
        .hasMessageContaining("do not exist in table mytable: nocol");
  }

  @Test
  void should_compute_that_all_columns_are_mapped() {
    TableConfig config =
        makeTableConfig(
            "myks", "mytable", String.format("%s=key.f1, \"%s\"=key.f2, %s=key.f3", C1, C2, C3));
    assertThat(LifeCycleManager.validateMappingColumns(table, config)).isTrue();
  }

  @Test
  void should_compute_that_all_columns_are_not_mapped() {
    TableConfig config =
        makeTableConfig("myks", "mytable", String.format("%s=key.f1, %s=key.f3", C1, C3));
    assertThat(LifeCycleManager.validateMappingColumns(table, config)).isFalse();
  }

  @Test
  void should_make_correct_insert_cql() {
    TableConfig config =
        makeTableConfig(
            "myks", "mytable", String.format("%s=key.f1, \"%s\"=key.f2, %s=key.f3", C1, C2, C3));
    assertThat(LifeCycleManager.makeInsertStatement(config))
        .isEqualTo(
            String.format(
                "INSERT INTO myks.mytable(%s,\"%s\",%s) VALUES (:%s,:\"%s\",:%s) USING TIMESTAMP :%s",
                C1, C2, C3, C1, C2, C3, SinkUtil.TIMESTAMP_VARNAME));
  }

  @Test
  void should_make_correct_insert_cql_with_ttl_using_time_unit_converter() {
    TableConfig config =
        makeTableConfig(
            "myks",
            "mytable",
            String.format("%s=key.f1, \"%s\"=key.f2, %s=key.f3", C1, C2, C3),
            1000,
            "MILLISECONDS");
    assertThat(LifeCycleManager.makeInsertStatement(config))
        .isEqualTo(
            String.format(
                "INSERT INTO myks.mytable(%s,\"%s\",%s) VALUES (:%s,:\"%s\",:%s) "
                    + "USING TIMESTAMP :%s AND TTL 1",
                C1, C2, C3, C1, C2, C3, SinkUtil.TIMESTAMP_VARNAME));
  }

  @Test
  void should_make_correct_insert_cql_with_ttl() {
    TableConfig config =
        makeTableConfig(
            "myks",
            "mytable",
            String.format("%s=key.f1, \"%s\"=key.f2, %s=key.f3", C1, C2, C3),
            1234);
    assertThat(LifeCycleManager.makeInsertStatement(config))
        .isEqualTo(
            String.format(
                "INSERT INTO myks.mytable(%s,\"%s\",%s) VALUES (:%s,:\"%s\",:%s) "
                    + "USING TIMESTAMP :%s AND TTL 1234",
                C1, C2, C3, C1, C2, C3, SinkUtil.TIMESTAMP_VARNAME));
  }

  @Test
  void should_make_correct_insert_cql_with_ttl_from_mapping() {
    TableConfig config =
        makeTableConfig(
            "myks",
            "mytable",
            String.format("%s=key.f1, \"%s\"=key.f2, %s=key.f3, __ttl=key.f3", C1, C2, C3));
    assertThat(LifeCycleManager.makeInsertStatement(config))
        .isEqualTo(
            String.format(
                "INSERT INTO myks.mytable(%s,\"%s\",%s) VALUES (:%s,:\"%s\",:%s) "
                    + "USING TIMESTAMP :%s AND TTL :%s",
                C1, C2, C3, C1, C2, C3, SinkUtil.TIMESTAMP_VARNAME, SinkUtil.TTL_VARNAME));
  }

  @Test
  void should_make_correct_insert_cql_with_ttl_from_mapping_over_global() {
    TableConfig config =
        makeTableConfig(
            "myks",
            "mytable",
            String.format("%s=key.f1, \"%s\"=key.f2, %s=key.f3, __ttl=key.f3", C1, C2, C3),
            1000);
    assertThat(LifeCycleManager.makeInsertStatement(config))
        .isEqualTo(
            String.format(
                "INSERT INTO myks.mytable(%s,\"%s\",%s) VALUES (:%s,:\"%s\",:%s) "
                    + "USING TIMESTAMP :%s AND TTL :%s",
                C1, C2, C3, C1, C2, C3, SinkUtil.TIMESTAMP_VARNAME, SinkUtil.TTL_VARNAME));
  }

  @Test
  void should_make_correct_update_counter_cql_simple_key() {
    when(col2.getType()).thenReturn(COUNTER);
    when(col3.getType()).thenReturn(COUNTER);

    TableConfig config =
        makeTableConfig(
            "myks", "mytable", String.format("%s=key.f1, \"%s\"=key.f2, %s=key.f3", C1, C2, C3));

    assertThat(LifeCycleManager.makeUpdateCounterStatement(config, table))
        .isEqualTo(
            String.format(
                "UPDATE myks.mytable SET \"%s\" = \"%s\" + :\"%s\",%s = %s + :%s WHERE %s = :%s",
                C2, C2, C2, C3, C3, C3, C1, C1));
  }

  @Test
  void should_make_correct_update_counter_cql_complex_key() {
    when(col3.getType()).thenReturn(COUNTER);
    when(table.getPrimaryKey()).thenReturn(Arrays.asList(col1, col2));

    TableConfig config =
        makeTableConfig(
            "myks", "mytable", String.format("%s=key.f1, \"%s\"=key.f2, %s=key.f3", C1, C2, C3));

    assertThat(LifeCycleManager.makeUpdateCounterStatement(config, table))
        .isEqualTo(
            String.format(
                "UPDATE myks.mytable SET %s = %s + :%s WHERE %s = :%s AND \"%s\" = :\"%s\"",
                C3, C3, C3, C1, C1, C2, C2));
  }

  @Test
  void should_make_correct_delete_cql() {
    when(table.getPrimaryKey()).thenReturn(Arrays.asList(col1, col2));

    TableConfig config =
        makeTableConfig(
            "myks", "mytable", String.format("%s=key.f1, \"%s\"=key.f2, %s=key.f3", C1, C2, C3));

    assertThat(LifeCycleManager.makeDeleteStatement(config, table))
        .isEqualTo(
            String.format(
                "DELETE FROM myks.mytable WHERE %s = :%s AND \"%s\" = :\"%s\"", C1, C1, C2, C2));
  }

  @Test
  void should_throw_config_exception_if_contact_points_are_correct_but_localDc_not_supplied() {
    // given
    Map<String, String> config = ImmutableMap.of("contactPoints", "127.0.0.1");

    // when, then
    assertThatThrownBy(() -> new DseSinkConfig(config))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining(DC_OPT);
  }

  private static TableConfig makeTableConfig(
      String keyspaceName, String tableName, String mapping) {
    return makeTableConfig(keyspaceName, tableName, mapping, -1);
  }

  private static TableConfig makeTableConfig(
      String keyspaceName, String tableName, String mapping, int ttl) {
    return constructBaseProperties(keyspaceName, tableName, mapping, ttl).build();
  }

  private static TableConfigBuilder constructBaseProperties(
      String keyspaceName, String tableName, String mapping, int ttl) {
    return new TableConfigBuilder("mytopic", keyspaceName, tableName, false)
        .addSimpleSetting(MAPPING_OPT, mapping)
        .addSimpleSetting(TTL_OPT, String.valueOf(ttl));
  }

  private static TableConfig makeTableConfig(
      String keyspaceName, String tableName, String mapping, int ttl, String timeUnit) {
    return constructBaseProperties(keyspaceName, tableName, mapping, ttl)
        .addSimpleSetting(TTL_TIME_UNIT_OPT, timeUnit)
        .build();
  }
}
