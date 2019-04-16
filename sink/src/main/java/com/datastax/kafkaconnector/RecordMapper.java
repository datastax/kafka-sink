/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector;

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.ASCII;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARCHAR;

import com.datastax.kafkaconnector.record.JsonNodeTimeUnitConverter;
import com.datastax.kafkaconnector.record.RawData;
import com.datastax.kafkaconnector.record.Record;
import com.datastax.kafkaconnector.record.RecordMetadata;
import com.datastax.kafkaconnector.record.StructTimeUnitConverter;
import com.datastax.kafkaconnector.util.SinkUtil;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps {@link Record}s into {@link BoundStatement}s, applying any necessary transformations via
 * codecs.
 */
public class RecordMapper {
  private static final Logger log = LoggerFactory.getLogger(RecordMapper.class);
  private final PreparedStatement insertUpdateStatement;
  private final PreparedStatement deleteStatement;
  private final Set<CqlIdentifier> primaryKey;
  private final Mapping mapping;
  private final boolean allowExtraFields;
  private final boolean allowMissingFields;
  private final TimeUnit ttlTimeUnit;
  private final TimeUnit timestampTimeUnit;

  /** Whether to map null input to "unset" */
  private final boolean nullToUnset;

  public RecordMapper(
      PreparedStatement insertUpdateStatement,
      PreparedStatement deleteStatement,
      List<CqlIdentifier> primaryKey,
      Mapping mapping,
      boolean nullToUnset,
      boolean allowExtraFields,
      boolean allowMissingFields,
      TimeUnit ttlTimeUnit,
      TimeUnit timestampTimeUnit) {
    this.insertUpdateStatement = insertUpdateStatement;
    this.deleteStatement = deleteStatement;
    this.primaryKey = new LinkedHashSet<>(primaryKey);
    this.mapping = mapping;
    this.nullToUnset = nullToUnset;
    this.allowExtraFields = allowExtraFields;
    this.allowMissingFields = allowMissingFields;
    this.ttlTimeUnit = ttlTimeUnit;
    this.timestampTimeUnit = timestampTimeUnit;
  }

  @NotNull
  private static String getExternalName(@NotNull String field) {
    if (field.endsWith(RawData.FIELD_NAME)) {
      // e.g. value.__self => value
      return field.substring(0, field.length() - RawData.FIELD_NAME.length() - 1);
    }
    return field;
  }

  @NotNull
  public BoundStatement map(RecordMetadata recordMetadata, Record record) {
    Object raw;
    DataType cqlType;
    if (!allowMissingFields) {
      ensureAllFieldsPresent(record.fields());
    }

    // Determine if we're doing an insert-update or a delete
    PreparedStatement preparedStatement;
    boolean isInsertUpdate = true;
    if (deleteStatement == null) {
      // There is no delete statement, meaning deletesEnabled must be false. So just
      // do an insert/update.
      preparedStatement = insertUpdateStatement;
    } else {
      // Walk through each record field and check if any non-null fields map to a non-primary-key
      // column. If so, this is an insert; otherwise it is a delete. However, there is a
      // special case: if the table only has primary key columns, there is no case for delete.
      isInsertUpdate =
          mapping.getMappedColumns().equals(primaryKey)
              || record
                  .fields()
                  .stream()
                  .filter(field -> record.getFieldValue(field) != null)
                  .anyMatch(
                      field -> {
                        @Nullable
                        Collection<CqlIdentifier> mappedCols =
                            mapping.fieldToColumns(CqlIdentifier.fromInternal(field));
                        return mappedCols != null
                            && mappedCols.stream().anyMatch(col -> !primaryKey.contains(col));
                      });
      preparedStatement = isInsertUpdate ? insertUpdateStatement : deleteStatement;
    }
    BoundStatementBuilder builder = preparedStatement.boundStatementBuilder();
    ColumnDefinitions variableDefinitions = preparedStatement.getVariableDefinitions();
    for (String field : record.fields()) {
      Collection<CqlIdentifier> columns = mapping.fieldToColumns(CqlIdentifier.fromInternal(field));
      if ((columns == null || columns.isEmpty()) && !allowExtraFields) {
        throw new ConfigException(
            "Extraneous field '"
                + getExternalName(field)
                + "' was found in record. "
                + "Please declare it explicitly in the mapping.");
      }
      if (columns != null) {
        for (CqlIdentifier column : columns) {
          if (!variableDefinitions.contains(column)) {
            // This can happen if we're binding a delete statement (which
            // only contains params for primary key columns, not other
            // mapped columns).
            continue;
          }
          cqlType = variableDefinitions.get(column).getType();
          GenericType<?> fieldType = recordMetadata.getFieldType(field, cqlType);
          if (fieldType != null) {
            raw =
                getFieldValueAndMaybeTransform(
                    record, field, column, ttlTimeUnit, timestampTimeUnit);
            log.trace(
                "binding field {} with value {} to column {}", field, raw, column.asInternal());
            bindColumn(builder, column, raw, cqlType, fieldType);
          }
        }
      }
    }
    // set timestamp from record only if it was not set from mapping
    if (record.getTimestamp() != null && isInsertUpdate && timestampIsNotSet(builder)) {
      bindColumn(
          builder,
          SinkUtil.TIMESTAMP_VARNAME_CQL_IDENTIFIER,
          record.getTimestamp() * 1000,
          DataTypes.BIGINT,
          GenericType.LONG);
    }
    BoundStatement bs = builder.build();
    ensurePrimaryKeySet(bs);
    return bs;
  }

  private boolean timestampIsNotSet(BoundStatementBuilder builder) {
    return !builder.isSet(SinkUtil.TIMESTAMP_VARNAME_CQL_IDENTIFIER);
  }

  @VisibleForTesting
  static Object getFieldValueAndMaybeTransform(
      Record record,
      String field,
      CqlIdentifier column,
      TimeUnit ttlTimeUnit,
      TimeUnit timestampTimeUnit) {
    Object raw;
    Object fieldValue = record.getFieldValue(field);

    if (SinkUtil.isTtlMappingColumn(column)) {
      if (fieldValue instanceof NumericNode) { // case that ttl is from Json node
        raw = JsonNodeTimeUnitConverter.transformTtlField(ttlTimeUnit, fieldValue);
      } else if (fieldValue instanceof Number) { // case that ttl is from Struct
        raw = StructTimeUnitConverter.transformTtlField(ttlTimeUnit, (Number) fieldValue);
      } else {
        throw new IllegalArgumentException(
            "The value: "
                + fieldValue
                + " for field: "
                + field
                + " used as a TTL is not a Number but should be.");
      }
    } else if (SinkUtil.isTimestampMappingColumn(column)) {
      if (fieldValue instanceof NumericNode) { // case that timestamp is from Json node
        raw = JsonNodeTimeUnitConverter.transformTimestampField(timestampTimeUnit, fieldValue);
      } else if (fieldValue instanceof Number) { // case that timestamp is from Struct
        raw =
            StructTimeUnitConverter.transformTimestampField(timestampTimeUnit, (Number) fieldValue);
      } else {
        throw new IllegalArgumentException(
            "The value: "
                + fieldValue
                + " for field: "
                + field
                + " used as a Timestamp is not a Number but should be.");
      }
    } else {
      raw = fieldValue;
    }
    return raw;
  }

  private <T> void bindColumn(
      BoundStatementBuilder builder,
      CqlIdentifier variable,
      T raw,
      DataType cqlType,
      GenericType<? extends T> javaType) {
    TypeCodec<T> codec = mapping.codec(variable, cqlType, javaType);
    ByteBuffer bb = codec.encode(raw, builder.protocolVersion());
    // Account for nullToUnset.
    if (isNull(bb, cqlType)) {
      if (isPrimaryKey(variable)) {
        throw new ConfigException(
            "Primary key column "
                + variable.asCql(true)
                + " cannot be mapped to null. "
                + "Check that your mapping setting matches your dataset contents.");
      }
      if (nullToUnset) {
        return;
      }
    }
    builder.setBytesUnsafe(variable, bb);
  }

  private boolean isNull(ByteBuffer bb, DataType cqlType) {
    if (bb == null) {
      return true;
    }
    if (bb.hasRemaining()) {
      return false;
    }
    switch (cqlType.getProtocolCode()) {
      case VARCHAR:
      case ASCII:
        // empty strings are encoded as zero-length buffers,
        // and should not be considered as nulls.
        return false;
      default:
        return true;
    }
  }

  private boolean isPrimaryKey(CqlIdentifier variable) {
    return primaryKey.contains(variable);
  }

  private void ensureAllFieldsPresent(Set<String> recordFields) {
    ColumnDefinitions variables = insertUpdateStatement.getVariableDefinitions();
    for (int i = 0; i < variables.size(); i++) {
      CqlIdentifier variable = variables.get(i).getName();
      if (variable.asInternal().equals(SinkUtil.TIMESTAMP_VARNAME)) {
        // Not a real field; it's just the bound variable for the timestamp.
        continue;
      }
      CqlIdentifier field = mapping.columnToField(variable);
      if (field != null && !recordFields.contains(field.asInternal())) {
        throw new ConfigException(
            "Required field '"
                + getExternalName(field.asInternal())
                + "' (mapped to column "
                + variable.asCql(true)
                + ") was missing from record. "
                + "Please remove it from the mapping.");
      }
    }
  }

  private void ensurePrimaryKeySet(BoundStatement bs) {
    // This cannot fail unless the insert/update CQL is custom and the user didn't specify
    // all key columns.
    String unsetKeys =
        primaryKey
            .stream()
            .filter(key -> !bs.isSet(key))
            .map(key -> key.asCql(true))
            .collect(Collectors.joining(", "));
    if (!unsetKeys.isEmpty()) {
      throw new ConfigException(
          String.format(
              "Primary key column(s) %s cannot be left unmapped. Check that your mapping setting "
                  + "matches your dataset contents.",
              unsetKeys));
    }
  }
}
