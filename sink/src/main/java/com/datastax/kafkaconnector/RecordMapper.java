/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector;

import static com.datastax.kafkaconnector.record.RawData.VALUE_FIELD_NAME;
import static com.datastax.kafkaconnector.util.FunctionMapper.SUPPORTED_FUNCTIONS_IN_MAPPING;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.ASCII;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARCHAR;

import com.datastax.kafkaconnector.record.JsonNodeTimeUnitConverter;
import com.datastax.kafkaconnector.record.RawData;
import com.datastax.kafkaconnector.record.Record;
import com.datastax.kafkaconnector.record.RecordMetadata;
import com.datastax.kafkaconnector.record.StructTimeUnitConverter;
import com.datastax.kafkaconnector.util.FunctionMapper;
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
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigException;
import org.jetbrains.annotations.NotNull;
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
    if (!allowMissingFields) {
      ensureAllFieldsPresent(
          record.fields(), insertUpdateStatement.getVariableDefinitions(), mapping);
    }

    // Determine if we're doing an insert-update or a delete
    PreparedStatement preparedStatement;
    boolean isInsertUpdate = true;
    if (deleteStatement == null) {
      // There is no delete statement, meaning deletesEnabled must be false. So just
      // do an insert/update.
      preparedStatement = insertUpdateStatement;
    } else {
      isInsertUpdate = isInsertUpdate(record, mapping, primaryKey);
      preparedStatement = isInsertUpdate ? insertUpdateStatement : deleteStatement;
    }
    BoundStatementBuilder builder = preparedStatement.boundStatementBuilder();
    ColumnDefinitions variableDefinitions = preparedStatement.getVariableDefinitions();
    for (String field : record.fields()) {
      bindColumnsToBuilder(recordMetadata, record, builder, variableDefinitions, field, false);
    }
    for (CqlIdentifier function : mapping.functions()) {
      bindColumnsToBuilder(
          recordMetadata, record, builder, variableDefinitions, function.asInternal(), true);
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

  private void bindColumnsToBuilder(
      RecordMetadata recordMetadata,
      Record record,
      BoundStatementBuilder builder,
      ColumnDefinitions variableDefinitions,
      String fieldOrFunction,
      boolean isFunction) {
    DataType cqlType;
    Object raw;
    Collection<CqlIdentifier> columns =
        mapping.fieldToColumns(CqlIdentifier.fromInternal(fieldOrFunction));
    if ((columns == null || columns.isEmpty()) && !allowExtraFields) {
      throw new ConfigException(
          "Extraneous field '"
              + getExternalName(fieldOrFunction)
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
        if (isFunction) {
          GenericType<?> fieldType = FunctionMapper.typeForFunction(fieldOrFunction);
          if (fieldType != null) {
            log.trace("binding function {} to column {}", fieldOrFunction, column.asInternal());
            bindColumn(
                builder,
                column,
                FunctionMapper.valueForFunction(fieldOrFunction),
                cqlType,
                fieldType);
          }
        } else {
          GenericType<?> fieldType = recordMetadata.getFieldType(fieldOrFunction, cqlType);
          if (fieldType != null) {
            raw =
                getFieldValueAndMaybeTransform(
                    record, fieldOrFunction, column, ttlTimeUnit, timestampTimeUnit);
            log.trace(
                "binding field {} with value {} to column {}",
                fieldOrFunction,
                raw,
                column.asInternal());
            bindColumn(builder, column, raw, cqlType, fieldType);
          }
        }
      }
    }
  }

  // Walk through each record field and check if any non-null fields map to a non-primary-key
  // column. If so, this is an insert; otherwise it is a delete. However, there is a
  // special case: if the table only has primary key columns, there is no case for delete.
  @VisibleForTesting
  static boolean isInsertUpdate(Record record, Mapping mapping, Set<CqlIdentifier> primaryKey) {
    if (mapping.getMappedColumns().equals(primaryKey)) {
      return true;
    }
    for (String field : record.fields()) {
      Object fieldValue = record.getFieldValue(field);
      if (fieldValue == null || fieldValue instanceof NullNode) {
        continue;
      }
      Collection<CqlIdentifier> mappedCols =
          mapping.fieldToColumns(CqlIdentifier.fromInternal(field));
      if (mappedCols != null) {
        for (CqlIdentifier mappedCol : mappedCols) {
          if (!primaryKey.contains(mappedCol)) {
            return true;
          }
        }
      }
    }
    return false;
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

  private <T> BoundStatementBuilder bindColumn(
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
        return builder;
      }
    }
    return builder.setBytesUnsafe(variable, bb);
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

  @VisibleForTesting
  static void ensureAllFieldsPresent(
      Set<String> recordFields, ColumnDefinitions variables, Mapping mapping) {
    for (int i = 0; i < variables.size(); i++) {
      CqlIdentifier variable = variables.get(i).getName();
      if (variable.asInternal().equals(SinkUtil.TIMESTAMP_VARNAME)) {
        // Not a real field; it's just the bound variable for the timestamp.
        continue;
      }
      CqlIdentifier field = mapping.columnToField(variable);
      if (field != null
          && isFieldValue(field.asInternal())
          && isValueSelfOnlyValueField(recordFields)) {
        // if kafka record value=null don't analyze fields mapped from value
        continue;
      }

      if (field != null && fieldIsAFunction(field)) {
        // if field is a function (i.e. now()) don't analyze it
        continue;
      }

      if (noFieldInRecord(recordFields, field)) {
        throw new ConfigException(
            "Required field '"
                + getExternalName(field.asInternal())
                + "' (mapped to column "
                + variable.asCql(true)
                + ") was missing from record. "
                + "Please remove it from the mapping. Or provided function does not exists.");
      }
    }
  }

  private static boolean fieldIsAFunction(CqlIdentifier functionName) {
    return SUPPORTED_FUNCTIONS_IN_MAPPING.contains(functionName);
  }

  private static boolean noFieldInRecord(Set<String> recordFields, CqlIdentifier field) {
    return field != null && !recordFields.contains(field.asInternal());
  }

  private static boolean isValueSelfOnlyValueField(Set<String> recordFields) {
    List<String> result =
        recordFields.stream().filter(RecordMapper::isFieldValue).collect(Collectors.toList());
    return result.size() == 1 && result.contains(VALUE_FIELD_NAME);
  }

  private static boolean isFieldValue(String variable) {
    return variable.startsWith("value.");
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
