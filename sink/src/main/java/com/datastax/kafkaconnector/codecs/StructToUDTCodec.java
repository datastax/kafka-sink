/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.codecs;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.kafkaconnector.record.StructDataMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/** Codec to convert a Kafka {@link Struct} to a UDT. */
public class StructToUDTCodec extends ConvertingCodec<Struct, UdtValue> {
  private final KafkaCodecRegistry codecRegistry;
  private final UserDefinedType definition;

  @SuppressWarnings("unchecked")
  StructToUDTCodec(KafkaCodecRegistry codecRegistry, UserDefinedType cqlType) {
    super((TypeCodec<UdtValue>) codecRegistry.codecFor(cqlType), Struct.class);

    this.codecRegistry = codecRegistry;
    definition = cqlType;
  }

  @Override
  public UdtValue externalToInternal(Struct external) {
    if (external == null) {
      return null;
    }

    int size = definition.getFieldNames().size();
    Schema schema = external.schema();
    StructDataMetadata structMetadata = new StructDataMetadata(schema);
    Set<String> structFieldNames =
        schema.fields().stream().map(Field::name).collect(Collectors.toSet());
    if (structFieldNames.size() != size) {
      throw new IllegalArgumentException(
          String.format("Expecting %d fields, got %d", size, structFieldNames.size()));
    }

    UdtValue value = definition.newValue();
    List<CqlIdentifier> fieldNames = definition.getFieldNames();
    List<DataType> fieldTypes = definition.getFieldTypes();
    assert (fieldNames.size() == fieldTypes.size());

    for (int idx = 0; idx < size; idx++) {
      CqlIdentifier udtFieldName = fieldNames.get(idx);
      DataType udtFieldType = fieldTypes.get(idx);

      if (!structFieldNames.contains(udtFieldName.asInternal())) {
        throw new IllegalArgumentException(
            String.format(
                "Field %s in UDT %s not found in input struct",
                udtFieldName, definition.getName()));
      }
      ConvertingCodec<Object, Object> fieldCodec =
          codecRegistry.convertingCodecFor(
              udtFieldType, structMetadata.getFieldType(udtFieldName.asInternal(), udtFieldType));
      Object o = fieldCodec.externalToInternal(external.get(udtFieldName.asInternal()));
      value.set(udtFieldName, o, fieldCodec.getInternalJavaType());
    }
    return value;
  }

  @Override
  public Struct internalToExternal(UdtValue internal) {
    if (internal == null) {
      return null;
    }
    throw new UnsupportedOperationException(
        "This codec does not support converting from Struct to UDT");
  }
}
