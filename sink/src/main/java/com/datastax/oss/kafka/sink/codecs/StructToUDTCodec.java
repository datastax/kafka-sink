/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.kafka.sink.codecs;

import com.datastax.oss.common.sink.AbstractField;
import com.datastax.oss.common.sink.AbstractSchema;
import com.datastax.oss.common.sink.record.StructDataMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.kafka.sink.KafkaStruct;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/** Codec to convert a Kafka {@link Struct} to a UDT. */
public class StructToUDTCodec extends ConvertingCodec<KafkaStruct, UdtValue> {

  private final ConvertingCodecFactory codecFactory;
  private final UserDefinedType definition;
  private final int size;
  private final List<CqlIdentifier> udtFieldNames;
  private final List<DataType> udtFieldTypes;
  private final List<String> udtFieldNamesInternal;
  private final ConcurrentMap<Schema, StructPlan> plansBySchema = new ConcurrentHashMap<>();

  StructToUDTCodec(ConvertingCodecFactory codecFactory, UserDefinedType cqlType) {
    super(codecFactory.getCodecRegistry().codecFor(cqlType), KafkaStruct.class);
    this.codecFactory = codecFactory;
    definition = cqlType;
    udtFieldNames = definition.getFieldNames();
    udtFieldTypes = definition.getFieldTypes();
    size = udtFieldNames.size();
    assert (size == udtFieldTypes.size());
    udtFieldNamesInternal =
        udtFieldNames.stream().map(CqlIdentifier::asInternal).collect(Collectors.toList());
  }

  @Override
  public UdtValue externalToInternal(KafkaStruct external) {
    if (external == null) {
      return null;
    }

    StructPlan plan =
        plansBySchema.computeIfAbsent(
            external.kafkaSchema(), schema -> createPlan(external.schema()));
    UdtValue value = definition.newValue();
    for (FieldBinding binding : plan.bindings) {
      Object o = binding.codec.externalToInternal(external.get(binding.fieldNameInternal));
      value = setValue(value, binding.fieldName, o, binding.codecInternalType);
    }
    return value;
  }

  private static UdtValue setValue(
      UdtValue value, CqlIdentifier fieldName, Object raw, GenericType<Object> targetType) {
    return value.set(fieldName, raw, targetType);
  }

  private StructPlan createPlan(AbstractSchema schema) {
    StructDataMetadata structMetadata = new StructDataMetadata(schema);
    Set<String> structFieldNames =
        schema.fields().stream().map(AbstractField::name).collect(Collectors.toSet());
    if (structFieldNames.size() != size) {
      throw new IllegalArgumentException(
          String.format("Expecting %d fields, got %d", size, structFieldNames.size()));
    }

    FieldBinding[] bindings = new FieldBinding[size];
    for (int idx = 0; idx < size; idx++) {
      CqlIdentifier udtFieldName = udtFieldNames.get(idx);
      DataType udtFieldType = udtFieldTypes.get(idx);
      String fieldNameInternal = udtFieldNamesInternal.get(idx);

      if (!structFieldNames.contains(fieldNameInternal)) {
        throw new IllegalArgumentException(
            String.format(
                "Field %s in UDT %s not found in input struct",
                udtFieldName, definition.getName()));
      }

      @SuppressWarnings("unchecked")
      GenericType<Object> fieldType =
          (GenericType<Object>) structMetadata.getFieldType(fieldNameInternal, udtFieldType);
      ConvertingCodec<Object, Object> fieldCodec =
          codecFactory.createConvertingCodec(udtFieldType, fieldType, false);
      bindings[idx] = new FieldBinding(udtFieldName, fieldNameInternal, fieldCodec);
    }
    return new StructPlan(bindings);
  }

  private static final class StructPlan {
    private final FieldBinding[] bindings;

    private StructPlan(FieldBinding[] bindings) {
      this.bindings = bindings;
    }
  }

  private static final class FieldBinding {
    private final CqlIdentifier fieldName;
    private final String fieldNameInternal;
    private final ConvertingCodec<Object, Object> codec;
    private final GenericType<Object> codecInternalType;

    private FieldBinding(
        CqlIdentifier fieldName, String fieldNameInternal, ConvertingCodec<Object, Object> codec) {
      this.fieldName = fieldName;
      this.fieldNameInternal = fieldNameInternal;
      this.codec = codec;
      @SuppressWarnings("unchecked")
      GenericType<Object> internalType = (GenericType<Object>) codec.getInternalJavaType();
      this.codecInternalType = internalType;
    }
  }

  @Override
  public KafkaStruct internalToExternal(UdtValue internal) {
    if (internal == null) {
      return null;
    }
    throw new UnsupportedOperationException(
        "This codec does not support converting from Struct to UDT");
  }
}
