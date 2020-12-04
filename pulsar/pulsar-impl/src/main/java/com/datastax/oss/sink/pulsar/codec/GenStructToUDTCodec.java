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
package com.datastax.oss.sink.pulsar.codec;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.sink.pulsar.GenAPIAdapter;
import com.datastax.oss.sink.pulsar.gen.GenSchema;
import com.datastax.oss.sink.pulsar.gen.GenStruct;
import com.datastax.oss.sink.record.StructDataMetadata;
import java.util.List;
import java.util.Set;
import org.apache.avro.util.Utf8;

/** Codec to convert a Pulsar {@link GenStruct} to a UDT. */
public class GenStructToUDTCodec extends ConvertingCodec<GenStruct, UdtValue> {

  private final ConvertingCodecFactory codecFactory;
  private final UserDefinedType definition;
  private static GenAPIAdapter adapter = new GenAPIAdapter();

  GenStructToUDTCodec(ConvertingCodecFactory codecFactory, UserDefinedType cqlType) {
    super(codecFactory.getCodecRegistry().codecFor(cqlType), GenStruct.class);
    this.codecFactory = codecFactory;
    definition = cqlType;
  }

  @Override
  public UdtValue externalToInternal(GenStruct external) {
    if (external == null) {
      return null;
    }

    int size = definition.getFieldNames().size();
    GenSchema.StructGenSchema schema = (GenSchema.StructGenSchema) external.getSchema();
    StructDataMetadata<GenSchema> structMetadata = new StructDataMetadata<>(schema, adapter);
    Set<String> structFieldNames = schema.fields();
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
      @SuppressWarnings("unchecked")
      GenericType<Object> fieldType =
          (GenericType<Object>)
              structMetadata.getFieldType(udtFieldName.asInternal(), udtFieldType);
      ConvertingCodec<Object, Object> fieldCodec =
          codecFactory.createConvertingCodec(udtFieldType, fieldType, false);
      Object fv = external.value(udtFieldName.asInternal());
      if (fv instanceof Utf8) fv = fv.toString();
      Object o = fieldCodec.externalToInternal(fv);
      value = value.set(udtFieldName, o, fieldCodec.getInternalJavaType());
    }
    return value;
  }

  @Override
  public GenStruct internalToExternal(UdtValue internal) {
    if (internal == null) {
      return null;
    }
    throw new UnsupportedOperationException(
        "This codec does not support converting from Struct to UDT");
  }
}
