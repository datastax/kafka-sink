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

import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.sink.pulsar.GenAPIAdapter;
import com.datastax.oss.sink.pulsar.gen.GenSchema;
import com.datastax.oss.sink.pulsar.gen.GenStruct;
import com.datastax.oss.sink.record.StructDataMetadataSupport;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;

/** Codec to convert an avro {@link GenericRecord} to a Map. */
public class GenStructToMapCodec extends ConvertingCodec<GenStruct, Map> {

  private final ConvertingCodecFactory codecFactory;
  private final MapType definition;
  private static GenAPIAdapter adapter = new GenAPIAdapter();

  GenStructToMapCodec(ConvertingCodecFactory codecFactory, MapType cqlType) {
    super(codecFactory.getCodecRegistry().codecFor(cqlType), GenStruct.class);
    this.codecFactory = codecFactory;
    definition = cqlType;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map externalToInternal(GenStruct record) {
    if (record == null) {
      return null;
    }

    Map result = new HashMap<>();

    GenSchema.StructGenSchema schema = record.getSchema();
    GenSchema valueSchema = schema.field(schema.fieldNames().iterator().next());
    System.out.println(
        "adapter.type(valuesSchema) " + adapter.type(valueSchema) + " " + valueSchema.type);
    GenericType valueGenType = StructDataMetadataSupport.getGenericType(valueSchema, adapter);
    ConvertingCodec codec =
        codecFactory.createConvertingCodec(definition.getValueType(), valueGenType, false);

    for (String field : schema.fieldNames()) {
      result.put(field, codec.externalToInternal(record.value(field)));
    }

    return result;
  }

  @Override
  public GenStruct internalToExternal(Map internal) {
    if (internal == null) {
      return null;
    }
    throw new UnsupportedOperationException(
        "This codec does not support converting from Struct to UDT");
  }
}
