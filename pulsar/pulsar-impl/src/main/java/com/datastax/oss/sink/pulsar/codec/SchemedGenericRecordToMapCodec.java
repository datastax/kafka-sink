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
import com.datastax.oss.sink.pulsar.AvroAPIAdapter;
import com.datastax.oss.sink.pulsar.SchemedGenericRecord;
import com.datastax.oss.sink.record.StructDataMetadataSupport;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/** Codec to convert an avro {@link GenericRecord} to a Map. */
public class SchemedGenericRecordToMapCodec extends ConvertingCodec<SchemedGenericRecord, Map> {

  private final ConvertingCodecFactory codecFactory;
  private final MapType definition;
  private static AvroAPIAdapter<?> adapter = new AvroAPIAdapter<>();

  SchemedGenericRecordToMapCodec(ConvertingCodecFactory codecFactory, MapType cqlType) {
    super(codecFactory.getCodecRegistry().codecFor(cqlType), SchemedGenericRecord.class);
    this.codecFactory = codecFactory;
    definition = cqlType;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map externalToInternal(SchemedGenericRecord record) {
    if (record == null) {
      return null;
    }

    Map result = new HashMap<>();

    Schema schema = record.getSchema();
    Schema valueSchema = schema.getFields().get(0).schema();
    GenericType valueGenType = StructDataMetadataSupport.TYPE_MAP.get(adapter.type(valueSchema));
    ConvertingCodec codec =
        codecFactory.createConvertingCodec(definition.getValueType(), valueGenType, false);

    for (Schema.Field field : schema.getFields()) {
      result.put(field.name(), codec.externalToInternal(record.getField(field.name())));
    }

    return result;
  }

  @Override
  public SchemedGenericRecord internalToExternal(Map internal) {
    if (internal == null) {
      return null;
    }
    throw new UnsupportedOperationException(
        "This codec does not support converting from Struct to UDT");
  }
}
