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
package com.datastax.oss.sink.pulsar.util;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.sink.pulsar.util.kite.JsonUtil;
import com.datastax.oss.sink.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

public interface DataReader<D> {

  D read(byte[] data) throws IOException;

  D read(String data) throws IOException;

  DataReader<GenericContainer> WORN_AVRO = new WornAvroReader();
  DataReader<GenericContainer> WORN_JSON = new WornJsonReader();
  DataReader<ByteBuffer> BLOB = new BlobReader();
  DataReader<String> STRING = new StringReader();
  DataReader<Long> LONG = new LongReader();
  DataReader<UUID> UUID = new UUIDReader();
  DataReader<Integer> INT = new IntReader();
  DataReader<Float> FLOAT = new FloatReader();
  DataReader<Double> DOUBLE = new DoubleReader();
  DataReader<Short> SHORT = new ShortReader();
  DataReader<Byte> BYTE = new ByteReader();
  DataReader<Boolean> BOOLEAN = new BooleanReader();

  Map<Integer, DataReader> PREDEFS =
      ImmutableMap.<Integer, DataReader>builder()
          .put(ProtocolConstants.DataType.ASCII, STRING)
          .put(ProtocolConstants.DataType.VARCHAR, STRING)
          .put(ProtocolConstants.DataType.BIGINT, LONG)
          .put(ProtocolConstants.DataType.COUNTER, LONG)
          .put(ProtocolConstants.DataType.BLOB, BLOB)
          .put(ProtocolConstants.DataType.INT, INT)
          .put(ProtocolConstants.DataType.SMALLINT, SHORT)
          .put(ProtocolConstants.DataType.VARINT, INT)
          .put(ProtocolConstants.DataType.TINYINT, BYTE)
          .put(ProtocolConstants.DataType.UUID, UUID)
          .put(ProtocolConstants.DataType.TIMEUUID, UUID)
          .put(ProtocolConstants.DataType.FLOAT, FLOAT)
          .put(ProtocolConstants.DataType.DOUBLE, DOUBLE)
          .put(ProtocolConstants.DataType.DECIMAL, DOUBLE)
          .put(ProtocolConstants.DataType.BOOLEAN, BOOLEAN)
          .build();

  static Optional<DataReader> get(DataType type) {
    return Optional.ofNullable(PREDEFS.get(type.getProtocolCode()));
  }

  static DataReader<GenericContainer> createSingleAvro(Schema schema) {
    return new SingleAvroReader(schema);
  }

  class SingleAvroReader implements DataReader<GenericContainer> {
    private Schema schema;

    public SingleAvroReader(Schema schema) {
      this.schema = schema;
    }

    private DecoderFactory decoderFactory = DecoderFactory.get();

    @Override
    public GenericContainer read(byte[] data) throws IOException {
      DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
      return reader.read(null, decoderFactory.binaryDecoder(data, null));
    }

    @Override
    public GenericContainer read(String data) throws IOException {
      DatumReader<GenericRecord> reader = new GenericDatumReader<>();
      return reader.read(null, decoderFactory.jsonDecoder(schema, data));
    }
  }

  class WornAvroReader implements DataReader<GenericContainer> {
    @Override
    public GenericContainer read(byte[] data) throws IOException {
      DatumReader<GenericContainer> reader = new GenericDatumReader<>();
      try (DataFileReader<GenericContainer> drdr =
          new DataFileReader<>(new SeekableByteArrayInput(data), reader)) {
        return drdr.next();
      }
    }

    @Override
    public GenericContainer read(String data) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  class WornJsonReader implements DataReader<GenericContainer> {
    @Override
    public GenericContainer read(byte[] data) throws IOException {
      JsonNode node = mapper.readTree(data);
      if (!node.isObject() && !node.isArray()) throw new JsonIsNotContainer(node);
      //            Schema s = new JsonToAvroSchema(mapper).infer(node, "_", "inferred._._");
      Schema s = JsonUtil.inferSchema(node, "_");
      DatumReader<GenericContainer> jrdr = new GenericDatumReader<>(s);
      return jrdr.read(null, DecoderFactory.get().jsonDecoder(s, new ByteArrayInputStream(data)));
    }

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public GenericContainer read(String data) throws IOException {
      JsonNode node = mapper.readTree(data);
      if (!node.isObject() && !node.isArray()) throw new JsonIsNotContainer(node);
      //            Schema s = new JsonToAvroSchema(mapper).infer(node, "_", "inferred._._");
      Schema s = JsonUtil.inferSchema(node, "_");
      DatumReader<GenericContainer> jrdr = new GenericDatumReader<>(s);
      return jrdr.read(null, DecoderFactory.get().jsonDecoder(s, data));
    }
  }

  class BooleanReader implements DataReader<Boolean> {
    @Override
    public Boolean read(byte[] data) throws IOException {
      return null;
    }

    @Override
    public Boolean read(String data) throws IOException {
      return Boolean.valueOf(data);
    }
  }

  class BlobReader implements DataReader<ByteBuffer> {

    @Override
    public ByteBuffer read(byte[] data) {
      return ByteBuffer.wrap(data);
    }

    @Override
    public ByteBuffer read(String data) {
      return ByteBuffer.wrap(StringUtil.stringToBytes(data));
    }
  }

  class StringReader implements DataReader<String> {
    @Override
    public String read(byte[] data) throws IOException {
      return new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public String read(String data) {
      return data;
    }
  }

  class LongReader implements DataReader<Long> {
    @Override
    public Long read(byte[] data) throws IOException {
      return ByteBuffer.wrap(data).getLong();
    }

    @Override
    public Long read(String data) {
      return Long.valueOf(data);
    }
  }

  class UUIDReader implements DataReader<UUID> {
    @Override
    public UUID read(byte[] data) throws IOException {
      ByteBuffer bb = ByteBuffer.wrap(data);
      return new UUID(bb.getLong(), bb.getLong());
    }

    @Override
    public UUID read(String data) {
      return java.util.UUID.fromString(data);
    }
  }

  class IntReader implements DataReader<Integer> {
    @Override
    public Integer read(byte[] data) throws IOException {
      return ByteBuffer.wrap(data).getInt();
    }

    @Override
    public Integer read(String data) {
      return Integer.valueOf(data);
    }
  }

  class FloatReader implements DataReader<Float> {
    @Override
    public Float read(byte[] data) throws IOException {
      return ByteBuffer.wrap(data).getFloat();
    }

    @Override
    public Float read(String data) {
      return Float.valueOf(data);
    }
  }

  class DoubleReader implements DataReader<Double> {
    @Override
    public Double read(byte[] data) throws IOException {
      return ByteBuffer.wrap(data).getDouble();
    }

    @Override
    public Double read(String data) {
      return Double.valueOf(data);
    }
  }

  class ShortReader implements DataReader<Short> {
    @Override
    public Short read(byte[] data) throws IOException {
      return ByteBuffer.wrap(data).getShort();
    }

    @Override
    public Short read(String data) {
      return Short.valueOf(data);
    }
  }

  class ByteReader implements DataReader<Byte> {
    @Override
    public Byte read(byte[] data) throws IOException {
      return data[0];
    }

    @Override
    public Byte read(String data) {
      return Byte.valueOf(data);
    }
  }

  static boolean mayContainAvroSchema(byte[] bs) {
    return bs.length > 4 && bs[0] == 0x4f && bs[1] == 0x62 && bs[2] == 0x6a && bs[3] == 0x01;
  }

  static Object primitiveValue(JsonNode node) {
    if (node.isTextual()) return node.textValue();
    if (node.isDouble()) return node.doubleValue();
    if (node.isFloat()) return node.floatValue();
    //    if (node.isInt()) return node.intValue();
    if (node.isLong() || node.isInt()) return node.longValue();
    if (node.isBoolean()) return node.booleanValue();
    return node.asText();
  }
}
