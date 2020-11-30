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

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;

public class Utf8ToStringGenericDatumReader<D> extends GenericDatumReader<D> {

  public Utf8ToStringGenericDatumReader() {}

  public Utf8ToStringGenericDatumReader(Schema schema) {
    super(schema);
  }

  public Utf8ToStringGenericDatumReader(Schema writer, Schema reader) {
    super(writer, reader);
  }

  public Utf8ToStringGenericDatumReader(Schema writer, Schema reader, GenericData data) {
    super(writer, reader, data);
  }

  public Utf8ToStringGenericDatumReader(GenericData data) {
    super(data);
  }

  @Override
  protected Object createString(String value) {
    return value;
  }

  @Override
  protected Object readString(Object old, Decoder in) throws IOException {
    Object object = super.readString(old, in);
    return object == null ? null : object.toString();
  }

  @Override
  protected Object readString(Object old, Schema expected, Decoder in) throws IOException {
    return super.readString(old, expected, in);
  }
}
