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
package com.datastax.oss.sink.pulsar.containers;

import com.datastax.oss.sink.pulsar.containers.bytes.BytesDeletePart;
import com.datastax.oss.sink.pulsar.containers.bytes.BytesHeadersPart;
import com.datastax.oss.sink.pulsar.containers.bytes.BytesJsonEndToEndPart;
import com.datastax.oss.sink.pulsar.containers.bytes.BytesNowFunctionPart;
import com.datastax.oss.sink.pulsar.containers.bytes.BytesProvidedQueryPart;
import com.datastax.oss.sink.pulsar.containers.bytes.BytesRawDataEndToEndPart;
import com.datastax.oss.sink.pulsar.containers.bytes.BytesStructEndToEndPart;
import com.datastax.oss.sink.pulsar.containers.bytes.BytesWriteTimestampAndTtlPart;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;

@Tag("containers")
class MappingSuiteIT {

  @Nested
  class Bytes {
    @Nested
    class Delete extends BytesDeletePart {}

    @Nested
    class Headers extends BytesHeadersPart {}

    @Nested
    class JsonEndToEnd extends BytesJsonEndToEndPart {}

    @Nested
    class NowFunction extends BytesNowFunctionPart {}

    @Nested
    class ProvidedQuery extends BytesProvidedQueryPart {}

    @Nested
    class RawDataEndToEnd extends BytesRawDataEndToEndPart {}

    @Nested
    class StructEndToEnd extends BytesStructEndToEndPart {}

    @Nested
    class WriteTimestampAndTtl extends BytesWriteTimestampAndTtlPart {}
  }
}
