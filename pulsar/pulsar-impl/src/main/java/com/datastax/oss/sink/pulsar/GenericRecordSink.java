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
package com.datastax.oss.sink.pulsar;

import com.datastax.oss.sink.config.TopicConfig;
import com.datastax.oss.sink.pulsar.util.DataReader;
import java.util.Map;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.io.core.SinkContext;

public abstract class GenericRecordSink<Payload> extends BaseSink<GenericRecord, Payload> {

  @Override
  protected void beforeStart(Map<String, Object> config, SinkContext sinkContext)
      throws Exception {}

  @Override
  protected void onEachTopicConfig(
      String topic, TopicConfig topicConfig, SinkContext sinkContext) {}

  @Override
  protected void onValueReaderDetected(String topic, DataReader reader) {}

  @Override
  protected void release() {}
}
