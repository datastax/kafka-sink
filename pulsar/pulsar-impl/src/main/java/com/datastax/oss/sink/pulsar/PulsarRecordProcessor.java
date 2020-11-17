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

import com.datastax.oss.sink.RecordProcessor;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarRecordProcessor extends RecordProcessor {

  private static final Logger log = LoggerFactory.getLogger(PulsarRecordProcessor.class);

  private PulsarSinkConnector connector;

  PulsarRecordProcessor(PulsarSinkConnector connector) {
    this.connector = connector;
  }

  @Override
  protected void beforeStart(Map<String, String> config) {}

  @Override
  protected void onProcessingStart() {}

  @Override
  protected void handleFailure(SinkRecord record, Throwable e, String cql, Runnable failCounter) {
    log.info("failure on {}", record);
    log.info("failed cql {}", cql);
    log.error(e.getMessage(), e);
    connector.onFailure(record, e);
  }

  @Override
  protected void handleSuccess(SinkRecord record) {
    connector.onSuccess(record);
  }

  @Override
  public String version() {
    return null;
  }
}
