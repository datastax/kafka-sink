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

import com.datastax.oss.common.sink.AbstractSinkRecord;
import com.datastax.oss.common.sink.AbstractSinkTask;
import com.datastax.oss.common.sink.state.InstanceState;
import com.datastax.oss.common.sink.util.SinkUtil;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Connector(
  name = "dssc-simple",
  type = IOType.SINK,
  help = "DataStax Pulsar Sink is used for moving messages from Pulsar to Cassandra",
  configClass = PulsarSinkConfig.class
)
public class CassandraSinkTask implements Sink<GenericRecord> {

  private static final String APPLICATION_NAME = "DataStax Pulsar Connector";
  private static final Logger log = LoggerFactory.getLogger(CassandraSinkTask.class);
  protected AbstractSinkTask processor;
  private String version;
  private final LocalSchemaRegistry schemaRegistry = new LocalSchemaRegistry();

  public CassandraSinkTask() {
    processor =
        new AbstractSinkTask() {
          @Override
          protected void handleFailure(
              AbstractSinkRecord record, Throwable e, String cql, Runnable failCounter) {
            failCounter.run();
            PulsarSinkRecordImpl impl = (PulsarSinkRecordImpl) record;
            log.error("Error while processing record {}, Statement: {} ", impl, cql, e);
            impl.getRecord().fail();
          }

          @Override
          protected void handleSuccess(AbstractSinkRecord record) {
            PulsarSinkRecordImpl impl = (PulsarSinkRecordImpl) record;
            log.debug("ack record {}", impl);
            impl.getRecord().ack();
          }

          @Override
          public String version() {
            return getVersion();
          }

          @Override
          public String applicationName() {
            return APPLICATION_NAME;
          }
        };
  }

  @Override
  public void open(Map<String, Object> cfg, SinkContext sc) {
    log.info("start {}, config {}", getClass().getName(), cfg);
    try {
      // TODO
      Map<String, String> processorConfig = ConfigUtil.flatString(cfg);
      processorConfig.put(SinkUtil.NAME_OPT, sc.getSinkName());
      processor.start(processorConfig);
      log.debug("started {}", getClass().getName(), processorConfig);
    } catch (Throwable ex) {
      log.error("initialization error", ex);
      close();
      throw ex;
    }
  }

  @Override
  public void write(Record<GenericRecord> record) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("write {}", record);
    }
    log.info("write {}", record);
    PulsarSinkRecordImpl pulsarSinkRecordImpl = buildRecordImpl(record);
    processor.put(Collections.singleton(pulsarSinkRecordImpl));
  }

  PulsarSinkRecordImpl buildRecordImpl(Record<GenericRecord> record) {
    // TODO: batch records, in Kafka the system sends batches, here we
    // are procesing only one record at a time
    PulsarSchema schema = schemaRegistry.ensureAndUpdateSchema(record);
    PulsarSinkRecordImpl pulsarSinkRecordImpl =
        new PulsarSinkRecordImpl(record, schema, schemaRegistry);
    return pulsarSinkRecordImpl;
  }

  @Override
  public void close() {
    if (processor != null) {
      processor.stop();
    }
  }

  private String getVersion() {
    if (version != null) {
      return version;
    }
    synchronized (this) {
      if (version != null) {
        return version;
      }

      // Get the version from version.txt.
      version = "UNKNOWN";
      try (InputStream versionStream =
          CassandraSinkTask.class.getResourceAsStream(
              "/com/datastax/oss/pulsar/sink/version.txt")) {
        if (versionStream != null) {
          BufferedReader reader =
              new BufferedReader(new InputStreamReader(versionStream, StandardCharsets.UTF_8));
          version = reader.readLine();
        }
      } catch (Exception e) {
        // swallow
      }
      return version;
    }
  }

  public AbstractSinkTask getProcessor() {
    return processor;
  }

  public InstanceState getInstanceState() {
    return processor.getInstanceState();
  }
}
