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
package com.datastax.oss.kafka.sink;

import com.datastax.oss.kafka.sink.config.DseSinkConfig;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

/** Sink connector to insert Kafka records into DSE. */
public class DseSinkConnector extends SinkConnector {
  private String version;
  private Map<String, String> properties;

  @Override
  public String version() {
    if (version != null) {
      return version;
    }
    synchronized (this) {
      if (version != null) {
        return version;
      }

      // Get the version from version.txt.
      version = "UNKNOWN";
      try (InputStream versionStream = DseSinkConnector.class.getResourceAsStream("/version.txt")) {
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

  @Override
  public void start(Map<String, String> props) {
    properties = props;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return DseSinkTask.class;
  }

  /**
   * Invoked by the Connect infrastructure to retrieve the settings to pass to new {@link
   * DseSinkTask}'s. Since all configuration is actually managed by the DseSinkTask, all sink
   * properties are passed to tasks.
   *
   * @param maxTasks max number of tasks the infrastructure will create
   * @return list of collections of settings, one for each task
   */
  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    ArrayList<Map<String, String>> configs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      configs.add(properties);
    }
    return configs;
  }

  @Override
  public void stop() {}

  @Override
  public ConfigDef config() {
    return DseSinkConfig.GLOBAL_CONFIG_DEF;
  }
}
