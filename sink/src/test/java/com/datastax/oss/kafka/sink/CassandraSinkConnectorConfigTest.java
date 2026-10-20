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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.kafka.sink.spi.RecordListener;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;

class CassandraSinkConnectorConfigTest {

  @Test
  void recordListenerKeysAreOnConfigDefAndOffByDefault() {
    ConfigDef configDef = new CassandraSinkConnector().config();
    assertThat(configDef.configKeys())
        .containsKeys(RecordListener.CLASS_CONFIG, RecordListener.PATH_CONFIG);
    assertThat(configDef.defaultValues().get(RecordListener.CLASS_CONFIG)).isNull();
    assertThat(configDef.defaultValues().get(RecordListener.PATH_CONFIG)).isNull();
    assertThat(configDef.configKeys()).doesNotContainKeys("source.name", "connector.type");
  }
}
