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
package com.datastax.oss.kafka.sink.spi;

import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;

/** Configures, then throws from callbacks so the sink can prove fail-open behavior. */
public class ThrowingRecordListener extends RecordListener {

  public static boolean throwOnConfigure;

  @Override
  public void configure(Map<String, String> props) {
    if (throwOnConfigure) {
      throw new RuntimeException("configure boom");
    }
  }

  @Override
  public void onSuccess(SinkRecord record) {
    throw new RuntimeException("onSuccess boom");
  }

  @Override
  public void onFailure(SinkRecord record, Throwable error, String cql) {
    throw new RuntimeException("onFailure boom");
  }
}
