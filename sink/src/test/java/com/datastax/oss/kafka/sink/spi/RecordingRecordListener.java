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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.connect.sink.SinkRecord;

/** Test double; public no-arg constructor required by {@link RecordListener#load}. */
public class RecordingRecordListener extends RecordListener {

  public static final AtomicInteger CONFIGURE_COUNT = new AtomicInteger();
  public static final List<SinkRecord> SUCCESSES = new ArrayList<SinkRecord>();
  public static final List<SinkRecord> FAILURES = new ArrayList<SinkRecord>();
  public static final List<String> FAILURE_CQL = new ArrayList<String>();

  public static void reset() {
    CONFIGURE_COUNT.set(0);
    SUCCESSES.clear();
    FAILURES.clear();
    FAILURE_CQL.clear();
  }

  @Override
  public void configure(Map<String, String> props) {
    CONFIGURE_COUNT.incrementAndGet();
  }

  @Override
  public void onSuccess(SinkRecord record) {
    SUCCESSES.add(record);
  }

  @Override
  public void onFailure(SinkRecord record, Throwable error, String cql) {
    FAILURES.add(record);
    FAILURE_CQL.add(cql);
  }
}
