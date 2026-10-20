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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RecordListenerTest {

  @BeforeEach
  void reset() {
    RecordingRecordListener.reset();
    ThrowingRecordListener.throwOnConfigure = false;
  }

  @Test
  void blankConfigReturnsNoOp() {
    RecordListener listener =
        RecordListener.load(new HashMap<String, String>(), getClass().getClassLoader());
    assertThat(listener).isSameAs(RecordListener.NoOpRecordListener.INSTANCE);
    RecordListener.invokeSuccess(listener, record());
    RecordListener.invokeFailure(listener, record(), new RuntimeException("x"), "INSERT");
    RecordListener.invokeClose(listener);
  }

  @Test
  void classNameLoadsRecordingListener() {
    Map<String, String> props = new HashMap<String, String>();
    props.put(RecordListener.CLASS_CONFIG, RecordingRecordListener.class.getName());
    RecordListener listener = RecordListener.load(props, getClass().getClassLoader());
    assertThat(RecordingRecordListener.CONFIGURE_COUNT.get()).isEqualTo(1);
    SinkRecord ok = record();
    RecordListener.invokeSuccess(listener, ok);
    RecordListener.invokeFailure(listener, ok, new RuntimeException("cql"), "INSERT INTO t");
    RecordListener.invokeFailure(listener, ok, new RuntimeException("map"), null);
    assertThat(RecordingRecordListener.SUCCESSES).containsExactly(ok);
    assertThat(RecordingRecordListener.FAILURES).containsExactly(ok, ok);
    assertThat(RecordingRecordListener.FAILURE_CQL).containsExactly("INSERT INTO t", null);
    RecordListener.invokeClose(listener);
  }

  @Test
  void classAndPathLoadWhenDirectoryExists() throws Exception {
    File dir = Files.createTempDirectory("record-listener").toFile();
    dir.deleteOnExit();
    Map<String, String> props = new HashMap<String, String>();
    props.put(RecordListener.CLASS_CONFIG, RecordingRecordListener.class.getName());
    props.put(RecordListener.PATH_CONFIG, dir.getAbsolutePath());
    RecordListener listener = RecordListener.load(props, getClass().getClassLoader());
    assertThat(RecordingRecordListener.CONFIGURE_COUNT.get()).isEqualTo(1);
    RecordListener.invokeSuccess(listener, record());
    assertThat(RecordingRecordListener.SUCCESSES).hasSize(1);
    RecordListener.invokeClose(listener);
  }

  @Test
  void missingClassReturnsNoOp() {
    Map<String, String> props = new HashMap<String, String>();
    props.put(RecordListener.CLASS_CONFIG, "com.example.DoesNotExistListener");
    RecordListener listener = RecordListener.load(props, getClass().getClassLoader());
    assertThat(listener).isSameAs(RecordListener.NoOpRecordListener.INSTANCE);
  }

  @Test
  void missingPathReturnsNoOp() {
    Map<String, String> props = new HashMap<String, String>();
    props.put(RecordListener.CLASS_CONFIG, RecordingRecordListener.class.getName());
    props.put(RecordListener.PATH_CONFIG, "/tmp/record-listener-missing-" + System.nanoTime());
    RecordListener listener = RecordListener.load(props, getClass().getClassLoader());
    assertThat(listener).isSameAs(RecordListener.NoOpRecordListener.INSTANCE);
    assertThat(RecordingRecordListener.CONFIGURE_COUNT.get()).isZero();
  }

  @Test
  void pathWithoutClassReturnsNoOp() {
    Map<String, String> props = new HashMap<String, String>();
    props.put(RecordListener.PATH_CONFIG, "/tmp");
    RecordListener listener = RecordListener.load(props, getClass().getClassLoader());
    assertThat(listener).isSameAs(RecordListener.NoOpRecordListener.INSTANCE);
  }

  @Test
  void configureFailureReturnsNoOp() {
    ThrowingRecordListener.throwOnConfigure = true;
    Map<String, String> props = new HashMap<String, String>();
    props.put(RecordListener.CLASS_CONFIG, ThrowingRecordListener.class.getName());
    RecordListener listener = RecordListener.load(props, getClass().getClassLoader());
    assertThat(listener).isSameAs(RecordListener.NoOpRecordListener.INSTANCE);
  }

  @Test
  void callbackThrowIsSwallowed() {
    Map<String, String> props = new HashMap<String, String>();
    props.put(RecordListener.CLASS_CONFIG, ThrowingRecordListener.class.getName());
    RecordListener listener = RecordListener.load(props, getClass().getClassLoader());
    assertThat(listener).isNotSameAs(RecordListener.NoOpRecordListener.INSTANCE);
    assertThatCode(() -> RecordListener.invokeSuccess(listener, record()))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                RecordListener.invokeFailure(listener, record(), new RuntimeException("e"), "cql"))
        .doesNotThrowAnyException();
    RecordListener.invokeClose(listener);
  }

  private static SinkRecord record() {
    return new SinkRecord("t", 0, null, null, null, "v", 1L);
  }
}
