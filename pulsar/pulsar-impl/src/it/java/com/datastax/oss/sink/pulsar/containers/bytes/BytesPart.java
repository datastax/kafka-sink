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
package com.datastax.oss.sink.pulsar.containers.bytes;

import static org.assertj.core.api.Assertions.*;

import com.datastax.oss.sink.pulsar.BaseSink;
import com.datastax.oss.sink.pulsar.BytesSink;
import com.datastax.oss.sink.pulsar.containers.ContainersBase;
import com.datastax.oss.sink.pulsar.util.ConfigUtil;
import com.datastax.oss.sink.util.Tuple2;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class BytesPart extends ContainersBase {
  @Override
  protected Class<? extends BaseSink> sinkClass() {
    return BytesSink.class;
  }

  @BeforeAll
  public static void init() throws PulsarClientException {
    initClients();
  }

  @AfterAll
  public static void destroy() throws PulsarClientException {
    releaseClients();
  }

  protected void send(String topic, String key, byte[] value) throws PulsarClientException {
    send(topic, key, value, Collections.emptyMap());
  }

  protected void send(String topic, String key, byte[] value, Map<String, String> props)
      throws PulsarClientException {
    send(topic, key, value, null, props);
  }

  protected void send(String topic, String key, byte[] value, Long eventTime)
      throws PulsarClientException {
    send(topic, key, value, eventTime, Collections.emptyMap());
  }

  protected void send(
      String topic, String key, byte[] value, Long eventTime, Map<String, String> props)
      throws PulsarClientException {
    Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
    TypedMessageBuilder<byte[]> mb = producer.newMessage();
    if (key != null) mb = mb.key(key);
    if (value != null) mb = mb.value(value);
    mb = mb.properties(props);
    if (eventTime != null) mb = mb.eventTime(eventTime);
    mb.send();
    waitForProcessedMessages(topic, lastMessageNum(topic) + 1);
    producer.close();
  }

  protected void assertTtl(int ttlValue, Number expectedTtlValue) {
    if (expectedTtlValue.equals(0)) {
      assertThat(ttlValue).isEqualTo(expectedTtlValue.intValue());
    } else {
      // actual ttl value can be less that or equal to expectedTtlValue because some time may elapse
      // between the moment the record was inserted and retrieved from db.
      assertThat(ttlValue).isLessThanOrEqualTo(expectedTtlValue.intValue()).isGreaterThan(0);
    }
  }
}
