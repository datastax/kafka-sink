/*
 * Copyright DataStax, Inc.
 *
 *   This software is subject to the below license agreement.
 *   DataStax may make changes to the agreement from time to time,
 *   and will post the amended terms at
 *   https://www.datastax.com/terms/datastax-apache-kafka-connector-license-terms.
 */
package com.datastax.kafkaconnector.record;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KeyValueRecordTest {

  private KeyOrValue key;
  private KeyOrValue value;
  private Headers headers = new ConnectHeaders().addString("h1", "hv1").addString("h2", "hv2");
  private Map<String, String> keyFields =
      ImmutableMap.<String, String>builder().put("kf1", "kv1").put("kf2", "kv2").build();
  private Map<String, String> valueFields =
      ImmutableMap.<String, String>builder().put("vf1", "vv1").put("vf2", "vv2").build();

  @BeforeEach
  void setUp() {
    key =
        new KeyOrValue() {
          @Override
          public Set<String> fields() {
            return keyFields.keySet();
          }

          @Override
          public Object getFieldValue(String field) {
            return keyFields.get(field);
          }
        };

    value =
        new KeyOrValue() {
          @Override
          public Set<String> fields() {
            return valueFields.keySet();
          }

          @Override
          public Object getFieldValue(String field) {
            return valueFields.get(field);
          }
        };
  }

  @Test
  void should_qualify_field_names() {
    KeyValueRecord record = new KeyValueRecord(key, value, null, null);
    assertThat(record.fields()).containsOnly("key.kf1", "key.kf2", "value.vf1", "value.vf2");
  }

  @Test
  void should_qualify_field_names_and_headers() {
    KeyValueRecord record = new KeyValueRecord(key, value, null, headers);
    assertThat(record.fields())
        .containsOnly("key.kf1", "key.kf2", "value.vf1", "value.vf2", "header.h1", "header.h2");
  }

  @Test
  void should_qualify_field_names_keys_only() {
    KeyValueRecord record = new KeyValueRecord(key, null, null, null);
    assertThat(record.fields()).containsOnly("key.kf1", "key.kf2");
  }

  @Test
  void should_qualify_field_names_values_only() {
    KeyValueRecord record = new KeyValueRecord(null, value, null, null);
    assertThat(record.fields()).containsOnly("value.vf1", "value.vf2");
  }

  @Test
  void should_qualify_field_names_headers_only() {
    KeyValueRecord record = new KeyValueRecord(null, null, null, headers);
    assertThat(record.fields()).containsOnly("header.h1", "header.h2");
  }

  @Test
  void should_get_field_values() {
    KeyValueRecord record = new KeyValueRecord(key, value, null, headers);
    assertThat(record.getFieldValue("key.kf1")).isEqualTo("kv1");
    assertThat(record.getFieldValue("value.vf2")).isEqualTo("vv2");
    assertThat(record.getFieldValue("value.not_exist")).isNull();
    assertThat(record.getFieldValue("header.h1")).isEqualTo("hv1");
    assertThat(record.getFieldValue("header.not_exists")).isNull();
  }
}
