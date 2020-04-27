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
package com.datastax.oss.kafka.sink.ccm.auth;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig;
import com.datastax.oss.kafka.sink.ccm.EndToEndCCMITBase;
import com.datastax.oss.kafka.sink.config.SslConfig;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ConstantConditions")
@CCMConfig(ssl = true, hostnameVerification = true)
@Tag("medium")
class SslHostnameValidationCCMIT extends EndToEndCCMITBase {
  public SslHostnameValidationCCMIT(CCMCluster ccm, @SessionConfig(ssl = true) CqlSession session) {
    super(ccm, session);
  }

  @Test
  void raw_bigint_value() {
    Map<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(SslConfig.PROVIDER_OPT, "JDK")
            .put(
                SslConfig.KEYSTORE_PATH_OPT,
                CcmBridge.DEFAULT_CLIENT_KEYSTORE_FILE.getAbsolutePath())
            .put(SslConfig.KEYSTORE_PASSWORD_OPT, CcmBridge.DEFAULT_CLIENT_KEYSTORE_PASSWORD)
            .put(
                SslConfig.TRUSTSTORE_PATH_OPT,
                CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath())
            .put(SslConfig.TRUSTSTORE_PASSWORD_OPT, CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD)
            .build();

    conn.start(makeConnectorProperties(extras));

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, 5725368L, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void raw_bigint_value_with_openssl() {
    Map<String, String> extras =
        ImmutableMap.<String, String>builder()
            .put(SslConfig.PROVIDER_OPT, "OpenSSL")
            .put(
                SslConfig.OPENSSL_KEY_CERT_CHAIN_OPT,
                CcmBridge.DEFAULT_CLIENT_CERT_CHAIN_FILE.getAbsolutePath())
            .put(
                SslConfig.OPENSSL_PRIVATE_KEY_OPT,
                CcmBridge.DEFAULT_CLIENT_PRIVATE_KEY_FILE.getAbsolutePath())
            .put(
                SslConfig.TRUSTSTORE_PATH_OPT,
                CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath())
            .put(SslConfig.TRUSTSTORE_PASSWORD_OPT, CcmBridge.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD)
            .build();

    conn.start(makeConnectorProperties(extras));

    SinkRecord record = new SinkRecord("mytopic", 0, null, null, null, 5725368L, 1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in DSE.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }
}
