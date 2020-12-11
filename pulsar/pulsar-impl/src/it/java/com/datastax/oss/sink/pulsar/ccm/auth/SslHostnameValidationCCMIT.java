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
package com.datastax.oss.sink.pulsar.ccm.auth;

import static com.datastax.oss.driver.api.testinfra.ccm.CcmBridge.*;
import static com.datastax.oss.sink.config.SslConfig.*;
import static com.datastax.oss.sink.pulsar.TestUtil.*;
import static org.assertj.core.api.Assertions.*;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig;
import com.datastax.oss.sink.pulsar.BytesSink;
import com.datastax.oss.sink.pulsar.TestUtil;
import com.datastax.oss.sink.pulsar.ccm.EndToEndCCMITBase;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@CCMConfig(ssl = true, hostnameVerification = true)
@Tag("medium")
class SslHostnameValidationCCMIT extends EndToEndCCMITBase<byte[]> {
  public SslHostnameValidationCCMIT(CCMCluster ccm, @SessionConfig(ssl = true) CqlSession session) {
    super(ccm, session, new BytesSink(true));
  }

  @Test
  void raw_bigint_value() throws Exception {
    Map<String, Object> extras =
        ImmutableMap.<String, Object>builder()
            .put(PROVIDER_OPT, "JDK")
            .put(KEYSTORE_PATH_OPT, DEFAULT_CLIENT_KEYSTORE_FILE.getAbsolutePath())
            .put(KEYSTORE_PASSWORD_OPT, CcmBridge.DEFAULT_CLIENT_KEYSTORE_PASSWORD)
            .put(TRUSTSTORE_PATH_OPT, DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath())
            .put(TRUSTSTORE_PASSWORD_OPT, DEFAULT_CLIENT_TRUSTSTORE_PASSWORD)
            .build();

    conn.open(makeConnectorProperties(extras), null);

    Record<byte[]> record = TestUtil.mockRecord("mytopic", null, longBytes(5725368L), 1234L);
    sendRecord(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }

  @Test
  void raw_bigint_value_with_openssl() throws Exception {
    Map<String, Object> extras =
        ImmutableMap.<String, Object>builder()
            .put(PROVIDER_OPT, "OpenSSL")
            .put(OPENSSL_KEY_CERT_CHAIN_OPT, DEFAULT_CLIENT_CERT_CHAIN_FILE.getAbsolutePath())
            .put(OPENSSL_PRIVATE_KEY_OPT, DEFAULT_CLIENT_PRIVATE_KEY_FILE.getAbsolutePath())
            .put(TRUSTSTORE_PATH_OPT, DEFAULT_CLIENT_TRUSTSTORE_FILE.getAbsolutePath())
            .put(TRUSTSTORE_PASSWORD_OPT, DEFAULT_CLIENT_TRUSTSTORE_PASSWORD)
            .build();

    conn.open(makeConnectorProperties(extras), null);

    Record<byte[]> record = TestUtil.mockRecord("mytopic", null, longBytes(5725368L), 1234L);
    sendRecord(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(5725368L);
  }
}
