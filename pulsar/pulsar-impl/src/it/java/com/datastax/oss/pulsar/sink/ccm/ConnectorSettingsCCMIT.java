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
package com.datastax.oss.pulsar.sink.ccm;

import static com.datastax.oss.common.sink.config.CassandraSinkConfig.withDriverPrefix;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.oss.common.sink.state.InstanceState;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.sink.pulsar.GenericRecordImpl;
import com.datastax.oss.sink.pulsar.PulsarRecordImpl;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("medium")
class ConnectorSettingsCCMIT extends EndToEndCCMITBase {

  ConnectorSettingsCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session);
  }

  /** Test for KAF-135 */
  @Test
  void should_load_settings_from_dse_reference_conf() {
    // given (connector mapping need to be defined)
    taskConfigs.add(makeConnectorProperties("bigintcol=value.bigint, doublecol=value.double"));
    initConnectorAndTask();

    // when
    InstanceState instanceState = task.getInstanceState();

    // then setting from dse-reference.conf should be defined
    assertThat(
            instanceState
                .getSession()
                .getContext()
                .getConfig()
                .getDefaultProfile()
                .getInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE))
        .isGreaterThan(0);
  }

  @Test
  void should_insert_when_using_java_driver_contact_points_setting() {
    Map<String, Object> connectorProperties =
        makeConnectorPropertiesWithoutContactPointsAndPort("bigintcol=key, listcol=value.field1");
    // use single datastax-java-driver prefixed property that carry host:port
    connectorProperties.put(
        withDriverPrefix(DefaultDriverOption.CONTACT_POINTS),
        getContactPoints()
            .stream()
            .map(
                a -> {
                  InetSocketAddress inetSocketAddress = (InetSocketAddress) a.resolve();
                  return String.format(
                      "%s:%d", inetSocketAddress.getHostString(), inetSocketAddress.getPort());
                })
            .collect(Collectors.joining(",")));

    taskConfigs.add(connectorProperties);

    PulsarRecordImpl record =
        new PulsarRecordImpl(
            "persistent://tenant/namespace/mytopic",
            "98761234",
            new GenericRecordImpl().put("field1", "[42, 37]"),
            recordType);
    record.setRecordSequence(1234L);
    runTaskWithRecords(record);

    // Verify that the record was inserted properly in the database.
    List<Row> results = session.execute("SELECT bigintcol, listcol FROM types").all();
    assertThat(results.size()).isEqualTo(1);
    Row row = results.get(0);
    assertThat(row.getLong("bigintcol")).isEqualTo(98761234L);
    assertThat(row.getList("listcol", Integer.class)).isEqualTo(Arrays.asList(42, 37));
  }
}
