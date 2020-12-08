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
package com.datastax.oss.sink.pulsar.ccm.schemed;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.sink.pulsar.SchemedGenericRecordSink;
import com.datastax.oss.sink.pulsar.ccm.GenericRecordHeadersCCM;
import org.junit.jupiter.api.Tag;

@Tag("medium")
class SchemedHeadersCCMIT extends GenericRecordHeadersCCM {
  SchemedHeadersCCMIT(CCMCluster ccm, CqlSession session) {
    super(ccm, session, new SchemedGenericRecordSink());
  }
}
