/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha.raft;

import com.arcadedb.server.ha.raft.ArcadeStateMachine.BootstrapBaseline;
import org.junit.jupiter.api.Test;

import java.net.http.HttpRequest;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the shared {@code /api/v1/cluster/bootstrap-state} probe helpers (issue #6124).
 * <p>
 * The bootstrap election's async fan-out and the post-bootstrap divergence verification now reach the
 * endpoint through the same request builder and the same response parser, so a change to either cannot
 * leave the two paths disagreeing about credentials or about what the response means.
 */
class BootstrapStateProbeParsingTest {

  private static final String BODY = """
      {"peerId":"n1","databases":[
        {"name":"alpha","fingerprint":"aaaa","lastTxId":12},
        {"name":"beta","fingerprint":"bbbb","lastTxId":7}
      ]}""";

  @Test
  void theRequestCarriesTheClusterCredentialsEveryPeerRpcUses() {
    final HttpRequest request = BootstrapElection.bootstrapStateRequest("host:2480", "the-token", 1234L);

    assertThat(request.uri().toString()).isEqualTo("http://host:2480/api/v1/cluster/bootstrap-state");
    assertThat(request.method()).isEqualTo("POST");
    assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).hasValue("the-token");
    assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User")).hasValue("root");
  }

  @Test
  void aBlankTokenIsOmittedRatherThanSentEmpty() {
    final HttpRequest request = BootstrapElection.bootstrapStateRequest("host:2480", "  ", 1234L);
    assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).isEmpty();
  }

  @Test
  void theResponseIsParsedIntoTheReportedStatePerDatabase() {
    final Map<String, BootstrapBaseline> states = BootstrapElection.parseBootstrapState(BODY, Set.of("alpha", "beta"));

    assertThat(states).hasSize(2);
    assertThat(states.get("alpha").fingerprint()).isEqualTo("aaaa");
    assertThat(states.get("alpha").lastTxId()).isEqualTo(12L);
    assertThat(states.get("beta").lastTxId()).isEqualTo(7L);
  }

  @Test
  void databasesOutsideTheFilterAreDropped() {
    final Map<String, BootstrapBaseline> states = BootstrapElection.parseBootstrapState(BODY, Set.of("beta"));

    assertThat(states).containsOnlyKeys("beta");
  }

  @Test
  void aNullFilterKeepsEveryReportedDatabase() {
    assertThat(BootstrapElection.parseBootstrapState(BODY, null)).containsOnlyKeys("alpha", "beta");
  }

  @Test
  void aMalformedBodyThrowsSoTheCallerCanTreatItAsAFailedProbe() {
    assertThatThrownBy(() -> BootstrapElection.parseBootstrapState("{\"peerId\":\"n1\"}", null))
        .isInstanceOf(RuntimeException.class);
  }
}
