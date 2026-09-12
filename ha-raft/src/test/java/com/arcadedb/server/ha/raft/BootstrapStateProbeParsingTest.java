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
    final HttpRequest request = BootstrapElection.bootstrapStateRequest("host:2480", null, false, "the-token",
        1234L);

    assertThat(request.uri().toString()).isEqualTo("http://host:2480/api/v1/cluster/bootstrap-state");
    assertThat(request.method()).isEqualTo("POST");
    assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).hasValue("the-token");
    assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User")).hasValue("root");
  }

  @Test
  void aBlankTokenIsOmittedRatherThanSentEmpty() {
    final HttpRequest request = BootstrapElection.bootstrapStateRequest("host:2480", null, false, "  ", 1234L);
    assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).isEmpty();
  }

  /**
   * The probe carries the cluster token, so on an SSL cluster it must not be the one dial in the package
   * that sends it in the clear (issue #7546). The rule is the one every other peer dial follows: HTTPS
   * when SSL is enabled AND an HTTPS address resolved, plain HTTP otherwise - never an HTTPS scheme
   * forced onto the plain HTTP port.
   */
  @Test
  void anSSLClusterProbesOverHTTPSWhenTheEncryptedEndpointIsKnown() {
    assertThat(BootstrapElection.bootstrapStateUrl("host:2480", "host:2490", true))
        .isEqualTo("https://host:2490/api/v1/cluster/bootstrap-state");
    assertThat(BootstrapElection.bootstrapStateRequest("host:2480", "host:2490", true, "the-token", 1234L)
        .uri().toString()).isEqualTo("https://host:2490/api/v1/cluster/bootstrap-state");
  }

  @Test
  void theProbeStaysOnPlainHTTPWithoutSSLOrWithoutAnEncryptedEndpoint() {
    // SSL off: the encrypted address, even when known, is not used.
    assertThat(BootstrapElection.bootstrapStateUrl("host:2480", "host:2490", false))
        .isEqualTo("http://host:2480/api/v1/cluster/bootstrap-state");
    // SSL on but no HTTPS endpoint resolved for the peer: the HTTP port keeps the HTTP scheme rather
    // than being dialled as if it spoke TLS.
    assertThat(BootstrapElection.bootstrapStateUrl("host:2480", null, true))
        .isEqualTo("http://host:2480/api/v1/cluster/bootstrap-state");
  }

  /**
   * The same endpoint is reached by {@link LeaderDatabaseQuery}, and the two must not disagree about the
   * scheme for one address pair - that disagreement is what left this probe on plain HTTP.
   */
  @Test
  void theProbeAgreesWithTheDatabaseListQueryAboutTheScheme() {
    for (final boolean useSSL : new boolean[] { true, false })
      for (final String httpsAddr : new String[] { "host:2490", null })
        assertThat(BootstrapElection.bootstrapStateUrl("host:2480", httpsAddr, useSSL))
            .isEqualTo(LeaderDatabaseQuery.chooseEndpoint("host:2480", httpsAddr, useSSL).url());
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
