/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import org.junit.jupiter.api.Test;

import java.net.http.HttpRequest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The scheme the two dials that were still hardcoded to {@code http://} now choose (issue #7546, folded into
 * issue #7563): the bootstrap-state probe and the remote-shutdown command.
 * <p>
 * Both carry the cluster token - the probe in {@code X-ArcadeDB-Cluster-Token}, the shutdown in
 * {@code Authorization: Bearer} - so on a cluster with {@code arcadedb.ssl.enabled} set they were putting it
 * on the wire in clear text while every sibling dial was already encrypted. What is pinned here is the
 * DECISION in isolation; that a decision of {@code https} is then carried by a real TLS socket, against a
 * certificate issued for the name dialled and validated through the configured truststore, is pinned by
 * {@link Issue7563HaTlsPeerDialIT} against a live two-node cluster.
 * <p>
 * The three rows of each table are the three states an operator can be in, and the middle one is the one that
 * must NOT be a refusal: an SSL cluster that never declared the optional 5th field of
 * {@code arcadedb.ha.serverList} has only the plain listener to be reached on, and every sibling
 * ({@code LeaderDial}, {@link PeerCapabilityQuery#chooseUrl}, {@link LeaderDatabaseQuery#chooseEndpoint})
 * falls back there rather than breaking the cluster.
 */
class Issue7563PlaintextPeerDialSchemeTest {

  private static final String HTTP_ADDR  = "peer-1:2480";
  private static final String HTTPS_ADDR = "peer-1:2490";

  // ------------------------------------------------------------------ bootstrap-state probe

  @Test
  void theBootstrapProbeTakesTheEncryptedEndpointWhenSslIsOnAndOneResolves() {
    assertThat(BootstrapElection.chooseUrl(HTTP_ADDR, HTTPS_ADDR, true))
        .isEqualTo("https://peer-1:2490/api/v1/cluster/bootstrap-state");
  }

  @Test
  void theBootstrapProbeFallsBackToThePlainListenerWhenNoHttpsEndpointResolves() {
    assertThat(BootstrapElection.chooseUrl(HTTP_ADDR, null, true))
        .as("an SSL cluster that declared no 'https' ports must still be able to bootstrap")
        .isEqualTo("http://peer-1:2480/api/v1/cluster/bootstrap-state");
  }

  @Test
  void theBootstrapProbeIgnoresAnHttpsEndpointWhenSslIsOff() {
    assertThat(BootstrapElection.chooseUrl(HTTP_ADDR, HTTPS_ADDR, false))
        .isEqualTo("http://peer-1:2480/api/v1/cluster/bootstrap-state");
  }

  @Test
  void aPeerWithNoAddressAtAllYieldsNoUrlRatherThanAMalformedOne() {
    assertThat(BootstrapElection.chooseUrl(null, null, true)).isNull();
    assertThat(BootstrapElection.chooseUrl(null, null, false)).isNull();
  }

  @Test
  void theEncryptedProbeCarriesExactlyTheCredentialsThePlainOneDid() {
    final HttpRequest request = BootstrapElection.bootstrapStateRequestTo(
        BootstrapElection.chooseUrl(HTTP_ADDR, HTTPS_ADDR, true), "the-token", 1234L);

    assertThat(request.uri().getScheme()).isEqualTo("https");
    assertThat(request.method()).isEqualTo("POST");
    assertThat(request.headers().firstValue("X-ArcadeDB-Cluster-Token")).hasValue("the-token");
    assertThat(request.headers().firstValue("X-ArcadeDB-Forwarded-User")).hasValue("root");
  }

  // ------------------------------------------------------------------ remote shutdown

  @Test
  void theRemoteShutdownTakesTheEncryptedEndpointWhenSslIsOnAndOneResolves() {
    assertThat(RaftHAPlugin.shutdownUrl(new PeerDialAddress(HTTP_ADDR, HTTPS_ADDR, null, null), true))
        .isEqualTo("https://peer-1:2490/api/v1/server");
  }

  @Test
  void theRemoteShutdownFallsBackToThePlainListenerWhenNoHttpsEndpointResolves() {
    assertThat(RaftHAPlugin.shutdownUrl(new PeerDialAddress(HTTP_ADDR, null, null, null), true))
        .isEqualTo("http://peer-1:2480/api/v1/server");
  }

  @Test
  void theRemoteShutdownIgnoresAnHttpsEndpointWhenSslIsOff() {
    assertThat(RaftHAPlugin.shutdownUrl(new PeerDialAddress(HTTP_ADDR, HTTPS_ADDR, null, null), false))
        .isEqualTo("http://peer-1:2480/api/v1/server");
  }
}
