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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.http.HttpServer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression test for issue #8033: the bootstrap election's peer fan-out - both the state collection and the
 * pass-conclusion broadcast - chose each peer's HTTPS endpoint through the RAW resolver, which DERIVES a missing
 * {@code https} port as the peer's Raft host plus <em>this</em> node's HTTPS port. On a cluster whose nodes differ
 * by port rather than by host, with the {@code http} ports declared and the {@code https} ones not, every peer
 * derived to this node's own listener, {@code chooseUrl} preferred it over the declared HTTP endpoint, and the
 * election collected its own state once per peer and filed it under each remote peer's id.
 * <p>
 * Driven against a real {@link RaftHAServer} built from a server list with Ratis never started: the live peer set
 * falls back to the declared group and the address maps are populated by the constructor, so these assertions bind
 * the real resolution rather than a stubbed answer.
 */
class Issue8033BootstrapElectionPeerProbeUrlTest {

  private static final int LOCAL_HTTP_PORT  = 2480;
  private static final int LOCAL_HTTPS_PORT = 2490;

  /**
   * The reported shape: one host, distinct declared {@code http} ports, no {@code https} ports. Every peer's derived
   * HTTPS endpoint is {@code localhost:2490}, this node's own listener, so it must be withheld and the probe must go
   * to the declared HTTP endpoint of the peer it is meant for.
   */
  @Test
  void derivedHttpsEndpointCollapsedOntoThisNodeIsNotProbed() {
    final BootstrapElection election = newElection("localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");

    final Map<RaftPeerId, String> urls = election.peerProbeUrls(true);

    assertThat(urls).containsOnlyKeys(RaftPeerId.valueOf("localhost_2435"), RaftPeerId.valueOf("localhost_2436"));
    assertThat(urls.get(RaftPeerId.valueOf("localhost_2435")))
        .as("the derived HTTPS endpoint is this node's own listener; the declared HTTP one names the peer")
        .isEqualTo("http://localhost:2481" + BootstrapElection.BOOTSTRAP_STATE_ROUTE);
    assertThat(urls.get(RaftPeerId.valueOf("localhost_2436")))
        .isEqualTo("http://localhost:2482" + BootstrapElection.BOOTSTRAP_STATE_ROUTE);
    assertThat(urls.values()).noneMatch(url -> url != null && url.contains(":" + LOCAL_HTTPS_PORT));
  }

  /**
   * With neither port declared the withheld HTTPS endpoint leaves nothing to probe: the peer must come back
   * unresolved, so the election logs it and counts it in {@code assumedEmpty} rather than silently answering for it.
   */
  @Test
  void aPeerWithNoUsableEndpointIsReportedUnresolved() {
    final BootstrapElection election = newElection("localhost:2434,localhost:2435,localhost:2436");

    final Map<RaftPeerId, String> urls = election.peerProbeUrls(true);

    assertThat(urls).containsOnlyKeys(RaftPeerId.valueOf("localhost_2435"), RaftPeerId.valueOf("localhost_2436"));
    assertThat(urls.values()).containsOnlyNulls();
  }

  /**
   * The shape the derive fallback was written for, a Kubernetes StatefulSet: peers differ by host and share one
   * HTTPS port, so the derived endpoint identifies its peer and the fan-out keeps probing over TLS.
   */
  @Test
  void aDerivedHttpsEndpointThatIdentifiesItsPeerIsStillUsed() {
    final BootstrapElection election = newElection("node-a:2434:2480,node-b:2434:2480,node-c:2434:2480");

    final Map<RaftPeerId, String> urls = election.peerProbeUrls(true);

    assertThat(urls.get(RaftPeerId.valueOf("node-b_2434")))
        .isEqualTo("https://node-b:" + LOCAL_HTTPS_PORT + BootstrapElection.BOOTSTRAP_STATE_ROUTE);
    assertThat(urls.get(RaftPeerId.valueOf("node-c_2434")))
        .isEqualTo("https://node-c:" + LOCAL_HTTPS_PORT + BootstrapElection.BOOTSTRAP_STATE_ROUTE);
  }

  /** Declared, distinct HTTPS ports stay on TLS: the guard does not cost a correctly configured cluster its TLS. */
  @Test
  void declaredDistinctHttpsEndpointsAreStillUsed() {
    final BootstrapElection election = newElection(
        "localhost:2434:2480:0:2490,localhost:2435:2481:0:2491,localhost:2436:2482:0:2492");

    final Map<RaftPeerId, String> urls = election.peerProbeUrls(true);

    assertThat(urls.get(RaftPeerId.valueOf("localhost_2435")))
        .isEqualTo("https://localhost:2491" + BootstrapElection.BOOTSTRAP_STATE_ROUTE);
    assertThat(urls.get(RaftPeerId.valueOf("localhost_2436")))
        .isEqualTo("https://localhost:2492" + BootstrapElection.BOOTSTRAP_STATE_ROUTE);
  }

  /** A declared HTTPS endpoint that is this node's own, spelled differently, is withheld the same way. */
  @Test
  void aDeclaredHttpsEndpointThatIsThisNodesOwnIsNotProbed() {
    final BootstrapElection election = newElection(
        "127.0.0.1:2434:2480:0:2490,localhost:2435:2481:0:2490,localhost:2436:2482:0:2492");

    final Map<RaftPeerId, String> urls = election.peerProbeUrls(true);

    assertThat(urls.get(RaftPeerId.valueOf("localhost_2435")))
        .as("localhost:2490 and 127.0.0.1:2490 are one socket, and it is ours")
        .isEqualTo("http://localhost:2481" + BootstrapElection.BOOTSTRAP_STATE_ROUTE);
    assertThat(urls.get(RaftPeerId.valueOf("localhost_2436")))
        .isEqualTo("https://localhost:2492" + BootstrapElection.BOOTSTRAP_STATE_ROUTE);
  }

  /**
   * The plain-HTTP half answers the same two questions. A peer that DECLARES this node's own {@code http} endpoint,
   * spelled the other way round, would be probed on our own listener and file our answer under its id - with SSL off
   * as much as on.
   */
  @Test
  void aDeclaredHttpEndpointThatIsThisNodesOwnIsNotProbed() {
    final BootstrapElection election = newElection("127.0.0.1:2434:2480,localhost:2435:2480,localhost:2436:2482");

    final Map<RaftPeerId, String> urls = election.peerProbeUrls(false);

    assertThat(urls.get(RaftPeerId.valueOf("localhost_2435")))
        .as("localhost:2480 and 127.0.0.1:2480 are one socket, and it is ours")
        .isNull();
    assertThat(urls.get(RaftPeerId.valueOf("localhost_2436")))
        .isEqualTo("http://localhost:2482" + BootstrapElection.BOOTSTRAP_STATE_ROUTE);
  }

  /** Two peers declaring one {@code http} endpoint: it identifies at most one of them, so neither is probed on it. */
  @Test
  void aDeclaredHttpEndpointSharedByTwoPeersIsNotProbed() {
    final BootstrapElection election = newElection("localhost:2434:2480,localhost:2435:2481,localhost:2436:2481");

    final Map<RaftPeerId, String> urls = election.peerProbeUrls(false);

    assertThat(urls).containsOnlyKeys(RaftPeerId.valueOf("localhost_2435"), RaftPeerId.valueOf("localhost_2436"));
    assertThat(urls.values()).containsOnlyNulls();
  }

  /** SSL off: plain HTTP to the declared endpoint, whatever an HTTPS endpoint would resolve to. */
  @Test
  void withoutSslTheDeclaredHttpEndpointIsProbed() {
    final BootstrapElection election = newElection(
        "localhost:2434:2480:0:2490,localhost:2435:2481:0:2491,localhost:2436:2482:0:2492");

    final Map<RaftPeerId, String> urls = election.peerProbeUrls(false);

    assertThat(urls.get(RaftPeerId.valueOf("localhost_2435")))
        .isEqualTo("http://localhost:2481" + BootstrapElection.BOOTSTRAP_STATE_ROUTE);
    assertThat(urls.get(RaftPeerId.valueOf("localhost_2436")))
        .isEqualTo("http://localhost:2482" + BootstrapElection.BOOTSTRAP_STATE_ROUTE);
  }

  /**
   * An election over a {@link RaftHAServer} built from {@code serverList}, with Ratis never started. This node is
   * the FIRST entry of the list, and its HTTP/HTTPS listeners report {@link #LOCAL_HTTP_PORT} and
   * {@link #LOCAL_HTTPS_PORT} - which is what the derive fallback reads.
   */
  private static BootstrapElection newElection(final String serverList) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList);

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("ArcadeDB_0");
    when(mockServer.getConfiguration()).thenReturn(config);
    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getPort()).thenReturn(LOCAL_HTTP_PORT);
    when(httpServer.getHttpsPort()).thenReturn(LOCAL_HTTPS_PORT);
    when(mockServer.getHttpServer()).thenReturn(httpServer);

    return new BootstrapElection(new RaftHAServer(mockServer, config), mockServer);
  }
}
