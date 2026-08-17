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

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A withheld peer-to-peer endpoint says so, once (issue #6267).
 * <p>
 * {@code getUnambiguousPeerHttpAddress} / {@code getUnambiguousPeerHttpsAddress} refuse an address two peers
 * resolve to by answering {@code null}, and each caller then decided for itself whether to say anything - so the
 * misconfiguration was visible only where one happened to log it. Neither pre-existing warning covers it: the
 * derive warnings fire whenever an address is derived <em>at all</em>, which a healthy homogeneous Kubernetes
 * StatefulSet also does, and {@code warnAmbiguousRouting} says exactly this but about the client routing tables
 * of issue #6183.
 * <p>
 * Driven against a real {@link RaftHAServer} built from a server list with Ratis never started - the peer group
 * and the declared addresses are both populated by the constructor, which is all the resolver reads.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6267AmbiguousPeerAddressWarningTest {

  private static final String AMBIGUOUS_HTTP  = "HA HTTP peer endpoints are ambiguous";
  private static final String AMBIGUOUS_HTTPS = "HA HTTPS peer endpoints are ambiguous";

  private CapturingTestLogger log;

  @BeforeEach
  void installLogger() {
    log = CapturingTestLogger.install();
  }

  @AfterEach
  void restoreLogger() {
    log.uninstall();
  }

  /**
   * Two peers deriving to one {@code host:port}: the address is withheld from both, and the operator is told
   * which peers could not be told apart, on which address, and what to write instead.
   */
  @Test
  void aWithheldHttpAddressIsReportedOnceNamingThePeersThatShareIt() {
    final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2490,localhost:2436:2490");

    assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("localhost_2435"))).isNull();
    assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("localhost_2436"))).isNull();
    // The resync and verify paths ask on every attempt; a misconfiguration that has not changed since the last
    // attempt must not be re-reported on each one.
    assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("localhost_2435"))).isNull();

    assertThat(log.countFormattedContaining(AMBIGUOUS_HTTP, "localhost_2435", "localhost_2436", "localhost:2490",
        GlobalConfiguration.HA_SERVER_LIST.getKey()))
        .as("one warning, naming both peers, the address they share and the setting that fixes it")
        .isEqualTo(1);
  }

  /** The address every peer owns is handed out, and nothing is logged: a correct cluster stays quiet. */
  @Test
  void anUnambiguousClusterIsNotWarnedAbout() {
    final RaftHAServer raft = newDetachedServer("localhost:2434:2480,localhost:2435:2481,localhost:2436:2482");

    assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("localhost_2435"))).isEqualTo("localhost:2481");

    assertThat(log.countContaining(AMBIGUOUS_HTTP)).isZero();
    assertThat(log.countContaining(AMBIGUOUS_HTTPS)).isZero();
  }

  /**
   * The HTTPS endpoint has its own latch. A cluster that declares distinct {@code http} ports and shares an
   * {@code https} one passes the HTTP check with every HTTPS endpoint collapsed, and that must still be
   * reported - the HTTP verdict cannot stand in for it, and neither can the HTTP latch.
   */
  @Test
  void theHttpsWarningIsNotMutedByTheHttpOne() {
    // host:raftPort:httpPort:priority:httpsPort - distinct HTTP ports, one shared HTTPS port.
    final RaftHAServer raft = newDetachedServer(
        "localhost:2434:2480:0:2490,localhost:2435:2481:0:2491,localhost:2436:2482:0:2491");

    assertThat(raft.getUnambiguousPeerHttpAddress(RaftPeerId.valueOf("localhost_2435"))).isEqualTo("localhost:2481");
    assertThat(raft.getUnambiguousPeerHttpsAddress(RaftPeerId.valueOf("localhost_2435"))).isNull();

    assertThat(log.countContaining(AMBIGUOUS_HTTP))
        .as("the HTTP endpoints are fine, so nothing is said about them")
        .isZero();
    assertThat(log.countFormattedContaining(AMBIGUOUS_HTTPS, "localhost_2435", "localhost_2436", "localhost:2491"))
        .isEqualTo(1);
  }

  /**
   * A {@link RaftHAServer} built from {@code serverList} with Ratis never started. The node names itself with the
   * {@code prefix_N} convention, so it is the FIRST entry of the list.
   */
  private static RaftHAServer newDetachedServer(final String serverList) {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, serverList);

    final ArcadeDBServer mockServer = mock(ArcadeDBServer.class);
    when(mockServer.getServerName()).thenReturn("ArcadeDB_0");

    return new RaftHAServer(mockServer, config);
  }
}
