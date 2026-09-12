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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #7508: the endpoint a forward to the leader is dialled on.
 * <p>
 * {@code RaftHAServer.getLeaderHttpsAddress()} is what tells the shared {@code LeaderDial} decision that the cluster
 * has an encrypted endpoint for the leader. It has to withhold one in three cases, each of which sends the caller
 * back to the plain-HTTP address - the listener {@code HttpServer.buildUndertowServer} always binds - rather than
 * refusing the forward outright. That is the same withhold-rather-than-refuse rule
 * {@link PeerDialAddress#encryptedEndpointOf} applies to every other peer-to-peer dial (issue #6221).
 */
class Issue7508LeaderHttpsEndpointTest {

  private static final String LEADER_HTTPS = "leader.example.com:2490";
  private static final String LOCAL_HTTPS  = "follower.example.com:2490";

  @Test
  void theLeaderHttpsEndpointIsOfferedOnAnSslClusterThatResolvesOne() {
    assertThat(RaftHAServer.preferredLeaderHttpsAddress(true, LEADER_HTTPS, LOCAL_HTTPS))
        .isEqualTo(LEADER_HTTPS);
  }

  @Test
  void nothingIsOfferedWhenSslIsOff() {
    // The 5th field of arcadedb.ha.serverList can name an https port on a cluster that never enabled SSL, so the
    // address resolving is not on its own a reason to dial it.
    assertThat(RaftHAServer.preferredLeaderHttpsAddress(false, LEADER_HTTPS, LOCAL_HTTPS)).isNull();
  }

  @Test
  void nothingIsOfferedWhenNoHttpsEndpointResolvesForTheLeader() {
    assertThat(RaftHAServer.preferredLeaderHttpsAddress(true, null, LOCAL_HTTPS)).isNull();
  }

  @Test
  void nothingIsOfferedWhenTheResolvedHttpsEndpointIsThisNodesOwn() {
    // What the derive fallback produces on a cluster whose peers share a host and declare no 'https' port: every
    // peer's HTTPS endpoint collapses onto this node's own, and dialling it comes straight back here. The caller
    // cannot catch this itself - isOwnHttpAddress compares against the HTTP listener, not the HTTPS one.
    assertThat(RaftHAServer.preferredLeaderHttpsAddress(true, LOCAL_HTTPS, LOCAL_HTTPS)).isNull();

    // Loopback spelled two ways on one port is one socket, which isSameHttpEndpoint already knows (issue #6204).
    assertThat(RaftHAServer.preferredLeaderHttpsAddress(true, "127.0.0.1:2490", "localhost:2490")).isNull();
  }

  @Test
  void theEndpointIsStillOfferedWhenThisNodeHasNoHttpsAddressToCompareAgainst() {
    // getLocalHttpsAddress() degrades to null while the local HTTPS listener is coming up. The self-check is then
    // inactive, exactly as PeerDialAddress.resolve leaves the HTTP one, rather than withholding every endpoint.
    assertThat(RaftHAServer.preferredLeaderHttpsAddress(true, LEADER_HTTPS, null)).isEqualTo(LEADER_HTTPS);
  }
}
