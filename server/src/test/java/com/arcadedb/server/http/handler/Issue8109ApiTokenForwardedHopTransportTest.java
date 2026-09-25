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
package com.arcadedb.server.http.handler;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.LeaderForwardContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8109: {@code POST /server/api-tokens} is forwarded to the leader on an HA cluster, so the plaintext token
 * travels back over two legs - leader to follower, follower to client. The follower checks the client's leg before
 * forwarding; the leader checks the hop it received the forward on. When the hop is what fails the check, the
 * refusal has to name the hop: the client did connect over TLS, and "connect over TLS" would send it after the wrong
 * connection.
 * <p>
 * Asserted on the decision rather than end to end for the reason {@link Issue7372ApiTokenTransportGateTest} gives:
 * every node of an in-JVM cluster dials 127.0.0.1, which is always a safe leg.
 */
class Issue8109ApiTokenForwardedHopTransportTest {

  @AfterEach
  void clearForwardContext() {
    LeaderForwardContext.clear();
  }

  @Test
  void aClientLegRefusalIsAnsweredAsIs() {
    final ExecutionResponse refusal = cleartextRemoteRefusal();

    assertThat(PostApiTokenHandler.refusalForThisLeg(refusal)).isSameAs(refusal);
  }

  @Test
  void aForwardedHopRefusalNamesTheHopBetweenTheNodes() {
    final ExecutionResponse clientRefusal = cleartextRemoteRefusal();
    LeaderForwardContext.markAlreadyForwarded();

    final ExecutionResponse refusal = PostApiTokenHandler.refusalForThisLeg(clientRefusal);

    assertThat(refusal.getCode()).isEqualTo(412);
    final String error = new JSONObject(refusal.getResponse()).getString("error");
    assertThat(error).contains("from another node");
    assertThat(error).contains("HTTPS between the cluster nodes");
    assertThat(error).contains(GlobalConfiguration.SERVER_API_TOKEN_REQUIRE_SECURE_TRANSPORT.getKey());
  }

  /** The refusal {@code checkTransport} builds for a cleartext mint from a peer that is not on this machine. */
  private static ExecutionResponse cleartextRemoteRefusal() {
    final ExecutionResponse refusal = PostApiTokenHandler.checkTransport("http", new InetSocketAddress("203.0.113.7", 51234),
        true);
    assertThat(refusal).as("a cleartext mint from a remote peer must be refused with the setting on").isNotNull();
    return refusal;
  }
}
