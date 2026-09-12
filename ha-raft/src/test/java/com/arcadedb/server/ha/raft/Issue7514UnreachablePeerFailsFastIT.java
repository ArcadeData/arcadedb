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

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7514: naming a peer that is not running must be refused, quickly, in a sentence.
 * <p>
 * Before the fix every entry point below issued the {@code Mode.ADD} configuration change regardless.
 * Ratis does not commit one until the new peer has caught up, so the request could not succeed - but it
 * only found that out after the client's {@code RetryLimited(60, 1s)} budget, and what it handed back
 * was the serialized {@code SetConfigurationRequest} ("Failed SetConfigurationRequest:client-48FB...,
 * cid=11, seq=null, RW, null, ADD, servers:[...] for 60 attempts"), under HTTP 500.
 * <p>
 * Three assertions per entry point, because each one is a separate half of the report:
 * <ul>
 *   <li><b>400</b> - the caller named a server that is not there, which is the caller's to fix. HTTP 500
 *       said "server fault, the request was fine";</li>
 *   <li><b>the address appears in the message</b> - an operator with several joins in flight has to be
 *       able to tell which one was refused, and a serialized protocol object does not tell them;</li>
 *   <li><b>it gave up quickly</b> - the tripwire that separates the pre-flight probe from the Ratis
 *       retry budget it replaces. Deliberately generous: anything under the ~60 s of a single Ratis
 *       attempt proves the probe ran, and widening it can never turn a passing run red.</li>
 * </ul>
 * Plus: the committed configuration is untouched, since a refusal that half-applied would be worse than
 * the hang.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue7514UnreachablePeerFailsFastIT extends BaseRaftHATest {

  /**
   * The tripwire, not a latency budget. One Ratis {@code setConfiguration} attempt is
   * {@code RetryLimited(maxAttempts=60, sleepTime=1s)} and {@code setConfigurationWithRetry} starts a
   * second one, so the unbounded behaviour is ~60-120 s; the probe is bounded by
   * {@code HA_ADD_PEER_PROBE_TIMEOUT} (2 s by default). 25 s sits clear of both.
   */
  private static final long FAST_REFUSAL_TRIPWIRE_MS = 25_000;

  /**
   * {@code POST /api/v1/cluster/peer}, the route the issue's repro used.
   */
  @Test
  void addPeerRouteRefusesAnUnreachableAddress() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a leader must be elected before membership can change").isNotNegative();
    final List<String> before = peerIds(leader);

    final String address = addressNothingListensOn();
    final StallAwareStopwatch watch = StallAwareStopwatch.start();

    final Response response = addPeer(leader, peerIdFor(address), address);

    watch.assertGaveUpWithin(FAST_REFUSAL_TRIPWIRE_MS,
        "a pre-flight reachability probe from the Ratis add-peer retry budget it replaces");
    assertThat(response.status()).as("body: %s", response.body()).isEqualTo(400);
    assertThat(response.body()).contains(address);
    assertThat(peerIds(leader)).as("the committed configuration").isEqualTo(before);
  }

  /**
   * {@code POST /api/v1/server} with {@code connect cluster}, the verb #7401 added and the surface the
   * issue was found on. It reaches the same method by a different route - through
   * {@code ServerControlPlane} and {@code RaftHAPlugin.connectCluster} - so it is its own row.
   */
  @Test
  void connectClusterRefusesAnUnreachableAddress() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).isNotNegative();
    final List<String> before = peerIds(leader);

    final String address = addressNothingListensOn();
    final StallAwareStopwatch watch = StallAwareStopwatch.start();

    final Response response = serverCommand(leader, "connect cluster " + address);

    watch.assertGaveUpWithin(FAST_REFUSAL_TRIPWIRE_MS,
        "a pre-flight reachability probe from the Ratis add-peer retry budget it replaces");
    assertThat(response.status()).as("body: %s", response.body()).isEqualTo(400);
    assertThat(response.body()).contains(address);
    assertThat(peerIds(leader)).isEqualTo(before);
  }

  /**
   * The embedded {@code HAServerPlugin.addPeer} API, which neither HTTP route goes through: an
   * application that drives the cluster in-process gets the same refusal, and it has to be an
   * {@link IllegalArgumentException} because that is the type both shared error mappers key on to answer
   * HTTP 400 and gRPC {@code INVALID_ARGUMENT}.
   */
  @Test
  void theEmbeddedAddPeerApiRefusesTheSameWay() throws Exception {
    final int leader = findLeaderIndex();
    assertThat(leader).isNotNegative();
    final List<String> before = peerIds(leader);

    final String address = addressNothingListensOn();
    final StallAwareStopwatch watch = StallAwareStopwatch.start();

    assertThatThrownBy(() -> getRaftPlugin(leader).addPeer(peerIdFor(address), address))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(address);

    watch.assertGaveUpWithin(FAST_REFUSAL_TRIPWIRE_MS,
        "a pre-flight reachability probe from the Ratis add-peer retry budget it replaces");
    assertThat(peerIds(leader)).isEqualTo(before);
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * An address on this host that nothing is listening on: bind port 0, read the port the OS handed out,
   * release it. Asking the OS is what makes it a port no other test in the run owns - a hardcoded one
   * would be a port conflict waiting to be blamed on this fix.
   */
  private static String addressNothingListensOn() throws IOException {
    try (final ServerSocket socket = new ServerSocket(0)) {
      return "localhost:" + socket.getLocalPort();
    }
  }

  /** The peer id the server-list rule derives from an address: the same string with the colon changed. */
  private static String peerIdFor(final String address) {
    return address.replace(':', '_');
  }

  private List<String> peerIds(final int serverIndex) {
    return getRaftPlugin(serverIndex).getRaftHAServer().getLivePeers().stream()
        .map(peer -> peer.getId().toString()).sorted().toList();
  }

  private record Response(int status, String body) {
  }

  private Response addPeer(final int serverIndex, final String peerId, final String address) throws Exception {
    return post(serverIndex, "/api/v1/cluster/peer",
        new JSONObject().put("peerId", peerId).put("address", address));
  }

  private Response serverCommand(final int serverIndex, final String command) throws Exception {
    return post(serverIndex, "/api/v1/server", new JSONObject().put("command", command));
  }

  private Response post(final int serverIndex, final String path, final JSONObject payload) throws Exception {
    final int port = getServer(serverIndex).getHttpServer().getPort();
    final HttpURLConnection conn = (HttpURLConnection) new URI(
        "http://localhost:" + port + path).toURL().openConnection();
    conn.setRequestMethod("POST");
    conn.setRequestProperty("Content-Type", "application/json");
    conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(
        ("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes(StandardCharsets.UTF_8)));
    conn.setDoOutput(true);
    try {
      conn.getOutputStream().write(payload.toString().getBytes(StandardCharsets.UTF_8));
      final int status = conn.getResponseCode();
      final InputStream stream = status < 400 ? conn.getInputStream() : conn.getErrorStream();
      final String body = stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, body);
    } finally {
      conn.disconnect();
    }
  }
}
