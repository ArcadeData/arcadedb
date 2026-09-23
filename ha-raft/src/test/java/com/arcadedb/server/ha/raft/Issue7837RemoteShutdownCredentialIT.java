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

import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The credential a remote shutdown presents, against a REAL peer's {@code /api/v1/server} route (issue #7837).
 * <p>
 * {@code RaftHAPlugin.shutdownRemoteServer} used to send the cluster token as {@code Authorization: Bearer}.
 * {@code AbstractServerHttpHandler} authenticates a {@code Bearer} credential in exactly two forms - an API
 * token ({@code at-} prefix) and a session token ({@code AU-} prefix) - and a cluster token is neither, so the
 * POST was answered 401, the peer stayed up, and the method read the status only to log it: an operator's
 * {@code shutdown &lt;server&gt;} reported success and did nothing.
 *
 * <h2>Why the POST carries a target name instead of being the bare shutdown command</h2>
 * The peer cannot be allowed to actually stop: {@code ServerControlPlane.shutdownServer("")} ends in
 * {@code System.exit(0)}, which in an in-process cluster takes the whole test JVM with it. So the request built
 * here is the shutdown request - the same headers, the same route, the same {@code shutdown} verb - aimed at a
 * server name no peer answers to. That reaches {@code PostServerCommandHandler}'s root check and its
 * {@code shutdown} branch and stops one step short of the exit, in {@code resolveShutdownTarget}, which is
 * exactly the step this issue is not about.
 * <p>
 * What that proves is the whole of the question: whether the credential the shutdown carries is authenticated
 * by the route the shutdown targets. A 401 is the bug; anything past it means the request was authenticated and
 * authorized as root. {@link Issue7837ShutdownRequestCredentialTest} pins the headers themselves, so the two
 * together cover the request and its reception.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class Issue7837RemoteShutdownCredentialIT extends BaseRaftHATest {

  /** A name no peer in this cluster answers to, so the command is authenticated and then refused a target. */
  private static final String NO_SUCH_SERVER = "no-such-peer-7837";

  @Override
  protected int getServerCount() {
    return 2;
  }

  /**
   * The regression. The request {@code shutdownRemoteServer} builds today is authenticated by the peer: the
   * answer is the "no such server" refusal from inside the command, not the 401 that used to come before it.
   */
  @Test
  void theCredentialTheShutdownCarriesIsAuthenticatedByThePeer() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("a Raft leader must be elected first").isGreaterThanOrEqualTo(0);
    final int peerIndex = leaderIndex == 0 ? 1 : 0;

    final HttpResponse<String> response = send(shutdownRequestAimedAtAnUnknownServer(leaderIndex, peerIndex));

    assertThat(response.statusCode())
        .as("the cluster token the shutdown carries must authenticate against the peer's /api/v1/server route")
        .isNotEqualTo(401);
    assertThat(response.body())
        .as("the request must have reached the shutdown command itself, which is where the unknown name is refused")
        .contains(NO_SUCH_SERVER);
  }

  /**
   * The negative that gives the test above its meaning: the credential this dial used to present really is
   * refused by the peer. Without it, a passing assertion above would be equally consistent with a route that
   * authenticates anything.
   */
  @Test
  void theBearerFormThisDialUsedToSendIsRefused() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).isGreaterThanOrEqualTo(0);
    final int peerIndex = leaderIndex == 0 ? 1 : 0;

    final String token = getRaftPlugin(leaderIndex).getRaftHAServer().getClusterToken();
    assertThat(token).as("the cluster must have a cluster token for this comparison to mean anything").isNotBlank();

    final HttpResponse<String> response = send(HttpRequest.newBuilder()
        .uri(URI.create(serverCommandUrl(peerIndex)))
        .timeout(Duration.ofSeconds(10))
        .header("Content-Type", "application/json")
        .header("Authorization", "Bearer " + token)
        .POST(HttpRequest.BodyPublishers.ofString(shutdownBody(), StandardCharsets.UTF_8))
        .build());

    assertThat(response.statusCode())
        .as("a cluster token presented as a Bearer credential is not a credential this route knows")
        .isEqualTo(401);
  }

  /** The request {@link RaftHAPlugin#shutdownRequest} builds, with a target no peer answers to. */
  private HttpRequest shutdownRequestAimedAtAnUnknownServer(final int fromIndex, final int toIndex) {
    final RaftHAServer raft = getRaftPlugin(fromIndex).getRaftHAServer();
    final PeerDialAddress dial = PeerDialAddress.resolve(raft, RaftPeerId.valueOf(peerIdForIndex(toIndex)), "peer");
    assertThat(dial.refused()).as("resolving the peer must not be refused: %s", dial.refusal()).isFalse();
    assertThat(RaftHAPlugin.shutdownUrl(dial, false)).isEqualTo(serverCommandUrl(toIndex));

    // Rebuilt rather than mutated: HttpRequest is immutable, so the headers under test are copied across
    // verbatim from the very request the production dial would send.
    final HttpRequest production = RaftHAPlugin.shutdownRequest(serverCommandUrl(toIndex), raft.getClusterToken());
    final HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(production.uri())
        .timeout(Duration.ofSeconds(10))
        .POST(HttpRequest.BodyPublishers.ofString(shutdownBody(), StandardCharsets.UTF_8));
    production.headers().map().forEach((name, values) -> values.forEach(value -> builder.header(name, value)));
    return builder.build();
  }

  private static String shutdownBody() {
    return "{\"command\":\"shutdown " + NO_SUCH_SERVER + "\"}";
  }

  private String serverCommandUrl(final int index) {
    return "http://localhost:" + getServer(index).getHttpServer().getPort() + RaftHAPlugin.SERVER_COMMAND_ROUTE;
  }

  private static HttpResponse<String> send(final HttpRequest request) throws Exception {
    try (final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
      return client.send(request, HttpResponse.BodyHandlers.ofString());
    }
  }
}
