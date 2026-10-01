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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.LeaderForwardContext;
import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.PostBatchHandler.CountingInputStream;
import com.arcadedb.server.security.ServerSecurityUser;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import io.undertow.util.Methods;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #8393: a one-hop refusal the receiving node cannot classify - the forwarding peer sent no intended leader id,
 * because leadership changed while it resolved the leader's address or because it runs a build that predates the
 * header (a rolling upgrade), or this node cannot name itself - is almost always an election. It used to be answered
 * with the non-retryable 400 meant for a misconfigured {@code arcadedb.ha.serverList}, and it used up the one warning
 * that misconfiguration gets. It is now retryable (503, no leader address) and leaves the latch alone; only the
 * refusal that PROVES the address named the wrong node keeps the 400 and the warning.
 * <p>
 * The third refusal site, the SQL forward in {@code RaftReplicatedDatabase}, lives in the ha-raft module and is driven
 * end to end by {@code Issue7603LeaderForwardHopIT}.
 */
class Issue8393UndeterminedForwardRefusalTest {

  private static final String LOCAL_PEER  = "peer-follower";
  private static final String LEADER_PEER = "peer-leader";

  @AfterEach
  void clear() {
    LeaderForwardContext.clear();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // POST /api/v1/server and the /server/* routes: LeaderCommandForwarder
  // ---------------------------------------------------------------------------------------------------------------

  /** Path 2 of the issue: a peer on an older build sends no leader id at all. */
  @Test
  void aServerCommandFromAPeerThatNamedNoLeaderIsRetryable() {
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(LOCAL_PEER)));
    LeaderForwardContext.markAlreadyForwarded();

    final ServerIsNotTheLeaderException refusal = serverCommandRefusal(forwarder);

    assertThat(refusal.getLeaderAddress())
        .as("a named leader is what AbstractServerHttpHandler answers 400; an unnamed one is the retryable 503")
        .isNull();
    assertThat(refusal.getMessage()).contains("already forwarded").contains("retry");
    assertThat(forwarder.misconfigurationWarned())
        .as("an unclassifiable refusal is usually an election and must not spend the misconfiguration's notice")
        .isFalse();
  }

  /** This node cannot name itself: nothing proves the address was wrong either. */
  @Test
  void aServerCommandRefusedByANodeThatCannotNameItselfIsRetryable() {
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(null)));
    LeaderForwardContext.markAlreadyForwarded(LEADER_PEER);

    final ServerIsNotTheLeaderException refusal = serverCommandRefusal(forwarder);

    assertThat(refusal.getLeaderAddress()).isNull();
    assertThat(forwarder.misconfigurationWarned()).isFalse();
  }

  /**
   * The latch is kept for the fault it was written for: a run of unclassifiable refusals first, then the one that
   * proves the address wrong, which must still be logged and still be the configuration error.
   */
  @Test
  void theMisconfigurationStillGetsItsNoticeAfterUndeterminedRefusals() {
    final LeaderCommandForwarder forwarder = new LeaderCommandForwarder(httpServerWith(ha(LOCAL_PEER)));
    for (int i = 0; i < 3; i++) {
      LeaderForwardContext.markAlreadyForwarded();
      assertThat(serverCommandRefusal(forwarder).getLeaderAddress()).isNull();
    }
    assertThat(forwarder.misconfigurationWarned()).isFalse();

    LeaderForwardContext.markAlreadyForwarded(LEADER_PEER);
    final ServerIsNotTheLeaderException misidentified = serverCommandRefusal(forwarder);

    assertThat(misidentified.getLeaderAddress()).isEqualTo("leader");
    assertThat(misidentified.getMessage()).contains("does not identify")
        .contains(GlobalConfiguration.HA_SERVER_LIST.getKey());
    assertThat(forwarder.misconfigurationWarned()).isTrue();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // POST /api/v1/batch: PostBatchHandler
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void aBatchFromAPeerThatNamedNoLeaderIsRetryable() throws Exception {
    final HAServerPlugin ha = ha(LOCAL_PEER);
    final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha));
    LeaderForwardContext.markAlreadyForwarded();

    final ExecutionResponse refusal = batchRefusal(handler, ha);

    assertThat(refusal.getCode()).isEqualTo(503);
    final JSONObject body = new JSONObject(refusal.getResponse());
    assertThat(body.getString("error")).contains("already forwarded").contains("retry");
    assertThat(body.getString("exception", null))
        .as("typed like the other unnamed refusals, so a relaying follower recognizes it")
        .isEqualTo(ServerIsNotTheLeaderException.class.getName());
    assertThat(LeaderCommandForwarder.isUnnamedNotTheLeaderRefusal(refusal)).isTrue();
    assertThat(handler.misconfigurationWarned()).isFalse();
  }

  @Test
  void aBatchRefusalThatProvesTheAddressWrongIsStillTheConfigurationError() throws Exception {
    final HAServerPlugin ha = ha(LOCAL_PEER);
    final PostBatchHandler handler = new PostBatchHandler(httpServerWith(ha));
    LeaderForwardContext.markAlreadyForwarded();
    assertThat(batchRefusal(handler, ha).getCode()).isEqualTo(503);
    assertThat(handler.misconfigurationWarned()).isFalse();

    LeaderForwardContext.markAlreadyForwarded(LEADER_PEER);
    final ExecutionResponse misidentified = batchRefusal(handler, ha);

    assertThat(misidentified.getCode()).isEqualTo(400);
    assertThat(misidentified.getResponse()).contains("does not identify");
    assertThat(handler.misconfigurationWarned()).isTrue();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------------------------------------------

  private static ServerIsNotTheLeaderException serverCommandRefusal(final LeaderCommandForwarder forwarder) {
    final ServerIsNotTheLeaderException refusal = catchThrowableOfType(ServerIsNotTheLeaderException.class,
        () -> forwarder.forwardIfReplica(exchange("/api/v1/server"), user(), "/api/v1/server", "{}", false));
    assertThat(refusal).as("an already-forwarded request on a non-leader must be refused in one hop").isNotNull();
    return refusal;
  }

  private static ExecutionResponse batchRefusal(final PostBatchHandler handler, final HAServerPlugin ha)
      throws Exception {
    return handler.forwardBatchToLeader(exchange("/api/v1/batch/graph"), ha, "graph", user(), "application/json",
        new CountingInputStream(new HttpServerExchange(null),
            new ByteArrayInputStream("{\"@type\":\"vertex\"}\n".getBytes(StandardCharsets.UTF_8))), false);
  }

  private static HAServerPlugin ha(final String localPeerId) {
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.isLeader()).thenReturn(false);
    // Never dialled: every request in this class is refused before an address is resolved.
    when(ha.getLeaderAddress()).thenReturn("127.0.0.1:1");
    when(ha.getLeaderName()).thenReturn("leader");
    when(ha.getLeaderPeerId()).thenReturn(LEADER_PEER);
    when(ha.getLocalPeerId()).thenReturn(localPeerId);
    when(ha.getClusterToken()).thenReturn("issue8393-cluster-token");
    return ha;
  }

  private static HttpServer httpServerWith(final HAServerPlugin ha) {
    final ContextConfiguration cfg = new ContextConfiguration();
    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getHA()).thenReturn(ha);
    when(server.getConfiguration()).thenReturn(cfg);
    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);
    return httpServer;
  }

  private static HttpServerExchange exchange(final String path) {
    final HttpServerExchange exchange = new HttpServerExchange(null);
    exchange.setRequestPath(path);
    exchange.setRequestURI(path);
    exchange.setRequestMethod(Methods.POST);
    exchange.getRequestHeaders().put(new HttpString("Authorization"), "Basic cm9vdDpwbGF5d2l0aGRhdGE=");
    return exchange;
  }

  private static ServerSecurityUser user() {
    final ServerSecurityUser user = mock(ServerSecurityUser.class);
    when(user.getName()).thenReturn("root");
    return user;
  }
}
