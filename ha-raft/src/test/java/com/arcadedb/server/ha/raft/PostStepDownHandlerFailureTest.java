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
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.security.ServerSecurityUser;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Issue #7127 follow-up: {@code stepDown()} gained a terminal {@link ReplicationException}, so
 * {@code POST /api/v1/cluster/stepdown} gained a failure mode it had never seen. Before, that case returned
 * normally and the endpoint answered 200 "Leadership step-down initiated" for a step-down that never happened.
 * <p>
 * The exception must not be left to {@code AbstractServerHttpHandler}'s central mapper: that mapper lives in the
 * server module, which must not depend on this Raft type, so it has no arm for it and would answer a generic
 * 500 "Internal error" - indistinguishable from a bug, and read by clients and load balancers as do-not-retry.
 * The three terminal outcomes of the endpoint are asserted together here so a future change cannot move one of
 * them without the others being looked at.
 *
 * @see PostStepDownHandler
 */
class PostStepDownHandlerFailureTest {

  private final RaftHAServer raftHAServer = mock(RaftHAServer.class);
  private final PostStepDownHandler handler = new PostStepDownHandler(null, pluginReturning(raftHAServer));

  @Test
  void exhaustedTransfersAnswer503RatherThanTheGeneric500() {
    doThrow(new ReplicationException("Cannot step down: no other peer available for leadership transfer"))
        .when(raftHAServer).stepDown();

    final ExecutionResponse response = handler.execute(null, rootUser(), new JSONObject());

    assertThat(response.getCode())
        .as("a leader that could not hand off is retryable as issued, not an internal error")
        .isEqualTo(503);
    assertThat(new JSONObject(response.getResponse()).getString("error", null))
        .contains("no other peer available for leadership transfer");
  }

  @Test
  void aRefusalStillAnswers409() {
    doThrow(new NotTheLeaderRefusalException("Refusing to step down", RaftPeerId.valueOf("ArcadeDB_1")))
        .when(raftHAServer).stepDown();

    final ExecutionResponse response = handler.execute(null, rootUser(), new JSONObject());

    assertThat(response.getCode()).isEqualTo(409);
    assertThat(new JSONObject(response.getResponse()).getString("error", null)).contains("ArcadeDB_1");
  }

  @Test
  void aSuccessfulStepDownStillAnswers200() {
    final ExecutionResponse response = handler.execute(null, rootUser(), new JSONObject());

    assertThat(response.getCode()).isEqualTo(200);
    assertThat(new JSONObject(response.getResponse()).getString("result", null))
        .isEqualTo("Leadership step-down initiated");
  }

  private static RaftHAPlugin pluginReturning(final RaftHAServer raftHAServer) {
    final RaftHAPlugin plugin = mock(RaftHAPlugin.class);
    when(plugin.getRaftHAServer()).thenReturn(raftHAServer);
    return plugin;
  }

  private static ServerSecurityUser rootUser() {
    final ServerSecurityUser user = mock(ServerSecurityUser.class);
    when(user.getName()).thenReturn("root");
    return user;
  }
}
