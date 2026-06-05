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
package com.arcadedb.server.http;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.HAServerPlugin;
import com.arcadedb.server.http.handler.ExecutionResponse;
import com.arcadedb.server.http.handler.GetReadyHandler;
import com.arcadedb.server.security.ServerSecurityUser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GetReadyHandlerHATest {

  private GetReadyHandler newHandler(final ArcadeDBServer server) {
    final HttpServer httpServer = mock(HttpServer.class);
    when(httpServer.getServer()).thenReturn(server);
    return new GetReadyHandler(httpServer);
  }

  @Test
  void flagOffReturns204RegardlessOfHA() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, false);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getStatus()).thenReturn(ArcadeDBServer.STATUS.ONLINE);
    when(server.getConfiguration()).thenReturn(cfg);
    // HA still electing, but the flag is off so it must be ignored.
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.VOTING_FOR_ME);
    when(server.getHA()).thenReturn(ha);

    final ExecutionResponse response = newHandler(server).execute(null, (ServerSecurityUser) null, null);
    assertThat(response.getCode()).isEqualTo(204);
  }

  @Test
  void flagOnAndElectionDoneReturns204() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
    cfg.setValue(GlobalConfiguration.HA_ENABLED, true);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getStatus()).thenReturn(ArcadeDBServer.STATUS.ONLINE);
    when(server.getConfiguration()).thenReturn(cfg);
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.DONE);
    when(server.getHA()).thenReturn(ha);

    final ExecutionResponse response = newHandler(server).execute(null, (ServerSecurityUser) null, null);
    assertThat(response.getCode()).isEqualTo(204);
  }

  @Test
  void flagOnAndElectionInProgressReturns503() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
    cfg.setValue(GlobalConfiguration.HA_ENABLED, true);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getStatus()).thenReturn(ArcadeDBServer.STATUS.ONLINE);
    when(server.getConfiguration()).thenReturn(cfg);
    final HAServerPlugin ha = mock(HAServerPlugin.class);
    when(ha.getElectionStatus()).thenReturn(HAServerPlugin.ELECTION_STATUS.VOTING_FOR_ME);
    when(server.getHA()).thenReturn(ha);

    final ExecutionResponse response = newHandler(server).execute(null, (ServerSecurityUser) null, null);
    assertThat(response.getCode()).isEqualTo(503);
  }

  @Test
  void flagOnButHaDisabledReturns204() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
    // HA not enabled: single-node deployment, the readiness flag has no effect.
    cfg.setValue(GlobalConfiguration.HA_ENABLED, false);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getStatus()).thenReturn(ArcadeDBServer.STATUS.ONLINE);
    when(server.getConfiguration()).thenReturn(cfg);
    when(server.getHA()).thenReturn(null);

    final ExecutionResponse response = newHandler(server).execute(null, (ServerSecurityUser) null, null);
    assertThat(response.getCode()).isEqualTo(204);
  }

  @Test
  void flagOnHaEnabledButPluginAbsentReturns503() throws Exception {
    final ContextConfiguration cfg = new ContextConfiguration();
    cfg.setValue(GlobalConfiguration.SERVER_READINESS_REQUIRES_HA, true);
    // HA requested but no HA plugin registered (e.g. arcadedb-ha-raft missing): the node runs
    // standalone, which does not satisfy the operator's "require HA for readiness" intent.
    cfg.setValue(GlobalConfiguration.HA_ENABLED, true);

    final ArcadeDBServer server = mock(ArcadeDBServer.class);
    when(server.getStatus()).thenReturn(ArcadeDBServer.STATUS.ONLINE);
    when(server.getConfiguration()).thenReturn(cfg);
    when(server.getHA()).thenReturn(null);

    final ExecutionResponse response = newHandler(server).execute(null, (ServerSecurityUser) null, null);
    assertThat(response.getCode()).isEqualTo(503);
  }
}
