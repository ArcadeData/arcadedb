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
package com.arcadedb.mcp;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9960, the MCP member of the #9845 family: a follower's refusal of leader-only work reached an agent as the
 * refusal's bare message, which carries no address, or in production mode as the concealment placeholder, so the agent
 * could tell neither where the leader is nor that a leader problem happened at all. The tool error now names the leader
 * in both modes, as the Bolt, PostgreSQL, Redis and MongoDB transports do.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9960MCPLeaderRefusalTest {
  private static final String LEADER   = "arcadedb-0:2480";
  private static final String SENTENCE = "The leader is server '" + LEADER + "'";

  @TempDir
  Path root;

  private static ServerIsNotTheLeaderException refusal(final String leader) {
    return new ServerIsNotTheLeaderException("Changes to the schema must be executed on the leader server", leader);
  }

  private MCPDispatcher dispatcher(final boolean production) {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_MODE, production ? "production" : "development");
    final ArcadeDBServer server = TestServerHelper.unstartedServer(root, configuration);
    return new MCPDispatcher(server, new MCPConfiguration(root.resolve("config")), "http");
  }

  @Test
  void productionModeNamesTheLeaderAfterThePlaceholder() {
    final String text = dispatcher(true).failureMessage("tools/call 'execute_command'",
        new TransactionException("commit", refusal(LEADER)));

    assertThat(text).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE + ". " + SENTENCE);
  }

  @Test
  void developmentModeNamesTheLeaderAfterTheMessage() {
    final String text = dispatcher(false).failureMessage("tools/call 'execute_command'", refusal(LEADER));

    assertThat(text).isEqualTo("Changes to the schema must be executed on the leader server. " + SENTENCE);
  }

  @Test
  void aRefusalNamingNoLeaderIsUnchanged() {
    assertThat(dispatcher(true).failureMessage("tools/call 'query'", refusal(""))).isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
    assertThat(dispatcher(false).failureMessage("tools/call 'query'", refusal(null)))
        .isEqualTo("Changes to the schema must be executed on the leader server");
  }

  @Test
  void anOrdinaryConflictIsUnchanged() {
    assertThat(dispatcher(true).failureMessage("tools/call 'query'", new ConcurrentModificationException("conflict")))
        .isEqualTo(ArcadeDBServer.CONCEALED_ERROR_MESSAGE);
    assertThat(dispatcher(false).failureMessage("tools/call 'query'", new ConcurrentModificationException("conflict")))
        .isEqualTo("conflict");
  }
}
