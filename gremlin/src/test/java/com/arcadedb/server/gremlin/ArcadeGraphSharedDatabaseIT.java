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
package com.arcadedb.server.gremlin;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for the WAL version gap issue on follower nodes after HA cluster restart.
 *
 * Root cause: ArcadeGraph.close() was unconditionally calling database.close() even when the
 * graph was backed by a server-managed (shared) database. ServerDatabase.close() throws
 * UnsupportedOperationException by design, causing a WARN log flood and a race condition during
 * shutdown where the Gremlin stop thread could commit transactions after the HA state machine
 * had stopped, leaving the WAL in an inconsistent state.
 *
 * Fix: ArcadeGraph.openShared(database) creates a graph that does not own the database
 * lifecycle. ArcadeGraph.close() on such a graph rolls back any active transaction but does
 * NOT close the underlying database.
 */
class ArcadeGraphSharedDatabaseIT extends BaseGraphServerTest {

  @Test
  void closeSharedGraphDoesNotCloseUnderlyingDatabase() {
    final BasicDatabase serverDb = getServerDatabase(0, getDatabaseName());
    assertThat(serverDb.isOpen()).isTrue();

    final ArcadeGraph graph = ArcadeGraph.openShared(serverDb);
    assertThatCode(graph::close).doesNotThrowAnyException();

    assertThat(serverDb.isOpen())
        .as("server-managed database must remain open after ArcadeGraph.close()")
        .isTrue();
  }

  @Test
  void closeSharedGraphWithActiveTransactionRollsBackAndLeavesDbOpen() {
    final BasicDatabase serverDb = getServerDatabase(0, getDatabaseName());
    assertThat(serverDb.isOpen()).isTrue();

    final ArcadeGraph graph = ArcadeGraph.openShared(serverDb);
    serverDb.begin();
    assertThat(serverDb.isTransactionActive()).isTrue();

    assertThatCode(graph::close).doesNotThrowAnyException();

    assertThat(serverDb.isTransactionActive())
        .as("transaction must have been rolled back by ArcadeGraph.close()")
        .isFalse();
    assertThat(serverDb.isOpen())
        .as("server-managed database must remain open after ArcadeGraph.close()")
        .isTrue();
  }
}
