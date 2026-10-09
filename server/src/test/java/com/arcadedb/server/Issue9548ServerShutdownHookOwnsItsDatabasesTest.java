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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9548: the server's JVM shutdown hook stops the Raft HA service before it closes the databases, and the engine's
 * own hook, which the JVM runs at the same time, closed them in between - a leader committing across a rolling restart
 * then quarantined its own database. The server registers its hook as the owner of its databases, so the engine's hook
 * waits for it ({@code Issue9548OwningShutdownHookTest} pins the wait itself).
 */
class Issue9548ServerShutdownHookOwnsItsDatabasesTest {

  @Test
  void theServerRegistersItsShutdownHookAsTheOwnerOfItsDatabases() {
    final ArcadeDBServer server = new ArcadeDBServer(configuration());
    final Thread hook = server.getShutdownHook();
    try {
      assertThat(hook).isNotNull();
      assertThat(hook.getName()).isEqualTo("arcadedb-shutdown-hook");
      assertThat(hook.getState()).as("the hook only runs at the JVM shutdown").isEqualTo(Thread.State.NEW);
      assertThat(DatabaseFactory.isOwningShutdownHook(hook)).isTrue();
    } finally {
      server.stop();
    }
  }

  /**
   * Review of PR #9551: a stopped server has closed its databases, so its hook has nothing left to order. Left in place,
   * the runtime and the engine's registry would keep one hook, and the whole server it captures, per instance created in
   * the JVM. A start() of the same instance puts it back.
   */
  @Test
  void aStopTakesTheHookAwayAndAStartPutsItBack() {
    final ArcadeDBServer server = new ArcadeDBServer(configuration());
    final Thread hook = server.getShutdownHook();
    assertThat(DatabaseFactory.isOwningShutdownHook(hook)).isTrue();

    server.stop();
    assertThat(DatabaseFactory.isOwningShutdownHook(hook)).isFalse();
    assertThat(Runtime.getRuntime().removeShutdownHook(hook)).as("no longer added to the runtime").isFalse();

    // What start() does first, before bringing anything up
    server.installShutdownHook();
    try {
      assertThat(DatabaseFactory.isOwningShutdownHook(hook)).isTrue();
    } finally {
      server.stop();
    }
    assertThat(DatabaseFactory.isOwningShutdownHook(hook)).isFalse();
  }

  private static ContextConfiguration configuration() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.SERVER_NAME, "issue9548");
    config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
    config.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "./target/databases");
    return config;
  }
}
