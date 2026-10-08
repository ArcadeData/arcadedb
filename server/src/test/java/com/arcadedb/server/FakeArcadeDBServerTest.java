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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/** Issue #9464: {@link FakeArcadeDBServer} starts where an unstarted server does and answers what the test set. */
class FakeArcadeDBServerTest {
  @TempDir
  Path root;

  @Test
  void aBareFakeAnswersLikeAnUnstartedServer() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();

    assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.OFFLINE);
    assertThat(server.getSecurity()).isNull();
    assertThat(server.getHttpServer()).isNull();
    assertThat(server.getPlugins()).isEmpty();
    assertThat(server.getDatabaseNames()).isEmpty();
    assertThat(server.existsDatabase("db")).isFalse();
    assertThat(server.getDatabase("db")).isNull();
  }

  @Test
  void theNameAndConfigurationAreTheRealServers() {
    final ContextConfiguration configuration = new ContextConfiguration();
    final FakeArcadeDBServer server = FakeArcadeDBServer.create("node-a", configuration).online();

    assertThat(server.getServerName()).isEqualTo("node-a");
    assertThat(server.getConfiguration()).isSameAs(configuration);
    assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);
  }

  @Test
  void aServedDatabaseIsListedAndReturned() {
    final DatabaseInternal database = (DatabaseInternal) new DatabaseFactory(root.resolve("db").toString()).create();
    try {
      final ServerDatabase served = new ServerDatabase(null, database);
      final FakeArcadeDBServer server = FakeArcadeDBServer.create(root, new ContextConfiguration()).database("db", served)
          .databaseNames("listed");

      assertThat(server.getDatabase("db")).isSameAs(served);
      assertThat(server.existsDatabase("db")).isTrue();
      assertThat(server.getDatabaseNames()).containsExactly("db", "listed");
      assertThat(server.existsDatabase("listed")).as("listed by name only, not served").isFalse();
      assertThat(server.getDatabase("listed")).isNull();
    } finally {
      database.drop();
    }
  }
}
