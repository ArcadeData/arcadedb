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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.DatabaseNotFoundException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12830: a client of a binary protocol (gRPC, Bolt, Postgres, Redis, MongoDB) naming a database that does not exist was
 * answered with the engine's own message, which carries the server's absolute database directory. HTTP and the other
 * paths name the database only.
 */
class DatabaseNotFoundHidesPathTest extends BaseGraphServerTest {
  private static final String MISSING = "Issue12830Missing";

  @Test
  void aClientNamingNoDatabaseLearnsTheNameAndNotTheDirectory() {
    final ArcadeDBServer server = getServer(0);
    final String databaseDirectory = server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);

    assertThatThrownBy(() -> server.getDatabaseForClient(MISSING)).isInstanceOf(DatabaseNotFoundException.class)
        .hasMessage("Database '" + MISSING + "' does not exist")
        .satisfies(e -> assertThat(String.valueOf(e.getMessage())).doesNotContain(databaseDirectory));

    assertThat(server.existsDatabase(MISSING)).as("a refused request creates nothing").isFalse();
  }
}
