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
package com.arcadedb.server;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10553: {@link ArcadeDBServer#isDatabaseHeld} says a name an operator holds is a database although it is not registered,
 * which is what a protocol that guesses whether a key prefix names a database (Redis) needs: {@code existsDatabase} cannot tell a
 * held database from a name that is no database at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10553DatabaseHeldTest {

  @Test
  void aNameNobodyHoldsIsNotHeld() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();

    assertThat(server.isDatabaseHeld("nobody")).isFalse();
    assertThat(server.existsDatabase("nobody")).isFalse();
  }

  @Test
  void aRestoreHoldsItsTargetUntilItIsReleased() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();

    server.reserveDatabaseNameForRestore("restored");
    try {
      assertThat(server.isDatabaseHeld("restored")).isTrue();
      assertThat(server.existsDatabase("restored")).as("it is not registered").isFalse();
      assertThat(server.isDatabaseHeld("other")).as("only the name it claimed").isFalse();
    } finally {
      server.releaseDatabaseNameReservedForRestore("restored");
    }
    assertThat(server.isDatabaseHeld("restored")).isFalse();
  }

  @Test
  void aCloseCommandHoldsTheDatabaseItClosedUntilItIsUnmarked() {
    final FakeArcadeDBServer server = FakeArcadeDBServer.create();

    server.markClosedByCommand("closed");
    try {
      assertThat(server.isDatabaseHeld("closed")).isTrue();
    } finally {
      server.unmarkClosedByCommand("closed");
    }
    assertThat(server.isDatabaseHeld("closed")).isFalse();
  }
}
