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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Issue #8404: {@code DEFINE FUNCTION} and {@code DELETE FUNCTION} changed only the node that executed them. Both
 * persisted the schema with a bare {@code saveConfiguration()}, outside any {@code recordFileChanges} session, so the
 * leader never proposed a {@code SCHEMA_ENTRY} and followers never saw the function. Each statement is driven here
 * through both of its cluster entry points - executed on the leader, and sent to a follower, which forwards it
 * because it is not idempotent - and must land on every node.
 */
@Tag("slow")
class Issue8404FunctionDdlReplicationIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(180)
  void defineAndDeleteFunctionReachEveryNode() {
    final int leader = findLeaderIndex();
    assertThat(leader).as("a Raft leader must be elected").isGreaterThanOrEqualTo(0);
    final int follower = firstFollower(leader);
    final Database onLeader = getServer(leader).getDatabase(getDatabaseName());
    final Database onFollower = getServer(follower).getDatabase(getDatabaseName());

    // DEFINE FUNCTION on the leader, creating the library: the case the issue reports.
    onLeader.command("sql", "DEFINE FUNCTION lib8404.twice \"return n * 2;\" PARAMETERS [n] LANGUAGE js");
    awaitFunctionOnEveryServer("lib8404", "twice", true);

    // The replicated definition is a working function on the follower, not only a schema entry.
    try (final ResultSet rs = onFollower.query("sql", "SELECT `lib8404.twice`(21) AS r")) {
      assertThat(((Number) rs.next().getProperty("r")).intValue()).isEqualTo(42);
    }

    // DEFINE FUNCTION into the library that now exists: the other branch of the statement.
    onLeader.command("sql", "DEFINE FUNCTION lib8404.thrice \"return n * 3;\" PARAMETERS [n] LANGUAGE js");
    awaitFunctionOnEveryServer("lib8404", "thrice", true);

    // DEFINE FUNCTION sent to a follower, in a declarative language: forwarded to the leader, replicated back.
    onFollower.command("sql", "DEFINE FUNCTION sql8404.plusOne \"SELECT :n + 1 AS r\" PARAMETERS [n] LANGUAGE sql");
    awaitFunctionOnEveryServer("sql8404", "plusOne", true);

    // DELETE FUNCTION on the leader.
    onLeader.command("sql", "DELETE FUNCTION lib8404.thrice");
    awaitFunctionOnEveryServer("lib8404", "thrice", false);

    // DELETE FUNCTION sent to a follower.
    onFollower.command("sql", "DELETE FUNCTION lib8404.twice");
    awaitFunctionOnEveryServer("lib8404", "twice", false);

    // Nothing else moved: the functions not deleted are still on every node.
    awaitFunctionOnEveryServer("sql8404", "plusOne", true);
  }

  private void awaitFunctionOnEveryServer(final String library, final String function, final boolean present) {
    await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).untilAsserted(() -> {
      for (int i = 0; i < getServerCount(); i++) {
        final Schema schema = getServer(i).getDatabase(getDatabaseName()).getSchema();
        final boolean defined = schema.hasFunctionLibrary(library) && schema.getFunctionLibrary(library).hasFunction(function);
        assertThat(defined).as("function %s.%s on server %d", library, function, i).isEqualTo(present);
      }
    });
  }

  private int firstFollower(final int leader) {
    for (int i = 0; i < getServerCount(); i++)
      if (i != leader)
        return i;
    throw new IllegalStateException("no follower in a " + getServerCount() + "-node cluster");
  }
}
