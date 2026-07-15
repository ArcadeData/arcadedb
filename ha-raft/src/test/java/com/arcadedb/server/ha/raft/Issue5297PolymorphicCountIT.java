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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5297: `SELECT count(*) FROM V` on a base type returned a different (and wrong) total on
 * the leader than on the followers, while the per-type counts agreed everywhere. The leader mutates its in-memory
 * schema incrementally, whereas the followers rebuild theirs from the replicated schema.json: any bucket added to a
 * sub type after the inheritance was declared was missing from the leader's polymorphic bucket cache only.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5297PolymorphicCountIT extends BaseRaftHATest {

  private static final int TENANTS = 12;
  private static final int AGENTS  = 31;

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  void polymorphicCountIsIdenticalOnEveryNode() {
    final int leaderIndex = findLeaderIndex();
    final Database leader = getServer(leaderIndex).getDatabase(getDatabaseName());

    leader.getSchema().createVertexType("Base");
    leader.getSchema().createVertexType("Tenant").addSuperType("Base");
    leader.getSchema().createVertexType("Agent").addSuperType("Base");

    // GROW THE SUB TYPES *AFTER* THE INHERITANCE HAS BEEN DECLARED: THIS IS WHAT DRIFTED THE LEADER'S CACHE
    leader.command("sql", "ALTER TYPE Tenant BUCKET +Tenant_extra");
    leader.command("sql", "ALTER TYPE Agent BUCKET +Agent_extra");

    leader.transaction(() -> {
      for (int i = 0; i < TENANTS; i++)
        leader.newVertex("Tenant").set("id", i).save();
      for (int i = 0; i < AGENTS; i++)
        leader.newVertex("Agent").set("id", i).save();
    });

    for (int i = 0; i < getServerCount(); i++)
      waitForReplicationIsCompleted(i);

    for (int i = 0; i < getServerCount(); i++) {
      final Database db = getServer(i).getDatabase(getDatabaseName());
      final String role = i == leaderIndex ? "leader" : "follower";

      assertThat(count(db, "Tenant")).as("per-type count of Tenant on the %s (server %d)", role, i).isEqualTo(TENANTS);
      assertThat(count(db, "Agent")).as("per-type count of Agent on the %s (server %d)", role, i).isEqualTo(AGENTS);
      assertThat(count(db, "Base")).as("polymorphic count of Base on the %s (server %d)", role, i)
          .isEqualTo(TENANTS + AGENTS);
    }
  }

  private long count(final Database db, final String typeName) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) as c FROM " + typeName)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
