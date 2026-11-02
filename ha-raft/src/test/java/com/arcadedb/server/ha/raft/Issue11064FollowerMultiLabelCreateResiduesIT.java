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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.function.sql.misc.SQLFunctionEnsureLabelTypes;
import com.arcadedb.network.binary.ServerIsNotTheLeaderException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/**
 * Issue #11064, the two residues of #11063: a multi-label {@code CREATE} in a client transaction on a follower.
 * <ol>
 * <li>A composite that exists but is incomplete (a label type missing, or not linked) used to skip the leader hand-off, so the
 * follower opened a local schema frame and was refused.</li>
 * <li>A leader that predates the {@code ensureLabelTypes} function answers a SQL error, not the retryable refusal: the follower now
 * falls back to the refusal it gave before the function existed.</li>
 * </ol>
 */
class Issue11064FollowerMultiLabelCreateResiduesIT extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(180)
  void aCompositeCreatedByHandWithoutInheritanceIsCompletedByTheLeader() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    final Database followerDb = getServerDatabase(follower, getDatabaseName());

    leaderDb.command("sql", "CREATE VERTEX TYPE Issue11064A").close();
    leaderDb.command("sql", "CREATE VERTEX TYPE Issue11064B").close();
    leaderDb.command("sql", "CREATE VERTEX TYPE `Issue11064A~Issue11064B`").close();
    awaitType(followerDb, "Issue11064A~Issue11064B");
    assertThat(followerDb.getSchema().getType("Issue11064A~Issue11064B").instanceOf("Issue11064A")).isFalse();

    followerDb.begin();
    try {
      markClientManaged(followerDb);
      followerDb.command("opencypher", "CREATE (n:Issue11064A:Issue11064B {name: 'x'})").close();
      followerDb.commit();
    } finally {
      if (followerDb.isTransactionActive())
        followerDb.rollback();
    }

    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      await().atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() -> {
        final Schema schema = getServerDatabase(server, getDatabaseName()).getSchema();
        assertThat(schema.getType("Issue11064A~Issue11064B").instanceOf("Issue11064A")).as("server %d", server).isTrue();
        assertThat(schema.getType("Issue11064A~Issue11064B").instanceOf("Issue11064B")).as("server %d", server).isTrue();
        assertThat(countOn(getServerDatabase(server, getDatabaseName()), "Issue11064A")).as("server %d", server).isEqualTo(1L);
      });
    }
  }

  @Test
  @Timeout(180)
  void aCompositeWhoseLabelTypeIsMissingIsCompletedByTheLeader() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    final Database followerDb = getServerDatabase(follower, getDatabaseName());

    leaderDb.command("sql", "CREATE VERTEX TYPE `Issue11064C~Issue11064D`").close();
    awaitType(followerDb, "Issue11064C~Issue11064D");
    assertThat(followerDb.getSchema().existsType("Issue11064C")).isFalse();

    followerDb.begin();
    try {
      markClientManaged(followerDb);
      followerDb.command("opencypher", "CREATE (n:Issue11064C:Issue11064D {name: 'x'})").close();
      followerDb.commit();
    } finally {
      if (followerDb.isTransactionActive())
        followerDb.rollback();
    }

    for (int i = 0; i < getServerCount(); i++) {
      final int server = i;
      await().atMost(60, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).untilAsserted(() -> {
        final Schema schema = getServerDatabase(server, getDatabaseName()).getSchema();
        assertThat(schema.existsType("Issue11064C")).as("server %d", server).isTrue();
        assertThat(schema.getType("Issue11064C~Issue11064D").instanceOf("Issue11064D")).as("server %d", server).isTrue();
      });
    }
  }

  /** A leader without the function answers "Function not found": the follower gives the retryable refusal of before, not that error. */
  @Test
  @Timeout(180)
  void aLeaderWithoutTheFunctionGivesTheRefusalOfBeforeIt() {
    final int leader = requireLeaderIndex();
    final int follower = firstFollowerOf(leader);
    final Database followerDb = getServerDatabase(follower, getDatabaseName());

    final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
    final String key = SQLFunctionEnsureLabelTypes.NAME.toLowerCase(Locale.ENGLISH);
    // The factory is shared by every node of this JVM: removing the function makes every leader one that predates it
    factory.unregister(key);
    try {
      assertThat(factory.getFunctions()).doesNotContainKey(key);
      followerDb.begin();
      try {
        markClientManaged(followerDb);
        final Throwable error = catchThrowable(
            () -> followerDb.command("opencypher", "CREATE (n:Issue11064E:Issue11064F {name: 'x'})").close());
        assertThat(CauseChain.find(error, ServerIsNotTheLeaderException.class)).as("got %s", error).isNotNull();
      } finally {
        if (followerDb.isTransactionActive())
          followerDb.rollback();
      }
    } finally {
      factory.register(SQLFunctionEnsureLabelTypes.NAME, SQLFunctionEnsureLabelTypes.class);
    }
    assertThat(factory.getFunctions()).containsKey(key);
  }

  private static void awaitType(final Database db, final String type) {
    await().atMost(30, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> assertThat(db.getSchema().existsType(type)).isTrue());
  }

  private static void markClientManaged(final Database db) {
    ((DatabaseInternal) db).getTransaction().setClientManaged(true);
  }

  private static long countOn(final Database db, final String label) {
    try (final ResultSet rs = db.query("opencypher", "MATCH (n:" + label + ") RETURN count(n) AS c")) {
      return rs.next().<Number>getProperty("c").longValue();
    } catch (final RuntimeException e) {
      return 0L;
    }
  }
}
