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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5655, the Cypher half of #5492: {@code OpenCypherQueryEngine} held the inner
 * {@code LocalDatabase} and handed it to everything it executed, so the commits that happen inside Cypher
 * execution went to {@code LocalDatabase.commit()} on an HA leader - pages published locally, nothing
 * proposed to Raft. Followers then trail by exactly those page versions and the next replicated entry
 * touching one of them fails its version check with {@code WALVersionGapException}.
 * <p>
 * Two distinct paths reach that commit, and they fail independently, so each has its own test:
 * <ul>
 *   <li>the explicit {@code COMMIT} statement, which {@code executeTransaction()} runs straight against
 *       the engine's field;</li>
 *   <li>the auto-commit of a write step - {@code SetStep}, {@code DeleteStep}, {@code MergeStep},
 *       {@code RemoveStep}, {@code ForeachStep} - which calls {@code begin()}/{@code commit()} directly on
 *       {@code context.getDatabase()} when no transaction is already open.</li>
 * </ul>
 * A third test pins the entry point rather than the commit site: a write can reach execution through
 * {@code query()} via {@code PROFILE}, which is why the fix keys off {@code isReadOnly()} instead of off
 * {@code command()}-vs-{@code query()} as the SQL fix could.
 * <p>
 * {@code CreateStep} is deliberately not covered: it wraps its work in {@code database.transaction(...)},
 * and {@code LocalDatabase.transaction()} drives {@code wrappedDatabaseInstance} internally, so a bare
 * {@code CREATE} already replicated. That asymmetry is why a Cypher smoke test over CREATE alone stayed
 * green while SET and the explicit COMMIT lost their writes.
 * <p>
 * Each test asserts the WAL gap counter <em>before</em> querying the follower: a resync reinstalls the
 * follower's database, and a count issued after it throws {@code DatabaseIsClosed}, which reads like
 * infrastructure noise rather than the divergence that caused it.
 */
@Tag("slow")
class Issue5655CypherCommitsOnInnerDatabaseIT extends BaseRaftHATest {

  private static final String TYPE_EXPLICIT_TX = "CypherExplicitTxNode";
  private static final String TYPE_AUTOCOMMIT  = "CypherAutocommitNode";
  private static final String TYPE_PROFILE     = "CypherProfileNode";
  private static final String TYPE_DDL         = "CypherDDLNode";

  @Override
  protected int getServerCount() {
    return 2;
  }

  @AfterEach
  void cleanupHooks() {
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = null;
  }

  /**
   * {@code START TRANSACTION} / write / {@code COMMIT} issued through the Cypher engine. The COMMIT is the
   * only one of the three that differs between the two instances: {@code begin()} and {@code rollback()}
   * delegate to the inner database on the Raft wrapper too, {@code commit()} is where the proposal happens.
   */
  @Test
  void explicitCypherCommitReachesFollowers() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE VERTEX TYPE " + TYPE_EXPLICIT_TX);
    waitForAllServers();

    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

    leaderDb.command("cypher", "START TRANSACTION");
    leaderDb.command("cypher", "CREATE (n:" + TYPE_EXPLICIT_TX + " {id: 1})");
    leaderDb.command("cypher", "COMMIT");

    assertThat(countOn(leaderIndex, TYPE_EXPLICIT_TX)).as("leader wrote the vertex").isEqualTo(1);

    waitForAllServers();

    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
        .as("follower must see no WAL version gap: the Cypher COMMIT has to be proposed to Raft")
        .isZero();

    assertThat(awaitCountOn(followerIndex, TYPE_EXPLICIT_TX, 1))
        .as("a write committed through the Cypher COMMIT statement must reach the follower")
        .isEqualTo(1);

    // A page bumped on the leader but not on the follower does not fail until something touches it again,
    // so an ordinary replicated write into the same bucket is what surfaces a gap that survived the above.
    leaderDb.command("sql", "INSERT INTO " + TYPE_EXPLICIT_TX + " SET id = 2");
    waitForAllServers();

    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
        .as("no WAL version gap after a subsequent ordinary write into the same bucket")
        .isZero();
    assertThat(awaitCountOn(followerIndex, TYPE_EXPLICIT_TX, 2))
        .as("follower converges with the leader after the subsequent ordinary write")
        .isEqualTo(2);
  }

  /**
   * Auto-commit {@code SET} with no transaction open. {@code SetStep} opens its own transaction and commits
   * it on {@code context.getDatabase()}, which is whatever the engine handed the execution plan - so this
   * fails even though the {@code CREATE} that seeded the vertex replicated correctly.
   */
  @Test
  void autocommitCypherWriteStepReachesFollowers() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE VERTEX TYPE " + TYPE_AUTOCOMMIT);
    leaderDb.command("cypher", "CREATE (n:" + TYPE_AUTOCOMMIT + " {id: 1, marked: 0})");

    waitForAllServers();
    assertThat(awaitCountOn(followerIndex, TYPE_AUTOCOMMIT, 1))
        .as("baseline replicated before the SET: CREATE routes through database.transaction() and is unaffected")
        .isEqualTo(1);

    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

    leaderDb.command("cypher", "MATCH (n:" + TYPE_AUTOCOMMIT + " {id: 1}) SET n.marked = 1");

    assertThat(markedCountOn(leaderIndex, TYPE_AUTOCOMMIT)).as("leader applied the SET").isEqualTo(1);

    waitForAllServers();

    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
        .as("follower must see no WAL version gap: the write step's auto-commit has to be proposed to Raft")
        .isZero();

    assertThat(awaitMarkedCountOn(followerIndex, TYPE_AUTOCOMMIT, 1))
        .as("the value written by the auto-committing SET step must reach the follower")
        .isEqualTo(1);
  }

  /**
   * The reason {@code executionDatabase()} switches on {@code isReadOnly()} rather than on the entry point, which is
   * how the SQL fix (#5652) drew the same line.
   * <p>
   * {@code query()} does not imply read-only on this engine: {@code PROFILE} deliberately bypasses the idempotency
   * gate so a plan can be inspected under execution, so {@code PROFILE MATCH ... SET ...} arrives through
   * {@code query()} and writes. Switching on the entry point would leave exactly this statement committing on the
   * inner instance, and nothing else in the suite would notice - which is what makes it worth its own test rather
   * than a comment. It also covers the uncached branch of {@code execute()}, since profile mode never uses the plan
   * cache.
   */
  @Test
  void profiledCypherWriteThroughQueryReachesFollowers() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE VERTEX TYPE " + TYPE_PROFILE);
    leaderDb.command("cypher", "CREATE (n:" + TYPE_PROFILE + " {id: 1, marked: 0})");

    waitForAllServers();
    assertThat(awaitCountOn(followerIndex, TYPE_PROFILE, 1)).as("baseline replicated before the profiled SET")
        .isEqualTo(1);

    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

    // query(), not command(): the whole point is that a write reaches execution through the read entry point.
    leaderDb.query("cypher", "PROFILE MATCH (n:" + TYPE_PROFILE + " {id: 1}) SET n.marked = 1").close();

    assertThat(markedCountOn(leaderIndex, TYPE_PROFILE)).as("leader applied the profiled SET").isEqualTo(1);

    waitForAllServers();

    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
        .as("follower must see no WAL version gap: a profiled write executed through query() still commits")
        .isZero();

    assertThat(awaitMarkedCountOn(followerIndex, TYPE_PROFILE, 1))
        .as("a write that reaches execution through query() via PROFILE must replicate like any other")
        .isEqualTo(1);
  }

  /**
   * Cypher DDL is the one write-shaped path this fix deliberately leaves on the raw instance, so it gets a test
   * rather than an argument.
   * <p>
   * {@code executeDDL()} runs against {@code database.getSchema()}, and {@code LocalSchema} is constructed with
   * {@code wrappedDatabaseInstance} (`LocalDatabase.java:2433`), so the schema layer replicates regardless of which
   * instance the engine holds - which is also why SQL DDL replicated before #5492 was found. Given that the whole
   * class of bug is "committed on the wrong instance", reading the constructor is weaker evidence than watching an
   * index appear on a follower.
   */
  @Test
  void cypherDDLReachesFollowers() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

    leaderDb.command("cypher", "CREATE INDEX FOR (n:" + TYPE_DDL + ") ON (n.code)").close();

    assertThat(indexedPropertyExistsOn(leaderIndex)).as("leader created the index").isTrue();

    waitForAllServers();

    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
        .as("no WAL version gap from Cypher DDL")
        .isZero();

    assertThat(awaitIndexOn(followerIndex))
        .as("a Cypher CREATE INDEX must reach the follower: the schema layer holds the wrapped instance already")
        .isTrue();
  }

  /**
   * Counts through a freshly resolved database handle. A snapshot resync reinstalls the follower's database, so a
   * handle cached before it throws {@code DatabaseIsClosed} - which reads like an infrastructure failure rather than
   * the divergence that caused it.
   */
  private long countOn(final int serverIndex, final String typeName) {
    // count(id) rather than count(*): the latter reads a cached per-bucket counter, which is the wrong tool
    // when the question is whether the pages themselves arrived.
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    return ((Number) db.command("sql", "SELECT count(id) AS cnt FROM " + typeName).next().getProperty("cnt"))
        .longValue();
  }

  private long markedCountOn(final int serverIndex, final String typeName) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    return ((Number) db.command("sql", "SELECT count(id) AS cnt FROM " + typeName + " WHERE marked = 1")
        .next().getProperty("cnt")).longValue();
  }

  /**
   * True when the follower has both the type and an index covering {@code code}. Resolved through a fresh handle for
   * the same reason as {@link #countOn}.
   */
  private boolean indexedPropertyExistsOn(final int serverIndex) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    final Schema schema = db.getSchema();
    if (!schema.existsType(TYPE_DDL))
      return false;
    return schema.getType(TYPE_DDL).getIndexByProperties("code") != null;
  }

  private boolean awaitIndexOn(final int serverIndex) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      try {
        if (indexedPropertyExistsOn(serverIndex))
          return true;
      } catch (final RuntimeException e) {
        // Mid-resync the database is closed and being reinstalled; keep polling until the deadline.
      }
      Thread.sleep(250);
    }
    return false;
  }

  private long awaitCountOn(final int serverIndex, final String typeName, final long expected)
      throws InterruptedException {
    return await(expected, () -> countOn(serverIndex, typeName));
  }

  private long awaitMarkedCountOn(final int serverIndex, final String typeName, final long expected)
      throws InterruptedException {
    return await(expected, () -> markedCountOn(serverIndex, typeName));
  }

  /**
   * Polls until the count reaches {@code expected} or the deadline passes, then returns whatever was last read
   * successfully.
   * <p>
   * On timeout it deliberately returns that last good reading rather than a sentinel, so the assertion reports the
   * state the follower is actually stuck in - {@code but was: 0L}, the write never arrived - instead of a
   * {@code -1L} that says only "the helper gave up" and hides which of the two happened. {@code -1} survives to the
   * assertion only when every single attempt threw, which is itself the distinct diagnosis: the follower never
   * became queryable at all.
   */
  private long await(final long expected, final LongSupplier supplier) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 30_000;
    long lastRead = -1;
    while (System.currentTimeMillis() < deadline) {
      try {
        lastRead = supplier.getAsLong();
        if (lastRead == expected)
          return lastRead;
      } catch (final RuntimeException e) {
        // Mid-resync the database is closed and being reinstalled; keep polling until the deadline. The previous
        // good reading is kept: it describes the follower better than the fact that one poll hit a resync window.
      }
      Thread.sleep(250);
    }
    return lastRead;
  }
}
