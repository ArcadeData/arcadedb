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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.QueryNotIdempotentException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A commit that runs in the middle of a statement, or inside a schema callback, must go through the Raft wrapper
 * ({@code RaftReplicatedDatabase.commit()}), not the inner {@code LocalDatabase}: a commit on the inner instance applies
 * its pages on the leader and proposes nothing, so the followers trail by exactly those page versions and the next
 * replicated entry touching one of them fails its version check with {@code WALVersionGapException}, marking the
 * database diverged. End to end on ONE two-node cluster shared by every test ({@link SharedClusterRaftHATest}). Renamed
 * from {@code Issue5655CypherCommitsOnInnerDatabaseIT}, with {@code Issue5492SchemaWalNotShippedIT} and
 * {@code Issue5492TruncateBatchNotReplicatedIT} folded in: all three ran on the same fixture, two servers, in-memory Raft
 * storage, no configuration override, and cleared {@code ArcadeStateMachine.TEST_WAL_GAP_COUNTER} after each test.
 * <ul>
 * <li><b>#5492</b>, statements: {@code SQLQueryEngine} handed statements the raw instance, so {@code TRUNCATE TYPE}
 * batching every {@link GlobalConfiguration#TRUNCATE_BATCH_SIZE} records committed on {@code LocalDatabase}.
 * {@code SQLScriptQueryEngine} always resolved the wrapper, which is why the same statement replicated under
 * {@code sqlscript} and not under {@code sql}. Since #6220 the batching runs only when {@code TRUNCATE} owns its
 * transaction, so both tests issue the statement with NO enclosing transaction, and they lower the batch size because the
 * default of 1000 is why this never reproduced in-process. The analyzed-query test covers the MCP command tool's entry
 * point ({@code AnalyzedQuery.execute()}).</li>
 * <li><b>#5492</b>, schema callback: a commit inside a {@code recordFileChanges()} callback has its WAL buffered to travel
 * in the {@code SCHEMA_ENTRY}, and the send was guarded on files and schema version only, so a callback that only
 * committed pages had them applied on the leader and the only copy for the followers discarded.</li>
 * <li><b>#5655</b>, the Cypher half of #5492: {@code OpenCypherQueryEngine} held the inner instance, so the explicit
 * {@code COMMIT} statement and the auto-commit of a write step ({@code SetStep}, {@code DeleteStep}, ...) bypassed
 * replication; {@code PROFILE} of a write runs the uncached branch and is refused by {@code query()} since #9313.
 * {@code CreateStep} wraps its work in {@code database.transaction(...)}, which drives the wrapper, which is why a CREATE
 * smoke test stayed green. Cypher DDL goes through the schema layer, which holds the wrapper already, and gets a test
 * rather than an argument.</li>
 * </ul>
 * Every test asserts the WAL gap counter BEFORE querying the follower: a resync reinstalls the follower's database, and a
 * count issued after it throws {@code DatabaseIsClosed}, which reads like infrastructure noise rather than the divergence
 * that caused it.
 * <p>
 * Why these share a cluster: every type a test creates is named through {@link #uniqueName(String)}, and the counts they
 * assert are of that type only. The JVM-wide {@code TEST_WAL_GAP_COUNTER} is armed inside a test and cleared after each
 * one ({@link #cleanupHooks()}). The truncate tests lower {@code TRUNCATE_BATCH_SIZE} on the leader's database
 * configuration and put the previous value back in a {@code finally} ({@link #restoreTruncateBatchSize}). A test that
 * finds a gap leaves the follower quarantined or resyncing, which the shared base's health check sees and reboots. No
 * test stops, restarts or isolates a node or moves leadership.
 * <p>
 * Every source class was tagged {@code slow}, so this class is.
 */
@Tag("slow")
class InnerDatabaseCommitReplicationIT extends SharedClusterRaftHATest {

  /** #5492 (truncate). */
  private static final int BATCH_SIZE = 2;
  private static final int RECORDS    = 5;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @AfterEach
  void cleanupHooks() {
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = null;
  }

  // ------------------------------------------------------------------ Cypher commits (#5655)

  /**
   * Issue #5655, from {@code Issue5655CypherCommitsOnInnerDatabaseIT}.
   * <p>
   * {@code START TRANSACTION} / write / {@code COMMIT} issued through the Cypher engine. The COMMIT is the
   * only one of the three that differs between the two instances: {@code begin()} and {@code rollback()}
   * delegate to the inner database on the Raft wrapper too, {@code commit()} is where the proposal happens.
   */
  @Test
  void explicitCypherCommitReachesFollowers() throws Exception {
    final String typeExplicitTx = uniqueName("CypherExplicitTxNode");
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE VERTEX TYPE " + typeExplicitTx);
    waitForAllServers();

    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

    leaderDb.command("cypher", "START TRANSACTION");
    leaderDb.command("cypher", "CREATE (n:" + typeExplicitTx + " {id: 1})");
    leaderDb.command("cypher", "COMMIT");

    assertThat(countOn(leaderIndex, typeExplicitTx)).as("leader wrote the vertex").isEqualTo(1);

    waitForAllServers();

    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
        .as("follower must see no WAL version gap: the Cypher COMMIT has to be proposed to Raft")
        .isZero();

    assertThat(awaitCountOn(followerIndex, typeExplicitTx, 1))
        .as("a write committed through the Cypher COMMIT statement must reach the follower")
        .isEqualTo(1);

    // A page bumped on the leader but not on the follower does not fail until something touches it again,
    // so an ordinary replicated write into the same bucket is what surfaces a gap that survived the above.
    leaderDb.command("sql", "INSERT INTO " + typeExplicitTx + " SET id = 2");
    waitForAllServers();

    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
        .as("no WAL version gap after a subsequent ordinary write into the same bucket")
        .isZero();
    assertThat(awaitCountOn(followerIndex, typeExplicitTx, 2))
        .as("follower converges with the leader after the subsequent ordinary write")
        .isEqualTo(2);
  }

  /**
   * Issue #5655, from {@code Issue5655CypherCommitsOnInnerDatabaseIT}.
   * <p>
   * Auto-commit {@code SET} with no transaction open. {@code SetStep} opens its own transaction and commits
   * it on {@code context.getDatabase()}, which is whatever the engine handed the execution plan - so this
   * fails even though the {@code CREATE} that seeded the vertex replicated correctly.
   */
  @Test
  void autocommitCypherWriteStepReachesFollowers() throws Exception {
    final String typeAutocommit = uniqueName("CypherAutocommitNode");
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE VERTEX TYPE " + typeAutocommit);
    leaderDb.command("cypher", "CREATE (n:" + typeAutocommit + " {id: 1, marked: 0})");

    waitForAllServers();
    assertThat(awaitCountOn(followerIndex, typeAutocommit, 1))
        .as("baseline replicated before the SET: CREATE routes through database.transaction() and is unaffected")
        .isEqualTo(1);

    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

    leaderDb.command("cypher", "MATCH (n:" + typeAutocommit + " {id: 1}) SET n.marked = 1");

    assertThat(markedCountOn(leaderIndex, typeAutocommit)).as("leader applied the SET").isEqualTo(1);

    waitForAllServers();

    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
        .as("follower must see no WAL version gap: the write step's auto-commit has to be proposed to Raft")
        .isZero();

    assertThat(awaitMarkedCountOn(followerIndex, typeAutocommit, 1))
        .as("the value written by the auto-committing SET step must reach the follower")
        .isEqualTo(1);
  }

  /**
   * Issue #5655, from {@code Issue5655CypherCommitsOnInnerDatabaseIT}.
   * <p>
   * {@code PROFILE} executes the statement it wraps, so a profiled write is a write: since #9313 {@code query()}
   * refuses it exactly like the bare write, and {@code command()} is the legitimate entry point. The profiled write
   * must still commit on the replicated instance, and nothing else in the suite would notice if it did not - which is
   * what makes it worth its own test rather than a comment. It also covers the uncached branch of {@code execute()},
   * since profile mode never uses the plan cache.
   */
  @Test
  void profiledCypherWriteThroughQueryReachesFollowers() throws Exception {
    final String typeProfile = uniqueName("CypherProfileNode");
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.command("sql", "CREATE VERTEX TYPE " + typeProfile);
    leaderDb.command("cypher", "CREATE (n:" + typeProfile + " {id: 1, marked: 0})");

    waitForAllServers();
    assertThat(awaitCountOn(followerIndex, typeProfile, 1)).as("baseline replicated before the profiled SET")
        .isEqualTo(1);

    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

    final String profiledWrite = "PROFILE MATCH (n:" + typeProfile + " {id: 1}) SET n.marked = 1";

    // The read entry point refuses a profiled write (#9313) and must leave nothing behind.
    assertThatThrownBy(() -> leaderDb.query("cypher", profiledWrite).close())
        .isInstanceOf(QueryNotIdempotentException.class);
    assertThat(markedCountOn(leaderIndex, typeProfile)).as("a refused profiled write must not be applied")
        .isZero();

    leaderDb.command("cypher", profiledWrite).close();

    assertThat(markedCountOn(leaderIndex, typeProfile)).as("leader applied the profiled SET").isEqualTo(1);

    waitForAllServers();

    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
        .as("follower must see no WAL version gap: a profiled write still commits on the replicated instance")
        .isZero();

    assertThat(awaitMarkedCountOn(followerIndex, typeProfile, 1))
        .as("a profiled write must replicate like any other")
        .isEqualTo(1);
  }

  /**
   * Issue #5655, from {@code Issue5655CypherCommitsOnInnerDatabaseIT}.
   * <p>
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
    final String typeDdl = uniqueName("CypherDDLNode");
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

    leaderDb.command("cypher", "CREATE INDEX FOR (n:" + typeDdl + ") ON (n.code)").close();

    assertThat(indexedPropertyExistsOn(leaderIndex, typeDdl)).as("leader created the index").isTrue();

    waitForAllServers();

    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
        .as("no WAL version gap from Cypher DDL")
        .isZero();

    assertThat(awaitIndexOn(followerIndex, typeDdl))
        .as("a Cypher CREATE INDEX must reach the follower: the schema layer holds the wrapped instance already")
        .isTrue();
  }

  // countOn()/awaitCountOn()/awaitValue() moved to BaseRaftHATest (issue #5977): #5655 and #5492 were two
  // independent reinventions of the same resolve-fresh-and-retry shape.

  /** #5655. */
  private long markedCountOn(final int serverIndex, final String typeName) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    return ((Number) db.command("sql", "SELECT count(id) AS cnt FROM " + typeName + " WHERE marked = 1")
        .next().getProperty("cnt")).longValue();
  }

  /**
   * #5655: true when the server has both the type and an index covering {@code code}. Resolved through a fresh handle
   * for the same reason as {@link BaseRaftHATest#countOn}: a stale handle cached across a snapshot-reinstall
   * resync throws {@code DatabaseIsClosed}.
   */
  private boolean indexedPropertyExistsOn(final int serverIndex, final String typeName) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    final Schema schema = db.getSchema();
    if (!schema.existsType(typeName))
      return false;
    return schema.getType(typeName).getIndexByProperties("code") != null;
  }

  /** #5655. */
  private boolean awaitIndexOn(final int serverIndex, final String typeName) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      try {
        if (indexedPropertyExistsOn(serverIndex, typeName))
          return true;
      } catch (final RuntimeException e) {
        // Mid-resync the database is closed and being reinstalled; keep polling until the deadline.
      }
      Thread.sleep(250);
    }
    return false;
  }

  /** #5655. */
  private long awaitMarkedCountOn(final int serverIndex, final String typeName, final long expected)
      throws InterruptedException {
    return awaitValue(expected, () -> markedCountOn(serverIndex, typeName));
  }

  // ------------------------------------------------------------------ WAL buffered in a schema callback (#5492)

  /**
   * Issue #5492, from {@code Issue5492SchemaWalNotShippedIT}.
   * <p>
   * The follower's page version trails the leader's by the number of dropped page writes, so the next ordinary
   * transaction touching one of those pages ships a version the follower cannot apply. Both halves are asserted so a
   * regression that loses the write is distinguishable from one that also diverges the cluster. The sibling
   * {@code runWithCompactionReplication} always guarded the same two thread-locals with {@code walEntries.isEmpty()}
   * included; the asymmetry between the two was the defect.
   */
  @Test
  void walBufferedInsideRecordFileChangesReachesFollowers() throws Exception {
    final String typeName = uniqueName("SchemaWalDoc");
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // Create the type through the ordinary DDL path and let it replicate, so the backing files exist
    // on both nodes before the callback below writes into them. Without this the follower would drop
    // the pages for a missing file and the test could not tell that apart from the defect.
    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + typeName);
    leaderDb.command("sql", "INSERT INTO " + typeName + " SET id = 0");
    waitForAllServers();
    // Resolved through countOn(serverIndex, ...), not a handle cached before this point: a snapshot-reinstall
    // resync can close and reinstall the follower's database at any time, and a stale handle then throws
    // DatabaseIsClosedException, which reads like infrastructure noise rather than the resync it is (issue #5977).
    assertThat(countOn(followerIndex, typeName)).as("baseline replicated before the callback").isEqualTo(1);

    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

    // The shape the guard dropped: a recordFileChanges() callback that commits a record into an
    // already-existing type and touches nothing else. No file is created, none is removed, and the
    // schema version does not move, so none of the three conditions in the send guard held.
    final DatabaseInternal wrapped = ((DatabaseInternal) leaderDb).getWrappedDatabaseInstance();
    wrapped.recordFileChanges(() -> {
      wrapped.begin();
      wrapped.newDocument(typeName).set("id", 1).save();
      wrapped.commit();
      return null;
    });

    assertThat(countOn(leaderIndex, typeName)).as("leader applied the record written inside the callback").isEqualTo(2);

    waitForAllServers();
    assertThat(awaitCountOn(followerIndex, typeName, 2))
        .as("WAL buffered inside recordFileChanges must reach the follower, not be discarded")
        .isEqualTo(2);

    // A page whose version was bumped on the leader but not on the follower does not fail until the
    // next transaction touches it. Commit an ordinary write into the same bucket and require the
    // follower to apply it without a gap.
    leaderDb.command("sql", "INSERT INTO " + typeName + " SET id = 2");
    waitForAllServers();

    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
        .as("follower must see no WAL version gap after the callback's pages were replicated")
        .isZero();
    assertThat(awaitCountOn(followerIndex, typeName, 3))
        .as("follower converges with the leader after the subsequent ordinary write")
        .isEqualTo(3);
  }

  // ------------------------------------------------------------------ TRUNCATE batch commits (#5492)

  /** Issue #5492, from {@code Issue5492TruncateBatchNotReplicatedIT}. */
  @Test
  void truncateBatchCommitsReachFollowers() throws Exception {
    final String typeCommand = uniqueName("TruncateBatchDoc");
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // RECORDS deleted in batches of BATCH_SIZE leaves at least one mid-statement commit that is not the
    // statement's last, which is the one whose pages went missing.
    final Object batchSizeBefore = leaderDb.getConfiguration().getValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE);
    leaderDb.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, BATCH_SIZE);
    try {
      leaderDb.command("sql", "CREATE DOCUMENT TYPE " + typeCommand);
      for (int i = 0; i < RECORDS; i++)
        leaderDb.command("sql", "INSERT INTO " + typeCommand + " SET id = " + i);

      waitForAllServers();
      assertThat(awaitCountOn(followerIndex, typeCommand, RECORDS)).as("baseline replicated before the truncate").isEqualTo(RECORDS);

      ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

      // Issued through the 'sql' engine with NO transaction active, which is what makes the statement own one and
      // batch (issue #6220): inside a caller's transaction TRUNCATE no longer commits at all, so wrapping this would
      // silently retire the very mid-statement commit #5492 is about.
      final DatabaseInternal wrapped = ((DatabaseInternal) leaderDb).getWrappedDatabaseInstance();
      wrapped.command("sql", "TRUNCATE TYPE `" + typeCommand + "`").close();

      assertThat(countOn(leaderIndex, typeCommand)).as("leader truncated the type").isZero();

      waitForAllServers();

      // Asserted before any follower query: the statement's last batch IS replicated, and it carries page
      // versions the follower cannot reach because the earlier batches never shipped, so the gap fires here.
      // The resync it triggers reinstalls the follower's database, which would otherwise surface as an
      // unrelated-looking DatabaseIsClosed from the count below rather than as the divergence it is.
      assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
          .as("follower must see no WAL version gap: every batch commit of the truncate has to be replicated")
          .isZero();

      assertThat(awaitCountOn(followerIndex, typeCommand, 0))
          .as("every batch commit of the truncate must reach the follower, not only the statement's last one")
          .isZero();

      // A page bumped on the leader but not on the follower does not fail until something touches it again,
      // so an ordinary replicated write into the same bucket is what would surface a surviving gap.
      leaderDb.command("sql", "INSERT INTO " + typeCommand + " SET id = 100");
      waitForAllServers();

      assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
          .as("no WAL version gap after a subsequent ordinary write into the truncated bucket")
          .isZero();
      assertThat(awaitCountOn(followerIndex, typeCommand, 1))
          .as("follower converges with the leader after the subsequent ordinary write")
          .isEqualTo(1);
    } finally {
      restoreTruncateBatchSize(leaderDb, batchSizeBefore);
    }
  }

  /**
   * Issue #5492, from {@code Issue5492TruncateBatchNotReplicatedIT}.
   * <p>
   * The MCP command tool ({@code ExecuteCommandTool}) never calls {@code command()}: it analyzes once and runs SQL
   * through {@code AnalyzedQuery.execute()}, falling back to {@code database.command()} only for engines whose
   * {@code execute()} returns null. That is a second entry point into statement execution, and it took its own fix -
   * so it takes its own test, or a refactor reopens #5492 for that caller alone with everything else still green.
   */
  @Test
  void truncateBatchCommitsReachFollowersViaAnalyzedQuery() throws Exception {
    final String typeAnalyzed = uniqueName("TruncateBatchAnalyzedDoc");
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    final Object batchSizeBefore = leaderDb.getConfiguration().getValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE);
    leaderDb.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, BATCH_SIZE);
    try {
      leaderDb.command("sql", "CREATE DOCUMENT TYPE " + typeAnalyzed);
      for (int i = 0; i < RECORDS; i++)
        leaderDb.command("sql", "INSERT INTO " + typeAnalyzed + " SET id = " + i);

      waitForAllServers();
      assertThat(awaitCountOn(followerIndex, typeAnalyzed, RECORDS)).as("baseline replicated before the truncate").isEqualTo(RECORDS);

      ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

      // Exactly the MCP tool's shape: resolve the engine off the database, analyze, then execute - and, as above,
      // with no transaction active so the statement owns one and batches.
      final DatabaseInternal wrapped = ((DatabaseInternal) leaderDb).getWrappedDatabaseInstance();
      final QueryEngine.AnalyzedQuery analyzed = wrapped.getQueryEngine("sql")
          .analyze("TRUNCATE TYPE `" + typeAnalyzed + "`");
      final ResultSet rs = analyzed.execute(Collections.emptyMap());
      // Cast to Object: ResultSet extends both Iterator and Spliterator, so the AssertJ overloads are ambiguous.
      assertThat((Object) rs).as("SQL analyze().execute() returns a result set rather than deferring to command()")
          .isNotNull();
      rs.close();

      assertThat(countOn(leaderIndex, typeAnalyzed)).as("leader truncated the type").isZero();

      waitForAllServers();
      assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
          .as("no WAL version gap when the truncate runs through analyze().execute()")
          .isZero();
      assertThat(awaitCountOn(followerIndex, typeAnalyzed, 0))
          .as("batch commits must replicate on the analyzed-query path too, not only through command()")
          .isZero();
    } finally {
      restoreTruncateBatchSize(leaderDb, batchSizeBefore);
    }
  }

  /**
   * #5492: puts back the leader's {@code TRUNCATE_BATCH_SIZE}, so the next test of the shared cluster runs with the value
   * this one found. A failing restore is logged, never thrown, so it cannot mask the failure of the test itself.
   */
  private void restoreTruncateBatchSize(final Database leaderDb, final Object previous) {
    try {
      leaderDb.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, previous);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.WARNING, "TEST: could not restore %s to %s", e,
          GlobalConfiguration.TRUNCATE_BATCH_SIZE.getKey(), previous);
    }
  }
}
