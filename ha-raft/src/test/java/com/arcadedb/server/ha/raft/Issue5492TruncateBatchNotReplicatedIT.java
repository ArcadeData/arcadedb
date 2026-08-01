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
import com.arcadedb.query.QueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5492: a statement that commits mid-execution committed on the inner
 * {@code LocalDatabase} instead of the Raft wrapper, so those commits applied pages on the leader and were never
 * proposed to Raft.
 * <p>
 * {@code TRUNCATE TYPE} deletes in batches of {@link GlobalConfiguration#TRUNCATE_BATCH_SIZE} (default 1000),
 * calling {@code commit()} on {@code CommandContext.getDatabase()} after each. {@code SQLQueryEngine} handed
 * statements the raw instance, so on an HA leader that resolved to {@code LocalDatabase.commit()} - pages published
 * locally, nothing replicated. Followers then trail by exactly those page versions, and the next replicated entry
 * touching one of those pages fails its version check with {@code WALVersionGapException}, marking the database
 * diverged and triggering a snapshot resync that the next entry immediately re-breaks.
 * <p>
 * {@code SQLScriptQueryEngine} has always resolved {@code getWrappedDatabaseInstance()}, which is why the identical
 * statement replicates correctly under {@code sqlscript} and not under {@code sql}. That asymmetry was the defect.
 * <p>
 * The batch size is lowered here because the default of 1000 is also why this never reproduced in-process: earlier
 * harnesses truncated 180 to 450 records, so {@code count % 1000 == 0} never held and no mid-statement commit ever
 * ran. The materialized view of the original report refreshes a 3000-row view, which crosses it on every refresh.
 * <p>
 * Both halves are asserted separately, so a regression that loses the deletes is distinguishable from one that also
 * diverges the cluster.
 */
@Tag("slow")
class Issue5492TruncateBatchNotReplicatedIT extends BaseRaftHATest {

  private static final String TYPE_COMMAND  = "TruncateBatchDoc";
  private static final String TYPE_ANALYZED = "TruncateBatchAnalyzedDoc";
  private static final int    BATCH_SIZE = 2;
  private static final int    RECORDS    = 5;

  @Override
  protected int getServerCount() {
    return 2;
  }

  @AfterEach
  void cleanupHooks() {
    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = null;
  }

  @Test
  void truncateBatchCommitsReachFollowers() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    // RECORDS deleted in batches of BATCH_SIZE leaves at least one mid-statement commit that is not the
    // statement's last, which is the one whose pages went missing.
    leaderDb.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, BATCH_SIZE);

    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + TYPE_COMMAND);
    for (int i = 0; i < RECORDS; i++)
      leaderDb.command("sql", "INSERT INTO " + TYPE_COMMAND + " SET id = " + i);

    waitForAllServers();
    assertThat(awaitCountOn(followerIndex, TYPE_COMMAND, RECORDS)).as("baseline replicated before the truncate").isEqualTo(RECORDS);

    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

    // Run it exactly as MaterializedViewRefresher does: a dedicated transaction on the wrapper, with the
    // statement issued through the 'sql' engine. The batch commits happen inside, before this one returns.
    final DatabaseInternal wrapped = ((DatabaseInternal) leaderDb).getWrappedDatabaseInstance();
    wrapped.transaction(() -> wrapped.command("sql", "TRUNCATE TYPE `" + TYPE_COMMAND + "`").close(), false);

    assertThat(countOn(leaderIndex, TYPE_COMMAND)).as("leader truncated the type").isZero();

    waitForAllServers();

    // Asserted before any follower query: the statement's last batch IS replicated, and it carries page
    // versions the follower cannot reach because the earlier batches never shipped, so the gap fires here.
    // The resync it triggers reinstalls the follower's database, which would otherwise surface as an
    // unrelated-looking DatabaseIsClosed from the count below rather than as the divergence it is.
    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
        .as("follower must see no WAL version gap: every batch commit of the truncate has to be replicated")
        .isZero();

    assertThat(awaitCountOn(followerIndex, TYPE_COMMAND, 0))
        .as("every batch commit of the truncate must reach the follower, not only the statement's last one")
        .isZero();

    // A page bumped on the leader but not on the follower does not fail until something touches it again,
    // so an ordinary replicated write into the same bucket is what would surface a surviving gap.
    leaderDb.command("sql", "INSERT INTO " + TYPE_COMMAND + " SET id = 100");
    waitForAllServers();

    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
        .as("no WAL version gap after a subsequent ordinary write into the truncated bucket")
        .isZero();
    assertThat(awaitCountOn(followerIndex, TYPE_COMMAND, 1))
        .as("follower converges with the leader after the subsequent ordinary write")
        .isEqualTo(1);
  }

  /**
   * The MCP command tool ({@code ExecuteCommandTool}) never calls {@code command()}: it analyzes once and runs SQL
   * through {@code AnalyzedQuery.execute()}, falling back to {@code database.command()} only for engines whose
   * {@code execute()} returns null. That is a second entry point into statement execution, and it took its own fix -
   * so it takes its own test, or a refactor reopens #5492 for that caller alone with everything else still green.
   */
  @Test
  void truncateBatchCommitsReachFollowersViaAnalyzedQuery() throws Exception {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final int followerIndex = 1 - leaderIndex;

    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());
    leaderDb.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, BATCH_SIZE);

    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + TYPE_ANALYZED);
    for (int i = 0; i < RECORDS; i++)
      leaderDb.command("sql", "INSERT INTO " + TYPE_ANALYZED + " SET id = " + i);

    waitForAllServers();
    assertThat(awaitCountOn(followerIndex, TYPE_ANALYZED, RECORDS)).as("baseline replicated before the truncate").isEqualTo(RECORDS);

    ArcadeStateMachine.TEST_WAL_GAP_COUNTER = new AtomicInteger(0);

    // Exactly the MCP tool's shape: resolve the engine off the database, analyze, then execute.
    final DatabaseInternal wrapped = ((DatabaseInternal) leaderDb).getWrappedDatabaseInstance();
    wrapped.transaction(() -> {
      final QueryEngine.AnalyzedQuery analyzed = wrapped.getQueryEngine("sql")
          .analyze("TRUNCATE TYPE `" + TYPE_ANALYZED + "`");
      final ResultSet rs = analyzed.execute(Collections.emptyMap());
      // Cast to Object: ResultSet extends both Iterator and Spliterator, so the AssertJ overloads are ambiguous.
      assertThat((Object) rs).as("SQL analyze().execute() returns a result set rather than deferring to command()")
          .isNotNull();
      rs.close();
    }, false);

    assertThat(countOn(leaderIndex, TYPE_ANALYZED)).as("leader truncated the type").isZero();

    waitForAllServers();
    assertThat(ArcadeStateMachine.TEST_WAL_GAP_COUNTER.get())
        .as("no WAL version gap when the truncate runs through analyze().execute()")
        .isZero();
    assertThat(awaitCountOn(followerIndex, TYPE_ANALYZED, 0))
        .as("batch commits must replicate on the analyzed-query path too, not only through command()")
        .isZero();
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

  private long awaitCountOn(final int serverIndex, final String typeName, final long expected) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + 30_000;
    long count = -1;
    while (System.currentTimeMillis() < deadline) {
      try {
        count = countOn(serverIndex, typeName);
        if (count == expected)
          return count;
      } catch (final RuntimeException e) {
        // Mid-resync the database is closed and being reinstalled; keep polling until the deadline.
        count = -1;
      }
      Thread.sleep(250);
    }
    return count;
  }
}
