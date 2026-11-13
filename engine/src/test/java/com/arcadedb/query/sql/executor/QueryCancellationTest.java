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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What stops a running SQL statement or bounds its retries: the command deadline on the scan sources, termination and
 * timeout of the index replay, and the {@code COMMIT RETRY} backoff.
 */
class QueryCancellationTest extends TestHelper {

  // ---- #5693: COMMIT RETRY reads the database-scoped retry delay ----
  /*
   * The SQL {@code COMMIT RETRY} step has its own copy of the backoff. TX_RETRY_DELAY has database scope, so that
   * copy must come from the database the script runs against and not from the global static.
   * <p>
   * The command context's own configuration is not a substitute: the script engine is handed an empty one on the
   * embedded API and the server's one over HTTP, and neither carries a per-database override.
   */

  /** Regression test for issue #5693. */
  @Test
  void theStepReadsTheDelayConfiguredOnThisDatabase() {
    final int savedGlobal = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    GlobalConfiguration.TX_RETRY_DELAY.setValue(11);
    try {
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, 777);

      assertThat(newRetryStep().getRetryDelay()).isEqualTo(777);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, savedGlobal);
      GlobalConfiguration.TX_RETRY_DELAY.setValue(savedGlobal);
    }
  }

  /** Regression test for issue #5693. */
  @Test
  void theStepFallsBackToTheGlobalDelayWhenTheDatabaseDoesNotOverrideIt() {
    final int savedGlobal = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    GlobalConfiguration.TX_RETRY_DELAY.setValue(13);
    try {
      assertThat(newRetryStep().getRetryDelay()).isEqualTo(13);
    } finally {
      GlobalConfiguration.TX_RETRY_DELAY.setValue(savedGlobal);
    }
  }

  /**
   * Builds the step the way the script engine does, with a context that carries the database but an empty
   * configuration of its own.
   */
  private RetryStep newRetryStep() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(((DatabaseInternal) database).getWrappedDatabaseInstance());
    return new RetryStep(List.of(), 3, null, Boolean.FALSE, context, false);
  }

  // ---- #6465: plain scan sources honour the command deadline ----
  /*
   * Issue #6465: a WHERE-less aggregation/ORDER BY/DISTINCT over a plain scan drains the whole type
   * server-side with nothing consulting {@code arcadedb.command.timeout}. {@code ScanWithFilterStep} and
   * {@code FilterStep} (used when a WHERE exists) already guard their loop with
   * {@link WorkGuard#forCommandDeadline(CommandContext)}; the plain scan sources - reached whenever there
   * is no WHERE to build a filtered step from - carried no such guard, so a blocking consumer downstream
   * (aggregation, {@code ORDER BY}, {@code DISTINCT}) could run past the configured deadline unbounded by
   * anything but the data size.
   * <p>
   * Rather than reproducing that with a dataset large enough to outrun a real deadline under wall-clock
   * timing (flaky across machines), each test here pins an already-expired deadline directly on the
   * {@link CommandContext} - the same technique
   * {@code Issue6266CommandTimeoutCoverageTest#aPinnedDeadlineIsHonouredWhateverItsValue} uses - and drains
   * the guarded step directly, so the assertion is deterministic: the very first record the step tries to
   * hand back must instead raise a {@link TimeoutException}.
   */

  /** Regression test for issue #6465. */
  @Test
  void fetchFromClusterExecutionStepHonoursAnExpiredDeadline() throws Exception {
    TestHelper.executeInNewDatabase(db -> {
      final DocumentType type = db.getSchema().createDocumentType("PlainScanType");
      type.createProperty("value", Long.class);
      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          db.newDocument("PlainScanType").set("value", (long) i).save();
      });

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      context.setCommandDeadline(System.currentTimeMillis() - 1, "an already expired deadline");

      final FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(type.getFirstBucketId(), context);
      final ResultSet result = step.syncPull(context, 100);

      assertThatThrownBy(() -> drain(result))
          .as("a plain bucket scan with no WHERE must still stop at the command deadline")
          .isInstanceOf(TimeoutException.class)
          .hasMessageContaining("an already expired deadline");
    });
  }

  /** Regression test for issue #6465. */
  @Test
  void fetchFromIndexStepHonoursAnExpiredDeadline() throws Exception {
    TestHelper.executeInNewDatabase(db -> {
      final DocumentType type = db.getSchema().createDocumentType("PlainScanIndexType");
      type.createProperty("value", Long.class);
      final TypeIndex index = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          db.newDocument("PlainScanIndexType").set("value", (long) i).save();
      });

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      context.setCommandDeadline(System.currentTimeMillis() - 1, "an already expired deadline");

      // No condition: a full, unfiltered ascending scan of the index - the DISTINCT/ORDER BY fast path.
      final FetchFromIndexValuesStep step = new FetchFromIndexValuesStep((RangeIndex) index, true, context);
      final ResultSet result = step.syncPull(context, 100);

      assertThatThrownBy(() -> drain(result))
          .as("a full index scan with no WHERE must still stop at the command deadline")
          .isInstanceOf(TimeoutException.class)
          .hasMessageContaining("an already expired deadline");
    });
  }

  /** Regression test for issue #6465. */
  @Test
  void fetchFromRidsStepHonoursAnExpiredDeadline() throws Exception {
    TestHelper.executeInNewDatabase(db -> {
      final DocumentType type = db.getSchema().createDocumentType("PlainScanRidType");
      type.createProperty("value", Long.class);

      final List<RID> rids = new ArrayList<>();
      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          rids.add(db.newDocument("PlainScanRidType").set("value", (long) i).save().getIdentity());
      });

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      context.setCommandDeadline(System.currentTimeMillis() - 1, "an already expired deadline");

      final FetchFromRidsStep step = new FetchFromRidsStep(rids, context);
      final ResultSet result = step.syncPull(context, 100);

      assertThatThrownBy(() -> drain(result))
          .as("a RID-list scan with no WHERE must still stop at the command deadline")
          .isInstanceOf(TimeoutException.class)
          .hasMessageContaining("an already expired deadline");
    });
  }

  /**
   * Regression test for issue #6465.
   * <p>
   * Sanity check: with no deadline configured (the default), every guarded step still returns every row -
   * the guard must cost nothing more than a comparison when disabled, never change the result.
   */
  @Test
  void unguardedScansAreUnaffectedWhenNoDeadlineIsConfigured() throws Exception {
    TestHelper.executeInNewDatabase(db -> {
      final DocumentType type = db.getSchema().createDocumentType("PlainScanNoDeadlineType");
      type.createProperty("value", Long.class);
      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          db.newDocument("PlainScanNoDeadlineType").set("value", (long) i).save();
      });

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);

      final FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(type.getFirstBucketId(), context);
      assertThat(drain(step.syncPull(context, 100))).hasSize(10);
    });
  }

  private static List<Result> drain(final ResultSet resultSet) {
    final List<Result> rows = new ArrayList<>();
    while (resultSet.hasNext())
      rows.add(resultSet.next());
    return rows;
  }

  // ---- #9893: index replay stops on termination and timeout ----
  /*
   * Issue #9893: the replay that reconciles an index entry whose lookup overlapped a commit (#9369) re-reads the whole key and
   * loads every record it holds, up to nine times, and never asked whether the statement was still wanted. A cancelled or
   * timed-out statement kept doing it, and the re-read turned the termination into a WARNING and a fallback.
   */

  /** More than the 256 entries a point lookup holds, so the lookup streams and reconciles entry by entry. */
  private static final int ENTRIES = 2_000;

  private final RunningQueryRegistry registry = new RunningQueryRegistry();

  private void createReplayCancellationItems() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Item");
      type.createProperty("k", String.class);
      database.command("sql", "CREATE INDEX ON Item (k) NOTUNIQUE");
      database.getSchema().createDocumentType("Other");
      for (int i = 0; i < ENTRIES; i++)
        database.newDocument("Item").set("k", "hot").set("n", i).save();
    });
  }

  /** Regression test for issue #9893. */
  @Test
  void terminatedStatementStopsTheReplayInsteadOfReloadingTheKey() throws Exception {
    createReplayCancellationItems();
    final AtomicBoolean armed = new AtomicBoolean(false);
    final AtomicInteger loadsAfterTermination = new AtomicInteger();
    final AtomicBoolean terminated = new AtomicBoolean(false);
    final AtomicReference<Throwable> committerFailure = new AtomicReference<>();
    final AtomicReference<RunningQuery> statement = new AtomicReference<>();

    // On the first record load of the lookup a commit lands (so the lookup overlapped one and the entry is replayed) and the
    // statement is terminated: everything loaded after that is work the termination should have stopped
    final AfterRecordReadListener listener = record -> {
      if (terminated.get())
        loadsAfterTermination.incrementAndGet();
      else if (armed.compareAndSet(true, false)) {
        final Thread committer = new Thread(() -> {
          try {
            database.transaction(() -> database.newDocument("Other").set("x", 1).save());
          } catch (final Throwable e) {
            committerFailure.set(e);
          }
        });
        committer.start();
        try {
          committer.join(30_000);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        statement.get().terminate("root");
        terminated.set(true);
      }
      return record;
    };

    database.getSchema().getType("Item").getEvents().registerListener(listener);
    try (final RunningQuery q = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
      statement.set(q);
      armed.set(true);
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM Item WHERE k = 'hot'")) {
          while (rs.hasNext())
            rs.next();
        }
      }).isInstanceOf(QueryTerminatedException.class);
    } finally {
      database.getSchema().getType("Item").getEvents().unregisterListener(listener);
    }

    assertThat(committerFailure.get()).isNull();
    assertThat(terminated.get()).as("the commit and the termination must have happened during the lookup").isTrue();
    // Before the fix the replay re-read the key and loaded every record of it (ENTRIES - 1) before anything checked
    assertThat(loadsAfterTermination.get()).as("records loaded by the replay after the statement was terminated").isZero();
  }

  /** Regression test for issue #9893. */
  @Test
  void reReadOfTheKeyPassesATerminationThrough() {
    createReplayCancellationItems();
    final FetchFromIndexStep step = indexStepOf("SELECT FROM Item WHERE k = 'hot'");
    // A healthy re-read answers every entry of the key
    final List<RID> all = step.lookupKeyAgain("hot", WorkGuard.forCommandDeadline(null));
    assertThat(all).hasSize(ENTRIES);

    try (final RunningQuery q = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
      q.terminate("root");
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase((DatabaseInternal) database);
      // Before the fix: logged as "Cannot look the key ... up again" and answered null, so the caller carried on
      assertThatThrownBy(() -> step.lookupKeyAgain("hot", WorkGuard.forCommandDeadline(context)))
          .isInstanceOf(QueryTerminatedException.class);
    }
  }

  /** Regression test for issue #9893. */
  @Test
  void reReadOfTheKeyPassesATimeoutThrough() {
    createReplayCancellationItems();
    final FetchFromIndexStep step = indexStepOf("SELECT FROM Item WHERE k = 'hot'");
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase((DatabaseInternal) database);
    context.setCommandDeadline(System.currentTimeMillis() - 1, "test deadline");

    assertThatThrownBy(() -> step.lookupKeyAgain("hot", WorkGuard.forCommandDeadline(context)))
        .isInstanceOf(TimeoutException.class);
  }

  private FetchFromIndexStep indexStepOf(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      final FetchFromIndexStep step = findIndexStep(rs.getExecutionPlan().orElseThrow().getSteps());
      assertThat(step).as("the query must be answered by an index lookup").isNotNull();
      return step;
    }
  }

  private static FetchFromIndexStep findIndexStep(final List<ExecutionStep> steps) {
    if (steps == null)
      return null;
    for (final ExecutionStep step : steps) {
      if (step instanceof final FetchFromIndexStep indexStep)
        return indexStep;
      final FetchFromIndexStep nested = findIndexStep(step.getSubSteps());
      if (nested != null)
        return nested;
    }
    return null;
  }

  // ---- #9902: index replay amplification ----
  /*
   * Issue #9902: a point lookup on a non-unique key that overlapped a commit re-read the whole key and loaded every one of its
   * records, up to 9 times and the last time under the publication lock, whatever the commit had touched. The rows of a key are
   * now checked one by one against the key they were found under, and the key is read again only when one of its entries really
   * lost its record, once, after the original entries of the key ran out.
   */

  private static final int KEY_SIZE = 20_000;

  private void createReplayEntityType() {
    database.command("sql", "CREATE DOCUMENT TYPE Entity");
    database.command("sql", "CREATE PROPERTY Entity.graph STRING");
    database.command("sql", "CREATE PROPERTY Entity.score INTEGER");
    database.command("sql", "CREATE INDEX ON Entity (graph) NOTUNIQUE");
  }

  /**
   * Regression test for issue #9902.
   * <p>
   * A commit that neither removes a record of the key nor moves one to another key must not make the lookup read the key again
   * or load more records than the rows it is asked for take.
   */
  @Test
  void aCommitThatLosesNoEntryCostsNoReplay() throws Exception {
    createReplayEntityType();
    populate("g", KEY_SIZE);
    populate("h", 100);

    final List<RID> served = new ArrayList<>();
    final long readsBefore;
    final GetValueFromIndexEntryStep step;
    try (final ResultSet rs = database.query("sql", "SELECT FROM Entity WHERE graph = 'g' AND score = 3 LIMIT 200")) {
      step = findStep(rs.getExecutionPlan().orElseThrow());
      assertThat(step).as("the lookup must go through the index").isNotNull();
      readsBefore = readRecords();
      for (int i = 0; i < 5; i++)
        served.add(rs.next().getIdentity().orElseThrow());

      // commits on the same type, during the lookup: new records of the key, and an update of a field the key does not hold
      inAnotherThread(() -> database.transaction(() -> {
        for (int i = 0; i < 10; i++)
          database.newDocument("Entity").set("graph", "g").set("score", 7).save();
        database.command("sql", "UPDATE Entity SET score = 9 WHERE graph = 'h'").close();
      }));

      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("graph")).isEqualTo("g");
        assertThat(row.<Integer>getProperty("score")).isEqualTo(3);
        served.add(row.getIdentity().orElseThrow());
      }
    }
    final long loaded = readRecords() - readsBefore;

    assertThat(served).hasSize(200).doesNotHaveDuplicates();
    // 200 rows that pass 1 in 10 take about 2,000 entries, plus the batches pulled ahead: far from the 20,000 of the key
    assertThat(loaded).as("records loaded by a LIMIT 200 that filters 1 in 10 of a key of " + KEY_SIZE).isLessThan(5_000);
    assertThat(step.getKeyRereads()).as("no entry of the key lost its record: the key must not be read again").isZero();
    assertThat(step.getVerifiedRows()).as("the rows served after the commit are checked against their key").isPositive();
  }

  /**
   * Regression test for issue #9902.
   * <p>
   * Records of the key deleted, replaced, or moved to another key while the lookup reads it: no row is served twice, no record
   * is served for a key it no longer holds, every record that holds the key throughout is served, and so is the replacement of
   * a deleted one, at the cost of ONE more read of the key.
   */
  @Test
  void entriesThatLoseTheirRecordAreReconciledWithOneReadOfTheKey() throws Exception {
    createReplayEntityType();
    loseEntriesWhileReading(null);
  }

  /** Regression test for issue #9902. */
  @Test
  void entriesThatLoseTheirRecordAreReconciledInAReadCommittedTransaction() throws Exception {
    createReplayEntityType();
    loseEntriesWhileReading(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
  }

  /** Regression test for issue #9902. */
  @Test
  void entriesThatLoseTheirRecordAreReconciledInARepeatableReadTransaction() throws Exception {
    createReplayEntityType();
    loseEntriesWhileReading(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
  }

  private void loseEntriesWhileReading(final Database.TRANSACTION_ISOLATION_LEVEL isolation) throws Exception {
    final int size = 3_000;
    populate("g", size);
    populate("h", 100);

    final Set<RID> served = new HashSet<>();
    final Set<RID> removed = new HashSet<>();
    final Set<RID> replacements = new HashSet<>();
    final GetValueFromIndexEntryStep step;
    if (isolation != null)
      database.begin(isolation);
    try (final ResultSet rs = database.query("sql", "SELECT FROM Entity WHERE graph = 'g'")) {
      step = findStep(rs.getExecutionPlan().orElseThrow());
      for (int i = 0; i < 5; i++)
        assertThat(served.add(rs.next().getIdentity().orElseThrow())).isTrue();

      inAnotherThread(() -> database.transaction(() -> {
        int deleted = 0, moved = 0, skipped = 0;
        try (final ResultSet all = database.query("sql", "SELECT FROM Entity WHERE graph = 'g'")) {
          while (all.hasNext() && (deleted < 50 || moved < 20)) {
            final RID rid = all.next().getIdentity().orElseThrow();
            // past the rows the lookup may have loaded already for the page of rows it hands out
            if (++skipped <= 500)
              continue;
            if (deleted < 50) {
              rid.asDocument().delete();
              ++deleted;
            } else {
              final MutableDocument doc = rid.asDocument().modify();
              doc.set("graph", "h").save();
              ++moved;
            }
            removed.add(rid);
          }
        }
        // the replacements, which may take the slots of the deleted records
        for (int i = 0; i < 50; i++)
          replacements.add(database.newDocument("Entity").set("graph", "g").set("score", 1).save().getIdentity());
      }));

      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("graph")).as("a row served for a key its record no longer holds").isEqualTo("g");
        assertThat(served.add(row.getIdentity().orElseThrow())).as("a row served twice").isTrue();
      }
    } finally {
      if (isolation != null)
        database.rollback();
    }

    final Set<RID> expected = new HashSet<>();
    try (final ResultSet all = database.query("sql", "SELECT FROM Entity WHERE graph = 'g'")) {
      all.forEachRemaining(r -> expected.add(r.getIdentity().orElseThrow()));
    }
    assertThat(expected).hasSize(size - 20);
    final Set<RID> throughout = new HashSet<>(expected);
    throughout.removeAll(replacements);
    assertThat(served).as("every record that held the key the whole time").containsAll(throughout);

    if (isolation == Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ) {
      // The pages the transaction read before the commit keep their image: a record on one of them is served as the
      // transaction saw it, which is what REPEATABLE_READ promises, and holds the key in that image
      assertThat(step.getKeyRereads()).isLessThanOrEqualTo(1);
      return;
    }

    // the answer is the state after the commit
    assertThat(served).containsAll(replacements);
    for (final RID rid : removed)
      if (!replacements.contains(rid))
        assertThat(served).as("a record removed from the key is not served").doesNotContain(rid);

    assertThat(step.getKeyRereads()).as("one read of the key settles every entry it lost").isEqualTo(1);
    assertThat(step.getRereadEntries()).isLessThanOrEqualTo(size);
  }

  /**
   * Regression test for issue #9902.
   * <p>
   * A writer keeps replacing records of the key - delete and create, in one transaction - while readers stream it, outside and
   * inside transactions: every record the writer never touches is served exactly once, no row is served twice or for a key its
   * record does not hold, and reading the key again stays bounded however many commits land.
   */
  @Test
  @Tag("slow")
  void streamingAKeyUnderSteadyReplacementsKeepsEveryStableRecord() throws Exception {
    createReplayEntityType();
    final int stable = 5_000;
    populate("g", stable);
    final Set<RID> stableRids = new HashSet<>();
    try (final ResultSet all = database.query("sql", "SELECT FROM Entity WHERE graph = 'g'")) {
      all.forEachRemaining(r -> stableRids.add(r.getIdentity().orElseThrow()));
    }
    // the records the writer churns, created after the stable ones
    final List<RID> churn = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 200; i++)
        churn.add(database.newDocument("Entity").set("graph", "g").set("score", -1).save().getIdentity());
    });

    final AtomicBoolean stop = new AtomicBoolean();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicLong reads = new AtomicLong();
    final Thread[] readers = new Thread[3];
    for (int m = 0; m < readers.length; m++) {
      final int mode = m;
      readers[m] = new Thread(() -> {
        try {
          while (!stop.get() && failure.get() == null) {
            if (mode == 1)
              database.begin(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
            else if (mode == 2)
              database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
            try (final ResultSet rs = database.query("sql", "SELECT FROM Entity WHERE graph = 'g'")) {
              final GetValueFromIndexEntryStep step = findStep(rs.getExecutionPlan().orElseThrow());
              final Set<RID> served = new HashSet<>();
              while (rs.hasNext()) {
                final Result row = rs.next();
                if (!"g".equals(row.getProperty("graph")))
                  throw new AssertionError("row " + row.getIdentity() + " served for a key it does not hold");
                if (!served.add(row.getIdentity().orElseThrow()))
                  throw new AssertionError("row " + row.getIdentity() + " served twice");
              }
              if (!served.containsAll(stableRids))
                throw new AssertionError("a record that held the key the whole time was not served (mode " + mode + ")");
              if (step.getKeyRereads() > 8)
                throw new AssertionError("the key was read again " + step.getKeyRereads() + " times");
            } finally {
              if (mode != 0)
                database.rollback();
            }
            reads.incrementAndGet();
          }
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      }, "reader-" + m);
      readers[m].start();
    }

    final long deadline = System.currentTimeMillis() + 6_000;
    int next = 0;
    while (System.currentTimeMillis() < deadline && failure.get() == null) {
      final int index = next++ % churn.size();
      database.transaction(() -> {
        churn.get(index).asDocument().delete();
        churn.set(index, database.newDocument("Entity").set("graph", "g").set("score", -1).save().getIdentity());
      });
    }
    stop.set(true);
    for (final Thread reader : readers)
      reader.join();

    if (failure.get() != null)
      throw new AssertionError("a reader failed", failure.get());
    assertThat(reads.get()).isPositive();
  }

  /**
   * Regression test for issue #9902.
   * <p>
   * An entry whose record is gone for good, with no commit around, is a dangling index entry: it is skipped without reading the
   * key again.
   */
  @Test
  void aKeyThatOverlapsNoCommitIsNeverReadAgain() {
    createReplayEntityType();
    populate("g", 1_000);
    try (final ResultSet rs = database.query("sql", "SELECT FROM Entity WHERE graph = 'g'")) {
      final GetValueFromIndexEntryStep step = findStep(rs.getExecutionPlan().orElseThrow());
      int rows = 0;
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
      assertThat(rows).isEqualTo(1_000);
      assertThat(step.getKeyRereads()).isZero();
      assertThat(step.getVerifiedRows()).isZero();
    }
  }

  /**
   * Regression test for issue #9902.
   * <p>
   * The same check holds for an {@code IN} over several large keys, for a case-insensitive key and for a numeric key looked up
   * with a value of another type: the check compares the keys as the index does, so a row is never lost for that.
   */
  @Test
  void theKeyCheckComparesKeysAsTheIndexDoes() throws Exception {
    createReplayEntityType();
    database.command("sql", "CREATE PROPERTY Entity.code STRING");
    database.command("sql", "CREATE INDEX ON Entity (code COLLATE ci) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Entity (score) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++) {
        database.newDocument("Entity").set("graph", "x").set("code", i % 2 == 0 ? "AbC" : "abc").set("score", 100).save();
        database.newDocument("Entity").set("graph", "y").set("code", "ZZ").set("score", 200).save();
      }
    });

    for (final String query : List.of("SELECT FROM Entity WHERE code = 'abc'", "SELECT FROM Entity WHERE score = 100.0",
        "SELECT FROM Entity WHERE graph IN ['x', 'y']", "SELECT FROM Entity WHERE code IN ['abc', 'zz']")) {
      // the rows the query answers with no commit around: the filter on top of the index compares as the property does
      final Set<RID> expected = new HashSet<>();
      try (final ResultSet rs = database.query("sql", query)) {
        rs.forEachRemaining(r -> expected.add(r.getIdentity().orElseThrow()));
      }
      assertThat(expected).as(query).hasSizeGreaterThanOrEqualTo(500);
      final Set<RID> served = new HashSet<>();
      try (final ResultSet rs = database.query("sql", query)) {
        final GetValueFromIndexEntryStep step = findStep(rs.getExecutionPlan().orElseThrow());
        assertThat(step).as(query).isNotNull();
        served.add(rs.next().getIdentity().orElseThrow());
        inAnotherThread(() -> database.transaction(() -> database.newDocument("Entity").set("graph", "other").save()));
        while (rs.hasNext())
          assertThat(served.add(rs.next().getIdentity().orElseThrow())).as(query + ": a row served twice").isTrue();
        assertThat(served).as(query).isEqualTo(expected);
        assertThat(step.getVerifiedRows()).as(query + ": the rows after the commit are checked").isPositive();
        assertThat(step.getKeyRereads()).as(query + ": every row passes the check, nothing to read again").isZero();
      }
    }
  }

  /**
   * Regression test for issue #9902.
   * <p>
   * A lookup of a prefix of a composite key checks a record on that prefix, and a key an {@code IN} lists twice serves each of its
   * records once, before and after a commit.
   */
  @Test
  void compositePrefixAndRepeatedInKeysAreServedOnce() throws Exception {
    createReplayEntityType();
    database.command("sql", "CREATE DOCUMENT TYPE Pair");
    database.command("sql", "CREATE PROPERTY Pair.a STRING");
    database.command("sql", "CREATE PROPERTY Pair.b INTEGER");
    database.command("sql", "CREATE INDEX ON Pair (a, b) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++) {
        database.newDocument("Pair").set("a", "p").set("b", i).save();
        database.newDocument("Pair").set("a", "q").set("b", i).save();
      }
    });

    for (final String query : List.of("SELECT FROM Pair WHERE a = 'p'", "SELECT FROM Pair WHERE a IN ['p', 'p']",
        "SELECT FROM Pair WHERE a IN ['p', 'q', 'p']")) {
      final int expectedRows = query.contains("'q'") ? 2_000 : 1_000;
      final Set<RID> served = new HashSet<>();
      try (final ResultSet rs = database.query("sql", query)) {
        final GetValueFromIndexEntryStep step = findStep(rs.getExecutionPlan().orElseThrow());
        assertThat(step).as(query).isNotNull();
        served.add(rs.next().getIdentity().orElseThrow());
        inAnotherThread(() -> database.transaction(() -> database.newDocument("Pair").set("a", "other").set("b", -1).save()));
        while (rs.hasNext())
          assertThat(served.add(rs.next().getIdentity().orElseThrow())).as(query + ": a row served twice").isTrue();
        assertThat(served).as(query).hasSize(expectedRows);
        assertThat(step.getVerifiedRows()).as(query).isPositive();
        assertThat(step.getKeyRereads()).as(query).isZero();
      }
    }
  }

  /**
   * Regression test for issue #9902.
   * <p>
   * An {@code IN} over more keys of a unique index than are loaded against one state streams them: a key whose record is replaced
   * while it is read is read again for that one entry, and serves the replacement.
   */
  @Test
  void aUniqueKeyReplacedDuringTheLookupServesItsReplacement() throws Exception {
    createReplayEntityType();
    database.command("sql", "CREATE DOCUMENT TYPE Item");
    database.command("sql", "CREATE PROPERTY Item.code INTEGER");
    database.command("sql", "CREATE INDEX ON Item (code) UNIQUE");
    final int count = 1_000;
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newDocument("Item").set("code", i).save();
    });
    final List<Integer> codes = new ArrayList<>();
    for (int i = 0; i < count; i++)
      codes.add(i);

    final Set<Integer> served = new HashSet<>();
    final GetValueFromIndexEntryStep step;
    try (final ResultSet rs = database.query("sql", "SELECT FROM Item WHERE code IN :codes", Map.of("codes", codes))) {
      step = findStep(rs.getExecutionPlan().orElseThrow());
      assertThat(step).isNotNull();
      served.add(rs.next().getProperty("code"));
      // replace the records of the last 100 codes, which the lookup has not reached yet
      inAnotherThread(() -> database.transaction(() -> {
        for (int code = count - 100; code < count; code++) {
          database.command("sql", "DELETE FROM Item WHERE code = ?", code).close();
          database.newDocument("Item").set("code", code).save();
        }
      }));
      while (rs.hasNext())
        assertThat(served.add(rs.next().getProperty("code"))).as("a code served twice").isTrue();
    }
    assertThat(served).as("every code is held by a record in every committed state").hasSize(count);
    assertThat(step.getKeyRereads()).as("one read again per replaced code").isBetween(1L, 100L);
  }

  /**
   * Regression test for issue #9902.
   * <p>
   * Past the records a key remembers it served, the key is not read again for the entries a commit takes from it, since it could
   * not tell what it served: those are counted as unsettled, and still no row is served twice or for a key it does not hold.
   */
  @Test
  void pastTheTrackedRecordsAKeyIsNotReadAgainAndServesNoRowTwice() throws Exception {
    createReplayEntityType();
    populate("g", 2_000);
    final Set<RID> served = new HashSet<>();
    final Set<RID> deleted = new HashSet<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM Entity WHERE graph = 'g'")) {
      final GetValueFromIndexEntryStep step = findStep(rs.getExecutionPlan().orElseThrow());
      step.trackedRidsPerKey = 10;
      for (int i = 0; i < 20; i++)
        served.add(rs.next().getIdentity().orElseThrow());

      inAnotherThread(() -> database.transaction(() -> {
        int skipped = 0;
        try (final ResultSet all = database.query("sql", "SELECT FROM Entity WHERE graph = 'g'")) {
          while (all.hasNext() && deleted.size() < 30) {
            final RID rid = all.next().getIdentity().orElseThrow();
            if (++skipped > 500) {
              rid.asDocument().delete();
              deleted.add(rid);
            }
          }
        }
        for (int i = 0; i < 30; i++)
          database.newDocument("Entity").set("graph", "g").set("score", 1).save();
      }));

      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("graph")).isEqualTo("g");
        assertThat(served.add(row.getIdentity().orElseThrow())).as("a row served twice").isTrue();
      }
      assertThat(step.getKeyRereads()).as("the key cannot tell what it served, so it is not read again").isZero();
      assertThat(step.getUnsettledEntries()).as("the entries the commit took are counted").isPositive();
    }
    for (final RID rid : deleted)
      assertThat(served).as("a deleted record loaded after the commit is not served").doesNotContain(rid);
    assertThat(served).hasSize(2_000 - deleted.size());
  }

  /**
   * Regression test for issue #9902.
   * <p>
   * A prefix of a composite UNIQUE index is one key with many entries: a read of it again for the entries a commit took must not
   * serve again the rows served before, although the index is unique.
   */
  @Test
  void aPrefixOfACompositeUniqueKeyReadAgainServesNoRowTwice() throws Exception {
    createReplayEntityType();
    database.command("sql", "CREATE DOCUMENT TYPE Pair");
    database.command("sql", "CREATE PROPERTY Pair.a STRING");
    database.command("sql", "CREATE PROPERTY Pair.b INTEGER");
    database.command("sql", "CREATE INDEX ON Pair (a, b) UNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newDocument("Pair").set("a", "p").set("b", i).save();
    });

    final Set<RID> served = new HashSet<>();
    final Set<RID> replacements = new HashSet<>();
    final GetValueFromIndexEntryStep step;
    try (final ResultSet rs = database.query("sql", "SELECT FROM Pair WHERE a = 'p'")) {
      step = findStep(rs.getExecutionPlan().orElseThrow());
      assertThat(step).isNotNull();
      for (int i = 0; i < 20; i++)
        served.add(rs.next().getIdentity().orElseThrow());

      inAnotherThread(() -> database.transaction(() -> {
        int skipped = 0, deleted = 0;
        try (final ResultSet all = database.query("sql", "SELECT FROM Pair WHERE a = 'p'")) {
          while (all.hasNext() && deleted < 20) {
            final Result row = all.next();
            if (++skipped <= 500)
              continue;
            row.getIdentity().orElseThrow().asDocument().delete();
            ++deleted;
          }
        }
        for (int i = 0; i < 20; i++)
          replacements.add(database.newDocument("Pair").set("a", "p").set("b", 10_000 + i).save().getIdentity());
      }));

      while (rs.hasNext())
        assertThat(served.add(rs.next().getIdentity().orElseThrow())).as("a row served twice").isTrue();
    }
    assertThat(served).containsAll(replacements).hasSize(1_000);
    assertThat(step.getKeyRereads()).isEqualTo(1);
  }

  /**
   * Regression test for issue #9902.
   * <p>
   * Issue #9893: reading a key again stops on the command deadline instead of logging it as a failed read and going on.
   */
  @Test
  void readingAKeyAgainHonoursTheCommandDeadline() {
    createReplayEntityType();
    populate("g", 300);
    try (final ResultSet rs = database.query("sql", "SELECT FROM Entity WHERE graph = 'g'")) {
      FetchFromIndexStep indexStep = null;
      for (final ExecutionStep s : rs.getExecutionPlan().orElseThrow().getSteps())
        if (s instanceof FetchFromIndexStep found)
          indexStep = found;
      assertThat(indexStep).isNotNull();
      rs.next();

      final FetchFromIndexStep step = indexStep;
      assertThat(step.lookupKeyAgain(new Object[] { "g" }, WorkGuard.forCommandDeadline(step.context))).hasSize(300);

      step.context.setCommandDeadline(System.currentTimeMillis() - 1_000, "test deadline");
      assertThatThrownBy(() -> step.lookupKeyAgain(new Object[] { "g" }, WorkGuard.forCommandDeadline(step.context)))
          .isInstanceOf(TimeoutException.class);
    }
  }

  private void populate(final String graph, final int count) {
    database.transaction(() -> {
      for (int i = 0; i < count; i++)
        database.newDocument("Entity").set("graph", graph).set("score", i % 10).save();
    });
  }

  private long readRecords() {
    return ((Number) database.getStats().get("readRecord")).longValue();
  }

  private static void inAnotherThread(final Runnable task) throws Exception {
    final Throwable[] failure = new Throwable[1];
    final Thread thread = new Thread(() -> {
      try {
        task.run();
      } catch (final Throwable e) {
        failure[0] = e;
      }
    });
    thread.start();
    thread.join();
    if (failure[0] != null)
      throw new AssertionError("the concurrent commit failed", failure[0]);
  }

  private static GetValueFromIndexEntryStep findStep(final ExecutionPlan plan) {
    for (final ExecutionStep step : plan.getSteps())
      if (step instanceof GetValueFromIndexEntryStep found)
        return found;
    return null;
  }
}
