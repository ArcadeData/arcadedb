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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression coverage for #6041 (follow-up from #6031/#6040): {@code DatabaseChecker}'s auto-fix index-rebuild loop
 * gained a ~50-line retry/backoff/per-index-isolation block in #6031, and #6040 then reworked it to distinguish a
 * lock-acquisition failure (nothing touched) from a failure after that attempt's {@code dropIndex()} already
 * committed (extended retry budget, since giving up there leaves the index missing). Neither shape had ANY test
 * coverage before this class - {@code CheckDatabaseFixPreservesIndexMetadataTest} only exercises the
 * uncontended happy path.
 *
 * <p>Contention is induced deterministically by holding the target bucket sub-index's file lock from a second
 * thread via {@link TestHelper.LockHoldingThread}, which wraps the same {@code DatabaseInternal.executeLockingFiles}
 * primitive {@code DatabaseChecker} itself uses - exactly the mechanism suggested in the issue.
 * {@code LocalDatabase.executeLockingFiles} waits a hardcoded 5 seconds per attempt before raising a
 * {@code LockTimeoutException} (a {@code NeedRetryException}), which is what makes both tests here multi-second:
 * {@code @Tag("slow")} routes them to the slow lane rather than the regular unit-test one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class DatabaseCheckerIndexRebuildLockContentionTest extends TestHelper {

  /**
   * Holds the lock comfortably past the single hardcoded 5s {@code tryLockFiles} timeout (1s margin over it,
   * against scheduling jitter on a loaded CI runner - see {@code engine/CLAUDE.md} on that timeout being "a common
   * source of intermittent failures in contention tests"), forcing exactly one {@code LockTimeoutException} on
   * FIX's first attempt. The release still lands far short of attempt 2's own 5s window (which starts after a
   * ~400ms backoff), so attempt 2 acquires quickly and completes the rebuild - the retry-then-succeed branch #6041
   * asks for.
   */
  @Test
  void fixRetriesThroughTransientLockContentionAndSucceeds() throws Exception {
    final AtomicReference<RID> victim = new AtomicReference<>();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      // Index created BEFORE the inserts: CREATE INDEX's bulk-scan build only sees already-committed data, so
      // creating it after inserting in the SAME transaction would miss records inserted earlier in that same,
      // still-open transaction. Once the index exists, ordinary inserts maintain it incrementally as they happen.
      database.command("sql", "CREATE INDEX myIdx ON Doc (name) NOTUNIQUE");
      final Result inserted = database.command("sql", "INSERT INTO Doc SET name = 'alpha'").next();
      // Not corrupted: survives the fix (which deletes the unrecoverable 'alpha' record), used below to prove the
      // rebuilt index is actually queryable afterwards rather than merely present.
      database.command("sql", "INSERT INTO Doc SET name = 'beta'");
      victim.set(inserted.toElement().getIdentity());
    });

    final DatabaseInternal db = (DatabaseInternal) database;
    corruptRecordTypeByte(db, victim.get());

    final List<Integer> fileIds = bucketSubIndexFileIds(db);

    // Taken when the rebuild step BEGINS, not before the check: the passes before it (the index content comparison waits for
    // the same lock, #11734) would otherwise spend the 6 s window and let the first rebuild attempt through, never reaching
    // the retry. Held 6 s from there, past the 5 s lock timeout of the first attempt and short of the second one
    final LockHoldingThread holder = new LockHoldingThread(db, fileIds, 6_000);
    final boolean[] heldAtRebuild = new boolean[1];
    try {
      final Map<String, Object> row = new DatabaseChecker(database).setFix(true)
          .setProgressCallback((stepName, stepIndex, totalSteps, done, total) -> {
            if ("Rebuilding indexes".equals(stepName) && !heldAtRebuild[0]) {
              heldAtRebuild[0] = true;
              holder.start();
              try {
                assertThat(holder.lockAcquired.await(5, TimeUnit.SECONDS))
                    .as("background thread must acquire the lock before the rebuild runs").isTrue();
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
            }
          }).check();
      assertThat(heldAtRebuild[0]).as("the rebuild step must have run under the held lock").isTrue();
      assertThat((Long) row.get("autoFix")).isGreaterThan(0L);
      // rebuiltIndexes names the BUCKET sub-index DatabaseChecker actually rebuilds (e.g. "Doc_0_<nanoTime>"),
      // not the logical TypeIndex wrapper "myIdx" - it must be non-empty, i.e. the rebuild was recorded as done.
      @SuppressWarnings("unchecked")
      final Set<String> rebuiltIndexes = (Set<String>) row.get("rebuiltIndexes");
      assertThat(rebuiltIndexes).isNotEmpty();
    } finally {
      if (heldAtRebuild[0])
        holder.join(15_000);
    }
    assertThat(holder.isAlive()).isFalse();
    assertThat(holder.error.get()).isNull();

    database.transaction(() -> {
      assertThat(database.getSchema().existsIndex("myIdx")).isTrue();
      final String planString = plan("EXPLAIN SELECT FROM Doc WHERE name = 'beta'");
      assertThat(planString).contains("FETCH FROM INDEX");
      final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE name = 'beta'");
      assertThat(rs.hasNext()).isTrue();
    });
  }

  /**
   * Holds the lock past the worst-case total of all 5 attempts (5 * 5s wait + backoff between them), so every
   * attempt is a pure lock-acquisition failure - the retry-exhaustion branch #6041 asks for. Because none of the 5
   * attempts ever got past {@code tryLockFiles}, {@code dropIndex()} never ran on any of them: per #6040 this is
   * exactly the case where exhaustion must leave the index exactly as it was, not missing.
   */
  @Test
  void fixExhaustsRetryOnSustainedLockContentionAndLeavesIndexUntouched() throws Exception {
    final AtomicReference<RID> victim = new AtomicReference<>();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      // See fixRetriesThroughTransientLockContentionAndSucceeds for why the index must be created before inserting.
      database.command("sql", "CREATE INDEX myIdx ON Doc (name) NOTUNIQUE");
      final Result inserted = database.command("sql", "INSERT INTO Doc SET name = 'alpha'").next();
      // Not corrupted: used below to prove the untouched index is still queryable after exhaustion.
      database.command("sql", "INSERT INTO Doc SET name = 'beta'");
      victim.set(inserted.toElement().getIdentity());
    });

    final DatabaseInternal db = (DatabaseInternal) database;
    corruptRecordTypeByte(db, victim.get());

    final List<Integer> fileIds = bucketSubIndexFileIds(db);

    // Worst case for 5 attempts: 5 * 5000ms wait + (400+600+800+1000)ms backoff = 27800ms, on top of whatever the passes before
    // the rebuild spend (the index content comparison waits for the same lock, #11734). Held until FIX returns, with a ceiling
    final LockHoldingThread holder = new LockHoldingThread(db, fileIds, 120_000);
    holder.start();
    try {
      assertThat(holder.lockAcquired.await(5, TimeUnit.SECONDS))
          .as("background thread must acquire the lock before FIX runs").isTrue();

      final ResultSet result = database.command("sql", "check database fix");
      assertThat(result.hasNext()).isTrue();
      final Result row = result.next();

      // rebuiltIndexes/warnings name the BUCKET sub-index DatabaseChecker actually rebuilds (e.g.
      // "Doc_0_<nanoTime>"), not the logical TypeIndex wrapper "myIdx": it must report NO rebuild happened, with a
      // matching warning explaining why.
      @SuppressWarnings("unchecked")
      final Set<String> rebuiltIndexes = (Set<String>) row.getProperty("rebuiltIndexes");
      assertThat(rebuiltIndexes).isEmpty();

      @SuppressWarnings("unchecked")
      final Set<String> warnings = (Set<String>) row.getProperty("warnings");
      assertThat(warnings).anyMatch(w -> w.contains("did not finish rebuilding")
          && w.contains("the index lock could not be acquired; the index itself is unchanged"));
    } finally {
      holder.release();
      holder.join(35_000);
    }
    assertThat(holder.isAlive()).isFalse();
    assertThat(holder.error.get()).isNull();

    // #6040: a pure lock-acquisition exhaustion must never have touched the index - it is still there, unchanged,
    // and still answers queries through it (not merely present-but-broken).
    database.transaction(() -> {
      assertThat(database.getSchema().existsIndex("myIdx")).isTrue();
      final String planString = plan("EXPLAIN SELECT FROM Doc WHERE name = 'beta'");
      assertThat(planString).contains("FETCH FROM INDEX");
      final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE name = 'beta'");
      assertThat(rs.hasNext()).isTrue();
    });

    // Untouched, it still holds the entry of the corrupted record FIX removed: the next check names it, and the next FIX, with
    // the lock free, rebuilds the index without it (#11734)
    try (final ResultSet rs = database.command("sql", "check database")) {
      assertThat(rs.next().<Long>getProperty("danglingIndexEntries")).isEqualTo(1L);
    }
    try (final ResultSet rs = database.command("sql", "check database fix")) {
      assertThat(rs.next().<Set<String>>getProperty("rebuiltIndexes")).isNotEmpty();
    }
  }

  private String plan(final String query) {
    return database.query("sql", query).getExecutionPlan().get().prettyPrint(0, 3);
  }

  private static List<Integer> bucketSubIndexFileIds(final DatabaseInternal db) {
    final TypeIndex typeIdx = (TypeIndex) db.getSchema().getIndexByName("myIdx");
    final List<IndexInternal> subIndexes = typeIdx.getSubIndexes();
    assertThat(subIndexes).as("single-bucket type: exactly one bucket sub-index").hasSize(1);
    return subIndexes.get(0).getFileIds();
  }
}
