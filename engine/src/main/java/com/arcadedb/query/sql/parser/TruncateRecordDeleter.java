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
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.log.LogManager;

import java.util.function.Predicate;
import java.util.logging.Level;

/**
 * The record-deletion loop shared by {@code TRUNCATE TYPE} and {@code TRUNCATE BUCKET}, together with the rule that
 * decides who owns the transaction it runs in.
 * <p>
 * <b>Who owns the transaction.</b> A {@code TRUNCATE} that finds a transaction already active is one operation inside
 * somebody else's unit of work, so it must neither commit nor change the schema: both would commit through the caller
 * and a later {@code ROLLBACK} would put back only whatever the last, uncommitted batch held (issue #6220). It
 * therefore deletes every record inside the caller's transaction, leaving index maintenance to the per-record path
 * {@code DELETE FROM} already uses. A {@code TRUNCATE} that finds no transaction owns one: it opens it here, is free
 * to split the deletion into small committed batches, and - for {@code TRUNCATE TYPE} - to drop and rebuild the
 * indexes around them.
 * <p>
 * The batching is a throughput and HA concern, never an atomicity one: in HA each committed transaction becomes one
 * Raft log entry, so a small batch keeps that entry small and lets the leader's per-follower append pipeline emit
 * heartbeats between batches instead of stalling on a single multi-MB entry (issue #4817). That is exactly why it is
 * safe to give it up when the caller owns the transaction - the caller's own commit is one entry either way.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class TruncateRecordDeleter {
  /**
   * The scan a {@code TRUNCATE} runs to reach the records it must delete: {@code scanType} for a type,
   * {@code scanBucket} for a bucket. The callback returns {@code false} to stop the scan.
   */
  @FunctionalInterface
  interface RecordScan {
    void run(Predicate<Record> callback);
  }

  /**
   * Work a statement needs to do inside its own transaction after the last record is deleted and before that
   * transaction is closed - for {@code TRUNCATE TYPE}, rebuilding the indexes it dropped.
   * <p>
   * It is handed the failure so far ({@code null} when the deletion succeeded) and returns the failure to carry on
   * with, so a hook can both react to one and report one of its own. Throwing works too and is treated identically;
   * returning is only the tidier option for a hook that has a partial result worth keeping.
   */
  @FunctionalInterface
  interface AfterDeletion {
    RuntimeException run(RuntimeException deletionFailure);
  }

  private TruncateRecordDeleter() {
  }

  /**
   * Deletes every record the scan yields.
   *
   * @param batchSize when positive, commit the current transaction and begin a new one every {@code batchSize}
   *                  records. Only ever passed by a {@code TRUNCATE} that owns its transaction; {@code 0} keeps every
   *                  delete in the caller's transaction so a {@code ROLLBACK} undoes all of them.
   *
   * @return the failure that stopped the scan, or {@code null} when every record was deleted.
   */
  static RuntimeException deleteAll(final Database db, final int batchSize, final RecordScan scan) {
    // LocalBucket.scan wraps every callback in a try/catch that logs any exception as a per-record "Error on loading
    // record" and keeps scanning - so a failure here (an HA leader change interrupting a mid-scan commit, a
    // BeforeRecordDelete listener refusing) used to be silently turned into a partial truncate reported as success,
    // with the dropped indexes then rebuilt over the records that were never deleted (issue #4817). Catch it here,
    // before that swallowing catch sees it, stop the scan and hand it back to the caller to surface.
    final RuntimeException[] failure = { null };
    final long[] count = { 0 };
    scan.run(record -> {
      try {
        record.delete();
        if (batchSize > 0 && ++count[0] % batchSize == 0 && db.isTransactionActive()) {
          db.commit();
          db.begin();
        }
        return true;
      } catch (final RuntimeException e) {
        failure[0] = e;
        return false; // stop the scan; the failure is surfaced by the caller
      }
    });
    return failure[0];
  }

  /**
   * Runs the deletion in a transaction this statement opens and closes itself, which is the only place batching and
   * schema changes are allowed (see the class javadoc). Written once because both statements need exactly this
   * bookkeeping and only one of them has an {@code afterDeletion} hook: duplicating it left the two copies free to
   * drift, and a metric or a new failure mode added to one would silently miss the other.
   *
   * @param afterDeletion work to run inside the transaction before it is closed, or {@code null} for none.
   * @param what          how to name this statement in the last-resort rollback warning.
   *
   * @return the failure to report, or {@code null} when everything succeeded.
   */
  static RuntimeException deleteAllInOwnTransaction(final Database db, final int batchSize, final RecordScan scan,
      final AfterDeletion afterDeletion, final String what) {
    // Without this every delete below would fail with "Transaction not begun" unless the database happens to run
    // with autoTransaction on, which is what made this whole path unreachable before issue #6220 - the batching and
    // the index drop/rebuild only ever ran on a caller's transaction, which is precisely where they must not.
    db.begin();

    RuntimeException failure = null;
    try {
      try {
        failure = deleteAll(db, batchSize, scan);
      } catch (final RuntimeException e) {
        // Not a duplicate of deleteAll's own handling: that one catches what the per-record callback raises and
        // hands it back as a value, this one catches what the SCAN raises around the callback - resolving buckets,
        // reading a page - which never reaches the callback at all. Both have to end at the same place, because the
        // afterDeletion hook runs either way.
        failure = e;
      }

      try {
        if (afterDeletion != null)
          failure = afterDeletion.run(failure);
      } catch (final RuntimeException e) {
        // A hook that throws goes through the same bookkeeping as everything else rather than skipping the
        // close-out below and landing in the finally: its transaction gets a decision, not just a safety net.
        if (failure == null)
          failure = e;
        else
          failure.addSuppressed(e);
      }

      if (db.isTransactionActive()) {
        try {
          if (failure == null)
            db.commit();
          else
            db.rollback();
        } catch (final RuntimeException e) {
          // A commit that fails IS the failure to report. A rollback that fails while a failure is already on its
          // way out must not replace it - the original is the one that says what went wrong.
          if (failure == null)
            failure = e;
          else
            failure.addSuppressed(e);
        }
      }
    } finally {
      // Whatever escaped the block above, this statement's own transaction must not be left on the caller's thread
      // for somebody else's next command to inherit. Reported rather than thrown, for the same reason as above.
      if (db.isTransactionActive())
        try {
          db.rollback();
        } catch (final RuntimeException e) {
          LogManager.instance().log(TruncateRecordDeleter.class, Level.WARNING,
              "Error on rolling back the transaction opened by TRUNCATE %s", e, what);
        }
    }
    return failure;
  }
}
