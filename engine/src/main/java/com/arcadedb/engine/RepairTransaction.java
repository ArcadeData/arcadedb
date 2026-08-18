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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.DatabaseInternal;

/**
 * The transaction ONE repair pass of a database check owns, and the batch commits that bound what it may spend.
 * <p>
 * Shared by every repair pass of the engine - {@code LocalBucket.check(fix)} (issue #6320) and the three passes of
 * {@code GraphDatabaseChecker}: the vertex arm, the edge arm and the orphaned-edge-segment reclaim (issue #6342) -
 * because the rule below is the thing that must not drift apart between them, and it had already been got wrong once
 * in each direction.
 * <p>
 * <b>The budget.</b> {@code arcadedb.checkDatabaseRepairBatchPages} is how many dirtied pages one transaction of a
 * pass may accumulate before committing and opening the next (issue #6128). PAGES, not repairs: the WAL entry carries
 * page images, and how many records a repair touched says nothing about how many distinct pages it dirtied.
 * {@code TransactionContext.getModifiedPages()} counts modified and new pages, which is what the entry will hold - and
 * under HA what a Raft entry must stay below.
 * <p>
 * A SOFT ceiling, checked BETWEEN units of repair work and never inside one: a transaction can exceed it by whatever
 * the unit in flight dirties. It never interrupts a repair half-way, which is the property that makes a partial run
 * safe - every record is either repaired or untouched. Set the configuration to 0 to get the single all-or-nothing
 * transaction back, memory cost included.
 * <p>
 * <b>Why ownership is a field and not {@code database.isTransactionActive()}</b>, which is the subtle half: that
 * question asks whether ANY transaction is on the thread, and the answer is yes for the caller's own whenever a check
 * runs nested inside one - which through HTTP is every production run. A batch commit that throws has already disposed
 * its context ({@code LocalDatabase.commit} pops it in a {@code finally} whether or not the write succeeded), so from
 * that moment the pass owns nothing and what is on the thread belongs to the caller. Cleaning "the" transaction up on
 * that evidence would roll back work the pass never made - the other buckets a {@code CHECK DATABASE FIX} already
 * repaired into the same command transaction, or anything else the caller was holding (PR review on #6320). Tracked
 * explicitly instead, so {@link #finish} can only ever touch a transaction this pass opened and still holds.
 * <p>
 * <b>And why {@link #finish} exists at all</b> (issue #6342): a pass used to commit as the LAST statement of its
 * {@code try}, with a {@code finally} that filled in the returned counters and nothing else. So every way the body
 * could throw except the batch commit - a scan that fails, an unreadable page, an {@code IllegalStateException} out of
 * a repair - left the pass's own transaction OPEN on the thread. Nested under the HTTP handler that is worse than a
 * leak: the handler's own {@code rollback()}, cleaning up after the very exception that caused this, pops the pass's
 * nested transaction and leaves the handler's, which is the opposite of what it intended.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RepairTransaction {
  private final DatabaseInternal database;
  private final int              batchPages;
  private       boolean          owned;

  /**
   * @param batchPages modified-page budget of one batch, {@code <= 0} for a single all-or-nothing transaction.
   */
  public RepairTransaction(final DatabaseInternal database, final int batchPages) {
    this.database = database;
    this.batchPages = batchPages;
  }

  /**
   * The budget a pass of THIS database runs under: read once per pass rather than per repaired record, since it is a
   * database-scoped setting and cannot change under a running check.
   */
  public static int configuredBatchPages(final DatabaseInternal database) {
    return database.getConfiguration().getValueAsInteger(GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES);
  }

  /** Opens the transaction the repairs are made in, nested inside whatever the caller has open. */
  public void begin() {
    database.begin();
    owned = true;
  }

  /**
   * Commits the batch in flight and opens the next one, once it has dirtied as many pages as the budget allows.
   * <p>
   * Only ever legal BETWEEN units of repair work, never inside a {@code scanType}/{@code scan} callback:
   * {@code LocalDatabase.scanType} holds the database read lock and owns an implicit transaction for the length of the
   * scan, so committing under it would commit a transaction the scan believes it still owns.
   * <p>
   * WHY THIS WORKS UNDER AN OUTER TRANSACTION, which is the linchpin and not obvious: {@code CHECK DATABASE} runs
   * through the HTTP handler, which wraps the command in its own transaction, so these are NESTED begin/commit pairs.
   * They are not savepoints. {@code DatabaseContext.DatabaseContextTL.pushTransaction} gives each one a genuinely
   * separate {@code TransactionContext}, and {@code commit()} runs the full {@code commit1stPhase}/{@code
   * commit2ndPhase} on THAT context - a real WAL write and, under HA, a real replication round trip. If nesting
   * deferred the write to the outermost commit instead, the whole batching would be inert: the repair would still
   * reach Raft as one entry.
   * <p>
   * FAILURE PATH: a commit that throws propagates to the caller and leaves no transaction open on the thread -
   * {@code commit()} disposes its context even when the write fails - which is exactly why ownership is dropped BEFORE
   * the commit and taken back after it. Between those two lines this pass owns nothing, which is the state
   * {@link #finish} must not act on. Batches already committed stay committed.
   */
  public void commitBatchIfFull() {
    if (!owned || batchPages <= 0)
      return;
    if (database.getTransaction().getModifiedPages() < batchPages)
      return;

    owned = false;
    database.commit();
    database.begin();
    owned = true;
  }

  /**
   * Ends the transaction this pass opened, if it still holds one. A repair left half-applied is never committed: the
   * batches before it stay, which is the semantics #6128 gave the graph repairs, and the batch in flight goes back.
   *
   * @param completed whether the pass reached its end without throwing.
   */
  public void finish(final boolean completed) {
    if (!owned)
      return;

    owned = false;
    if (completed)
      database.commit();
    else
      database.rollback();
  }
}
