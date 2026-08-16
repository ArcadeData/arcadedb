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
package com.arcadedb.schema;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

public class MaterializedViewRefresher {

  public static void fullRefresh(final Database database, final MaterializedViewImpl view) {
    if (!view.tryBeginRefresh()) {
      // A refresh is already running. Hand it our request so it makes a further pass over the
      // now-newer source data: simply returning here left the view reflecting a snapshot taken
      // before this caller's commit, with nothing scheduled to ever correct it.
      if (view.markRefreshPendingIfRunning()) {
        LogManager.instance().log(MaterializedViewRefresher.class, Level.FINE,
            "Refresh for materialized view '%s' already in progress, requested a further pass", null, view.getName());
        return;
      }
      // It finished between the two calls, so there is nobody left to service the request: run it here.
      if (!view.tryBeginRefresh())
        return;
    }

    try {
      do {
        view.setStatus(MaterializedViewStatus.BUILDING);
        final long startNs = System.nanoTime();

        refreshOnce(database, view);

        view.recordRefreshSuccess((System.nanoTime() - startNs) / 1_000_000);
        view.updateLastRefreshTime();
        view.setStatus(MaterializedViewStatus.VALID);
      } while (view.finishRefreshPassAndCheckPending());

    } catch (final Throwable e) {
      // Throwable, not Exception: refresh ownership is held from the tryBeginRefresh() above, and the only
      // places that give it back are the two releases in this method. Letting an Error past them latched the
      // view in RUNNING for the life of the database - every later refresh, periodic or manual or incremental,
      // then found the state machine busy and either coalesced onto a pass that had already died or returned
      // having done nothing, so the view silently froze at its last successful snapshot. The pass is still
      // reported as failed and the Error still propagates to the caller; what changes is that the next one can run.
      view.recordRefreshError();
      view.setStatus(MaterializedViewStatus.ERROR);
      // Ownership is released here rather than in a finally: the success path already released it,
      // atomically with the check for a pending request. This release is a CAS too, so a request
      // registered during the failing pass is discarded deliberately rather than clobbered - the
      // status above leaves that staleness visible instead of silent.
      final boolean discardedPendingRequest = view.releaseRefreshAfterFailure();
      LogManager.instance().log(MaterializedViewRefresher.class, Level.SEVERE,
          "Error refreshing materialized view '%s': %s", e, view.getName(), e.getMessage());
      if (discardedPendingRequest)
        // Deliberately does not name the status: callers layer their own on top of the ERROR set
        // above (MaterializedViewChangeListener overwrites it with STALE), so naming one here would
        // report a state the operator never sees.
        LogManager.instance().log(MaterializedViewRefresher.class, Level.WARNING,
            "A refresh requested during the failed refresh of materialized view '%s' was not run; the view is left non-VALID and stale",
            null, view.getName());
      throw e;
    }
  }

  /**
   * Replaces the backing type's contents with the defining query's rows, as ONE transaction.
   * <p>
   * The {@code TRUNCATE TYPE ... UNSAFE} this used to start with is gone, and that is the whole point (issue #6203).
   * {@code TRUNCATE} committed the caller's transaction from the inside - {@code schema.dropIndex()} for every index
   * on the backing type before a single record was touched, then {@code commit(); begin()} once per
   * {@code arcadedb.truncateBatchSize} records, then once more before it rebuilt the indexes it dropped - so the
   * refresh was a sequence of committed transactions wearing the costume of one. (The statement itself was fixed
   * afterwards, in issue #6220: it now commits nothing when it runs inside a caller's transaction. The refresh does
   * not go back to it, because writing the new rows over the old ones is a better fit for an indexed view than a
   * clear-and-repopulate - see the last paragraph.) Both consequences were silent:
   * <ul>
   * <li>every other reader saw the view empty, or holding some prefix of the new rows, for the whole runtime of the
   * defining query - which for a view worth materialising is exactly the expensive case, and for a PERIODIC view is
   * every tick;</li>
   * <li>a defining query that threw after the truncate had committed left the backing type with zero rows. The view
   * then reported ERROR or STALE, and both of those read as "the data you are looking at is old" when the data was in
   * fact gone - until some later refresh happened to succeed, which for a MANUAL view may be never.</li>
   * </ul>
   * As one transaction the refresh gets the isolation every other ArcadeDB transaction has: every write stays in the
   * transaction's own page buffer until commit, so a concurrent reader sees the previous snapshot throughout and the
   * new one afterwards, and a failure anywhere rolls the whole pass back onto the previous snapshot rather than onto
   * nothing.
   * <p>
   * The rejected alternative was to build into a shadow backing type and swap the two at the end. It reads better on
   * paper - the swap is metadata only and the old rows are dropped as files rather than deleted one by one - but in
   * this engine the swap has no cheap implementation. Renaming a type renames its buckets, and
   * {@code PaginatedComponent.rename()} first waits for every page of the whole database to be flushed: paying a
   * database-wide flush barrier twice per refresh, on every tick of every PERIODIC view, is a worse defect than the
   * one being fixed. Repointing the view at a differently named backing type instead of renaming avoids the barrier
   * but makes the view's own name stop being the type name, which is what makes {@code SELECT FROM <view>} work at
   * all and what every record's {@code @type} reports.
   * <p>
   * The new rows are written OVER the previous snapshot's records rather than into fresh ones, and only the surplus
   * is deleted (or the shortfall created). That is not a micro-optimisation, it is what keeps an index on the backing
   * type from paying for the atomicity. Clearing the view with a delete and repopulating with new records would give
   * every row a new RID, and {@code TransactionIndexContext} collapses a REMOVE followed by an ADD only on the same
   * (key, RID) - so each pass would leave a full set of real tombstones in the index, where the truncate used to drop
   * the index and rebuild it empty. Measured over 120 refreshes of a 2000-row indexed view: delete-and-recreate grew
   * the index from 256KB to 3.9MB and climbing, held down only by compaction it was itself provoking, against a flat
   * 256KB for the truncate. Rewriting in place instead means {@code DocumentIndexer.updateDocument} finds the key
   * values unchanged for every row a stable view reproduces, and skips the index entirely: zero index writes per
   * pass, which is better than either.
   * <p>
   * The RIDs are collected up front rather than iterated live. An update whose content no longer fits its slot can
   * relocate the record, and a scan that is still walking the bucket would then meet it a second time at its new
   * home - rewriting a row already written and losing count of what is surplus.
   * <p>
   * What this costs relative to the truncate: the transaction holds the pages of the rows it rewrites, and under HA
   * the pass is one Raft log entry rather than one per truncate batch plus one for the repopulate. Neither is a new
   * ceiling - the repopulate was already a single unbounded transaction, so a view too large to refresh in one
   * transaction was already too large to refresh.
   */
  private static void refreshOnce(final Database database, final MaterializedViewImpl view) {
    final String backingTypeName = view.getBackingTypeName();

    // Use joinCurrentTx=false to always create a dedicated transaction for the refresh.
    // This ensures changes are committed immediately even when called from an async context
    // (e.g. HTTP API with awaitResponse=false), where a long-running batched transaction
    // would otherwise defer the commit indefinitely.
    // See: https://github.com/ArcadeData/arcadedb/issues/3941
    database.transaction(() -> {
      final RecordIdentities previousSnapshot = collectIdentities(database, backingTypeName);
      int reused = 0;

      // Execute the defining query and write its rows over the previous snapshot's records
      try (final ResultSet rs = database.query("sql", view.getQuery())) {
        while (rs.hasNext()) {
          final Result result = rs.next();
          final MutableDocument doc = reused < previousSnapshot.size ?
              ((Document) database.lookupByRID(previousSnapshot.get(reused++), true)).modify() :
              database.newDocument(backingTypeName);
          applyRow(doc, result);
          doc.save();
        }
      }

      // Whatever the new snapshot did not need
      for (int i = reused; i < previousSnapshot.size; i++)
        database.lookupByRID(previousSnapshot.get(i), true).delete();
    }, false);
  }

  /** Copies one query row onto a document, dropping any property the row does not carry. */
  private static void applyRow(final MutableDocument doc, final Result row) {
    // A reused document still holds the columns of the row it used to carry, and whatever this row does not
    // reproduce has to go. Snapshot the names first: getPropertyNames() is a live view of the document, so both
    // the writes below and the removals after would otherwise mutate what is being iterated. A newly created
    // document has none, and skipping the copy is what keeps the create path allocating exactly as it did.
    final Set<String> previous = doc.getPropertyNames();
    final List<String> carriedOver = previous.isEmpty() ? null : new ArrayList<>(previous);

    for (final String prop : row.getPropertyNames())
      if (!prop.startsWith("@"))
        doc.set(prop, row.getProperty(prop));

    if (carriedOver != null)
      for (final String prop : carriedOver)
        if (!row.hasProperty(prop))
          doc.remove(prop);
  }

  /**
   * The identities currently holding the view, in scan order. {@code countType} is a cached counter rather than a
   * scan and is used here only as a capacity hint - the arrays are filled by the scan, so a stale count costs a
   * resize and nothing else - which spares the common case, a view whose row count barely moves between refreshes,
   * from growing them at all.
   */
  private static RecordIdentities collectIdentities(final Database database, final String backingTypeName) {
    final long count = database.countType(backingTypeName, false);
    final RecordIdentities identities = new RecordIdentities(
        count > 0 && count < Integer.MAX_VALUE ? (int) count : 16);
    database.scanType(backingTypeName, false, record -> {
      identities.add(record.getIdentity());
      return true;
    });
    return identities;
  }

  /**
   * The previous snapshot's record identities, held as the two primitives a {@link RID} is made of rather than as a
   * {@code List<RID>}.
   * <p>
   * This is allocated on every refresh of every view and is proportional to the view's size, and a boxed RID per row
   * costs several times what the pair of primitives does - a reference plus an object header plus the same two
   * fields, against 12 bytes. Identities are rebuilt one at a time as the rewrite consumes them, so at most one is
   * ever live, and each is handed straight to {@code lookupByRID} rather than to {@code RID.asDocument()}: the
   * latter resolves the database from the thread's {@link com.arcadedb.database.DatabaseContext}, which this refresh
   * has no reason to depend on.
   */
  private static final class RecordIdentities {
    private int[] bucketIds;
    private long[] positions;
    private int   size;

    private RecordIdentities(final int initialCapacity) {
      this.bucketIds = new int[initialCapacity];
      this.positions = new long[initialCapacity];
    }

    private void add(final RID rid) {
      if (size == bucketIds.length) {
        final int grown = size + (size >> 1) + 1;
        bucketIds = Arrays.copyOf(bucketIds, grown);
        positions = Arrays.copyOf(positions, grown);
      }
      bucketIds[size] = rid.getBucketId();
      positions[size] = rid.getPosition();
      ++size;
    }

    private RID get(final int index) {
      return new RID(bucketIds[index], positions[index]);
    }
  }
}
