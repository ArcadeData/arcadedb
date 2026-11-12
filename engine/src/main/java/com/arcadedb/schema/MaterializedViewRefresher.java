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
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

public class MaterializedViewRefresher {

  public static void fullRefresh(final Database database, final MaterializedViewImpl view) {
    // A per-record view passes the request through its pending delta (issue #11475): the pass that drains it, this caller's or
    // the running owner's next one, then rewrites the whole view instead of only the records changed meanwhile
    if (view.isPerRecord())
      view.requestFullPass(false);
    refresh(database, view, false);
  }

  /**
   * The refresh an INCREMENTAL view owes the commit that changed one of its sources. It runs on the committing thread, so
   * that thread must not be kept for longer than the one pass it owes (issue #11464): when other writers asked for a further
   * pass meanwhile, the owner hands that pass to the schema's scheduler thread instead of making it itself. Without the
   * hand-off the owner made pass after pass for as long as the others kept committing, which under a steady write load from
   * several clients was until they stopped, and its own client had long timed out waiting for the commit to return.
   */
  static void refreshAfterCommit(final Database database, final MaterializedViewImpl view) {
    refresh(database, view, true);
  }

  private static void refresh(final Database database, final MaterializedViewImpl view, final boolean mayHandOffFurtherPasses) {
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

    runOwnedPasses(database, view, mayHandOffFurtherPasses, false);
  }

  /**
   * Makes the passes of a refresh whose ownership the caller already holds, and gives the ownership back when none is owed.
   *
   * @param onScheduler whether the caller is the scheduler task a hand-off queued, which makes the recovery full pass of a
   *                    per-record view itself instead of handing it off again
   */
  private static void runOwnedPasses(final Database database, final MaterializedViewImpl view,
      final boolean mayHandOffFurtherPasses, final boolean onScheduler) {
    // Set when an ALTER replaced this instance: the passes still owed are made on the registered one, AFTER the try, so a failure
    // of theirs is handled once, by the registered view's own passes, and never again here, by an instance already given back
    boolean replaced = false;
    try {
      do {
        if (view.current() != view) {
          replaced = true;
          break;
        }
        MaterializedViewImpl.PendingDelta delta = view.isPerRecord() ? view.drainPendingDelta() : null;
        if (delta != null && delta.recovery() && mayHandOffFurtherPasses && !onScheduler) {
          // The full pass a failed pass owes (issue #11475) rewrites the whole view, and the commit that found it pending owes
          // its client nothing of that cost: the request goes back and the scheduler makes it. Until it succeeds the view says
          // STALE, persisted, because the records the failed pass drained are not in it.
          view.requestFullPass(true);
          view.setStatus(MaterializedViewStatus.STALE);
          persistStatus(database, view.getName(), view.getStatus());
          if (handOffFurtherPasses(database, view))
            return;
          // the scheduler cannot take it: this thread keeps the pass, as it did before the hand-off existed
          delta = view.drainPendingDelta();
        }

        // #11019: a pass that starts from a non-VALID status has to write the VALID it ends in, or a reopen brings the old one back
        final boolean persistedStatusIsStale = !MaterializedViewStatus.VALID.name().equals(view.getStatus());
        view.setStatus(MaterializedViewStatus.BUILDING);
        final long startNs = System.nanoTime();

        if (delta != null)
          perRecordPass(database, view, delta);
        else
          refreshOnce(database, view);

        view.recordRefreshSuccess((System.nanoTime() - startNs) / 1_000_000);
        view.updateLastRefreshTime();
        view.setStatus(MaterializedViewStatus.VALID);
        if (persistedStatusIsStale)
          persistStatus(database, view.getName(), view.getStatus());

        // A whole-view pass that read its guard before an ALTER replaced the view, and wrote after the whole ALTER ran, committed
        // rows without the source link the upgraded view matches by (issue #11475). The ALTER registered its replacement before
        // its own pass started, so either this pass committed first and that pass deleted its rows, or it is seen here and the
        // full pass passed on deletes them
        if (delta == null && view.current() != view) {
          replaced = true;
          break;
        }
      } while (view.finishRefreshPassAndCheckPending() && !(mayHandOffFurtherPasses && handOffFurtherPasses(database, view)));

    } catch (final Throwable e) {
      // Throwable, not Exception: refresh ownership is held from the tryBeginRefresh() above, and the only
      // places that give it back are the two releases in this method. Letting an Error past them latched the
      // view in RUNNING for the life of the database - every later refresh, periodic or manual or incremental,
      // then found the state machine busy and either coalesced onto a pass that had already died or returned
      // having done nothing, so the view silently froze at its last successful snapshot. The pass is still
      // reported as failed and the Error still propagates to the caller; what changes is that the next one can run.
      view.recordRefreshError();
      // A per-record pass that failed took its records out of the pending delta and wrote none of them: only a full pass
      // repairs that, so it is asked for here, before the ownership is released, and the next pass of any trigger makes it.
      // Without it the next commit's delta pass ended VALID over a view still missing the failed pass's records (#11475).
      if (view.isPerRecord())
        view.requestFullPass(true);
      view.setStatus(MaterializedViewStatus.ERROR);
      persistStatus(database, view.getName(), view.getStatus());
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

    if (replaced)
      passToReplacement(database, view, mayHandOffFurtherPasses);
  }

  /**
   * An ALTER replaced the view whose refresh the caller owns (issue #11475): the passes it still owes are made on the registered
   * instance, never on this one, whose pass is the one of the mode and shape the view had before (a legacy view's whole-view
   * rewrite drops the source link from every row of the upgraded view). Ownership of the replaced instance is given back first,
   * discarding what it had pending: every commit that asked for it is covered by the full pass asked for here, and a request
   * that reaches it later owns it again and is passed on the same way.
   */
  private static void passToReplacement(final Database database, final MaterializedViewImpl replaced,
      final boolean mayHandOffFurtherPasses) {
    replaced.endRefresh();
    final MaterializedViewImpl current = replaced.current();
    if (current.isPerRecord())
      current.requestFullPass(false);
    refresh(database, current, mayHandOffFurtherPasses);
  }

  /**
   * One pass of a per-record view (issue #11475) over the delta drained at its start, so everything appended before that point is
   * covered by the pass and everything appended while it runs stays pending for the next one. A full mark makes it a full pass,
   * otherwise only the drained records are re-evaluated; with neither there is nothing to write.
   */
  private static void perRecordPass(final Database database, final MaterializedViewImpl view,
      final MaterializedViewImpl.PendingDelta delta) {
    if (delta.full())
      MaterializedViewPerRecordRefresher.fullPass(database, view);
    else if (delta.rids().size() > 0)
      MaterializedViewPerRecordRefresher.deltaPass(database, view, delta.rids());
  }

  /**
   * Passes the ownership of the refresh, with the request it still holds, to the schema's scheduler thread, which makes the
   * further passes. Ownership is never released in between, so a request cannot be lost and no second owner can start. Returns
   * {@code false} when the scheduler cannot take it (closed, or not a local schema): the caller then keeps the passes, as it
   * did before the hand-off existed.
   * <p>
   * The scheduler thread also hands off: a pass it makes while further requests are owed ends by queueing the next one behind
   * whatever is already due, instead of looping, because that single thread also runs every periodic refresh of every view of
   * the database (issue #11470).
   */
  private static boolean handOffFurtherPasses(final Database database, final MaterializedViewImpl view) {
    if (!(database.getSchema() instanceof LocalSchema schema))
      return false;
    return schema.getMaterializedViewScheduler().handOff(database, view, () -> {
      try {
        // true: one pass, then back to the end of the scheduler's queue if more are owed, so the periodic ticks of this
        // database's other views interleave instead of waiting for as long as writers keep requesting passes (issue #11470)
        runOwnedPasses(database, view, true, true);
      } catch (final Throwable e) {
        // runOwnedPasses() has already marked the view ERROR and logged it; there is no caller left to rethrow to
        view.setStatus(MaterializedViewStatus.STALE);
        persistStatus(database, view.getName(), view.getStatus());
      }
    });
  }

  /**
   * Writes the schema so the status just set survives a close and reopen (issue #11019). Only the transitions into and out of a
   * non-VALID status call it: both are rare, a healthy view never takes either, so no schema write lands on the per-tick path.
   * Best effort: the refresh outcome the caller is about to report matters more than the persistence of its label, and a failed
   * save is logged, not thrown over the original failure.
   */
  static void persistStatus(final Database database, final String name, final String status) {
    try {
      ((LocalSchema) database.getSchema()).saveConfiguration();
    } catch (final Exception e) {
      LogManager.instance().log(MaterializedViewRefresher.class, Level.SEVERE,
          "Could not persist the status %s of '%s': a reopen will report the status of the last schema save instead", e, status, name);
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
    // The scope is what lets these writes through: nothing else writes a backing type (issue #11020)
    try (final BackingTypeWriteScope ignored = BackingTypeWriteScope.open(view)) {
      writeSnapshot(database, view, backingTypeName);
    }
  }

  private static void writeSnapshot(final Database database, final MaterializedViewImpl view, final String backingTypeName) {
    database.transaction(() -> {
      // A whole-view rewrite that an ALTER upgrading the view to per-record overtook (a retry after a conflict with the upgrade's
      // own pass) writes nothing: it would drop the source link from every row (issue #11475). Nothing is lost: the commits that
      // asked for it landed before the ALTER, whose full pass covers them, and its owner passes any further request on
      final MaterializedViewImpl current = view.current();
      if (current != view && current.isPerRecord())
        return;
      final RecordIdentities previousSnapshot = collectIdentities(database, backingTypeName);
      int reused = 0;

      // Execute the defining query and write its rows over the previous snapshot's records
      try (final ResultSet rs = database.query("sql", view.getQuery())) {
        while (rs.hasNext()) {
          final Result result = rs.next();
          final MutableDocument doc = reused < previousSnapshot.size() ?
              ((Document) database.lookupByRID(previousSnapshot.get(reused++), true)).modify() :
              database.newDocument(backingTypeName);
          // The row comes from the defining query, not from the reused record: no stale-read refusal (#8610)
          doc.clearBasedOnStaleRead();
          applyRow(doc, result);
          doc.save();
        }
      }

      // Whatever the new snapshot did not need
      for (int i = reused; i < previousSnapshot.size(); i++)
        database.lookupByRID(previousSnapshot.get(i), true).delete();
    }, false);
  }

  /** Copies one query row onto a document, dropping any property the row does not carry. */
  static void applyRow(final MutableDocument doc, final Result row) {
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
}
