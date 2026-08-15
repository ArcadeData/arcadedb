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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

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
   * Clears the backing type and repopulates it from the defining query, as ONE transaction.
   * <p>
   * The clear is a {@code DELETE FROM} rather than the {@code TRUNCATE TYPE ... UNSAFE} this used to run, and the
   * difference is the whole point (issue #6203). {@code TRUNCATE} commits the caller's transaction from the inside -
   * {@code schema.dropIndex()} for every index on the backing type before a single record is touched, then
   * {@code commit(); begin()} once per {@code arcadedb.truncateBatchSize} records, then once more before it rebuilds
   * the indexes it dropped - so the refresh was a sequence of committed transactions wearing the costume of one. Both
   * consequences were silent:
   * <ul>
   * <li>every other reader saw the view empty, or holding some prefix of the new rows, for the whole runtime of the
   * defining query - which for a view worth materialising is exactly the expensive case, and for a PERIODIC view is
   * every tick;</li>
   * <li>a defining query that threw after the truncate had committed left the backing type with zero rows. The view
   * then reported ERROR or STALE, and both of those read as "the data you are looking at is old" when the data was in
   * fact gone - until some later refresh happened to succeed, which for a MANUAL view may be never.</li>
   * </ul>
   * As one transaction the refresh gets the isolation every other ArcadeDB transaction has: the deletes and the
   * inserts stay in the transaction's own page buffer until commit, so a concurrent reader sees the previous snapshot
   * throughout and the new one afterwards, and a failure anywhere rolls the whole pass back onto the previous
   * snapshot rather than onto nothing.
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
   * What this costs relative to the truncate: the transaction now holds the pages of the old rows as well as the new
   * ones - bounded in practice by the free space the deletes hand straight back to the inserts in the same buckets -
   * and an index on the backing type is maintained entry by entry instead of being dropped and rebuilt empty. Under
   * HA the pass is one Raft log entry rather than one per truncate batch plus one for the repopulate. None of this is
   * a new ceiling: the repopulate was already a single unbounded transaction, so a view too large to refresh in one
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
      // Clear the previous snapshot inside this transaction, so it stays visible until the new one replaces it.
      database.command("sql", "DELETE FROM `" + backingTypeName + "`").close();

      // Execute the defining query and insert results
      try (final ResultSet rs = database.query("sql", view.getQuery())) {
        while (rs.hasNext()) {
          final Result result = rs.next();
          final MutableDocument doc = database.newDocument(backingTypeName);
          for (final String prop : result.getPropertyNames()) {
            if (!prop.startsWith("@"))
              doc.set(prop, result.getProperty(prop));
          }
          doc.save();
        }
      }
    }, false);
  }
}
