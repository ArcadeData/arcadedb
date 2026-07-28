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

    } catch (final Exception e) {
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

  private static void refreshOnce(final Database database, final MaterializedViewImpl view) {
    final String backingTypeName = view.getBackingTypeName();

    // Use joinCurrentTx=false to always create a dedicated transaction for the refresh.
    // This ensures changes are committed immediately even when called from an async context
    // (e.g. HTTP API with awaitResponse=false), where a long-running batched transaction
    // would otherwise defer the commit indefinitely.
    // See: https://github.com/ArcadeData/arcadedb/issues/3941
    database.transaction(() -> {
      // Truncate existing data, faster than DELETE FROM (no per-record WAL entries).
      database.command("sql", "TRUNCATE TYPE `" + backingTypeName + "` UNSAFE").close();

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
