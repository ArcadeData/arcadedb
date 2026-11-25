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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerDatabase;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.logging.Level;

/**
 * Recovers the INCREMENTAL materialized views of each database once, in the term in which this node leads (issue #11555):
 * when it becomes the leader, and for a database opened later in the same term.
 * <p>
 * Split out of {@link ArcadeStateMachine}, which triggers the recovery on a leader change.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MaterializedViewRecovery {
  private final ArcadeStateMachine sm;

  // Issue #11555: the Raft term in which this node, as the leader, last recovered the INCREMENTAL materialized views of each
  // database. The leadership hook recovers the databases open when it runs, the open of a database recovers it when it happens
  // later in the leadership: whichever comes first in a term makes the full recovery, the other only what the views still owe
  final ConcurrentHashMap<String, Long> materializedViewsRecoveredInTerm = new ConcurrentHashMap<>();

  MaterializedViewRecovery(final ArcadeStateMachine sm) {
    this.sm = sm;
  }

  /**
   * Hands the recovery full pass of every INCREMENTAL materialized view of every open database to that database's scheduler
   * thread (issues #11550, #12220), once this leader is READY: Ratis reports that only after it has applied every entry committed
   * before its term, so the passes read every source change the previous leader committed. The passes themselves do not run here, so
   * this executor waits for the readiness only. A database being replaced with a peer's copy is skipped: the leadership is handed
   * on in that case, and the next leader recovers it. A database this leader opens later is recovered by its open
   * ({@link #recoverMaterializedViewsOfOpenedDatabase}, issue #11555).
   */
  void recoverMaterializedViewsAsNewLeader(final RaftHAServer raftHA) {
    if (!awaitLeaderReadyForMaterializedViewRecovery(raftHA))
      return;

    final ArcadeDBServer srv = sm.server;
    if (srv == null)
      return;
    final long term = raftHA.getCurrentTerm();

    // Read under the registry lock (issue #11555): an open still between the HA wrapper, which found this node not leading yet,
    // and its registration is then either finished, and listed here, or not started, and its wrapper finds this leader ready
    final List<String> openNames;
    synchronized (srv.getDatabasesLock()) {
      openNames = new ArrayList<>(srv.getDatabaseNames());
    }
    for (final String dbName : openNames) {
      if (ArcadeDBServer.isReservedDatabaseName(dbName) || sm.replacement.databasesBeingReplaced.containsKey(dbName))
        continue;
      try {
        // Never loads one: a database not open now is recovered by its open
        final ServerDatabase db = srv.getDatabase(dbName, false, false);
        if (db == null || !db.isOpen())
          continue;
        recoverMaterializedViewsAsLeader(dbName, db.getSchema().getEmbedded(), term);
      } catch (final Exception e) {
        LogManager.instance().log(sm, Level.WARNING,
            "Leader recovery of the materialized views of database '%s' failed: run REFRESH MATERIALIZED VIEW on its INCREMENTAL "
                + "views (issue #11550): %s", e, dbName, e.getMessage());
      }
    }
  }

  /**
   * The recovery a database opened on this node owes its INCREMENTAL materialized views when this node leads (issue #11555). The
   * open-time recovery of the engine is skipped on every HA node, which cannot tell at the open whether it leads, and the
   * leadership hook ({@link #recoverMaterializedViewsAsNewLeader}) recovers only the databases open when it runs: a database the
   * leader opens later (loaded lazily, closed and opened again) would otherwise keep views that miss the commits whose pending
   * delta died with the previous leader. Called by the HA wrapper on every open; on a node not leading it does nothing, and the
   * leadership hook covers the database if this node is elected while it is open. The recovery waits for the leader to be ready
   * on {@link ArcadeStateMachine#lifecycleExecutor}, never on the opening thread.
   */
  void recoverMaterializedViewsOfOpenedDatabase(final String dbName, final DatabaseInternal db) {
    final RaftHAServer raftHA = sm.raftHAServer;
    if (raftHA == null || dbName == null || ArcadeDBServer.isReservedDatabaseName(dbName) || !raftHA.isLeader())
      return;
    try {
      sm.lifecycleExecutor.submit(() -> {
        if (!awaitLeaderReadyForMaterializedViewRecovery(raftHA) || sm.replacement.databasesBeingReplaced.containsKey(dbName) || !db.isOpen())
          return;
        try {
          recoverMaterializedViewsAsLeader(dbName, db.getSchema().getEmbedded(), raftHA.getCurrentTerm());
        } catch (final Exception e) {
          LogManager.instance().log(sm, Level.WARNING,
              "Recovery of the materialized views of database '%s' at its open failed: run REFRESH MATERIALIZED VIEW on its "
                  + "INCREMENTAL views (issue #11555): %s", e, dbName, e.getMessage());
        }
      });
    } catch (final RejectedExecutionException ree) {
      HALog.log(sm, HALog.BASIC, "Cannot schedule the materialized view recovery of database '%s': executor is shut down",
          dbName);
    }
  }

  /**
   * The full recovery the first time in the leadership term, then only the views that still owe it: the leadership hook and the
   * open of the same database can both reach a database in one term, and the commits of the previous leader are recovered once.
   */
  private void recoverMaterializedViewsAsLeader(final String dbName, final LocalSchema schema, final long term) {
    // An unreadable term (-1) is never recorded, so every recovery under it is a full one
    final Long previous = term >= 0 ? materializedViewsRecoveredInTerm.put(dbName, term) : null;
    final boolean everyView = previous == null || previous != term;
    final int views;
    try {
      views = schema.recoverIncrementalMaterializedViews(everyView);
    } catch (final RuntimeException | Error e) {
      // A full recovery that failed is not recorded as made: the next open in this term makes it again, not the partial one
      if (everyView && term >= 0)
        materializedViewsRecoveredInTerm.remove(dbName, term);
      throw e;
    }
    if (views > 0)
      LogManager.instance().log(sm, Level.INFO,
          "Leader recovery of database '%s': %d INCREMENTAL materialized view(s) handed a %s pass", dbName, views,
          everyView ? "full" : "recovery");
  }

  /** Whether this node became the READY leader within the quorum timeout: what every materialized view recovery waits for. */
  private boolean awaitLeaderReadyForMaterializedViewRecovery(final RaftHAServer raftHA) {
    try {
      if (raftHA.awaitApplied(raftHA::isLeaderReady, raftHA.getQuorumTimeout()))
        return true;
      if (raftHA.isLeader())
        LogManager.instance().log(sm, Level.WARNING,
            "This leader did not become ready within %d ms: the INCREMENTAL materialized views are not recovered and may miss "
                + "commits of the previous leader; run REFRESH MATERIALIZED VIEW on them (issue #11550)",
            raftHA.getQuorumTimeout());
      return false;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}
