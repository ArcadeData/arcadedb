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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.log.LogManager;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class MaterializedViewChangeListener
    implements AfterRecordCreateListener, AfterRecordUpdateListener, AfterRecordDeleteListener {

  private static final AtomicLong INSTANCES = new AtomicLong();

  private final MaterializedViewImpl view;
  private final DatabaseInternal database;
  /**
   * Unique to this listener INSTANCE, not to the view's name (issue #11475): an ALTER replaces the view and its listener while a
   * transaction that already captured changes through the old listener may still be open. With a per-name key the new listener's
   * callback was refused as a duplicate, and what the transaction changed after the ALTER was buffered where no callback read it.
   * Each listener therefore registers its own callback, and both follow the view to its current instance at commit.
   */
  private final String callbackKey;

  public MaterializedViewChangeListener(final DatabaseInternal database, final MaterializedViewImpl view) {
    this.database = database;
    this.view = view;
    this.callbackKey = "mv-refresh:" + view.getName() + '#' + INSTANCES.incrementAndGet();
  }

  @Override
  public void onAfterCreate(final Record record) {
    onChange(record);
  }

  @Override
  public void onAfterUpdate(final Record record) {
    onChange(record);
  }

  @Override
  public void onAfterDelete(final Record record) {
    onChange(record);
  }

  private void onChange(final Record record) {
    if (!database.isTransactionActive())
      return;

    final TransactionContext tx = database.getTransaction();
    if (view.isPerRecord())
      captureDelta(tx, record);
    else
      tx.addAfterCommitCallbackIfAbsent(callbackKey, () -> {
        final MaterializedViewImpl target = targetAtCommit();
        if (target == null)
          return;
        try {
          // This capture holds no records: a view upgraded to per-record since the change was made owes a full pass instead
          if (target.isPerRecord())
            target.requestFullPass(false);
          MaterializedViewRefresher.refreshAfterCommit(database, target);
        } catch (final Exception e) {
          markStale(target, e);
        }
      });
  }

  /**
   * The view a commit refreshes: the one registered now, which an ALTER made while the transaction was open may have put in place of
   * the one this listener was built for (issue #11475). Null when that replacement is no longer maintained on commit.
   */
  private MaterializedViewImpl targetAtCommit() {
    final MaterializedViewImpl target = view.current();
    if (target != view && target.getRefreshMode() != MaterializedViewRefreshMode.INCREMENTAL)
      return null;
    return target;
  }

  /**
   * Buffers the RID of a changed source record of a per-record view (issue #11475) in the transaction's own state, kept with
   * {@code putDerived} rather than as an attachment, which would mark the running statement unrestorable. The buffer is also the
   * after-commit callback, registered under the same key as a non-per-record view's, so the per-record path allocates nothing per
   * record beyond the two primitives. A rolled-back transaction drops both; a rolled-back statement drops the callback, and the
   * next change re-registers it with the same buffer, whose extra RIDs only cost a re-evaluation of their current state.
   */
  private void captureDelta(final TransactionContext tx, final Record record) {
    TransactionDelta delta = (TransactionDelta) tx.getDerived(this);
    if (delta == null) {
      delta = new TransactionDelta();
      tx.putDerived(this, delta);
    }
    delta.add(record.getIdentity());
    tx.addAfterCommitCallbackIfAbsent(callbackKey, delta);
  }

  private void markStale(final MaterializedViewImpl view, final Exception e) {
    view.setStatus(MaterializedViewStatus.STALE);
    MaterializedViewRefresher.persistStatus(database, view.getName(), view.getStatus());
    LogManager.instance().log(this, Level.WARNING,
        "Error in incremental refresh for view '%s', marking as STALE: %s",
        e, view.getName(), e.getMessage());
  }

  /**
   * The source records one transaction changed, and the after-commit callback that hands them to the view. The callback holds the
   * buffer itself: the transaction's derived state is released before its after-commit callbacks run.
   */
  private final class TransactionDelta implements Runnable {
    private final RecordIdentities rids        = new RecordIdentities(8);
    private final int              maxDelta    = view.maxPerRecordDelta();
    private       int              nextCountAt = maxDelta;
    private       boolean          overflow;

    /**
     * Appends raw, so a record changed N times costs N slots of two primitives and no allocation. Past the cap the distinct RIDs
     * are counted: still over it, the buffer is dropped and the transaction owes a full pass instead. The next count waits until
     * the buffer has doubled, so a transaction that keeps changing the same few records does not sort on every change.
     */
    void add(final RID rid) {
      if (overflow)
        return;
      rids.add(rid);
      if (rids.size() <= nextCountAt)
        return;
      rids.sortAndDeduplicate();
      if (rids.size() > maxDelta) {
        overflow = true;
        rids.clear();
      } else
        nextCountAt = Math.max(maxDelta, rids.size() * 2);
    }

    @Override
    public void run() {
      final MaterializedViewImpl target = targetAtCommit();
      if (target == null)
        return;
      try {
        // A target that is not per-record rewrites the whole view on every pass and has no pending delta to feed
        if (overflow && target.isPerRecord())
          target.requestFullPass(false);
        else if (target.isPerRecord())
          // Appended BEFORE trying to begin the refresh: if another thread owns it, its next pass drains these RIDs. Handed over,
          // not copied: this callback runs once per commit and never reads the buffer again
          target.adoptPendingDelta(rids);
        MaterializedViewRefresher.refreshAfterCommit(database, target);
      } catch (final Exception e) {
        markStale(target, e);
      }
    }
  }

  public MaterializedViewImpl getView() {
    return view;
  }
}
