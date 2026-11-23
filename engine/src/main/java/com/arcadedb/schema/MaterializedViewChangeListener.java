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

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.OrderedAfterCommitCallback;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.AfterRecordUpdateListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.MaterializedViewGroupedPlan.Contribution;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class MaterializedViewChangeListener
    implements AfterRecordCreateListener, AfterRecordUpdateListener, AfterRecordDeleteListener, BeforeRecordUpdateListener,
    BeforeRecordDeleteListener {

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

  /**
   * A grouped view maintained by delta (issue #11937) takes the state a record had before the transaction changed it here, before
   * the first change: the update and the delete overwrite it.
   */
  @Override
  public boolean onBeforeUpdate(final Record record) {
    if (database.isTransactionActive() && view.isGroupedDelta())
      groupedCapture(database.getTransaction()).before(record, true);
    return true;
  }

  @Override
  public boolean onBeforeDelete(final Record record) {
    if (database.isTransactionActive() && view.isGroupedDelta())
      groupedCapture(database.getTransaction()).before(record, false);
    return true;
  }

  @Override
  public void onAfterCreate(final Record record) {
    onChange(record, true);
  }

  @Override
  public void onAfterUpdate(final Record record) {
    onChange(record, false);
  }

  @Override
  public void onAfterDelete(final Record record) {
    onChange(record, false);
  }

  private void onChange(final Record record, final boolean created) {
    if (!database.isTransactionActive())
      return;

    final TransactionContext tx = database.getTransaction();
    if (view.isPerRecord())
      captureDelta(tx, record);
    else if (view.isGroupedDelta())
      groupedCapture(tx).after(record, created);
    else
      tx.addAfterCommitCallbackIfAbsent(callbackKey, () -> {
        final MaterializedViewImpl target = targetAtCommit();
        if (target == null)
          return;
        try {
          // This capture holds no records: a view upgraded to per-record since the change was made owes a full pass instead
          if (target.tracksPendingDelta())
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

  /** The capture of this transaction's changes for a grouped view (issue #11937), registered with the transaction on first use. */
  private GroupedCapture groupedCapture(final TransactionContext tx) {
    GroupedCapture capture = (GroupedCapture) tx.getDerived(this);
    if (capture == null) {
      capture = new GroupedCapture();
      tx.putDerived(this, capture);
    }
    // Every time, as the per-record path does: a statement taken back drops the after-commit callback it registered
    tx.addAfterCommitCallbackIfAbsent(callbackKey, capture);
    tx.addBeforeCommitCallbackIfAbsent(callbackKey, capture.beforeCommitTask);
    return capture;
  }

  /**
   * What a transaction did to one source record of a grouped view: its contribution to the view before the transaction first
   * changed it (null when it was not in the view, or did not exist), and as the transaction commits it (null when it is not in the
   * view, or was deleted).
   */
  static final class RecordChange {
    Contribution before;
    Contribution after;
  }

  /**
   * The changes one transaction makes to the source records of a grouped view (issue #11937), and the ordered after-commit callback
   * that hands their delta to the view.
   * <p>
   * The state of a record before the transaction is taken at its first change in the transaction: for an update, the image the
   * record was read from, which the update's own MVCC check refuses to commit over anything else (#6950); for a delete, the record
   * as its page holds it, with the page taken into the transaction first, so the commit's page-version check refuses a concurrent
   * change landing after the read. A record the transaction creates had no state before. The state after is read when the commit
   * starts ({@code TransactionContext.addBeforeCommitCallbackIfAbsent}): whatever statements were taken back, it is what the
   * commit writes. The delta of the transaction is then {@code after - before} for each record.
   * <p>
   * The callback is ordered ({@link OrderedAfterCommitCallback}): at its commit point the transaction registers with the view, so a
   * rebuild pass of the view running concurrently knows that its scan may or may not have read this transaction's changes, and
   * corrects for them (see {@link MaterializedViewGroupedState.Watch}). The delta is handed over once the pages are published, while
   * the transaction still holds its locks ({@link #onPublished()}), and not when the callback runs, after they are released (issue
   * #12720): a later transaction may by then have changed the same records again and handed its own delta over, and a rebuild
   * beginning meanwhile would take this transaction's after-images, still in flight, for the newest state of those records. The run
   * only refreshes the view.
   */
  final class GroupedCapture implements OrderedAfterCommitCallback {
    private final MaterializedViewGroupedPlan       plan             = view.groupedPlan();
    private final Map<RID, RecordChange>            changes          = new LinkedHashMap<>();
    private final int                               maxDelta         = view.maxPerRecordDelta();
    final         Runnable                          beforeCommitTask = this::beforeCommit;
    private       WhereClause                       where;
    private       CommandContext                    context;
    /** Past the cap, or a record whose state before could not be read: the transaction owes the view a rebuild. */
    private       boolean                           overflow;
    private       MaterializedViewGroupedState.Groups delta;
    /** The view this transaction registered with at its commit point, null before it (or when it had nothing to write). */
    private volatile MaterializedViewImpl            registeredWith;
    /** Set under the view's pending-delta lock when a rebuild pass took this transaction's changes over. */
    boolean                                          absorbed;
    /** Set once {@link #onPublished()} ran; the failure to hand the delta over there, if any, is written before it. */
    private volatile boolean                         delivered;
    private          RuntimeException                deliveryFailure;

    void before(final Record record, final boolean update) {
      final RID rid = record.getIdentity();
      if (overflow || rid == null || changes.containsKey(rid))
        return;
      if (changes.size() >= maxDelta) {
        overflow();
        return;
      }
      final RecordChange change = new RecordChange();
      try {
        final Document committed = update && record instanceof MutableDocument ? readImage(record) : null;
        change.before = contributionOf(committed != null ? committed : pinnedRead(rid));
      } catch (final RecordNotFoundException e) {
        // nothing to change: the operation itself fails on it
        change.before = null;
      } catch (final RuntimeException e) {
        overflow();
        return;
      }
      changes.put(rid, change);
    }

    void after(final Record record, final boolean created) {
      final RID rid = record.getIdentity();
      if (overflow || rid == null || changes.containsKey(rid))
        return;
      if (!created || changes.size() >= maxDelta)
        // an update or a delete whose state before was not taken
        overflow();
      else
        changes.put(rid, new RecordChange());
    }

    /** The image the update of a record read in this transaction is computed from: its buffer, which the update leaves alone. */
    private Document readImage(final Record record) {
      final Binary buffer = ((RecordInternal) record).getBuffer();
      if (buffer == null)
        return null;
      return (Document) database.getRecordFactory()
          .newImmutableRecord(database, ((Document) record).getType(), record.getIdentity(), buffer.copyOfContent(), null);
    }

    /** The record as its page holds it, with the page taken into the transaction first: the commit then checks its version. */
    private Document pinnedRead(final RID rid) {
      final LocalBucket bucket = (LocalBucket) database.getSchema().getEmbedded().getBucketById(rid.getBucketId());
      try {
        bucket.fetchPageInTransaction(rid);
      } catch (final IOException e) {
        throw new DatabaseOperationException("Cannot read record " + rid, e);
      }
      return pageRead(bucket, rid);
    }

    /** The record as the transaction's view of its page holds it, bypassing the record caches. */
    private Document pageRead(final LocalBucket bucket, final RID rid) {
      final Binary content = bucket.getRecord(rid);
      return (Document) database.getRecordFactory().newImmutableRecord(database,
          database.getSchema().getEmbedded().getTypeByBucketId(rid.getBucketId()), rid, content.copyOfContent(), null);
    }

    /**
     * The record as the commit writes it: the instance queued for the deferred update, whose content the commit serializes, or else
     * what the transaction's pages hold (a creation or a delete is written to the page at once). Null for a record that is gone.
     */
    private Document committedState(final TransactionContext tx, final RID rid) {
      if (tx.isUpdateQueued(rid) && tx.getWrittenRecord(rid) instanceof Document queued)
        return queued;
      try {
        return pageRead((LocalBucket) database.getSchema().getEmbedded().getBucketById(rid.getBucketId()), rid);
      } catch (final RecordNotFoundException e) {
        return null;
      }
    }

    private Contribution contributionOf(final Document record) {
      if (where == null && context == null) {
        where = plan.newWhere();
        context = MaterializedViewGroupedPlan.newContext(database);
      }
      return plan.contributionOf(record, where, context);
    }

    private void overflow() {
      overflow = true;
      changes.clear();
      delta = null;
    }

    /** Run when the commit starts: reads the state each changed record is committed in, and folds the changes into the delta. */
    private void beforeCommit() {
      if (overflow)
        return;
      try {
        final TransactionContext tx = database.getTransaction();
        final MaterializedViewGroupedState.Groups groups = new MaterializedViewGroupedState.Groups(plan);
        for (final Map.Entry<RID, RecordChange> entry : changes.entrySet()) {
          final RecordChange change = entry.getValue();
          final Document current = committedState(tx, entry.getKey());
          change.after = current != null ? contributionOf(current) : null;
          if (change.before == null ? change.after != null : !change.before.sameAs(change.after)) {
            groups.add(change.before, -1);
            groups.add(change.after, 1);
          }
        }
        delta = groups;
      } catch (final RuntimeException e) {
        LogManager.instance().log(this, Level.FINE, "Cannot read the changes of a transaction for view '%s': it is rebuilt", e,
            view.getName());
        overflow();
      }
    }

    /** What a rebuild pass absorbing this transaction corrects for, or null when it cannot (see {@code Watch.absorb}). */
    Map<RID, RecordChange> changesForWatch() {
      return overflow || delta == null ? null : changes;
    }

    @Override
    public void onCommitPoint() {
      final MaterializedViewImpl target = view.current();
      if (registeredWith != null || !target.isGroupedDelta())
        return;
      registeredWith = target;
      target.groupedCommitPoint(this);
    }

    @Override
    public void onPublished() {
      try {
        deliver(targetAtCommit());
      } catch (final RuntimeException e) {
        deliveryFailure = e;
      } finally {
        delivered = true;
      }
    }

    @Override
    public void onAbandoned() {
      final MaterializedViewImpl registered = registeredWith;
      if (registered != null)
        registered.groupedAbandoned(this);
    }

    @Override
    public void run() {
      final MaterializedViewImpl target = targetAtCommit();
      // read first: the failure is written before it
      final boolean published = delivered;
      if (published && deliveryFailure != null) {
        if (target != null)
          markStale(target, deliveryFailure);
        return;
      }
      try {
        if (!published)
          // Not told of its publication: it never reached its commit point (registered after it, or the replication layer published
          // the pages elsewhere), so it has nothing in flight a rebuild could have taken over
          deliver(target);
        if (target != null)
          MaterializedViewRefresher.refreshAfterCommit(database, target);
      } catch (final Exception e) {
        if (target != null)
          markStale(target, e);
      }
    }

    /** Takes the transaction out of flight and hands its delta to the view, or asks for the full pass it owes the view instead. */
    private void deliver(final MaterializedViewImpl target) {
      final MaterializedViewImpl registered = registeredWith;
      final boolean maintained = target != null && target.isGroupedDelta();
      // The delta can be applied when it is the one of the view this transaction registered with, built for the view's plan. A
      // transaction that never registered (it was not told of its commit point) cannot say what a rebuild saw: the view is rebuilt
      final boolean exact = maintained && !overflow && delta != null && registered == target && target.groupedPlan() == plan;
      final boolean absorbed = registered != null && registered.groupedDelivered(this,
          exact && (delta.size() > 0 || delta.inexact) ? delta : null);
      if (maintained && !exact && !absorbed)
        target.requestFullPass(false);
    }
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
