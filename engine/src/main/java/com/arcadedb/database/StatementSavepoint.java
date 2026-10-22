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
package com.arcadedb.database;

import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageUndoLog;
import com.arcadedb.graph.MutableEdgeSegment;
import com.arcadedb.index.fulltext.FullTextCorpusDeltas;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;

/**
 * The savepoint of one statement run inside a transaction the CALLER keeps (issue #10346). A statement that fails midway through a
 * multi-row write leaves the rows it wrote before the error in the transaction; with the savepoint the failure takes them back, and
 * the transaction goes on as if the statement had never run, the way SQLite undoes a failed statement. Without one (see
 * {@link TransactionContext#refuseCommitIfWrittenSince}) the transaction could only be thrown away.
 * <p>
 * <b>What it costs.</b> A statement that owns its transaction never gets one: the whole transaction rolls back. Inside a caller's
 * transaction the savepoint starts as a handful of marks and journals nothing until the statement changes something, and then it
 * keeps only what it needs to put back: a log of the bytes the statement overwrote on pages the transaction had already modified
 * ({@link PageUndoLog}, bounded by the pages the statement re-touches), the previous value of every map entry it changed, and the
 * lanes of index entries it queued ({@link TransactionIndexContext#beginStatementJournal}). A page the statement is the first to
 * modify is simply dropped. One object per transaction context, reused by every statement: nothing is allocated for a statement
 * that writes nothing.
 * <p>
 * <b>What it does not do.</b> The events a statement already fired cannot be taken back, and neither can the schema changes it made.
 * A statement it cannot take back EXACTLY is not taken back at all: {@link #rollback()} answers {@code false} and the caller falls
 * back to refusing the commit of the transaction. That covers a statement that changed more than
 * {@link com.arcadedb.GlobalConfiguration#TX_STATEMENT_SAVEPOINT_MAX_ENTRIES} entries, one that changed again a record an earlier
 * statement had updated and not written yet (its only copy is the object the statement mutated), and one that dropped a file. Never a
 * partial undo.
 * <p>
 * Not thread-safe: a transaction belongs to one thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class StatementSavepoint implements PageUndoLog.GrainBudget {
  private static final byte OP_MODIFIED_CACHE  = 0;
  private static final byte OP_UPDATED         = 1;
  private static final byte OP_DELETED_ADDED   = 2;
  private static final byte OP_BUCKET_DELTA    = 3;
  private static final byte OP_LIGHT_EDGE      = 4;
  private static final byte OP_INDEX_SNAPSHOT  = 5;
  private static final byte OP_OFF_PAGE        = 6;
  private static final byte OP_UNIDIR_TARGET   = 7;
  private static final byte OP_PAGE_ADDED      = 8;
  private static final byte OP_PAGE_COUNTER    = 9;
  private static final byte OP_CALLBACK_KEY    = 10;
  private static final byte OP_CORPUS          = 11;

  /** Larger log buffers are not kept for the next statement. */
  private static final int  MAX_RETAINED_JOURNAL = 4096;
  private static final int  MAX_RETAINED_LOGS    = 64;

  private final TransactionContext tx;

  private boolean active;
  private boolean rolledBack;
  private String  unrestorable;
  private Throwable nestedFailure;
  private long    beginSequence;
  private long    epoch;
  private int     limit;

  // The marks of the append-only structures, taken when the statement begins
  private long    completedRecordWrites;
  private int     newRecordsMark;
  private int     reservationMark;
  private int     callbackMark;
  private int     indexJournalMark;
  private long    unidirectionalSequence;

  // The journal of the changes to maps and sets: parallel arrays, so a change costs no object
  private byte[]   ops = new byte[32];
  private Object[] a   = new Object[32];
  private Object[] b   = new Object[32];
  private Object[] c   = new Object[32];
  private long[]   n   = new long[32];
  private long[]   m   = new long[32];
  private int      size;

  // The records a deferred update of this statement queued, to tell the first touch of a record (whose previous value is the one
  // before the statement) from the later ones
  private RidHashSet queuedInStatement;
  // The cached records the statement obtained clean: the ones that are dirty when it fails were changed by it and are reloaded
  private final List<Record> obtained = new ArrayList<>();
  // The records an earlier statement queued for an update and this one read: the object IS the only copy of that update, so what the
  // record held before the statement is kept, to be installed back in place if the statement fails
  private RidHashSet           snapshotted;
  private final List<Record>          pendingRecords   = new ArrayList<>();
  private final List<Binary>          pendingSnapshots = new ArrayList<>();
  private       boolean[]             pendingDirty     = new boolean[8];
  private       boolean[]             pendingAssigned  = new boolean[8];

  private PageUndoLog[] logs = new PageUndoLog[8];
  private int           logCount;
  private int           grainBudget;

  StatementSavepoint(final TransactionContext tx) {
    this.tx = tx;
  }

  void begin(final int limit) {
    this.active = true;
    this.rolledBack = false;
    this.unrestorable = null;
    this.nestedFailure = null;
    this.limit = limit;
    this.grainBudget = limit;
    this.beginSequence = tx.getBeginSequence();
    ++epoch;

    completedRecordWrites = tx.getCompletedRecordWrites();
    newRecordsMark = tx.getNewRecordsCount();
    reservationMark = tx.getInsertReservationCount();
    callbackMark = tx.getAfterCommitCallbackCount();
    unidirectionalSequence = tx.getUnidirectionalSequence();
    indexJournalMark = tx.getIndexChanges().beginStatementJournal(limit);
  }

  // ------------------------------------------------------------------------------------------------------------------------------
  // STATE
  // ------------------------------------------------------------------------------------------------------------------------------

  public boolean isActive() {
    return active;
  }

  /**
   * Whether the statement changed anything the savepoint would have to put back. A statement can fail with no record written and still
   * have changed the object the transaction queued for an update, whose refused values the commit would store.
   */
  boolean hasChanges() {
    return size > 0 || logCount > 0 || !pendingRecords.isEmpty() || !obtained.isEmpty()
        || tx.getIndexChanges().getStatementJournalSize(indexJournalMark) > 0;
  }

  /** Whether {@link #rollback()} took the statement back. */
  public boolean isRolledBack() {
    return rolledBack;
  }

  /** Why the statement cannot be taken back, or null while it can. */
  public String getUnrestorableReason() {
    return unrestorable;
  }

  /**
   * Makes the statement one that cannot be taken back: whatever it changes from here is not journaled, and {@link #rollback()}
   * answers {@code false}. The first reason is kept.
   */
  void markUnrestorable(final String reason) {
    if (unrestorable == null)
      unrestorable = reason;
  }

  /** A statement run inside this one failed after writing: this one has to fail too, or the transaction cannot be committed. */
  public void nestedStatementFailed(final Throwable cause) {
    if (nestedFailure == null)
      nestedFailure = cause;
  }

  /** The failure of a nested statement this one did not end with, or null. */
  public Throwable getNestedFailure() {
    return nestedFailure;
  }

  long getEpoch() {
    return epoch;
  }

  int getNewRecordsMark() {
    return newRecordsMark;
  }

  // ------------------------------------------------------------------------------------------------------------------------------
  // JOURNAL
  // ------------------------------------------------------------------------------------------------------------------------------

  private boolean add(final byte op, final Object first, final Object second, final Object third, final long number,
      final long amount) {
    if (unrestorable != null)
      return false;
    if (size == limit) {
      markUnrestorable("the statement changed more than " + limit + " entries of the transaction");
      return false;
    }
    if (size == ops.length) {
      final int newLength = size << 1;
      ops = Arrays.copyOf(ops, newLength);
      a = Arrays.copyOf(a, newLength);
      b = Arrays.copyOf(b, newLength);
      c = Arrays.copyOf(c, newLength);
      n = Arrays.copyOf(n, newLength);
      m = Arrays.copyOf(m, newLength);
    }
    ops[size] = op;
    a[size] = first;
    b[size] = second;
    c[size] = third;
    n[size] = number;
    m[size] = amount;
    ++size;
    return true;
  }

  /** {@code modifiedRecordsCache.put(rid, record)} replaced {@code previous}, which is null when there was none. */
  void modifiedCachePut(final RID rid, final Record previous, final Record record) {
    add(OP_MODIFIED_CACHE, rid, previous, record, 0, 0);
  }

  /** {@code modifiedRecordsCache.remove(rid)} removed {@code previous}. */
  void modifiedCacheRemoved(final RID rid, final Record previous) {
    add(OP_MODIFIED_CACHE, rid, previous, null, 0, 0);
  }

  /**
   * {@code updatedRecords.put(rid, record)} replaced {@code previous}. The instance an earlier statement queued is the only copy of
   * what that statement did to the record until the commit writes it: a statement that mutates that very instance and fails leaves
   * it half changed with nothing to restore it from.
   */
  void updatedPut(final RID rid, final Record previous, final Record record) {
    if (queuedInStatement == null)
      queuedInStatement = new RidHashSet(64);
    if (queuedInStatement.add(rid) && previous != null && previous == record && (snapshotted == null || !snapshotted.contains(rid)))
      markUnrestorable("the statement changed again the record " + rid + ", updated by an earlier statement of the transaction");
    add(OP_UPDATED, rid, previous, null, 0, 0);
  }

  /** {@code updatedRecords.remove(rid)} removed {@code previous}. */
  void updatedRemoved(final RID rid, final Record previous) {
    add(OP_UPDATED, rid, previous, null, 0, 0);
  }

  /**
   * The statement read a record from the transaction's own cache of written records. One an earlier statement queued for an update
   * holds the only copy of that update, and this statement may change the very object: what it holds now is kept, and put back in place
   * if the statement fails. A clean one that is dirty when the statement fails was changed by it and is reloaded.
   */
  void recordObtained(final RID rid, final Record record, final boolean queued) {
    if (unrestorable != null)
      return;
    if (queued) {
      if (queuedInStatement != null && queuedInStatement.contains(rid))
        // queued by this statement, which a failure drops
        return;
      if (snapshotted == null)
        snapshotted = new RidHashSet(16);
      if (snapshotted.add(rid))
        snapshotPending(rid, record);
    } else if (record instanceof MutableEdgeSegment) {
      // A segment the transaction created or read earlier is appended to in place, and a reload would hand it the page's own buffer
      if (snapshotted == null)
        snapshotted = new RidHashSet(16);
      if (snapshotted.add(rid))
        snapshotPending(rid, record);
    } else if (record instanceof MutableDocument document && !document.isDirty()) {
      if (obtained.size() >= limit)
        markUnrestorable("the statement read more than " + limit + " records the transaction wrote");
      else
        obtained.add(record);
    }
  }

  private void snapshotPending(final RID rid, final Record record) {
    final boolean segment = record instanceof MutableEdgeSegment;
    if (!segment && (!(record instanceof MutableDocument document) || (document.getType() instanceof LocalDocumentType type
        && type.hasExternalProperties()))) {
      // The content of an EXTERNAL property lives in a bucket of its own that a snapshot does not hold, and a record of any other
      // kind has no way to be put back
      markUnrestorable("the statement read the record " + rid + ", updated by an earlier statement of the transaction, and its content "
          + "cannot be kept");
      return;
    }
    if (pendingRecords.size() >= limit) {
      markUnrestorable("the statement read more than " + limit + " records updated by earlier statements of the transaction");
      return;
    }
    try {
      final DatabaseInternal database = tx.getDatabase();
      final int index = pendingRecords.size();
      if (index == pendingDirty.length) {
        pendingDirty = Arrays.copyOf(pendingDirty, index << 1);
        pendingAssigned = Arrays.copyOf(pendingAssigned, index << 1);
      }
      if (segment) {
        pendingSnapshots.add(((MutableEdgeSegment) record).snapshotContent());
      } else {
        final MutableDocument document = (MutableDocument) record;
        pendingSnapshots.add(database.getSerializer().serializeForRead(database, document).getNotReusable());
        pendingDirty[index] = document.isDirty();
        pendingAssigned[index] = document.arePropertiesAssigned();
      }
      pendingRecords.add(record);
    } catch (final RuntimeException e) {
      markUnrestorable("the record " + rid + ", updated by an earlier statement of the transaction, cannot be kept (" + e.getMessage() + ")");
    }
  }

  void deletedAdded(final RID rid) {
    add(OP_DELETED_ADDED, rid, null, null, 0, 0);
  }

  void bucketDelta(final int bucketId, final int delta) {
    add(OP_BUCKET_DELTA, null, null, null, bucketId, delta);
  }

  void lightEdgeChanged(final TransactionContext.LightEdgeEntry entry, final int counter, final int amount) {
    add(OP_LIGHT_EDGE, entry, null, null, counter, amount);
  }

  void indexSnapshotChanged(final RID rid, final Document previous) {
    add(OP_INDEX_SNAPSHOT, rid, previous, null, 0, 0);
  }

  void offPageFingerprintChanged(final RID rid, final Long previous) {
    add(OP_OFF_PAGE, rid, previous, null, 0, 0);
  }

  void unidirectionalTargetAdded(final long target) {
    add(OP_UNIDIR_TARGET, null, null, null, target, 0);
  }

  void callbackKeyAdded(final String key) {
    add(OP_CALLBACK_KEY, key, null, null, 0, 0);
  }

  /** The statement moved the corpus counters of a full-text index by these amounts. */
  void corpusDelta(final FullTextCorpusDeltas deltas, final FullTextIndexMetadata metadata, final String indexName,
      final int docs, final long length) {
    add(OP_CORPUS, deltas, metadata, indexName, docs, length);
  }

  void pageCounterChanged(final int fileId, final Integer previous) {
    add(OP_PAGE_COUNTER, null, null, null, fileId, previous == null ? -1 : previous);
  }

  /** The statement brought {@code page} into the transaction: taking it back drops it, and gives back the page it replaced. */
  void pageAdded(final MutablePage page, final boolean isNew, final ImmutablePage pinned) {
    page.setUndoEpoch(epoch);
    add(OP_PAGE_ADDED, page.getPageId(), pinned, null, isNew ? 1 : 0, 0);
  }

  /**
   * The statement is about to write to {@code page}, which the transaction already held. Starts the log of its writes, unless this
   * statement did already.
   */
  void touchPage(final MutablePage page) {
    // One log per page the statement re-touches, taken from the pool below and handed back when the statement ends: a transaction that
    // keeps running statements allocates them once, up to the most pages one statement re-touched (and no more than MAX_RETAINED_LOGS)
    if (page.getUndoEpoch() == epoch)
      return;
    page.setUndoEpoch(epoch);
    if (unrestorable != null)
      return;
    if (logCount == logs.length)
      logs = Arrays.copyOf(logs, logCount << 1);
    PageUndoLog log = logs[logCount];
    if (log == null)
      logs[logCount] = log = new PageUndoLog();
    ++logCount;
    log.begin(page, this);
  }

  /**
   * One granule more for a page log, or false once the statement logged {@link #limit} of them: a bulk rewrite of the pages the
   * transaction modified must not copy them all, and a statement past the budget cannot be taken back.
   */
  @Override
  public boolean takeGrain() {
    if (grainBudget == 0) {
      markUnrestorable("the statement overwrote more than " + limit + " granules of the pages the transaction had modified");
      return false;
    }
    --grainBudget;
    return true;
  }

  // ------------------------------------------------------------------------------------------------------------------------------
  // ROLLBACK
  // ------------------------------------------------------------------------------------------------------------------------------

  /**
   * Puts the transaction back as it was when the statement began.
   *
   * @return true when the statement was taken back, false when it cannot be, in which case nothing was changed
   */
  boolean rollback() {
    if (!active || rolledBack || unrestorable != null)
      return false;
    // Each refusal says why: it is the text of the error the commit throws
    if (tx.getStatus() != TransactionContext.STATUS.BEGUN || tx.getBeginSequence() != beginSequence) {
      markUnrestorable("the transaction the statement ran in ended while it was running");
      return false;
    }
    if (!tx.getIndexChanges().isStatementJournalIntact()) {
      markUnrestorable("the statement queued more than " + limit + " index entries, or an index lane it queued to was dropped or replaced");
      return false;
    }

    // A statement cancelled by an interrupt arrives here with the flag set, and the reload of a record reads its file: a channel read
    // by a thread with the flag set is closed under it for every thread of the database (issue #10521). Cleared for the undo, handed back
    final boolean interrupted = Thread.interrupted();
    try {
      undo();
      rolledBack = true;
      return true;
    } catch (final RuntimeException | Error e) {
      // The undo stopped halfway, so the transaction is neither what the statement found nor what it left: it cannot be published
      LogManager.instance().log(this, Level.SEVERE, "Cannot take back the failed statement: the transaction is marked rollback-only", e);
      tx.setRollbackOnly("the failed statement could not be taken back (" + e.getClass().getSimpleName() + ")");
      rolledBack = true;
      return true;
    } finally {
      if (interrupted)
        Thread.currentThread().interrupt();
    }
  }

  private void undo() {
    // THE LANES FIRST: they hold the records' index entries, which refer to nothing else of the statement
    tx.getIndexChanges().undoStatementChanges(indexJournalMark);

    // THE PAGES THE TRANSACTION ALREADY HELD get their bytes back
    for (int i = 0; i < logCount; i++) {
      final PageUndoLog log = logs[i];
      final MutablePage page = log.getPage();
      log.restore();
      tx.poisonPageMerges(page.getPageId());
    }

    // THE JOURNAL, newest first, so a key changed twice ends with the value it had before the statement
    final List<Record> mutatedInPlace = new ArrayList<>();
    RidHashSet deleted = null;
    List<Long> unidirectional = null;
    for (int i = size - 1; i >= 0; i--) {
      switch (ops[i]) {
      case OP_MODIFIED_CACHE -> {
        final Record previous = (Record) b[i];
        tx.restoreModifiedRecord((RID) a[i], previous);
        if (previous != null && previous == c[i])
          mutatedInPlace.add(previous);
      }
      case OP_UPDATED -> tx.restoreUpdatedRecord((RID) a[i], (Record) b[i]);
      case OP_DELETED_ADDED -> {
        if (deleted == null)
          deleted = new RidHashSet(16);
        deleted.add((RID) a[i]);
      }
      case OP_BUCKET_DELTA -> tx.updateBucketRecordDelta((int) n[i], -(int) m[i]);
      case OP_LIGHT_EDGE -> tx.restoreLightEdgeCounter((TransactionContext.LightEdgeEntry) a[i], (int) n[i], (int) m[i]);
      case OP_INDEX_SNAPSHOT -> tx.restoreIndexSnapshot((RID) a[i], (Document) b[i]);
      case OP_OFF_PAGE -> tx.restoreOffPageFingerprint((RID) a[i], (Long) b[i]);
      case OP_UNIDIR_TARGET -> {
        if (unidirectional == null)
          unidirectional = new ArrayList<>();
        unidirectional.add(n[i]);
      }
      case OP_PAGE_ADDED -> {
        final PageId pageId = (PageId) a[i];
        tx.restoreAddedPage(pageId, n[i] == 1, (ImmutablePage) b[i]);
        tx.poisonPageMerges(pageId);
      }
      case OP_PAGE_COUNTER -> tx.restorePageCounter((int) n[i], m[i] < 0 ? null : (int) m[i]);
      case OP_CALLBACK_KEY -> tx.restoreCallbackKey((String) a[i]);
      case OP_CORPUS -> ((FullTextCorpusDeltas) a[i]).retract((FullTextIndexMetadata) b[i], (String) c[i], (int) n[i], m[i]);
      default -> throw new IllegalStateException("Unknown savepoint operation " + ops[i]);
      }
    }
    if (deleted != null)
      tx.restoreDeletedRecords(deleted);
    if (unidirectional != null)
      tx.restoreUnidirectionalTargets(unidirectional);

    tx.restoreAppendOnlyState(newRecordsMark, reservationMark, callbackMark, completedRecordWrites, unidirectionalSequence);

    // A record an earlier statement updated and this one changed in place gets back the content it had: the object is the only copy
    for (int i = 0; i < pendingRecords.size(); i++) {
      final Record record = pendingRecords.get(i);
      if (record instanceof MutableEdgeSegment segment)
        segment.restoreContent(pendingSnapshots.get(i));
      else
        ((MutableDocument) record).restoreContent(pendingSnapshots.get(i), pendingDirty[i], pendingAssigned[i]);
    }

    // A record the statement changed in place, or read clean and left dirty, holds content that never reached a page
    for (final Record record : mutatedInPlace)
      if (!isPending(record))
        reloadQuietly(record);
    for (final Record record : obtained)
      if (record instanceof MutableDocument document && document.isDirty())
        reloadQuietly(record);
  }

  private boolean isPending(final Record record) {
    for (int i = 0; i < pendingRecords.size(); i++)
      if (pendingRecords.get(i) == record)
        return true;
    return false;
  }

  private void reloadQuietly(final Record record) {
    try {
      record.reload();
    } catch (final RuntimeException e) {
      // THE RECORD IS GONE (it was created by the statement, or its type was removed): nothing to give back to the object
    }
  }

  // ------------------------------------------------------------------------------------------------------------------------------
  // END
  // ------------------------------------------------------------------------------------------------------------------------------

  /** Releases everything the statement journaled; the object is kept for the next statement of the transaction. */
  void end() {
    if (!active)
      return;
    active = false;
    for (int i = 0; i < logCount; i++)
      logs[i].end();
    logCount = 0;
    // A statement that rewrote thousands of pages must not leave a log for each of them behind on a long-lived transaction context
    if (logs.length > MAX_RETAINED_LOGS)
      logs = new PageUndoLog[8];
    tx.getIndexChanges().endStatementJournal();

    Arrays.fill(a, 0, size, null);
    Arrays.fill(b, 0, size, null);
    Arrays.fill(c, 0, size, null);
    size = 0;
    if (ops.length > MAX_RETAINED_JOURNAL) {
      ops = new byte[32];
      a = new Object[32];
      b = new Object[32];
      c = new Object[32];
      n = new long[32];
      m = new long[32];
    }
    queuedInStatement = null;
    snapshotted = null;
    obtained.clear();
    pendingRecords.clear();
    pendingSnapshots.clear();
  }
}
