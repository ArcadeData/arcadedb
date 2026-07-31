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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.MutableEdgeSegment;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.utility.IntHashSet;
import com.arcadedb.utility.IntIntHashMap;
import com.arcadedb.utility.LongHashSet;
import com.arcadedb.utility.RidHashSet;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.stream.Collectors;

/**
 * Manage the transaction context. When the transaction begins, the modifiedPages map is initialized. This allows to always delegate
 * to the transaction context, even if there is no active transaction by ignoring tx data.
 * <br>
 * At commit time, the files are locked in order (to avoid deadlocks) and to allow parallel commit on different files.
 * <br>
 * Format of WAL:
 * <br>
 * txId:long|pages:int|&lt;segmentSize:int|fileId:int|pageNumber:long|pageModifiedFrom:int|pageModifiedTo:int|&lt;prevContent&gt;&lt;newContent&gt;segmentSize:int&gt;MagicNumber:long
 */
public class TransactionContext implements Transaction {
  // Bounded internal retry budget for the snapshot-vs-lock race against compaction-induced file migration during
  // commit lock acquisition (see lockFilesInOrder). Each retry only re-resolves file ids and re-acquires locks (no
  // rollback, no page reload), so it converges in microseconds. Sized for the worst legitimate case of concurrent
  // compactions across all indexes of a multi-indexed type; exhausting it signals a real concurrency problem.
  private static final int                           MAX_LOCK_MIGRATION_RETRIES = 10;

  private final DatabaseInternal                     database;
  private final Map<Integer, Integer>                newPageCounters       = new ConcurrentHashMap<>();
  // Per-tx record-count delta per bucket. Single-threaded (HashMap was used, not ConcurrentHashMap),
  // so AtomicInteger was only a mutable-cell trick. IntIntHashMap.add(key, delta) covers it directly.
  private final IntIntHashMap                        bucketRecordDelta     = new IntIntHashMap();
  private final Map<RID, Record>                     immutableRecordsCache = new HashMap<>(1024);
  private final Map<RID, Record>                     modifiedRecordsCache  = new HashMap<>(1024);
  // Records created in this transaction (they got an optimistically-assigned RID at creation time). On rollback that
  // RID no longer exists, so the identity is reset to provisional (null) letting the same in-memory object be cleanly
  // re-inserted in a later transaction instead of being treated as an update of a missing record (issue #4562).
  private final List<Record>                         newRecords            = new ArrayList<>();
  private final TransactionIndexContext              indexChanges;
  private final Map<PageId, ImmutablePage>           immutablePages        = new HashMap<>(64);
  private final RidHashSet                            deletedRecordsInTx    = new RidHashSet();
  private       Map<PageId, MutablePage>             modifiedPages;
  private       Map<PageId, MutablePage>             newPages;
  // GRAPH_EDGE_APPEND_MERGE (super-node write contention): appends to an edge-list chunk commute, so a
  // commit-time page-version conflict whose ONLY cause is concurrent in-chunk edge appends can be resolved by
  // re-applying THIS transaction's appends on top of the newer committed page, instead of failing the whole
  // transaction with a ConcurrentModificationException and retrying. Tracked per SEGMENT rid with the appended
  // pairs packed into primitive arrays (see EdgeAppendBuffer) so the append hot path allocates nothing per
  // edge; segment page ids are resolved lazily only on the rare commit conflict. A page stays eligible only
  // while EVERY modification this transaction made to it is a tracked append; the first non-append write to it
  // (edge removal, new-chunk allocation, bulk addAll) poisons its page so it can never be rebased.
  private       Map<RID, EdgeAppendBuffer>           edgeAppendsBySegment;
  // Poisoned edge-list pages, keyed by a packed (fileId, pageNumber) long so poisoning and the hot-path skip
  // check allocate nothing (no PageId objects).
  private       LongHashSet                          edgeAppendPoisonedPages;
  private       boolean                              edgeAppendMerge;
  // TX_PAGE_SLOT_MERGE (#5381): the general form of the edge-append merge. Two transactions writing DIFFERENT record
  // slots on the same bucket page (logically-unrelated records that merely share a page) conflict at page
  // granularity even though their changes commute. On such a commit-time conflict we re-apply THIS transaction's
  // slot writes on top of the newer committed page instead of failing the whole transaction. Tracked per page
  // (packed fileId+pageNumber key): for each written slot we keep the final serialized body, plus - for an
  // UPDATE - the pre-image, so the rebase can tell a false page conflict (a concurrent write to another
  // slot) from a true one (a concurrent write to the SAME record). A page stays eligible only while every change
  // this transaction made to it is a tracked disjoint-slot insert or an update that stayed INSIDE the page (an
  // overwrite, or a growth the page could host by shifting the records that follow - #5279); the first
  // non-rebasable write to it (delete, multi-page/placeholder record, a record spilled out of the page) poisons it.
  private       Map<Long, SlotRebaseBuffer>          slotRebaseByPage;
  private       LongHashSet                          slotRebasePoisonedPages;
  private       boolean                              slotMerge;
  // #5279: the insert slots this transaction claimed on EXISTING bucket pages, so concurrent inserts into the same
  // page get different slots (hence different RIDs) and commute instead of conflicting. Held in parallel primitive
  // arrays - grown by doubling and reused across transactions - so inserting N records allocates nothing per record,
  // and released as a block when the transaction ends (see releaseInsertSlotReservations).
  private       LocalBucket[]                        insertReservationBuckets;
  private       int[]                                insertReservationPages;
  private       int[]                                insertReservationSlots;
  private       int                                  insertReservationCount;
  // Per-transaction soft cap on the bytes retained for the slot merge and the running total: once exceeded the
  // merge is disabled for the rest of the transaction so heap stays bounded on a very large transaction.
  private       long                                 slotMergeMaxBytes;
  private       long                                 slotRebaseTrackedBytes;
  private       boolean                              useWAL;
  // #5064: set by the HA layer AFTER the replication quorum durably committed this transaction and BEFORE
  // the local phase-2 apply. Shifts the durability boundary for the failure regimes in commit2ndPhase's
  // finally: a local failure past this point must never roll back user-held record identities (the cluster
  // committed them - a retry would duplicate) nor fence the database (no orphaned local WAL record exists;
  // the Raft layer reconciles pages from the replicated payload).
  private       boolean                              remotelyCommitted;
  private       boolean                              asyncFlush            = true;
  private       WALFile.FlushType                    walFlush;
  private       List<Integer>                        lockedFiles;
  private       List<Integer>                        explicitLockedFiles   = null;
  private       long                                 txId                  = -1;
  private       STATUS                               status                = STATUS.INACTIVE;
  // Whether the 1st phase in progress ends by replaying the queued index operations (leader only). See
  // isIndexChangesReplayed().
  private       boolean                              indexChangesReplayed  = true;
  // KEEPS TRACK OF MODIFIED RECORD IN TX. AT 1ST PHASE COMMIT TIME THE RECORD ARE SERIALIZED AND INDEXES UPDATED. THIS DEFERRING IMPROVES SPEED ESPECIALLY
  // WITH GRAPHS WHERE EDGES ARE CREATED AND CHUNKS ARE UPDATED MULTIPLE TIMES IN THE SAME TX
  // TODO: OPTIMIZE modifiedRecordsCache STRUCTURE, MAYBE JOIN IT WITH UPDATED RECORDS?
  private       Map<RID, Record>                     updatedRecords              = null;
  // #4935: snapshot of the last in-transaction indexed state per RID (indexed property values only). When the
  // same record is updated more than once inside one transaction, the second+ update must diff its index change
  // against the previous in-transaction value, not the committed buffer (which stays frozen until commit because
  // serialization is deferred). Without this, every intermediate value leaks a phantom index entry.
  // Holds one small entry (indexed values only) per distinct CHANGED-index RID; entries are dropped on
  // delete and the map is released on reset(), so memory scales with updatedRecords, never beyond it.
  private       Map<RID, Document>                   updatedRecordsIndexSnapshot = null;
  private       Database.TRANSACTION_ISOLATION_LEVEL isolationLevel              = Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED;
  private       LocalTransactionExplicitLock         explicitLock;
  // INVARIANT (#4941/#4959): this field is written by exactly two paths. (1) setRequester(), the explicit
  // protocol-session identity, which always wins. (2) captureRequester(), the lazy owner-thread capture,
  // called ONLY from the lock-ACQUISITION paths (lockFilesInOrder's tryLockFiles, LocalDatabase's
  // executeLockingFiles) and therefore always on the owning thread BEFORE any lock exists. Release paths
  // (reset, kill, the commit unlocks) must use the PURE getRequester() and must never write the field: a
  // capture on a non-owner thread would key later lock operations inconsistently and leak the file locks
  // forever (#4941). The check-then-set in captureRequester() is not atomic and is safe only under this
  // single-writer discipline.
  // volatile (#5060): the DEAD-owner release path gets its happens-before from
  // isAlive()==false (JLS 17.4.4), but closeInternal can roll back a LIVE owner's context, where no such
  // edge exists - a plain read could see a stale null, re-capture the closing thread and key the unlock
  // wrong, leaking the very locks #4941 fixes. The volatile read extends the guarantee to live owners
  // whose lazy capture ran at lock-acquisition time (well before the close).
  private volatile Object                             requester;
  private       List<Runnable>                       afterCommitCallbacks        = null;
  private       Set<String>                          registeredCallbackKeys      = null;

  // #5279: above this many tracked insert reservations the parallel arrays are dropped instead of being reused, so a
  // one-off bulk insert does not keep its (per-record) tracking arrays alive on a long-lived transaction context.
  private static final int MAX_RETAINED_INSERT_RESERVATIONS = 8192;

  public enum STATUS {INACTIVE, BEGUN, COMMIT_1ST_PHASE, COMMIT_2ND_PHASE}

  public static class TransactionPhase1 {
    public final Binary            result;
    public final List<MutablePage> modifiedPages;

    public TransactionPhase1(final Binary result, final List<MutablePage> modifiedPages) {
      this.result = result;
      this.modifiedPages = modifiedPages;
    }
  }

  public TransactionContext(final DatabaseInternal database) {
    this.database = database;
    this.walFlush = WALFile.getWALFlushType(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FLUSH));
    this.useWAL = database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL);
    this.indexChanges = new TransactionIndexContext(database);
  }

  @Override
  public void begin(final Database.TRANSACTION_ISOLATION_LEVEL isolationLevel) {
    // #5064: the base context is REUSED across transactions on this thread - the regime flag must never
    // leak from a previous (Raft-committed) transaction into a fresh one, or a plain local failure would
    // silently skip the #4940 rollback. Cleared here AND in reset() (belt and braces).
    remotelyCommitted = false;
    this.isolationLevel = isolationLevel;

    if (status != STATUS.INACTIVE)
      throw new TransactionException("Transaction already begun");

    status = STATUS.BEGUN;

    // Read once per transaction (DATABASE-scope, constant for the DB lifetime): keeps the per-append hot path
    // to a plain field read instead of a configuration lookup.
    edgeAppendMerge = database.getConfiguration().getValueAsBoolean(GlobalConfiguration.GRAPH_EDGE_APPEND_MERGE);
    slotMerge = database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_PAGE_SLOT_MERGE);
    slotMergeMaxBytes = database.getConfiguration().getValueAsLong(GlobalConfiguration.TX_PAGE_SLOT_MERGE_MAX_BYTES);
    slotRebaseTrackedBytes = 0;

    // Optimized: initial capacity 32 for typical transaction page count
    modifiedPages = new HashMap<>(32);

    if (newPages == null)
      // KEEP ORDERING IN CASE MULTIPLE PAGES FOR THE SAME FILE ARE CREATED
      // Optimized: initial capacity 16 for typical new pages per transaction
    newPages = new LinkedHashMap<>(16);
  }

  @Override
  public Binary commit() {
    if (status == STATUS.INACTIVE)
      throw new TransactionException("Transaction not begun");

    if (status != STATUS.BEGUN)
      throw new TransactionException("Transaction already in commit phase");

    final TransactionPhase1 phase1 = commit1stPhase(true);
    if (phase1 != null) {
      commit2ndPhase(phase1);
    } else
      resetAndFireCallbacks();

    if (database.getSchema().getEmbedded().isDirty())
      database.getSchema().getEmbedded().saveConfiguration();

    return phase1 != null ? phase1.result : null;
  }

  public LocalTransactionExplicitLock lock() {
    if (explicitLock == null)
      explicitLock = new LocalTransactionExplicitLock(this);
    return explicitLock;
  }

  public Database.TRANSACTION_ISOLATION_LEVEL getIsolationLevel() {
    return isolationLevel;
  }

  /** #5064: see the {@code remotelyCommitted} field - HA-layer only, call after quorum commit, before phase 2. */
  public void setRemotelyCommitted(final boolean remotelyCommitted) {
    this.remotelyCommitted = remotelyCommitted;
  }

  public void setRequester(final Object requester) {
    this.requester = requester;
  }

  /**
   * Returns the transaction's own WRITTEN copy of a record - a deferred update ({@code updatedRecords}) or the
   * mutable working copy ({@code modifiedRecordsCache}) - or null when this transaction has not written it.
   * Unlike {@link #getRecordFromCache(RID)} it NEVER returns a read-only cached record: callers that need to
   * re-read their own deferred writes (updates are applied to pages only at commit) use this so a stale
   * read-only copy can never masquerade as the current state and silently roll back a concurrent change.
   */
  public Record getWrittenRecord(final RID rid) {
    Record rec = updatedRecords != null ? updatedRecords.get(rid) : null;
    if (rec == null)
      rec = modifiedRecordsCache.get(rid);
    return rec;
  }

  public Record getRecordFromCache(final RID rid) {
    Record rec = modifiedRecordsCache.get(rid);
    if (rec == null)
      rec = immutableRecordsCache.get(rid);
    if (rec == null && updatedRecords != null)
      // IN CASE `READ-YOUR-WRITE` IS FALSE, THE MODIFIED RECORD IS NOT IN CACHE AND MUST BE READ FROM THE UPDATE RECORDS
      rec = updatedRecords.get(rid);
    return rec;
  }

  /**
   * Tracks a record created in the current transaction so that, if the transaction is rolled back, its
   * optimistically-assigned identity can be reset to provisional (see {@link #rollback()}). Only records whose
   * {@code save()} decides create-vs-update on the presence of a RID (i.e. {@link MutableDocument} and its subtypes)
   * need this; internal records such as edge segments are never re-saved by user code.
   */
  public void registerNewRecord(final Record record) {
    newRecords.add(record);
  }

  public void updateRecordInCache(final Record record) {
    if (database.isReadYourWrites()) {
      final RID rid = record.getIdentity();
      if (rid == null)
        throw new IllegalArgumentException("Cannot update record in TX cache because it is not persistent: " + record);

      if (record instanceof RecordInternal)
        modifiedRecordsCache.put(rid, record);
      else
        immutableRecordsCache.put(rid, record);
    }
  }

  public void removeImmutableRecordsOfSamePage(final RID rid) {
    final int bucketId = rid.getBucketId();
    final long pos = rid.getPosition();

    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(bucketId);

    final long pageNum = pos / bucket.getMaxRecordsInPage();

    // FOR IMMUTABLE RECORDS AVOID THAT THEY ARE POINTING TO THE OLD OFFSET IN A MODIFIED PAGE
    immutableRecordsCache.values().removeIf(
        r -> r.getIdentity().getBucketId() == bucketId && r.getIdentity().getPosition() / bucket.getMaxRecordsInPage() == pageNum);
  }

  public void removeRecordFromCache(final RID rid) {
    if (updatedRecords != null)
      updatedRecords.remove(rid);

    if (database.isReadYourWrites()) {
      if (rid == null)
        throw new IllegalArgumentException("Cannot remove record in TX cache because it is not persistent");
      modifiedRecordsCache.remove(rid);
      immutableRecordsCache.remove(rid);
    }

    removeImmutableRecordsOfSamePage(rid);
  }

  public DatabaseInternal getDatabase() {
    return database;
  }

  @Override
  public void setUseWAL(final boolean useWAL) {
    this.useWAL = useWAL;
  }

  @Override
  public void setWALFlush(final WALFile.FlushType flush) {
    this.walFlush = flush;
  }

  @Override
  public boolean isUseWAL() {
    return useWAL;
  }

  @Override
  public WALFile.FlushType getWALFlush() {
    return walFlush;
  }

  @Override
  public void rollback() {
    LogManager.instance()
        .log(this, Level.FINE, "Rollback transaction newPages=%s modifiedPages=%s (threadId=%d)", newPages, modifiedPages,
            Thread.currentThread().threadId());

    if (database.isOpen() && database.getSchema().getDictionary() != null) {
      if (modifiedPages != null) {
        final int dictionaryId = database.getSchema().getDictionary().getFileId();

        for (final PageId pageId : modifiedPages.keySet()) {
          if (dictionaryId == pageId.getFileId()) {
            // RELOAD THE DICTIONARY
            try {
              database.getSchema().getDictionary().reload();
            } catch (final IOException e) {
              throw new SchemaException("Error on reloading schema dictionary");
            }
            break;
          }
        }
      }
    }
    modifiedPages = null;
    newPages = null;
    updatedRecords = null;
    updatedRecordsIndexSnapshot = null;

    // RECORDS CREATED IN THIS TX HAVE NO COMMITTED VERSION TO RELOAD TO: PULL THEM OUT OF THE MODIFIED-RECORDS CACHE SO
    // THE RELOAD LOOP BELOW LEAVES THEIR IN-MEMORY CONTENT INTACT (reload() WOULD WIPE map/buffer), THEN RESET THEIR
    // IDENTITY TO PROVISIONAL SO THE SAME OBJECT CAN BE CLEANLY RE-INSERTED INSTEAD OF UPDATING A MISSING RECORD (#4562).
    for (final Record r : newRecords) {
      final RID rid = r.getIdentity();
      if (rid != null)
        modifiedRecordsCache.remove(rid);
    }

    // RELOAD PREVIOUS VERSION OF MODIFIED RECORDS
    if (database.isOpen())
      for (final Record r : modifiedRecordsCache.values())
        try {
          r.reload();
        } catch (final Exception e) {
          // IGNORE EXCEPTION (RECORD DELETED OR TYPE REMOVED)
        }

    for (final Record r : newRecords)
      ((RecordInternal) r).setIdentity(null);

    reset();
  }

  /**
   * Registers a callback to be executed after a successful commit. Callbacks fire in registration order.
   * If the transaction is rolled back, callbacks are discarded without firing.
   * Callback exceptions are logged but do not affect the commit or other callbacks.
   */
  public void addAfterCommitCallback(final Runnable callback) {
    if (afterCommitCallbacks == null)
      afterCommitCallbacks = new ArrayList<>();
    afterCommitCallbacks.add(callback);
  }

  /**
   * Registers a post-commit callback only if no callback with the given key has been registered yet
   * in this transaction. Returns true if the callback was registered, false if skipped.
   */
  public boolean addAfterCommitCallbackIfAbsent(final String key, final Runnable callback) {
    if (registeredCallbackKeys == null)
      registeredCallbackKeys = new HashSet<>();
    if (!registeredCallbackKeys.add(key))
      return false;
    addAfterCommitCallback(callback);
    return true;
  }

  public boolean hasCallbackKey(final String key) {
    return registeredCallbackKeys != null && registeredCallbackKeys.contains(key);
  }

  private void resetAndFireCallbacks() {
    final List<Runnable> callbacks = afterCommitCallbacks;
    reset();
    if (callbacks != null) {
      for (final Runnable callback : callbacks) {
        try {
          callback.run();
        } catch (final Exception e) {
          LogManager.instance().log(this, Level.WARNING, "Error in post-commit callback: %s", e, e.getMessage());
        }
      }
    }
  }

  public void assureIsActive() {
    if (!isActive())
      throw new TransactionException("Transaction not begun");
  }

  public void addUpdatedRecord(final Record record) throws IOException {
    final RID rid = record.getIdentity();

    if (updatedRecords == null)
      updatedRecords = new HashMap<>();
    if (updatedRecords.put(record.getIdentity(), record) == null)
      ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).fetchPageInTransaction(rid);
    updateRecordInCache(record);
    removeImmutableRecordsOfSamePage(record.getIdentity());
  }

  /**
   * Returns the snapshot of the last in-transaction indexed state for the given record, or {@code null} if the
   * record has not yet been updated in this transaction. Used by {@code updateRecord} so that a second (or later)
   * update of the same record diffs its index change against the previous in-transaction value rather than the
   * committed buffer (issue #4935).
   */
  public Document getLastIndexedSnapshot(final RID rid) {
    return updatedRecordsIndexSnapshot == null ? null : updatedRecordsIndexSnapshot.get(rid);
  }

  public void setLastIndexedSnapshot(final RID rid, final Document snapshot) {
    if (updatedRecordsIndexSnapshot == null)
      updatedRecordsIndexSnapshot = new HashMap<>();
    updatedRecordsIndexSnapshot.put(rid, snapshot);
  }

  /**
   * Used to determine if a page has been already loaded. This is important for isolation.
   */
  public boolean hasPageForRecord(final PageId pageId) {
    if (modifiedPages != null)
      if (modifiedPages.containsKey(pageId))
        return true;

    if (newPages != null)
      if (newPages.containsKey(pageId))
        return true;

    return immutablePages.containsKey(pageId);
  }

  /**
   * Looks for the page in the TX context first, then delegates to the database.
   */
  public BasePage getPage(final PageId pageId, final int size) throws IOException {
    BasePage page = null;

    if (modifiedPages != null)
      page = modifiedPages.get(pageId);

    if (page == null && newPages != null)
      page = newPages.get(pageId);

    if (page == null)
      page = immutablePages.get(pageId);

    if (page == null) {
      // NOT FOUND, DELEGATES TO THE DATABASE
      page = database.getPageManager().getImmutablePage(pageId, size, false, true);

      if (page != null) {
        switch (isolationLevel) {
        case READ_COMMITTED:
          break;
        case REPEATABLE_READ:
          final PaginatedComponentFile file = (PaginatedComponentFile) database.getFileManager().getFile(pageId.getFileId());
          final boolean isNewPage = pageId.getPageNumber() >= file.getTotalPages();
          if (!isNewPage)
            // CACHE THE IMMUTABLE PAGE ONLY IF IT IS NOT NEW
            immutablePages.put(pageId, (ImmutablePage) page);
          break;
        }
      }
    }

    return page;
  }

  /**
   * If the page is not already in transaction tx, loads from the database and clone it locally.
   */
  public MutablePage getPageToModify(final PageId pageId, final int pageSize, final boolean isNew) throws IOException {
    if (!isActive())
      throw new TransactionException("Transaction not active");

    MutablePage page = modifiedPages != null ? modifiedPages.get(pageId) : null;
    if (page == null) {
      if (newPages != null)
        page = newPages.get(pageId);

      if (page == null) {
        // IF AVAILABLE REMOVE THE PAGE FROM IMMUTABLE PAGES TO KEEP ONLY ONE PAGE IN RAM
        final ImmutablePage loadedPage = immutablePages.remove(pageId);
        if (loadedPage == null)
          // NOT FOUND, DELEGATES TO THE DATABASE
          page = database.getPageManager().getMutablePage(pageId, pageSize, isNew, true);
        else
          page = loadedPage.modify();

        if (isNew)
          newPages.put(pageId, page);
        else
          modifiedPages.put(pageId, page);
      }
    }
    return page;
  }

  /**
   * Puts the page in the TX modified pages.
   */
  public MutablePage getPageToModify(final BasePage page) throws IOException {
    if (!isActive())
      throw new TransactionException("Transaction not active");

    final MutablePage mutablePage = page.modify();

    final PageId pageId = page.getPageId();
    if (newPages.containsKey(pageId))
      newPages.put(pageId, mutablePage);
    else
      modifiedPages.put(pageId, mutablePage);

    immutablePages.remove(pageId);

    return mutablePage;
  }

  public MutablePage addPage(final PageId pageId, final int pageSize) {
    assureIsActive();

    // CREATE A PAGE ID BASED ON NEW PAGES IN TX. IN CASE OF ROLLBACK THEY ARE SIMPLY REMOVED AND THE GLOBAL PAGE COUNT IS UNCHANGED
    final MutablePage page = new MutablePage(pageId, pageSize);
    newPages.put(pageId, page);

    final Integer indexCounter = newPageCounters.get(pageId.getFileId());
    if (indexCounter == null || indexCounter < pageId.getPageNumber() + 1)
      newPageCounters.put(pageId.getFileId(), pageId.getPageNumber() + 1);

    return page;
  }

  public Integer getPageCounter(final int indexFileId) {
    return newPageCounters.get(indexFileId);
  }

  /**
   * Allocation-light store of the in-chunk appends made to ONE edge segment in this transaction: the (edge,
   * vertex) RID pairs are packed into growable primitive arrays instead of one object per appended edge, so a
   * hot vertex receiving thousands of edges costs a handful of array growths, not thousands of allocations.
   * The RID objects are rebuilt only at rebase time (on the rare commit conflict). See {@link
   * #edgeAppendsBySegment}.
   */
  private static final class EdgeAppendBuffer {
    private int[]  edgeBucket   = new int[8];
    private long[] edgePosition = new long[8];
    private int[]  vertexBucket = new int[8];
    private long[] vertexPosition = new long[8];
    private int    size;

    void add(final RID edge, final RID vertex) {
      if (size == edgeBucket.length) {
        final int n = size << 1;
        edgeBucket = Arrays.copyOf(edgeBucket, n);
        edgePosition = Arrays.copyOf(edgePosition, n);
        vertexBucket = Arrays.copyOf(vertexBucket, n);
        vertexPosition = Arrays.copyOf(vertexPosition, n);
      }
      edgeBucket[size] = edge.getBucketId();
      edgePosition[size] = edge.getPosition();
      vertexBucket[size] = vertex.getBucketId();
      vertexPosition[size] = vertex.getPosition();
      ++size;
    }
  }

  /**
   * Tells whether the commutative edge-append merge is enabled for this transaction.
   */
  public boolean isEdgeAppendMergeEnabled() {
    return edgeAppendMerge;
  }

  /**
   * Records an in-place edge append (from {@link com.arcadedb.graph.EdgeLinkedList#add}) as rebasable: if the
   * segment's page loses the commit-time version check because another transaction appended to the same head
   * chunk, the append can be replayed on top of the newer page instead of failing the whole transaction.
   * <p>
   * Hot path: keyed by SEGMENT rid (a super-node is one segment no matter how many edges land on it), the pair
   * is packed into primitive arrays, and the segment's {@link PageId} is NOT computed here - it is resolved
   * lazily only when a page actually conflicts at commit (rare). So the common no-contention append costs a
   * map lookup on an existing key, nothing allocated per edge. No-op when the feature is off.
   */
  public void trackEdgeAppend(final RID segmentRID, final RID edgeRID, final RID vertexRID) {
    if (!edgeAppendMerge)
      return;

    // Skip pages already poisoned (a brand-new chunk - e.g. a just-created source vertex's edge list - poisons
    // its own page at createRecord). Those can never be rebased, so tracking them would only waste a buffer.
    // The page key is packed arithmetic, no allocation.
    if (edgeAppendPoisonedPages != null && edgeAppendPoisonedPages.contains(edgeSegmentPageKey(segmentRID)))
      return;

    if (edgeAppendsBySegment == null)
      edgeAppendsBySegment = new HashMap<>();

    EdgeAppendBuffer buffer = edgeAppendsBySegment.get(segmentRID);
    if (buffer == null)
      edgeAppendsBySegment.put(segmentRID, buffer = new EdgeAppendBuffer());
    buffer.add(edgeRID, vertexRID);
  }

  /**
   * Marks the edge-segment page holding {@code segmentRID} as NOT rebasable: this transaction changed it in a
   * way that does not commute with a concurrent append (edge removal, new-chunk allocation, bulk load). Tracked
   * appends on that page are ignored at commit (the conflict check below excludes poisoned pages), so a rebase
   * can never silently re-derive the page from committed-state + appends. No-op when the feature is off.
   * <p>
   * Since #5596 this is the fast, precise opt-OUT, no longer the only line of defence: the merge additionally
   * requires the page to PROVE that every byte this transaction wrote there was declared replayable by it (see
   * {@link MutablePage#beginCoveredWrite(int)}), so a forgotten call here costs a retry, not a lost write. That
   * does NOT make the call redundant - see {@link #poisonSlotRebasePage(int, int)} for the #5608 audit of what
   * poisoning still buys over the proof (skipped tracking work, and a clean {@code mergesDeclinedByCoverage}).
   */
  public void poisonEdgeAppendPage(final RID segmentRID) {
    if (!edgeAppendMerge)
      return;

    if (edgeAppendPoisonedPages == null)
      edgeAppendPoisonedPages = new LongHashSet();
    edgeAppendPoisonedPages.add(edgeSegmentPageKey(segmentRID));
  }

  /**
   * Same as {@link #poisonEdgeAppendPage(RID)} for a caller that already knows the (fileId, pageNumber) of the page,
   * so no bucket lookup is needed. Used by the delete path: freeing a slot is not an edge append, and the edge merge
   * re-derives the page from the committed image plus the tracked appends ALONE - so a delete left on a page that
   * also received a tracked append would simply vanish at rebase time.
   */
  public void poisonEdgeAppendPage(final int fileId, final int pageNumber) {
    if (!edgeAppendMerge)
      return;

    if (edgeAppendPoisonedPages == null)
      edgeAppendPoisonedPages = new LongHashSet();
    edgeAppendPoisonedPages.add(packPageKey(fileId, pageNumber));
  }

  /** Packs the (fileId, pageNumber) of the page holding {@code segmentRID} into a long key (no allocation). */
  private long edgeSegmentPageKey(final RID segmentRID) {
    // #5608: the null-tolerant two-arg lookup, deliberately. The single-arg getBucketById raises SchemaException for an
    // unknown id (and for a component that is not a bucket) BEFORE it can return null, which made the branch below
    // dead code and silently defeated the retryable-conflict contract it states: the SchemaException that escaped
    // instead is not a NeedRetryException, so a commit reaching it failed hard where it was meant to retry. Asking for
    // the null the branch was written against makes the contract true again.
    final LocalBucket bucket = database.getSchema().getEmbedded().getBucketById(segmentRID.getBucketId(), false);
    if (bucket == null)
      // The edge bucket cannot vanish under the held commit lock; treat a missing one as a retryable conflict
      // rather than NPE.
      throw new ConcurrentModificationException("Edge-append: bucket " + segmentRID.getBucketId()
          + " for segment " + segmentRID + " not found. Please retry the operation");
    final int pageNumber = (int) (segmentRID.getPosition() / bucket.getMaxRecordsInPage());
    return packPageKey(segmentRID.getBucketId(), pageNumber);
  }

  private static long packPageKey(final int fileId, final int pageNumber) {
    return ((long) fileId << 32) | (pageNumber & 0xFFFFFFFFL);
  }

  /**
   * True when an in-chunk append to {@code segmentRID} was registered by {@link #trackEdgeAppend} in this
   * transaction, i.e. the commit-time edge-append merge is able to replay it. Used by the writer
   * ({@link LocalBucket}) to declare the segment's page write as covered by that merge (#5596).
   */
  public boolean isEdgeAppendTracked(final RID segmentRID) {
    return edgeAppendMerge && edgeAppendsBySegment != null && edgeAppendsBySegment.containsKey(segmentRID);
  }

  /**
   * True when an edge-append merge write to page (fileId, pageNumber) has already been poisoned in this transaction.
   */
  public boolean isEdgeAppendPagePoisoned(final int fileId, final int pageNumber) {
    return edgeAppendPoisonedPages != null && edgeAppendPoisonedPages.contains(packPageKey(fileId, pageNumber));
  }

  /**
   * True when this transaction registered at least one in-chunk append on {@code pageId} that the edge-append merge
   * could replay, ignoring whether the page can PROVE the rest of its changes are replayable too. Split out from
   * {@link #isRebasableEdgeAppendPage(MutablePage)} so the coverage verdict can be reported exactly once, at the
   * point the transaction actually fails.
   */
  private boolean hasReplayableEdgeAppends(final PageId pageId) {
    if (!edgeAppendMerge || edgeAppendsBySegment == null)
      return false;
    final long key = packPageKey(pageId.getFileId(), pageId.getPageNumber());
    if (edgeAppendPoisonedPages != null && edgeAppendPoisonedPages.contains(key))
      return false;
    // Is at least one tracked segment on this (conflicting) page? Computing the segment page keys here, on the
    // rare conflict path, is what keeps the append hot path free of allocation.
    for (final RID segmentRID : edgeAppendsBySegment.keySet())
      if (edgeSegmentPageKey(segmentRID) == key)
        return true;
    return false;
  }

  private boolean isRebasableEdgeAppendPage(final MutablePage page) {
    // #5596: the rebase drops this transaction's whole image of the page, so it is sound only when every byte this
    // transaction wrote there was declared replayable by THIS merge. A writer that forgot to poison the page (or
    // never knew the merge exists) leaves at least one undeclared byte behind and is refused here - a retry instead
    // of a silently lost write. See reportCoverageDecline for the counter and the log.
    return hasReplayableEdgeAppends(page.getPageId()) && page.isFullyCoveredBy(MutablePage.COVERAGE_EDGE_APPEND_MERGE);
  }

  /**
   * Replays this transaction's tracked in-chunk appends for {@code pageId} on top of the current committed
   * version of that page, resolving a version conflict that was caused solely by concurrent appends to the
   * same edge-list chunk(s). Runs only on the leader/embedded commit, while the edge file's commit lock is
   * held (so the current version is stable), and only for pages whose every modification was a tracked append -
   * an eligibility {@link #isRebasableEdgeAppendPage(MutablePage)} verifies against the page itself rather than
   * trusting every writer to have excluded itself (#5596).
   *
   * @return the freshly rebased page, ALREADY COMPRESSED and ready to be version-checked and committed. The
   * compression is part of the contract, not the caller's job: see the note at the end of the method body (#5608).
   *
   * @throws ConcurrentModificationException if a targeted chunk filled up in the meantime (a pure in-chunk
   *                                         rebase is no longer possible): the caller falls back to a normal
   *                                         full-transaction retry.
   */
  private MutablePage rebaseEdgeAppends(final PageId pageId) throws IOException {
    // Drop the stale copies so the reload observes the current committed version of the page.
    modifiedPages.remove(pageId);
    immutablePages.remove(pageId);

    final long pageKey = packPageKey(pageId.getFileId(), pageId.getPageNumber());

    // Re-apply every tracked segment that lives on this page (usually one: the hot vertex's head chunk).
    for (final Map.Entry<RID, EdgeAppendBuffer> entry : edgeAppendsBySegment.entrySet()) {
      final RID segmentRID = entry.getKey();
      if (edgeSegmentPageKey(segmentRID) != pageKey)
        continue;

      final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(segmentRID.getBucketId());

      // MULTI-PAGE / INDIRECTED CHUNK: the rebase re-derives ONE page, but a record whose content continues on
      // other pages was drain-written there through page copies created only at drain time - copies that PASS
      // the version check while carrying this transaction's STALE view of the record's continuation bytes.
      // Rebasing would commit that stale slice, silently reverting concurrent appends on the continuation page
      // (issue #565: zeroed/shifted chunk tails on chunks straddling a page boundary). Only a record living
      // entirely in place on the conflicted page can be safely re-derived; anything else falls back to a full
      // retry.
      if (!bucket.isRecordStoredInSinglePage(segmentRID))
        throw new ConcurrentModificationException(
            "Edge-append rebase not possible: chunk " + segmentRID + " spans multiple pages. Please retry the operation");

      // Load the chunk fresh from the (now current) page, bypassing the tx record cache which still holds the
      // stale, already-appended instance. A concurrently-vanished chunk (RecordNotFoundException) cannot happen
      // under the held commit lock; if it ever did, fall back to a full retry rather than aborting the tx.
      final MutableEdgeSegment segment;
      try {
        segment = new MutableEdgeSegment(database, segmentRID, bucket.getRecord(segmentRID).copyOfContent());
      } catch (final RecordNotFoundException e) {
        throw new ConcurrentModificationException(
            "Edge-append rebase not possible: chunk " + segmentRID + " no longer exists. Please retry the operation");
      }

      final EdgeAppendBuffer buffer = entry.getValue();
      for (int i = 0; i < buffer.size; ++i)
        if (!segment.add(new RID(buffer.edgeBucket[i], buffer.edgePosition[i]),
            new RID(buffer.vertexBucket[i], buffer.vertexPosition[i])))
          throw new ConcurrentModificationException(
              "Edge-append rebase not possible: chunk " + segmentRID + " filled up concurrently. Please retry the operation");

      // Immediate synchronous page write: updatedRecords was already drained earlier in commit1stPhase, so the
      // deferred updateRecord path would never be flushed.
      database.updateRecordNoLock(segment, false);
    }

    final MutablePage rebased = modifiedPages.get(pageId);
    if (rebased == null)
      throw new ConcurrentModificationException("Edge-append rebase produced no page for " + pageId + ". Please retry the operation");

    // #5608: MUST stay here, in the method that produces the rebased page. Re-running the compression on the page the
    // merge just re-derived is what makes compressPage's COVERAGE_ALL_MERGES declaration true rather than merely
    // plausible - a declaration that, without this call, would vouch for a defrag the rebase silently dropped.
    // Pinned by Issue5608RebasedPageCompressionTest.
    //
    // The null-tolerant two-arg lookup, where rebaseSlots below simply reuses the bucket it already resolved, is not an
    // accident: THAT method needs its bucket to replay the slots at all, so it is non-null long before it compresses,
    // while here the bucket is wanted only for the defrag. This preserves the tolerance the commit loop had before the
    // call moved in - a file the schema does not map to a bucket costs the page its defrag, rather than replacing a
    // retryable conflict with a schema error on the way out of a merge that already succeeded.
    final LocalBucket bucket = database.getSchema().getEmbedded().getBucketById(pageId.getFileId(), false);
    if (bucket != null)
      bucket.compressPage(rebased, false);

    // Counted AFTER the compression, so an IOException there leaves the merge uncounted: the commit fails anyway, and
    // a counter this PR is about to put in front of operators should report merges that actually happened. Deliberate
    // - before the move the increment ran first and a failed compress still counted.
    database.getPageManager().incrementEdgeAppendMerges();
    return rebased;
  }

  /**
   * Per-page record-slot writes this transaction made, retained for the disjoint-slot merge (#5381). Small maps
   * keyed by the in-page slot index; only populated on pages that receive at least one rebasable write, and only
   * while the feature is on. Released on {@link #reset()}.
   */
  private static final class SlotRebaseBuffer {
    // slot -> this transaction's FINAL serialized record body (no size prefix); the latest write wins. A NULL value
    // is the third kind (#5569): the slot was DELETED by this transaction, so the replay frees it instead of writing
    // a body. HashMap allows null values, so one map still describes every tracked slot of the page.
    private final Map<Integer, byte[]> finalBody     = new HashMap<>();
    // slot -> the record body this transaction started from (UPDATES and DELETES only; absent for inserts).
    private final Map<Integer, byte[]> baseBody      = new HashMap<>();
    // slots holding a brand-new record (an INSERT): kept explicit so an insert that is later updated in the same
    // transaction stays an insert (base absent) rather than being mistaken for an in-place update.
    private final Set<Integer>         insertedSlots = new HashSet<>();
  }

  /**
   * Tells whether the disjoint-slot page merge is enabled for this transaction.
   */
  public boolean isSlotMergeEnabled() {
    return slotMerge;
  }

  /**
   * Registers an insert slot {@link LocalBucket} handed to this transaction on an existing page, so it is given
   * back when the transaction ends - whatever the outcome (issue #5279). Called once per created record
   * that lands on a reused page; a record on a brand-new page needs no reservation because the page number itself is
   * already reserved atomically.
   */
  public void trackInsertSlotReservation(final LocalBucket bucket, final int pageNumber, final int slot) {
    if (insertReservationBuckets == null) {
      insertReservationBuckets = new LocalBucket[16];
      insertReservationPages = new int[16];
      insertReservationSlots = new int[16];
    } else if (insertReservationCount == insertReservationBuckets.length) {
      final int newSize = insertReservationCount * 2;
      insertReservationBuckets = Arrays.copyOf(insertReservationBuckets, newSize);
      insertReservationPages = Arrays.copyOf(insertReservationPages, newSize);
      insertReservationSlots = Arrays.copyOf(insertReservationSlots, newSize);
    }

    insertReservationBuckets[insertReservationCount] = bucket;
    insertReservationPages[insertReservationCount] = pageNumber;
    insertReservationSlots[insertReservationCount] = slot;
    ++insertReservationCount;
  }

  /**
   * Gives every insert slot this transaction claimed back to its bucket. Must run on EVERY transaction teardown path
   * (commit, rollback and kill): a leaked reservation would keep a slot of a live page permanently unusable.
   */
  private void releaseInsertSlotReservations() {
    for (int i = 0; i < insertReservationCount; i++) {
      final LocalBucket bucket = insertReservationBuckets[i];
      insertReservationBuckets[i] = null;
      try {
        bucket.releaseInsertSlot(insertReservationPages[i], insertReservationSlots[i]);
      } catch (final Exception e) {
        // A RELEASE FAILURE (E.G. THE BUCKET WAS DROPPED MEANWHILE) MUST NEVER ABORT THE TRANSACTION TEARDOWN
        LogManager.instance()
            .log(this, Level.FINE, "Error on releasing the insert slot reservation of page %d", e, insertReservationPages[i]);
      }
    }
    insertReservationCount = 0;

    if (insertReservationBuckets != null && insertReservationBuckets.length > MAX_RETAINED_INSERT_RESERVATIONS) {
      // A HUGE BULK INSERT MUST NOT LEAVE ITS ARRAYS BEHIND ON A THREAD-LOCAL, REUSED TRANSACTION CONTEXT
      insertReservationBuckets = null;
      insertReservationPages = null;
      insertReservationSlots = null;
    }
  }

  /**
   * Records a brand-new record inserted into a FREE slot of an EXISTING page as rebasable: at commit, a
   * page-version conflict caused only by concurrent writes to OTHER slots of that page can be resolved by
   * replaying this insert on the newer committed page. No-op when the feature is off or the page is poisoned.
   * The caller ({@link LocalBucket}) passes the already-computed page/slot so no schema lookup happens per write.
   */
  public void trackRebasableInsert(final int fileId, final int pageNumber, final int slot, final byte[] finalBody) {
    if (!slotMerge)
      return;
    final long key = packPageKey(fileId, pageNumber);
    if (slotRebasePoisonedPages != null && slotRebasePoisonedPages.contains(key))
      return;
    if (slotRebaseByPage == null)
      slotRebaseByPage = new HashMap<>();
    final SlotRebaseBuffer buffer = slotRebaseByPage.computeIfAbsent(key, k -> new SlotRebaseBuffer());
    if (buffer.finalBody.containsKey(slot) && buffer.finalBody.get(slot) == null && !buffer.insertedSlots.contains(slot)) {
      // DELETE followed by an INSERT on the SAME slot within one transaction. LocalBucket.getFreeSpaceInPage never
      // hands back a slot this transaction has just freed (guarded by
      // Issue5279ConcurrentInsertTest.deletedSlotsAreRecycledByTheNextInsert), so this cannot happen today - but
      // replaying it as a plain insert would skip the pre-image check the delete relies on, so if that allocation
      // policy ever changes the page must fall back to a normal retry instead of silently mis-merging.
      poisonSlotRebasePage(fileId, pageNumber);
      return;
    }
    final byte[] prev = buffer.finalBody.put(slot, finalBody);
    buffer.insertedSlots.add(slot);
    // Account the NET change in retained bytes: a repeated write to the same slot replaces its image, it does not
    // add one - so the running total tracks bytes actually held, matching the cap's retained-heap intent.
    accountTrackedBytes(finalBody.length - (prev != null ? prev.length : 0));
  }

  /**
   * Records a record update that stayed INSIDE its page - an overwrite of the same size or smaller, or a growth the
   * page could host by shifting the records that follow (#5279) - as rebasable, keeping {@code baseBody} (the
   * pre-image) so the rebase can distinguish a false page conflict (a concurrent write to another slot) from a
   * true one (a concurrent write to THIS record). No-op when the feature is off or the page is poisoned. The caller
   * passes the already-computed page/slot so no schema lookup happens per write.
   */
  public void trackRebasableUpdate(final int fileId, final int pageNumber, final int slot, final byte[] baseBody,
      final byte[] finalBody) {
    if (!slotMerge)
      return;
    final long key = packPageKey(fileId, pageNumber);
    if (slotRebasePoisonedPages != null && slotRebasePoisonedPages.contains(key))
      return;
    if (slotRebaseByPage == null)
      slotRebaseByPage = new HashMap<>();
    final SlotRebaseBuffer buffer = slotRebaseByPage.computeIfAbsent(key, k -> new SlotRebaseBuffer());
    final byte[] prevFinal = buffer.finalBody.put(slot, finalBody);
    long delta = finalBody.length - (prevFinal != null ? prevFinal.length : 0);
    // First-touch base wins: a second in-tx update must still diff against the COMMITTED pre-image, not the
    // intermediate one. An insert-then-update keeps insert semantics (no base recorded). Only the FIRST base
    // capture for a slot adds to the retained-byte total.
    if (!buffer.insertedSlots.contains(slot) && buffer.baseBody.putIfAbsent(slot, baseBody) == null)
      delta += baseBody.length;
    accountTrackedBytes(delta);
  }

  /**
   * Records the DELETE of a plain in-place record - a single-slot change (zero the slot-table entry, optionally wipe
   * the content) that commutes with writes to every other slot of the page, exactly like the insert and the in-page
   * update above (#5569). {@code baseBody} is the pre-image captured BEFORE the optional content wipe-out, so the
   * replay can still tell a false page conflict from a concurrent write to THIS record. No-op when the feature is off
   * or the page is poisoned; the caller ({@link com.arcadedb.engine.LocalBucket}) keeps every other delete shape
   * (placeholder pointer or content, chunk chain, corrupted slot) poisoning the page.
   */
  public void trackRebasableDelete(final int fileId, final int pageNumber, final int slot, final byte[] baseBody) {
    if (!slotMerge)
      return;
    final long key = packPageKey(fileId, pageNumber);
    if (slotRebasePoisonedPages != null && slotRebasePoisonedPages.contains(key))
      return;
    if (slotRebaseByPage == null)
      slotRebaseByPage = new HashMap<>();
    final SlotRebaseBuffer buffer = slotRebaseByPage.computeIfAbsent(key, k -> new SlotRebaseBuffer());
    // A null final image marks the slot as deleted. The image the slot held so far (if any) is released.
    final byte[] prevFinal = buffer.finalBody.put(slot, null);
    long delta = -(prevFinal != null ? prevFinal.length : 0);
    // First-touch base wins, as for an update: a record this transaction already updated must still be diffed
    // against the COMMITTED pre-image, not against the intermediate image it wrote itself. A slot this transaction
    // INSERTED has no committed pre-image at all: it stays an insert-and-delete, replayed as nothing.
    if (!buffer.insertedSlots.contains(slot) && buffer.baseBody.putIfAbsent(slot, baseBody) == null)
      delta += baseBody.length;
    accountTrackedBytes(delta);
  }

  /**
   * Marks the bucket page (fileId, pageNumber) as NOT rebasable via the slot merge: this transaction changed it in
   * a way that is not a single-slot insert, in-page update or plain-record delete (a placeholder or multi-page
   * record, a record that had to spill out of the page). Any tracked slots on it are dropped so a rebase can never
   * silently re-derive the page from committed-state + our slot writes. No-op when the feature is off.
   * <p>
   * As on the edge-append side, since #5596 this is the fast, precise opt-OUT and not the only line of defence:
   * {@link #isRebasableSlotPage(MutablePage)} also requires the page to prove that every byte this transaction
   * wrote there was declared replayable by this merge (see {@link MutablePage#beginCoveredWrite(int)}).
   * <p>
   * #5608 audited every call site to decide whether the ones the coverage proof now subsumes - most clearly the
   * multi-page writers', whose inline record-table writes are undeclared anyway - should be dropped. They stay,
   * all of them, because poisoning is not only a duplicate of the proof. It runs BEFORE the page is read and is
   * O(1); it makes {@link #isSlotRebasePagePoisoned(int, int)} true, which is what lets the write path skip the
   * pre-image/final-image record copies for every later write to the page (they would be allocated and then
   * discarded); and, since {@code reportCoverageDecline} deliberately ignores poisoned pages, it is what keeps a
   * DELIBERATE exclusion out of {@code mergesDeclinedByCoverage} - the statistic that exists to expose an
   * ACCIDENTAL one. Removing a redundant call would therefore cost allocations and blunt that signal, to save
   * nothing. Keep poisoning any page whose writes this merge cannot replay, even when the proof would catch it.
   */
  public void poisonSlotRebasePage(final int fileId, final int pageNumber) {
    if (!slotMerge)
      return;
    final long key = packPageKey(fileId, pageNumber);
    if (slotRebasePoisonedPages == null)
      slotRebasePoisonedPages = new LongHashSet();
    slotRebasePoisonedPages.add(key);
    if (slotRebaseByPage != null)
      slotRebaseByPage.remove(key);
  }

  /**
   * Bounds the heap the slot merge may retain within one transaction: once the running total of tracked images
   * exceeds {@link GlobalConfiguration#TX_PAGE_SLOT_MERGE_MAX_BYTES}, the merge is disabled for the remainder of the
   * transaction. Already-tracked pages are dropped (freeing their images) so a conflict on any of them now falls
   * back to a normal retry - a huge transaction degrades to plain MVCC instead of holding ~2x its touched records.
   */
  private void accountTrackedBytes(final long added) {
    slotRebaseTrackedBytes += added;
    if (slotRebaseTrackedBytes > slotMergeMaxBytes) {
      slotMerge = false;
      if (slotRebaseByPage != null)
        slotRebaseByPage.clear();
      // NOTE: slotRebasePoisonedPages is intentionally NOT cleared. Once slotMerge is off, isRebasableSlotPage
      // short-circuits so the set is not consulted anyway; keeping it costs nothing and avoids any assumption
      // that a poisoned page could become rebasable again after the feature disables mid-transaction.
    }
  }

  /**
   * True when {@code slot} of page (fileId, pageNumber) holds a record this transaction CREATED and is tracked as a
   * rebasable insert. Lets the update path recognise a write to a record of its own making: it has no committed
   * pre-image, so it must keep insert semantics (#5279) - and, for the record kinds normally left to the
   * edge-append merge, it is the ONLY mechanism that can replay it, since the edge merge cannot rebase a chunk the
   * committed page does not contain yet.
   */
  public boolean isSlotTrackedAsInsert(final int fileId, final int pageNumber, final int slot) {
    if (!slotMerge || slotRebaseByPage == null)
      return false;
    final SlotRebaseBuffer buffer = slotRebaseByPage.get(packPageKey(fileId, pageNumber));
    return buffer != null && buffer.insertedSlots.contains(slot);
  }

  /**
   * True when a slot-merge write to page (fileId, pageNumber) has already been poisoned this transaction, so the
   * caller can skip building the (allocation-heavy) record-image copy for a page that can no longer be rebased.
   */
  public boolean isSlotRebasePagePoisoned(final int fileId, final int pageNumber) {
    return slotRebasePoisonedPages != null && slotRebasePoisonedPages.contains(packPageKey(fileId, pageNumber));
  }

  /**
   * True when this transaction registered at least one slot write on {@code pageId} that the disjoint-slot merge
   * could replay, ignoring whether the page can PROVE the rest of its changes are replayable too. See
   * {@link #hasReplayableEdgeAppends(PageId)}.
   */
  private boolean hasReplayableSlotWrites(final PageId pageId) {
    if (!slotMerge || slotRebaseByPage == null)
      return false;
    final long key = packPageKey(pageId.getFileId(), pageId.getPageNumber());
    if (slotRebasePoisonedPages != null && slotRebasePoisonedPages.contains(key))
      return false;
    return slotRebaseByPage.containsKey(key);
  }

  private boolean isRebasableSlotPage(final MutablePage page) {
    // #5596: same coverage proof as the edge-append merge - the page is re-derived from the tracked slot writes
    // alone, so any byte written outside a slot-merge declaration disqualifies it.
    return hasReplayableSlotWrites(page.getPageId()) && page.isFullyCoveredBy(MutablePage.COVERAGE_SLOT_MERGE);
  }

  /**
   * Reports, ONCE per page and only when the transaction is really about to fail, that a merge which had tracked
   * writes on {@code page} was refused solely because the page could not prove the rest of its changes replayable
   * (#5596). Counting inside the {@code isRebasable*} predicates instead would double-count a page both merges
   * looked at, and could even count a decline for a page the other merge went on to rebase - which would make the
   * one statistic meant to expose a forgotten declaration untrustworthy.
   * <p>
   * #5608: TOTAL IN PRODUCTION by construction. It runs inside the {@code catch (ConcurrentModificationException)} of
   * the commit loop, one statement before {@code throw e}, so anything thrown here would REPLACE the conflict the
   * caller is propagating with a different one - {@link #hasReplayableEdgeAppends(PageId)} alone reaches
   * {@link #edgeSegmentPageKey(RID)}, which raises on a bucket it cannot resolve. Swallowing here, rather than making
   * that one call non-throwing, is deliberate: it also covers whatever a future diagnostic adds to this method.
   * <p>
   * "In production" is the precise claim, and the assertion in the catch block is exactly where it stops holding: an
   * {@code AssertionError} is not a {@code RuntimeException}, so under {@code -ea} (surefire's default) a genuine bug
   * on this path DOES escape and replace the caller's conflict. That is the point - a defect in the diagnostic must
   * fail a test loudly rather than hide behind the guarantee - and it costs nothing where the guarantee matters,
   * since assertions are off in a production JVM and the swallow is then unconditional.
   * <p>
   * Auditing that path is what exposed the bug fixed alongside this guard: the lookup used to raise
   * {@code SchemaException}, which is NOT a {@link NeedRetryException}, so the commit's generic
   * {@code catch (Exception)} would have wrapped it into a plain {@code TransactionException} - a hard failure where
   * a retry was intended. That is fixed at the source now (see {@code edgeSegmentPageKey}), and this guard remains
   * the reason a diagnostic cannot rewrite the caller's verdict whatever it starts throwing next.
   */
  private void reportCoverageDecline(final MutablePage page) {
    try {
      final PageId pageId = page.getPageId();
      final boolean edgeDeclined = hasReplayableEdgeAppends(pageId)//
          && !page.isFullyCoveredBy(MutablePage.COVERAGE_EDGE_APPEND_MERGE);
      final boolean slotDeclined = hasReplayableSlotWrites(pageId)//
          && !page.isFullyCoveredBy(MutablePage.COVERAGE_SLOT_MERGE);
      if (!edgeDeclined && !slotDeclined)
        // An ordinary conflict on a page no merge ever tracked: nothing to do with coverage.
        return;

      database.getPageManager().incrementMergesDeclinedByCoverage();
      LogManager.instance().log(this, Level.FINE,
          "%s merge declined page %s: it carries changes no tracked write accounts for",
          edgeDeclined && slotDeclined ? "Edge-append and slot" : edgeDeclined ? "Edge-append" : "Slot", pageId);
    } catch (final RuntimeException diagnosticError) {
      // The conflict the caller is about to rethrow is the truth; losing one counter increment is not worth
      // corrupting it. Logged at FINE so the swallowed cause is still recoverable when debugging this path.
      //
      // FINE, and not WARNING, because a bug here would fire on EVERY declined merge of a contended page: a louder
      // level would turn one defect into a log flood on the very path that is already under pressure. The cost is
      // that a programming error introduced into this method later (an NPE, a bad index) would be swallowed as
      // quietly as the engine exception this catch exists for - so the assertion below draws the line between the
      // two. It fails fast under -ea (surefire's default) at zero production cost, exactly like the leaked-coverage
      // check in MutablePage.beginCoveredWrite; every exception this method can legitimately meet is an
      // ArcadeDBException (SchemaException from the bucket lookup, ConcurrentModificationException from its
      // sibling branch), so anything else is a defect in the diagnostic itself and not a state it must tolerate.
      assert diagnosticError instanceof ArcadeDBException :
          "reportCoverageDecline is a diagnostic and must not fail on its own: " + diagnosticError;

      LogManager.instance().log(this, Level.FINE, "Unable to report the coverage decline of page %s", diagnosticError,
          page.getPageId());
    }
  }

  /**
   * Replays this transaction's tracked slot writes for {@code pageId} on top of the current committed version of
   * that page, resolving a version conflict caused solely by concurrent writes to OTHER slots on the same page.
   * Runs only on the leader/embedded commit, while the bucket file's commit lock is held (so the current version
   * is stable), and only for a page whose every modification this transaction made was a tracked disjoint-slot
   * insert or an update that stayed inside the page.
   *
   * @return the freshly rebased page, ALREADY COMPRESSED and ready to be version-checked and committed. The
   * compression is part of the contract, not the caller's job: see the note at the end of the method body (#5608).
   *
   * @throws ConcurrentModificationException if a slot was taken/changed by a concurrent commit (a true conflict)
   *                                         or the page can no longer host a record: the caller falls back to a
   *                                         normal full-transaction retry.
   */
  private MutablePage rebaseSlots(final PageId pageId) throws IOException {
    final long key = packPageKey(pageId.getFileId(), pageId.getPageNumber());
    final SlotRebaseBuffer buffer = slotRebaseByPage.get(key);

    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(pageId.getFileId());

    // Drop the stale copies so the reload observes the current committed version of the page.
    modifiedPages.remove(pageId);
    immutablePages.remove(pageId);

    final MutablePage committed = getPageToModify(pageId, bucket.getPageSize(), false);

    for (final Map.Entry<Integer, byte[]> entry : buffer.finalBody.entrySet()) {
      final int slot = entry.getKey();
      final byte[] finalBody = entry.getValue();
      final boolean insertedHere = buffer.insertedSlots.contains(slot);

      if (finalBody == null) {
        // DELETE (#5569). A slot this transaction both CREATED and deleted never existed on the committed page: its
        // net effect there is nothing, so it must be skipped entirely - looking for a pre-image that cannot be there
        // would turn a perfectly mergeable transaction into a conflict.
        if (insertedHere)
          continue;
        if (!bucket.rebaseRecordDeleteOnPage(committed, slot, buffer.baseBody.get(slot)))
          throw new ConcurrentModificationException("Slot rebase not possible on page " + pageId + " slot " + slot
              + " (concurrent change to the record being deleted). Please retry the operation");
        continue;
      }

      final byte[] baseBody = insertedHere ? null : buffer.baseBody.get(slot);
      if (!bucket.rebaseRecordOnPage(committed, slot, finalBody, baseBody))
        throw new ConcurrentModificationException(
            "Slot rebase not possible on page " + pageId + " slot " + slot + " (concurrent change to the same record). Please retry the operation");
    }

    // #5608: MUST stay here, in the method that produces the rebased page. A shrinking update or a delete replayed
    // above leaves a hole exactly as the original write did, and re-running the compression is what makes
    // compressPage's COVERAGE_ALL_MERGES declaration true rather than merely plausible (a declaration that, without
    // this call, would vouch for a defrag the rebase silently dropped). Pinned by Issue5608RebasedPageCompressionTest.
    bucket.compressPage(committed, false);

    // Counted AFTER the compression, for the reason given at the end of rebaseEdgeAppends: only a merge that really
    // produced a committable page belongs in the statistic operators watch.
    database.getPageManager().incrementTxPageSlotMerges();
    return committed;
  }

  @Override
  public boolean isActive() {
    return status != STATUS.INACTIVE;
  }

  public Map<String, Object> getStats() {
    final LinkedHashMap<String, Object> map = new LinkedHashMap<>();

    final Set<Integer> involvedFiles = new LinkedHashSet<>();
    if (modifiedPages != null)
      for (final PageId pid : modifiedPages.keySet())
        involvedFiles.add(pid.getFileId());
    if (newPages != null)
      for (final PageId pid : newPages.keySet())
        involvedFiles.add(pid.getFileId());
    involvedFiles.addAll(newPageCounters.keySet());

    map.put("status", status.name());
    map.put("involvedFiles", involvedFiles);
    map.put("modifiedPages", modifiedPages != null ? modifiedPages.size() : 0);
    map.put("newPages", newPages != null ? newPages.size() : 0);
    map.put("updatedRecords", updatedRecords != null ? updatedRecords.size() : 0);
    map.put("newPageCounters", newPageCounters);
    map.put("indexChanges", indexChanges != null ? indexChanges.getTotalEntries() : 0);
    return map;
  }

  public boolean hasChanges() {
    final int totalImpactedPages = modifiedPages.size() + (newPages != null ? newPages.size() : 0);
    return totalImpactedPages > 0 || !indexChanges.isEmpty();
  }

  public int getModifiedPages() {
    int result = 0;
    if (modifiedPages != null)
      result += modifiedPages.size();
    if (newPages != null)
      result += newPages.size();
    return result;
  }

  /**
   * Test only API.
   */
  public void kill() {
    if (explicitLockedFiles != null) {
      database.getTransactionManager().unlockFilesInOrder(explicitLockedFiles, getRequester());
      explicitLockedFiles = null;
    }
    // #5067: SAME RELEASE SYMMETRY AS reset(). NULLING THE FIELD WITHOUT UNLOCKING LEAKED THE LockManager
    // ENTRIES OF A TRANSACTION KILLED IN COMMIT_1ST_PHASE (WHERE lockFilesInOrder POPULATED lockedFiles)
    // UNTIL TransactionManager.kill() CLOSED THE WHOLE LOCK MANAGER
    if (lockedFiles != null) {
      database.getTransactionManager().unlockFilesInOrder(lockedFiles, getRequester());
      lockedFiles = null;
    }
    releaseInsertSlotReservations();
    modifiedPages = null;
    newPages = null;
    edgeAppendsBySegment = null;
    edgeAppendPoisonedPages = null;
    slotRebaseByPage = null;
    slotRebasePoisonedPages = null;
    slotRebaseTrackedBytes = 0;
    updatedRecords = null;
    updatedRecordsIndexSnapshot = null;
    newPageCounters.clear();
    immutablePages.clear();
  }

  /**
   * Captures (lazily) and returns the identity file locks are keyed by for this transaction: the explicit
   * requester (e.g. a server-side session) when one was set via {@link #setRequester(Object)} (#4959),
   * otherwise the OWNING thread, captured at first call - which is always lock-acquisition time on the
   * owner (#4941). MUST be called only from the lock-acquisition paths on the owning thread; see the
   * INVARIANT note on the {@code requester} field. Release paths use the pure {@link #getRequester()}.
   * Package-private on purpose: only same-package acquisition code (this class and LocalDatabase's
   * executeLockingFiles) may capture.
   */
  Object captureRequester() {
    Object captured = requester;
    if (captured == null) {
      captured = Thread.currentThread();
      requester = captured;
    }
    return captured;
  }

  /**
   * PURE read of the identity file locks are keyed by for this transaction (no capture, safe to call from
   * any thread): the explicit requester when one was set via {@link #setRequester(Object)} (#4959),
   * otherwise the owning thread captured by {@link #captureRequester()} at lock-acquisition time (#4941).
   * Re-resolving Thread.currentThread() per call returned the WRONG requester when the locks were released
   * by another thread (database close rolling back foreign contexts, or the dead-thread sweep rolling back
   * an abandoned transaction): the unlock failed with LockException and the file locks leaked forever. The
   * LockManager explicitly supports acquire-on-one-thread/release-on-another as long as the requester
   * object is identical. The current-thread fallback is harmless: a null requester at release time means
   * this transaction never acquired a file lock, so there is nothing keyed by it to release.
   */
  public Object getRequester() {
    final Object captured = requester;
    return captured != null ? captured : Thread.currentThread();
  }

  public Map<Integer, Integer> getBucketRecordDelta() {
    final Map<Integer, Integer> map = new HashMap<>(bucketRecordDelta.size());
    bucketRecordDelta.forEach((k, v) -> map.put(k, v));
    return map;
  }

  /**
   * Returns the delta of records considering the pending changes in transaction.
   */
  public long getBucketRecordDelta(final int bucketId) {
    return bucketRecordDelta.get(bucketId, 0);
  }

  /**
   * Updates the record counter for buckets. At transaction commit, the delta is updated into the schema.
   */
  public void updateBucketRecordDelta(final int bucketId, final int delta) {
    bucketRecordDelta.add(bucketId, delta);
  }

  /**
   * Executes 1st phase from a replica.
   */
  public void commitFromReplica(final WALFile.WALTransaction buffer,
      final Map<String, TreeMap<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>>> keysTx,
      final Map<Integer, Integer> bucketRecordDelta) throws TransactionException {

    for (Map.Entry<Integer, Integer> entry : bucketRecordDelta.entrySet())
      this.bucketRecordDelta.put(entry.getKey(), entry.getValue());

    final int totalImpactedPages = buffer.pages.length;
    if (totalImpactedPages == 0 && keysTx.isEmpty()) {
      // EMPTY TRANSACTION = NO CHANGES
      modifiedPages = null;
      return;
    }

    try {
      // LOCK FILES (IN ORDER, SO TO AVOID DEADLOCK).
      // IntHashSet (zero-boxing) is used because this set is built on every commit
      // and the small Integer boxing churn shows up in young-gen GC pressure under
      // high transaction throughput.
      final IntHashSet modifiedFiles = new IntHashSet(buffer.pages.length + 4);

      for (final WALFile.WALPage p : buffer.pages)
        modifiedFiles.add(p.fileId);

      indexChanges.setKeys(keysTx);
      indexChanges.addFilesToLock(modifiedFiles);

      final int dictionaryFileId = database.getSchema().getDictionary().getFileId();
      boolean dictionaryModified = false;

      for (final WALFile.WALPage p : buffer.pages) {
        final PaginatedComponentFile file = (PaginatedComponentFile) database.getFileManager().getFile(p.fileId);
        final int pageSize = file.getPageSize();

        final PageId pageId = new PageId(database, p.fileId, p.pageNumber);

        final boolean isNew = p.pageNumber >= file.getTotalPages();

        final MutablePage page = getPageToModify(pageId, pageSize, isNew);

        // APPLY THE CHANGE TO THE PAGE
        page.writeByteArray(p.changesFrom - BasePage.PAGE_HEADER_SIZE, p.currentContent.content);
        page.setContentSize(p.currentPageSize);

        if (isNew) {
          newPages.put(pageId, page);
          // Same max-semantics as addPage: the WAL pages of one file arrive in no particular order, so a plain put
          // could leave the counter BELOW the highest new page of this file, and getTotalPages() (which prefers
          // this counter) would then under-report the size of the file for the rest of the transaction.
          final Integer indexCounter = newPageCounters.get(pageId.getFileId());
          if (indexCounter == null || indexCounter < pageId.getPageNumber() + 1)
            newPageCounters.put(pageId.getFileId(), pageId.getPageNumber() + 1);
        } else
          modifiedPages.put(pageId, page);

        if (!dictionaryModified && dictionaryFileId == pageId.getFileId())
          dictionaryModified = true;
      }

      database.commit();

      if (dictionaryModified)
        database.getSchema().getDictionary().reload();

    } catch (final ConcurrentModificationException e) {
      rollback();
      throw e;
    } catch (final Exception e) {
      rollback();
      throw new TransactionException("Transaction error on commit", e);
    }
  }

  /**
   * Locks the files in order, then checks all the pre-conditions.
   */
  public TransactionPhase1 commit1stPhase(final boolean isLeader) {
    if (status == STATUS.INACTIVE)
      throw new TransactionException("Transaction not started");

    if (status != STATUS.BEGUN)
      throw new TransactionException("Transaction in phase " + status);

    // Acquire file locks BEFORE processing updatedRecords so that updateRecordNoLock
    // (which loads pages and follows multi-page record chunk chains) is serialized.
    // Without this, concurrent updateRecordNoLock calls could both load the same page
    // at the same version, bypassing MVCC version checks.
    status = STATUS.COMMIT_1ST_PHASE;
    // Only the leader replays the queued index operations below: on a replica the index pages arrive with the
    // leader's changes. An index that skips work during this phase because "the replay will do it" must know.
    this.indexChangesReplayed = isLeader;

    try {
      // #4937: explicit-lock mode captured before checkExplicitLocks nulls explicitLockedFiles, so the
      // late-joiner block below can still tell an explicit-lock transaction from an auto-locked one.
      // Determine files to lock — must include files from updatedRecords
      if (updatedRecords != null)
        for (final Record rec : updatedRecords.values()) {
          final RID rid = rec.getIdentity();
          ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).fetchPageInTransaction(rid);
        }

      // #5503: replicas lock the modified files too. The page-version validation below is a
      // check-then-act, and the lock is what makes it atomic against other local committers. A replica
      // used to skip it, so two concurrent transactions validated against the same base version and both
      // shipped a delta stamped with the same next version; the state machine applied the first and then
      // re-applied the second onto it through the equal-version repair path (#4926), splicing two partial
      // page images together and dropping the records the first delta carried.
      final IntHashSet modifiedFiles = lockFilesFromChanges();

      // checkExplicitLocks nulls explicitLockedFiles on success, so remember the mode now for the
      // late-joiner check further down (#4937): otherwise that check always sees null and would
      // silently expand an explicit-lock transaction's lock set, defeating the explicit-locking contract.
      final boolean explicitLockMode = explicitLockedFiles != null;

      if (explicitLockedFiles != null)
        checkExplicitLocks(modifiedFiles);
      else
        // LOCK FILES IN ORDER (TO AVOID DEADLOCK)
        lockedFiles = lockFilesInOrder(modifiedFiles);

      // Process updatedRecords AFTER acquiring locks
      if (updatedRecords != null) {
        for (final Record rec : updatedRecords.values())
          try {
            database.updateRecordNoLock(rec, false);
          } catch (final RecordNotFoundException e) {
            // #4959: the record vanished between the in-tx update and the commit (deleted by a concurrent
            // transaction whose effect this tx's page was loaded after, so the MVCC page-version check cannot
            // see the conflict; an in-tx delete removes the entry from updatedRecords and never gets here).
            // Silently skipping would commit the rest of the transaction without this update (a silent
            // partial commit): fail the whole transaction with a retryable conflict instead.
            throw new ConcurrentModificationException(
                "Record " + rec.getIdentity() + " updated in transaction was deleted by a concurrent transaction");
          }
        updatedRecords = null;
        // The indexed-state snapshots (#4935) share updatedRecords' lifecycle: no further updateRecord can
        // happen for this tx past the 1st phase, so release them here instead of waiting for reset() and
        // free the memory one phase earlier for large batches.
        updatedRecordsIndexSnapshot = null;
      }

      if (!isLeader || !hasChanges()) {
        if (!hasChanges()) {
          if (lockedFiles != null) {
            database.getTransactionManager().unlockFilesInOrder(lockedFiles, getRequester());
            lockedFiles = null;
          }
          status = STATUS.INACTIVE;
          return null;
        }
      }

      if (isLeader)
        // COMMIT INDEX CHANGES (IN CASE OF REPLICA THIS IS DEMANDED TO THE LEADER EXECUTION)
        indexChanges.commit();

      // #4937: the steps above (updateRecordNoLock, indexChanges.commit) can add pages of files that were
      // NOT in the lock set computed at lock time - EXTERNAL-property buckets, indexes created inside this
      // transaction, multi-file index components. Their pages would pass the version checks below WITHOUT
      // their file lock held, which is what made a phase-2 failure after the WAL append reachable (#4936:
      // an aborted transaction partially replayed by recovery). Verify coverage; on a late joiner, release
      // and re-acquire the UNION in order (lock-ordering discipline forbids acquiring extra locks in
      // place). The version checks below run after this block, so anything that changed while unlocked
      // fails validation with the standard retriable ConcurrentModificationException.
      if (lockedFiles != null) {
        // Fast path first: in the overwhelmingly common case every touched file is already locked, and this
        // is one of the hottest paths in the engine - detect late joiners with a plain scan (no allocation,
        // no boxing) and only build the union set when one is actually found.
        boolean lateJoiners = false;
        for (final PageId pid : modifiedPages.keySet())
          if (!containsFileId(lockedFiles, pid.getFileId())) {
            lateJoiners = true;
            break;
          }
        if (!lateJoiners && newPages != null)
          for (final PageId pid : newPages.keySet())
            if (!containsFileId(lockedFiles, pid.getFileId())) {
              lateJoiners = true;
              break;
            }

        if (lateJoiners) {
          final IntHashSet union = new IntHashSet(lockedFiles.size() + modifiedPages.size());
          for (final int fileId : lockedFiles)
            union.add(fileId);
          for (final PageId pid : modifiedPages.keySet())
            union.add(pid.getFileId());
          if (newPages != null)
            for (final PageId pid : newPages.keySet())
              union.add(pid.getFileId());

          if (explicitLockMode)
            // The user's explicit lock list does not cover files this transaction actually modified:
            // re-locking on their behalf would defeat the explicit-locking contract - surface it as the
            // "not all the modified resources were locked" violation instead. (explicitLockedFiles was
            // already nulled by the earlier checkExplicitLocks, so we branch on the captured mode.)
            checkExplicitLocksForUnion(union);
          else {
            database.getTransactionManager().unlockFilesInOrder(lockedFiles, getRequester());
            lockedFiles = lockFilesInOrder(union);
          }
        }
      }

      // CHECK THE VERSIONS FIRST
      final List<MutablePage> pages = new ArrayList<>();
      final PageManager pageManager = database.getPageManager();

      // COMPRESS PAGES BEFORE SAVING THEM
      final LocalSchema localSchema = database.getSchema().getEmbedded();

      for (MutablePage page : modifiedPages.values()) {
        final LocalBucket bucket = localSchema.getBucketById(page.getPageId().getFileId(), false);
        if (bucket != null)
          bucket.compressPage(page, false);
      }
      for (MutablePage page : newPages.values()) {
        final LocalBucket bucket = localSchema.getBucketById(page.getPageId().getFileId(), false);
        if (bucket != null)
          bucket.compressPage(page, false);
      }

      List<PageId> pagesToRebase = null;
      List<PageId> slotPagesToRebase = null;
      for (final Iterator<MutablePage> it = modifiedPages.values().iterator(); it.hasNext(); ) {
        final MutablePage p = it.next();

        final int[] range = p.getModifiedRange();
        if (range[1] <= 0) {
          // PAGE NOT MODIFIED, REMOVE IT
          it.remove();
          continue;
        }

        try {
          pageManager.checkPageVersion(p, false);
          pages.add(p);
        } catch (final ConcurrentModificationException e) {
          // Commutative edge-append merge: when the ONLY change this (leader/embedded) transaction made to the
          // conflicting page was appending edges to their chunk(s), the appends can be replayed on the newer
          // committed version instead of failing the whole transaction. The edge file's commit lock is held
          // here, so the current version stays stable across the rebase performed after the loop (rebasing here
          // would structurally modify modifiedPages while iterating it).
          if (isLeader && isRebasableEdgeAppendPage(p)) {
            if (pagesToRebase == null)
              pagesToRebase = new ArrayList<>();
            pagesToRebase.add(p.getPageId());
            it.remove();
          } else if (isLeader && isRebasableSlotPage(p)) {
            // Disjoint-slot merge (#5381): the conflict is only because a concurrent commit touched OTHER slots
            // of this page; replay this transaction's slot writes on the newer committed page (after the loop).
            if (slotPagesToRebase == null)
              slotPagesToRebase = new ArrayList<>();
            slotPagesToRebase.add(p.getPageId());
            it.remove();
          } else {
            if (isLeader)
              // #5596 diagnostics: exactly one report, for a page a merge would have taken but for its coverage.
              reportCoverageDecline(p);
            throw e;
          }
        }
      }

      // #5608: the rebased page is ALREADY compressed - rebaseEdgeAppends/rebaseSlots re-run compressPage themselves,
      // because that re-run is what makes compressPage's COVERAGE_ALL_MERGES declaration true. Do not lift it back up
      // here: see the note on those two methods.
      if (pagesToRebase != null)
        for (final PageId pageId : pagesToRebase) {
          final MutablePage rebased = rebaseEdgeAppends(pageId);
          pageManager.checkPageVersion(rebased, false);
          pages.add(rebased);
        }

      if (slotPagesToRebase != null)
        for (final PageId pageId : slotPagesToRebase) {
          final MutablePage rebased = rebaseSlots(pageId);
          pageManager.checkPageVersion(rebased, false);
          pages.add(rebased);
        }

      if (newPages != null)
        for (final MutablePage p : newPages.values()) {
          final int[] range = p.getModifiedRange();
          if (range[1] > 0) {
            pageManager.checkPageVersion(p, true);
            pages.add(p);
          }
        }

      Binary result = null;

      if (useWAL) {
        txId = database.getTransactionManager().getNextTransactionId();
        //LogManager.instance().log(this, Level.FINE, "Creating buffer for TX %d (threadId=%d)", txId, Thread.currentThread().threadId());
        result = database.getTransactionManager().createTransactionBuffer(txId, pages);
      }

      return new TransactionPhase1(result, pages);

    } catch (final DuplicatedKeyException | NeedRetryException e) {
      // NeedRetryException covers ConcurrentModificationException AND every other retryable conflict, notably
      // LockTimeoutException from the commit file-lock acquisition: wrapping those in the generic
      // TransactionException below silently made a retryable contention error NON-retryable, so under heavy
      // commit-lock pressure (e.g. concurrent super-node writes, #5156) transactions failed to the caller
      // instead of retrying.
      rollback();
      throw e;
    } catch (final TransactionException e) {
      // Already a first-class transaction error (e.g. the explicit-lock contract violation): rethrow as-is
      // instead of double-wrapping it in a generic "Transaction error on commit" (#5053).
      rollback();
      throw e;
    } catch (final Exception e) {
      LogManager.instance()
          .log(this, Level.FINE, "Unknown exception during commit (threadId=%d)", e, Thread.currentThread().threadId());
      rollback();
      throw new TransactionException("Transaction error on commit", e);
    }
  }

  public void commit2ndPhase(final TransactionPhase1 changes) {
    if (changes == null) {
      // Nothing to apply: release resources without touching user-held record state (the pre-#4940 behavior for
      // this no-op call; entering the try would route it to the rollback() branch below).
      reset();
      return;
    }

    boolean committed = false;
    boolean walAppended = false;
    try {
      if (database.getMode() == ComponentFile.MODE.READ_ONLY)
        throw new TransactionException("Cannot commit changes because the database is open in read-only mode");

      if (status != STATUS.COMMIT_1ST_PHASE)
        throw new TransactionException("Cannot execute 2nd phase commit without having started the 1st phase");

      status = STATUS.COMMIT_2ND_PHASE;

      // #4936: validate the page versions and bump them BEFORE the WAL append, making the append the point
      // of no return: a WAL record must only ever exist for a transaction that can no longer fail
      // validation. Previously the order was WAL-then-validate, so a phase-2 ConcurrentModificationException
      // (reachable through the late-joining-files hole, #4937) aborted the transaction while its record
      // stayed in the WAL with no abort marker - crash recovery then replayed the aborted transaction's
      // other pages (version == disk+1), a partial application: torn records, index entries pointing at
      // data never committed. The bump below mutates only this transaction's private MutablePage instances,
      // so a WAL-append failure still aborts cleanly with nothing observable. The in-between window is safe
      // because every modified file's lock is held (#4937 guarantees coverage) until reset().
      // #5053: refuse to commit on a fenced database, before doing any work. The file locks this
      // transaction acquired in phase 1 give the happens-before edge: for any transaction sharing a page
      // with the failed one, the fence write (made before the failed commit's reset() released those locks)
      // is visible here - it cannot append a WAL record targeting the same page version the orphaned record
      // already claims.
      if (database.getEmbedded() instanceof LocalDatabase localDatabase && localDatabase.isFencedForRecovery())
        throw new TransactionException(
            "Cannot commit: the database is fenced after a failure past the WAL commit point, close and reopen it to run recovery");

      final List<MutablePage> pagesToPublish = database.getPageManager().validateAndBumpVersions(newPages, modifiedPages);

      if (changes.result != null) {
        // WRITE TO THE WAL: THE POINT OF NO RETURN
        database.getTransactionManager().writeTransactionToWAL(changes.modifiedPages, walFlush, txId, changes.result);
        // Only a REAL append crosses the point of no return (#5053/#4940): with useWAL=false (bulk loads)
        // changes.result is null, nothing is durable and there is no WAL record for recovery to replay - a
        // publish failure must then behave like any pre-durability failure (full rollback of user-held
        // record state, no fence: fencing would promise a replay that cannot happen).
        walAppended = true;
      }

      LogManager.instance()
          .log(this, Level.FINE, "TX committing pages newPages=%s modifiedPages=%s (threadId=%d)", newPages, modifiedPages,
              Thread.currentThread().threadId());

      // From here the transaction is durable in the WAL: a failure below is repaired by recovery replay,
      // never by aborting.
      database.getPageManager().publishPages(pagesToPublish, newPages, asyncFlush);

      for (final Map.Entry<Integer, Integer> entry : newPageCounters.entrySet())
        ((PaginatedComponent) database.getSchema().getFileById(entry.getKey())).updatePageCount(entry.getValue());

      // UPDATE RECORD COUNT
      bucketRecordDelta.forEach((bucketId, delta) -> {
        // THE BUCKET/FILE COULD HAVE BEEN REMOVED IN THE CURRENT TRANSACTION
        final LocalBucket bucket = (LocalBucket) database.getSchema().getFileByIdIfExists(bucketId);
        if (bucket != null && bucket.getCachedRecordCount() > -1)
          // UPDATE THE CACHE COUNTER ONLY IF ALREADY COMPUTED
          bucket.setCachedRecordCount(bucket.getCachedRecordCount() + delta);
      });

      for (final Record r : modifiedRecordsCache.values())
        ((RecordInternal) r).unsetDirty();

      for (final int fileId : lockedFiles) {
        final PaginatedComponent file = (PaginatedComponent) database.getSchema().getFileByIdIfExists(fileId);
        if (file != null)
          // THE FILE COULD BE NULL IN CASE OF INDEX COMPACTION
          file.onAfterCommit();
      }

      committed = true;

    } catch (final ConcurrentModificationException e) {
      throw e;
    } catch (final TransactionException e) {
      // Already a first-class transaction error (e.g. the recovery fence): rethrow as-is instead of
      // double-wrapping it in a generic "Transaction error on commit" (#5053 review, same as phase 1).
      throw e;
    } catch (final Exception e) {
      LogManager.instance()
          .log(this, Level.FINE, "Unknown exception during commit (threadId=%d)", e, Thread.currentThread().threadId());
      throw new TransactionException("Transaction error on commit", e);
    } finally {
      if (committed)
        resetAndFireCallbacks();
      else if (walAppended) {
        // #5053: the transaction IS durable (its WAL record survives and recovery will replay it) but its
        // pages may not all be published - the live state and the WAL may diverge. Fence BEFORE reset()
        // releases the file locks: the volatile write is then visible to any transaction that was blocked
        // on these locks, so no conflicting WAL record for the same page versions can follow. The orphaned
        // record's pages were never flush-acked, so the ack-gated close (#4928) preserves the WAL and the
        // lock file, and reopening the database replays it.
        //
        // DELIBERATELY WIDE (covers every failure from the append to the end of the try, not just
        // publishPages): a mid-publish failure can leave SOME pages visible and others not - a partial
        // publication no boolean flag can distinguish from a complete one - and post-publish failures
        // (page counters, onAfterCommit) leave component metadata that recovery replay also repairs.
        // Under-fencing risks exactly the divergence this exists to prevent; the cost (a restart after an
        // exceptional post-append failure) is the acceptable side.
        if (database.getEmbedded() instanceof LocalDatabase localDatabase)
          localDatabase.fenceForRecovery("commit of tx " + txId + " failed after its WAL append");
        // The RIDs optimistically assigned to records created in this transaction remain valid (the replay
        // makes them real): release resources WITHOUT touching user-held record state.
        reset();
      } else if (remotelyCommitted)
        // #5064: the replication QUORUM already durably committed this transaction cluster-wide; only the
        // LOCAL apply failed, before anything local was durable. The records the user holds carry the
        // identities the cluster committed, so rollback()'s identity reset would invite an application
        // retry to INSERT DUPLICATES of already-committed records - release resources WITHOUT touching
        // user-held record state. No fence in THIS branch because it is only reachable pre-append (no
        // orphaned local WAL record to diverge from; the Raft layer reconciles the pages from the
        // replicated payload). A failure AFTER the local append takes the walAppended branch above and
        // still fences, INTENTIONALLY: an orphaned local WAL record exists there regardless of the remote
        // commit, and that branch also preserves identities via reset(). Unlike the #4940 rollback below,
        // modified records are intentionally NOT reloaded: their in-memory content is exactly what the
        // cluster committed, so there is nothing to restore.
        reset();
      else if (database.getEmbedded() instanceof LocalDatabase localDatabase && localDatabase.isFencedForRecovery())
        // A fence-REFUSED commit (this tx appended nothing; the fence came from an earlier failure) cannot
        // roll back its record state: rollback()'s record reload would hit the fence choke point itself and
        // replace the fence error with a confusing secondary failure. Release resources only - the database
        // is unusable until close/reopen anyway, so user-held record state is moot.
        reset();
      else
        // #4940: the failure happened BEFORE anything durable exists. Restore user-held records exactly like
        // a phase-1 failure does: reload the modified records to their committed content and reset the
        // identity of records created in this transaction to provisional, so a retry re-inserts them instead
        // of updating a record that was never persisted (#4562).
        try {
          rollback();
        } catch (final Throwable rollbackError) {
          // #5061: the cleanup must never SUPPRESS the primary commit exception - e.g. a failed
          // dictionary reload here would surface a retryable ConcurrentModificationException as a
          // non-retryable SchemaException, breaking the caller's retry loop. Log the secondary failure and
          // degrade to reset() so locks and status are still released (reset() is safe after a partial
          // rollback: it null-guards the lock lists and only releases what is still held).
          LogManager.instance().log(this, Level.WARNING,
              "Error during phase-2 failure rollback (the primary commit error is propagated)", rollbackError);
          // The #4940 core must survive the degraded path too: rollback() can only throw at the dictionary
          // reload, which runs BEFORE its identity reset - without this, records created in the failed tx
          // would keep their dangling RID, the exact defect #4940 fixes. The loop is cheap and cannot throw.
          // Documented asymmetry vs the happy rollback(): user-held MODIFIED records are NOT reloaded here
          // (that loop also never ran) and keep their uncommitted in-memory content - the safest available
          // degradation, since reloading is exactly what just failed.
          for (final Record newRecord : newRecords)
            ((RecordInternal) newRecord).setIdentity(null);
          reset();
        }
    }
  }

  public void addIndexOperation(final IndexInternal index, final TransactionIndexContext.IndexKey.IndexKeyOperation operation,
      final Object[] keys, final RID rid) {
    indexChanges.addIndexKeyLock(index, operation, keys, rid);
  }

  @Override
  public boolean isAsyncFlush() {
    return asyncFlush;
  }

  @Override
  public void setAsyncFlush(final boolean value) {
    this.asyncFlush = value;
  }

  public void reset() {
    remotelyCommitted = false;
    status = STATUS.INACTIVE;

    if (explicitLockedFiles != null) {
      database.getTransactionManager().unlockFilesInOrder(explicitLockedFiles, getRequester());
      explicitLockedFiles = null;
    }

    if (lockedFiles != null) {
      database.getTransactionManager().unlockFilesInOrder(lockedFiles, getRequester());
      lockedFiles = null;
    }

    indexChanges.reset();
    releaseInsertSlotReservations();

    modifiedPages = null;
    newPages = null;
    edgeAppendsBySegment = null;
    edgeAppendPoisonedPages = null;
    slotRebaseByPage = null;
    slotRebasePoisonedPages = null;
    slotRebaseTrackedBytes = 0;
    updatedRecords = null;
    updatedRecordsIndexSnapshot = null;
    newPageCounters.clear();
    modifiedRecordsCache.clear();
    immutableRecordsCache.clear();
    immutablePages.clear();
    bucketRecordDelta.clear();
    deletedRecordsInTx.clear();
    newRecords.clear();
    afterCommitCallbacks = null;
    registeredCallbackKeys = null;
    txId = -1;
  }

  public void removeFile(final int fileId) {
    if (newPages != null)
      newPages.values().removeIf(mutablePage -> fileId == mutablePage.getPageId().getFileId());

    newPageCounters.remove(fileId);

    if (modifiedPages != null)
      modifiedPages.values().removeIf(mutablePage -> fileId == mutablePage.getPageId().getFileId());

    immutablePages.values().removeIf(page -> fileId == page.getPageId().getFileId());

    // IMMUTABLE RECORD, AVOID IT'S POINTING TO THE OLD OFFSET IN A MODIFIED PAGE
    // SAME PAGE, REMOVE IT
    immutableRecordsCache.values().removeIf(r -> r.getIdentity().getBucketId() == fileId);

    if (lockedFiles != null)
      lockedFiles.remove(Integer.valueOf(fileId));

    if (updatedRecords != null)
      // FILE DELETED: REMOVE ALL PENDING UPDATED OBJECTS
      updatedRecords.entrySet().removeIf(entry -> entry.getKey().bucketId == fileId);

    final PaginatedComponent component = (PaginatedComponent) database.getSchema().getFileByIdIfExists(fileId);
    if (component instanceof LSMTreeIndexAbstract)
      indexChanges.removeIndex(component.getName());
  }

  public TransactionIndexContext getIndexChanges() {
    return indexChanges;
  }

  public STATUS getStatus() {
    return status;
  }

  /**
   * Whether the index operations queued on this transaction are replayed at the end of the 1st commit phase. True on
   * a leader (and on any non-replicated database); false on a replica, where the index pages come from the leader's
   * changes and {@link TransactionIndexContext#commit()} is never invoked. An index consulted during
   * {@link STATUS#COMMIT_1ST_PHASE} - the phase that re-runs {@code DocumentIndexer.updateDocument} while
   * serializing the records updated in this transaction - uses this to tell whether it can leave the work to the
   * replay instead of applying it a second time (issue #5516).
   */
  public boolean isIndexChangesReplayed() {
    return indexChangesReplayed;
  }

  public void setStatus(final STATUS status) {
    this.status = status;
  }

  protected void explicitLock(final IntHashSet filesToLock) {
    if (explicitLockedFiles != null)
      throw new TransactionException("Explicit lock already acquired");

    if (!indexChanges.isEmpty() || !immutablePages.isEmpty() || //
        (modifiedPages != null && !modifiedPages.isEmpty()) || //
        (newPages != null && !newPages.isEmpty()) //
    )
      throw new TransactionException("Explicit lock must be acquired before any modification");

    explicitLockedFiles = lockFilesInOrder(filesToLock);
  }

  /**
   * Allocation-free containment scan for the late-joiner fast path (lockedFiles is small, typically < 10):
   * get(i) == fileId unboxes the stored Integer but allocates nothing new. Assumes a RandomAccess list -
   * lockFilesInOrder and checkExplicitLocks both produce ArrayLists; revisit the loop if that ever changes.
   */
  private static boolean containsFileId(final List<Integer> lockedFiles, final int fileId) {
    for (int i = 0; i < lockedFiles.size(); i++)
      if (lockedFiles.get(i) == fileId)
        return true;
    return false;
  }

  private IntHashSet lockFilesFromChanges() {
    final IntHashSet modifiedFiles = new IntHashSet(modifiedPages.size() + 16);

    for (final PageId p : modifiedPages.keySet())
      modifiedFiles.add(p.getFileId());
    if (newPages != null)
      for (final PageId p : newPages.keySet())
        modifiedFiles.add(p.getFileId());

    indexChanges.addFilesToLock(modifiedFiles);

    for (final Integer fid : newPageCounters.keySet())
      modifiedFiles.add(fid);

    return modifiedFiles;
  }

  private List<Integer> lockFilesInOrder(final IntHashSet files) {
    final long timeout = database.getConfiguration().getValueAsLong(GlobalConfiguration.COMMIT_LOCK_TIMEOUT);
    final LocalSchema schema = database.getSchema().getEmbedded();

    // Work on a private copy so the caller's set is never mutated by the migration re-resolution below
    // (the explicit-lock path passes its own filesToLock set).
    IntHashSet filesToLock = new IntHashSet(files.size() + 4);
    files.forEach(filesToLock::add);

    // Bounded internal retry for the snapshot-vs-lock race against a background index compaction.
    // A compaction can migrate an index file (registerFile + setMigratedFileId, old file removed) between the
    // moment the file id is resolved (lockFilesFromChanges/addFilesToLock or an explicit-lock collection) and the
    // moment we acquire and verify the lock here. When that happens the old file id no longer exists. Re-resolving
    // the migrated id and retrying converges transparently without surfacing a ConcurrentModificationException to
    // the caller: at this point no transaction modification has been applied yet (record updates and index entries
    // are written later in commit1stPhase, after this method returns), so re-locking is side-effect free. The new
    // file id is authoritative because the buffered index entries are resolved by index name at commit time and
    // therefore land in the current (migrated) file regardless of which id we lock here.
    for (int attempt = 0; ; ++attempt) {
      // ACQUISITION PATH: CAPTURES THE REQUESTER ON THE OWNER THREAD (SEE THE INVARIANT ON THE FIELD)
      final List<Integer> locked = database.getTransactionManager().tryLockFiles(filesToLock.toArray(), timeout, captureRequester());

      // CHECK IF ALL THE LOCKED FILES STILL EXIST. FILE MISSING CAN HAPPEN IN CASE OF INDEX COMPACTION OR DROP OF A BUCKET OR AN INDEX.
      int missingFile = -1;
      for (final Integer f : locked)
        if (!database.getFileManager().existsFile(f)) {
          missingFile = f;
          break;
        }

      if (missingFile == -1)
        return locked;

      // FOUND A MISSING FILE: RELEASE EVERY LOCK ACQUIRED IN THIS ATTEMPT BEFORE DECIDING WHAT TO DO.
      database.getTransactionManager().unlockFilesInOrder(locked, getRequester());

      final Integer migrated = schema.getMigratedFileId(missingFile);
      if (migrated == null)
        throw new ConcurrentModificationException("File with id '" + missingFile + "' has been removed");

      if (attempt >= MAX_LOCK_MIGRATION_RETRIES)
        throw new ConcurrentModificationException(
            "Error on commit transaction: file '" + missingFile + "' has been migrated to '" + migrated
                + "' (likely by an index compaction). Please retry the operation.");

      LogManager.instance().log(this, Level.FINE,
          "Retrying lock acquisition after compaction migrated file '%d' to '%d' (attempt %d/%d, threadId=%d)", missingFile, migrated,
          attempt + 1, MAX_LOCK_MIGRATION_RETRIES, Thread.currentThread().threadId());

      // RE-RESOLVE THE MIGRATED FILE ID AND RETRY TRANSPARENTLY. IntHashSet has no remove(), so rebuild the set
      // replacing the migrated id (rare path: only taken when a compaction raced this commit).
      final int removed = missingFile;
      final IntHashSet next = new IntHashSet(filesToLock.size() + 1);
      filesToLock.forEach(id -> {
        if (id != removed)
          next.add(id);
      });
      next.add(migrated);
      filesToLock = next;
    }
  }

  /**
   * #4937: late-joiner coverage check for an EXPLICIT-lock transaction. By this point the earlier
   * {@link #checkExplicitLocks} has moved the user's explicit lock set into {@code lockedFiles} (and nulled
   * {@code explicitLockedFiles}), so a file in the transaction's page set that is NOT in {@code lockedFiles}
   * is one the user did not lock. Surface it as the same contract violation a direct modification of an
   * unlocked file raises, rather than silently expanding their lock set.
   * <p>
   * Unlike {@link #checkExplicitLocks}, no migrated-file (index compaction) translation is needed here: that
   * race lives in the window between snapshotting the file ids and acquiring the locks, and by this point
   * the locks have been held continuously since the user's {@code lock()} call. Compaction's splitIndex must
   * {@code tryLockFile} the file it migrates, so no file in {@code lockedFiles} can have been migrated while
   * this transaction runs - a missing file here is genuinely unlocked, never a stale id of a locked one.
   */
  private void checkExplicitLocksForUnion(final IntHashSet union) {
    final Set<Integer> locked = new HashSet<>(lockedFiles);
    final HashSet<Integer> left = new HashSet<>();
    union.forEach(fid -> {
      if (!locked.contains(fid))
        left.add(fid);
    });
    if (!left.isEmpty()) {
      if (allUncoveredAreLockedIndexSiblings(left, locked))
        throw new ConcurrentModificationException(
            "Error on commit transaction: an index component was migrated by a concurrent compaction, please retry the operation");

      // TreeSet: deterministic name ordering, so a multi-file violation always reads the same.
      final Set<String> resourceNames = left.stream().map(fileId -> database.getSchema().getFileById(fileId).getName())
          .collect(Collectors.toCollection(TreeSet::new));
      throw new TransactionException(
          "Cannot commit transaction because not all the modified resources were locked: " + resourceNames);
    }
  }

  private void checkExplicitLocks(final IntHashSet modifiedFiles) {
    // CHECK THE LOCKED FILES ARE ALL LOCKED ALREADY: every modified file must already be in explicitLockedFiles.
    final boolean[] missing = { false };
    modifiedFiles.forEach(fid -> {
      if (!missing[0] && !explicitLockedFiles.contains(fid))
        missing[0] = true;
    });
    if (missing[0]) {
      boolean anyMigration = false;
      // CHECK FOR ANY MIGRATED FILES (INDEX COMPACTION)
      final List<Integer> migratedFileIds = new ArrayList<>(explicitLockedFiles.size());
      for (Integer f : explicitLockedFiles) {
        migratedFileIds.add(f);

        final Integer newFileId = ((LocalSchema) database.getSchema()).getMigratedFileId(f);
        if (newFileId != null) {
          migratedFileIds.add(newFileId);
          LogManager.instance().log(this, Level.FINE, "Found upgraded file '%d' to '%d' during transaction lock", f, newFileId);
          anyMigration = true;
        }
      }

      if (anyMigration) {
        // Check if EVERY modifiedFiles entry is in migratedFileIds
        final HashSet<Integer> migratedSet = new HashSet<>(migratedFileIds);
        final boolean[] allMigrated = { true };
        modifiedFiles.forEach(fid -> {
          if (allMigrated[0] && !migratedSet.contains(fid))
            allMigrated[0] = false;
        });
        if (allMigrated[0])
          // FOUND MIGRATED FILE(S), FORCE THE CLIENT TO RETRY THE OPERATION
          throw new ConcurrentModificationException(
              "Error on commit transaction: some files have been migrated, please retry the operation");
      }

      // ERROR: NOT ALL THE MODIFIED FILES ARE LOCKED
      final HashSet<Integer> left = new HashSet<>(modifiedFiles.size());
      modifiedFiles.forEach(left::add);
      left.removeAll(explicitLockedFiles);

      if (allUncoveredAreLockedIndexSiblings(left, new HashSet<>(explicitLockedFiles)))
        throw new ConcurrentModificationException(
            "Error on commit transaction: an index component was migrated by a concurrent compaction, please retry the operation");

      final Set<String> resourceNames = left.stream().map(fileId -> database.getSchema().getFileById(fileId).getName())
          .collect(Collectors.toSet());

      throw new TransactionException(
          "Cannot commit transaction because not all the modified resources were locked: " + resourceNames);
    }

    lockedFiles = explicitLockedFiles;
    explicitLockedFiles = null;
  }

  /**
   * Distinguishes a benign compaction race from a genuine explicit-lock contract violation. A modified file
   * left uncovered by the explicit locks is a retryable conflict - not a user error - when it is a current
   * component of an index whose OTHER component this transaction already holds locked. A background
   * compaction that migrated an index's mutable between the LOCK snapshot and the lock acquisition also
   * creates a fresh compacted sub-index whose file id the snapshot could not have known; the mutable lock is
   * held, so re-running the LOCK block (COMMIT RETRY) re-resolves the now-stable component set and commits.
   * A file belonging to an index with NO locked component is a real "modified an unlocked resource"
   * violation and stays a hard error.
   */
  private boolean allUncoveredAreLockedIndexSiblings(final Set<Integer> left, final Set<Integer> lockedFiles) {
    for (final Integer uncovered : left)
      if (!isLockedIndexSibling(uncovered, lockedFiles))
        return false;
    return true;
  }

  private boolean isLockedIndexSibling(final int fileId, final Set<Integer> lockedFiles) {
    for (final Index typeIndex : database.getSchema().getIndexes()) {
      if (!(typeIndex instanceof final TypeIndex ti))
        continue;
      for (final IndexInternal bucketIndex : ti.getIndexesOnBuckets()) {
        final List<Integer> components = bucketIndex.getFileIds();
        if (components.contains(fileId))
          for (final Integer c : components)
            if (c != fileId && lockedFiles.contains(c))
              return true;
      }
    }
    return false;
  }

  /**
   * Marks a RID as deleted in the current transaction to prevent its reuse before commit.
   *
   * @param rid The RID that was deleted
   */
  public void addDeletedRecord(final RID rid) {
    deletedRecordsInTx.add(rid);
    // A record deleted after an in-tx update no longer needs its indexed-state snapshot (#4935): the delete
    // removes the index entries through its own path, so drop the retained memory right away.
    if (updatedRecordsIndexSnapshot != null)
      updatedRecordsIndexSnapshot.remove(rid);
  }

  /**
   * Checks if a RID was deleted in the current transaction.
   *
   * @param rid The RID to check
   * @return true if the RID was deleted in this transaction, false otherwise
   */
  public boolean isDeletedInTransaction(final RID rid) {
    return deletedRecordsInTx.contains(rid);
  }
}
