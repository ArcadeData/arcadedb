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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.utility.IntHashSet;
import com.arcadedb.utility.IntIntHashMap;
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
  private       boolean                              useWAL;
  private       boolean                              asyncFlush            = true;
  private       WALFile.FlushType                    walFlush;
  private       List<Integer>                        lockedFiles;
  private       List<Integer>                        explicitLockedFiles   = null;
  private       long                                 txId                  = -1;
  private       STATUS                               status                = STATUS.INACTIVE;
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
  private       Object                               requester;
  private       List<Runnable>                       afterCommitCallbacks        = null;
  private       Set<String>                          registeredCallbackKeys      = null;

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
    this.isolationLevel = isolationLevel;

    if (status != STATUS.INACTIVE)
      throw new TransactionException("Transaction already begun");

    status = STATUS.BEGUN;

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

  public void setRequester(final Object requester) {
    this.requester = requester;
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
    lockedFiles = null;
    modifiedPages = null;
    newPages = null;
    updatedRecords = null;
    updatedRecordsIndexSnapshot = null;
    newPageCounters.clear();
    immutablePages.clear();
  }

  private Object getRequester() {
    return requester != null ? requester : Thread.currentThread();
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

    try {
      if (isLeader) {
        // Determine files to lock — must include files from updatedRecords
        if (updatedRecords != null)
          for (final Record rec : updatedRecords.values()) {
            final RID rid = rec.getIdentity();
            ((LocalBucket) database.getSchema().getBucketById(rid.getBucketId())).fetchPageInTransaction(rid);
          }

        final IntHashSet modifiedFiles = lockFilesFromChanges();

        if (explicitLockedFiles != null)
          checkExplicitLocks(modifiedFiles);
        else
          // LOCK FILES IN ORDER (TO AVOID DEADLOCK)
          lockedFiles = lockFilesInOrder(modifiedFiles);

      } else
        // IN CASE OF REPLICA THIS IS DEMANDED TO THE LEADER EXECUTION
        lockedFiles = new ArrayList<>();

      // Process updatedRecords AFTER acquiring locks
      if (updatedRecords != null) {
        for (final Record rec : updatedRecords.values())
          try {
            database.updateRecordNoLock(rec, false);
          } catch (final RecordNotFoundException e) {
            LogManager.instance()
                .log(this, Level.WARNING, "Attempt to update the delete record %s in transaction", rec.getIdentity());
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

      for (final Iterator<MutablePage> it = modifiedPages.values().iterator(); it.hasNext(); ) {
        final MutablePage p = it.next();

        final int[] range = p.getModifiedRange();
        if (range[1] > 0) {
          pageManager.checkPageVersion(p, false);
          pages.add(p);
        } else
          // PAGE NOT MODIFIED, REMOVE IT
          it.remove();
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

    } catch (final DuplicatedKeyException | ConcurrentModificationException e) {
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
    boolean committed = false;
    try {
      if (changes == null)
        return;

      if (database.getMode() == ComponentFile.MODE.READ_ONLY)
        throw new TransactionException("Cannot commit changes because the database is open in read-only mode");

      if (status != STATUS.COMMIT_1ST_PHASE)
        throw new TransactionException("Cannot execute 2nd phase commit without having started the 1st phase");

      status = STATUS.COMMIT_2ND_PHASE;

      if (changes.result != null)
        // WRITE TO THE WAL FIRST
        database.getTransactionManager().writeTransactionToWAL(changes.modifiedPages, walFlush, txId, changes.result);

      // AT THIS POINT, LOCK + VERSION CHECK, THERE IS NO NEED TO MANAGE ROLLBACK BECAUSE THERE CANNOT BE CONCURRENT TX THAT UPDATE THE SAME PAGE CONCURRENTLY
      // UPDATE PAGE COUNTER FIRST
      LogManager.instance()
          .log(this, Level.FINE, "TX committing pages newPages=%s modifiedPages=%s (threadId=%d)", newPages, modifiedPages,
              Thread.currentThread().threadId());

      database.getPageManager().updatePages(newPages, modifiedPages, asyncFlush);

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
    } catch (final Exception e) {
      LogManager.instance()
          .log(this, Level.FINE, "Unknown exception during commit (threadId=%d)", e, Thread.currentThread().threadId());
      throw new TransactionException("Transaction error on commit", e);
    } finally {
      if (committed)
        resetAndFireCallbacks();
      else
        reset();
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

    modifiedPages = null;
    newPages = null;
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
      final List<Integer> locked = database.getTransactionManager().tryLockFiles(filesToLock.toArray(), timeout, getRequester());

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

      final Set<String> resourceNames = left.stream().map(fileId -> database.getSchema().getFileById(fileId).getName())
          .collect(Collectors.toSet());

      throw new TransactionException(
          "Cannot commit transaction because not all the modified resources were locked: " + resourceNames);
    }

    lockedFiles = explicitLockedFiles;
    explicitLockedFiles = null;
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
