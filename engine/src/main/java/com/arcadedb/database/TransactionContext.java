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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
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
  private final DatabaseInternal                     database;
  private final Map<Integer, Integer>                newPageCounters       = new ConcurrentHashMap<>();
  private final Map<Integer, AtomicInteger>          bucketRecordDelta     = new HashMap<>();
  private final Map<RID, Record>                     immutableRecordsCache = new HashMap<>(1024);
  private final Map<RID, Record>                     modifiedRecordsCache  = new HashMap<>(1024);
  private final TransactionIndexContext              indexChanges;
  private final Map<PageId, ImmutablePage>           immutablePages        = new HashMap<>(64);
  private final Set<RID>                             deletedRecordsInTx    = new HashSet<>();
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
  private       Map<RID, Record>                     updatedRecords        = null;
  private       Database.TRANSACTION_ISOLATION_LEVEL isolationLevel        = Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED;
  private       LocalTransactionExplicitLock         explicitLock;
  private       Object                               requester;
  private       List<Runnable>                       afterCommitCallbacks  = null;
  private       Set<String>                          registeredCallbackKeys = null;

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
    if (phase1 != null)
      commit2ndPhase(phase1);
    else
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

    // RELOAD PREVIOUS VERSION OF MODIFIED RECORDS
    if (database.isOpen())
      for (final Record r : modifiedRecordsCache.values())
        try {
          r.reload();
        } catch (final Exception e) {
          // IGNORE EXCEPTION (RECORD DELETED OR TYPE REMOVED)
        }

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
    newPageCounters.clear();
    immutablePages.clear();
  }

  private Object getRequester() {
    return requester != null ? requester : Thread.currentThread();
  }

  public Map<Integer, Integer> getBucketRecordDelta() {
    final Map<Integer, Integer> map = new HashMap<>(bucketRecordDelta.size());
    for (Map.Entry<Integer, AtomicInteger> entry : bucketRecordDelta.entrySet())
      map.put(entry.getKey(), entry.getValue().get());
    return map;
  }

  /**
   * Returns the delta of records considering the pending changes in transaction.
   */
  public long getBucketRecordDelta(final int bucketId) {
    final AtomicInteger delta = bucketRecordDelta.get(bucketId);
    if (delta != null)
      return delta.get();
    return 0;
  }

  /**
   * Updates the record counter for buckets. At transaction commit, the delta is updated into the schema.
   */
  public void updateBucketRecordDelta(final int bucketId, final int delta) {
    AtomicInteger counter = bucketRecordDelta.get(bucketId);
    if (counter == null) {
      counter = new AtomicInteger(delta);
      bucketRecordDelta.put(bucketId, counter);
    } else
      counter.addAndGet(delta);
  }

  /**
   * Executes 1st phase from a replica.
   */
  public void commitFromReplica(final WALFile.WALTransaction buffer,
      final Map<String, TreeMap<TransactionIndexContext.ComparableKey, Map<TransactionIndexContext.IndexKey, TransactionIndexContext.IndexKey>>> keysTx,
      final Map<Integer, Integer> bucketRecordDelta) throws TransactionException {

    for (Map.Entry<Integer, Integer> entry : bucketRecordDelta.entrySet())
      this.bucketRecordDelta.put(entry.getKey(), new AtomicInteger(entry.getValue()));

    final int totalImpactedPages = buffer.pages.length;
    if (totalImpactedPages == 0 && keysTx.isEmpty()) {
      // EMPTY TRANSACTION = NO CHANGES
      modifiedPages = null;
      return;
    }

    try {
      // LOCK FILES (IN ORDER, SO TO AVOID DEADLOCK)
      final Set<Integer> modifiedFiles = new HashSet<>();

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

    if (updatedRecords != null) {
      for (final Record rec : updatedRecords.values())
        try {
          database.updateRecordNoLock(rec, false);
        } catch (final RecordNotFoundException e) {
          // DELETED IN TRANSACTION, THIS IS FULLY MANAGED TO NEVER HAPPEN, BUT IF IT DOES DUE TO THE INTRODUCTION OF A BUG, JUST LOG SOMETHING AND MOVE ON
          LogManager.instance()
              .log(this, Level.WARNING, "Attempt to update the delete record %s in transaction", rec.getIdentity());
        }
      updatedRecords = null;
    }

    if (!hasChanges())
      // EMPTY TRANSACTION = NO CHANGES
      return null;

    status = STATUS.COMMIT_1ST_PHASE;

    try {
      if (isLeader) {
        final Set<Integer> modifiedFiles = lockFilesFromChanges();

        if (explicitLockedFiles != null)
          checkExplicitLocks(modifiedFiles);
        else
          // LOCK FILES IN ORDER (TO AVOID DEADLOCK)
          lockedFiles = lockFilesInOrder(modifiedFiles);

      } else
        // IN CASE OF REPLICA THIS IS DEMANDED TO THE LEADER EXECUTION
        lockedFiles = new ArrayList<>();

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
      for (Map.Entry<Integer, AtomicInteger> entry : bucketRecordDelta.entrySet()) {
        // THE BUCKET/FILE COULD HAVE BEEN REMOVED IN THE CURRENT TRANSACTION
        final LocalBucket bucket = (LocalBucket) database.getSchema().getFileByIdIfExists(entry.getKey());
        if (bucket != null && bucket.getCachedRecordCount() > -1)
          // UPDATE THE CACHE COUNTER ONLY IF ALREADY COMPUTED
          bucket.setCachedRecordCount(bucket.getCachedRecordCount() + entry.getValue().get());
      }

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
    newPageCounters.clear();
    modifiedRecordsCache.clear();
    immutableRecordsCache.clear();
    immutablePages.clear();
    bucketRecordDelta.clear();
    deletedRecordsInTx.clear();
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
      lockedFiles.remove(fileId);

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

  protected void explicitLock(final Set<Integer> filesToLock) {
    if (explicitLockedFiles != null)
      throw new TransactionException("Explicit lock already acquired");

    if (!indexChanges.isEmpty() || !immutablePages.isEmpty() || //
        (modifiedPages != null && !modifiedPages.isEmpty()) || //
        (newPages != null && !newPages.isEmpty()) //
    )
      throw new TransactionException("Explicit lock must be acquired before any modification");

    explicitLockedFiles = lockFilesInOrder(filesToLock);
  }

  private Set<Integer> lockFilesFromChanges() {
    final Set<Integer> modifiedFiles = new HashSet<>();

    for (final PageId p : modifiedPages.keySet())
      modifiedFiles.add(p.getFileId());
    if (newPages != null)
      for (final PageId p : newPages.keySet())
        modifiedFiles.add(p.getFileId());

    indexChanges.addFilesToLock(modifiedFiles);

    modifiedFiles.addAll(newPageCounters.keySet());

    return modifiedFiles;
  }

  private List<Integer> lockFilesInOrder(final Set<Integer> files) {
    final long timeout = database.getConfiguration().getValueAsLong(GlobalConfiguration.COMMIT_LOCK_TIMEOUT);

    final List<Integer> locked = database.getTransactionManager().tryLockFiles(files, timeout, getRequester());

    // CHECK IF ALL THE LOCKED FILES STILL EXIST. FILE MISSING CAN HAPPEN IN CASE OF INDEX COMPACTION OR DROP OF A BUCKET OR AN INDEX
    for (Integer f : locked)
      if (!database.getFileManager().existsFile(f)) {
        // THE FILE HAS BEEN REMOVED, CHECK IF A NEW COMPACTION WAS EXECUTED
        final Integer migratedFileIs = ((LocalSchema) database.getSchema()).getMigratedFileId(f);
        if (migratedFileIs == null) {
          database.getTransactionManager().unlockFilesInOrder(locked, getRequester());
          rollback();
          throw new ConcurrentModificationException("File with id '" + f + "' has been removed");
        }
      }

    return locked;
  }

  private void checkExplicitLocks(final Set<Integer> modifiedFiles) {
    // CHECK THE LOCKED FILES ARE ALL LOCKED ALREADY
    if (!explicitLockedFiles.containsAll(modifiedFiles)) {
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

      if (anyMigration && migratedFileIds.containsAll(modifiedFiles))
        // FOUND MIGRATED FILE(S), FORCE THE CLIENT TO RETRY THE OPERATION
        throw new ConcurrentModificationException(
            "Error on commit transaction: some files have been migrated, please retry the operation");

      // ERROR: NOT ALL THE MODIFIED FILES ARE LOCKED
      final HashSet<Integer> left = new HashSet<>(modifiedFiles);
      left.removeAll(explicitLockedFiles);

      final Set<String> resourceNames = left.stream().map((fileId -> database.getSchema().getFileById(fileId).getName()))
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
