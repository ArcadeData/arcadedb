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
import com.arcadedb.engine.Bucket;
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
import com.arcadedb.schema.Schema;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

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
  private final Map<Integer, Integer>                newPageCounters       = new HashMap<>();
  private final Map<Integer, AtomicInteger>          bucketRecordDelta     = new HashMap<>();
  private final Map<RID, Record>                     immutableRecordsCache = new HashMap<>(1024);
  private final Map<RID, Record>                     modifiedRecordsCache  = new HashMap<>(1024);
  private final TransactionIndexContext              indexChanges;
  private final Map<PageId, ImmutablePage>           immutablePages        = new HashMap<>(64);
  private       Map<PageId, MutablePage>             modifiedPages;
  private       Map<PageId, MutablePage>             newPages;
  private       boolean                              useWAL;
  private       boolean                              asyncFlush            = true;
  private       WALFile.FLUSH_TYPE                   walFlush;
  private       List<Integer>                        lockedFiles;
  private       long                                 txId                  = -1;
  private       STATUS                               status                = STATUS.INACTIVE;
  // KEEPS TRACK OF MODIFIED RECORD IN TX. AT 1ST PHASE COMMIT TIME THE RECORD ARE SERIALIZED AND INDEXES UPDATED. THIS DEFERRING IMPROVES SPEED ESPECIALLY
  // WITH GRAPHS WHERE EDGES ARE CREATED AND CHUNKS ARE UPDATED MULTIPLE TIMES IN THE SAME TX
  // TODO: OPTIMIZE modifiedRecordsCache STRUCTURE, MAYBE JOIN IT WITH UPDATED RECORDS?
  private       Map<RID, Record>                     updatedRecords        = null;
  private       Database.TRANSACTION_ISOLATION_LEVEL isolationLevel        = Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED;

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

    modifiedPages = new HashMap<>();

    if (newPages == null)
      // KEEP ORDERING IN CASE MULTIPLE PAGES FOR THE SAME FILE ARE CREATED
      newPages = new LinkedHashMap<>();
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
      reset();

    if (database.getSchema().getEmbedded().isDirty())
      database.getSchema().getEmbedded().saveConfiguration();

    return phase1 != null ? phase1.result : null;
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
  public void setWALFlush(final WALFile.FLUSH_TYPE flush) {
    this.walFlush = flush;
  }

  @Override
  public void rollback() {
    LogManager.instance()
        .log(this, Level.FINE, "Rollback transaction newPages=%s modifiedPages=%s (threadId=%d)", newPages, modifiedPages,
            Thread.currentThread().getId());

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
    final MutablePage page = new MutablePage(database.getPageManager(), pageId, pageSize);
    newPages.put(pageId, page);

    final Integer indexCounter = newPageCounters.get(pageId.getFileId());
    if (indexCounter == null || indexCounter < pageId.getPageNumber() + 1)
      newPageCounters.put(pageId.getFileId(), pageId.getPageNumber() + 1);

    return page;
  }

  public long getFileSize(final int fileId) throws IOException {
    final Integer lastPage = newPageCounters.get(fileId);
    if (lastPage != null)
      return (long) (lastPage + 1) * ((PaginatedComponentFile) database.getFileManager().getFile(fileId)).getPageSize();

    return database.getFileManager().getVirtualFileSize(fileId);
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
    lockedFiles = null;
    modifiedPages = null;
    newPages = null;
    updatedRecords = null;
    newPageCounters.clear();
    immutablePages.clear();
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

        final PageId pageId = new PageId(p.fileId, p.pageNumber);

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

    final int totalImpactedPages = modifiedPages.size() + (newPages != null ? newPages.size() : 0);
    if (totalImpactedPages == 0 && indexChanges.isEmpty()) {
      // EMPTY TRANSACTION = NO CHANGES
      return null;
    }

    status = STATUS.COMMIT_1ST_PHASE;

    if (isLeader)
      // LOCK FILES IN ORDER (TO AVOID DEADLOCK)
      lockedFiles = lockFilesInOrder();
    else
      // IN CASE OF REPLICA THIS IS DEMANDED TO THE LEADER EXECUTION
      lockedFiles = new ArrayList<>();

    try {
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
          bucket.compressPage(page);
      }
      for (MutablePage page : newPages.values()) {
        final LocalBucket bucket = localSchema.getBucketById(page.getPageId().getFileId(), false);
        if (bucket != null)
          bucket.compressPage(page);
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
        //LogManager.instance().log(this, Level.FINE, "Creating buffer for TX %d (threadId=%d)", txId, Thread.currentThread().getId());
        result = database.getTransactionManager().createTransactionBuffer(txId, pages);
      }

      return new TransactionPhase1(result, pages);

    } catch (final DuplicatedKeyException | ConcurrentModificationException e) {
      rollback();
      throw e;
    } catch (final Exception e) {
      LogManager.instance()
          .log(this, Level.FINE, "Unknown exception during commit (threadId=%d)", e, Thread.currentThread().getId());
      rollback();
      throw new TransactionException("Transaction error on commit", e);
    }
  }

  public void commit2ndPhase(final TransactionContext.TransactionPhase1 changes) {
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
              Thread.currentThread().getId());

      database.getPageManager().updatePages(newPages, modifiedPages, asyncFlush);

      if (newPages != null) {
        for (final Map.Entry<Integer, Integer> entry : newPageCounters.entrySet()) {
          final PaginatedComponent component = (PaginatedComponent) database.getSchema().getFileById(entry.getKey());
          component.setPageCount(entry.getValue());
          database.getFileManager().setVirtualFileSize(entry.getKey(),
              (long) entry.getValue() * ((PaginatedComponentFile) database.getFileManager().getFile(entry.getKey())).getPageSize());
        }
      }

      // UPDATE RECORD COUNT
      for (Map.Entry<Integer, AtomicInteger> entry : bucketRecordDelta.entrySet()) {
        final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(entry.getKey());
        if (bucket.getCachedRecordCount() > -1)
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

    } catch (final ConcurrentModificationException e) {
      throw e;
    } catch (final Exception e) {
      LogManager.instance()
          .log(this, Level.FINE, "Unknown exception during commit (threadId=%d)", e, Thread.currentThread().getId());
      throw new TransactionException("Transaction error on commit", e);
    } finally {
      reset();
    }
  }

  public void addIndexOperation(final IndexInternal index, final boolean addOperation, final Object[] keys, final RID rid) {
    indexChanges.addIndexKeyLock(index, addOperation, keys, rid);
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

    if (lockedFiles != null) {
      database.getTransactionManager().unlockFilesInOrder(lockedFiles);
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
    txId = -1;
  }

  public void removePagesOfFile(final int fileId) {
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

  private List<Integer> lockFilesInOrder() {
    final Set<Integer> modifiedFiles = new HashSet<>();

    for (final PageId p : modifiedPages.keySet())
      modifiedFiles.add(p.getFileId());
    if (newPages != null)
      for (final PageId p : newPages.keySet())
        modifiedFiles.add(p.getFileId());

    indexChanges.addFilesToLock(modifiedFiles);

    modifiedFiles.addAll(newPageCounters.keySet());

    final long timeout = database.getConfiguration().getValueAsLong(GlobalConfiguration.COMMIT_LOCK_TIMEOUT);

    final List<Integer> locked = database.getTransactionManager().tryLockFiles(modifiedFiles, timeout);

    // CHECK IF ALL THE LOCKED FILES STILL EXIST. FILE MISSING CAN HAPPEN IN CASE OF INDEX COMPACTION OR DROP OF A BUCKET OR AN INDEX
    for (Integer f : locked)
      if (!database.getFileManager().existsFile(f)) {
        // ONE FILE HAS BEEN REMOVED
        database.getTransactionManager().unlockFilesInOrder(locked);
        rollback();
        throw new ConcurrentModificationException("File with id '" + f + "' has been removed");
      }

    return locked;
  }
}
