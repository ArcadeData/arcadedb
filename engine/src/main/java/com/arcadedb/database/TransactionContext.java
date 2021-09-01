/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.*;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.Index;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.Pair;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

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
  private final DatabaseInternal         database;
  private final Map<Integer, Integer>    newPageCounters       = new HashMap<>();
  private final Map<RID, Record>         immutableRecordsCache = new HashMap<>(1024);
  private final Map<RID, Record>         modifiedRecordsCache  = new HashMap<>(1024);
  private final TransactionIndexContext  indexChanges;
  private       Map<PageId, MutablePage> modifiedPages;
  private       Map<PageId, MutablePage> newPages;
  private       boolean                  useWAL;
  private       boolean                  asyncFlush            = true;
  private       WALFile.FLUSH_TYPE       walFlush;
  private       Collection<Integer>      lockedFiles;
  private       long                     txId                  = -1;
  private       STATUS                   status                = STATUS.INACTIVE;
  // KEEPS TRACK OF MODIFIED RECORD IN TX. AT 1ST PHASE COMMIT TIME THE RECORD ARE SERIALIZED AND INDEXES UPDATED. THIS DEFERRING IMPROVES SPEED ESPECIALLY
  // WITH GRAPHS WHERE EDGES ARE CREATED AND CHUNKS ARE UPDATED MULTIPLE TIMES IN THE SAME TX
  // TODO: OPTIMIZE modifiedRecordsCache STRUCTURE, MAYBE JOIN IT WITH UPDATED RECORDS?
  private       Set<Record>              updatedRecords        = null;

  public enum STATUS {INACTIVE, BEGUN, COMMIT_1ST_PHASE, COMMIT_2ND_PHASE}

  public TransactionContext(final DatabaseInternal database) {
    this.database = database;
    this.walFlush = WALFile.getWALFlushType(database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_WAL_FLUSH));
    this.useWAL = database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL);
    this.indexChanges = new TransactionIndexContext(database);
  }

  @Override
  public void begin() {
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

    final Pair<Binary, List<MutablePage>> changes = commit1stPhase(true);

    if (changes != null)
      commit2ndPhase(changes);
    else
      reset();

    if (database.getSchema().getEmbedded().isDirty())
      database.getSchema().getEmbedded().saveConfiguration();

    return changes != null ? changes.getFirst() : null;
  }

  public Record getRecordFromCache(final RID rid) {
    Record rec = null;
    if (database.isReadYourWrites()) {
      rec = modifiedRecordsCache.get(rid);
      if (rec == null)
        rec = immutableRecordsCache.get(rid);
    }
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

    final Bucket bucket = database.getSchema().getBucketById(bucketId);

    final long pageNum = pos / bucket.getMaxRecordsInPage();

    // IMMUTABLE RECORD, AVOID IT'S POINTING TO THE OLD OFFSET IN A MODIFIED PAGE
    // SAME PAGE, REMOVE IT
    immutableRecordsCache.values()
        .removeIf(r -> r.getIdentity().getBucketId() == bucketId && r.getIdentity().getPosition() / bucket.getMaxRecordsInPage() == pageNum);
  }

  public void removeRecordFromCache(final Record record) {
    if (updatedRecords != null)
      updatedRecords.remove(record);

    if (database.isReadYourWrites()) {
      final RID rid = record.getIdentity();
      if (rid == null)
        throw new IllegalArgumentException("Cannot remove record in TX cache because it is not persistent: " + record);
      modifiedRecordsCache.remove(rid);
      immutableRecordsCache.remove(rid);
      if (updatedRecords != null)
        updatedRecords.remove(record);
    }

    removeImmutableRecordsOfSamePage(record.getIdentity());
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
    LogManager.instance().log(this, Level.FINE, "Rollback transaction newPages=%s modifiedPages=%s (threadId=%d)", null, newPages, modifiedPages,
        Thread.currentThread().getId());

    if (database.isOpen() && database.getSchema().getDictionary() != null) {
      if (modifiedPages != null) {
        final int dictionaryId = database.getSchema().getDictionary().getId();
        boolean reloadDictionary = false;

        for (PageId pageId : modifiedPages.keySet()) {
          if (dictionaryId == pageId.getFileId()) {
            reloadDictionary = true;
            break;
          }
        }

        if (reloadDictionary) {
          try {
            database.getSchema().getDictionary().reload();
          } catch (IOException e) {
            throw new SchemaException("Error on reloading schema dictionary");
          }
        }
      }
    }
    modifiedPages = null;
    newPages = null;
    updatedRecords = null;

    // RELOAD PREVIOUS VERSION OF MODIFIED RECORDS
    if (database.isOpen())
      for (Record r : modifiedRecordsCache.values())
        r.reload();

    reset();
  }

  public void assureIsActive() {
    if (!isActive())
      throw new TransactionException("Transaction not begun");
  }

  public void addUpdatedRecord(final Record record) throws IOException {
    final RID rid = record.getIdentity();

    if (updatedRecords == null)
      updatedRecords = new HashSet<>();
    if (updatedRecords.add(record))
      database.getSchema().getBucketById(rid.getBucketId()).fetchPageInTransaction(rid);
    updateRecordInCache(record);
    removeImmutableRecordsOfSamePage(record.getIdentity());
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

    if (page == null) {
      // NOT FOUND, DELEGATES TO THE DATABASE
      page = database.getPageManager().getPage(pageId, size, false, true);
      if (page != null)
        page = page.createImmutableView();
    }

    return page;
  }

  /**
   * If the page is not already in transaction tx, loads from the database and clone it locally.
   */
  public MutablePage getPageToModify(final PageId pageId, final int size, final boolean isNew) throws IOException {
    if (!isActive())
      throw new TransactionException("Transaction not active");

    MutablePage page = modifiedPages != null ? modifiedPages.get(pageId) : null;
    if (page == null) {
      if (newPages != null)
        page = newPages.get(pageId);

      if (page == null) {
        // NOT FOUND, DELEGATES TO THE DATABASE
        final BasePage loadedPage = database.getPageManager().getPage(pageId, size, isNew, true);
        if (loadedPage != null) {
          final MutablePage mutablePage = loadedPage.modify();
          if (isNew)
            newPages.put(pageId, mutablePage);
          else
            modifiedPages.put(pageId, mutablePage);
          page = mutablePage;
        }
      }
    }

    return page;
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
      return (long) (lastPage + 1) * database.getFileManager().getFile(fileId).getPageSize();

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
      for (PageId pid : modifiedPages.keySet())
        involvedFiles.add(pid.getFileId());
    for (PageId pid : newPages.keySet())
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
  }

  /**
   * Executes 1st phase from a replica.
   */
  public void commitFromReplica(final WALFile.WALTransaction buffer,
      final Map<String, TreeMap<TransactionIndexContext.ComparableKey, Set<TransactionIndexContext.IndexKey>>> keysTx) throws TransactionException {

    final int totalImpactedPages = buffer.pages.length;
    if (totalImpactedPages == 0 && keysTx.isEmpty()) {
      // EMPTY TRANSACTION = NO CHANGES
      modifiedPages = null;
      return;
    }

    try {
      // LOCK FILES (IN ORDER, SO TO AVOID DEADLOCK)
      final Set<Integer> modifiedFiles = new HashSet<>();

      for (WALFile.WALPage p : buffer.pages)
        modifiedFiles.add(p.fileId);

      indexChanges.setKeys(keysTx);
      indexChanges.addFilesToLock(modifiedFiles);

      for (WALFile.WALPage p : buffer.pages) {
        final PaginatedFile file = database.getFileManager().getFile(p.fileId);
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
      }

      database.commit();

    } catch (ConcurrentModificationException e) {
      rollback();
      throw e;
    } catch (Exception e) {
      rollback();
      throw new TransactionException("Transaction error on commit", e);
    }
  }

  /**
   * Locks the files in order, then checks all the pre-conditions.
   */
  public Pair<Binary, List<MutablePage>> commit1stPhase(final boolean isLeader) {
    if (status == STATUS.INACTIVE)
      throw new TransactionException("Transaction not started");

    if (status != STATUS.BEGUN)
      throw new TransactionException("Transaction in phase " + status);

    if (updatedRecords != null) {
      for (Record rec : updatedRecords)
        database.updateRecordNoLock(rec);
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
      if (isLeader) {
        // COMMIT INDEX CHANGES (IN CASE OF REPLICA THIS IS DEMANDED TO THE LEADER EXECUTION)
        indexChanges.commit();
      }

      // CHECK THE VERSIONS FIRST
      final List<MutablePage> pages = new ArrayList<>();

      final PageManager pageManager = database.getPageManager();

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
        for (MutablePage p : newPages.values()) {
          final int[] range = p.getModifiedRange();
          if (range[1] > 0) {
            pageManager.checkPageVersion(p, true);
            pages.add(p);
          }
        }

      Binary result = null;

      if (useWAL) {
        txId = database.getTransactionManager().getNextTransactionId();

        LogManager.instance().log(this, Level.FINE, "Creating buffer for TX %d (threadId=%d)", null, txId, Thread.currentThread().getId());

        result = database.getTransactionManager().createTransactionBuffer(txId, pages);
      }

      return new Pair<>(result, pages);

    } catch (DuplicatedKeyException | ConcurrentModificationException e) {
      rollback();
      throw e;
    } catch (Exception e) {
      LogManager.instance().log(this, Level.FINE, "Unknown exception during commit (threadId=%d)", e, Thread.currentThread().getId());
      rollback();
      throw new TransactionException("Transaction error on commit", e);
    }
  }

  public void commit2ndPhase(final Pair<Binary, List<MutablePage>> changes) {
    if (database.getMode() == PaginatedFile.MODE.READ_ONLY)
      throw new TransactionException("Cannot commit changes because the database is open in read-only mode");

    if (status != STATUS.COMMIT_1ST_PHASE)
      throw new TransactionException("Cannot execute 2nd phase commit without having started the 1st phase");

    status = STATUS.COMMIT_2ND_PHASE;

    final PageManager pageManager = database.getPageManager();

    try {
      if (changes.getFirst() != null)
        // WRITE TO THE WAL FIRST
        database.getTransactionManager().writeTransactionToWAL(changes.getSecond(), walFlush, txId, changes.getFirst());

      // AT THIS POINT, LOCK + VERSION CHECK, THERE IS NO NEED TO MANAGE ROLLBACK BECAUSE THERE CANNOT BE CONCURRENT TX THAT UPDATE THE SAME PAGE CONCURRENTLY
      // UPDATE PAGE COUNTER FIRST
      LogManager.instance().log(this, Level.FINE, "TX committing pages newPages=%s modifiedPages=%s (threadId=%d)", null, newPages, modifiedPages,
          Thread.currentThread().getId());

      pageManager.updatePages(newPages, modifiedPages, asyncFlush);

      if (newPages != null) {
        for (Map.Entry<Integer, Integer> entry : newPageCounters.entrySet()) {
          database.getSchema().getFileById(entry.getKey()).setPageCount(entry.getValue());
          database.getFileManager()
              .setVirtualFileSize(entry.getKey(), (long) entry.getValue() * database.getFileManager().getFile(entry.getKey()).getPageSize());
        }
      }

      for (Record r : modifiedRecordsCache.values())
        ((RecordInternal) r).unsetDirty();

      for (int fileId : lockedFiles) {
        final PaginatedComponent file = database.getSchema().getFileByIdIfExists(fileId);
        if (file != null)
          // THE FILE COULD BE NULL IN CASE OF INDEX COMPACTION
          file.onAfterCommit();
      }

    } catch (ConcurrentModificationException e) {
      throw e;
    } catch (Exception e) {
      LogManager.instance().log(this, Level.FINE, "Unknown exception during commit (threadId=%d)", e, Thread.currentThread().getId());
      throw new TransactionException("Transaction error on commit", e);
    } finally {
      reset();
    }
  }

  public void addIndexOperation(final Index index, final boolean addOperation, final Object[] keys, final RID rid) {
    indexChanges.addIndexKeyLock(index.getName(), addOperation, keys, rid);
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

    final TransactionManager txManager = database.getTransactionManager();

    if (lockedFiles != null) {
      txManager.unlockFilesInOrder(lockedFiles);
      lockedFiles = null;
    }

    indexChanges.reset();

    modifiedPages = null;
    newPages = null;
    updatedRecords = null;
    newPageCounters.clear();
    modifiedRecordsCache.clear();
    immutableRecordsCache.clear();
    txId = -1;
  }

  public void removePagesOfFile(final int fileId) {
    if (newPages != null)
      newPages.values().removeIf(mutablePage -> fileId == mutablePage.getPageId().getFileId());

    newPageCounters.remove(fileId);

    if (modifiedPages != null)
      modifiedPages.values().removeIf(mutablePage -> fileId == mutablePage.getPageId().getFileId());

    // IMMUTABLE RECORD, AVOID IT'S POINTING TO THE OLD OFFSET IN A MODIFIED PAGE
    // SAME PAGE, REMOVE IT
    immutableRecordsCache.values().removeIf(r -> r.getIdentity().getBucketId() == fileId);

    if (lockedFiles != null)
      lockedFiles.remove(fileId);

    final PaginatedComponent component = database.getSchema().getFileByIdIfExists(fileId);
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

    for (PageId p : modifiedPages.keySet())
      modifiedFiles.add(p.getFileId());
    if (newPages != null)
      for (PageId p : newPages.keySet())
        modifiedFiles.add(p.getFileId());

    indexChanges.addFilesToLock(modifiedFiles);

    modifiedFiles.addAll(newPageCounters.keySet());

    final long timeout = database.getConfiguration().getValueAsLong(GlobalConfiguration.COMMIT_LOCK_TIMEOUT);

    return database.getTransactionManager().tryLockFiles(modifiedFiles, timeout);
  }
}
