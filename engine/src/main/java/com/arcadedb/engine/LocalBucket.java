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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordEventsRegistry;
import com.arcadedb.database.RecordInternal;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseIsReadOnlyException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.IntIntHashMap;
import com.arcadedb.utility.LockManager;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;
import static com.arcadedb.database.Binary.LONG_SERIALIZED_SIZE;

/**
 * PAGE CONTENT = [version(long:8),recordCountInPage(short:2),recordOffsetsInPage(2048*uint=8192)]
 * <br><br>
 * Record size is the length of the record, managing also the following special cases:
 * <ul>
 * <li>> 0 = active record</li>
 * <li>0 = deleted record</li>
 * <li>-1 = placeholder pointer that points to another record in another page</li>
 * <li>-2 = first chunk of a multi page record</li>
 * <li>-3 = after first chunk of a multi page record</li>
 * <li><-5 = placeholder content pointed from another record in another page</li>
 * </ul>
 * The record size is stored as a varint (variable integer size) + the length of the size itself.
 * So if the record size is 10, it would take 1 byte (to store the number 10 in 7 bits) + 1 byte (to store how many bytes is 10))
 * The minimum size of a record stored in a page is 5 bytes. If the record is smaller than 5 bytes,
 * it is filled with blanks.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LocalBucket extends PaginatedComponent implements Bucket {
  public static final    String                    BUCKET_EXT                       = "bucket";
  public static final    int                       CURRENT_VERSION                  = 0;
  // Bucket file-format version 1 is reserved for paired external-property buckets. They hold heavier records,
  // so the page-slot table is sized down to 256 (vs 2048 at v0). The bucket-page header (computed by
  // contentHeaderSize = PAGE_RECORD_TABLE_OFFSET + maxSlots*4) shrinks from 8194 bytes at v0 to 1026 bytes at
  // v1 - reclaiming ~7KB of header per page that becomes available for value blobs. Sized to host typical
  // 1-2KB records (especially with compression enabled).
  public static final    int                       EXTERNAL_BUCKET_VERSION          = 1;
  private static final   int                       DEF_MAX_RECORDS_IN_PAGE_V1       = 256;
  public static final    long                      RECORD_PLACEHOLDER_POINTER       = -1L;    // USE -1 AS SIZE TO STORE A PLACEHOLDER (THAT POINTS TO A RECORD ON ANOTHER PAGE)
  public static final    long                      FIRST_CHUNK                      = -2L;    // USE -2 TO MARK THE FIRST CHUNK OF A BIG RECORD. FOLLOWS THE CHUNK SIZE AND THE POINTER TO THE NEXT CHUNK
  public static final    long                      NEXT_CHUNK                       = -3L;    // USE -3 TO MARK THE SECOND AND FURTHER CHUNK THAT IS PART OF A BIG RECORD THAT DOES NOT FIT A PAGE. FOLLOWS THE CHUNK SIZE AND THE POINTER TO THE NEXT CHUNK OR 0 IF THE CURRENT CHUNK IS THE LAST (NO FURTHER CHUNKS)
  protected static final int                       PAGE_RECORD_COUNT_IN_PAGE_OFFSET = 0;
  protected static final int                       PAGE_RECORD_TABLE_OFFSET         =
          PAGE_RECORD_COUNT_IN_PAGE_OFFSET + Binary.SHORT_SERIALIZED_SIZE;
  private static final   int                       DEF_MAX_RECORDS_IN_PAGE          = 2048;
  private static final   int                       MINIMUM_RECORD_SIZE              = 5;    // RECORD SIZE CANNOT BE < 13 BYTES IN CASE OF UPDATE AND PLACEHOLDER, 5 BYTES IS THE SPACE REQUIRED TO HOST THE PLACEHOLDER AND 1ST CHUCK FOR MULTI-PAGE CONTENT
  private static final   long                      RECORD_PLACEHOLDER_CONTENT       =
          MINIMUM_RECORD_SIZE * -1L;    // < -5 FOR SURROGATE RECORDS
  private static final   long                      MINIMUM_SPACE_LEFT_IN_PAGE       = 50L;
  private static final   int                       MAX_PAGES_GATHER_STATS           = 100;
  private static final   long                      MAX_TIMEOUT_GATHER_STATS         = 5000L;
  private static final   int                       GATHER_STATS_MIN_SPACE_PERC      = 10;
  private static final   int                       SPARE_SPACE_FOR_GROWTH           = 32;
  protected final        int                       contentHeaderSize;
  private final          int                       maxRecordsInPage;
  private final          AtomicLong                cachedRecordCount                = new AtomicLong(-1);

  /**
   * Bucket purpose tag. Declared up here (next to the {@link #purpose} field that uses it) so the enum is the
   * first thing a reader sees alongside the other bucket-level constants - the alternative was burying it
   * mid-file between unrelated state, which made the EXTERNAL property contract hard to discover.
   */
  public enum Purpose {
    /** Bucket holding the primary records of a type (vertex/edge/document). Targetable by user DML. */
    PRIMARY,
    /** Paired infrastructure bucket holding externalised property values. NOT targetable by user DML. */
    EXTERNAL_PROPERTY
  }

  // Buckets are PRIMARY by default (they hold the primary records of a type and are user-targetable via DML).
  // Internal kinds (e.g. EXTERNAL_PROPERTY) hold serializer infrastructure that user-facing DML must not target.
  // The purpose is persisted in schema.json (per-type) and restored at load time, see LocalDocumentType.
  private                Purpose                   purpose                          = Purpose.PRIMARY;
  // pageId → free-space-bytes. TreeMap ordering is unused (verified by grep), so a primitive
  // open-addressing map saves memory and avoids Integer boxing on every read/write/remove on
  // the page-allocation hot path. Bounded by MAX_PAGES_GATHER_STATS (100). Single-threaded
  // access is enforced by `synchronized (freeSpaceInPages)` blocks at every callsite.
  private final          IntIntHashMap             freeSpaceInPages                 = new IntIntHashMap();
  private final          REUSE_SPACE_MODE          reuseSpaceMode;
  // #4958: both fields are read/written outside the freeSpaceInPages monitor on some paths (delete,
  // updatePageStatistics), so they must be safe on their own: volatile timestamp + atomic counter.
  private volatile       long                      timeOfLastStats                  = 0L;
  private final          AtomicLong                changesFromLastStats             = new AtomicLong();

  private enum REUSE_SPACE_MODE {
    LOW, MEDIUM, HIGH
  }

  private static class PageAnalysis {
    public final BasePage page;
    public       int      newRecordPositionInPage     = -1;
    public       int      availablePositionIndex      = -1;
    public       boolean  createNewPage               = false;
    public       int      totalRecordsInPage          = -1;
    public       int      spaceAvailableInCurrentPage = -1;
    public       int      lastRecordPositionInPage    = -1;

    public PageAnalysis(final BasePage page) {
      this.page = page;
    }
  }

  public static class PaginatedComponentFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int id,
                                           final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new LocalBucket(database, name, filePath, id, mode, pageSize, version);
    }
  }

  /**
   * Record position (the RID's long position) for a slot in a page. Package-private static so the overflow
   * regression test for #4931 can verify it directly: the pre-fix inline {@code int * int} arithmetic
   * overflowed for buckets beyond 2^31 positions, and {@code check(fix=true)} then deleted an innocent
   * record at the wrong RID.
   */
  static long recordPosition(final int pageId, final int maxRecordsInPage, final int positionInPage) {
    return (long) pageId * maxRecordsInPage + positionInPage;
  }

  /**
   * Called at creation time.
   */
  public LocalBucket(final DatabaseInternal database, final String name, final String filePath, final ComponentFile.MODE mode,
                     final int pageSize, final int version) throws IOException {
    super(database, name, filePath, BUCKET_EXT, mode, pageSize, version);
    this.maxRecordsInPage = maxRecordsInPageForVersion(version);
    this.contentHeaderSize = PAGE_RECORD_TABLE_OFFSET + (maxRecordsInPage * INT_SERIALIZED_SIZE);
    this.cachedRecordCount.set(0);
    this.reuseSpaceMode = REUSE_SPACE_MODE.valueOf(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.getValueAsString().toUpperCase());
    this.purpose = purposeForVersion(version);
  }

  /**
   * Called at load time.
   * <p>
   * Free-space statistics are NOT pre-warmed here. {@link #findAvailableSpace} already calls
   * {@link #gatherPageStatistics()} lazily on the first allocation that needs it, so a leader
   * still gets reuse for free; a follower that only applies leader-shipped pages via the state
   * machine never triggers it at all. Pre-warming here would scan up to all pages of every
   * bucket during {@link com.arcadedb.schema.LocalSchema#load} - which on a follower under
   * heavy bulk-load fires repeatedly per LSM compaction SCHEMA_ENTRY and exhausts the heap
   * (issue #4219).
   */
  public LocalBucket(final DatabaseInternal database, final String name, final String filePath, final int id,
                     final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
    this.maxRecordsInPage = maxRecordsInPageForVersion(version);
    contentHeaderSize = PAGE_RECORD_TABLE_OFFSET + (maxRecordsInPage * INT_SERIALIZED_SIZE);
    this.reuseSpaceMode = REUSE_SPACE_MODE.valueOf(GlobalConfiguration.BUCKET_REUSE_SPACE_MODE.getValueAsString().toUpperCase());
    // Derive purpose from the bucket file version - the version itself is persisted in the on-disk file name
    // (e.g. Doc_0_ext.<id>.<pageSize>.v1.bucket), so this assignment is reliable as soon as the LocalBucket
    // is constructed - long before LocalDocumentType.restoreExternalBuckets() runs. That closes the gap where
    // a write path firing between FileManager scan and schema-load completion would have seen purpose=PRIMARY
    // by default and bypassed the user-DML guard. Schema JSON still maps primary->external by name (which is
    // an orthogonal concern), but the write guard now no longer depends on schema-load ordering.
    this.purpose = purposeForVersion(version);
  }

  private static Purpose purposeForVersion(final int version) {
    return version >= EXTERNAL_BUCKET_VERSION ? Purpose.EXTERNAL_PROPERTY : Purpose.PRIMARY;
  }

  @Override
  public void close() {
    super.close();
    freeSpaceInPages.clear();
  }

  public int getMaxRecordsInPage() {
    return maxRecordsInPage;
  }

  /** Slot-table sizing for the bucket file format version. v0=2048 (legacy), v1=256 (paired external buckets). */
  private static int maxRecordsInPageForVersion(final int version) {
    return version >= EXTERNAL_BUCKET_VERSION ? DEF_MAX_RECORDS_IN_PAGE_V1 : DEF_MAX_RECORDS_IN_PAGE;
  }

  public Purpose getPurpose() {
    return purpose;
  }

  public void setPurpose(final Purpose purpose) {
    this.purpose = purpose;
  }

  @Override
  public RID createRecord(final Record record, final boolean discardRecordAfter) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.CREATE_RECORD);
    // Provisional identity for Document records (and subtypes: vertex, edge) so the serializer can resolve the
    // target primary bucket for EXTERNAL property handling. EdgeSegment, ExternalValueRecord, and other internal
    // record types do not need this and must not be touched.
    if (record.getIdentity() == null && record instanceof Document && record instanceof RecordInternal ri)
      ri.setIdentity(RID.create(database, fileId, -1L));
    return createRecordInternal(record, false, discardRecordAfter);
  }

  @Override
  public void updateRecord(final Record record, final boolean discardRecordAfter) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.UPDATE_RECORD);
    updateRecordInternal(record, record.getIdentity(), false, discardRecordAfter);
  }

  @Override
  public Binary getRecord(final RID rid) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.READ_RECORD);

    final Binary rec = getRecordInternal(rid, false);
    if (rec == null)
      // DELETED
      throw new RecordNotFoundException("Record " + rid + " not found", rid);
    return rec;
  }

  @Override
  public boolean existsRecord(final RID rid) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.READ_RECORD);

    try {
      final long[] recordSize = readRecordSizeMarker(rid);
      return recordSize != null && (recordSize[0] > 0 || recordSize[0] == RECORD_PLACEHOLDER_POINTER
          || recordSize[0] == FIRST_CHUNK);

    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on checking record existence for " + rid);
    }
  }

  /**
   * Tells whether the record is stored entirely in place on its own page (positive size marker), as opposed to
   * a placeholder indirection or a multi-page record whose continuation chunks live on other pages. The
   * commit-time edge-append rebase ({@code TransactionContext.rebaseEdgeAppends}) relies on this: rebasing
   * re-reads and re-writes a record assuming its bytes live on the ONE conflicted page, so any record with
   * content on other pages must fall back to a full-transaction retry - re-writing it would publish the
   * transaction's stale copy of the continuation pages, silently reverting concurrently committed bytes there
   * (those pages pass the MVCC version check because their transaction copy is created only at drain time).
   * Reads the CURRENT state through the transaction (the caller holds the file's commit lock).
   */
  public boolean isRecordStoredInSinglePage(final RID rid) {
    if (rid == null || rid.getPosition() < 0)
      return false;

    try {
      final long[] recordSize = readRecordSizeMarker(rid);
      return recordSize != null && recordSize[0] > 0;

    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on checking record layout for " + rid, e);
    }
  }

  /**
   * Reads the raw size marker of the record's slot on its page through the transaction (positive = in-place
   * record, {@link #RECORD_PLACEHOLDER_POINTER}, {@link #FIRST_CHUNK}, {@link #NEXT_CHUNK}, negative =
   * placeholder content), or {@code null} when the page or slot does not exist or the record was deleted.
   */
  private long[] readRecordSizeMarker(final RID rid) throws IOException {
    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    if (pageId >= pageCount.get()) {
      final int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        return null;
    }

    final BasePage page = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageId), pageSize);

    final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
    if (positionInPage >= recordCountInPage)
      return null;

    final int recordPositionInPage = getRecordPositionInPage(page, positionInPage);
    if (recordPositionInPage == 0)
      // DELETED RECORD (>= 24.1.1, IT WAS CLEANED CORRUPTED RECORD BEFORE)
      return null;

    return page.readNumberAndSize(recordPositionInPage);
  }

  @Override
  public void deleteRecord(final RID rid) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.DELETE_RECORD);
    deleteRecordInternal(rid, false, false);
  }

  @Override
  public void scan(final RawRecordCallback callback, final ErrorRecordCallback errorRecordCallback) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.READ_RECORD);

    final int txPageCount = getTotalPages();

    try {
      for (int pageId = 0; pageId < txPageCount; ++pageId) {
        final BasePage page = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageId), pageSize);
        final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

        if (recordCountInPage > 0) {
          for (int recordIdInPage = 0; recordIdInPage < recordCountInPage; ++recordIdInPage) {
            final RID rid = new RID(fileId, recordPosition(pageId, maxRecordsInPage, recordIdInPage));

            try {
              final int recordPositionInPage = getRecordPositionInPage(page, recordIdInPage);
              if (recordPositionInPage == 0)
                // DELETED RECORD (>= 24.1.1, IT WAS CLEANED CORRUPTED RECORD BEFORE)
                continue;

              final long[] recordSize = page.readNumberAndSize(recordPositionInPage);

              if (recordSize[0] > 0) {
                // NOT DELETED
                final int recordContentPositionInPage = recordPositionInPage + (int) recordSize[1];

                final Binary view = page.getImmutableView(recordContentPositionInPage, (int) recordSize[0]);

                if (!callback.onRecord(rid, view))
                  return;

              } else if (recordSize[0] == RECORD_PLACEHOLDER_POINTER) {
                // LOAD PLACEHOLDER CONTENT
                final RID placeHolderPointer = new RID(fileId,
                        page.readLong((int) (recordPositionInPage + recordSize[1])));
                final Binary view = getRecordInternal(placeHolderPointer, true);
                if (view != null && !callback.onRecord(rid, view))
                  return;
              } else if (recordSize[0] == FIRST_CHUNK) {
                // LOAD THE ENTIRE RECORD IN CHUNKS
                final Binary view = loadMultiPageRecord(rid, page, recordPositionInPage, recordSize);
                if (!callback.onRecord(rid, view))
                  return;
              }

            } catch (final Exception e) {
              final boolean reThrowException = e instanceof DatabaseIsReadOnlyException;

              if (errorRecordCallback != null) {
                if (!errorRecordCallback.onErrorLoading(rid, e))
                  // STOP THE SCAN
                  return;
              } else if (!reThrowException)
                // LOG THE EXCEPTION
                LogManager.instance()
                        .log(this, Level.SEVERE, "Error on loading record %s (error: %s)".formatted(rid, e.getMessage()));

              if (reThrowException)
                throw e;
            }
          }
        }
      }
    } catch (final IOException e) {
      throw new DatabaseOperationException("Cannot scan bucket '" + componentName + "'", e);
    }
  }

  public void fetchPageInTransaction(final RID rid) throws IOException {
    if (rid.getPosition() < 0L) {
      LogManager.instance().log(this, Level.WARNING, "Cannot load a page from a record with invalid RID (" + rid + ")");
      return;
    }

    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);

    if (pageId >= pageCount.get()) {
      final int txPageCount = getTotalPages();
      if (pageId >= txPageCount) {
        LogManager.instance().log(this, Level.WARNING, "Record " + rid + " not found");
      }
    }

    database.getTransaction().getPageToModify(new PageId(database, file.getFileId(), pageId), pageSize, false);
  }

  @Override
  public Iterator<Record> iterator() {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.READ_RECORD);
    return new BucketIterator(this, true);
  }

  @Override
  public Iterator<Record> inverseIterator() {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.READ_RECORD);
    return new BucketIterator(this, false);
  }

  @Override
  public String toString() {
    return componentName;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof LocalBucket))
      return false;

    return ((LocalBucket) obj).fileId == this.fileId;
  }

  @Override
  public int hashCode() {
    return fileId;
  }

  @Override
  public long count() {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.READ_RECORD);

    final TransactionContext transaction = database.getTransactionIfExists();

    final long cached = cachedRecordCount.get();
    if (cached > -1)
      // O(1) fast path: the counter is kept up to date on every commit, so return it directly. With an active
      // transaction add its uncommitted delta; without one there is nothing pending, so the cached value is the
      // committed count. Only a still-unknown (-1) counter falls through to the full recompute below.
      return cached + (transaction != null ? transaction.getBucketRecordDelta(fileId) : 0);

    // #5152: the recompute below (scan + publish of cachedRecordCount) must be mutually exclusive with a
    // commit's publishPages + record-count fold on this bucket. A commit holds this bucket's file lock across
    // both (TransactionContext.commit2ndPhase, until reset()), so acquiring the same lock here serializes the
    // recompute with commits. Without it a commit could publish a record and then, seeing the still-(-1)
    // counter, drop its delta at the fold while our scan misses that just-published record (lost update), or
    // conversely our scan counts a published record whose delta the fold then re-adds (double count). The lock
    // is taken only on this rare recompute path (counter unknown) - never on the O(1) cached fast-path returned
    // above. The requester mirrors the transaction's so a recompute invoked while the same transaction already
    // holds the lock re-enters instead of self-deadlocking.
    final TransactionManager txManager = database.getTransactionManager();
    // Deliberately the pure getRequester() read (not captureRequester()): this call acquires and releases the
    // lock symmetrically on one thread, so there is nothing to capture, and reading avoids mutating the
    // transaction's requester field from a possibly-foreign counting thread. Falls back to the current thread.
    final Object requester = transaction != null ? transaction.getRequester() : Thread.currentThread();
    final long lockTimeout = database.getConfiguration().getValueAsLong(GlobalConfiguration.COMMIT_LOCK_TIMEOUT);

    LockManager.LOCK_STATUS lockStatus = LockManager.LOCK_STATUS.NO;
    try {
      lockStatus = txManager.tryLockFile(fileId, lockTimeout, requester);

      // Another thread may have recomputed the counter while we were queued on the lock. Re-check now that we
      // hold it (or timed out) and skip the duplicate O(N) scan, which also shortens how long we hold the lock.
      final long recomputed = cachedRecordCount.get();
      if (recomputed > -1)
        return recomputed + (transaction != null ? transaction.getBucketRecordDelta(fileId) : 0);

      long total = 0;

      final int txPageCount = getTotalPages();

      for (int pageId = 0; pageId < txPageCount; ++pageId) {
        final PageId pageIdToLoad = new PageId(database, file.getFileId(), pageId);
        final BasePage page = transaction != null ?
                transaction.getPage(pageIdToLoad, pageSize) :
                database.getPageManager().getImmutablePage(pageIdToLoad, pageSize, false, false);

        final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

        if (recordCountInPage > 0) {
          for (int recordIdInPage = 0; recordIdInPage < recordCountInPage; ++recordIdInPage) {
            final int recordPositionInPage = getRecordPositionInPage(page, recordIdInPage);
            if (recordPositionInPage == 0)
              // DELETED RECORD (>= 24.1.1, IT WAS CLEANED CORRUPTED RECORD BEFORE)
              continue;

            final long[] recordSize = page.readNumberAndSize(recordPositionInPage);

            if (recordSize[0] > 0 || recordSize[0] == RECORD_PLACEHOLDER_POINTER || recordSize[0] == FIRST_CHUNK)
              total++;
          }
        }
      }

      // Publish the recomputed value only when this scan ran under the lock (acquired now, or already held by
      // an enclosing transaction). On a lock-acquisition timeout (NO) the scan ran lock-free and may be drifted,
      // so leave the counter at -1 and return a best-effort value: a later call recomputes cleanly.
      if (lockStatus != LockManager.LOCK_STATUS.NO)
        // The scan reads the transaction's view (getPage returns its uncommitted pages first), so `total`
        // already includes this transaction's pending delta. Cache the COMMITTED base (total - pending) so the
        // commit-time fold adds the delta exactly once instead of double-counting it; the caller still gets the
        // transaction-visible `total` below.
        cachedRecordCount.set(transaction != null ? total - transaction.getBucketRecordDelta(fileId) : total);
      else
        LogManager.instance().log(this, Level.FINE,
                "count() recompute on bucket '%s' ran lock-free after a %dms lock-acquisition timeout; result not cached", componentName,
                lockTimeout);
      return total;

    } catch (final IOException e) {
      throw new DatabaseOperationException("Cannot count bucket '" + componentName + "'", e);
    } finally {
      // Release only if WE acquired it. ALREADY_ACQUIRED means an enclosing transaction owns the lock and is
      // responsible for releasing it; NO means the acquisition timed out and we ran the scan lock-free.
      if (lockStatus == LockManager.LOCK_STATUS.YES)
        txManager.unlockFile(fileId, requester);
    }
  }

  public Map<String, Object> check(final int verboseLevel, final boolean fix) {
    final Map<String, Object> stats = new HashMap<>();

    final int totalPages = getTotalPages();

    if (verboseLevel > 1)
      LogManager.instance()
              .log(this, Level.INFO, "- Checking bucket '%s' (totalPages=%d spaceOnDisk=%s pageSize=%s)...", componentName, totalPages,
                      FileUtils.getSizeAsString((long) totalPages * pageSize), FileUtils.getSizeAsString(pageSize));

    long totalAllocatedRecords = 0L;
    long totalActiveRecords = 0L;
    long totalPlaceholderRecords = 0L;
    long totalSurrogateRecords = 0L;
    long totalMultiPageRecords = 0L;
    long totalDeletedRecords = 0L;
    long totalMaxOffset = 0L;
    long totalChunks = 0L;

    long totalErrors = 0L;
    final List<String> warnings = new ArrayList<>();
    final List<RID> deletedRecordsAfterFix = new ArrayList<>();

    String warning = null;

    for (int pageId = 0; pageId < totalPages; ++pageId) {
      try {
        final BasePage page = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageId), pageSize);
        final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

        int pageActiveRecords = 0;
        int pagePlaceholderRecords = 0;
        int pageSurrogateRecords = 0;
        int pageDeletedRecords = 0;
        int pageMaxOffset = 0;
        int pageMultiPageRecords = 0;
        int pageChunks = 0;

        for (int positionInPage = 0; positionInPage < recordCountInPage; ++positionInPage) {
          // #4931: int*int overflowed here for buckets beyond 2^31 positions, and check(fix=true) then
          // deleted an innocent record at the wrong RID. recordPosition() widens to long.
          final RID rid = new RID(file.getFileId(), recordPosition(pageId, maxRecordsInPage, positionInPage));

          final int recordPositionInPage = (int) page.readUnsignedInt(
                  PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);

          if (recordPositionInPage == 0) {
            // DELETED RECORD (>= 24.1.1, IT WAS CLEANED CORRUPTED RECORD BEFORE)
            pageDeletedRecords++;
            totalDeletedRecords++;

          } else if (recordPositionInPage > page.getContentSize()) {
            ++totalErrors;
            warning = "invalid record offset %d in page for record %s".formatted(recordPositionInPage, rid);
            if (fix) {
              deleteRecordInternal(rid, true, true);
              deletedRecordsAfterFix.add(rid);
              ++totalDeletedRecords;
            }
          } else {

            try {
              final long[] recordSize = page.readNumberAndSize(recordPositionInPage);

              totalAllocatedRecords++;

              if (recordSize[0] == 0) {
                pageDeletedRecords++;
                totalDeletedRecords++;
              } else if (recordSize[0] == RECORD_PLACEHOLDER_POINTER) {
                pagePlaceholderRecords++;
                totalPlaceholderRecords++;
                recordSize[0] = MINIMUM_RECORD_SIZE;
              } else if (recordSize[0] == FIRST_CHUNK) {
                pageActiveRecords++;
                pageMultiPageRecords++;
                totalMultiPageRecords++;
                recordSize[0] = page.readInt((int) (recordPositionInPage + recordSize[1]));
              } else if (recordSize[0] == NEXT_CHUNK) {
                totalChunks++;
                pageChunks++;
                recordSize[0] = page.readInt((int) (recordPositionInPage + recordSize[1]));
              } else if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
                pageSurrogateRecords++;
                totalSurrogateRecords++;
                recordSize[0] *= -1;
              } else {
                pageActiveRecords++;
                totalActiveRecords++;
              }

              final long endPosition = recordPositionInPage + recordSize[1] + recordSize[0];
              if (endPosition > file.getPageSize()) {
                ++totalErrors;
                warning = "wrong record size %d found for record %s".formatted(recordSize[1] + recordSize[0], rid);
                if (fix) {
                  deleteRecordInternal(rid, true, true);
                  deletedRecordsAfterFix.add(rid);
                  ++totalDeletedRecords;
                }
              }

              if (endPosition > pageMaxOffset)
                pageMaxOffset = (int) endPosition;

            } catch (final Exception e) {
              ++totalErrors;
              warning = "unknown error on loading record %s: %s".formatted(rid, e.getMessage());

              if (fix && !(e instanceof RecordNotFoundException)) {
                deleteRecordInternal(rid, true, true);
                deletedRecordsAfterFix.add(rid);
                ++totalDeletedRecords;
              }
            }
          }

          if (warning != null) {
            warnings.add(warning);
            if (verboseLevel > 0)
              LogManager.instance().log(this, Level.SEVERE, "- " + warning);
            warning = null;
          }
        }

        totalMaxOffset += pageMaxOffset;

        if (verboseLevel > 2)
          LogManager.instance().log(this, Level.FINE,
                  "-- Page %d records=%d (actives=%d deleted=%d placeholders=%d surrogates=%d multiPageRecords=%d chunks=%d) maxOffset=%d",
                  pageId, recordCountInPage, pageActiveRecords, pageDeletedRecords, pagePlaceholderRecords, pageSurrogateRecords,
                  pageMultiPageRecords, pageChunks, pageMaxOffset);

      } catch (final Exception e) {
        ++totalErrors;
        warning = "unknown error on checking page %d: %s".formatted(pageId, e.getMessage());
      }

      if (warning != null) {
        warnings.add(warning);
        if (verboseLevel > 0)
          LogManager.instance().log(this, Level.SEVERE, "- " + warning);
        warning = null;
      }
    }

    if (fix)
      // #5149: reconcile the cached record counter that count(*) relies on. Invalidating forces the next
      // count() to rescan authoritatively, and reusing the existing -1 sentinel means the value is
      // repopulated by count()'s own scan logic (no risk of a rule mismatch with this method's tallies) and
      // survives a rollback of the enclosing transaction. We invalidate rather than write the freshly scanned
      // value because check(fix=true) may run inside a caller-managed transaction: at commit TransactionContext
      // folds that transaction's accumulated bucket delta into the counter, but only when it is > -1. Writing a
      // scanned value would let unrelated inserts/deletes in that same transaction be double-counted on top of
      // it; leaving -1 makes the fold skip. (check()'s own corrupt-record deletions go through
      // deleteRecordInternal, which does not register a bucket delta, so they are not the concern here.)
      // Caveat: no count() must run on this bucket between here and the checker's commit - not from this caller
      // (DatabaseChecker does not) nor from a concurrent transaction. count() would repopulate the counter
      // (> -1) and let the commit-time fold re-apply the delta, reintroducing drift. That window (publishPages
      // then the fold in TransactionContext) is inherent to the incremental counter; CHECK FIX is an admin
      // operation, so it is expected to run without concurrent counts on the same bucket.
      cachedRecordCount.set(-1);

    final float avgPageUsed = totalPages > 0 ? ((float) totalMaxOffset) / totalPages * 100F / pageSize : 0;

    if (verboseLevel > 1)
      LogManager.instance()
              .log(this, Level.INFO, "-- Total records=%d (actives=%d deleted=%d placeholders=%d surrogates=%d) avgPageUsed=%.2f%%",
                      totalAllocatedRecords, totalActiveRecords, totalDeletedRecords, totalPlaceholderRecords, totalSurrogateRecords,
                      avgPageUsed);

    stats.put("pageSize", (long) pageSize);
    stats.put("totalPages", (long) totalPages);
    stats.put("totalAllocatedRecords", totalAllocatedRecords);
    stats.put("totalActiveRecords", totalActiveRecords);
    stats.put("totalPlaceholderRecords", totalPlaceholderRecords);
    stats.put("totalSurrogateRecords", totalSurrogateRecords);
    stats.put("totalDeletedRecords", totalDeletedRecords);
    stats.put("totalMaxOffset", totalMaxOffset);
    stats.put("totalMultiPageRecords", totalMultiPageRecords);
    stats.put("totalChunks", totalChunks);

    final DocumentType type = database.getSchema().getTypeByBucketId(fileId);
    if (type instanceof LocalVertexType) {
      stats.put("totalAllocatedVertices", totalAllocatedRecords);
      stats.put("totalActiveVertices", totalActiveRecords);
    } else if (type instanceof LocalEdgeType) {
      stats.put("totalAllocatedEdges", totalAllocatedRecords);
      stats.put("totalActiveEdges", totalActiveRecords);
    } else {
      stats.put("totalAllocatedDocuments", totalAllocatedRecords);
      stats.put("totalActiveDocuments", totalActiveRecords);
    }

    stats.put("deletedRecordsAfterFix", deletedRecordsAfterFix);
    stats.put("warnings", warnings);
    stats.put("autoFix", 0L);
    stats.put("totalErrors", totalErrors);

    return stats;
  }

  /**
   * The caller should call @{@link DatabaseInternal#invokeAfterReadEvents(Record)} after created the record and manage the result correctly.
   */
  public Binary getRecordInternal(final RID rid, final boolean readPlaceHolderContent) {
    // INVOKE EVENT CALLBACKS
    if (!((RecordEventsRegistry) database.getEvents()).onBeforeRead(rid))
      return null;
    final DocumentType type = database.getSchema().getTypeByBucketId(rid.getBucketId());
    if (type != null)
      if (!((RecordEventsRegistry) type.getEvents()).onBeforeRead(rid))
        return null;

    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    if (pageId >= pageCount.get()) {
      final int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);
    }

    try {
      final BasePage page = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageId), pageSize);

      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage >= recordCountInPage)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      final int recordPositionInPage = getRecordPositionInPage(page, positionInPage);
      if (recordPositionInPage == 0)
        // DELETED RECORD (>= 24.1.1, IT WAS CLEANED CORRUPTED RECORD BEFORE)
        return null;

      final long[] recordSize = page.readNumberAndSize(recordPositionInPage);

      if (recordSize[0] == 0)
        // DELETED
        return null;

      if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
        if (!readPlaceHolderContent)
          // PLACEHOLDER
          return null;

        recordSize[0] *= -1;
      }

      if (recordSize[0] == RECORD_PLACEHOLDER_POINTER) {
        // FOUND PLACEHOLDER, LOAD THE REAL RECORD
        final RID placeHolderPointer = new RID(rid.getBucketId(),
                page.readLong((int) (recordPositionInPage + recordSize[1])));
        return getRecordInternal(placeHolderPointer, true);
      } else if (recordSize[0] == FIRST_CHUNK) {
        // FOUND 1ST CHUNK, LOAD THE ENTIRE MULTI-PAGE RECORD
        return loadMultiPageRecord(rid, page, recordPositionInPage, recordSize);
      } else if (recordSize[0] == NEXT_CHUNK)
        // CANNOT LOAD PARTIAL CHUNK
        return null;

      final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);

      return page.getImmutableView(recordContentPositionInPage, (int) recordSize[0]);

    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on lookup of record " + rid, e);
    }
  }

  public long getCachedRecordCount() {
    return cachedRecordCount.get();
  }

  public void setCachedRecordCount(final long count) {
    cachedRecordCount.set(count);
  }

  private RID createRecordInternal(final Record record, final boolean isPlaceHolder, final boolean discardRecordAfter) {
    final Binary buffer = database.getSerializer().serialize(database, record);

    // RECORD SIZE CANNOT BE < 13 BYTES IN CASE OF UPDATE AND PLACEHOLDER, 5 BYTES IS THE SPACE REQUIRED TO HOST THE PLACEHOLDER. FILL THE DIFFERENCE WITH BLANK (0)
    while (buffer.size() < MINIMUM_RECORD_SIZE) {
      buffer.append((byte) 0);
    }

    final int bufferSize = buffer.size();

    try {
      int newRecordPositionInPage = -1;
      int availablePositionIndex = -1;
      boolean createNewPage = false;
      BasePage foundPage = null;

      final int txPageCounter = getTotalPages();

      final int spaceNeeded = Binary.getNumberSpace(isPlaceHolder ? (-1L * bufferSize) : bufferSize) + bufferSize;

      if (txPageCounter > 0) {
        final PageAnalysis pageAnalysis = findAvailableSpace(-1, spaceNeeded, txPageCounter, false);
        foundPage = pageAnalysis.page;
        newRecordPositionInPage = pageAnalysis.newRecordPositionInPage;
        availablePositionIndex = pageAnalysis.availablePositionIndex;
        createNewPage = pageAnalysis.createNewPage;
      } else
        createNewPage = true;

      final MutablePage selectedPage;
      if (createNewPage) {
        // Atomically reserve the next page number to prevent two concurrent transactions
        // from allocating the same page number (which would cause silent data corruption
        // when their commits both succeed and overwrite each other's chunks).
        final int reservedPageNumber = reservedPageCounter.getAndIncrement();
        selectedPage = database.getTransaction().addPage(new PageId(database, file.getFileId(), reservedPageNumber), pageSize);
        newRecordPositionInPage = contentHeaderSize;
        availablePositionIndex = 0;
      } else
        selectedPage = database.getTransaction().getPageToModify(foundPage);

      LogManager.instance()
              .log(this, Level.FINE, "Creating record (%s records=%d threadId=%d)", selectedPage, availablePositionIndex,
                      Thread.currentThread().threadId());
      final RID rid = new RID(file.getFileId(),
              ((long) selectedPage.getPageId().getPageNumber()) * maxRecordsInPage + availablePositionIndex);

      final int spaceAvailableInCurrentPage = selectedPage.getMaxContentSize() - newRecordPositionInPage;

      // RESERVE A SPOT IMMEDIATELY TO AVOID USAGE FOR MULTI PAGE RECORD
      selectedPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + availablePositionIndex * INT_SERIALIZED_SIZE,
              newRecordPositionInPage);

      short recordCountInPage = selectedPage.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (availablePositionIndex + 1 > recordCountInPage) {
        // UPDATE RECORD NUMBER
        recordCountInPage = (short) (availablePositionIndex + 1);
        selectedPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, recordCountInPage);
      }

      if (spaceNeeded > spaceAvailableInCurrentPage) {
        // MULTI-PAGE RECORD
        writeMultiPageRecord(rid, buffer, selectedPage, newRecordPositionInPage, spaceAvailableInCurrentPage);

      } else {
        final int byteWritten = selectedPage.writeNumber(newRecordPositionInPage, isPlaceHolder ? (-1L * bufferSize) : bufferSize);
        selectedPage.writeByteArray(newRecordPositionInPage + byteWritten, buffer.getContent(), buffer.getContentBeginOffset(),
                bufferSize);
        updatePageStatistics(selectedPage.pageId.getPageNumber(), spaceAvailableInCurrentPage, -spaceNeeded);
      }

      LogManager.instance()
              .log(this, Level.FINE, "Created record %s (%s records=%d threadId=%d)", rid, selectedPage, recordCountInPage,
                      Thread.currentThread().threadId());

      // DISJOINT-SLOT MERGE (#5381): a brand-new record inserted into a FREE slot of an EXISTING (reused) page
      // commutes with concurrent writes to other slots of that page. Track it so a commit-time page-version
      // conflict can be resolved by replaying this insert on the newer committed page (see TransactionContext).
      // A record on a brand-new page (createNewPage) has no existing-page conflict to rebase; a multi-page or
      // placeholder record is not a plain single-slot insert, so it poisons the page instead.
      final TransactionContext slotTx = database.getTransactionIfExists();
      if (slotTx != null && slotTx.isSlotMergeEnabled()) {
        final int slotPageNumber = selectedPage.getPageId().getPageNumber();
        if (!isSlotMergeCandidate(record))
          slotTx.poisonSlotRebasePage(fileId, slotPageNumber);
        else if (!createNewPage) {
          if (isPlaceHolder || spaceNeeded > spaceAvailableInCurrentPage)
            slotTx.poisonSlotRebasePage(fileId, slotPageNumber);
          // Skip the record-image copy on a page that is already poisoned (it would be discarded anyway).
          else if (!slotTx.isSlotRebasePagePoisoned(fileId, slotPageNumber))
            slotTx.trackRebasableInsert(fileId, slotPageNumber, availablePositionIndex,
                    Arrays.copyOfRange(buffer.getContent(), buffer.getContentBeginOffset(), buffer.getContentBeginOffset() + bufferSize));
        }
      }

      if (!discardRecordAfter)
        ((RecordInternal) record).setBuffer(buffer.getNotReusable());

      return rid;

    } catch (final IOException e) {
      throw new DatabaseOperationException("Cannot add a new record to the bucket '" + componentName + "'", e);
    }
  }

  /**
   * A record is a disjoint-slot-merge candidate unless it is an edge-list segment (record type 3): edge-segment
   * pages are owned by the commutative edge-append merge ({@link TransactionContext#trackEdgeAppend}), so they
   * are deliberately kept out of the generic slot merge to avoid the two mechanisms rebasing the same page.
   */
  private static boolean isSlotMergeCandidate(final Record record) {
    return record.getRecordType() != EdgeSegment.RECORD_TYPE;
  }

  /**
   * Commit-time primitive for the disjoint-slot page merge (#5381). Re-applies ONE record write this transaction
   * made to a bucket page, on top of {@code page} freshly reloaded at its current committed version, keeping the
   * record's RID (page+slot) fixed. Called only on the leader/embedded commit while the bucket file's commit
   * lock is held, and only for a page whose every modification this transaction made was a tracked disjoint-slot
   * insert or same-or-smaller in-place update (see {@link TransactionContext#rebaseSlots}).
   *
   * @param page           the reloaded committed page to re-apply the write onto.
   * @param positionInPage the record slot (RID position modulo maxRecordsInPage).
   * @param body           this transaction's final serialized record body (no size prefix).
   * @param baseBody       for an in-place UPDATE, the record body this transaction started from - used to detect a
   *                       concurrent modification of the SAME record (a TRUE conflict); {@code null} for an INSERT.
   *
   * @return true when the write was safely re-applied; false when a concurrent commit took/changed the slot or the
   * page can no longer host the record - the caller then falls back to a full-transaction retry.
   */
  public boolean rebaseRecordOnPage(final MutablePage page, final int positionInPage, final byte[] body, final byte[] baseBody) {
    try {
      final int pageNumber = page.getPageId().getPageNumber();
      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      final int existingPos = positionInPage < recordCountInPage ? getRecordPositionInPage(page, positionInPage) : 0;

      if (baseBody == null) {
        // INSERT: the slot must still be free on the committed page (else a concurrent commit used it).
        if (existingPos != 0)
          return false;
        return writeRecordAtSlot(page, pageNumber, positionInPage, recordCountInPage, body);
      }

      // IN-PLACE UPDATE: the slot must still hold the record this transaction started from, byte-for-byte, so we
      // never overwrite (lose) a concurrent update of the SAME record.
      if (existingPos == 0)
        return false;
      final long[] rs = page.readNumberAndSize(existingPos);
      if (rs[0] <= 0)
        // Deleted, placeholder, or multi-page marker: not a plain in-place record anymore.
        return false;
      final int committedSize = (int) rs[0];
      if (committedSize != baseBody.length)
        return false;
      final byte[] committed = new byte[committedSize];
      page.readByteArray((int) (existingPos + rs[1]), committed, 0, committedSize);
      if (!Arrays.equals(committed, baseBody))
        // The committed record differs from our base: a concurrent transaction changed THIS record -> real conflict.
        return false;

      if (body.length > committedSize)
        // Defensive: a tracked in-place update is always same-or-smaller than its base (== committed here).
        return false;

      final int sizeLen = page.writeNumber(existingPos, body.length);
      page.writeByteArray((int) (existingPos + sizeLen), body, 0, body.length);
      return true;

    } catch (final IOException e) {
      // Intentional asymmetry: a "cannot rebase this slot" outcome returns false (the caller raises a clean CME
      // and the transaction retries), but a genuine I/O failure reading the page is not a retryable conflict - it
      // aborts the transaction like any other storage error rather than masquerading as a version conflict.
      throw new DatabaseOperationException("Error on slot rebase for page " + page.getPageId(), e);
    }
  }

  private boolean writeRecordAtSlot(final MutablePage page, final int pageNumber, final int positionInPage,
                                    final short recordCountInPage, final byte[] body) throws IOException {
    final int spaceNeeded = Binary.getNumberSpace(body.length) + body.length;

    // Find where free content begins on the CURRENT committed page (reuses the tested free-space walker).
    final PageAnalysis analysis = getAvailableSpaceInPage(pageNumber, spaceNeeded, false);
    if (analysis.createNewPage || analysis.newRecordPositionInPage < 0
            || spaceNeeded > page.getMaxContentSize() - analysis.newRecordPositionInPage)
      // The page filled up under concurrency: fall back to a full retry (which will pick a new page/slot).
      return false;

    final int contentPos = analysis.newRecordPositionInPage;

    page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, contentPos);
    if (positionInPage + 1 > recordCountInPage)
      page.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) (positionInPage + 1));

    final int sizeLen = page.writeNumber(contentPos, body.length);
    page.writeByteArray(contentPos + sizeLen, body, 0, body.length);
    updatePageStatistics(pageNumber, page.getMaxContentSize() - contentPos, -spaceNeeded);
    return true;
  }

  /**
   * Creates multiple records from pre-serialized Binary buffers in a single operation.
   * Always appends to new pages to avoid complexity with existing page layouts, deletions,
   * and multi-page records. This avoids the per-record findAvailableSpace overhead and is
   * designed for bulk import scenarios where sequential page writes are optimal.
   *
   * @param buffers    array of pre-serialized record content (NOT including the size prefix)
   * @param from       start index in buffers array (inclusive)
   * @param to         end index in buffers array (exclusive)
   * @param ridsOut    output array for assigned RIDs (must have length >= to - from)
   */
  public void createRecordsBulk(final Binary[] buffers, final int from, final int to, final RID[] ridsOut) {
      // Always start from a fresh new page for bulk writes.
      // Atomically reserve the next page number to prevent two concurrent transactions
      // from allocating the same page number (which would cause silent data corruption).
      MutablePage currentPage = database.getTransaction()
          .addPage(new PageId(database, file.getFileId(), reservedPageCounter.getAndIncrement()), pageSize);
      int recordPositionInPage = contentHeaderSize;
      int availablePositionIndex = 0;
      final int maxContent = currentPage.getMaxContentSize();

      for (int i = from; i < to; i++) {
        final Binary buffer = buffers[i];

        // Pad to minimum record size
        while (buffer.size() < MINIMUM_RECORD_SIZE)
          buffer.append((byte) 0);

        final int actualSize = buffer.size();
        final int spaceNeeded = Binary.getNumberSpace(actualSize) + actualSize;

        // Check if current page has room
        if (availablePositionIndex >= maxRecordsInPage
            || recordPositionInPage + spaceNeeded > maxContent) {
          // Finalize current page record count
          currentPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) availablePositionIndex);

          // Create a new page
          currentPage = database.getTransaction()
              .addPage(new PageId(database, file.getFileId(), reservedPageCounter.getAndIncrement()), pageSize);
          recordPositionInPage = contentHeaderSize;
          availablePositionIndex = 0;
        }

        // Write the record offset in the record table
        currentPage.writeUnsignedInt(
            PAGE_RECORD_TABLE_OFFSET + availablePositionIndex * INT_SERIALIZED_SIZE,
            recordPositionInPage);

        // Write record size + content
        final int sizeBytes = currentPage.writeNumber(recordPositionInPage, actualSize);
        currentPage.writeByteArray(recordPositionInPage + sizeBytes,
            buffer.getContent(), buffer.getContentBeginOffset(), actualSize);

        // Assign RID
        ridsOut[i - from] = new RID(file.getFileId(),
            ((long) currentPage.getPageId().getPageNumber()) * maxRecordsInPage + availablePositionIndex);

        recordPositionInPage += sizeBytes + actualSize;
        availablePositionIndex++;
      }

    // Finalize last page record count
    if (availablePositionIndex > 0)
      currentPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) availablePositionIndex);
  }

  private boolean updateRecordInternal(final Record record, final RID rid, final boolean updatePlaceholderContent,
                                       final boolean discardRecordAfter) {
    if (rid.getPosition() < 0)
      throw new IllegalArgumentException("Cannot update a record with invalid RID");

    final Binary buffer = database.getSerializer().serialize(database, record);

    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    if (pageId >= pageCount.get()) {
      final int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);
    }

    try {
      final MutablePage page = database.getTransaction()
              .getPageToModify(new PageId(database, file.getFileId(), pageId), pageSize, false);
      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage >= recordCountInPage)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      final int recordPositionInPage = getRecordPositionInPage(page, positionInPage);
      if (recordPositionInPage == 0)
        // DELETED RECORD (>= 24.1.1, IT WAS CLEANED CORRUPTED RECORD BEFORE)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      final long[] recordSize = page.readNumberAndSize(recordPositionInPage);
      if (recordSize[0] == 0L)
        // DELETED
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

//      LogManager.instance()
//          .log(this, Level.SEVERE, "UPDATE %s pageV=%d content %s (threadId=%d)", rid, page.getVersion(), record.toJSON(), Thread.currentThread().threadId());

      // DISJOINT-SLOT MERGE (#5381): only a same-or-smaller in-place overwrite (the branch far below) touches
      // just this record's slot and thus commutes with concurrent writes to other slots on the page. Every other
      // update shape here (placeholder pointer, multi-page chunk, record growth that shifts other slots or spills
      // to a placeholder) changes more than this slot, so it poisons the page: the slot merge must never rebase it.
      final TransactionContext slotTx = database.getTransactionIfExists();
      final boolean slotCandidate = slotTx != null && slotTx.isSlotMergeEnabled() && isSlotMergeCandidate(record);

      boolean isPlaceHolder = false;
      if (recordSize[0] == RECORD_PLACEHOLDER_POINTER) {

        // FOUND A RECORD POINTED FROM A PLACEHOLDER
        final RID placeHolderContentRID = new RID(fileId, page.readLong((int) (recordPositionInPage + recordSize[1])));
        if (updateRecordInternal(record, placeHolderContentRID, true, discardRecordAfter)) {
          // UPDATE PLACEHOLDER CONTENT, THE PLACEHOLDER POINTER STAY THE SAME
          if (slotCandidate)
            slotTx.poisonSlotRebasePage(fileId, pageId);
          if (!discardRecordAfter)
            ((RecordInternal) record).setBuffer(buffer.getNotReusable());
          return true;
        }

        // DELETE OLD PLACEHOLDER, A NEW PLACEHOLDER WILL BE CREATED WITH ENOUGH SPACE
        deleteRecordInternal(placeHolderContentRID, true, false);

        recordSize[0] = LONG_SERIALIZED_SIZE;
        recordSize[1] = 1L;
      } else if (recordSize[0] == FIRST_CHUNK) {
        if (slotCandidate)
          slotTx.poisonSlotRebasePage(fileId, pageId);

        updateMultiPageRecord(rid, buffer, page, (int) (recordPositionInPage + recordSize[1]));

        if (!discardRecordAfter)
          ((RecordInternal) record).setBuffer(buffer.getNotReusable());

        return true;

      } else if (recordSize[0] == NEXT_CHUNK) {
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      } else if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
        if (!updatePlaceholderContent)
          throw new RecordNotFoundException("Record " + rid + " not found", rid);

        isPlaceHolder = true;
        recordSize[0] *= -1L;
      }

      final int bufferSize = buffer.size();
      if (bufferSize > recordSize[0]) {
        // GROWTH: shifts other records / spills to a placeholder or chunks. Not a single-slot change -> poison.
        if (slotCandidate)
          slotTx.poisonSlotRebasePage(fileId, pageId);

        // UPDATED RECORD IS LARGER THAN THE PREVIOUS VERSION: MAKE ROOM IN THE PAGE IF POSSIBLE
        final int lastRecordPositionInPage = getLastRecordPositionInPage(page, recordCountInPage);

        final long[] lastRecordSize;
        if (lastRecordPositionInPage != recordPositionInPage) {
          // CURRENT RECORD IS NOT THE LATEST RIGHT IN THE PAGE

          if (lastRecordPositionInPage < contentHeaderSize)
            // IT SHOULD NEVER OCCUR BECAUSE THE CURRENT RECORD IS PRESENT IN THIS PAGE
            throw new DatabaseOperationException("Invalid position on expanding existing record " + rid);

          lastRecordSize = page.readNumberAndSize(lastRecordPositionInPage);

          if (lastRecordSize[0] == RECORD_PLACEHOLDER_POINTER) {
            lastRecordSize[0] = LONG_SERIALIZED_SIZE;
            lastRecordSize[1] = 1L;
          } else if (lastRecordSize[0] == FIRST_CHUNK || lastRecordSize[0] == NEXT_CHUNK) {
            // CONSIDER THE CHUNK SIZE
            lastRecordSize[0] = page.readInt((int) (lastRecordPositionInPage + lastRecordSize[1]));
            lastRecordSize[1] = 1L + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE;
          } else if (lastRecordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
            lastRecordSize[0] *= -1L;
          }
        } else
          lastRecordSize = recordSize;

        final int pageOccupiedInBytes = (int) (lastRecordPositionInPage + lastRecordSize[0] + lastRecordSize[1]);
        final int spaceAvailableInCurrentPage = page.getMaxContentSize() - pageOccupiedInBytes;
        final int bufferSizeLength = Binary.getNumberSpace(isPlaceHolder ? -1L * bufferSize : bufferSize);
        final int additionalSpaceNeeded = (int) (bufferSize + bufferSizeLength - recordSize[0] - recordSize[1]);

        if (additionalSpaceNeeded < spaceAvailableInCurrentPage) {
          // THERE IS SPACE LEFT IN THE PAGE, SHIFT ON THE RIGHT THE EXISTENT RECORDS

          if (lastRecordPositionInPage != recordPositionInPage) {
            // NOT LAST RECORD IN PAGE, SHIFT NEXT RECORDS
            final int from = (int) (recordPositionInPage + recordSize[0] + recordSize[1]);

            page.move(from, from + additionalSpaceNeeded, pageOccupiedInBytes - from);

            // TODO: CALCULATE THE REAL SIZE TO COMPACT DELETED RECORDS/PLACEHOLDERS
            for (int pos = 0; pos < recordCountInPage; ++pos) {
              final int nextRecordPosInPage = getRecordPositionInPage(page, pos);
              if (nextRecordPosInPage != 0 &&//
                      nextRecordPosInPage >= from &&//
                      nextRecordPosInPage <= pageOccupiedInBytes)
                page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pos * INT_SERIALIZED_SIZE,
                        nextRecordPosInPage + additionalSpaceNeeded);

              assert nextRecordPosInPage + additionalSpaceNeeded < page.getMaxContentSize();
            }
          }

          recordSize[1] = page.writeNumber(recordPositionInPage, isPlaceHolder ? -1L * bufferSize : bufferSize);
          final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);

          page.writeByteArray(recordContentPositionInPage, buffer.getContent(), buffer.getContentBeginOffset(), bufferSize);

          LogManager.instance()
                  .log(this, Level.FINE, "Updated record %s by allocating new space on the same page (%s threadId=%d)", null, rid, page,
                          Thread.currentThread().threadId());

          updatePageStatistics(pageId, spaceAvailableInCurrentPage, -additionalSpaceNeeded);

        } else {
          if (isPlaceHolder)
            // CANNOT CREATE A PLACEHOLDER OF PLACEHOLDER
            return false;

          int availableSpaceInCurrentPage = (int) (recordSize[0] + recordSize[1]);
          if (lastRecordPositionInPage == recordPositionInPage)
            // SINCE IT'S THE LAST RECORD IN THE PAGE, GET ALSO THE REST OF THE SPACE AVAILABLE IN THE PAGE
            availableSpaceInCurrentPage += spaceAvailableInCurrentPage;

          // TODO: LOOK FOR 1/2 OF THE RECORD SIZE
          if (availableSpaceInCurrentPage < 2 + LONG_SERIALIZED_SIZE + INT_SERIALIZED_SIZE) {
            final int bytesWritten = page.writeNumber(recordPositionInPage, RECORD_PLACEHOLDER_POINTER);

            final RID realRID = createRecordInternal(record, true, false);
            page.writeLong(recordPositionInPage + bytesWritten, realRID.getPosition());

            LogManager.instance()
                    .log(this, Level.FINE, "Updated record %s by allocating new space with a placeholder (%s threadId=%d)", null, rid,
                            page, Thread.currentThread().threadId());
          } else {
            // SPLIT THE RECORD IN CHUNKS AS LINKED LIST AND STORE THE FIRST PART ON CURRENT PAGE ISSUE https://github.com/ArcadeData/arcadedb/issues/332
            writeMultiPageRecord(rid, buffer, page, recordPositionInPage, availableSpaceInCurrentPage);

            LogManager.instance().log(this, Level.FINE,
                    "Updated record %s by splitting it in multiple chunks to be saved in multiple pages (%s threadId=%d)", null, rid,
                    page, Thread.currentThread().threadId());
          }
        }
      } else {
        // UPDATED RECORD CONTENT IS NOT LARGER THAN PREVIOUS VERSION: OVERWRITE THE CONTENT
        // CREATE A HOLE (REMOVED LATER BY COMPRESS-PAGE)

        // DISJOINT-SLOT MERGE (#5381): a plain in-place overwrite of a single record - the rebasable case (e.g.
        // the vertex edge-list head-pointer flip on super-node insertion). Capture the pre-image BEFORE writing:
        // at commit it lets the rebase tell a false page conflict (concurrent write to ANOTHER slot) from a true
        // one (a concurrent write to THIS record). Placeholder content lives behind a pointer on another page, so
        // rebasing this page in isolation would be unsound: poison it instead.
        if (slotCandidate) {
          if (isPlaceHolder)
            slotTx.poisonSlotRebasePage(fileId, pageId);
          // Skip the pre-image + final-image copies on a page that is already poisoned (they would be discarded).
          else if (!slotTx.isSlotRebasePagePoisoned(fileId, pageId)) {
            final byte[] baseBody = new byte[(int) recordSize[0]];
            page.readByteArray((int) (recordPositionInPage + recordSize[1]), baseBody, 0, baseBody.length);
            slotTx.trackRebasableUpdate(fileId, pageId, positionInPage, baseBody,
                    Arrays.copyOfRange(buffer.getContent(), buffer.getContentBeginOffset(), buffer.getContentBeginOffset() + bufferSize));
          }
        }

        recordSize[1] = page.writeNumber(recordPositionInPage, isPlaceHolder ? -1L * bufferSize : bufferSize);
        final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);
        page.writeByteArray(recordContentPositionInPage, buffer.getContent(), buffer.getContentBeginOffset(), bufferSize);

        LogManager.instance()
                .log(this, Level.FINE, "Updated record %s with the same size or less as before (%s threadId=%d)", null, rid, page,
                        Thread.currentThread().threadId());
      }

      if (!discardRecordAfter)
        ((RecordInternal) record).setBuffer(buffer.getNotReusable());

      return true;

    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on update record " + rid, e);
    }
  }

  private void deleteRecordInternal(final RID rid, final boolean deletePlaceholderContent, final boolean deleteChunks) {
    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    // DISJOINT-SLOT MERGE (#5381): a delete frees a slot and can relink placeholder/chunk records elsewhere, so
    // it is never a pure single-slot change - keep the page out of the slot merge.
    final TransactionContext slotTx = database.getTransactionIfExists();
    if (slotTx != null && slotTx.isSlotMergeEnabled())
      slotTx.poisonSlotRebasePage(fileId, pageId);

    database.getTransaction().removeRecordFromCache(rid);

    if (pageId >= pageCount.get()) {
      final int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);
    }

    MutablePage page = null;
    try {
      page = database.getTransaction().getPageToModify(new PageId(database, file.getFileId(), pageId), pageSize, false);

      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage >= recordCountInPage)
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      final int recordPositionInPage = getRecordPositionInPage(page, positionInPage);
      if (recordPositionInPage < 1)
        // CLEANED CORRUPTED RECORD
        throw new RecordNotFoundException("Record " + rid + " not found", rid);

      // AVOID DELETION OF CONTENT IN CORRUPTED RECORD
      if (recordPositionInPage < page.getContentSize()) {
        long[] recordSize = page.readNumberAndSize(recordPositionInPage);
        if (recordSize[0] == 0)
          // ALREADY DELETED
          throw new RecordNotFoundException("Record " + rid + " not found", rid);

        if (recordSize[0] == RECORD_PLACEHOLDER_POINTER) {
          // FOUND PLACEHOLDER POINTER: DELETE THE PLACEHOLDER CONTENT FIRST
          final RID placeHolderContentRID = new RID(fileId, page.readLong((int) (recordPositionInPage + recordSize[1])));
          try {
            deleteRecordInternal(placeHolderContentRID, true, false);
          } catch (RecordNotFoundException e) {
            // PARTIAL RECORD NOT FOUND
          }

        } else if (recordSize[0] == FIRST_CHUNK) {
          // 1ST CHUNK: DELETE ALL THE CHUNKS
          MutablePage chunkPage = page;
          int chunkRecordPositionInPage = recordPositionInPage;
          for (int chunkId = 0; ; ++chunkId) {
            final long nextChunkPointer = chunkPage.readLong(
                    (int) (chunkRecordPositionInPage + recordSize[1] + INT_SERIALIZED_SIZE));

            if (nextChunkPointer == 0)
              // LAST CHUNK
              break;

            // READ THE NEXT CHUNK
            final int chunkPageId = (int) (nextChunkPointer / maxRecordsInPage);
            final int chunkPositionInPage = (int) (nextChunkPointer % maxRecordsInPage);

            chunkPage = database.getTransaction()
                    .getPageToModify(new PageId(database, file.getFileId(), chunkPageId), pageSize, false);
            chunkRecordPositionInPage = getRecordPositionInPage(chunkPage, chunkPositionInPage);
            if (chunkRecordPositionInPage == 0)
              // Chunk was deleted by a concurrent update — signal retry
              throw new ConcurrentModificationException(
                      "Multi-page record " + rid + " chunk " + chunkId + " was modified concurrently. Please retry");

            recordSize = chunkPage.readNumberAndSize(chunkRecordPositionInPage);

            if (recordSize[0] != NEXT_CHUNK)
              // Chunk was overwritten by a concurrent operation — signal retry
              throw new ConcurrentModificationException(
                      "Multi-page record " + rid + " chunk " + chunkId + " was modified concurrently. Please retry");

            try {
              deleteRecordInternal(new RID(fileId, nextChunkPointer), false, true);
            } catch (RecordNotFoundException e) {
              // PARTIAL RECORD NOT FOUND
            }
          }

        } else if (recordSize[0] == NEXT_CHUNK) {
          if (!deleteChunks)
            // CANNOT DELETE A CHUNK DIRECTLY
            throw new RecordNotFoundException("Record " + rid + " not found", rid);

        } else if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
          if (!deletePlaceholderContent)
            // CANNOT DELETE A PLACEHOLDER DIRECTLY
            throw new RecordNotFoundException("Record " + rid + " not found", rid);
        } else if (database.getConfiguration().getValueAsBoolean(GlobalConfiguration.BUCKET_WIPEOUT_ONDELETE)) {
          // WIPE OUT RECORD CONTENT
          try {
            page.writeZeros(recordPositionInPage + 1, (int) (recordSize[0] + recordSize[1] - 1));
          } catch (Exception e) {
            // IGNORE IT
            LogManager.instance().log(this, Level.SEVERE, "Error on wiping out page content", e);
          }
        }

        // POINTER = 0 MEANS DELETED
        page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0);

        // Track deleted RID to prevent reuse within the same transaction
        database.getTransaction().addDeletedRecord(rid);

        if (recordSize[0] > -1) {
          if (reuseSpaceMode.ordinal() >= REUSE_SPACE_MODE.HIGH.ordinal()) {
            // UPDATE THE STATISTICS
            final PageAnalysis pageAnalysis = new PageAnalysis(page);
            pageAnalysis.totalRecordsInPage = recordCountInPage;
            getFreeSpaceInPage(pageAnalysis);
            updatePageStatistics(pageId, pageAnalysis.spaceAvailableInCurrentPage, (int) ((recordSize[0] + recordSize[1]) * -1));
          } else
            changesFromLastStats.incrementAndGet();
        }

      } else {
        // CORRUPTED RECORD: WRITE ZERO AS POINTER TO RECORD
        page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0L);
      }

      LogManager.instance()
              .log(this, Level.FINE, "Deleted record %s (%s threadId=%d)", null, rid, page, Thread.currentThread().threadId());

    } catch (final RecordNotFoundException e) {
      throw e;
    } catch (final ConcurrentModificationException e) {
      // #4932: this is the retry signal deliberately thrown above when a multi-page chunk chain was modified
      // concurrently. The generic catch below used to swallow it, zero the slot pointer and return success:
      // the retry never happened, the remaining NEXT_CHUNK records were orphaned (permanent space leak) and
      // the caller believed the delete succeeded. Rethrow so the retry machinery actually retries.
      throw e;
    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on deletion of record " + rid, e);
    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on deleting record %s content", e, rid);

      if (page != null)
        // POINTER = 0 MEANS DELETED
        page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0);
    }
  }

  public void compressPage(final MutablePage page, final boolean forceWipeOut) throws IOException {
    final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

    final List<int[]> orderedRecordContentInPage = getOrderedRecordsInPage(page, recordCountInPage, false);

    if (orderedRecordContentInPage.isEmpty()) {
      if (recordCountInPage > 0) {
        // RESET RECORD COUNTER TO 0
        page.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) 0);
        LogManager.instance().log(this, Level.FINE, "Update record count from %d to 0 in page %s", recordCountInPage, page.pageId);
        wipeOutFreeSpace(page, (short) 0);
      }
      return;
    }

    final List<int[]> holes = computePageHoles(orderedRecordContentInPage);

    defragPage(page, holes, recordCountInPage);

    if (!holes.isEmpty()) {
      LogManager.instance().log(this, Level.FINE, "Compressed page %s removed %d holes", page.pageId, holes.size());

      // UPDATE THE RECORD COUNT
      // LAST POSITION IN THE PAGE, UPDATE THE TOTAL RECORDS IN THE PAGE GOING BACK FROM THE CURRENT RECORD
      int newRecordCount = -1;
      for (int i = recordCountInPage - 1; i > -1; i--) {
        final int pos = getRecordPositionInPage(page, i);
        if (pos == 0 || page.readNumberAndSize(pos)[0] == 0)
          // DELETED RECORD
          newRecordCount = i;
        else
          break;
      }

      if (newRecordCount > -1) {
        // UPDATE TOTAL RECORDS IN THE PAGE
        LogManager.instance()
                .log(this, Level.FINE, "Update record count from %d to %d in page %s", recordCountInPage, newRecordCount, page.pageId);
        page.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) newRecordCount);
      }

    }

    if (forceWipeOut || !holes.isEmpty())
      wipeOutFreeSpace(page, recordCountInPage);
  }

  private void wipeOutFreeSpace(final MutablePage page, final short recordCountInPage) throws IOException {
    if (database.getConfiguration().getValueAsBoolean(GlobalConfiguration.BUCKET_WIPEOUT_ONDELETE)) {
      // WIPE OUT FREE SPACE IN THE PAGE. THIS HELPS WITH THE BACKUP OF DATABASE INCREASING THE COMPRESSION RATE
      try {
        final PageAnalysis pageAnalysis = new PageAnalysis(page);
        pageAnalysis.totalRecordsInPage = recordCountInPage;
        getFreeSpaceInPage(pageAnalysis);

        if (pageAnalysis.spaceAvailableInCurrentPage > 0)
          page.writeZeros(pageAnalysis.newRecordPositionInPage, page.getMaxContentSize() - pageAnalysis.newRecordPositionInPage);
      } catch (Exception e) {
        // IGNORE IT
        LogManager.instance().log(this, Level.SEVERE, "Error on wiping out page content", e);
      }
    }
  }

  private void defragPage(final MutablePage page, final List<int[]> holes, final short recordCountInPage) {
    int gap = 0;
    for (int i = 0; i < holes.size(); i++) {
      final int[] hole = holes.get(i);
      final int marginEnd = i < holes.size() - 1 ? holes.get(i + 1)[0] : page.getContentSize();

      final int from = hole[0] + hole[1];
      final int to = hole[0] - gap;
      final int length = marginEnd - from;
      if (length < 1)
        LogManager.instance().log(this, Level.SEVERE, "Error on reusing hole in page %s, invalid length %d", page.pageId, length);

      LogManager.instance().log(this, Level.FINE, "Moving segment page %s %d-(%d)->%d...", page.pageId, from, length, to);
      page.move(from, to, length);

      // SHIFT ALL THE POINTERS FROM THE HOLE TO THE LAST
      for (int positionInPage = 0; positionInPage < recordCountInPage; positionInPage++) {
        final int recordPositionInPage = (int) page.readUnsignedInt(
                PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);
        if (recordPositionInPage == 0 || recordPositionInPage >= page.getContentSize())
          // AVOID TOUCHING DELETED OR CORRUPTED RECORD
          continue;

        if (recordPositionInPage >= from && recordPositionInPage <= from + length) {
          page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE,
                  recordPositionInPage - hole[1] - gap);
          LogManager.instance().log(this, Level.FINE, "- record %d %d->%d", positionInPage, recordPositionInPage,
                  recordPositionInPage - hole[1] - gap);
        }
      }

      gap += hole[1];
    }
  }

  private List<int[]> computePageHoles(final List<int[]> orderedRecordContentInPage) {
    final List<int[]> holes = new ArrayList<>(128);

    // USE INT BECAUSE OF THE LIMITED SIZE OF THE PAGE
    int[] lastPointer = new int[] { PAGE_RECORD_TABLE_OFFSET + maxRecordsInPage * INT_SERIALIZED_SIZE, 0 };
    for (int i = 0; i < orderedRecordContentInPage.size(); i++) {
      final int[] pointer = orderedRecordContentInPage.get(i);
      final int lastPointerEnd = lastPointer[0] + lastPointer[1];
      if (pointer[0] != lastPointerEnd) {
        final int[] lastHole = holes.isEmpty() ? null : holes.getLast();
        if (lastHole != null && lastHole[0] + lastHole[1] == pointer[0]) {
          // UPDATE PREVIOUS HOLE
          lastHole[1] += pointer[1];
        } else {
          final int delta = pointer[0] - lastPointerEnd;
          if (delta < 1)
            continue;
          // CREATE A NEW HOLE
          holes.add(new int[] { lastPointerEnd, delta });
        }
      }
      lastPointer = pointer;
    }
    return holes;
  }

  private List<int[]> getOrderedRecordsInPage(BasePage page, final short recordCountInPage, final boolean readOnly) {
    final List<int[]> orderedRecordContentInPage = new ArrayList<>(DEF_MAX_RECORDS_IN_PAGE);

    for (int positionInPage = 0; positionInPage < recordCountInPage; positionInPage++) {
      final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);
      if (recordPositionInPage < 1 || recordPositionInPage >= page.getContentSize())
        // SKIP DELETED RECORD (>=24.1.1), OR CORRUPTED
        continue;

      final int size;
      try {
        final long[] recordSize = page.readNumberAndSize(recordPositionInPage);
        if (recordSize[0] == 0) {
          // DELETED <24.1.1
          if (!readOnly) {
            // SET 0 IN THE RECORD TABLE
            if (!(page instanceof MutablePage))
              page = database.getTransaction().getPageToModify(page);
            ((MutablePage) page).writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0L);
          }
          continue;
        }

        if (recordSize[0] == RECORD_PLACEHOLDER_POINTER)
          size = LONG_SERIALIZED_SIZE + (int) recordSize[1];
        else if (recordSize[0] == FIRST_CHUNK || recordSize[0] == NEXT_CHUNK) {
          final int chunkSize = page.readInt(recordPositionInPage + (int) recordSize[1]);
          size = chunkSize + (int) recordSize[1] + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE; // LONG = nextChunkPointer
        } else if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT)
          // PLACEHOLDER CONTENT, CONSIDER THE RECORD SIZE (CONVERTED FROM NEGATIVE NUMBER) + VARINT SIZE
          size = (int) (-1 * recordSize[0]) + (int) recordSize[1];
        else
          size = (int) recordSize[0] + (int) recordSize[1];

        if (size < 0 || size > getPageSize() - contentHeaderSize) {
          // INVALID SIZE
          LogManager.instance().log(this, Level.SEVERE,
                  "Invalid record size " + size + " for record #" + fileId + ":"
                          + recordPosition(page.pageId.getPageNumber(), maxRecordsInPage, positionInPage) + ": deleting record");

          if (readOnly) {
            if (!(page instanceof MutablePage))
              page = database.getTransaction().getPageToModify(page);
            ((MutablePage) page).writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0L);
          }
          continue;
        }
      } catch (Exception e) {
        LogManager.instance().log(this, Level.SEVERE,
                "Error on loading record #" + fileId + ":" + recordPosition(page.pageId.getPageNumber(), maxRecordsInPage, positionInPage));
        continue;
      }

      orderedRecordContentInPage.add(new int[] { recordPositionInPage, size });
    }

    orderedRecordContentInPage.sort(Comparator.comparingLong(a -> a[0]));

    return orderedRecordContentInPage;
  }

  /**
   * Loads a multi-page record with version validation to detect concurrent modifications.
   * Under READ_COMMITTED isolation (the default), pages are not cached in the transaction,
   * so different pages of a multi-page record can be read from different commit points.
   * This can cause silent data corruption when another transaction modifies the record
   * between page reads. To detect this, we validate that the first page version hasn't
   * changed after reading all chunks. If it has, the read is automatically retried (up to
   * {@link GlobalConfiguration#TX_RETRIES} times) to handle transient conflicts transparently.
   * This ensures read-only queries do not fail with ConcurrentModificationException.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private Binary loadMultiPageRecord(final RID originalRID, BasePage firstPage, int recordPositionInPage,
                                     long[] recordSize) throws IOException {
    final int maxRetries = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES);
    final PageId firstPageId = firstPage.pageId;

    for (int retry = 0; retry <= maxRetries; retry++) {
      // Track ALL page versions during the chain walk for consistency validation.
      // Under READ_COMMITTED isolation, each page is loaded independently from disk/cache.
      // A concurrent commit between page loads can produce an inconsistent mix of old and new
      // chunk data, leading to truncated records (BufferUnderflowException on deserialization).
      final List<long[]> pageVersions = new ArrayList<>(); // [fileId, pageNumber, version]
      pageVersions.add(new long[] { firstPage.pageId.getFileId(), firstPage.pageId.getPageNumber(), firstPage.getVersion() });

      boolean chainInconsistent = false;
      final Binary record = new Binary();
      try {
        BasePage page = firstPage;
        int currentRecordPositionInPage = recordPositionInPage;
        long[] currentRecordSize = recordSize;

        while (true) {
          final int chunkSize = page.readInt((int) (currentRecordPositionInPage + currentRecordSize[1]));
          final long nextChunkPointer = page.readLong(
                  (int) (currentRecordPositionInPage + currentRecordSize[1] + INT_SERIALIZED_SIZE));
          final Binary chunk = page.getImmutableView(
                  (int) (currentRecordPositionInPage + currentRecordSize[1] + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE),
                  chunkSize);
          record.append(chunk);

          if (nextChunkPointer == 0)
            break;

          final int chunkPageId = (int) (nextChunkPointer / maxRecordsInPage);
          final int chunkPositionInPage = (int) (nextChunkPointer % maxRecordsInPage);

          if (chunkPageId >= getTotalPages()) {
            chainInconsistent = true;
            break;
          }

          final BasePage nextPage = database.getTransaction()
                  .getPage(new PageId(database, file.getFileId(), chunkPageId), pageSize);

          final int nextRecordPositionInPage = getRecordPositionInPage(nextPage, chunkPositionInPage);
          if (nextRecordPositionInPage == 0) {
            chainInconsistent = true;
            break;
          }

          if (nextPage.equals(page) && currentRecordPositionInPage == nextRecordPositionInPage)
            throw new DatabaseOperationException(
                    "Infinite loop on loading multi-page record " + originalRID + " chunk " + chunkPageId + "/"
                            + chunkPositionInPage);

          page = nextPage;
          pageVersions.add(new long[] { page.pageId.getFileId(), page.pageId.getPageNumber(), page.getVersion() });
          currentRecordPositionInPage = nextRecordPositionInPage;

          currentRecordSize = page.readNumberAndSize(currentRecordPositionInPage);

          if (currentRecordSize[0] != NEXT_CHUNK) {
            chainInconsistent = true;
            break;
          }
        }
      } catch (final Exception e) {
        chainInconsistent = true;
      }

      if (!chainInconsistent) {
        // Validate ALL page versions: re-check each page to detect concurrent modifications.
        // If any page was modified during our read, the assembled data may be inconsistent.
        for (final long[] pv : pageVersions) {
          final BasePage currentPage = database.getPageManager()
                  .getImmutablePage(new PageId(database, (int) pv[0], (int) pv[1]), pageSize, false, true);
          if (currentPage != null && currentPage.getVersion() != pv[2]) {
            chainInconsistent = true;
            break;
          }
        }
      }

      if (!chainInconsistent) {
        record.position(0);
        return record;
      }

      // Retry by re-fetching the first page with fresh data
      if (retry < maxRetries) {
        LogManager.instance().log(this, Level.FINE,
                "Multi-page record %s read inconsistent (attempt %d/%d), retrying...", originalRID,
                retry + 1, maxRetries);
        firstPage = database.getPageManager().getImmutablePage(firstPageId, pageSize, false, true);
        if (firstPage == null)
          throw new ConcurrentModificationException(
                  "First page of multi-page record " + originalRID + " was removed during read");
        recordPositionInPage = getRecordPositionInPage(firstPage, (int) (originalRID.getPosition() % maxRecordsInPage));
        if (recordPositionInPage == 0)
          throw new ConcurrentModificationException(
                  "Multi-page record " + originalRID + " was deleted during read");
        recordSize = firstPage.readNumberAndSize(recordPositionInPage);
      } else
        throw new ConcurrentModificationException(
                "Multi-page record " + originalRID + " was modified during read after " + maxRetries
                        + " retries. Please retry the operation");
    }

    throw new DatabaseOperationException("Failed to load multi-page record " + originalRID);
  }

  private int getRecordPositionInPage(final BasePage page, final int positionInPage) throws IOException {
    final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);
    if (recordPositionInPage != 0 && recordPositionInPage < contentHeaderSize)
      throw new IOException("Invalid record #" + fileId + ":" + (page.pageId.getPageNumber() * maxRecordsInPage + positionInPage));
    return recordPositionInPage;
  }

  private void writeMultiPageRecord(final RID originalRID, final Binary buffer, MutablePage currentPage, int newPosition,
                                    final int availableSpaceForFirstChunk) throws IOException {
    int bufferSize = buffer.size();

    // WRITE THE 1ST CHUNK
    int byteWritten = currentPage.writeNumber(newPosition, FIRST_CHUNK);

    newPosition += byteWritten;

    // WRITE CHUNK SIZE
    int chunkSize = availableSpaceForFirstChunk - byteWritten - INT_SERIALIZED_SIZE - LONG_SERIALIZED_SIZE;
    currentPage.writeInt(newPosition, chunkSize);

    newPosition += INT_SERIALIZED_SIZE;

    // SAVE THE POSITION OF THE POINTER TO THE NEXT CHUNK
    int nextChunkPointerOffset = newPosition;

    newPosition += LONG_SERIALIZED_SIZE;

    final byte[] content = buffer.getContent();
    int contentOffset = buffer.getContentBeginOffset();

    currentPage.writeByteArray(newPosition, content, contentOffset, chunkSize);

    bufferSize -= chunkSize;
    contentOffset += chunkSize;

    // WRITE ALL THE REMAINING CHUNKS IN NEW PAGES
    int txPageCounter = getTotalPages();
    while (bufferSize > 0) {
      MutablePage nextPage = null;
      int recordIdInPage = 0;

      final int spaceNeededForChunk = bufferSize + 2 + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE;

      final PageAnalysis pageAnalysis = findAvailableSpace(currentPage.pageId.getPageNumber(), spaceNeededForChunk, txPageCounter,
              true);

      if (!pageAnalysis.createNewPage) {
        nextPage = database.getTransaction().getPageToModify(pageAnalysis.page.pageId, pageSize, false);

        if (nextPage.getVersion() != pageAnalysis.page.getVersion()) {
          // Page was modified by another committed transaction since the space analysis — skip reuse
          nextPage = null;
        } else {
          newPosition = pageAnalysis.newRecordPositionInPage;
          recordIdInPage = pageAnalysis.availablePositionIndex;

          // Verify the slot is still available on the actual mutable page (guard against stale analysis)
          final int existSlotOff = (int) nextPage.readUnsignedInt(
                  PAGE_RECORD_TABLE_OFFSET + recordIdInPage * INT_SERIALIZED_SIZE);
          final short curRecCount = nextPage.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
          if (recordIdInPage < curRecCount && existSlotOff != 0) {
            // Slot was consumed by another operation — fall through to create a new page
            nextPage = null;
          } else {
            nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + recordIdInPage * INT_SERIALIZED_SIZE, newPosition);

            if (recordIdInPage >= pageAnalysis.totalRecordsInPage)
              nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) (recordIdInPage + 1));
          }
        }
      }

      if (nextPage == null) {
        // CREATE A NEW PAGE
        // Atomically reserve the next page number to prevent two concurrent transactions
        // from allocating the same page number (which would cause silent data corruption
        // when their commits both succeed and overwrite each other's chunks).
        final int reservedPageNumber = reservedPageCounter.getAndIncrement();
        nextPage = database.getTransaction().addPage(new PageId(database, file.getFileId(), reservedPageNumber), pageSize);
        txPageCounter = reservedPageNumber + 1;
        newPosition = contentHeaderSize;
        nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET, newPosition);
        nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) 1);
      }

      // WRITE IN THE PREVIOUS PAGE POINTER THE CURRENT POSITION OF THE NEXT CHUNK
      currentPage.writeLong(nextChunkPointerOffset,
              (long) nextPage.getPageId().getPageNumber() * maxRecordsInPage + recordIdInPage);

      int spaceAvailableInCurrentPage = nextPage.getMaxContentSize() - newPosition;

      byteWritten = nextPage.writeNumber(newPosition, NEXT_CHUNK);
      spaceAvailableInCurrentPage -= byteWritten;

      // WRITE CHUNK SIZE
      chunkSize = spaceAvailableInCurrentPage - INT_SERIALIZED_SIZE - LONG_SERIALIZED_SIZE;
      final boolean lastChunk = bufferSize <= chunkSize;
      if (bufferSize < chunkSize)
        // LAST CHUNK, OVERWRITE THE SIZE WITH THE REMAINING CONTENT SIZE
        chunkSize = bufferSize;

      if (chunkSize < 1)
        throw new IllegalArgumentException("Chunk size invalid (" + chunkSize + ")");

      newPosition += byteWritten;
      nextPage.writeInt(newPosition, chunkSize);

      // SAVE THE POSITION OF THE POINTER TO THE NEXT CHUNK
      nextChunkPointerOffset = newPosition + INT_SERIALIZED_SIZE;
      if (lastChunk)
        nextPage.writeLong(nextChunkPointerOffset, 0L);

      newPosition += INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE;

      nextPage.writeByteArray(newPosition, content, contentOffset, chunkSize);

      updatePageStatistics(nextPage.pageId.getPageNumber(), spaceAvailableInCurrentPage, -chunkSize);

      bufferSize -= chunkSize;
      contentOffset += chunkSize;
      currentPage = nextPage;
    }
  }

  private void updateMultiPageRecord(final RID originalRID, final Binary buffer, MutablePage currentPage, int newPosition)
          throws IOException {
    int chunkSize = currentPage.readInt(newPosition);
    int bufferSize = buffer.size();

    // WRITE THE 1ST CHUNK
    if (bufferSize < chunkSize)
      // 1ST AND LAST CHUNK
      chunkSize = bufferSize;

    currentPage.writeInt(newPosition, chunkSize);

    newPosition += INT_SERIALIZED_SIZE;

    // SAVE THE POSITION OF THE POINTER TO THE NEXT CHUNK
    int nextChunkPointerOffset = newPosition;
    long nextChunkPointer = currentPage.readLong(nextChunkPointerOffset);

    newPosition += LONG_SERIALIZED_SIZE;

    final byte[] content = buffer.getContent();
    int contentOffset = buffer.getContentBeginOffset();

    currentPage.writeByteArray(newPosition, content, contentOffset, chunkSize);

    bufferSize -= chunkSize;
    contentOffset += chunkSize;

    // WRITE ALL THE REMAINING CHUNKS IN NEW PAGES
    long chunkToDeletePointer = bufferSize > 0 ? 0L : nextChunkPointer;

    while (bufferSize > 0) {
      MutablePage nextPage = null;

      if (nextChunkPointer > 0) {
        final int chunkPageId = (int) (nextChunkPointer / maxRecordsInPage);
        final int chunkPositionInPage = (int) (nextChunkPointer % maxRecordsInPage);

        nextPage = database.getTransaction().getPageToModify(new PageId(database, file.getFileId(), chunkPageId), pageSize, false);
        final int recordPositionInPage = getRecordPositionInPage(nextPage, chunkPositionInPage);
        if (recordPositionInPage == 0)
          // Chunk was deleted by a concurrent update to the same record — signal retry
          throw new ConcurrentModificationException(
                  "Multi-page record " + originalRID + " chunk was modified concurrently. Please retry the operation");

        final long[] recordSize = nextPage.readNumberAndSize(recordPositionInPage);

        if (recordSize[0] != NEXT_CHUNK)
          // Chunk was overwritten by a concurrent operation — signal retry
          throw new ConcurrentModificationException(
                  "Multi-page record " + originalRID + " chunk was modified concurrently. Please retry the operation");

        newPosition = (int) (recordPositionInPage + recordSize[1]);
        chunkSize = nextPage.readInt(newPosition);
        nextChunkPointer = nextPage.readLong(newPosition + INT_SERIALIZED_SIZE);

      } else {
        // CREATE NEW SPACE FOR THE CURRENT AND REMAINING CHUNKS
        int recordIdInPage = 0;
        int txPageCounter = getTotalPages();

        final int totalSpaceNeeded = bufferSize + Binary.LONG_SERIALIZED_SIZE + INT_SERIALIZED_SIZE;

        final PageAnalysis pageAnalysis = findAvailableSpace(currentPage.pageId.getPageNumber(), totalSpaceNeeded, txPageCounter,
                true);
        if (!pageAnalysis.createNewPage) {
          nextPage = database.getTransaction().getPageToModify(pageAnalysis.page.pageId, pageSize, false);

          if (nextPage.getVersion() != pageAnalysis.page.getVersion()) {
            // Page was modified by another committed transaction since the space analysis — skip reuse
            nextPage = null;
          } else {
            newPosition = pageAnalysis.newRecordPositionInPage;
            recordIdInPage = pageAnalysis.availablePositionIndex;

            // Verify the slot is still available on the actual mutable page (guard against stale analysis)
            final int existSlotOff2 = (int) nextPage.readUnsignedInt(
                    PAGE_RECORD_TABLE_OFFSET + recordIdInPage * INT_SERIALIZED_SIZE);
            final short curRecCount2 = nextPage.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
            if (recordIdInPage < curRecCount2 && existSlotOff2 != 0) {
              // Slot was consumed by another operation — fall through to create a new page
              nextPage = null;
            } else {
              nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pageAnalysis.availablePositionIndex * INT_SERIALIZED_SIZE,
                      newPosition);

              if (recordIdInPage >= pageAnalysis.totalRecordsInPage)
                nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) (recordIdInPage + 1));

              updatePageStatistics(nextPage.pageId.getPageNumber(), pageAnalysis.spaceAvailableInCurrentPage, -totalSpaceNeeded);
            }
          }
        }

        if (nextPage == null) {
          // CREATE A NEW PAGE
          // Atomically reserve the next page number to prevent two concurrent transactions
          // from allocating the same page number (which would cause silent data corruption).
          final int reservedPageNumber = reservedPageCounter.getAndIncrement();
          nextPage = database.getTransaction().addPage(new PageId(database, file.getFileId(), reservedPageNumber), pageSize);
          newPosition = contentHeaderSize;
          nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET, newPosition);
          nextPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) 1);
          updatePageStatistics(nextPage.pageId.getPageNumber(), nextPage.getAvailableContentSize(), -bufferSize);
        }

        // WRITE IN THE PREVIOUS PAGE POINTER THE CURRENT POSITION OF THE NEXT CHUNK
        currentPage.writeLong(nextChunkPointerOffset,
                (long) nextPage.getPageId().getPageNumber() * maxRecordsInPage + recordIdInPage);
        int spaceAvailableInCurrentPage = nextPage.getMaxContentSize() - newPosition;

        final int byteWritten = nextPage.writeNumber(newPosition, NEXT_CHUNK);
        spaceAvailableInCurrentPage -= byteWritten;

        chunkSize = spaceAvailableInCurrentPage - INT_SERIALIZED_SIZE - LONG_SERIALIZED_SIZE;
        newPosition += byteWritten;
      }

      // WRITE CHUNK SIZE
      final boolean lastChunk = bufferSize <= chunkSize;
      if (bufferSize < chunkSize) {
        // LAST CHUNK
        chunkSize = bufferSize;
        chunkToDeletePointer = nextChunkPointer;
      }

      nextPage.writeInt(newPosition, chunkSize);

      // SAVE THE POSITION OF THE POINTER TO THE NEXT CHUNK
      newPosition += INT_SERIALIZED_SIZE;

      nextChunkPointerOffset = newPosition;
      if (lastChunk)
        nextPage.writeLong(nextChunkPointerOffset, 0L);

      newPosition += LONG_SERIALIZED_SIZE;

      nextPage.writeByteArray(newPosition, content, contentOffset, chunkSize);

      bufferSize -= chunkSize;
      contentOffset += chunkSize;
      currentPage = nextPage;
    }

    // CHECK TO DELETE REMAINING CHUNKS IF THE RECORD SHRUNK
    while (chunkToDeletePointer > 0) {
      currentPage.writeLong(nextChunkPointerOffset, 0L);

      final int chunkPageId = (int) (chunkToDeletePointer / maxRecordsInPage);
      final int chunkPositionInPage = (int) (chunkToDeletePointer % maxRecordsInPage);

      final MutablePage nextPage = database.getTransaction()
              .getPageToModify(new PageId(database, file.getFileId(), chunkPageId), pageSize, false);
      final int recordPositionInPage = getRecordPositionInPage(nextPage, chunkPositionInPage);

      // DELETE THE CHUNK AS RECORD IN THE PAGE
      nextPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + chunkPositionInPage * INT_SERIALIZED_SIZE, 0L);

      if (recordPositionInPage == 0) {
        LogManager.instance()
                .log(this, Level.WARNING, "Error on deleting extra part of multi-page record %s after it shrunk", originalRID);
        break;
      }

      final long[] recordSize = nextPage.readNumberAndSize(recordPositionInPage);

      if (recordSize[0] != NEXT_CHUNK) {
        LogManager.instance()
                .log(this, Level.WARNING, "Error on deleting extra part of multi-page record %s after it shrunk", originalRID);
        break;
      }

      newPosition = (int) (recordPositionInPage + recordSize[1]);
      chunkToDeletePointer = nextPage.readLong(newPosition + INT_SERIALIZED_SIZE);
    }
  }

  private int getLastRecordPositionInPage(final MutablePage page, final int totalRecords) throws IOException {
    int lastRecordPositionInPage = -1;
    for (int i = 0; i < totalRecords; i++) {
      final int recordPositionInPage = getRecordPositionInPage(page, i);
      if (recordPositionInPage != 0 && recordPositionInPage > lastRecordPositionInPage)
        lastRecordPositionInPage = recordPositionInPage;
    }
    return lastRecordPositionInPage;
  }

  /**
   * @param multiPageRecord if true, avoid returning record 0 if available. This is a special case because the record 0 (first record in bucket)
   *                        as next pointer was used since the beginning to indicate the end of a record, before recycling was available.
   */
  private PageAnalysis getAvailableSpaceInPage(final int pageNumber, final int spaceNeeded, final boolean multiPageRecord)
          throws IOException {
    final PageAnalysis result = new PageAnalysis(
            database.getTransaction().getPage(new PageId(database, file.getFileId(), pageNumber), pageSize));

    result.totalRecordsInPage = result.page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
    if (result.totalRecordsInPage > 0) {
      getFreeSpaceInPage(result);

      if (!result.createNewPage && result.spaceAvailableInCurrentPage > -1) {
        if (spaceNeeded + SPARE_SPACE_FOR_GROWTH > result.spaceAvailableInCurrentPage)
          // RECORD TOO BIG FOR THIS PAGE, USE A NEW PAGE
          result.createNewPage = true;
      }
    } else if (pageNumber > 0 || !multiPageRecord) {
      // FIRST RECORD, START RIGHT AFTER THE HEADER
      result.availablePositionIndex = 0;
      result.newRecordPositionInPage = contentHeaderSize;
      result.spaceAvailableInCurrentPage = result.page.getMaxContentSize() - result.newRecordPositionInPage;
    } else {
      // NEVER RETURN THE FIRST RECORD IF IT'S A MULTI-PAGE RECORD
      result.createNewPage = true;
    }
    return result;
  }

  private void getFreeSpaceInPage(final PageAnalysis pageAnalysis) throws IOException {
    if (pageAnalysis.totalRecordsInPage >= maxRecordsInPage) {
      pageAnalysis.createNewPage = true;
      return;
    }

    pageAnalysis.availablePositionIndex = -1;
    pageAnalysis.lastRecordPositionInPage = -1;
    pageAnalysis.newRecordPositionInPage = -1;

    for (int i = 0; i < pageAnalysis.totalRecordsInPage; i++) {
      final int recordPositionInPage = getRecordPositionInPage(pageAnalysis.page, i);
      if (recordPositionInPage == 0) {
        // Check if this position was deleted in the current transaction
        final RID potentialRID = new RID(file.getFileId(),
                ((long) pageAnalysis.page.getPageId().getPageNumber()) * maxRecordsInPage + i);

        if (!database.getTransaction().isDeletedInTransaction(potentialRID)) {
          // REUSE THE FIRST AVAILABLE POSITION FROM DELETED RECORD (from previous transactions only)
          if (pageAnalysis.availablePositionIndex == -1)
            pageAnalysis.availablePositionIndex = i;
        }
      } else if (recordPositionInPage > pageAnalysis.lastRecordPositionInPage)
        pageAnalysis.lastRecordPositionInPage = recordPositionInPage;
    }

    if (pageAnalysis.availablePositionIndex == -1)
      // USE NEW POSITION
      pageAnalysis.availablePositionIndex = pageAnalysis.totalRecordsInPage;

    if (pageAnalysis.lastRecordPositionInPage == -1) {
      // TOTALLY EMPTY PAGE AFTER DELETION, GET THE FIRST POSITION
      pageAnalysis.newRecordPositionInPage = contentHeaderSize;
    } else {
      final long[] lastRecordSize = pageAnalysis.page.readNumberAndSize(pageAnalysis.lastRecordPositionInPage);
      if (lastRecordSize[0] == 0)
        // DELETED (V<24.1.1)
        pageAnalysis.newRecordPositionInPage = pageAnalysis.lastRecordPositionInPage + (int) lastRecordSize[1];
      else if (lastRecordSize[0] > 0)
        // RECORD PRESENT, CONSIDER THE RECORD SIZE + VARINT SIZE
        pageAnalysis.newRecordPositionInPage =
                pageAnalysis.lastRecordPositionInPage + (int) lastRecordSize[0] + (int) lastRecordSize[1];
      else if (lastRecordSize[0] == RECORD_PLACEHOLDER_POINTER)
        // PLACEHOLDER, CONSIDER NEXT 9 BYTES
        pageAnalysis.newRecordPositionInPage =
                pageAnalysis.lastRecordPositionInPage + LONG_SERIALIZED_SIZE + (int) lastRecordSize[1];
      else if (lastRecordSize[0] == FIRST_CHUNK || lastRecordSize[0] == NEXT_CHUNK) {
        // CHUNK
        final int chunkSize = pageAnalysis.page.readInt((int) (pageAnalysis.lastRecordPositionInPage + lastRecordSize[1]));
        pageAnalysis.newRecordPositionInPage =
                pageAnalysis.lastRecordPositionInPage + (int) lastRecordSize[1] + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE
                        + chunkSize;
      } else
        // PLACEHOLDER CONTENT, CONSIDER THE RECORD SIZE (CONVERTED FROM NEGATIVE NUMBER) + VARINT SIZE
        pageAnalysis.newRecordPositionInPage =
                pageAnalysis.lastRecordPositionInPage + (int) (-1 * lastRecordSize[0]) + (int) lastRecordSize[1];
    }
    pageAnalysis.spaceAvailableInCurrentPage = pageAnalysis.page.getMaxContentSize() - pageAnalysis.newRecordPositionInPage;
  }

  /**
   * Find the first page with enough space for the buffer. Algorithm:
   * 1. browse all the pages and find the first page with enough space
   * 2. keep the best page as the page with space available closer to the buffer size
   * 3. if no page has enough space, use the last page
   * 4. if the last page is full, create a new page
   *
   * @param multiPageRecord if true, avoid returning record 0 if available. This is a special case because the record 0 (first record in bucket)
   *                        as next pointer was used since the beginning to indicate the end of a record, before recycling was available.
   */
  private PageAnalysis findAvailableSpace(final int currentPageId, final int spaceNeeded, final int txPageCounter,
                                          final boolean multiPageRecord)
          throws IOException {
    if (reuseSpaceMode.ordinal() >= REUSE_SPACE_MODE.MEDIUM.ordinal()) {
      synchronized (freeSpaceInPages) {
        if (freeSpaceInPages.isEmpty())
          gatherPageStatistics();

        // TRY WITH THE CURRENT PAGE FIRST
        PageAnalysis bestPageAnalysis = null;

        if (currentPageId > -1) {
          // PRIORITIZE SPACE IN THE SAME PAGE
          bestPageAnalysis = getAvailableSpaceInPage(currentPageId, spaceNeeded, multiPageRecord);
          if (bestPageAnalysis.createNewPage || bestPageAnalysis.totalRecordsInPage > maxRecordsInPage)
            bestPageAnalysis = null;
        }

        if (bestPageAnalysis == null) {
          if (freeSpaceInPages.isEmpty())
            gatherPageStatistics();

          if (!freeSpaceInPages.isEmpty()) {
            bestPageAnalysis = findAvailableSpaceFromStatistics(currentPageId, spaceNeeded, multiPageRecord);
            if (bestPageAnalysis == null)
              // TRY AGAIN WITH HALF SIZE, THE RECORD WILL BE SPLIT IN MULTIPLE CHUNKS
              bestPageAnalysis = findAvailableSpaceFromStatistics(currentPageId, spaceNeeded / 2, multiPageRecord);
          }
        }

        if (bestPageAnalysis != null) {
//        LogManager.instance()
//            .log(this, Level.FINEST, "Requesting %db, allocating in %d/%d (free=%db totalRecords=%d total=%d)", bufferSize,
//                bestAvailableSpace.page.pageId.getFileId(), bestAvailableSpace.page.pageId.getPageNumber(),
//                bestAvailableSpace.spaceAvailableInCurrentPage, bestAvailableSpace.totalRecordsInPage, freeSpaceInPages.size());

          return bestPageAnalysis;
        }
      }
    }

    // CHECK IF THERE IS SPACE IN THE LAST PAGE
    return getAvailableSpaceInPage(txPageCounter - 1, spaceNeeded, multiPageRecord);
  }

  /**
   * @param multiPageRecord if true, avoid returning record 0 if available. This is a special case because the record 0 (first record in bucket)
   *                        as next pointer was used since the beginning to indicate the end of a record, before recycling was available.
   */
  private PageAnalysis findAvailableSpaceFromStatistics(final int currentPageId, final int spaceNeeded,
                                                        final boolean multiPageRecord)
          throws IOException {
    // Snapshot keys/values into local arrays so we can safely mutate freeSpaceInPages
    // (put/remove) inside the loop body. The snapshot is bounded by MAX_PAGES_GATHER_STATS (100).
    //
    // The snapshot is sorted by pageId to preserve the deterministic "fill lowest pageId first"
    // allocation behavior that the previous TreeMap-backed implementation provided implicitly via
    // its ordered iteration. Tests like RandomDeleteTest depend on this so that re-inserts after
    // bulk deletes land at the same RIDs as the original inserts.
    final int snapSize = freeSpaceInPages.size();
    final int[] snapPageIds = new int[snapSize];
    final int[] snapPageStats = new int[snapSize];
    final int[] cursor = { 0 };
    freeSpaceInPages.forEach((k, v) -> {
      snapPageIds[cursor[0]] = k;
      snapPageStats[cursor[0]] = v;
      cursor[0]++;
    });
    sortByPageIdAscending(snapPageIds, snapPageStats, snapSize);

    PageAnalysis bestPageAnalysis = null;
    int[] pagesToRemove = null;
    int pagesToRemoveCount = 0;
    // Visible page horizon: the tx can see committed pages PLUS any pages it has added itself
    // (tracked via tx.getPageCounter on this file). Pages beyond this are "phantoms" reserved
    // by other concurrent transactions, not yet committed and not visible via readCache; reusing
    // them at slot 0 would collide with the reserving tx and silently corrupt that record's chain.
    final int txVisiblePageHorizon = getTotalPages();
    for (int s = 0; s < snapSize; s++) {
      final int pageId = snapPageIds[s];
      if (pageId == currentPageId)
        // ALREADY EVALUATED
        continue;

      if (pageId >= txVisiblePageHorizon)
        continue;

      final int pageStats = snapPageStats[s];

      if (pageStats >= spaceNeeded) {
        // CHECK IF THE SPACE AVAILABLE IS REAL
        final PageAnalysis pageAnalysis = getAvailableSpaceInPage(pageId, spaceNeeded, multiPageRecord);

        if (pageAnalysis.totalRecordsInPage >= maxRecordsInPage) {
          if (pagesToRemove == null)
            pagesToRemove = new int[snapSize];
          pagesToRemove[pagesToRemoveCount++] = pageId;
          continue;
        }

        if (!pageAnalysis.createNewPage) {
          if (pageAnalysis.spaceAvailableInCurrentPage != pageStats) {
            // LOW COST UPDATE OF STATISTICS
            if (pageAnalysis.spaceAvailableInCurrentPage < MINIMUM_SPACE_LEFT_IN_PAGE) {
              if (pagesToRemove == null)
                pagesToRemove = new int[snapSize];
              pagesToRemove[pagesToRemoveCount++] = pageId;
            } else
              freeSpaceInPages.put(pageId, pageAnalysis.spaceAvailableInCurrentPage);
          }

          if (multiPageRecord && pageAnalysis.page.pageId.getPageNumber() == 0 && pageAnalysis.availablePositionIndex == 0)
            // AVOID REUSING THE FIRST RECORD IF IT'S A MULTI-PAGE RECORD
            continue;

          bestPageAnalysis = pageAnalysis;
          break;
        }
      }
    }

    if (pagesToRemove != null)
      for (int i = 0; i < pagesToRemoveCount; i++)
        freeSpaceInPages.remove(pagesToRemove[i], -1);

    return bestPageAnalysis;
  }

  /**
   * Gather statistics about the free space in pages. Algorithm:
   * 1. browse all the pages but the latest, and add in a tree map the pages with enough free space (>GATHER_STATS_MIN_SPACE_PERC)
   * 2. save in the map recordCountInPage and freeSpaceInPage
   * 3. if the tree map is full (size > MAX_PAGES_GATHER_STATS), stop
   */
  public void gatherPageStatistics() {
    final boolean firstRun = timeOfLastStats == 0L;
    if (!firstRun && System.currentTimeMillis() - timeOfLastStats <= MAX_TIMEOUT_GATHER_STATS)
      return;

    // #5063: consume the change counter atomically at the decision point. The previous
    // get() > 0 check paired with a set(0L) at the end of the scan wiped any increment landing while the
    // scan ran; getAndSet(0L) carries those increments into the next cycle instead of losing them.
    final long consumedChanges = changesFromLastStats.getAndSet(0L);
    if (consumedChanges > 0 || firstRun)
      try {
        int txPageCount = getTotalPages();

        synchronized (freeSpaceInPages) {
          for (int pageId = 0; pageId < txPageCount - 2; ++pageId) {
            final BasePage page = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageId), pageSize);
            final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
            final List<int[]> orderedRecordContentInPage = getOrderedRecordsInPage(page, recordCountInPage, true);

            // #4958: measure against the usable content region (getMaxContentSize), not the physical
            // page size: the latter overstated the free space of every page by the page header size.
            int freeSpaceInPage = page.getMaxContentSize() - contentHeaderSize;
            if (!orderedRecordContentInPage.isEmpty()) {
              final int[] lastRecord = orderedRecordContentInPage.getLast();
              freeSpaceInPage = page.getMaxContentSize() - (lastRecord[0] + lastRecord[1]);
            }

            final int freeSpacePerc = freeSpaceInPage * 100 / (page.getMaxContentSize() - contentHeaderSize);

            if (freeSpacePerc > GATHER_STATS_MIN_SPACE_PERC)
              freeSpaceInPages.put(pageId, freeSpaceInPage);

            if (freeSpaceInPages.size() >= MAX_PAGES_GATHER_STATS)
              break;
          }

          timeOfLastStats = System.currentTimeMillis();
        }
      } catch (Exception e) {
        // #5063: THE COUNTER WAS ALREADY CONSUMED. RESTORE THE FULL CONSUMED COUNT (NOT A
        // SINGLE INCREMENT, WHICH UNDERCOUNTED THE PENDING CHANGES) SO THE FAILED SCAN IS RETRIED AT THE
        // NEXT CYCLE; max(consumed, 1) COVERS THE firstRun CASE WHERE THE CONSUMED COUNT MAY BE ZERO
        changesFromLastStats.addAndGet(Math.max(consumedChanges, 1L));
        LogManager.instance().log(this, Level.WARNING, "Error on gathering statistics on bucket '%s'", e, getName());
      }
  }

  /**
   * Update the in memory statistics about the free space in page.
   */
  private void updatePageStatistics(final int pageId, final int availableSpace, final int delta) {
    changesFromLastStats.incrementAndGet();

    if (reuseSpaceMode.ordinal() < REUSE_SPACE_MODE.HIGH.ordinal())
      return;

    synchronized (freeSpaceInPages) {
      if (availableSpace + delta == 0)
        freeSpaceInPages.remove(pageId, -1);
      else {
        // #5067: same usable-space base as gatherPageStatistics() (#4958): measure against the usable
        // content region (physical page size minus the page header), not the physical page size, which
        // overstated the space of every page and skewed the GATHER_STATS_MIN_SPACE_PERC threshold
        final int usableSpaceInPage = getPageSize() - BasePage.PAGE_HEADER_SIZE - contentHeaderSize;

        final boolean hasEntry = freeSpaceInPages.containsKey(pageId);
        final int existingFreeSpace = hasEntry ? freeSpaceInPages.get(pageId, 0) : 0;
        final int prevSpace = availableSpace == 0 && hasEntry ? existingFreeSpace : availableSpace;
        final int newSpace = prevSpace + delta;

        if (hasEntry) {
          if (newSpace <= MINIMUM_SPACE_LEFT_IN_PAGE || (freeSpaceInPages.size() >= MAX_PAGES_GATHER_STATS
                  && newSpace * 100 / usableSpaceInPage < GATHER_STATS_MIN_SPACE_PERC))
            freeSpaceInPages.remove(pageId, -1);
          else
            freeSpaceInPages.put(pageId, newSpace);
        } else if (newSpace * 100 / usableSpaceInPage >= GATHER_STATS_MIN_SPACE_PERC) {
          if (freeSpaceInPages.size() >= MAX_PAGES_GATHER_STATS) {
            // REMOVE THE SMALLEST PAGE
            final int[] lowestPageId = { -1 };
            final int[] lowestPageSpace = { -1 };

            freeSpaceInPages.forEach((k, v) -> {
              if (lowestPageId[0] < 0 || v < lowestPageSpace[0]) {
                lowestPageId[0] = k;
                lowestPageSpace[0] = v;
              }
            });

            if (lowestPageId[0] > -1 && lowestPageSpace[0] < newSpace) {
              freeSpaceInPages.remove(lowestPageId[0], -1);
              freeSpaceInPages.put(pageId, newSpace);
            }
          } else
            freeSpaceInPages.put(pageId, newSpace);
        }
      }
    }
  }

  public void setPageStatistics(final JSONArray pages) {
    synchronized (freeSpaceInPages) {
      freeSpaceInPages.clear();
      for (int i = 0; i < pages.length(); i++) {
        final JSONObject page = pages.getJSONObject(i);
        freeSpaceInPages.put(page.getInt("id"), page.getInt("free"));
      }
    }
  }

  /**
   * Insertion sort on two parallel arrays, ordering by ascending {@code pageIds[i]}. Bounded to
   * MAX_PAGES_GATHER_STATS (100) entries so insertion sort is the right choice (low constant,
   * great cache behavior, in-place).
   */
  private static void sortByPageIdAscending(final int[] pageIds, final int[] pageStats, final int n) {
    for (int i = 1; i < n; i++) {
      final int kPage = pageIds[i];
      final int kStats = pageStats[i];
      int j = i - 1;
      while (j >= 0 && pageIds[j] > kPage) {
        pageIds[j + 1] = pageIds[j];
        pageStats[j + 1] = pageStats[j];
        j--;
      }
      pageIds[j + 1] = kPage;
      pageStats[j + 1] = kStats;
    }
  }

  public JSONObject getStatistics() {
    final JSONObject json = new JSONObject();

    final long cachedCount = getCachedRecordCount();
    if (cachedCount > -1)
      json.put("count", cachedCount);

    final JSONArray pages = new JSONArray();
    synchronized (freeSpaceInPages) {
      freeSpaceInPages.forEach((k, v) -> pages.put(new JSONObject().put("id", k).put("free", v)));
    }
    json.put("pages", pages);

    return json;
  }
}
