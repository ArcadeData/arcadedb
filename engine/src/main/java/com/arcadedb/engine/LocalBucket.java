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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
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
  // #5279: pageNumber → slots/space handed out to the transactions that are currently inserting into that page but
  // have not committed yet. Without it, every concurrent transaction inserting into the same bucket picked the SAME
  // free slot of the SAME page (same optimistic RID) and all but the first failed at commit with a page-level
  // ConcurrentModificationException, even though two inserts of DIFFERENT records always commute. An entry exists
  // only while at least one transaction is inserting into that page, so the map is as small as the write
  // concurrency; see reserveInsertSlot().
  private final          Map<Integer, PageInsertReservation> insertReservations     = new ConcurrentHashMap<>();
  private final          REUSE_SPACE_MODE          reuseSpaceMode;
  // #4958: both fields are read/written outside the freeSpaceInPages monitor on some paths (delete,
  // updatePageStatistics), so they must be safe on their own: volatile timestamp + atomic counter.
  private volatile       long                      timeOfLastStats                  = 0L;
  private final          AtomicLong                changesFromLastStats             = new AtomicLong();

  private enum REUSE_SPACE_MODE {
    LOW, MEDIUM, HIGH
  }

  /**
   * Slots and page space handed out to the transactions currently inserting into ONE bucket page (issue #5279).
   * Every access is made under the entry's own monitor, which serialises the claims on a given page.
   */
  private static final class PageInsertReservation {
    /** First slot index no transaction has been given yet: the append high-water mark of the page. */
    private int          nextFreeSlot;
    /** First content offset no transaction has been given yet: the space high-water mark of the page. */
    private int          nextFreeOffset;
    /** Slots reserved by still-running transactions. */
    private final BitSet reservedSlots = new BitSet();
    /** Number of reservations not released yet: the whole entry is dropped when it reaches zero. */
    private int          outstanding;
    /**
     * The only transaction holding reservations on this page, or {@code null} once a second one joined. While a page
     * is uncontended its owner's own view of it is authoritative and the high-water marks must NOT be used: they only
     * ever grow, so a long transaction that deletes and re-inserts (a bulk rewrite) would otherwise see the page as
     * permanently full and start a new one per record.
     */
    private volatile Object soleOwner;
    /** Set when the entry was taken out of the map, so a claim that raced with the removal retries on a fresh one. */
    private boolean         removed;
  }

  private static final Function<Integer, PageInsertReservation> NEW_INSERT_RESERVATION = k -> new PageInsertReservation();

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
    insertReservations.clear();
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
    deleteRecordInternal(rid, false, false, false);
  }

  /**
   * Force-deletes a record even when its multi-page chunk chain is structurally broken. A normal delete walks the
   * chain to free every chunk and, on a broken link, throws {@link ConcurrentModificationException} as a retry
   * signal (#4932) so it never orphans chunks. That guard makes a genuinely-corrupt record undeletable by every
   * path. With {@code force=true} a broken link stops the chain walk instead of aborting: the head slot is still
   * freed so the record finally disappears, and any chunks past the break are orphaned (a bounded space leak) to be
   * reclaimed by compaction or a database check. Intended for admin repair (CHECK DATABASE FIX), not the hot path.
   */
  public void deleteRecord(final RID rid, final boolean force) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.DELETE_RECORD);
    deleteRecordInternal(rid, false, false, force);
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
              deleteRecordInternal(rid, true, true, true);
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

                // Walk the continuation chain to detect a structurally broken multi-page record (a dangling or
                // overwritten chunk pointer). check() otherwise validates only the first chunk's on-page size and
                // would never notice a broken chain, leaving the record undeletable by every normal path because
                // deleteRecordInternal throws the #4932 retry signal on it. On fix, force-delete it: the head slot is
                // freed so the record finally disappears and any unreachable chunks are reclaimed later by compaction.
                final String chainProblem = findBrokenChunkChain(rid, page, recordPositionInPage);
                if (chainProblem != null) {
                  ++totalErrors;
                  warning = "broken multi-page chunk chain for record %s: %s".formatted(rid, chainProblem);
                  if (fix) {
                    deleteRecordInternal(rid, true, true, true);
                    deletedRecordsAfterFix.add(rid);
                    ++totalDeletedRecords;
                    recordSize[0] = 0;
                  }
                }

                if (recordSize[0] == FIRST_CHUNK)
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
                  deleteRecordInternal(rid, true, true, true);
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
                deleteRecordInternal(rid, true, true, true);
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

        if (!createNewPage) {
          final int reservedSlot = claimInsertSlot(foundPage, pageAnalysis, availablePositionIndex,
                  newRecordPositionInPage, spaceNeeded);
          if (reservedSlot < 0)
            // ONCE THE IN-FLIGHT RESERVATIONS ARE COUNTED THE PAGE CANNOT HOST THE RECORD ANYMORE: USE A NEW PAGE
            createNewPage = true;
          else
            availablePositionIndex = reservedSlot;
        }
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
      final boolean singleSlotInsert = !createNewPage && !isPlaceHolder && spaceNeeded <= spaceAvailableInCurrentPage;

      // #5596: a plain insert into a free slot of a REUSED page is exactly what the disjoint-slot merge replays
      // (writeRecordAtSlot writes the same three things: the slot pointer, the record count and the record itself),
      // so declare those bytes covered by it. Everything else here - a multi-page record, a placeholder, a record on
      // a brand-new page - stays undeclared, which is what makes a forgotten poison call harmless.
      final int previousCoverage = selectedPage.beginCoveredWrite(singleSlotInsert ? MutablePage.COVERAGE_SLOT_MERGE : 0);
      final short recordCountInPage;
      try {
        // RESERVE A SPOT IMMEDIATELY TO AVOID USAGE FOR MULTI PAGE RECORD
        selectedPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + availablePositionIndex * INT_SERIALIZED_SIZE,
                newRecordPositionInPage);

        short currentRecordCountInPage = selectedPage.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
        if (availablePositionIndex + 1 > currentRecordCountInPage) {
          // UPDATE RECORD NUMBER
          currentRecordCountInPage = (short) (availablePositionIndex + 1);
          selectedPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, currentRecordCountInPage);
        }
        recordCountInPage = currentRecordCountInPage;

        if (spaceNeeded > spaceAvailableInCurrentPage) {
          // MULTI-PAGE RECORD
          writeMultiPageRecord(rid, buffer, selectedPage, newRecordPositionInPage, spaceAvailableInCurrentPage);

        } else {
          final int byteWritten = selectedPage.writeNumber(newRecordPositionInPage,
                  isPlaceHolder ? (-1L * bufferSize) : bufferSize);
          selectedPage.writeByteArray(newRecordPositionInPage + byteWritten, buffer.getContent(), buffer.getContentBeginOffset(),
                  bufferSize);
          updatePageStatistics(selectedPage.pageId.getPageNumber(), spaceAvailableInCurrentPage, -spaceNeeded);
        }
      } finally {
        selectedPage.endCoveredWrite(previousCoverage);
      }

      LogManager.instance()
              .log(this, Level.FINE, "Created record %s (%s records=%d threadId=%d)", rid, selectedPage, recordCountInPage,
                      Thread.currentThread().threadId());

      // DISJOINT-SLOT MERGE (#5381): a brand-new record inserted into a FREE slot of an EXISTING (reused) page
      // commutes with concurrent writes to other slots of that page. Track it so a commit-time page-version
      // conflict can be resolved by replaying this insert on the newer committed page (see TransactionContext).
      // A record on a brand-new page (createNewPage) has no existing-page conflict to rebase; a multi-page or
      // placeholder record is not a plain single-slot insert, so it poisons the page instead.
      // #5279: an edge-list segment is tracked here too, unlike on the UPDATE path. Allocating a brand-new chunk
      // is an ordinary free-slot insert whose whole content this transaction wrote, so it is replayable; and it
      // is the dominant write of "create a vertex and its first edges", which otherwise made every concurrent
      // case-creation collide on the edge-segment bucket even though the chunks are unrelated.
      final TransactionContext slotTx = database.getTransactionIfExists();
      if (slotTx != null && slotTx.isSlotMergeEnabled() && !createNewPage) {
        final int slotPageNumber = selectedPage.getPageId().getPageNumber();
        if (!singleSlotInsert)
          slotTx.poisonSlotRebasePage(fileId, slotPageNumber);
        // Skip the record-image copy on a page that is already poisoned (it would be discarded anyway).
        else if (!slotTx.isSlotRebasePagePoisoned(fileId, slotPageNumber))
          slotTx.trackRebasableInsert(fileId, slotPageNumber, availablePositionIndex,
                  Arrays.copyOfRange(buffer.getContent(), buffer.getContentBeginOffset(), buffer.getContentBeginOffset() + bufferSize));
      }

      if (!discardRecordAfter)
        ((RecordInternal) record).setBuffer(buffer.getNotReusable());

      return rid;

    } catch (final IOException e) {
      throw new DatabaseOperationException("Cannot add a new record to the bucket '" + componentName + "'", e);
    }
  }

  /**
   * Emergency repair primitive: force-writes {@code record} into the EXPLICIT slot identified by {@code position},
   * bypassing the normal space allocator entirely. Used to recreate a record at the exact RID it held before an
   * out-of-band deletion (a raw delete that skipped graph cascade, or physical corruption), so existing references
   * to that RID - edge endpoints, adjacency-list entries - become valid again without rewriting every one of them.
   * <p>
   * Refuses if the slot is occupied by a live record (this can never silently overwrite data), or if its page does
   * not already exist (a RID that never held a record on this bucket has nothing to restore). A record too large
   * to fit in a single page is written as a multi-page chunk chain, same as a normal insert would - only the
   * FIRST chunk is pinned to the exact target slot (see {@link #writeMultiPageRecord}); continuation chunks need
   * no fixed position and are placed by the normal allocator, possibly on other pages. This includes position 0 of
   * the bucket: {@code getAvailableSpaceInPage}'s "never CHOOSE position 0 for a multi-page record" preference
   * (a legacy sentinel-collision guard - {@code nextChunkPointer == 0} also means "no next chunk") only applies
   * when the allocator is free to pick a different slot for a CONTINUATION chunk; it is never consulted for the
   * head/first chunk, whose position is fixed by the caller here exactly as it already is by an UPDATE that grows
   * an existing record into a chunk chain in place (same {@link #writeMultiPageRecord} call, no special-casing).
   * <p>
   * Unlike a normal allocator-driven insert, the content-insertion offset is computed directly against the target
   * page's CURRENT record count rather than via the allocator's free-page search - that search exists to let an
   * insert move to a DIFFERENT page when this one lacks room or its slot table is at capacity, which is not an
   * option here: the caller already committed to an exact slot on this exact page.
   * <p>
   * Always poisons the page for the commit-time disjoint-slot merge (unlike a normal allocator-driven insert):
   * this write's target slot was not discovered via the normal free-space search the merge's replay assumes, so any
   * real commit-time conflict on this page falls back to a full-transaction retry instead of attempting a replay.
   *
   * @throws DatabaseOperationException if the slot is occupied or its page does not exist.
   */
  public RID restoreRecordAtPosition(final long position, final Record record) {
    database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.CREATE_RECORD);

    final int pageId = (int) (position / maxRecordsInPage);
    final int positionInPage = (int) (position % maxRecordsInPage);

    final Binary buffer = database.getSerializer().serialize(database, record);
    while (buffer.size() < MINIMUM_RECORD_SIZE)
      buffer.append((byte) 0);
    final int bufferSize = buffer.size();
    final int spaceNeeded = Binary.getNumberSpace(bufferSize) + bufferSize;

    try {
      final int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        throw new DatabaseOperationException(
            "Cannot restore record at position " + position + " in bucket '" + componentName + "': its page (" + pageId
                + ") does not exist");

      final MutablePage selectedPage = database.getTransaction()
              .getPageToModify(new PageId(database, file.getFileId(), pageId), pageSize, false);

      final short currentRecordCountInPage = selectedPage.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage < currentRecordCountInPage) {
        final int existingPos = getRecordPositionInPage(selectedPage, positionInPage);
        if (existingPos != 0)
          throw new DatabaseOperationException(
              "Cannot restore record at position " + position + " in bucket '" + componentName
                  + "': the slot is occupied by a live record");
      }

      final int newRecordPositionInPage = findContentInsertionOffset(selectedPage, currentRecordCountInPage);
      final int spaceAvailableInCurrentPage = selectedPage.getMaxContentSize() - newRecordPositionInPage;

      final RID rid = new RID(file.getFileId(), position);

      LogManager.instance()
              .log(this, Level.WARNING, "Restoring record %s at its original position (records=%d threadId=%d)", rid,
                      positionInPage, Thread.currentThread().threadId());

      // Out-of-band write: always poison, never attempt a disjoint-slot-merge replay of this insert (see javadoc).
      final int previousCoverage = selectedPage.beginCoveredWrite(0);
      try {
        selectedPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, newRecordPositionInPage);

        if (positionInPage + 1 > currentRecordCountInPage)
          selectedPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) (positionInPage + 1));

        if (spaceNeeded > spaceAvailableInCurrentPage) {
          // MULTI-PAGE RECORD: only the first chunk is pinned to this slot; writeMultiPageRecord places the rest.
          writeMultiPageRecord(rid, buffer, selectedPage, newRecordPositionInPage, spaceAvailableInCurrentPage);
        } else {
          final int byteWritten = selectedPage.writeNumber(newRecordPositionInPage, bufferSize);
          selectedPage.writeByteArray(newRecordPositionInPage + byteWritten, buffer.getContent(), buffer.getContentBeginOffset(), bufferSize);
          updatePageStatistics(selectedPage.pageId.getPageNumber(), spaceAvailableInCurrentPage, -spaceNeeded);
        }
      } finally {
        selectedPage.endCoveredWrite(previousCoverage);
      }

      final TransactionContext slotTx = database.getTransactionIfExists();
      if (slotTx != null && slotTx.isSlotMergeEnabled())
        slotTx.poisonSlotRebasePage(fileId, selectedPage.getPageId().getPageNumber());

      ((RecordInternal) record).setBuffer(buffer.getNotReusable());
      ((RecordInternal) record).setIdentity(rid);

      return rid;

    } catch (final IOException e) {
      throw new DatabaseOperationException("Cannot restore record at position " + position + " in bucket '" + componentName + "'", e);
    }
  }

  /**
   * Computes the content-byte offset where a new record's bytes should start within {@code page}, given the
   * page's CURRENT record count - i.e. right after the last live record/placeholder/chunk head on the page.
   * Mirrors the core of {@link #getFreeSpaceInPage}, minus the free-slot search (the caller already has an exact
   * target slot) and minus its {@code totalRecordsInPage >= maxRecordsInPage} short-circuit: that short-circuit
   * exists only to make the ALLOCATOR give up on this page and try a different one, which is not an option for
   * {@link #restoreRecordAtPosition} - the target page is fixed.
   */
  private int findContentInsertionOffset(final BasePage page, final int totalRecordsInPage) throws IOException {
    int lastRecordPositionInPage = -1;
    for (int i = 0; i < totalRecordsInPage; i++) {
      final int recordPositionInPage = getRecordPositionInPage(page, i);
      if (recordPositionInPage > lastRecordPositionInPage)
        lastRecordPositionInPage = recordPositionInPage;
    }

    if (lastRecordPositionInPage == -1)
      // EMPTY PAGE (OR EVERY SLOT IS A HOLE): START RIGHT AFTER THE HEADER
      return contentHeaderSize;

    final long[] lastRecordSize = page.readNumberAndSize(lastRecordPositionInPage);
    if (lastRecordSize[0] == 0)
      // DELETED (V<24.1.1)
      return lastRecordPositionInPage + (int) lastRecordSize[1];
    else if (lastRecordSize[0] > 0)
      // RECORD PRESENT, CONSIDER THE RECORD SIZE + VARINT SIZE
      return lastRecordPositionInPage + (int) lastRecordSize[0] + (int) lastRecordSize[1];
    else if (lastRecordSize[0] == RECORD_PLACEHOLDER_POINTER)
      // PLACEHOLDER, CONSIDER NEXT 9 BYTES
      return lastRecordPositionInPage + LONG_SERIALIZED_SIZE + (int) lastRecordSize[1];
    else if (lastRecordSize[0] == FIRST_CHUNK || lastRecordSize[0] == NEXT_CHUNK) {
      // CHUNK
      final int chunkSize = page.readInt((int) (lastRecordPositionInPage + lastRecordSize[1]));
      return lastRecordPositionInPage + (int) lastRecordSize[1] + INT_SERIALIZED_SIZE + LONG_SERIALIZED_SIZE + chunkSize;
    } else
      // PLACEHOLDER CONTENT, CONSIDER THE RECORD SIZE (CONVERTED FROM NEGATIVE NUMBER) + VARINT SIZE
      return lastRecordPositionInPage + (int) (-1 * lastRecordSize[0]) + (int) lastRecordSize[1];
  }

  /**
   * Reserves, for the current transaction, the slot the record being created will occupy on the REUSED page the
   * allocator picked (issue #5279), and registers the reservation so it is given back when the transaction ends.
   *
   * @return the slot to use, or -1 when the page cannot host the record once the in-flight reservations of the
   * other transactions are accounted for - the caller must then allocate a new page.
   */
  private int claimInsertSlot(final BasePage foundPage, final PageAnalysis pageAnalysis, final int candidateSlot,
                              final int candidateOffset, final int spaceNeeded) {
    final int reusedPageNumber = foundPage.getPageId().getPageNumber();
    final TransactionContext tx = database.getTransaction();
    // A slot BELOW the record count is a hole left by a delete. The page image the allocator walked may be this
    // transaction's own - dirty, and therefore older than the committed page - so on a page somebody ELSE is
    // inserting into, the hole is confirmed against the committed page with a single O(1) record-table read. On an
    // uncontended page the caller's image is the truth and the extra lookup would just cost a page read per insert.
    final boolean holeCandidate = candidateSlot < pageAnalysis.totalRecordsInPage;
    final PageInsertReservation probe = insertReservations.get(reusedPageNumber);
    final boolean holeVerified = holeCandidate && probe != null && probe.soleOwner != tx//
            && isSlotFreeInCommittedPage(reusedPageNumber, candidateSlot);
    final int reservedSlot = reserveInsertSlot(tx, reusedPageNumber, holeCandidate ? candidateSlot : -1, holeVerified,
            pageAnalysis.totalRecordsInPage, candidateOffset, spaceNeeded, foundPage.getMaxContentSize());
    if (reservedSlot >= 0)
      tx.trackInsertSlotReservation(this, reusedPageNumber, reservedSlot);
    return reservedSlot;
  }

  /**
   * Claims, for the calling transaction, the slot a brand-new record will occupy on an EXISTING page of this bucket
   * (issue #5279). Concurrent transactions inserting into the same page used to be handed the very same free slot -
   * and therefore the very same optimistic RID - because each of them picked it from its own snapshot of the page,
   * with nothing to make the choices disjoint. Every commit but the first then failed with a page-level
   * {@link ConcurrentModificationException} that no application-level retry could avoid, although two inserts of
   * DIFFERENT records always commute.
   * <p>
   * All the claims on one page are serialised on its reservation entry. The claim hands out:
   * <ul>
   * <li>a slot: {@code holeSlot} when the caller offers a hole (a slot freed by a delete) nobody else reserved -
   * on a shared page only if the caller could also verify it is still a hole on the COMMITTED page, since the
   * caller's own image of a page it already modified can be older than that; otherwise the first slot at or after
   * BOTH the caller's append position and the page high-water mark;</li>
   * <li>internally, a content offset - never returned, only used to decide whether the record still FITS once the
   * space promised to the other in-flight transactions is accounted for. The record itself is written at the
   * caller's own offset (its page image is private until commit) and, on a commit-time page conflict, the
   * disjoint-slot merge recomputes the offset on the newer committed page.</li>
   * </ul>
   * Both are derived with a {@code max()} against the caller's own snapshot, so a transaction inserting several
   * records into the same page is never charged twice for the records it already wrote there - and while the page
   * is uncontended the snapshot is used alone, since then it is the whole truth about the page.
   *
   * @return the reserved slot, or -1 when the page can no longer host the record and the caller must allocate a new
   * page.
   */
  private int reserveInsertSlot(final Object owner, final int pageNumber, final int holeSlot, final boolean holeVerified,
                                final int appendBase, final int candidateOffset, final int spaceNeeded,
                                final int maxContentSize) {
    for (;;) {
      PageInsertReservation res = insertReservations.get(pageNumber);
      if (res == null)
        res = insertReservations.computeIfAbsent(pageNumber, NEW_INSERT_RESERVATION);

      synchronized (res) {
        // The entry was dropped (its last reservation was released) between the lookup and the lock: it is no
        // longer the page's entry, so claiming on it would not be visible to anybody. Take a fresh one.
        if (res.removed)
          continue;
        if (res.outstanding == 0)
          res.soleOwner = owner;
        else if (res.soleOwner != owner)
          res.soleOwner = null;

        final boolean uncontended = res.soleOwner != null;

        final int slot;
        if (uncontended)
          slot = holeSlot >= 0 ? holeSlot : appendBase;
        else if (holeSlot >= 0 && holeVerified && !res.reservedSlots.get(holeSlot))
          slot = holeSlot;
        else {
          int candidate = Math.max(appendBase, res.nextFreeSlot);
          while (candidate < maxRecordsInPage && res.reservedSlots.get(candidate))
            ++candidate;
          slot = candidate;
        }

        final int offset = uncontended ? candidateOffset : Math.max(candidateOffset, res.nextFreeOffset);
        // Physical fit only, exactly like the page selection this claim refines: SPARE_SPACE_FOR_GROWTH is a
        // preference there, not a requirement (findAvailableSpace deliberately retries with half the size to
        // squeeze a record into the tail of an almost full page), so turning it into a hard limit here would
        // send every record landing in that tail to a brand-new page.
        if (slot >= maxRecordsInPage || offset + spaceNeeded > maxContentSize) {
          if (res.outstanding == 0) {
            res.removed = true;
            insertReservations.remove(pageNumber, res);
          }
          return -1;
        }

        res.reservedSlots.set(slot);
        if (slot >= res.nextFreeSlot)
          res.nextFreeSlot = slot + 1;
        if (uncontended || offset + spaceNeeded > res.nextFreeOffset)
          res.nextFreeOffset = offset + spaceNeeded;
        ++res.outstanding;
        return slot;
      }
    }
  }

  /**
   * Tells whether {@code slot} is still a hole on the CURRENT COMMITTED version of the page, bypassing the calling
   * transaction's own (possibly older) image of it. One record-table read, no scan: it is what makes hole reuse safe
   * for a transaction that already modified the page and would otherwise keep offering a slot a concurrent
   * transaction has committed into meanwhile (#5279). A page with no committed version yet (created by this
   * transaction) has no hole to confirm.
   */
  private boolean isSlotFreeInCommittedPage(final int pageNumber, final int slot) {
    try {
      final BasePage committed = database.getPageManager()
              .getImmutablePage(new PageId(database, file.getFileId(), pageNumber), pageSize, false, false);
      if (committed == null)
        return false;
      return slot < committed.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET) && getRecordPositionInPage(committed, slot) == 0;
    } catch (final Exception e) {
      // Not being able to confirm the hole (page not on disk yet, unreadable, ...) is not an error: fall back to
      // appending, which is always safe.
      return false;
    }
  }

  /**
   * Tells whether a record is present at {@code rid} in the CURRENT COMMITTED state of its page, ignoring the
   * calling transaction's own (possibly older) image of that page. Only the record table is consulted - no content
   * is loaded - so it answers "is this slot taken right now?" at the cost of one page lookup.
   * <p>
   * Used by the unique-index check to tell a genuinely dangling entry (its record was deleted or its bucket is
   * gone) from one this transaction simply cannot see yet because a concurrent transaction inserted the record into
   * a page THIS transaction has also modified (#5279).
   */
  public boolean existsRecordInCommittedPage(final RID rid) {
    final long position = rid.getPosition();
    if (position < 0)
      return false;

    final int pageNumber = (int) (position / maxRecordsInPage);
    final int positionInPage = (int) (position % maxRecordsInPage);
    try {
      final BasePage committed = database.getPageManager()
              .getImmutablePage(new PageId(database, file.getFileId(), pageNumber), pageSize, false, false);
      if (committed == null || positionInPage >= committed.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET))
        return false;

      final int recordPositionInPage = getRecordPositionInPage(committed, positionInPage);
      if (recordPositionInPage == 0)
        // FREE SLOT (NEVER USED OR DELETED)
        return false;

      // A ZERO SIZE MARKS A RECORD DELETED BEFORE v24.1.1; ANY OTHER VALUE (INCLUDING PLACEHOLDERS AND CHUNKS) IS A RECORD
      return committed.readNumberAndSize(recordPositionInPage)[0] != 0L;

    } catch (final Exception e) {
      // The page is not there (or not readable): the record is certainly not committed at that position.
      return false;
    }
  }

  /**
   * Releases one slot claimed by {@link #reserveInsertSlot}. Called for every reservation when the owning
   * transaction ends, whatever the outcome (commit, rollback or kill), from {@code TransactionContext.reset()}.
   */
  public void releaseInsertSlot(final int pageNumber, final int slot) {
    final PageInsertReservation res = insertReservations.get(pageNumber);
    if (res == null)
      return;
    synchronized (res) {
      res.reservedSlots.clear(slot);
      if (--res.outstanding <= 0) {
        res.removed = true;
        insertReservations.remove(pageNumber, res);
      }
    }
  }

  /**
   * A record UPDATE is a disjoint-slot-merge candidate unless it is an edge-list segment (record type 3): appending
   * to an existing chunk is owned by the commutative edge-append merge ({@link TransactionContext#trackEdgeAppend}),
   * which also handles the case the slot merge cannot - two transactions appending to the SAME chunk - so the two
   * mechanisms are kept from rebasing the same page. Creating a chunk is a different matter: see the caller in
   * updateRecordInternal and {@link TransactionContext#isSlotTrackedAsInsert} (#5279).
   */
  private static boolean isSlotMergeCandidate(final Record record) {
    return record.getRecordType() != EdgeSegment.RECORD_TYPE;
  }

  /**
   * Commit-time primitive for the disjoint-slot page merge (#5381). Re-applies ONE record write this transaction
   * made to a bucket page, on top of {@code page} freshly reloaded at its current committed version, keeping the
   * record's RID (page+slot) fixed. Called only on the leader/embedded commit while the bucket file's commit
   * lock is held, and only for a page whose every modification this transaction made was a tracked disjoint-slot
   * insert or an update that stayed inside the page (see {@code TransactionContext.rebaseSlots}).
   *
   * @param page           the reloaded committed page to re-apply the write onto.
   * @param positionInPage the record slot (RID position modulo maxRecordsInPage).
   * @param body           this transaction's final serialized record body (no size prefix).
   * @param baseBody       for an UPDATE, the record body this transaction started from - used to detect a
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

      // UPDATE: the slot must still hold the record this transaction started from, byte-for-byte, so we never
      // overwrite (lose) a concurrent update of the SAME record.
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
        // GROWTH (#5279): re-do the in-page growth on the committed page. The record keeps its slot (hence its RID)
        // and only its own content changes: the records that follow simply move, and their offsets are recomputed
        // from THIS page, so the result is the same page a serial execution would have produced. Refuses (false) when
        // the page cannot host the extra bytes anymore, which sends the transaction to a normal retry.
        return growRecordInPage(page, pageNumber, recordCountInPage, existingPos,
                getLastRecordPositionInPage(page, recordCountInPage), rs, false, body, 0, body.length);

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

  /**
   * Commit-time primitive for the disjoint-slot page merge (#5569). Re-applies the DELETE of one plain in-place
   * record on top of {@code page} freshly reloaded at its current committed version. Called only on the
   * leader/embedded commit while the bucket file's commit lock is held, and only for a page whose every modification
   * this transaction made was a tracked single-slot write (see {@code TransactionContext.rebaseSlots}).
   *
   * @param page           the reloaded committed page to re-apply the delete onto.
   * @param positionInPage the record slot (RID position modulo maxRecordsInPage).
   * @param baseBody       the record body this transaction started from - used to detect a concurrent modification
   *                       of the SAME record (a TRUE conflict).
   *
   * @return true when the delete was safely re-applied; false when the committed slot no longer holds the very same
   * record - the caller then falls back to a full-transaction retry.
   */
  public boolean rebaseRecordDeleteOnPage(final MutablePage page, final int positionInPage, final byte[] baseBody) {
    try {
      final int pageNumber = page.getPageId().getPageNumber();
      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      final int existingPos = positionInPage < recordCountInPage ? getRecordPositionInPage(page, positionInPage) : 0;

      // The slot must still hold the record this transaction started from, byte-for-byte: a concurrent commit that
      // deleted or rewrote THIS record is a real conflict the application has to see.
      if (existingPos == 0 || baseBody == null)
        return false;
      final long[] rs = page.readNumberAndSize(existingPos);
      if (rs[0] <= 0)
        // Deleted, placeholder, or multi-page marker: not the plain in-place record we captured anymore.
        return false;
      final int committedSize = (int) rs[0];
      if (committedSize != baseBody.length)
        return false;
      final byte[] committed = new byte[committedSize];
      page.readByteArray((int) (existingPos + rs[1]), committed, 0, committedSize);
      if (!Arrays.equals(committed, baseBody))
        return false;

      if (database.getConfiguration().getValueAsBoolean(GlobalConfiguration.BUCKET_WIPEOUT_ONDELETE))
        // WIPE OUT RECORD CONTENT, EXACTLY AS deleteRecordInternal DOES
        page.writeZeros(existingPos + 1, (int) (committedSize + rs[1] - 1));

      // POINTER = 0 MEANS DELETED. The in-page record count is deliberately left alone, as on the normal delete
      // path: it is the size of the slot table, not the number of live records, and compressPage (which the commit
      // runs on the rebased page right after) shrinks it when the freed slots are the trailing ones.
      page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0);

      if (reuseSpaceMode.ordinal() >= REUSE_SPACE_MODE.HIGH.ordinal()) {
        // UPDATE THE STATISTICS
        final PageAnalysis pageAnalysis = new PageAnalysis(page);
        pageAnalysis.totalRecordsInPage = recordCountInPage;
        getFreeSpaceInPage(pageAnalysis);
        updatePageStatistics(pageNumber, pageAnalysis.spaceAvailableInCurrentPage, (int) ((committedSize + rs[1]) * -1));
      } else
        changesFromLastStats.incrementAndGet();

      return true;

    } catch (final IOException e) {
      // Same asymmetry as rebaseRecordOnPage: a "cannot rebase this slot" outcome returns false, a genuine I/O
      // failure aborts the transaction instead of masquerading as a version conflict.
      throw new DatabaseOperationException("Error on slot delete rebase for page " + page.getPageId(), e);
    }
  }

  private boolean writeRecordAtSlot(final MutablePage page, final int pageNumber, final int positionInPage,
                                    final short recordCountInPage, final byte[] body) throws IOException {
    final int spaceNeeded = Binary.getNumberSpace(body.length) + body.length;

    // Find where free content begins on the CURRENT committed page (reuses the tested free-space walker). Only the
    // HARD verdicts are honoured here: an unusable slot table or a page with no room left leaves
    // newRecordPositionInPage negative, while a plain createNewPage also fires on the SOFT
    // SPARE_SPACE_FOR_GROWTH preference - which would refuse to replay a record the allocator had deliberately
    // squeezed into the tail of an almost full page (#5279).
    final PageAnalysis analysis = getAvailableSpaceInPage(pageNumber, spaceNeeded, false);
    if (analysis.newRecordPositionInPage < 0//
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
      // Enforce the per-type CREATE_RECORD ACL once for the whole bulk (a no-op when no principal is bound,
      // e.g. internal import or HA apply). The single-record createRecord() path checks this per record; the
      // bulk edge path (GraphBatch flush) previously skipped it, so an authenticated user with no createRecord
      // grant on this type could still write edges here once the request thread bound the principal
      // (defense-in-depth for GHSA-c23x-pqcj-7hfm).
      database.checkPermissionsOnFile(fileId, SecurityDatabaseUser.ACCESS.CREATE_RECORD);

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

      // DISJOINT-SLOT MERGE (#5381, #5279): only an update that stays INSIDE this page - an overwrite of the same
      // size or smaller, or a growth the page can host by shifting the records that follow (the two branches far
      // below) - touches just this record's slot and thus commutes with concurrent writes to other slots on the
      // page. Every other update shape here (placeholder pointer or content, multi-page chunk, a record that has to
      // spill OUT of the page) changes more than this slot, so it poisons the page: the merge must never rebase it.
      final TransactionContext slotTx = database.getTransactionIfExists();
      final boolean slotMergeOn = slotTx != null && slotTx.isSlotMergeEnabled();
      // #5279: an edge-list segment is normally left to the edge-append merge, but a segment this transaction
      // CREATED (its slot is already tracked as an insert) has no committed image to append onto - the edge merge
      // cannot replay it and deliberately poisoned its page. The slot merge owns it instead: the whole chunk
      // content is ours, so re-applying the final image at the same free slot reproduces it exactly.
      final boolean slotInsertedHere = slotMergeOn && slotTx.isSlotTrackedAsInsert(fileId, pageId, positionInPage);
      final boolean slotCandidate = slotMergeOn && (isSlotMergeCandidate(record) || slotInsertedHere);
      // A NON-candidate update (an edge-list segment, owned by the edge-append merge) still modifies this page:
      // it must POISON the slot map, not stay invisible to it. Since super-node striping (#5156) a segments page
      // can also host a StripeDirectory - a slot-merge candidate - and a page carrying a tracked directory write
      // plus an untracked segment change would otherwise be slot-rebased from the directory write alone, silently
      // dropping this transaction's in-chunk appends. Mirrors the !isSlotMergeCandidate poison in createRecord.
      if (slotMergeOn && !slotCandidate)
        slotTx.poisonSlotRebasePage(fileId, pageId);

      // #5596: an in-chunk edge append is invisible to the slot map on purpose - the commutative edge-append merge
      // owns it - so the in-place rewrite of the segment below is declared covered by THAT merge instead. Only when
      // the append was really registered (TransactionContext.trackEdgeAppend) and the page is not already
      // excluded: a segment write nobody tracked (an edge removal, addAll, a bulk load) stays undeclared and can
      // therefore never be rebased away, whether or not its writer remembered to poison the page.
      final boolean edgeAppendReplayable = !slotCandidate && slotTx != null && slotTx.isEdgeAppendMergeEnabled()//
              && slotTx.isEdgeAppendTracked(rid) && !slotTx.isEdgeAppendPagePoisoned(fileId, pageId);

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
        deleteRecordInternal(placeHolderContentRID, true, false, false);

        // The slot is being turned from a placeholder POINTER back into a record, and the content record it pointed
        // to (on another page) has just been deleted: this is not a single-slot change, and the "pre-image" the
        // branches below would capture is the 8-byte pointer, not record content. Poison the page so no merge can
        // ever rebase this slot from it.
        if (slotCandidate)
          slotTx.poisonSlotRebasePage(fileId, pageId);

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
        // UPDATED RECORD IS LARGER THAN THE PREVIOUS VERSION: MAKE ROOM IN THE PAGE IF POSSIBLE
        final int lastRecordPositionInPage = getLastRecordPositionInPage(page, recordCountInPage);

        // DISJOINT-SLOT MERGE (#5279): growing a record INSIDE its own page changes only this record's content - the
        // records that follow merely move, and their new offsets are recomputed from the page itself - so the write
        // is replayable on a newer committed version of the page exactly like a same-or-smaller overwrite. Growth is
        // the normal update shape (a longer string, one more property), so leaving it out made concurrent updates of
        // unrelated records on one page conflict for good. The pre-image must be copied BEFORE the shift overwrites
        // it, but the tracking itself happens only if the growth really fits: spilling the record out of the page
        // (placeholder pointer or multi-page chunks) changes more than this slot and poisons the page. A PLACEHOLDER
        // CONTENT record is excluded as well: it lives behind a pointer on another page, so rebasing this page alone
        // would be unsound.
        final boolean growRebasable = slotCandidate && !isPlaceHolder && !slotTx.isSlotRebasePagePoisoned(fileId, pageId);
        final byte[] growBaseBody;
        if (growRebasable && !slotInsertedHere) {
          growBaseBody = new byte[(int) recordSize[0]];
          page.readByteArray((int) (recordPositionInPage + recordSize[1]), growBaseBody, 0, growBaseBody.length);
        } else
          growBaseBody = null;

        // #5596: the in-page growth (the shift of the following records plus their recomputed offsets) is exactly
        // what rebaseRecordOnPage re-does on the newer committed page, so those bytes are covered by the slot merge.
        // A growth that does NOT fit writes nothing here and falls through to the spill branch below, undeclared.
        final int growCoverage = page.beginCoveredWrite(growRebasable ?
                MutablePage.COVERAGE_SLOT_MERGE :
                (edgeAppendReplayable ? MutablePage.COVERAGE_EDGE_APPEND_MERGE : 0));
        final boolean grown;
        try {
          grown = growRecordInPage(page, pageId, recordCountInPage, recordPositionInPage, lastRecordPositionInPage, recordSize,
                  isPlaceHolder, buffer.getContent(), buffer.getContentBeginOffset(), bufferSize);
        } finally {
          page.endCoveredWrite(growCoverage);
        }

        if (grown) {
          if (slotCandidate) {
            if (growRebasable) {
              final byte[] finalBody = Arrays.copyOfRange(buffer.getContent(), buffer.getContentBeginOffset(),
                      buffer.getContentBeginOffset() + bufferSize);
              if (growBaseBody != null)
                slotTx.trackRebasableUpdate(fileId, pageId, positionInPage, growBaseBody, finalBody);
              else
                // The slot holds a record CREATED by this transaction: there is no committed pre-image to diff
                // against, so it stays an insert whose final image the merge re-writes at the same free slot.
                slotTx.trackRebasableInsert(fileId, pageId, positionInPage, finalBody);
            } else
              slotTx.poisonSlotRebasePage(fileId, pageId);
          }

          LogManager.instance()
                  .log(this, Level.FINE, "Updated record %s by allocating new space on the same page (%s threadId=%d)", null, rid, page,
                          Thread.currentThread().threadId());

        } else {
          // THE RECORD MUST SPILL OUT OF THE PAGE: a placeholder pointer or a chunk chain touches other pages too,
          // so this page can no longer be rebased from this transaction's slot writes alone.
          if (slotCandidate)
            slotTx.poisonSlotRebasePage(fileId, pageId);

          if (isPlaceHolder)
            // CANNOT CREATE A PLACEHOLDER OF PLACEHOLDER
            return false;

          final int pageOccupiedInBytes = getPageOccupiedInBytes(page, lastRecordPositionInPage, recordPositionInPage,
                  recordSize);
          int availableSpaceInCurrentPage = (int) (recordSize[0] + recordSize[1]);
          if (lastRecordPositionInPage == recordPositionInPage)
            // SINCE IT'S THE LAST RECORD IN THE PAGE, GET ALSO THE REST OF THE SPACE AVAILABLE IN THE PAGE
            availableSpaceInCurrentPage += page.getMaxContentSize() - pageOccupiedInBytes;

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
        final boolean slotTracked = slotCandidate && !isPlaceHolder && !slotTx.isSlotRebasePagePoisoned(fileId, pageId);
        if (slotCandidate) {
          if (isPlaceHolder)
            slotTx.poisonSlotRebasePage(fileId, pageId);
          // Skip the pre-image + final-image copies on a page that is already poisoned (they would be discarded).
          else if (slotTracked) {
            final byte[] finalBody = Arrays.copyOfRange(buffer.getContent(), buffer.getContentBeginOffset(),
                    buffer.getContentBeginOffset() + bufferSize);
            if (slotInsertedHere)
              // The slot holds a record CREATED by this transaction: it stays an insert (there is no committed
              // pre-image to diff against), so only refresh its final image - and skip copying one altogether.
              slotTx.trackRebasableInsert(fileId, pageId, positionInPage, finalBody);
            else {
              final byte[] baseBody = new byte[(int) recordSize[0]];
              page.readByteArray((int) (recordPositionInPage + recordSize[1]), baseBody, 0, baseBody.length);
              slotTx.trackRebasableUpdate(fileId, pageId, positionInPage, baseBody, finalBody);
            }
          }
        }

        // #5596: the overwrite of ONE record's size marker and content is what the slot merge replays (or, for a
        // tracked in-chunk edge append, what the edge-append merge re-derives). Declare exactly those bytes.
        final int previousCoverage = page.beginCoveredWrite(slotTracked ?
                MutablePage.COVERAGE_SLOT_MERGE :
                (edgeAppendReplayable ? MutablePage.COVERAGE_EDGE_APPEND_MERGE : 0));
        try {
          recordSize[1] = page.writeNumber(recordPositionInPage, isPlaceHolder ? -1L * bufferSize : bufferSize);
          final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);
          page.writeByteArray(recordContentPositionInPage, buffer.getContent(), buffer.getContentBeginOffset(), bufferSize);
        } finally {
          page.endCoveredWrite(previousCoverage);
        }

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

  /**
   * Structurally walks the chunk chain of a multi-page record WITHOUT MVCC version validation to detect a broken
   * chain: a continuation pointer to an out-of-range page, a slot that was cleaned, or a marker that is no longer
   * {@link #NEXT_CHUNK}. Used by {@link #check(int, boolean)}, which otherwise validates only the first chunk's
   * on-page size and would never notice a broken chain. Returns {@code null} when the chain is intact, or a short
   * human-readable reason when it is broken. Never throws.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  private String findBrokenChunkChain(final RID rid, final BasePage firstPage, final int firstRecordPositionInPage) {
    try {
      BasePage chunkPage = firstPage;
      int chunkPositionInPage = firstRecordPositionInPage;
      long[] chunkHeader = firstPage.readNumberAndSize(firstRecordPositionInPage); // [FIRST_CHUNK, headerBytes]
      final int totalPages = getTotalPages();

      // Exact loop detection: a chain can legitimately hold more chunks than the bucket has pages (chunk slots can
      // share pages after chain reuse/fragmentation), so any count-based heuristic risks a false positive - fatal
      // here, because check(fix) DELETES the record it flags. Revisiting a continuation pointer is the only certain
      // loop signal.
      final Set<Long> visitedPointers = new HashSet<>();

      for (int chunkId = 0; ; ++chunkId) {
        final long nextChunkPointer = chunkPage.readLong(
                (int) (chunkPositionInPage + chunkHeader[1] + INT_SERIALIZED_SIZE));
        if (nextChunkPointer == 0)
          // REACHED THE LAST CHUNK CLEANLY
          return null;

        if (!visitedPointers.add(nextChunkPointer))
          return "chain loop detected at chunk " + chunkId;

        final int nextPageId = (int) (nextChunkPointer / maxRecordsInPage);
        final int nextPositionInPage = (int) (nextChunkPointer % maxRecordsInPage);
        if (nextPageId >= totalPages)
          return "next chunk pointer out of range at chunk " + chunkId;

        chunkPage = database.getTransaction().getPage(new PageId(database, file.getFileId(), nextPageId), pageSize);
        chunkPositionInPage = getRecordPositionInPage(chunkPage, nextPositionInPage);
        if (chunkPositionInPage == 0)
          return "chunk slot cleaned at chunk " + chunkId;

        chunkHeader = chunkPage.readNumberAndSize(chunkPositionInPage);
        if (chunkHeader[0] != NEXT_CHUNK)
          return "unexpected marker at chunk " + chunkId;
      }
    } catch (final Exception e) {
      return "error walking chain: " + e.getMessage();
    }
  }

  /**
   * Structural probe for the tolerant delete path: tells whether the record at {@code rid} is a multi-page record
   * whose chunk chain is structurally broken. Unlike {@link #loadMultiPageRecord} this walk ignores page versions,
   * so a transient concurrent modification never reports {@code true} - only a genuinely broken chain does. Any
   * failure to probe (including a record that is not multi-page, or is already gone) conservatively returns
   * {@code false}, keeping the caller on the strict retry behaviour.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  public boolean isChunkChainBroken(final RID rid) {
    try {
      final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
      final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);
      if (pageId >= getTotalPages())
        return false;

      final BasePage page = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageId), pageSize);
      final int recordPositionInPage = getRecordPositionInPage(page, positionInPage);
      if (recordPositionInPage == 0)
        // DELETED
        return false;

      final long[] recordSize = page.readNumberAndSize(recordPositionInPage);
      if (recordSize[0] != FIRST_CHUNK)
        // NOT A MULTI-PAGE RECORD: A CME ON IT CANNOT COME FROM A BROKEN CHAIN
        return false;

      return findBrokenChunkChain(rid, page, recordPositionInPage) != null;
    } catch (final Exception e) {
      return false;
    }
  }

  private void deleteRecordInternal(final RID rid, final boolean deletePlaceholderContent, final boolean deleteChunks,
      final boolean force) {
    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    // DISJOINT-SLOT MERGE (#5381, #5569): whether this delete is a pure single-slot change depends on the record's
    // marker, which is only known further down once the page has been read - so the poison lives in each non-plain
    // branch below (placeholder pointer or content, chunk chain, corrupted slot, and the error fallback) instead of
    // here. A plain in-place record is deleted by zeroing its slot-table entry (plus the optional content wipe-out),
    // which commutes with writes to every other slot exactly like an insert or an in-page update.
    final TransactionContext slotTx = database.getTransactionIfExists();
    final boolean slotMergeOn = slotTx != null && slotTx.isSlotMergeEnabled();

    // EDGE-APPEND MERGE: that merge re-derives the page from its committed image plus the tracked appends alone, so
    // a delete left on a page which also received a tracked append would be dropped at rebase time. Freeing a slot
    // is not an append: exclude the page from it, whatever the record shape turns out to be.
    if (slotTx != null)
      slotTx.poisonEdgeAppendPage(fileId, pageId);

    // #5596: the merges this delete's page writes may be replayed by. Raised to COVERAGE_SLOT_MERGE only once the
    // record turns out to be a plain in-place one whose delete really was tracked; every other shape leaves it 0, so
    // the writes stay undeclared and no merge can re-derive the page from them.
    int deleteCoverage = 0;

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
          // The slot holds an 8-byte pointer, and the content record it points to lives on ANOTHER page: not a
          // single-slot change, so the page can never be slot-rebased from this transaction's writes alone.
          if (slotMergeOn)
            slotTx.poisonSlotRebasePage(fileId, pageId);

          // FOUND PLACEHOLDER POINTER: DELETE THE PLACEHOLDER CONTENT FIRST
          final RID placeHolderContentRID = new RID(fileId, page.readLong((int) (recordPositionInPage + recordSize[1])));
          try {
            deleteRecordInternal(placeHolderContentRID, true, false, force);
          } catch (RecordNotFoundException e) {
            // PARTIAL RECORD NOT FOUND
          }

        } else if (recordSize[0] == FIRST_CHUNK) {
          // The record continues on other pages through a chunk chain: freeing it touches more than this slot.
          if (slotMergeOn)
            slotTx.poisonSlotRebasePage(fileId, pageId);

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

            // Resolve the next chunk. A broken link (out-of-range pointer, a slot that was cleaned, or a marker that
            // is no longer NEXT_CHUNK) means the chain cannot be walked. Without force this is treated as a concurrent
            // modification and rethrown as the #4932 retry signal, so a half-freed chain is never left behind. With
            // force (admin repair) the walk stops here instead: the head slot is still freed below, and the chunks
            // past this break are orphaned - a bounded space leak reclaimed by compaction or a later database check.
            String chainProblem = null;
            try {
              if (chunkPageId >= getTotalPages())
                chainProblem = "next chunk pointer out of range";
              else {
                chunkPage = database.getTransaction()
                        .getPageToModify(new PageId(database, file.getFileId(), chunkPageId), pageSize, false);
                chunkRecordPositionInPage = getRecordPositionInPage(chunkPage, chunkPositionInPage);
                if (chunkRecordPositionInPage == 0)
                  chainProblem = "chunk slot was cleaned";
                else {
                  recordSize = chunkPage.readNumberAndSize(chunkRecordPositionInPage);
                  if (recordSize[0] != NEXT_CHUNK)
                    chainProblem = "chunk marker is not NEXT_CHUNK";
                }
              }
            } catch (final IOException e) {
              if (!force)
                throw e;
              chainProblem = "error reading chunk: " + e.getMessage();
            }

            if (chainProblem != null) {
              if (!force)
                // Chunk was modified/removed by a concurrent operation — signal retry (#4932)
                throw new ConcurrentModificationException(
                        "Multi-page record " + rid + " chunk " + chunkId + " was modified concurrently. Please retry");

              LogManager.instance().log(this, Level.WARNING,
                      "Force-deleting multi-page record %s with a broken chunk chain at chunk %d (%s); orphaned chunks (if "
                              + "any) will be reclaimed by compaction or a database check.", rid, chunkId, chainProblem);
              break;
            }

            try {
              deleteRecordInternal(new RID(fileId, nextChunkPointer), false, true, force);
            } catch (RecordNotFoundException e) {
              // PARTIAL RECORD NOT FOUND
            }
          }

        } else if (recordSize[0] == NEXT_CHUNK) {
          // A continuation chunk of a record whose head lives elsewhere: part of a multi-page structure.
          if (slotMergeOn)
            slotTx.poisonSlotRebasePage(fileId, pageId);

          if (!deleteChunks)
            // CANNOT DELETE A CHUNK DIRECTLY
            throw new RecordNotFoundException("Record " + rid + " not found", rid);

        } else if (recordSize[0] < RECORD_PLACEHOLDER_CONTENT) {
          // Placeholder CONTENT: it is reachable only through a pointer on another page, so rebasing this page in
          // isolation would leave that pointer dangling.
          if (slotMergeOn)
            slotTx.poisonSlotRebasePage(fileId, pageId);

          if (!deletePlaceholderContent)
            // CANNOT DELETE A PLACEHOLDER DIRECTLY
            throw new RecordNotFoundException("Record " + rid + " not found", rid);
        } else if (recordSize[0] > 0) {
          // PLAIN IN-PLACE RECORD: the delete is a single-slot change (zero the slot-table entry, optionally wipe the
          // content), so it commutes with concurrent writes to the other slots of the page and can be replayed on the
          // newer committed version at commit time (#5569). The pre-image must be captured BEFORE the wipe-out below,
          // and lets the replay tell a false page conflict from a concurrent write to THIS record.
          if (slotMergeOn) {
            if (page.readByte((int) (recordPositionInPage + recordSize[1])) == EdgeSegment.RECORD_TYPE)
              // An edge-list segment is owned by the commutative edge-append merge, not by this one: keep the two
              // mechanisms from ever rebasing the same page (mirrors isSlotMergeCandidate on the update path).
              slotTx.poisonSlotRebasePage(fileId, pageId);
            else if (!slotTx.isSlotRebasePagePoisoned(fileId, pageId)) {
              final byte[] baseBody = new byte[(int) recordSize[0]];
              page.readByteArray((int) (recordPositionInPage + recordSize[1]), baseBody, 0, baseBody.length);
              slotTx.trackRebasableDelete(fileId, pageId, positionInPage, baseBody);
              // #5596: from here on the two writes this delete makes - the content wipe-out and the slot-pointer
              // zeroing below - are exactly what rebaseRecordDeleteOnPage re-applies, so declare them covered.
              deleteCoverage = MutablePage.COVERAGE_SLOT_MERGE;
            }
          }

          if (database.getConfiguration().getValueAsBoolean(GlobalConfiguration.BUCKET_WIPEOUT_ONDELETE)) {
            // WIPE OUT RECORD CONTENT
            final int previousCoverage = page.beginCoveredWrite(deleteCoverage);
            try {
              page.writeZeros(recordPositionInPage + 1, (int) (recordSize[0] + recordSize[1] - 1));
            } catch (Exception e) {
              // IGNORE IT
              LogManager.instance().log(this, Level.SEVERE, "Error on wiping out page content", e);
            } finally {
              page.endCoveredWrite(previousCoverage);
            }
          }
        } else if (slotMergeOn)
          // AN UNKNOWN NEGATIVE MARKER: NOT A PLAIN SINGLE-SLOT RECORD, KEEP THE PAGE OUT OF THE MERGE
          slotTx.poisonSlotRebasePage(fileId, pageId);

        // POINTER = 0 MEANS DELETED. deleteCoverage is COVERAGE_SLOT_MERGE only for the tracked plain-record delete
        // above; every other shape leaves it 0, so the page cannot be slot-rebased (#5596).
        final int previousCoverage = page.beginCoveredWrite(deleteCoverage);
        try {
          page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0);
        } finally {
          page.endCoveredWrite(previousCoverage);
        }

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
        // CORRUPTED RECORD: WRITE ZERO AS POINTER TO RECORD. There is no readable pre-image to check the replay
        // against, so the page cannot take part in the slot merge.
        if (slotMergeOn)
          slotTx.poisonSlotRebasePage(fileId, pageId);

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

      if (page != null) {
        // The delete failed half-way, so whatever was tracked for this page no longer describes it: never let the
        // slot merge re-derive the page from it.
        if (slotMergeOn)
          slotTx.poisonSlotRebasePage(fileId, pageId);

        // POINTER = 0 MEANS DELETED
        page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE, 0);
      }
    }
  }

  /**
   * Defragments the page in place, closing the holes an update or a delete left behind.
   * <p>
   * #5596: with {@code forceWipeOut=false} every byte written here is declared covered by ALL the commit-time page
   * merges. That is sound - and necessary to keep them effective - because the merge re-runs exactly this call on the
   * page it re-derived, so the compression is reproduced rather than lost: it is a layout-only transformation of
   * whatever records the page ends up holding, not a change of this transaction's own. A FORCED wipe-out (the
   * database checker) is not re-run by the merge path, so it declares nothing and disqualifies the page.
   * <p>
   * #5608: that re-run is the load-bearing half of the declaration, so it lives INSIDE the two methods that produce a
   * rebased page ({@code TransactionContext.rebaseEdgeAppends} and {@code rebaseSlots}) rather than in the commit loop
   * that calls them - where reordering or dropping it would have invalidated the declaration with no test failing.
   * {@code Issue5608RebasedPageCompressionTest} pins the invariant from the outside.
   */
  public void compressPage(final MutablePage page, final boolean forceWipeOut) throws IOException {
    final int previousCoverage = page.beginCoveredWrite(forceWipeOut ? 0 : MutablePage.COVERAGE_ALL_MERGES);
    try {
      compressPageInternal(page, forceWipeOut);
    } finally {
      page.endCoveredWrite(previousCoverage);
    }
  }

  private void compressPageInternal(final MutablePage page, final boolean forceWipeOut) throws IOException {
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
    // DISJOINT-SLOT MERGE (#5381): a multi-page write places chunk records onto pages via inline record-table
    // writes that bypass create/update/deleteRecordInternal, so it is NOT tracked. A chunk landing on a REUSED
    // page that also holds a tracked single-slot write would let the rebase re-derive that page from the tracked
    // slot alone and silently drop the chunk (corrupting the record). Poison every page this write touches so none
    // of them can be rebased. Poisoning a brand-new page is harmless (new pages are never rebase-tracked).
    final TransactionContext slotTx = database.getTransactionIfExists();
    final boolean poisonSlots = slotTx != null && slotTx.isSlotMergeEnabled();
    if (poisonSlots)
      slotTx.poisonSlotRebasePage(fileId, currentPage.pageId.getPageNumber());

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

      if (poisonSlots)
        slotTx.poisonSlotRebasePage(fileId, nextPage.pageId.getPageNumber());

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
    // DISJOINT-SLOT MERGE (#5381): like writeMultiPageRecord, this rewrites/relocates/frees chunk records on
    // existing pages through inline record-table writes that bypass the tracking hooks. Poison every page it
    // touches so a page carrying both a tracked single-slot write and a chunk write can never be rebased.
    final TransactionContext slotTx = database.getTransactionIfExists();
    final boolean poisonSlots = slotTx != null && slotTx.isSlotMergeEnabled();
    if (poisonSlots)
      slotTx.poisonSlotRebasePage(fileId, currentPage.pageId.getPageNumber());

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

      if (poisonSlots)
        slotTx.poisonSlotRebasePage(fileId, nextPage.pageId.getPageNumber());

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

      if (poisonSlots)
        slotTx.poisonSlotRebasePage(fileId, nextPage.pageId.getPageNumber());

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

  /**
   * Byte the used content of a bucket page ends at: the position right after the last record it stores, and therefore
   * the start of its free tail. Resolves the real size of that last record for every marker shape it can have (plain
   * record, placeholder pointer, placeholder content, multi-page chunk).
   *
   * @param lastRecordPositionInPage highest content offset in use in the page (see {@link #getLastRecordPositionInPage}).
   * @param recordPositionInPage     content offset of the record being written, whose size the caller already resolved
   *                                 in {@code recordSize} - avoids reading it twice when it IS the last one.
   */
  private int getPageOccupiedInBytes(final MutablePage page, final int lastRecordPositionInPage,
                                     final int recordPositionInPage, final long[] recordSize) throws IOException {
    final long[] lastRecordSize;
    if (lastRecordPositionInPage != recordPositionInPage) {
      // CURRENT RECORD IS NOT THE LATEST RIGHT IN THE PAGE

      if (lastRecordPositionInPage < contentHeaderSize)
        // IT SHOULD NEVER OCCUR BECAUSE THE CURRENT RECORD IS PRESENT IN THIS PAGE
        throw new DatabaseOperationException(
                "Invalid position " + lastRecordPositionInPage + " on expanding a record of page " + page.getPageId());

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

    return (int) (lastRecordPositionInPage + lastRecordSize[0] + lastRecordSize[1]);
  }

  /**
   * Writes a record that no longer fits its own space INSIDE its page, shifting the content that follows it to the
   * right and fixing the slot offsets of the records that moved. Only the grown record's content changes: every other
   * slot keeps its record, merely at a different offset that is recomputed from the page - which is what makes the
   * write replayable on a newer committed version of the page by the disjoint-slot merge (#5279,
   * {@link #rebaseRecordOnPage}).
   * <p>
   * Shared by the update path and by that commit-time replay, so both grow a record exactly the same way.
   *
   * @param recordSize the {size, sizeLength} pair of the record as currently stored, as returned by
   *                   {@link MutablePage#readNumberAndSize}; {@code recordSize[1]} is updated on a successful write.
   *
   * @return true when the page could host the extra bytes and the record was written; false when the caller has to
   * spill the record out of the page (placeholder or chunk chain) - in that case nothing has been written.
   */
  private boolean growRecordInPage(final MutablePage page, final int pageNumber, final short recordCountInPage,
                                   final int recordPositionInPage, final int lastRecordPositionInPage, final long[] recordSize,
                                   final boolean isPlaceHolder, final byte[] content, final int contentOffset,
                                   final int contentSize) throws IOException {
    final int pageOccupiedInBytes = getPageOccupiedInBytes(page, lastRecordPositionInPage, recordPositionInPage, recordSize);
    final int spaceAvailableInCurrentPage = page.getMaxContentSize() - pageOccupiedInBytes;
    final int contentSizeLength = Binary.getNumberSpace(isPlaceHolder ? -1L * contentSize : contentSize);
    final int additionalSpaceNeeded = (int) (contentSize + contentSizeLength - recordSize[0] - recordSize[1]);

    if (additionalSpaceNeeded >= spaceAvailableInCurrentPage)
      // NOT ENOUGH ROOM LEFT IN THE PAGE
      return false;

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

    recordSize[1] = page.writeNumber(recordPositionInPage, isPlaceHolder ? -1L * contentSize : contentSize);
    page.writeByteArray((int) (recordPositionInPage + recordSize[1]), content, contentOffset, contentSize);

    updatePageStatistics(pageNumber, spaceAvailableInCurrentPage, -additionalSpaceNeeded);
    return true;
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
