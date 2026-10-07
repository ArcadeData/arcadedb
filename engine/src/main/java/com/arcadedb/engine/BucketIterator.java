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
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.BrokenChunkChainException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.QueryHeapBudget;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.utility.ScanPressureReporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

public class BucketIterator implements Iterator<Record>, ScanPressureReporter {
  private final static int              PREFETCH_SIZE = 1_024;
  // #9404: AN ITERATOR MAY READ AHEAD AT MOST THIS FRACTION OF WHAT IS LEFT OF THE QUERY HEAP BUDGET, SO A BUDGET CLOSE TO FULL
  // SHRINKS THE BATCH DOWN TO ONE RECORD INSTEAD OF HOLDING MEMORY NO QUERY ACCOUNTS FOR
  private final static int              BUDGET_SHARE  = 64;
  private final        DatabaseInternal database;
  // THE INSTANCE A RECORD LOOKED UP BY RID BELONGS TO (e.g. THE SERVER/HA WRAPPER), SO A SCANNED RECORD MODIFIES AND
  // SAVES THROUGH THE SAME ONE
  private final        DatabaseInternal recordDatabase;
  // RESOLVED ONCE PER ITERATOR, NOT PER RECORD: A BUCKET MOVED TO ANOTHER TYPE (OR ITS TYPE DROPPED) WHILE A SCAN IS OPEN
  // IS SEEN BY THE NEXT ITERATOR ONLY. THIS ONE KEEPS THE TYPE IT STARTED WITH FOR EVERY BATCH, HOWEVER LONG IT STAYS OPEN
  private final        DocumentType     type;
  // #9404: THE BYTES A BATCH MAY COPY OUT OF THE PAGES, FROM arcadedb.queryBatchMaxBytes, READ ONCE PER ITERATOR
  private final        long             maxBatchBytes;
  private final        LocalBucket      bucket;
  final                Record[]         nextBatch     = new Record[PREFETCH_SIZE];
  private              int              prefetchIndex = 0;
  final                long             limit;
  private final        boolean          forwardDirection;
  int      nextPageNumber;
  BasePage currentPage = null;
  short    recordCountInCurrentPage;
  int      totalPages;
  int      currentRecordInPage;
  long     browsed     = 0;
  private int  writeIndex     = 0;
  // RECORDS RESOLVED BY THE CURRENT fetchNext(), REPORTED TO THE readRecord STATISTIC ONCE PER BATCH
  private long recordsRead    = 0;
  // #9404: THE BYTES THE CURRENT BATCH COPIED OUT OF THE PAGES (A MULTI-PAGE OR PLACEHOLDER RECORD IS ASSEMBLED INTO A BUFFER OF
  // ITS OWN; A RECORD ON ITS OWN PAGE IS A VIEW OF THE CACHED PAGE AND COSTS NOTHING MORE), AND WHAT A BATCH MAY HOLD
  private long batchBytes     = 0;
  // #9404: THE BATCHES READ WITH THE READ-AHEAD REDUCED BY THE QUERY HEAP BUDGET, WHICH A PROFILED QUERY REPORTS
  // WHETHER THE LIMIT OF THE CURRENT BATCH IS THE SHARE OF THE BUDGET AND NOT THE SETTING
  private boolean limitedByBudget = false;
  // WRITTEN BY THE THREAD THAT SCANS, READ BY A PROFILE ONCE THE SCAN IS OVER: NO SYNCHRONIZATION IS NEEDED FOR A COUNT REPORTED THEN
  private long shrunkBatches  = 0;
  private long skippedRecords = 0;
  // POSITIONS MODE (#8333): THE SORTED POSITIONS [positionIndex, positionsEnd) TO READ, INSTEAD OF EVERY SLOT OF THE PAGES
  private final long[] positions;
  private       int    positionIndex;
  private final int    positionsEnd;
  // #8610: the transaction the current batch is read in (-1 outside one), recorded on each record of the batch. Taken
  // per batch, not per iterator: an iterator kept across commit() and begin() reads its next batch in the new
  // transaction, and a record read there must be checked there
  private       long   readInTransaction = -1;

  BucketIterator(final LocalBucket bucket, final boolean forwardDirection) {
    this(bucket, forwardDirection, 0, -1);
  }

  /**
   * Iterates the records at {@code positions[from, to)}, sorted ascending, rather than every record of the bucket: the
   * addresses an index range matched, loaded in physical order (issue #8333). A page is read once however many of the
   * positions it holds, and the records are built from it in batches under one read lock, exactly as a scan builds
   * them. A position whose record is gone - deleted since the index was read - is skipped, as a scan skips it.
   */
  BucketIterator(final LocalBucket bucket, final long[] positions, final int from, final int to) {
    this(bucket, true, 0, -1, positions, from, to);
  }

  /**
   * @param fromPage first page to scan, forward only (issue #8523: a bucket split in page ranges scanned in parallel)
   * @param toPage   page to stop at, excluded, or -1 to scan to the last page the bucket has when the iterator opens
   */
  BucketIterator(final LocalBucket bucket, final boolean forwardDirection, final int fromPage, final int toPage) {
    this(bucket, forwardDirection, fromPage, toPage, null, 0, 0);
  }

  private BucketIterator(final LocalBucket bucket, final boolean forwardDirection, final int fromPage, final int toPage,
      final long[] positions, final int positionsFrom, final int positionsTo) {
    final DatabaseInternal db = bucket.getDatabase();
    db.checkPermissionsOnFile(bucket.fileId, SecurityDatabaseUser.ACCESS.READ_RECORD);

    this.database = db;
    this.recordDatabase = db.getWrappedDatabaseInstance();
    this.type = db.getSchema().getTypeByBucketId(bucket.fileId);
    this.bucket = bucket;
    this.forwardDirection = forwardDirection;
    this.positions = positions;
    this.positionIndex = positionsFrom;
    this.positionsEnd = positionsTo;
    this.totalPages = bucket.pageCount.get();

    final Integer txPageCounter = database.getTransaction().getPageCounter(bucket.fileId);
    if (txPageCounter != null && txPageCounter > totalPages)
      this.totalPages = txPageCounter;

    if (toPage > -1 && toPage < this.totalPages) {
      if (!forwardDirection)
        throw new IllegalArgumentException("A page range is scanned forward only");
      this.totalPages = toPage;
    }

    limit = database.getResultSetLimit();
    maxBatchBytes = database.getConfiguration().getValueAsLong(GlobalConfiguration.QUERY_BATCH_MAX_BYTES);

    if (forwardDirection) {
      currentRecordInPage = 0;
      nextPageNumber = fromPage;
    } else {
      nextPageNumber = this.totalPages - 1;
      currentRecordInPage = Integer.MAX_VALUE;
    }

    fetchNext();
  }

  public void setPosition(final RID position) throws IOException {
    prefetchIndex = 0;
    nextBatch[prefetchIndex] = position.getRecord();
    nextPageNumber = (int) (position.getPosition() / bucket.getMaxRecordsInPage());
    currentRecordInPage = (int) (position.getPosition() % bucket.getMaxRecordsInPage()) + 1;
    currentPage = database.getTransaction().getPage(new PageId(database, position.getBucketId(), nextPageNumber),
        bucket.pageSize);
    recordCountInCurrentPage = currentPage.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
  }

  /**
   * Number of records skipped so far for a known, tolerated corruption reason - a corrupted on-disk record
   * ({@link SerializationException}), page-layer corruption in a raw slot/pointer read, or a multi-page record
   * whose chunk chain the loader itself confirms is structurally broken ({@link BrokenChunkChainException}) - all via
   * {@link #logSkippedRecord(Exception)} in {@link #fetchNext()}. A correctness-sensitive caller (e.g. an exact
   * {@code COUNT}) can check this after exhausting the iterator to detect a truncated result; it is not
   * incremented for the benign concurrent-delete race handled by {@link RecordNotFoundException}, nor for a
   * {@link ConcurrentModificationException}, which the loader raises for a break it could NOT confirm and which
   * therefore propagates so a retry can resolve it (#6282), nor for any other exception, which likewise
   * propagates (#6015). It is
   * also, deliberately, not incremented for a slot whose position resolves to 0: since 24.1.1 that is a plain
   * deleted record (the delete zeroes the slot), not corruption, so it is skipped the same way
   * {@link RecordNotFoundException} is - this counter tracks skipped-due-to-corruption, not every reason a scan
   * can return fewer records than the bucket's raw slot count.
   */
  public long getSkippedRecordCount() {
    return skippedRecords;
  }

  /**
   * Counts and logs a record slot skipped for a known, tolerated reason: a corrupted on-disk record
   * ({@link SerializationException}), page-layer corruption in a raw slot/pointer read (an unchecked exception
   * from a {@code BasePage.read*}/{@code Binary} accessor), or a confirmed structurally-broken multi-page chunk
   * chain ({@link BrokenChunkChainException}) - see the call sites in {@link #fetchNext()}.
   */
  private void logSkippedRecord(final Exception e) {
    skippedRecords++;
    final String msg = "Error on loading record #%d:%d (error: %s)".formatted(currentPage.pageId.getFileId(),
        (nextPageNumber * bucket.getMaxRecordsInPage()) + currentRecordInPage, e.getMessage());
    LogManager.instance().log(this, Level.SEVERE, msg);
  }

  /**
   * Builds the record over its content and runs the after-read events on it.
   *
   * @param pageVersion version of the page the content was read from when it was read whole from the record's own page,
   *                    -1 otherwise
   *
   * @return the record, or {@code null} if an after-read event filtered it away
   */
  private Record newRecord(final RID rid, final Binary content, final long pageVersion) {
    final Record record = database.getRecordFactory().newImmutableRecord(recordDatabase, type, rid, content, null);
    if (record instanceof ImmutableDocument document) {
      if (pageVersion > -1)
        document.setContentPageVersion(pageVersion);
      document.setReadInTransaction(readInTransaction);
    }
    return database.invokeAfterReadEvents(record);
  }

  @Override
  public boolean hasNext() {
    if (limit > -1 && browsed >= limit)
      return false;
    return prefetchIndex < writeIndex && nextBatch[prefetchIndex] != null;
  }

  @Override
  public Record next() {
    if (prefetchIndex >= writeIndex || nextBatch[prefetchIndex] == null)
      throw new IllegalStateException();

    ++browsed;
    final Record record = nextBatch[prefetchIndex];
    nextBatch[prefetchIndex] = null; // EARLY CLEANSE FOR GC
    prefetchIndex++;
    fetchNext();
    return record;
  }

  /**
   * Reads the slot {@link #currentRecordInPage} of {@link #currentPage}, the page {@link #nextPageNumber}, and appends
   * its record to the batch when it holds one of its own. Under the read lock {@link #fetchNext()} holds.
   */
  private void readSlot() {
    try {
      final int recordPositionInPage;
      final long[] recordSize;
      try {
        recordPositionInPage = (int) currentPage.readUnsignedInt(
            LocalBucket.PAGE_RECORD_TABLE_OFFSET + currentRecordInPage * INT_SERIALIZED_SIZE);
        if (recordPositionInPage == 0)
          // DELETED RECORD (>= 24.1.1; it was "cleaned corrupted record" before), not corruption - a plain
          // delete zeroes the slot. Skip it silently like RecordNotFoundException below, not counted by
          // getSkippedRecordCount() (see its javadoc): matches LocalBucket's own treatment of the same
          // check (e.g. deleteRecordInternal's recordPositionInPage < 1 throws RecordNotFoundException).
          return;

        recordSize = currentPage.readNumberAndSize(recordPositionInPage);
      } catch (final RuntimeException e) {
        // CORRUPTED SLOT-TABLE ENTRY OR RECORD HEADER: these two calls only ever touch raw page bytes (no
        // application/listener code runs here), so an out-of-bounds read (IndexOutOfBoundsException /
        // IllegalArgumentException / BufferUnderflowException depending on which Binary accessor caught it
        // first) is provably page corruption, the same "bad slot" signal as the RECORD_POSITION==0 case
        // just above. Skip it like SerializationException below, scoped narrowly to just these two reads so
        // it cannot also catch a bug from database.lookupByRID()/AfterRecordReadListener (#6015).
        logSkippedRecord(e);
        return;
      }

      final boolean inPage = recordSize[0] > 0;
      if (!inPage && recordSize[0] != LocalBucket.FIRST_CHUNK && recordSize[0] != LocalBucket.RECORD_PLACEHOLDER_POINTER)
        // DELETED, OR THE CONTENT/CHUNK OF ANOTHER RECORD: NOT A RECORD OF ITS OWN
        return;

      final RID rid = new RID(bucket.fileId,
          ((long) nextPageNumber) * bucket.getMaxRecordsInPage() + currentRecordInPage);
      // COUNTED HERE, BEFORE ANY EVENT CAN FILTER IT, AS lookupByRID() COUNTS EVERY LOOKUP ON ENTRY
      ++recordsRead;

      // A RECORD THIS TRANSACTION ALREADY HOLDS (POSSIBLY MODIFIED AND NOT SAVED YET) IS ANSWERED FROM IT, NOT
      // FROM THE PAGE, THE SAME WAY lookupByRID() DOES
      final Record inTransaction = database.getTransaction().getRecordFromCache(rid);
      if (inTransaction != null) {
        nextBatch[writeIndex++] = inTransaction;
        return;
      }

      // THE RECORD IS BUILT HERE, FROM THE PAGE ALREADY IN HAND, NOT HANDED OUT AS A LAZY SHELL THAT RE-READ IT
      // THROUGH A SECOND PAGE LOOKUP ON ITS FIRST PROPERTY ACCESS (#8312). THE READ EVENTS FIRE NOW, ONCE, AS FOR
      // ANY LOADED RECORD: A RECORD THE AFTER-READ EVENTS FILTER AWAY IS SKIPPED, AS lookupByRID(rid, true) DOES
      final Binary content;
      final long pageVersion;
      if (inPage) {
        if (!bucket.fireBeforeReadEvents(rid))
          return;
        content = currentPage.getImmutableView((int) (recordPositionInPage + recordSize[1]), (int) recordSize[0]);
        // modify() CAN SKIP ITS RELOAD WHILE THIS PAGE VERSION IS STILL THE CURRENT ONE
        pageVersion = currentPage.getVersion();
      } else {
        final Binary loaded;
        try {
          if (recordSize[0] == LocalBucket.FIRST_CHUNK)
            // MULTI-PAGE RECORD: THE LOADER FIRES THE BEFORE-READ EVENTS AND WALKS THE CHUNK CHAIN
            loaded = bucket.getRecordInternal(rid, false);
          else {
            // PLACEHOLDER: THE CONTENT LIVES AT THE POSITION THE POINTER NAMES. Same page-bytes-only shape as the
            // slot-table resolution above: no application/listener code runs in this read, so a corrupted pointer
            // field is page corruption, not an application bug.
            final long placeholderTargetPosition;
            try {
              placeholderTargetPosition = currentPage.readLong((int) (recordPositionInPage + recordSize[1]));
            } catch (final RuntimeException e) {
              logSkippedRecord(e);
              return;
            }
            if (!bucket.fireBeforeReadEvents(rid))
              return;
            // THE EVENTS JUST RAN FOR THE RECORD ITSELF: NOT AGAIN FOR THE INTERNAL POSITION OF ITS CONTENT
            loaded = bucket.getRecordInternal(new RID(bucket.fileId, placeholderTargetPosition), true, false);
          }
        } catch (final BrokenChunkChainException e) {
          // THE LOADER ITSELF SAYS SO (#6258): a chain the read could not parse, confirmed broken against the
          // newest committed image with the read's own chunks proven current. No second walk needed here.
          logSkippedRecord(e);
          return;
        }
        // NO ConcurrentModificationException ARM, AND ITS REMOVAL IS THE POINT (#6282). Until #6258 this
        // exception was the only thing the loader could say about a chain it could not parse, so the scan
        // re-walked the chain with isChunkChainBroken to tell corruption from contention and skipped the
        // record when the walk agreed. The loader now answers that question ITSELF, and it asks a strictly
        // STRONGER version of it: the committed image must fail to walk AND the chunks this read consumed
        // must still be current, which is what rules out a chain caught mid-publication. So a CME reaching
        // here means the break could NOT be confirmed - the record moved under the read - and a probe saying
        // "broken" about it is answering a weaker question about a chain that is no longer the one this read
        // followed. Skipping on that evidence silently drops a HEALTHY record from a scan; propagating lets
        // the caller's retry machinery re-read it, which is what getSkippedRecordCount's contract has said
        // all along.
        if (loaded == null)
          // FILTERED BY A BEFORE-READ EVENT, OR GONE
          return;
        content = loaded;
        // THE CONTENT IS NOT (ONLY) ON THE RECORD'S OWN PAGE, WHICH IS THE ONE modify() PINS: ALWAYS RELOAD THERE
        pageVersion = -1;
      }

      final Record record = newRecord(rid, content, pageVersion);
      if (record != null) {
        nextBatch[writeIndex++] = record;
        // ONLY A RECORD ASSEMBLED INTO A BUFFER OF ITS OWN (A MULTI-PAGE CHAIN OR A PLACEHOLDER'S CONTENT) ADDS HEAP: A RECORD ON ITS
        // OWN PAGE IS A VIEW OF THE CACHED PAGE, AND ONE THE TRANSACTION ALREADY HOLDS IS ANSWERED FROM IT ABOVE, BOTH ALREADY IN MEMORY
        if (!inPage)
          batchBytes += content.size();
      }
    } catch (final RecordNotFoundException e) {
      // BENIGN RACE: the record existed a moment ago when its slot was read from currentPage above, but
      // was concurrently deleted before getRecordInternal() executed. Skip it silently, the
      // same way the other "turned out to be gone" checks above already do with a plain `return`.
    } catch (final SerializationException e) {
      // KNOWN-CORRUPT ON-DISK RECORD: log and skip so one bad record does not abort an otherwise healthy
      // full scan (the CHECK DATABASE-shaped case). Every OTHER exception - including one from a
      // user-supplied AfterRecordReadListener/trigger, and a ConcurrentModificationException, which the
      // loader raises only for a break it could NOT confirm - is deliberately NOT caught here and
      // propagates instead, so a real bug (or genuine contention needing a real retry) surfaces where it
      // can be diagnosed or retried instead of silently looking like "this bucket has fewer records"
      // (#6015; see #5976 for a listener bug this used to hide).
      logSkippedRecord(e);
    }
  }

  /**
   * Positions mode: reads the record at the next position, loading its page unless it is the one in hand.
   *
   * @return false once every position has been read
   */
  private boolean readNextPosition() throws IOException {
    if (positionIndex >= positionsEnd)
      return false;

    final long position = positions[positionIndex++];
    final int pageNumber = (int) (position / bucket.getMaxRecordsInPage());
    if (currentPage == null || pageNumber != nextPageNumber) {
      currentPage = null;
      if (position < 0 || pageNumber >= totalPages)
        // GONE: THE BUCKET NEVER HAD THIS PAGE, OR NOT ANY MORE
        return true;
      nextPageNumber = pageNumber;
      currentPage = database.getTransaction().getPage(new PageId(database, bucket.file.getFileId(), pageNumber), bucket.pageSize);
      recordCountInCurrentPage = currentPage.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
    }

    currentRecordInPage = (int) (position % bucket.getMaxRecordsInPage());
    if (currentRecordInPage < recordCountInCurrentPage)
      readSlot();
    return true;
  }

  /**
   * The bytes copied out of the pages a batch may reach before it ends: {@link GlobalConfiguration#QUERY_BATCH_MAX_BYTES}, or less
   * when the queries running have taken most of the query heap budget. A batch always holds at least one record, and keeps taking records
   * that copy nothing (a record on its own page) until one is copied, so the floor is a scan that copies one record at a time. The budget share is sampled once per batch and is a heuristic, not a reservation: every
   * scan running takes its share of what is left, so the read-ahead of many scans together is bounded by the budget only roughly.
   */
  private long batchByteLimit() {
    if (maxBatchBytes <= 0)
      return Long.MAX_VALUE;
    final long budgetShare = QueryHeapBudget.getAvailableBytes() / BUDGET_SHARE;
    if (budgetShare >= maxBatchBytes)
      return maxBatchBytes;
    limitedByBudget = true;
    return budgetShare;
  }

  @Override
  public long getBudgetShrunkBatches() {
    return shrunkBatches;
  }

  private void fetchNext() {
    if (prefetchIndex < writeIndex)
      return;

    // One lookup per batch: every record of the batch is read now, in this transaction
    readInTransaction = database.getTransaction().getBeginSequence();

    recordsRead = 0;
    batchBytes = 0L;
    limitedByBudget = false;
    final long batchByteLimit = batchByteLimit();
    try {
      database.executeInReadLock(() -> {
        prefetchIndex = 0;
        nextBatch[prefetchIndex] = null;

        // A BATCH OF LARGE RECORDS ENDS BY BYTES, NOT BY COUNT: 1,024 RECORDS OF 100KB WOULD BE 100MB PER BUCKET, PER SCAN (#9404)
        for (writeIndex = 0; writeIndex < nextBatch.length && (batchBytes == 0 || batchBytes < batchByteLimit); ) {
          if (positions != null) {
            if (!readNextPosition())
              return null;
            continue;
          }

          if (currentPage == null) {
            if (forwardDirection) {
              // MOVE FORWARD
              if (nextPageNumber >= totalPages)
                return null;
            } else {
              // MOVE BACKWARDS
              if (nextPageNumber < 0)
                return null;
            }

            currentPage = database.getTransaction()
                .getPage(new PageId(database, bucket.file.getFileId(), nextPageNumber), bucket.pageSize);
            recordCountInCurrentPage = currentPage.readShort(LocalBucket.PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

            if (!forwardDirection && currentRecordInPage == Integer.MAX_VALUE)
              currentRecordInPage = recordCountInCurrentPage - 1;
          }

          if (recordCountInCurrentPage > 0 &&
              (forwardDirection && currentRecordInPage < recordCountInCurrentPage) ||
              (!forwardDirection && currentRecordInPage > -1)
          ) {
            try {
              readSlot();
            } finally {
              if (forwardDirection)
                currentRecordInPage++;
              else
                currentRecordInPage--;
            }

          } else if (forwardDirection && currentRecordInPage == recordCountInCurrentPage) {
            currentRecordInPage = 0;
            currentPage = null;
            nextPageNumber++;
          } else if (!forwardDirection && currentRecordInPage < 0) {
            currentRecordInPage = Integer.MAX_VALUE;
            currentPage = null;
            nextPageNumber--;
          } else {
            if (forwardDirection)
              currentRecordInPage++;
            else
              currentRecordInPage--;
          }
        }
        return null;
      });
    } finally {
      if (recordsRead > 0)
        database.countRecordsRead(recordsRead);
      // A BATCH IS REDUCED WHEN THE BUDGET, NOT THE SETTING, ENDED IT: IT COPIED BYTES AND THEY REACHED THE SHARE (AN EMPTY BUCKET, OR
      // RECORDS ON THEIR OWN PAGE, COPY NOTHING AND ARE NOT SLOWED BY IT)
      if (limitedByBudget && batchBytes > 0 && batchBytes >= batchByteLimit) {
        ++shrunkBatches;
        QueryHeapBudget.scanBatchShrunk();
      }
    }
  }
}
