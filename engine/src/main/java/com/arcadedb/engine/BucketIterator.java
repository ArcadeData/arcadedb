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

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.security.SecurityDatabaseUser;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;

public class BucketIterator implements Iterator<Record> {
  private final static int              PREFETCH_SIZE = 1_024;
  private final        DatabaseInternal database;
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
  private long skippedRecords = 0;

  BucketIterator(final LocalBucket bucket, final boolean forwardDirection) {
    final DatabaseInternal db = bucket.getDatabase();
    db.checkPermissionsOnFile(bucket.fileId, SecurityDatabaseUser.ACCESS.READ_RECORD);

    this.database = db;
    this.bucket = bucket;
    this.forwardDirection = forwardDirection;
    this.totalPages = bucket.pageCount.get();

    final Integer txPageCounter = database.getTransaction().getPageCounter(bucket.fileId);
    if (txPageCounter != null && txPageCounter > totalPages)
      this.totalPages = txPageCounter;

    limit = database.getResultSetLimit();

    if (forwardDirection) {
      currentRecordInPage = 0;
      nextPageNumber = 0;
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
   * whose chunk chain {@link LocalBucket#isChunkChainBroken} confirms is structurally broken - all via
   * {@link #logSkippedRecord(Exception)} in {@link #fetchNext()}. A correctness-sensitive caller (e.g. an exact
   * {@code COUNT}) can check this after exhausting the iterator to detect a truncated result; it is not
   * incremented for the benign concurrent-delete race handled by {@link RecordNotFoundException}, nor for a
   * {@link ConcurrentModificationException} not confirmed as a broken chain (transient contention propagates
   * instead so a retry can resolve it), nor for any other exception, which likewise propagates (#6015). It is
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
   * chain ({@link ConcurrentModificationException} where {@link LocalBucket#isChunkChainBroken} returns
   * {@code true}) - see the call sites in {@link #fetchNext()}.
   */
  private void logSkippedRecord(final Exception e) {
    skippedRecords++;
    final String msg = "Error on loading record #%d:%d (error: %s)".formatted(currentPage.pageId.getFileId(),
        (nextPageNumber * bucket.getMaxRecordsInPage()) + currentRecordInPage, e.getMessage());
    LogManager.instance().log(this, Level.SEVERE, msg);
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

  private void fetchNext() {
    if (prefetchIndex < writeIndex)
      return;

    database.executeInReadLock(() -> {
      prefetchIndex = 0;
      nextBatch[prefetchIndex] = null;

      for (writeIndex = 0; writeIndex < nextBatch.length; ) {
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
                continue;

              recordSize = currentPage.readNumberAndSize(recordPositionInPage);
            } catch (final RuntimeException e) {
              // CORRUPTED SLOT-TABLE ENTRY OR RECORD HEADER: these two calls only ever touch raw page bytes (no
              // application/listener code runs here), so an out-of-bounds read (IndexOutOfBoundsException /
              // IllegalArgumentException / BufferUnderflowException depending on which Binary accessor caught it
              // first) is provably page corruption, the same "bad slot" signal as the RECORD_POSITION==0 case
              // just above. Skip it like SerializationException below, scoped narrowly to just these two reads so
              // it cannot also catch a bug from database.lookupByRID()/AfterRecordReadListener (#6015).
              logSkippedRecord(e);
              continue;
            }

            if (recordSize[0] > 0 || recordSize[0] == LocalBucket.FIRST_CHUNK) {
              // NOT DELETED
              final RID rid = new RID(bucket.fileId,
                  ((long) nextPageNumber) * bucket.getMaxRecordsInPage() + currentRecordInPage);

              if (!bucket.existsRecord(rid))
                continue;

              try {
                nextBatch[writeIndex++] = database.lookupByRID(rid, false);
              } catch (final ConcurrentModificationException e) {
                if (!bucket.isChunkChainBroken(rid))
                  // TRANSIENT CONTENTION, NOT PROVEN CORRUPTION: loadMultiPageRecord() throws this after
                  // exhausting TX_RETRIES, but exhausted retries alone do not prove the chain is broken - its
                  // page-version validation also fails under ordinary concurrent writes to OTHER records sharing
                  // the chain's pages. Propagate so the caller's retry machinery (if any) re-runs, the same
                  // disambiguation LocalDatabase.deleteRecordInternal() already applies to this exception.
                  throw e;
                // CONFIRMED STRUCTURALLY BROKEN CHAIN: a version-blind walk (isChunkChainBroken) found a genuinely
                // bad continuation pointer, not just a version mismatch - this is corruption, not contention.
                // UNDO THE RESERVED SLOT: per JLS 15.26.1, `nextBatch[writeIndex++] = ...` evaluates the array
                // index (incrementing writeIndex) BEFORE the right-hand side, so the increment already happened
                // even though lookupByRID() threw before ever writing into that slot. Do not "simplify" this away.
                writeIndex--;
                logSkippedRecord(e);
              }

            } else if (recordSize[0] == LocalBucket.RECORD_PLACEHOLDER_POINTER) {
              // PLACEHOLDER
              final RID rid = new RID(bucket.fileId,
                  ((long) nextPageNumber) * bucket.getMaxRecordsInPage() + currentRecordInPage);

              final long placeholderTargetPosition;
              try {
                // Same page-bytes-only shape as the slot-table resolution above: no application/listener code
                // runs in this call, so a corrupted pointer field is page corruption, not an application bug.
                placeholderTargetPosition = currentPage.readLong((int) (recordPositionInPage + recordSize[1]));
              } catch (final RuntimeException e) {
                logSkippedRecord(e);
                continue;
              }

              final RID placeholderTargetRid = new RID(bucket.fileId, placeholderTargetPosition);
              final Binary view;
              try {
                view = bucket.getRecordInternal(placeholderTargetRid, true);
              } catch (final ConcurrentModificationException e) {
                if (!bucket.isChunkChainBroken(placeholderTargetRid))
                  // TRANSIENT CONTENTION, NOT PROVEN CORRUPTION: see the identical disambiguation on the
                  // NOT-DELETED branch above.
                  throw e;
                logSkippedRecord(e);
                continue;
              }

              if (view == null)
                continue;

              nextBatch[writeIndex++] = database.getRecordFactory().newImmutableRecord(database,
                  database.getSchema().getType(database.getSchema().getTypeNameByBucketId(rid.getBucketId())), rid,
                  view, null);
            }
          } catch (final RecordNotFoundException e) {
            // BENIGN RACE: the record existed a moment ago when bucket.existsRecord(rid) was checked above, but
            // was concurrently deleted before lookupByRID()/getRecordInternal() executed. Skip it silently, the
            // same way the other "turned out to be gone" checks in this loop already do with a plain `continue`.
          } catch (final SerializationException e) {
            // KNOWN-CORRUPT ON-DISK RECORD: log and skip so one bad record does not abort an otherwise healthy
            // full scan (the CHECK DATABASE-shaped case). Every OTHER exception - including one from a
            // user-supplied AfterRecordReadListener/trigger, and a ConcurrentModificationException not confirmed
            // as a broken chunk chain by the two dedicated catches above - is deliberately NOT caught here and
            // propagates instead, so a real bug (or genuine contention needing a real retry) surfaces where it
            // can be diagnosed or retried instead of silently looking like "this bucket has fewer records"
            // (#6015; see #5976 for a listener bug this used to hide).
            logSkippedRecord(e);
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
  }
}
