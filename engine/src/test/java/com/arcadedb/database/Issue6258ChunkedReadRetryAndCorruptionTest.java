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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PageManager;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.BrokenChunkChainException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6258, items 1 and 2: the two things {@code LocalBucket.loadMultiPageRecord} used to get wrong once a chunked
 * read did not come out clean.
 * <ol>
 * <li>It retried when retrying could not possibly change the answer. The retry re-fetches the head page straight from
 * the {@code PageManager}, but the walk takes its continuation pages from the transaction - so under
 * {@code REPEATABLE_READ} every attempt after the first pairs the very same fresh head with the very same pinned
 * tails, reproduces the very same mix, and is rejected for the very same reason. The read spent its whole budget of
 * complete chain walks on a verdict settled before the first retry started, on the slowest read path there is.</li>
 * <li>It reported a corrupted record as contention. A chunk pointer into nowhere, a slot that no longer holds a
 * chunk, a chain that loops - none of them get better by trying again, but all of them took the retry path and ended
 * at "was modified during read after 3 retries", sending whoever read the log looking for contention that was not
 * there.</li>
 * </ol>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6258ChunkedReadRetryAndCorruptionTest extends BucketPageLayoutTestSupport {
  private static final String TYPE = "ChunkedRecord";

  /** {@code LocalBucket.FIRST_CHUNK} (-2) and {@code NEXT_CHUNK} (-3), as the single zigzag byte each is stored as. */
  private static final byte FIRST_CHUNK_MARKER = 3;
  private static final byte NEXT_CHUNK_MARKER  = 5;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // Two of the tests below deliberately inject on-disk corruption, which the post-test integrity check would
    // (correctly) flag. The tests that do NOT corrupt anything run `checkDatabase()` themselves.
    return false;
  }

  /**
   * Item 1. Under {@code REPEATABLE_READ} the reading transaction pins every page the chain walks, so a rewrite of
   * the record itself leaves the read no way out: the first retry is worth making (the head page is re-fetched
   * outside the transaction, so it really is different), and every one after it reads byte for byte what the one
   * before it read.
   * <p>
   * The counter is the whole point of the test. What changed is not whether the read fails - it failed before and
   * fails now - but how much it costs to find out: one wasted chain walk instead of the entire {@code txRetries}
   * budget, which is what a chunked record under contention pays on the largest records in the database.
   */
  @Test
  void aChunkedReadStopsRetryingWhenNoRetryCouldReadAnythingElse() {
    final RID[] rids = createChunkedRecords(TYPE);
    final LocalBucket bucket = bucketOf(TYPE);
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();

    // A budget far larger than the default, so the assertion below measures the SHORT-CIRCUIT rather than the
    // budget: what must not happen is the cost growing with the number of retries the reader is allowed. The
    // DATABASE's configuration, not the global one - the global has no effect once the database is open.
    final int previousRetries = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES);
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, 10);

    final long retriesBefore = pageManager.getStats().chunkChainReadRetries;
    try {
      database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      try {
        // Pins the whole chain - head and every continuation page - in this transaction's snapshot.
        bucket.getRecordInternal(rids[0], false);

        // The record itself is rewritten, in every chunk, so the read that follows is a genuine conflict rather than
        // the false one #6217 removed.
        inAnotherThread(() -> database.transaction(
            () -> rids[0].asDocument(true).modify().set("payload", payload(0, 'y')).save()));

        assertThatThrownBy(() -> bucket.getRecordInternal(rids[0], false))
            .as("a record rewritten under a read must still fail the read")
            .isInstanceOf(ConcurrentModificationException.class)
            .hasMessageContaining("was modified during read")
            .as("and must say why retrying it here cannot help, rather than advertise retries it already spent")
            .hasMessageContaining("no retry can assemble a different one");
      } finally {
        database.rollback();
      }
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRIES, previousRetries);
    }

    assertThat(pageManager.getStats().chunkChainReadRetries - retriesBefore)
        .as("the read must stop as soon as a retry cannot read anything else: the first attempt, plus the one retry "
            + "that had a fresh head page to try - and NOT one per unit of a budget it can do nothing with")
        .isEqualTo(2);
  }

  /**
   * Item 1, the other side: the fail-fast must not fire while a retry still has something to read. Nothing of this
   * record's chain is pinned when the rewrite lands, so the read reloads all of it and simply succeeds - which is
   * also the proof that the new short-circuit is not a blanket "give up on the first conflict".
   */
  @Test
  void aChunkedReadStillRetriesWhileARetryCanStillReadSomethingElse() {
    final RID[] rids = createChunkedRecords(TYPE);
    final LocalBucket bucket = bucketOf(TYPE);

    final String rewritten = payload(0, 'y');
    inAnotherThread(() -> database.transaction(
        () -> rids[0].asDocument(true).modify().set("payload", rewritten).save()));

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      assertThat(rids[0].asDocument(true).getString("payload"))
          .as("a chunked record read for the first time after a rewrite reads the rewrite, not a conflict")
          .isEqualTo(rewritten);
    } finally {
      database.rollback();
    }

    checkDatabase();
  }

  /**
   * Item 2. A continuation pointer to a page the file does not have is corruption: every read of that record used to
   * walk the chain {@code txRetries + 1} times before reporting a concurrency problem that did not exist.
   */
  @Test
  void aChunkPointerIntoNowhereIsReportedAsCorruptionAndNotRetried() {
    final RID[] rids = createChunkedRecords(TYPE);
    final int maxRecordsInPage = bucketOf(TYPE).getMaxRecordsInPage();

    // A continuation pointer well past the end of the file: the chain breaks at chunk 0.
    corruptHeadChunkPointer(rids[0], 1_000_000L * maxRecordsInPage);

    assertBrokenChainIsReportedWithoutRetrying(rids[0], "next chunk pointer out of range at chunk 0");
  }

  /**
   * Item 2, the shape that was not merely retried but swallowed: the head chunk pointing at ITSELF used to raise a
   * {@code DatabaseOperationException} inside the walk, which the walk's own catch turned back into "the chain is
   * inconsistent" - so a record whose chain loops was retried like contention and then reported as contention, with
   * the loop the walk had positively identified nowhere in the message.
   */
  @Test
  void aChainThatLoopsIsReportedAsCorruptionAndNotRetried() {
    final RID[] rids = createChunkedRecords(TYPE);
    final int maxRecordsInPage = bucketOf(TYPE).getMaxRecordsInPage();

    // Record 1 rather than record 0: a self-reference is expressed as the chunk's own (page, slot), and record 0's
    // is page 0 slot 0, whose pointer value is 0 - which is how the LAST chunk of a chain says it has no next.
    final RID looping = rids[1];
    corruptHeadChunkPointer(looping, looping.getPosition() % maxRecordsInPage);

    assertBrokenChainIsReportedWithoutRetrying(looping, "chain loop detected at chunk 0");
  }

  /**
   * The same shape, one hop further out, where the walk's cheap guard cannot see it: chunk 2 pointing back at chunk 1
   * is a cycle no comparison of consecutive chunks detects, so the walk followed it forever - growing its trace and
   * the record it was assembling with every lap, on a record that could never be assembled. It now carries the exact
   * detector the confirmation walk always had (a visited-pointer set, allocated only once a chain is long enough to
   * need one), so the loop ends in the same corruption verdict as the self-reference above (code review on #6258).
   */
  @Test
  void aChainThatLoopsPastItsFirstHopIsAlsoReportedAsCorruption() {
    final RID[] rids = createChunkedRecords(TYPE);
    final int bucketId = rids[0].getBucketId();

    // head -> chunk1 -> chunk2 -> ... becomes head -> chunk1 -> chunk2 -> chunk1 -> ...
    final long chunk1 = nextChunkPointerOf(bucketId, rids[0].getPosition(), FIRST_CHUNK_MARKER);
    final long chunk2 = nextChunkPointerOf(bucketId, chunk1, NEXT_CHUNK_MARKER);
    assertThat(chunk1).as("the fixture must give record 0 a chain of at least three chunks").isPositive();
    assertThat(chunk2).as("the fixture must give record 0 a chain of at least three chunks").isPositive();

    writeNextChunkPointer(bucketId, chunk2, NEXT_CHUNK_MARKER, chunk1);

    // "visited twice" is the EXACT detector's wording, not the self-reference guard's: asserting it is what keeps
    // this test honest, since a cycle caught by the cheap guard would prove nothing about the one added here.
    assertBrokenChainIsReportedWithoutRetrying(rids[0], "visited twice");
  }

  /**
   * The boundary between the two items: a record that really moved under the read is contention, and must keep being
   * reported as contention. Only a chain that does not PARSE - and that still does not parse against the newest
   * committed image, with the read's own chunks proven current - is corruption.
   */
  @Test
  void aRecordThatMovedUnderTheReadIsContentionAndNotCorruption() {
    final RID[] rids = createChunkedRecords(TYPE);
    final LocalBucket bucket = bucketOf(TYPE);

    database.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      bucket.getRecordInternal(rids[0], false);

      inAnotherThread(() -> database.transaction(
          () -> rids[0].asDocument(true).modify().set("payload", payload(0, 'y')).save()));

      assertThatThrownBy(() -> bucket.getRecordInternal(rids[0], false))
          .as("a busy record must not be condemned as a corrupted one")
          .isInstanceOf(ConcurrentModificationException.class)
          .isNotInstanceOf(BrokenChunkChainException.class);
    } finally {
      database.rollback();
    }

    checkDatabase();
  }

  /**
   * A broken chain must be reported as what it is, on the FIRST attempt, and as an exception the retry machinery does
   * not act on: a {@link NeedRetryException} tells every layer above to run the whole transaction again, which for a
   * corrupted record multiplies the wasted work instead of surfacing the repair.
   */
  private void assertBrokenChainIsReportedWithoutRetrying(final RID broken, final String expectedReason) {
    reopenDatabase();

    final LocalBucket bucket = bucketOf(TYPE);
    final PageManager pageManager = ((DatabaseInternal) database).getPageManager();
    final long retriesBefore = pageManager.getStats().chunkChainReadRetries;

    database.begin();
    try {
      assertThatThrownBy(() -> bucket.getRecordInternal(broken, false))
          .isInstanceOf(BrokenChunkChainException.class)
          .isNotInstanceOf(NeedRetryException.class)
          .hasMessageContaining("has a broken chunk chain")
          .hasMessageContaining(expectedReason)
          .hasMessageContaining("CHECK DATABASE FIX");
    } finally {
      database.rollback();
    }

    assertThat(pageManager.getStats().chunkChainReadRetries - retriesBefore)
        .as("a chain that cannot be parsed must not spend a single retry: retrying cannot change the answer")
        .isZero();
  }

  /** Overwrites the next-chunk pointer of the head chunk of {@code rid}, which lives at slot 0..n of page 0. */
  private void corruptHeadChunkPointer(final RID rid, final long nextChunkPointer) {
    writeNextChunkPointer(rid.getBucketId(), rid.getPosition(), FIRST_CHUNK_MARKER, nextChunkPointer);
  }

  /** The pointer to the next chunk carried by the chunk at {@code chunkPointer}, or 0 when it is the last one. */
  private long nextChunkPointerOf(final int bucketId, final long chunkPointer, final byte marker) {
    final long[] read = new long[1];
    ((DatabaseInternal) database).transaction(
        () -> read[0] = onChunkPointerField(bucketId, chunkPointer, marker, null));
    return read[0];
  }

  /** Points the chunk at {@code chunkPointer} somewhere else, which is how every corruption here is injected. */
  private void writeNextChunkPointer(final int bucketId, final long chunkPointer, final byte marker,
      final long nextChunkPointer) {
    ((DatabaseInternal) database).transaction(
        () -> onChunkPointerField(bucketId, chunkPointer, marker, nextChunkPointer));
  }

  /**
   * Reads (when {@code newValue} is null) or overwrites the next-chunk pointer field of one chunk of a chain,
   * addressed the way the chain itself addresses it: {@code chunkPointer = pageNumber * maxRecordsInPage + slot}. The
   * on-page layout after the record's size marker is {@code [chunkSize:int][nextChunkPointer:long][content...]}, and
   * the marker itself is one zigzag-encoded byte for both of the values used here.
   *
   * @return the pointer as it was BEFORE any write.
   */
  private long onChunkPointerField(final int bucketId, final long chunkPointer, final byte marker,
      final Long newValue) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketId)).getPageSize();
    final int maxRecordsInPage = bucketOf(TYPE).getMaxRecordsInPage();
    final int pageNumber = (int) (chunkPointer / maxRecordsInPage);
    final int slot = (int) (chunkPointer % maxRecordsInPage);

    try {
      final MutablePage page = db.getTransaction()
          .getPageToModify(new PageId(db, bucketId, pageNumber), pageSize, false);
      // PAGE_RECORD_TABLE_OFFSET == PAGE_RECORD_COUNT_IN_PAGE_OFFSET(0) + SHORT_SERIALIZED_SIZE; one uint per slot.
      final int recordOffset = (int) page.readUnsignedInt(
          Binary.SHORT_SERIALIZED_SIZE + slot * Binary.INT_SERIALIZED_SIZE);

      assertThat(page.readByte(recordOffset))
          .as("chunk %d/%d must carry the expected chunk marker", pageNumber, slot).isEqualTo(marker);

      final int pointerOffset = recordOffset + 1 + Binary.INT_SERIALIZED_SIZE;
      final long previous = page.readLong(pointerOffset);
      if (newValue != null)
        page.writeLong(pointerOffset, newValue);
      return previous;
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }
}
