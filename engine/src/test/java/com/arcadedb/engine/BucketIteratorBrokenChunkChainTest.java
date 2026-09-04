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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the third round of code review on #6015's fix: a {@code ConcurrentModificationException}
 * from {@code LocalBucket.loadMultiPageRecord()} is not proof of corruption by itself - it also fires after
 * {@code TX_RETRIES} exhausted retries under ordinary transient contention on a healthy record (see
 * {@code LocalDatabase.deleteRecordInternal}'s identical disambiguation, and its precedent test
 * {@code BrokenMultiPageRecordDeleteTest}). {@code BucketIterator.fetchNext()} must therefore not treat every
 * such exception as "known corruption, skip and count" - only one that {@link LocalBucket#isChunkChainBroken}
 * confirms is a genuinely broken chain. This test reproduces the corrupted-chain half of that disambiguation:
 * the healthy half (a non-broken chain must never report broken, so a transient conflict keeps propagating) is
 * covered by {@code BrokenMultiPageRecordDeleteTest.forceDeleteRemovesBrokenMultiPageRecordWhilePlainDeleteRetries}'s
 * existing assertion on {@code isChunkChainBroken}, which both that test and this one's production code path
 * share.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BucketIteratorBrokenChunkChainTest extends TestHelper {

  private static final String TYPE = "LargeRecord";

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // This test deliberately injects on-disk corruption, which the post-test integrity check would (correctly) flag.
    return false;
  }

  @Test
  void confirmedBrokenChunkChainIsSkippedNotPropagated() {
    final RID broken = createBrokenMultiPageVertex();

    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(broken.getBucketId());
    assertThat(bucket.isChunkChainBroken(broken))
        .as("sanity check: the corruption must actually be a confirmed-broken chain for this test to be meaningful")
        .isTrue();

    // REPEATABLE_READ forces database.lookupByRID() to eagerly load content (and so eagerly call
    // loadMultiPageRecord()) inside BucketIterator.fetchNext(), the same shape needed to actually exercise the
    // catch under test: under the default READ_COMMITTED, lookupByRID(rid, false) returns a lazy record shell
    // and never touches the broken chain during the scan at all.
    database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    try {
      database.transaction(() -> {
        final BucketIterator iterator = (BucketIterator) bucket.iterator();
        int count = 0;
        while (iterator.hasNext()) {
          iterator.next();
          count++;
        }

        assertThat(count).as("the two healthy records must still be returned, the scan must not abort").isEqualTo(2);
        assertThat(iterator.getSkippedRecordCount())
            .as("the confirmed-broken chunk chain must be counted as skipped instead of aborting the scan")
            .isEqualTo(1);
      });
    } finally {
      database.setTransactionIsolationLevel(Database.TRANSACTION_ISOLATION_LEVEL.READ_COMMITTED);
    }
  }

  /**
   * Creates a genuine multi-page vertex at position 0, then corrupts the FIRST_CHUNK's continuation pointer so
   * the chunk chain is broken at chunk 0 (points to a page well beyond the file). Reopens so the record is
   * re-read from the corrupted page, not the in-memory cache. Returns the RID of the broken record. Adapted from
   * {@code com.arcadedb.BrokenMultiPageRecordDeleteTest}'s identical technique.
   */
  private RID createBrokenMultiPageVertex() {
    final DatabaseInternal db = (DatabaseInternal) database;

    final int[] bucketIdHolder = new int[1];
    db.transaction(() -> {
      final VertexType type = db.getSchema().createVertexType(TYPE, 1);
      type.createProperty("id", Type.INTEGER);
      type.createProperty("data", Type.STRING);
      bucketIdHolder[0] = type.getBuckets(false).get(0).getFileId();
    });

    final int bucketId = bucketIdHolder[0];
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketId)).getPageSize();

    // A payload several pages long guarantees a multi-page (FIRST_CHUNK) record spanning multiple chunks.
    final String bigData = "x".repeat(pageSize * 4);

    final RID[] rids = new RID[3];
    db.transaction(() -> {
      // Insert the big record FIRST so it lands at position 0 (page 0, slot 0).
      rids[0] = db.newVertex(TYPE).set("id", 0).set("data", bigData).save().getIdentity();
      rids[1] = db.newVertex(TYPE).set("id", 1).set("data", "small1").save().getIdentity();
      rids[2] = db.newVertex(TYPE).set("id", 2).set("data", "small2").save().getIdentity();
    });

    assertThat(rids[0].getPosition()).isEqualTo(0L);

    corruptFirstChunkPointer(bucketId);

    reopenDatabase();

    return rids[0];
  }

  /**
   * Overwrites the next-chunk pointer of the FIRST_CHUNK at page 0 / slot 0 with a value pointing to a page far
   * beyond the file, breaking the chain at chunk 0.
   */
  private void corruptFirstChunkPointer(final int bucketId) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketId)).getPageSize();
    final int maxRecordsInPage = ((LocalBucket) db.getSchema().getBucketById(bucketId)).getMaxRecordsInPage();

    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, bucketId, 0), pageSize, false);
        // PAGE_RECORD_TABLE_OFFSET == PAGE_RECORD_COUNT_IN_PAGE_OFFSET(0) + SHORT_SERIALIZED_SIZE; slot 0 holds
        // the content-relative offset of the first record.
        final int recordOffset = (int) page.readUnsignedInt(Binary.SHORT_SERIALIZED_SIZE);

        // Confirm the record really is a multi-page head: FIRST_CHUNK (-2) is zigzag-encoded as the single byte 0x03.
        assertThat(page.readByte(recordOffset)).as("record must be a multi-page FIRST_CHUNK").isEqualTo((byte) 3);

        // Layout after the marker: [chunkSize:int][nextChunkPointer:long][content...]. Point the continuation to
        // a page that does not exist so the chain is broken at chunk 0.
        final int nextChunkPointerOffset = recordOffset + 1 + Binary.INT_SERIALIZED_SIZE;
        page.writeLong(nextChunkPointerOffset, 1_000_000L * maxRecordsInPage);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
