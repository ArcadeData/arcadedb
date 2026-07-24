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
package com.arcadedb;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Regression test: a multi-page record whose chunk chain is structurally broken (a dangling continuation pointer) must
 * be removable. Before the fix such a record was undeletable by every path: a direct {@code DELETE} first failed while
 * lazy-loading the body for index cleanup (loadMultiPageRecord threw {@link ConcurrentModificationException}), and even
 * after that, the physical free walked the chain and threw the same #4932 retry signal on the broken link
 * ("chunk 0 was modified concurrently"). {@code CHECK DATABASE FIX} could not help either, because it never walked the
 * continuation chain and its own fix-delete funnels through the same guard.
 *
 * <p>The fix adds a {@code force} delete mode that stops at a broken link instead of aborting (freeing the head slot and
 * leaving unreachable chunks for compaction to reclaim), makes {@code CHECK DATABASE} detect a broken chain, and wires
 * its fix-delete to use force. The default (non-force) delete keeps the #4932 retry behaviour for transient conflicts.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BrokenMultiPageRecordDeleteTest extends TestHelper {

  private static final String TYPE = "LargeRecord";

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // This test deliberately injects on-disk corruption, which the post-test integrity check would (correctly) flag.
    return false;
  }

  @Test
  void checkDatabaseFixRemovesBrokenMultiPageRecord() {
    final RID broken = createBrokenMultiPageVertex();

    // The broken record is still allocated and would otherwise stay forever.
    assertThat(database.countType(TYPE, false)).isEqualTo(3L);

    // A plain check must SEE the broken chain (it used to be invisible - only the first chunk's on-page size was
    // validated), but must not remove anything.
    ResultSet result = database.command("sql", "check database");
    Result row = result.next();
    assertThat(((Collection<?>) row.getProperty("warnings")).size()).isGreaterThanOrEqualTo(1);
    assertThat(row.<Collection<?>>getProperty("warnings").toString()).contains("broken multi-page chunk chain");
    assertThat(database.countType(TYPE, false)).isEqualTo(3L);

    // CHECK DATABASE FIX must force-remove the broken record.
    result = database.command("sql", "check database fix");
    row = result.next();
    assertThat(((Collection<?>) row.getProperty("deletedRecordsAfterFix")).contains(broken)).isTrue();
    assertThat(database.getSchema().getBucketById(broken.getBucketId()).existsRecord(broken)).isFalse();
    assertThat(database.countType(TYPE, false)).isEqualTo(2L);

    // A follow-up check must now be clean.
    result = database.command("sql", "check database");
    row = result.next();
    assertThat(row.<Collection<?>>getProperty("warnings").toString()).doesNotContain("broken multi-page chunk chain");
  }

  @Test
  void forceDeleteRemovesBrokenMultiPageRecordWhilePlainDeleteRetries() {
    final RID broken = createBrokenMultiPageVertex();
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(broken.getBucketId());

    // The structural probe (used to gate the tolerant delete path) must flag the broken chain, and must NOT flag an
    // intact record: a false positive there would let a DELETE under contention skip index cleanup on a healthy record.
    database.transaction(() -> {
      assertThat(bucket.isChunkChainBroken(broken)).isTrue();
      final RID intact = new RID(broken.getBucketId(), 1L); // the first small vertex, same bucket
      assertThat(bucket.isChunkChainBroken(intact)).isFalse();
    });

    // The default (non-force) physical delete must still raise the #4932 retry signal on the broken chunk chain, so
    // transient conflicts keep retrying instead of silently orphaning chunks.
    database.begin();
    try {
      bucket.deleteRecord(broken);
      fail("Expected ConcurrentModificationException on deleting a record with a broken chunk chain");
    } catch (final ConcurrentModificationException expected) {
      // EXPECTED: broken chain surfaces as the retry signal without force.
    } finally {
      database.rollback();
    }
    assertThat(database.getSchema().getBucketById(broken.getBucketId()).existsRecord(broken)).isTrue();

    // The force delete removes the stuck record. existsRecord is the authoritative physical check; countType is not
    // used as the oracle here because this low-level bucket.deleteRecord bypasses the database-level record counter.
    database.transaction(() -> bucket.deleteRecord(broken, true));
    assertThat(database.getSchema().getBucketById(broken.getBucketId()).existsRecord(broken)).isFalse();

    // A CHECK DATABASE FIX afterwards reconciles the counter and finds nothing else to repair.
    final Result row = database.command("sql", "check database fix").next();
    assertThat(row.<Collection<?>>getProperty("warnings").toString()).doesNotContain("broken multi-page chunk chain");
    assertThat(database.countType(TYPE, false)).isEqualTo(2L);
  }

  /**
   * Creates a genuine multi-page vertex at position 0, then corrupts the FIRST_CHUNK's continuation pointer so the
   * chunk chain is broken at chunk 0 (points to a page well beyond the file). Reopens so the record is re-read from the
   * corrupted page, not the in-memory cache. Returns the RID of the broken record.
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
   * Overwrites the next-chunk pointer of the FIRST_CHUNK at page 0 / slot 0 with a value pointing to a page far beyond
   * the file, breaking the chain at chunk 0.
   */
  private void corruptFirstChunkPointer(final int bucketId) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketId)).getPageSize();
    final int maxRecordsInPage = ((LocalBucket) db.getSchema().getBucketById(bucketId)).getMaxRecordsInPage();

    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, bucketId, 0), pageSize, false);
        // PAGE_RECORD_TABLE_OFFSET == PAGE_RECORD_COUNT_IN_PAGE_OFFSET(0) + SHORT_SERIALIZED_SIZE; slot 0 holds the
        // content-relative offset of the first record.
        final int recordOffset = (int) page.readUnsignedInt(Binary.SHORT_SERIALIZED_SIZE);

        // Confirm the record really is a multi-page head: FIRST_CHUNK (-2) is zigzag-encoded as the single byte 0x03.
        assertThat(page.readByte(recordOffset)).as("record must be a multi-page FIRST_CHUNK").isEqualTo((byte) 3);

        // Layout after the marker: [chunkSize:int][nextChunkPointer:long][content...]. Point the continuation to a page
        // that does not exist so the chain is broken at chunk 0.
        final int nextChunkPointerOffset = recordOffset + 1 + Binary.INT_SERIALIZED_SIZE;
        page.writeLong(nextChunkPointerOffset, 1_000_000L * maxRecordsInPage);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
