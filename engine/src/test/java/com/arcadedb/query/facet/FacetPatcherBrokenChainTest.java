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
package com.arcadedb.query.facet;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.exception.BrokenChunkChainException;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.columnar.FacetOracle;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A record the projection's readers ({@link FacetPatcher#recordAt}: the facet patch and fallback paths, and the accelerated GROUP BY's
 * pending pages) read with a broken chunk chain fails the statement with a retryable error that still carries the original diagnosis:
 * on a replica the chain may be read while a replicated apply is still writing it (#10941), and the next attempt reads it whole, while
 * a chain that stays broken surfaces once the client's bounded retries are spent, with its CHECK DATABASE FIX hint. A record that is
 * not there stays "no record" (a delete), and a healthy one is read.
 */
class FacetPatcherBrokenChainTest extends TestHelper {

  private static final String TYPE = "LargeAsset";

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // the test injects on-disk corruption, which the post-test integrity check would (correctly) flag
    return false;
  }

  @Test
  void brokenChunkChainIsARetryableErrorKeepingTheDiagnosis() {
    final int bucketId = createType(false);
    final RID[] rids = insertRecords();
    corruptFirstChunkPointer(bucketId);
    reopenDatabase();
    final DatabaseInternal db = (DatabaseInternal) database;
    assertThat(((LocalBucket) db.getSchema().getBucketById(bucketId)).isChunkChainBroken(rids[0]))
        .as("precondition: the chain of the first record is confirmed broken").isTrue();

    db.transaction(() -> assertThatThrownBy(() -> FacetPatcher.recordAt(db, rids[0]))//
        .isInstanceOf(ConcurrentModificationException.class).isInstanceOf(NeedRetryException.class)
        .hasMessageContaining(rids[0].toString()).hasMessageContaining("replicated apply")
        .hasMessageContaining("Run CHECK DATABASE FIX to repair it").hasCauseInstanceOf(BrokenChunkChainException.class));
  }

  @Test
  void absentRecordIsNoRecordAndHealthyRecordIsRead() {
    final int bucketId = createType(false);
    final RID[] rids = insertRecords();
    corruptFirstChunkPointer(bucketId);
    reopenDatabase();
    final DatabaseInternal db = (DatabaseInternal) database;

    db.transaction(() -> {
      assertThat(FacetPatcher.recordAt(db, rids[1]).getInteger("id")).isEqualTo(1);
      // a position past the last record of the bucket: nothing there, as after a delete
      assertThat(FacetPatcher.recordAt(db, new RID(rids[1].getBucketId(), 1_000L))).isNull();
    });
  }

  /**
   * The accelerated GROUP BY reads the records of the pending pages through {@link FacetPatcher#recordAt}: a broken chain there fails the
   * statement with the same retryable error. Only the accelerated path can raise it: the normal plan's scan skips an unreadable record.
   */
  @Test
  void acceleratedGroupByReadingABrokenChainOnAPendingPageIsARetryableError() throws Exception {
    final int bucketId = createType(true);
    final RID[] rids = insertRecords();
    FacetOracle.awaitCaughtUp(database);
    final String sql = "SELECT brand, count(*) FROM " + TYPE + " GROUP BY brand";
    assertThat(groupCount(sql)).as("precondition: the healthy statement answers").isEqualTo(1L);

    // the corrupting commit marks page 0 pending, and the suspended applier keeps it pending: the step patches it from its records
    try (final AutoCloseable hold = ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager()
        .suspendApply()) {
      corruptFirstChunkPointer(bucketId);
      assertThatThrownBy(() -> groupCount(sql)).satisfies(failure -> {
        Throwable cause = failure;
        while (cause != null && !(cause instanceof ConcurrentModificationException))
          cause = cause.getCause();
        assertThat(cause).as("a retryable error from the pending-page read: " + failure).isInstanceOf(ConcurrentModificationException.class)
            .hasMessageContaining(rids[0].toString()).hasMessageContaining("Run CHECK DATABASE FIX to repair it")
            .hasCauseInstanceOf(BrokenChunkChainException.class);
      });
    }
  }

  /** @return the number of groups the statement answers */
  private long groupCount(final String sql) {
    long groups = 0L;
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext()) {
        rs.next();
        groups++;
      }
    }
    return groups;
  }

  /** @return the id of the type's only bucket */
  private int createType(final boolean columnarBrand) {
    final int[] bucketId = new int[1];
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE, 1);
      type.createProperty("id", Type.INTEGER);
      type.createProperty("brand", Type.STRING);
      type.createProperty("data", Type.STRING);
      bucketId[0] = type.getBuckets(false).getFirst().getFileId();
    });
    if (columnarBrand)
      database.command("sql", "CREATE INDEX ON " + TYPE + " (brand) COLUMNAR");
    return bucketId[0];
  }

  /** Inserts a multi-page record at position 0 and a small one after it. */
  private RID[] insertRecords() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int bucketId = db.getSchema().getType(TYPE).getBuckets(false).getFirst().getFileId();
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketId)).getPageSize();
    final RID[] rids = new RID[2];
    db.transaction(() -> {
      rids[0] = db.newDocument(TYPE).set("id", 0).set("brand", "b").set("data", "x".repeat(pageSize * 4)).save().getIdentity();
      rids[1] = db.newDocument(TYPE).set("id", 1).set("brand", "b").set("data", "small").save().getIdentity();
    });
    assertThat(rids[0].getPosition()).isZero();
    return rids;
  }

  /**
   * Points the first chunk's continuation of the record at page 0 / slot 0 to a page beyond the file, breaking its chain at chunk 0
   * (technique of {@code BucketIteratorBrokenChunkChainTest}).
   */
  private void corruptFirstChunkPointer(final int bucketId) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(bucketId)).getPageSize();
    final int maxRecordsInPage = ((LocalBucket) db.getSchema().getBucketById(bucketId)).getMaxRecordsInPage();
    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, bucketId, 0), pageSize, false);
        final int recordOffset = (int) page.readUnsignedInt(Binary.SHORT_SERIALIZED_SIZE);
        assertThat(page.readByte(recordOffset)).as("record must be a multi-page FIRST_CHUNK").isEqualTo((byte) 3);
        page.writeLong(recordOffset + 1 + Binary.INT_SERIALIZED_SIZE, 1_000_000L * maxRecordsInPage);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
