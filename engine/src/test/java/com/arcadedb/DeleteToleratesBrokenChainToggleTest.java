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
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Regression test for {@link GlobalConfiguration#DELETE_TOLERATE_BROKEN_CHAIN}: deleting a record whose own
 * multi-page chunk chain is structurally broken fails loudly by default, with the original
 * {@link ConcurrentModificationException}, so an operator has to run {@code CHECK DATABASE FIX} deliberately to
 * repair or remove it - that command's own force-delete is unaffected by this setting either way, so the record
 * is never permanently stuck. Explicitly enabling the setting restores the older behaviour: the delete completes
 * anyway (see {@link BrokenMultiPageRecordDeleteTest}), best-effort disconnecting a vertex's edges even if some
 * cannot be reached, which can leave dangling edges behind.
 *
 * <p>Reuses the exact corruption recipe from {@link BrokenMultiPageRecordDeleteTest}: a genuine multi-page vertex at
 * page 0/slot 0 whose FIRST_CHUNK continuation pointer is overwritten to point past the end of the file.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DeleteToleratesBrokenChainToggleTest extends TestHelper {

  private static final String TYPE = "LargeRecordToggle";

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // This test deliberately injects on-disk corruption, which the post-test integrity check would (correctly) flag.
    return false;
  }

  @Test
  void deleteFailsLoudlyByDefault() {
    // Disabled by default: no explicit configuration at all, same fixture, same delete - must now fail instead of
    // silently forcing through. This is the behaviour change from the default flip.
    final RID broken = createBrokenMultiPageVertex();

    database.begin();
    try {
      database.command("sql", "DELETE FROM " + broken);
      fail("Expected the delete to fail loudly instead of silently forcing through the broken chain");
    } catch (final ConcurrentModificationException expected) {
      // EXPECTED: disabled by default, the structurally broken chain surfaces instead of being masked.
    } finally {
      database.rollback();
    }

    // The record is untouched: the refused delete must not have force-removed it or any of its bytes.
    assertThat(database.getSchema().getBucketById(broken.getBucketId()).existsRecord(broken)).isTrue();
  }

  @Test
  void deleteForcesThroughWhenExplicitlyEnabled() {
    // Opting in restores the old (pre-flip) behaviour - this is the exact scenario
    // BrokenMultiPageRecordDeleteTest.directSqlDeleteRemovesBrokenMultiPageRecordNoIndex covers; repeated here so the
    // toggle's "off" (default) and "on" (opt-in) behaviours are asserted side by side in one file.
    final Object defaultValue = GlobalConfiguration.DELETE_TOLERATE_BROKEN_CHAIN.getDefValue();
    assertThat(defaultValue).as("this test documents the flip - update it if the default changes back").isEqualTo(false);

    final RID broken = createBrokenMultiPageVertex();

    database.getConfiguration().setValue(GlobalConfiguration.DELETE_TOLERATE_BROKEN_CHAIN, true);
    try {
      database.transaction(() -> database.command("sql", "DELETE FROM " + broken));
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.DELETE_TOLERATE_BROKEN_CHAIN, defaultValue);
    }

    assertThat(database.getSchema().getBucketById(broken.getBucketId()).existsRecord(broken)).isFalse();
  }

  /**
   * Creates a genuine multi-page vertex at position 0, then corrupts the FIRST_CHUNK's continuation pointer so the
   * chunk chain is broken at chunk 0 (points to a page well beyond the file). Reopens so the record is re-read from
   * the corrupted page, not the in-memory cache. Returns the RID of the broken record.
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

    final RID[] rids = new RID[2];
    db.transaction(() -> {
      // Insert the big record FIRST so it lands at position 0 (page 0, slot 0).
      rids[0] = db.newVertex(TYPE).set("id", 0).set("data", bigData).save().getIdentity();
      rids[1] = db.newVertex(TYPE).set("id", 1).set("data", "small1").save().getIdentity();
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
        final int recordOffset = (int) page.readUnsignedInt(Binary.SHORT_SERIALIZED_SIZE);

        // Confirm the record really is a multi-page head: FIRST_CHUNK (-2) is zigzag-encoded as the single byte 0x03.
        assertThat(page.readByte(recordOffset)).as("record must be a multi-page FIRST_CHUNK").isEqualTo((byte) 3);

        // Layout after the marker: [chunkSize:int][nextChunkPointer:long][content...]. Point the continuation to a
        // page that does not exist so the chain is broken at chunk 0.
        final int nextChunkPointerOffset = recordOffset + 1 + Binary.INT_SERIALIZED_SIZE;
        page.writeLong(nextChunkPointerOffset, 1_000_000L * maxRecordsInPage);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
