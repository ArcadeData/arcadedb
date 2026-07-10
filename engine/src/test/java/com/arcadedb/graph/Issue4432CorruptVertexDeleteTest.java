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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4432: a vertex record corrupted by an older version (issue #4319) could not be deleted on
 * 26.6.x. Reading the fixed 25-byte vertex prefix (edge head-chunk pointers) off a truncated buffer threw an unguarded
 * {@code java.nio.BufferUnderflowException} during index-key extraction on delete, which escaped command execution
 * (the tolerant-delete catch only handled {@code SerializationException}/{@code NegativeArraySizeException}).
 *
 * <p>This test corrupts the record-size varint of a vertex into a {@code FIRST_CHUNK} marker so the single-page record
 * is mis-read as a (truncated) multi-page record, then verifies the corrupted vertex can still be deleted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4432CorruptVertexDeleteTest extends TestHelper {

  private static final String TYPE = "AssetClassification";

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // This test deliberately injects on-disk corruption, which the post-test integrity check would (correctly) flag.
    return false;
  }

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType(TYPE);
      type.createProperty("tenantId", Type.STRING);
      type.createProperty("scheme", Type.STRING);
      type.createProperty("kind", Type.STRING);
      type.createProperty("code", Type.STRING);
      type.createProperty("name", Type.STRING);
      type.createProperty("parent", Type.STRING);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE, "tenantId", "scheme", "kind", "code");
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE, "tenantId", "scheme", "kind", "name");
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, TYPE, "tenantId", "parent", "name");
      database.getSchema().createEdgeType("hasParent");
    });
  }

  @Test
  void corruptedVertexCanStillBeDeleted() {
    final RID[] rids = new RID[5];
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        rids[i] = database.newVertex(TYPE).set("tenantId", "t1").set("scheme", "s").set("kind", "k")
            .set("code", "code" + i).set("name", "name" + i).set("parent", "name0").save().getIdentity();
      // An edge between two of them, mirroring the issue's data.
      database.lookupByRID(rids[1], true).asVertex().newEdge("hasParent", database.lookupByRID(rids[0], true).asVertex()).save();
    });

    // Corrupt the first vertex's record-size marker so it is mis-read as a (truncated) multi-page record.
    corruptRecordSizeToFirstChunk(rids[0]);

    // Reopen so records are re-read fresh from the corrupted page rather than the in-memory record cache.
    reopenDatabase();

    // The corrupted vertex (and the rest) must be deletable instead of failing with BufferUnderflowException (#4432).
    database.transaction(() -> database.command("sql", "DELETE VERTEX FROM " + TYPE));

    database.transaction(() -> assertThat(database.countType(TYPE, false)).isEqualTo(0L));
  }

  /**
   * Overwrites the record-size varint of the record at {@code rid} (page 0, slot 0) with the {@code FIRST_CHUNK}
   * marker (value -2, zigzag-encoded as the single byte 0x03). The bucket then treats the single-page record as the
   * head of a multi-page record and hands back a truncated buffer view.
   */
  private void corruptRecordSizeToFirstChunk(final RID rid) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = rid.getBucketId();
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();

    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, 0), pageSize, false);
        // PAGE_RECORD_TABLE_OFFSET == PAGE_RECORD_COUNT_IN_PAGE_OFFSET(0) + SHORT_SERIALIZED_SIZE; slot 0 holds the
        // content-relative offset of the first record, whose first byte is the record-size varint.
        final int recordTableOffset = Binary.SHORT_SERIALIZED_SIZE;
        final int recordOffset = (int) page.readUnsignedInt(recordTableOffset);
        page.writeByte(recordOffset, (byte) 3); // zigzag(-2) == 3  ->  FIRST_CHUNK
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
