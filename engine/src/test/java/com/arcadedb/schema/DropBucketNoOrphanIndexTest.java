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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the crash-consistency ordering in {@link LocalSchema#dropBucket}: dependent sub-indexes are dropped BEFORE the
 * bucket file. If the bucket file were deleted first, a crash mid-drop would leave an index file whose bucket is gone,
 * which reloads as a permanent orphan (associatedBucketId=-1) that breaks {@code REBUILD INDEX *} and its own toJSON().
 *
 * This test exercises the full, non-crash drop path and then reopens the database to prove no orphan index survives on
 * disk: after a clean drop + restart there must be no registered index pointing at a vanished bucket.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DropBucketNoOrphanIndexTest extends TestHelper {

  @Test
  void dropBucketWithIndexLeavesNoOrphanAfterReopen() {
    // Type with an indexed property; then add an extra bucket which is auto-indexed (issue #774).
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Order");
      type.createProperty("p1", Type.STRING);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "p1");
    });

    final String extraBucketName = "Order_extra";
    database.transaction(() -> {
      final Bucket bucket = database.getSchema().createBucket(extraBucketName);
      database.getSchema().getType("Order").addBucket(bucket);
    });

    final int extraBucketId = database.getSchema().getBucketByName(extraBucketName).getFileId();

    // Sanity: a sub-index is bound to the extra bucket before the drop.
    assertThat(countIndexesForBucket(extraBucketId)).as("extra bucket must have a sub-index before drop").isEqualTo(1);

    // Detach and drop the bucket. dropBucket must drop the sub-index before deleting the bucket file.
    database.transaction(() -> {
      final Bucket bucket = database.getSchema().getBucketByName(extraBucketName);
      database.getSchema().getType("Order").removeBucket(bucket);
      database.getSchema().dropBucket(extraBucketName);
    });

    assertThat(database.getSchema().existsBucket(extraBucketName)).as("bucket must be gone").isFalse();
    assertThat(countIndexesForBucket(extraBucketId)).as("no index may reference the dropped bucket").isEqualTo(0);

    // The decisive check: reopen the database and confirm no orphan index was left on disk. An orphan would come back
    // registered in indexMap with associatedBucketId=-1 (bucket file gone, index file present).
    reopenDatabase();

    // A TypeIndex wrapper legitimately reports associatedBucketId=-1 (it spans buckets); only the bucket-level
    // sub-indexes must have a valid association, matching how REBUILD INDEX * enumerates them.
    for (final Index idx : database.getSchema().getIndexes())
      if (!(idx instanceof TypeIndex))
        assertThat(idx.getAssociatedBucketId())
            .as("bucket sub-index '%s' must not be an orphan (associatedBucketId=-1) after reopen", idx.getName())
            .isNotEqualTo(-1);

    // And REBUILD INDEX * must run cleanly with no failed indexes.
    try (final ResultSet rs = database.command("sql", "REBUILD INDEX *")) {
      assertThat(rs.next().<java.util.List<String>>getProperty("failedIndexes")).isNull();
    }
  }

  private int countIndexesForBucket(final int bucketId) {
    int count = 0;
    for (final Index idx : database.getSchema().getIndexes())
      if (idx.getAssociatedBucketId() == bucketId)
        count++;
    return count;
  }
}
