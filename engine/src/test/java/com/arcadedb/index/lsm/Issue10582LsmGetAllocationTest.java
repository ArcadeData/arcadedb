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
package com.arcadedb.index.lsm;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10582: every {@code LSMTreeIndexMutable.get} built a {@code RidHashSet} for the tombstones it might meet - two arrays of 256
 * slots, about 3 KB - and nearly every lookup meets none. A UNIQUE index looks a key up once per inserted row, so a bulk insert paid
 * that per row (23.8% of all the bytes it allocated). The set now costs nothing until a tombstone is recorded.
 * <p>
 * Measured by allocation, which is exact per thread, and not by time, which is not. The bound is half of what the set alone used to
 * cost, so it cannot be met by anything that still builds the tables, and the rest of a lookup has room.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10582LsmGetAllocationTest extends TestHelper {
  private static final int ROWS    = 3_000;
  private static final int LOOKUPS = 2_000;

  /** The two arrays of the default {@code RidHashSet}: 256 ints and 256 longs, without the headers. */
  private static final long OLD_SET_BYTES = 256 * (Integer.BYTES + Long.BYTES);

  @Test
  void aLookupThatMeetsNoTombstoneDoesNotBuildTheTombstoneSet() throws IOException {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Row").createProperty("id", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Row", "id");
    });
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument("Row").set("id", i).save();
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Row[id]");
    final LSMTreeIndex bucketIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
    final LSMTreeIndexMutable mutable = bucketIndex.getMutableIndex();

    database.begin();
    try {
      // A KEY THAT BUCKET'S INDEX HOLDS: THE FIRST RECORD OF THE BUCKET IT IS ASSOCIATED WITH
      final String bucketName = database.getSchema().getBucketById(bucketIndex.getAssociatedBucketId()).getName();
      final int presentKey = database.iterateBucket(bucketName).next().asDocument().getInteger("id");
      try (final IndexCursor present = mutable.get(new Object[] { presentKey }, 2)) {
        assertThat(present.hasNext()).as("the key is in the index the lookup runs on").isTrue();
      }

      // THE KEY A UNIQUE CHECK OF A NEW ROW LOOKS UP: ABSENT FROM EVERY PAGE. THEN A PRESENT ONE
      assertThat(lookupAllocatedBytes(mutable, ROWS + 1_000_000)).as("an absent key, bytes per lookup").isLessThan(OLD_SET_BYTES / 2);
      assertThat(lookupAllocatedBytes(mutable, presentKey)).as("a present key, bytes per lookup").isLessThan(OLD_SET_BYTES / 2);
    } finally {
      database.rollback();
    }
  }

  /** Bytes the calling thread allocates per lookup of {@code key}, warmed up and averaged over {@link #LOOKUPS}. */
  private static long lookupAllocatedBytes(final LSMTreeIndexMutable mutable, final int key) throws IOException {
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    final long threadId = Thread.currentThread().threadId();
    final Object[] keys = new Object[] { key };

    for (int i = 0; i < LOOKUPS; i++)
      mutable.get(keys, 2).close();

    final long before = threads.getThreadAllocatedBytes(threadId);
    for (int i = 0; i < LOOKUPS; i++) {
      final IndexCursor cursor = mutable.get(keys, 2);
      cursor.close();
    }
    return (threads.getThreadAllocatedBytes(threadId) - before) / LOOKUPS;
  }
}
