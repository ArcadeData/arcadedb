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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for #12391: the automatically sized search cache of every vector index planned on the whole percent of the heap
 * free when it grew, and at least 8,192 entries, so 48 bucket caches of a 3 GB heap planned on 3.4 GB and filled 2.1 GB of it.
 * The percent is now the database's: what the other indexes' caches plan on is charged first.
 */
class Issue12391SearchCacheSharedBudgetTest extends TestHelper {
  private static final int DIMENSIONS = 32;

  @Test
  void theSearchCachesOfTheDatabaseShareOnePercentOfTheHeap() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(2).create()
          .createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\" }");
    });
    final IndexInternal[] buckets = ((TypeIndex) database.getSchema().getIndexByName("Doc[embedding]")).getIndexesOnBuckets();
    assertThat(buckets).hasSize(2);
    final LSMVectorIndex index = (LSMVectorIndex) buckets[0];
    final LSMVectorIndex sibling = (LSMVectorIndex) buckets[1];
    final long bytesPerEntry = FlatVectorCache.bytesPerVector(DIMENSIONS);

    assertThat(index.affordableSearchCacheEntries(1, bytesPerEntry, 0L, false))
        .as("the database's first cache keeps the floor, even with nothing free").isEqualTo(8_192);

    final FlatVectorCache siblingCache = sibling.getSearchVectorCache();
    assertThat(siblingCache).as("precondition: the other bucket's index has a search cache").isNotNull();
    final long siblingPlans = siblingCache.capacity() * bytesPerEntry;
    final long siblingHolds = siblingCache.allocatedBytes();

    // 1% of what is free plus what the sibling holds is what the sibling plans on, plus 30 entries
    final long available = (siblingPlans + 30 * bytesPerEntry) * 100 - siblingHolds;
    assertThat(index.affordableSearchCacheEntries(1, bytesPerEntry, available, false))
        .as("what the other index's cache plans on is charged to the same budget").isEqualTo(30);

    assertThat(index.affordableSearchCacheEntries(1, bytesPerEntry, siblingPlans * 100 - siblingHolds, false))
        .as("a budget the other caches have taken leaves this one none, not the floor").isZero();
  }

  /**
   * A BINARY index holds two search caches, the vectors and the one-bit codes: sizing one of them charges the other, and
   * only the one being sized is left out.
   */
  @Test
  void aBinaryIndexChargesItsOtherCacheToTheSameBudget() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("Bin").withTotalBuckets(1).create()
          .createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.command("sql", "CREATE INDEX ON Bin (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\", \"quantization\": \"BINARY\" }");
    });
    final LSMVectorIndex index = (LSMVectorIndex) ((TypeIndex) database.getSchema().getIndexByName("Bin[embedding]"))
        .getIndexesOnBuckets()[0];
    final FlatVectorCache vectors = index.getSearchVectorCache();
    assertThat(vectors).as("precondition: the index has a vector cache").isNotNull();
    final long vectorsPlan = vectors.capacity() * FlatVectorCache.bytesPerVector(DIMENSIONS);
    final long vectorsHold = vectors.allocatedBytes();
    final long bytesPerCode = BinaryCodeCache.bytesPerCode(DIMENSIONS);

    final long available = (vectorsPlan + 30 * bytesPerCode) * 100 - vectorsHold;
    assertThat(index.affordableSearchCacheEntries(1, bytesPerCode, available, true))
        .as("the codes are charged the vector cache of the same index").isEqualTo(30);
    assertThat(index.affordableSearchCacheEntries(1, FlatVectorCache.bytesPerVector(DIMENSIONS), 0L, false))
        .as("the vector cache being sized is not charged to itself: alone in the database, it keeps the floor")
        .isEqualTo(8_192);
  }
}
