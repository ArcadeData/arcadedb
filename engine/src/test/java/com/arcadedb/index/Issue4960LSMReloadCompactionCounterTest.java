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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4960: {@code LSMTreeIndexMutable.onAfterLoad()} hardcoded
 * {@code currentMutablePages = 1} regardless of how many uncompacted pages the reloaded file actually
 * contains. After a restart, an index that had accumulated N pages (but had not yet crossed the
 * auto-compaction threshold) restarted its counter at 1, deferring the scheduled compaction by up to a
 * full threshold's worth of new pages every restart.
 */
class Issue4960LSMReloadCompactionCounterTest extends TestHelper {
  private static final String TYPE_NAME = "ReloadItem";
  private static final int    PAGE_SIZE = 4096;

  @Test
  void reloadedIndexRestoresMutablePageCountFromFile() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("id", Integer.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(PAGE_SIZE).create();
    });

    final LSMTreeIndexMutable before = bucketMutableIndex();
    final int bucketId = database.getSchema().getType(TYPE_NAME).getFirstBucketId();

    // Enough entries to span several 4K pages, but below the auto-compaction threshold (default 10)
    // so no compaction interferes with the counter.
    database.transaction(() -> {
      for (int i = 0; i < 1200; ++i)
        before.put(new Object[] { i }, new RID[] { new RID(bucketId, i) });
    });

    final int pagesBeforeReopen = before.getTotalPages();
    assertThat(pagesBeforeReopen).as("the index must span multiple pages for the test to be meaningful")
        .isGreaterThan(1);

    reopenDatabase();

    final LSMTreeIndexMutable after = bucketMutableIndex();
    assertThat(after.getTotalPages()).isEqualTo(pagesBeforeReopen);
    assertThat(after.getCurrentMutablePages())
        .as("""
            after a reload the mutable-pages counter must track the file's page count (approximate: \
            getTotalPages() includes the header/root page, which is fine for a pacing-only signal), \
            otherwise auto-compaction is deferred by a full threshold every restart""")
        .isEqualTo(after.getTotalPages());
  }

  private LSMTreeIndexMutable bucketMutableIndex() {
    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("id").getFirst();
    for (final Index bucketIndex : typeIndex.getIndexesOnBuckets())
      if (bucketIndex instanceof LSMTreeIndex lsmIndex)
        return lsmIndex.getMutableIndex();
    throw new IllegalStateException("No LSM bucket index found for type " + TYPE_NAME);
  }
}
