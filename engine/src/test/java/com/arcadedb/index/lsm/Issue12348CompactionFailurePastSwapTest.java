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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12348: a compaction that failed AFTER it swapped the new mutable file in (an {@link OutOfMemoryError} formatting its
 * log line, for one) rolled its compacted file back as if nothing had been swapped: the first compaction dropped the file the
 * new mutable file already named, a later round rolled back the pages of the series the new mutable file no longer holds. The
 * index lost those entries, and every later open logged "Invalid sub-index ... File with id N was not found".
 * <p>
 * The failure is injected right after the swap with {@link LSMTreeIndex#setAfterSwapTestHook}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12348CompactionFailurePastSwapTest extends TestHelper {
  private static final String TYPE_NAME = "Event";
  private static final int    ROUND     = 2_000;

  @Test
  void aFirstCompactionFailingPastTheSwapKeepsTheFileTheNewMutableNames() throws Exception {
    createType();
    insert(0, ROUND);

    compactFailingPastTheSwap();

    assertCompleteAfterReopen(ROUND);
  }

  @Test
  void aLaterRoundFailingPastTheSwapKeepsTheSeriesItWrote() throws Exception {
    createType();
    insert(0, ROUND);
    assertThat(bucketIndex().scheduleCompaction()).isTrue();
    assertThat(bucketIndex().compact()).isTrue();
    assertThat(bucketIndex().getMutableIndex().getSubIndex()).isNotNull();

    insert(ROUND, ROUND);
    compactFailingPastTheSwap();

    assertCompleteAfterReopen(2 * ROUND);
  }

  private void compactFailingPastTheSwap() {
    final LSMTreeIndex index = bucketIndex();
    final LSMTreeIndexMutable before = index.getMutableIndex();
    assertThat(index.scheduleCompaction()).isTrue();
    LSMTreeIndex.setAfterSwapTestHook(() -> {
      throw new OutOfMemoryError("injected past the swap");
    });
    try {
      assertThatThrownBy(index::compact).isInstanceOf(OutOfMemoryError.class);
    } finally {
      LSMTreeIndex.setAfterSwapTestHook(null);
    }
    assertThat(index.getMutableIndex()).as("the failure must come after the swap for the test to mean anything").isNotSameAs(before);
    assertThat(index.getMutableIndex().getSubIndex()).isNotNull();
  }

  private void assertCompleteAfterReopen(final int keys) {
    assertThat(countByIndex(keys)).as("before the reopen").isEqualTo(keys);

    reopenDatabase();

    final LSMTreeIndexMutable mutable = bucketIndex().getMutableIndex();
    assertThat(mutable.getDetachedSubIndex()).as("the sub-index the new mutable file names must still be there").isNull();
    assertThat(mutable.getSubIndex()).isNotNull();
    assertThat(countByIndex(keys)).as("after the reopen").isEqualTo(keys);
  }

  private void createType() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("eid", Type.INTEGER);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "eid" }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true)
          .withPageSize(4_096).create();
    });
  }

  private void insert(final int from, final int count) {
    database.transaction(() -> {
      for (int i = from; i < from + count; i++)
        database.newDocument(TYPE_NAME).set("eid", i).save();
    });
  }

  private TypeIndex typeIndex() {
    return database.getSchema().getType(TYPE_NAME).getIndexesByProperties("eid").getFirst();
  }

  private LSMTreeIndex bucketIndex() {
    return (LSMTreeIndex) typeIndex().getIndexesOnBuckets()[0];
  }

  private long countByIndex(final int keys) {
    long found = 0;
    for (int i = 0; i < keys; i++)
      if (typeIndex().get(new Object[] { i }).hasNext())
        ++found;
    return found;
  }
}
