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
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5067 (item 3): {@code LSMTreeIndexMutable.createNewPage()} increments
 * {@code currentMutablePages} eagerly inside the transaction, so a rollback discards the pages but not
 * the increments. The counter drifted above the real page count for the rest of the run (it only
 * self-corrected on reload after #5063's onAfterLoad fix), inflating the auto-compaction scheduling
 * check in {@code onAfterCommit}. The fix clamps the counter to the real page count at commit time.
 */
class Issue5067MutablePagesCounterDriftTest extends TestHelper {

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType("Issue5067Doc");
    type.createProperty("id", Type.STRING);
    database.getSchema().buildTypeIndex("Issue5067Doc", new String[] { "id" }).withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false).withPageSize(4_096).create();
  }

  @Test
  void rolledBackPageCreationsDoNotDriftTheMutablePagesCounter() {
    final TypeIndex typeIndex = database.getSchema().getType("Issue5067Doc").getAllIndexes(false).iterator().next();
    final LSMTreeIndex lsmIndex = (LSMTreeIndex) typeIndex.getIndexesOnBuckets()[0];
    final LSMTreeIndexMutable mutable = lsmIndex.getMutableIndex();
    final int bucketFileId = database.getSchema().getType("Issue5067Doc").getBuckets(false).getFirst().getFileId();

    // Create index pages inside a transaction that rolls back: put directly on the mutable index (the
    // regular record path defers index writes to commit) so createNewPage runs, then discard everything.
    database.begin();
    try {
      for (int i = 0; i < 2_000; i++)
        mutable.put(new Object[] { "key-" + i }, new RID[] { new RID(bucketFileId, i) });
    } finally {
      database.rollback();
    }

    // Precondition documenting the drift: the rolled-back page creations left the counter above the
    // real (persistent) page count.
    assertThat(mutable.getCurrentMutablePages())
        .as("rolled-back page creations must have drifted the counter for this test to be meaningful")
        .isGreaterThan(mutable.getTotalPages());

    // A committed write to the index triggers onAfterCommit, which must re-align the counter with the
    // real page count instead of feeding the drifted value to the compaction scheduling check.
    database.transaction(() -> database.newDocument("Issue5067Doc").set("id", "committed").save());

    assertThat(mutable.getCurrentMutablePages())
        .as("after a commit the counter must not exceed the real page count")
        .isLessThanOrEqualTo(mutable.getTotalPages());
  }
}
