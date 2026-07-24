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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: a long, contiguous run of tombstoned keys must not trip the
 * "Detected infinite loop while iterating index" safety guard in {@link LSMTreeIndexCursor}.
 * <p>
 * The old guard budgeted iterations as {@code (totalCursors + 1) * 1024}: a heuristic based on the
 * number of page/series cursors, not on the amount of work a legitimate scan may have to do. Since
 * compaction never purges tombstones (2nd-level compaction is not implemented), a delete-heavy
 * workload (e.g. retention jobs on time-ordered data) accumulates thousands of consecutive dead
 * keys at the low end of the key space, exactly where an ascending range scan starts skipping.
 * Once the dead run exceeded the budget, every scan failed with a false "index may be corrupted"
 * error until the structure (page/series count) drifted enough to raise the budget again.
 * <p>
 * The guard now detects a genuine infinite loop exactly: each underlying cursor's consumed
 * (page, position) must advance strictly monotonically in scan direction. Tombstone runs of any
 * length are legal; a stuck cursor (like the historical DESC duplicate-group bug) is caught on its
 * first repeated consumption.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMTreeIndexCursorTombstoneRunTest extends TestHelper {
  private static final String TYPE_NAME = "ProcessExecution";
  private static final int    TOTAL     = 16_000;
  // ids [0, DELETED) are removed: a contiguous tombstone run far larger than the old guard budget
  private static final int    DELETED   = 15_000;

  @Override
  public void beforeTest() {
    // keep compaction manual so each test controls the index shape deterministically
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  @Test
  void scanOverLongTombstoneRunOnMutablePages() {
    final TypeIndex index = createTypeWithTombstoneRun();
    assertScanFindsOnlySurvivors(index);
  }

  @Test
  void scanOverLongTombstoneRunAfterCompaction() throws Exception {
    final TypeIndex index = createTypeWithTombstoneRun();
    assertThat(index.scheduleCompaction()).isTrue();
    assertThat(index.compact()).isTrue();
    assertScanFindsOnlySurvivors(index);
  }

  private TypeIndex createTypeWithTombstoneRun() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("id", Integer.class);
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withPageSize(64 * 1024).create();

    database.transaction(() -> {
      for (int i = 0; i < TOTAL; ++i)
        database.newDocument(TYPE_NAME).set("id", i).save();
    });

    // collect the victims with a bucket scan (no index range scan) and delete them
    final List<RID> toDelete = new ArrayList<>(DELETED);
    database.transaction(() -> {
      for (final Iterator<Record> it = database.iterateType(TYPE_NAME, false); it.hasNext(); ) {
        final Document doc = (Document) it.next();
        if (doc.getInteger("id") < DELETED)
          toDelete.add(doc.getIdentity());
      }
    });
    database.transaction(() -> {
      for (final RID rid : toDelete)
        database.deleteRecord(rid.getRecord());
    });

    return database.getSchema().getType(TYPE_NAME).getPolymorphicIndexByProperties("id");
  }

  private void assertScanFindsOnlySurvivors(final TypeIndex index) {
    database.transaction(() -> {
      // ascending: the scan starts right on the tombstone run
      assertThat(countEntries(index.range(true, new Object[] { 0 }, true, new Object[] { TOTAL }, true)))
          .isEqualTo(TOTAL - DELETED);
      // descending: the scan ends skipping through the tombstone run
      assertThat(countEntries(index.range(false, new Object[] { TOTAL }, true, new Object[] { 0 }, true)))
          .isEqualTo(TOTAL - DELETED);
    });
  }

  private static int countEntries(final IndexCursor cursor) {
    int count = 0;
    while (cursor.hasNext())
      if (cursor.next() != null)
        ++count;
    return count;
  }
}
