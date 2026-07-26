/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4743: when a compacted sub-index could not be resolved on load,
 * {@code LSMTreeIndexMutable.onAfterLoad()} called {@code database.getSchema().dropIndex(...)}.
 * <p>
 * That "self-repair" was wrong twice over. On a Raft replica the schema write threw
 * {@code ServerIsNotTheLeaderException} ("Changes to the schema must be executed on the leader server"),
 * which masked the real error, was then retried as if transient and finally escalated the whole database
 * to a snapshot resync - the exact sequence the reporter's leader log showed right after a failed
 * compaction left the schema pointing at a file id that no longer existed. And on a single node it
 * silently DELETED a user index because one of its files failed to load, turning a reportable and
 * repairable condition into data loss.
 * <p>
 * The index must survive: the unreadable sub-index is detached, the remaining (mutable) content stays
 * queryable, and the operator is told to run REBUILD INDEX.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4743InvalidSubIndexTest extends TestHelper {
  private static final String TYPE_NAME  = "Address";
  private static final String INDEX_NAME = "Address[uid]";
  private static final int    TOTAL      = 2_000;

  @Test
  void unresolvableSubIndexDetachesInsteadOfDroppingTheIndex() throws Exception {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("uid", Integer.class);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "uid" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(4096).create();
    });

    final int bucketId = database.getSchema().getType(TYPE_NAME).getFirstBucketId();
    final LSMTreeIndex bucketIndex = bucketIndex();
    database.transaction(() -> {
      for (int i = 0; i < TOTAL; i++)
        bucketIndex.getMutableIndex().put(new Object[] { i }, new RID[] { new RID(bucketId, i) });
    });

    // Compact so a sub-index (compacted) file exists and page 0 of the mutable index records its file id.
    bucketIndex.scheduleCompaction();
    bucketIndex.compact();
    assertThat(bucketIndex.getMutableIndex().getSubIndex())
        .as("the compaction must have produced a sub-index for this test to mean anything").isNotNull();

    // Point the mutable index at a file id that does not exist, reproducing the reporter's
    // "File with id '445' was not found": the compaction created the file, the schema recorded it, and
    // the replication of that schema change never completed.
    final int missingFileId = 9_999;
    writeSubIndexFileId(bucketIndex.getMutableIndex(), missingFileId);

    reopenDatabase();

    // Before the fix the index was gone from the schema entirely (or, under HA, the apply blew up).
    assertThat(database.getSchema().existsIndex(INDEX_NAME))
        .as("a sub-index that fails to load must NOT delete the whole index").isTrue();

    final LSMTreeIndex reloaded = bucketIndex();
    assertThat(reloaded.getMutableIndex().getSubIndex())
        .as("the unreadable sub-index must be detached, not kept in a broken state").isNull();

    // The index keeps working on what it can still read, and it accepts new entries.
    database.transaction(() -> {
      reloaded.getMutableIndex().put(new Object[] { TOTAL }, new RID[] { new RID(bucketId, TOTAL) });
    });
    assertThat(reloaded.get(new Object[] { TOTAL }).hasNext())
        .as("a degraded index must still serve its mutable content").isTrue();
  }

  /**
   * Overwrites the SUB-INDEX FILE ID slot in page 0 of the mutable index. The layout matches
   * {@code LSMTreeIndexMutable.createNewPage()}: maxContentSize(4) + entriesCount(4) + mutableFlag(1) +
   * compactedPages(4), then the sub-index file id.
   */
  private void writeSubIndexFileId(final LSMTreeIndexMutable mutable, final int fileId) {
    final int offset = Integer.BYTES + Integer.BYTES + Byte.BYTES + Integer.BYTES;
    final DatabaseInternal db = (DatabaseInternal) database;
    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction()
            .getPageToModify(new PageId(db, mutable.getFileId(), 0), mutable.getPageSize(), false);
        page.writeInt(offset, fileId);
      } catch (final IOException e) {
        throw new IllegalStateException(e);
      }
    });
  }

  private LSMTreeIndex bucketIndex() {
    final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("uid").getFirst();
    for (final Index index : typeIndex.getIndexesOnBuckets())
      if (index instanceof LSMTreeIndex lsmIndex)
        return lsmIndex;
    throw new IllegalStateException("No LSM bucket index found for type " + TYPE_NAME);
  }
}
