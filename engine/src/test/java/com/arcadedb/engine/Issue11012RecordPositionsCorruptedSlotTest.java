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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.arcadedb.database.Binary.INT_SERIALIZED_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11012: {@link LocalBucket#forEachRecordPosition} answers the positions a scan of the bucket hands out, so a slot
 * whose record-table entry is corrupted is skipped, as {@link BucketIterator} skips it, instead of aborting the walk.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11012RecordPositionsCorruptedSlotTest extends TestHelper {

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // The test injects the corruption on purpose
    return false;
  }

  @Test
  void aCorruptedSlotIsSkippedAndTheOtherPositionsAreAnswered() {
    final DocumentType type = database.getSchema().createDocumentType("Positions", 1);
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        rids.add(database.newDocument("Positions").set("id", i).save().getIdentity());
    });

    final LocalBucket bucket = (LocalBucket) type.getBuckets(false).get(0);
    database.transaction(() -> {
      try {
        final MutablePage page = ((DatabaseInternal) database).getTransaction()
            .getPageToModify(new PageId((DatabaseInternal) database, bucket.getFileId(), 0), bucket.getPageSize(), false);
        // THE SECOND SLOT POINTS FAR PAST THE END OF THE PAGE
        page.writeUnsignedInt(LocalBucket.PAGE_RECORD_TABLE_OFFSET + INT_SERIALIZED_SIZE, (long) bucket.getPageSize() * 1000);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });

    final List<Long> positions = new ArrayList<>();
    database.transaction(() -> bucket.forEachRecordPosition(positions::add));
    assertThat(positions).containsExactly(rids.get(0).getPosition(), rids.get(2).getPosition());
  }
}
