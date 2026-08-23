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
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Follow-up regression for https://github.com/ArcadeData/arcadedb/issues/6592: once a composite-index prefix
 * scan is answered with a range() cursor, {@link LSMTreeIndexCursor#getClosestEntryInTx} finds the single
 * closest pending (uncommitted) key with one floor/ceiling/higher/lower lookup and used to give up entirely
 * when THAT key turned out to be fully removed (every pending change on it a REMOVE) - e.g. a record inserted
 * and deleted again within the same transaction. Instead of walking to the next candidate key in the same
 * direction, exactly like the on-page cursor skips a fully-tombstoned key via advanceCursor()+continue, the
 * overlay was left empty for the rest of the scan: a prefix range scan whose ORDER BY DESC starts from the
 * just-deleted top of the group came back with nothing, even though older rows of the same group were still
 * live and pending in the very same transaction.
 * <p>
 * A second, narrower defect lived in the same loop: on a non-unique index, a key can carry BOTH a live insert
 * (a different RID) and a REMOVE in the same transaction. The old code {@code break}-ed the whole per-key
 * values loop on the first REMOVE it saw (map iteration order is unspecified), silently dropping the sibling
 * live RID too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6592ClosestEntryInTxSkipsTombstonedKeyTest extends TestHelper {
  private static final String TYPE_NAME   = "Supplier";
  private static final int    GROUP_SIZE  = 10;

  private TypeIndex createCompositeIndex() {
    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("key1", String.class);
    type.createProperty("key2", String.class);
    type.createProperty("orderedAt", Long.class);
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "key1", "key2", "orderedAt" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
    return database.getSchema().getType(TYPE_NAME).getPolymorphicIndexByProperties("key1", "key2", "orderedAt");
  }

  private static long countAndAssertOrder(final IndexCursor cursor, final boolean descending) {
    long previous = -1;
    long count = 0;
    try {
      while (cursor.hasNext()) {
        cursor.next();
        // getKeys() describes the entry next() LAST RETURNED (see LSMTreeIndexCursor's class docs, #5635).
        final long orderedAt = (Long) cursor.getKeys()[2];
        if (count > 0)
          assertThat(descending ? orderedAt < previous : orderedAt > previous)
              .as("keys must come out strictly %s (got %d after %d)", descending ? "descending" : "ascending", orderedAt, previous)
              .isTrue();
        previous = orderedAt;
        ++count;
      }
    } finally {
      cursor.close();
    }
    return count;
  }

  @Test
  void descendingPrefixScanSkipsTheJustDeletedTopOfTheGroupWhileUncommitted() {
    final TypeIndex index = createCompositeIndex();

    database.begin();
    try {
      // INSERT THE WHOLE GROUP AND DELETE ITS TOP - THE ENTRY A DESCENDING SCAN STARTS FROM - ALL WITHIN THE SAME
      // STILL-OPEN TRANSACTION, SO THE ENTRY EXISTS ONLY AS A PENDING INSERT+REMOVE PAIR IN THE TX OVERLAY, NEVER
      // ON ANY PAGE
      RID top = null;
      for (int i = 0; i < GROUP_SIZE; ++i) {
        final RID rid = database.newDocument(TYPE_NAME).set("key1", "a", "key2", "b", "orderedAt", (long) i).save().getIdentity();
        if (i == GROUP_SIZE - 1)
          top = rid;
      }
      database.deleteRecord(top.getRecord());

      final IndexCursor cursor = index.range(false, new Object[] { "a", "b" }, true, new Object[] { "a", "b" }, true);
      final long count = countAndAssertOrder(cursor, true);

      assertThat(count).as("the delete removed exactly one of the %d rows in the group", GROUP_SIZE).isEqualTo(GROUP_SIZE - 1);
    } finally {
      database.rollback();
    }
  }

  @Test
  void ascendingPrefixScanSkipsTheJustDeletedBottomOfTheGroupWhileUncommitted() {
    final TypeIndex index = createCompositeIndex();

    database.begin();
    try {
      // MIRROR CASE: DELETE THE BOTTOM OF THE GROUP, THE ENTRY AN ASCENDING SCAN STARTS FROM
      RID bottom = null;
      for (int i = 0; i < GROUP_SIZE; ++i) {
        final RID rid = database.newDocument(TYPE_NAME).set("key1", "a", "key2", "b", "orderedAt", (long) i).save().getIdentity();
        if (i == 0)
          bottom = rid;
      }
      database.deleteRecord(bottom.getRecord());

      final IndexCursor cursor = index.range(true, new Object[] { "a", "b" }, true, new Object[] { "a", "b" }, true);
      final long count = countAndAssertOrder(cursor, false);

      assertThat(count).as("the delete removed exactly one of the %d rows in the group", GROUP_SIZE).isEqualTo(GROUP_SIZE - 1);
    } finally {
      database.rollback();
    }
  }

  @Test
  void nonUniqueKeySharedByALiveInsertAndARemoveKeepsTheLiveOne() {
    final TypeIndex index = createCompositeIndex();

    database.begin();
    try {
      // TWO DOCUMENTS SHARE THE EXACT SAME COMPOSITE KEY (NON-UNIQUE INDEX): INSERT BOTH, THEN DELETE ONLY ONE OF
      // THEM, ALL WITHIN THIS SAME UNCOMMITTED TRANSACTION - THE KEY'S PENDING CHANGES NOW MIX A REMOVE (FOR THE
      // DELETED RID) WITH A LIVE INSERT (FOR THE SURVIVING RID)
      final RID toDelete = database.newDocument(TYPE_NAME).set("key1", "a", "key2", "b", "orderedAt", 0L).save().getIdentity();
      database.newDocument(TYPE_NAME).set("key1", "a", "key2", "b", "orderedAt", 0L).save();
      database.deleteRecord(toDelete.getRecord());

      final IndexCursor cursor = index.range(false, new Object[] { "a", "b" }, true, new Object[] { "a", "b" }, true);
      try {
        assertThat(cursor.hasNext()).as("the surviving sibling under the same key must still be found").isTrue();
        cursor.next();
        assertThat(cursor.hasNext()).as("only the deleted RID is gone, not the whole key").isFalse();
      } finally {
        cursor.close();
      }
    } finally {
      database.rollback();
    }
  }
}
