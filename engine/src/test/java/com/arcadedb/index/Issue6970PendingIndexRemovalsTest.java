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
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #6970: the rules that decide which RIDs a pending transaction removal hides from a read used to be written out
 * three times ({@code LSMTreeIndex.get()}, {@code HashIndex.get()}, {@code LSMTreeIndexCursor.getClosestEntryInTx()})
 * and now live in {@link PendingIndexRemovals} (whose rules {@code PendingIndexRemovalsTest} pins on their own). This
 * class drives the point-lookup shapes that {@code Issue6927RangeScanTxRemovesTest} does not reach on the HASH and
 * LSM {@code get()} paths, so every caller of the helper is exercised by at least one test. Rule 3 ({@code REPLACE}
 * with an {@code oldRid}) through a point lookup is covered there, on LSM by
 * {@code rangeScanAndLookupAgreeOnAUniqueKeyReplacedInTheSameTransaction} and on HASH by
 * {@code hashIndexLookupHonoursAUniqueKeyReplacedInTheSameTransaction}. The third caller, the range cursor
 * ({@code LSMTreeIndexCursor}), is driven there too on unique and non-unique indexes, both scan directions.
 */
class Issue6970PendingIndexRemovalsTest extends TestHelper {

  // ─── CALLERS: POINT LOOKUPS NOT COVERED BY Issue6927RangeScanTxRemovesTest ─────────────

  @Test
  void hashIndexLookupHonoursAPerRidRemoveOnANonUniqueKey() {
    final List<RID> rids = createTwoRowsSharingAKey(Schema.INDEX_TYPE.HASH);

    database.begin();
    try {
      final Index index = typeIndex();
      ((IndexInternal) index).remove(new Object[] { 5 }, rids.get(0));
      assertThat(drain(index.get(new Object[] { 5 }))).containsExactly(rids.get(1));
    } finally {
      database.rollback();
    }
  }

  @Test
  void hashIndexLookupHonoursAKeyWideRemoveOnANonUniqueKey() {
    createTwoRowsSharingAKey(Schema.INDEX_TYPE.HASH);

    database.begin();
    try {
      final Index index = typeIndex();
      assertThat(drain(index.get(new Object[] { 5 }))).hasSize(2);
      ((IndexInternal) index).remove(new Object[] { 5 });
      assertThat(index.get(new Object[] { 5 }).hasNext()).isFalse();
    } finally {
      database.rollback();
    }
  }

  @Test
  void lsmIndexLookupHonoursAPerRidRemoveOnANonUniqueKey() {
    final List<RID> rids = createTwoRowsSharingAKey(Schema.INDEX_TYPE.LSM_TREE);

    database.begin();
    try {
      final Index index = typeIndex();
      ((IndexInternal) index).remove(new Object[] { 5 }, rids.get(0));
      assertThat(drain(index.get(new Object[] { 5 }))).containsExactly(rids.get(1));
    } finally {
      database.rollback();
    }
  }

  @Test
  void hashIndexLookupIsEmptyAfterARemoveOnAUniqueKey() {
    assertLookupIsEmptyAfterDeletingAUniqueKey(Schema.INDEX_TYPE.HASH);
  }

  @Test
  void lsmIndexLookupIsEmptyAfterARemoveOnAUniqueKey() {
    assertLookupIsEmptyAfterDeletingAUniqueKey(Schema.INDEX_TYPE.LSM_TREE);
  }

  private void assertLookupIsEmptyAfterDeletingAUniqueKey(final Schema.INDEX_TYPE indexType) {
    database.transaction(() -> {
      final var type = database.getSchema().createDocumentType("Tag", 1);
      type.createProperty("n", Type.INTEGER);
      database.getSchema().buildTypeIndex("Tag", new String[] { "n" }).withType(indexType).withUnique(true).create();
    });
    database.transaction(() -> database.newDocument("Tag").set("n", 5).save());

    database.begin();
    try {
      final RID rid = database.lookupByKey("Tag", "n", 5).next().getIdentity();
      database.lookupByRID(rid, true).asDocument().delete();
      assertThat(database.lookupByKey("Tag", "n", 5).hasNext()).isFalse();
    } finally {
      database.rollback();
    }
  }

  private List<RID> createTwoRowsSharingAKey(final Schema.INDEX_TYPE indexType) {
    database.transaction(() -> {
      final var type = database.getSchema().createDocumentType("Tag", 1);
      type.createProperty("n", Type.INTEGER);
      database.getSchema().buildTypeIndex("Tag", new String[] { "n" }).withType(indexType).withUnique(false).create();
    });
    final List<RID> rids = new ArrayList<>();
    database.transaction(() -> {
      rids.add(database.newDocument("Tag").set("n", 5).save().getIdentity());
      rids.add(database.newDocument("Tag").set("n", 5).save().getIdentity());
    });
    return rids;
  }

  private Index typeIndex() {
    return database.getSchema().getType("Tag").getPolymorphicIndexByProperties("n");
  }

  private List<RID> drain(final IndexCursor cursor) {
    final List<RID> result = new ArrayList<>();
    while (cursor.hasNext())
      result.add(cursor.next().getIdentity());
    return result;
  }
}
