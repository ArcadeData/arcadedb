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
import com.arcadedb.database.TransactionIndexContext.ComparableKey;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinaryTypes;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #4947: NULL ordering must be consistent between the transaction overlay and the on-disk pages. The tx
 * overlay's {@link ComparableKey} used to sort nulls HIGH while {@link BinaryComparator} (the disk merge and
 * {@code LSMTreeIndexAbstract.compareKey}) sorts them LOW. With {@code NULL_STRATEGY.INDEX} and composite
 * keys containing nulls, the cursor's TreeMap navigation (ceiling/floor/higher/lower) then disagreed with
 * the disk order: uncommitted entries with null key components could be skipped or emitted out of order
 * during in-transaction iteration.
 */
class TransactionIndexNullOrderingTest extends TestHelper {

  @Test
  void txOverlayNullOrderingMatchesDiskComparator() {
    // Disk-side ground truth: BinaryComparator sorts null LOW.
    final BinaryComparator diskComparator = new BinaryComparator();
    assertThat(diskComparator.compare(null, BinaryTypes.TYPE_STRING, "x", BinaryTypes.TYPE_STRING)).isNegative();
    assertThat(diskComparator.compare("x", BinaryTypes.TYPE_STRING, null, BinaryTypes.TYPE_STRING)).isPositive();

    // The tx overlay must agree, or the cursor's TreeMap navigation diverges from the disk merge (#4947).
    final ComparableKey nullKey = new ComparableKey(new Object[] { null });
    final ComparableKey xKey = new ComparableKey(new Object[] { "x" });
    assertThat(nullKey.compareTo(xKey)).as("tx overlay must sort nulls LOW, matching BinaryComparator (#4947)").isNegative();
    assertThat(xKey.compareTo(nullKey)).as("tx overlay must sort nulls LOW, matching BinaryComparator (#4947)").isPositive();

    // Composite keys: the null component decides against the non-null one the same way.
    final ComparableKey compositeNull = new ComparableKey(new Object[] { 1, null });
    final ComparableKey compositeX = new ComparableKey(new Object[] { 1, "x" });
    assertThat(compositeNull.compareTo(compositeX)).isNegative();
    assertThat(compositeX.compareTo(compositeNull)).isPositive();

    // Equality is unaffected.
    assertThat(nullKey.compareTo(new ComparableKey(new Object[] { null }))).isZero();
  }

  @Test
  void inTxIterationWithNullKeyComponentsSeesAllEntries() {
    // Functional guard: with NULL_STRATEGY.INDEX, uncommitted entries whose composite key contains null must
    // be visible (and orderable) during in-tx iteration merged with committed disk entries.
    final DocumentType type = database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    type.createProperty("b", String.class);
    database.getSchema().buildTypeIndex("Doc", new String[] { "a", "b" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false)
        .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.INDEX).create();

    // Committed baseline. All composite keys in this test are DISTINCT on purpose: an uncommitted entry
    // with the same key as a committed one is shadowed by the tx overlay during in-tx iteration even on a
    // non-unique index (a separate pre-existing defect, discovered while writing this test and filed as #5055) - key collisions here would test that bug, not the #4947 ordering.
    database.transaction(() -> {
      database.newDocument("Doc").set("a", 1).set("b", "m").save();
      database.newDocument("Doc").set("a", 2, "b", null).save();
    });

    database.begin();
    try {
      // Uncommitted overlay entries interleaved around the committed ones, including null components.
      database.newDocument("Doc").set("a", 1, "b", null).save();
      database.newDocument("Doc").set("a", 1).set("b", "z").save();
      database.newDocument("Doc").set("a", 2).set("b", "q").save();

      int count = 0;
      final ResultSet rs = database.query("sql", "SELECT a, b FROM Doc WHERE a >= 1 AND a <= 2");
      while (rs.hasNext()) {
        rs.next();
        ++count;
      }
      assertThat(count)
          .as("in-tx index iteration must see committed AND uncommitted entries incl. null key components (#4947)")
          .isEqualTo(5);
    } finally {
      database.rollback();
    }
  }

  @Test
  void inTxPrefixRangeDoesNotSkipOverlayEntriesInThePrefixRun() {
    // Second leg of #4947, no nulls involved at all: getClosestEntryInTx used to navigate the overlay
    // TreeMap with a PARTIAL key, which compares equal to EVERY entry sharing the prefix - so
    // ceilingEntry([1]) landed on whichever (1,*) node the tree walk reached first (the middle of the run)
    // and the run's earlier entries were silently skipped during in-tx iteration.
    final DocumentType type = database.getSchema().buildDocumentType().withName("Doc2").withTotalBuckets(1).create();
    type.createProperty("a", Integer.class);
    type.createProperty("b", String.class);
    database.getSchema().buildTypeIndex("Doc2", new String[] { "a", "b" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.begin();
    try {
      // Three uncommitted entries sharing the prefix (a=1): with the run's middle as the tree root, the
      // pre-fix navigation dropped the first entry.
      database.newDocument("Doc2").set("a", 1).set("b", "a").save();
      database.newDocument("Doc2").set("a", 1).set("b", "b").save();
      database.newDocument("Doc2").set("a", 1).set("b", "c").save();

      int count = 0;
      final ResultSet rs = database.query("sql", "SELECT b FROM Doc2 WHERE a = 1");
      while (rs.hasNext()) {
        rs.next();
        ++count;
      }
      assertThat(count)
          .as("a prefix range must see EVERY uncommitted entry of the prefix run (#4947)")
          .isEqualTo(3);
    } finally {
      database.rollback();
    }
  }
}
