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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionIndexContext.ComparableKey;
import com.arcadedb.database.TransactionIndexContext.IndexKey;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7881: the transaction-side index overlay compared array-valued key elements (a
 * {@code BINARY} property is a {@code byte[]}) with the SHALLOW {@code Arrays.equals}/{@code Arrays.hashCode} in
 * {@code equals()}/{@code hashCode()}, while {@code ComparableKey.compareTo} compared the very same elements BY
 * CONTENT through {@code BinaryComparator}. Both are used, on the same data, by the same code path: {@code compareTo}
 * orders the {@code TreeMap} of pending entries and {@code equals}/{@code hashCode} key the {@code HashMap} inside
 * each of its values, so the outer lookup found the key and the inner one missed it.
 * <p>
 * What followed from that one miss is the REMOVE-then-ADD merge: the ADD never found the REMOVE it was meant to
 * retire, so it was not turned into a REPLACE carrying the superseded {@code oldRid} - which is exactly what commit
 * keys the removal of the old RID on. In practice the very next check, the one that sweeps the OTHER sub-indexes,
 * reads its per-key map by iteration rather than by lookup, so it still saw the stale ADD and REFUSED a perfectly
 * legitimate delete-and-reinsert of the same BINARY key in one transaction with a {@code DuplicatedKeyException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7881TransactionIndexArrayKeyEqualityTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      // ONE bucket, so both records of a transaction land on the same sub-index and go through the per-key map that
      // the shallow equality broke - the cross-sub-index check a few lines further down never used it.
      database.getSchema().createDocumentType("Bin7881", 1).createProperty("hash", Type.BINARY);
      database.getSchema().getType("Bin7881").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "hash");
    });
  }

  @Test
  void equalsAgreesWithCompareToOnContentEqualByteArrayKeys() {
    final Object[] a = new Object[] { new byte[] { 1, 2, 3 } };
    final Object[] b = new Object[] { new byte[] { 1, 2, 3 } };

    assertThat(new ComparableKey(a).compareTo(new ComparableKey(b))).isZero();
    assertThat(new ComparableKey(a)).isEqualTo(new ComparableKey(b));
    assertThat(new ComparableKey(a).hashCode()).isEqualTo(new ComparableKey(b).hashCode());

    final RID rid = new RID(1, 1);
    final IndexKey k1 = new IndexKey(true, IndexKey.IndexKeyOperation.ADD, a, rid);
    final IndexKey k2 = new IndexKey(true, IndexKey.IndexKeyOperation.ADD, b, rid);
    assertThat(k1).isEqualTo(k2);
    assertThat(k1.hashCode()).isEqualTo(k2.hashCode());
  }

  @Test
  void theNonUniqueBranchOfTheKeyComparesItsArrayValuesByContentToo() {
    // The unique branch above keys on the tuple alone; a non-unique index keys on the tuple AND the RID, and it is
    // the SAME helper doing the tuple half. Pinned so a future edit to one branch cannot silently leave the other
    // on a shallow comparison (PR #8091 review).
    final Object[] a = { new byte[] { 1, 2, 3 } };
    final Object[] b = { new byte[] { 1, 2, 3 } };
    final RID rid = new RID(1, 1);
    final RID otherRid = new RID(1, 2);

    final IndexKey k1 = new IndexKey(false, IndexKey.IndexKeyOperation.ADD, a, rid);
    final IndexKey sameRid = new IndexKey(false, IndexKey.IndexKeyOperation.ADD, b, rid);
    final IndexKey otherRidSameKey = new IndexKey(false, IndexKey.IndexKeyOperation.ADD, b, otherRid);

    assertThat(k1).isEqualTo(sameRid);
    assertThat(k1.hashCode()).isEqualTo(sameRid.hashCode());
    assertThat(k1).as("a non-unique index keeps one entry per RID, so the RID still separates them")
        .isNotEqualTo(otherRidSameKey);
  }

  /** The invariant the per-key duplicate check exists for, kept under a content-aware equality. */
  @Test
  void twoRecordsWithTheSameBinaryKeyInOneTransactionAreRefused() {
    assertThatThrownBy(() -> database.transaction(() -> {
      database.newDocument("Bin7881").set("hash", new byte[] { 1, 2, 3 }).save();
      database.newDocument("Bin7881").set("hash", new byte[] { 1, 2, 3 }).save();
    })).isInstanceOf(DuplicatedKeyException.class);

    assertThat(ridsFor(new byte[] { 1, 2, 3 })).isEmpty();
  }

  @Test
  void aDeleteAndReinsertOfTheSameBinaryKeyInOneTransactionLeavesNoStaleEntry() {
    final RID[] first = new RID[1];
    database.transaction(() -> first[0] = database.newDocument("Bin7881").set("hash", new byte[] { 9, 8, 7 }).save()
        .getIdentity());

    final RID[] second = new RID[1];
    database.transaction(() -> {
      database.lookupByRID(first[0], true).asDocument().modify().delete();
      second[0] = database.newDocument("Bin7881").set("hash", new byte[] { 9, 8, 7 }).save().getIdentity();
    });

    assertThat(second[0]).isNotEqualTo(first[0]);
    assertThat(ridsFor(new byte[] { 9, 8, 7 }))
        .as("the superseded RID must have been removed from the persisted index")
        .containsExactly(second[0]);
    assertThat(database.getSchema().getIndexByName("Bin7881[hash]").countEntries()).isEqualTo(1);
  }

  @Test
  void theUniqueConstraintStillHoldsAcrossTransactions() {
    database.transaction(() -> database.newDocument("Bin7881").set("hash", new byte[] { 4, 5, 6 }).save());
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Bin7881");
      doc.set("hash", new byte[] { 4, 5, 6 });
      doc.save();
    })).isInstanceOf(DuplicatedKeyException.class);
  }

  private List<RID> ridsFor(final byte[] key) {
    final List<RID> rids = new ArrayList<>();
    final IndexCursor cursor = database.lookupByKey("Bin7881", "hash", key);
    while (cursor.hasNext())
      rids.add(cursor.next().getIdentity());
    return rids;
  }
}
