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

import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionIndexContext.IndexKey;
import com.arcadedb.database.TransactionIndexContext.IndexKey.IndexKeyOperation;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #6970: the three rules deciding which disk RIDs a pending transaction removal hides from an index read, pinned on
 * {@link PendingIndexRemovals} itself. No database needed. The callers are driven by
 * {@code Issue6970PendingIndexRemovalsTest} and {@code Issue6927RangeScanTxRemovesTest}.
 */
class PendingIndexRemovalsTest {

  private static final Object[] KEY = new Object[] { 1 };
  private static final RID      A   = new RID(3, 10);
  private static final RID      B   = new RID(3, 11);
  private static final RID      C   = new RID(3, 12);

  @Test
  void addOnlyKeyHasNoRemovalsAndAllocatesNothing() {
    final PendingIndexRemovals removals = PendingIndexRemovals.accumulate(null, new IndexKey(false, IndexKeyOperation.ADD, KEY, A),
        false);
    assertThat(removals).isNull();
  }

  @Test
  void replaceWithoutOldRidHasNoRemovals() {
    // the cross-bucket duplicate check produces a REPLACE whose oldRid is null: commit replays no removal for it
    final PendingIndexRemovals removals = PendingIndexRemovals.accumulate(null,
        new IndexKey(true, IndexKeyOperation.REPLACE, KEY, A), true);
    assertThat(removals).isNull();
  }

  @Test
  void rule1RemoveWithRidHidesOnlyThatRid() {
    final PendingIndexRemovals removals = PendingIndexRemovals.accumulate(null,
        new IndexKey(false, IndexKeyOperation.REMOVE, KEY, A), false);
    assertThat(removals).isNotNull();
    assertThat(removals.isKeyWide()).isFalse();
    assertThat(removals.hides(A)).isTrue();
    assertThat(removals.hides(B)).isFalse();

    final List<RID> rids = new ArrayList<>(List.of(A, B));
    removals.removeFrom(rids);
    assertThat(rids).containsExactly(B);
  }

  @Test
  void rule2RemoveWithoutRidHidesTheWholeKey() {
    final PendingIndexRemovals removals = PendingIndexRemovals.accumulate(null,
        new IndexKey(false, IndexKeyOperation.REMOVE, KEY, null), false);
    assertThat(removals.isKeyWide()).isTrue();
    assertThat(removals.hides(A)).isTrue();
    assertThat(removals.hides(B)).isTrue();

    final List<RID> rids = new ArrayList<>(List.of(A, B));
    removals.removeFrom(rids);
    assertThat(rids).isEmpty();
  }

  @Test
  void rule2AnyRemoveOnAUniqueIndexHidesTheWholeKey() {
    final PendingIndexRemovals removals = PendingIndexRemovals.accumulate(null,
        new IndexKey(true, IndexKeyOperation.REMOVE, KEY, A), true);
    assertThat(removals.isKeyWide()).isTrue();
    assertThat(removals.hides(B)).isTrue();
  }

  @Test
  void rule3ReplaceHidesItsOldRid() {
    final IndexKey replace = new IndexKey(true, IndexKeyOperation.REPLACE, KEY, B);
    replace.oldRid = A;
    final PendingIndexRemovals removals = PendingIndexRemovals.accumulate(null, replace, true);
    assertThat(removals.isKeyWide()).isFalse();
    assertThat(removals.hides(A)).isTrue();
    assertThat(removals.hides(B)).isFalse();
  }

  @Test
  void removalsAccumulateAcrossEntriesAndKeyWideWinsInEitherOrder() {
    PendingIndexRemovals removals = PendingIndexRemovals.accumulate(null, new IndexKey(false, IndexKeyOperation.REMOVE, KEY, A), false);
    removals = PendingIndexRemovals.accumulate(removals, new IndexKey(false, IndexKeyOperation.ADD, KEY, C), false);
    removals = PendingIndexRemovals.accumulate(removals, new IndexKey(false, IndexKeyOperation.REMOVE, KEY, B), false);
    assertThat(removals.hides(A)).isTrue();
    assertThat(removals.hides(B)).isTrue();
    assertThat(removals.hides(C)).isFalse();

    // a key-wide removal after per-RID ones hides everything
    final PendingIndexRemovals widened = PendingIndexRemovals.accumulate(removals,
        new IndexKey(false, IndexKeyOperation.REMOVE, KEY, null), false);
    assertThat(widened.isKeyWide()).isTrue();
    assertThat(widened.hides(C)).isTrue();
    final List<RID> rids = new ArrayList<>(List.of(A, B, C));
    widened.removeFrom(rids);
    assertThat(rids).isEmpty();

    // a REPLACE carrying an oldRid after a key-wide removal does not narrow it back either
    final IndexKey replace = new IndexKey(false, IndexKeyOperation.REPLACE, KEY, C);
    replace.oldRid = A;
    assertThat(PendingIndexRemovals.accumulate(widened, replace, false).isKeyWide()).isTrue();

    // and a per-RID removal after a key-wide one does not narrow it back
    final PendingIndexRemovals stillWide = PendingIndexRemovals.accumulate(widened,
        new IndexKey(false, IndexKeyOperation.REMOVE, KEY, A), false);
    assertThat(stillWide.isKeyWide()).isTrue();
    assertThat(stillWide.hides(C)).isTrue();
  }
}
