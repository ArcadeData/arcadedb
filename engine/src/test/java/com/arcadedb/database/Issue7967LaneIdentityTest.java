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
package com.arcadedb.database;

import com.arcadedb.database.TransactionIndexContext.ComparableKey;
import com.arcadedb.database.TransactionIndexContext.IndexKey;
import com.arcadedb.database.TransactionIndexContext.IndexKey.IndexKeyOperation;
import com.arcadedb.index.IndexInternal;
import java.util.Map;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the aliasing defect found in the review of PR #8001, on the O(1) lane lookup issue #7967
 * added.
 * <p>
 * An index's lanes are held in an {@link java.util.IdentityHashMap} keyed by the index, precisely so that WHICH
 * index a lane belongs to is decided by identity. The per-index value, though, is a plain {@code List} of
 * {@code TreeMap}s - and {@code TreeMap} inherits {@code equals()} from {@code AbstractMap}, so it compares by
 * CONTENT, and two empty ones are always equal. Dropping a retracted lane with {@code List.remove(Object)}
 * therefore removed the first CONTENT-equal lane, which is not necessarily the one being retracted.
 * <p>
 * One index owns more than one lane whenever it renames itself mid-transaction, which an {@code LSM_VECTOR} index
 * does when a compaction swaps in the component file it is named after (issue #6105): the next write opens a second
 * lane under the new name, and {@code commit()} replays both. Evicting the wrong one leaves that lane queued and
 * still replayed at commit, but unreachable through the lookup - so a read-your-own-writes search answers with part
 * of the transaction's own writes missing, silently.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7967LaneIdentityTest {

  @Test
  void retractingOneLaneOfAnIndexLeavesTheOtherOneReachable() {
    final TransactionIndexContext changes = new TransactionIndexContext(null);
    final IndexInternal index = nonUniqueIndex("Doc_0_first");

    // Lane one, under the name the index answers to now.
    changes.addIndexKeyLock(index, IndexKeyOperation.ADD, new Object[] { "k1" }, new RID(1, 1));
    assertThat(changes.getIndexKeyLanes(index)).as("precondition: one lane so far").hasSize(1);

    // The rename a compaction performs, and the second lane the next write opens under the new name.
    Mockito.when(index.getName()).thenReturn("Doc_0_second");
    changes.addIndexKeyLock(index, IndexKeyOperation.ADD, new Object[] { "k2" }, new RID(1, 2));
    assertThat(changes.getIndexKeyLanes(index)).as("the rename must have opened a second lane").hasSize(2);

    // Retract a third entry that lands alone on the SECOND lane's key. Undoing it empties that key, which empties
    // no lane - so nothing is dropped and both lanes must survive.
    changes.armRecordUndo();
    changes.addIndexKeyLock(index, IndexKeyOperation.ADD, new Object[] { "k3" }, new RID(1, 3));
    changes.undoRecordChanges();
    changes.disarmRecordUndo();

    assertThat(changes.getIndexKeyLanes(index))
        .as("retracting an entry must not cost the index either of its lanes")
        .hasSize(2);
  }

  /**
   * The shape that actually got it wrong: two lanes of one index holding the SAME content, and the LATER one
   * dropped by name. {@code List.remove(Object)} scans from the front and removes the first CONTENT-equal element,
   * which here is the lane that was NOT being dropped - so the surviving lane became unreachable through the
   * lookup while still sitting in {@code indexEntries} and still being replayed by the commit, and the dropped
   * one stayed reachable through it.
   * <p>
   * Two lanes carry identical content whenever the same record is written, the index renames itself, and the record
   * is written again - a re-embedding across a compaction, which is exactly the sequence issue #6105 is about.
   */
  @Test
  void droppingOneLaneByNameLeavesAnIdenticalSiblingLaneReachable() {
    final TransactionIndexContext changes = new TransactionIndexContext(null);
    final IndexInternal index = nonUniqueIndex("Doc_0_first");

    final RID rid = new RID(1, 1);
    changes.addIndexKeyLock(index, IndexKeyOperation.ADD, new Object[] { "k1" }, rid);

    // The rename, and the SAME record written again: the second lane ends up content-equal to the first.
    Mockito.when(index.getName()).thenReturn("Doc_0_second");
    changes.addIndexKeyLock(index, IndexKeyOperation.ADD, new Object[] { "k1" }, rid);

    final var lanes = changes.getIndexKeyLanes(index);
    assertThat(lanes).as("precondition: two lanes").hasSize(2);
    assertThat(lanes.get(0)).as("...and they are content-equal, which is what made this go wrong").isEqualTo(lanes.get(1));
    final var firstLane = lanes.get(0);

    // The SECOND lane is dropped by name. A content-equality removal scans from the front and takes the FIRST
    // match, which is the other lane - the one that has to survive.
    changes.removeIndex("Doc_0_second");

    // isSameAs, not containsExactly: the two lanes are EQUAL, so an equality assertion cannot tell them apart -
    // which is the same reason List.remove(Object) could not, and is the whole defect.
    final var remaining = changes.getIndexKeyLanes(index);
    assertThat(remaining).hasSize(1);
    assertThat(remaining.getFirst())
        .as("the lane dropped by name is the one that goes, not whichever happened to look like it")
        .isSameAs(firstLane);
    assertThat(changes.getTotalEntries())
        .as("and the surviving lane's entry is still queued for the commit to replay")
        .isEqualTo(1);
  }

  /**
   * The commit picks one winner per RID across ALL of an index's lanes, not one per lane.
   * <p>
   * Raised by CodeRabbit on PR #8001. An index that renames itself mid-transaction owns a lane under each name, and
   * a record rewritten either side of that rename has an entry in both. Deduplicating per lane hands the batch a
   * winner from each - which is exactly the duplicate-embedding defect issue #7971 exists to close, reintroduced
   * through a second lane. The survivor has to be chosen by write order across the lanes.
   */
  @Test
  void theWinnerPerRidIsChosenAcrossEveryLaneOfTheIndex() {
    final TransactionIndexContext changes = new TransactionIndexContext(null);
    final IndexInternal index = nonUniqueIndex("Doc_0_first");
    final RID rid = new RID(1, 1);

    changes.addIndexKeyLock(index, IndexKeyOperation.ADD, new Object[] { "v1" }, rid);

    Mockito.when(index.getName()).thenReturn("Doc_0_second");
    changes.addIndexKeyLock(index, IndexKeyOperation.ADD, new Object[] { "v2" }, rid);

    final var lanes = changes.getIndexKeyLanes(index);
    assertThat(lanes).as("precondition: the rename opened a second lane").hasSize(2);

    // One entry in each lane, for one RID. The later write is the one on the second lane.
    final IndexKey first = onlyEntryOf(lanes.get(0));
    final IndexKey second = onlyEntryOf(lanes.get(1));
    assertThat(second.sequence)
        .as("the entry on the lane opened after the rename must carry the later write order")
        .isGreaterThan(first.sequence);
  }

  private static IndexKey onlyEntryOf(final java.util.TreeMap<ComparableKey, Map<IndexKey, IndexKey>> lane) {
    assertThat(lane).hasSize(1);
    final Map<IndexKey, IndexKey> values = lane.firstEntry().getValue();
    assertThat(values).hasSize(1);
    return values.values().iterator().next();
  }

  private static IndexInternal nonUniqueIndex(final String name) {
    final IndexInternal index = Mockito.mock(IndexInternal.class);
    Mockito.when(index.getName()).thenReturn(name);
    Mockito.when(index.isUnique()).thenReturn(false);
    Mockito.when(index.isTransactionKeyOrderRequired()).thenReturn(true);
    Mockito.when(index.getNullStrategy()).thenReturn(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR);
    return index;
  }
}
