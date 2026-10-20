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
package com.arcadedb.graph.olap;

import com.arcadedb.database.RID;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10305: the creation of an edge the transaction then deletes is taken back in its delta ({@link TxDelta#cancelAddedEdge}),
 * so the deletion is never reported for a slot another transaction may have filled. The edges the transaction keeps stay in the
 * order it created them, and a delta left with nothing but cancelled creations is empty.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TxDeltaCancelAddedEdgeTest {

  private static TxDelta.EdgeDelta edge(final long position) {
    return new TxDelta.EdgeDelta("E", new RID(1, 0L), new RID(1, 1L), new RID(4, position));
  }

  private static List<RID> kept(final TxDelta delta) {
    final List<TxDelta.EdgeDelta> copy = new ArrayList<>();
    delta.copyAddedEdgesTo(copy);
    return copy.stream().map(e -> e.rid).toList();
  }

  @Test
  void aCancelledCreationLeavesTheOthersInOrder() {
    final TxDelta delta = new TxDelta();
    for (long position = 0; position < 6; position++)
      delta.addEdge(edge(position));

    assertThat(delta.cancelAddedEdge(new RID(4, 1L))).isTrue();
    assertThat(delta.cancelAddedEdge(new RID(4, 4L))).isTrue();

    assertThat(kept(delta)).containsExactly(new RID(4, 0L), new RID(4, 2L), new RID(4, 3L), new RID(4, 5L));
    assertThat(delta.isEmpty()).isFalse();
  }

  @Test
  void anEdgeTheTransactionDidNotCreateIsNotCancelled() {
    final TxDelta delta = new TxDelta();
    delta.addEdge(edge(0L));

    assertThat(delta.cancelAddedEdge(new RID(4, 7L))).isFalse();
    assertThat(new TxDelta().cancelAddedEdge(new RID(4, 0L))).as("nothing created").isFalse();
    assertThat(kept(delta)).containsExactly(new RID(4, 0L));
  }

  @Test
  void aSlotCancelledOnceCannotBeCancelledAgain() {
    final TxDelta delta = new TxDelta();
    delta.addEdge(edge(3L));

    assertThat(delta.cancelAddedEdge(new RID(4, 3L))).isTrue();
    assertThat(delta.cancelAddedEdge(new RID(4, 3L))).isFalse();
  }

  @Test
  void aSlotCreatedAgainAfterItsCancellationIsCancelledAgain() {
    final TxDelta delta = new TxDelta();
    delta.addEdge(edge(3L));
    assertThat(delta.cancelAddedEdge(new RID(4, 3L))).isTrue();

    // the transaction gets the slot back, keeps this one
    delta.addEdge(edge(3L));
    delta.addEdge(edge(5L));
    assertThat(kept(delta)).containsExactly(new RID(4, 3L), new RID(4, 5L));

    assertThat(delta.cancelAddedEdge(new RID(4, 3L))).isTrue();
    assertThat(kept(delta)).containsExactly(new RID(4, 5L));
  }

  @Test
  void aDeltaWithOnlyCancelledCreationsIsEmpty() {
    final TxDelta delta = new TxDelta();
    delta.addEdge(edge(1L));
    delta.addEdge(edge(2L));
    assertThat(delta.isEmpty()).isFalse();

    delta.cancelAddedEdge(new RID(4, 1L));
    delta.cancelAddedEdge(new RID(4, 2L));

    assertThat(delta.isEmpty()).isTrue();
    assertThat(kept(delta)).isEmpty();
  }

  @Test
  void theUpdatesOfACancelledEdgeGoWithIt() {
    final TxDelta delta = new TxDelta();
    final TxDelta.EdgeDelta created = edge(2L);
    delta.addEdge(created);
    delta.updatedEdges.put(created.rid, created);
    final TxDelta.EdgeDelta other = edge(9L);
    delta.updatedEdges.put(other.rid, other);

    assertThat(delta.cancelAddedEdge(created.rid)).isTrue();

    assertThat(delta.updatedEdges).containsOnlyKeys(other.rid);
  }

  @Test
  void aLightweightEdgeChangeIsNeverCancelledByRid() {
    final TxDelta delta = new TxDelta();
    final RID key = TxDelta.lightEdgeKey(4);
    delta.addEdge(new TxDelta.EdgeDelta("E", new RID(1, 0L), new RID(1, 1L), key));
    delta.addEdge(edge(0L));

    assertThat(delta.cancelAddedEdge(key)).as("a light change has no RID of its own to take back").isFalse();
    assertThat(delta.cancelAddedEdge(new RID(4, 0L))).isTrue();
    assertThat(kept(delta)).containsExactly(key);
  }

  @Test
  void clearForgetsEveryCancellation() {
    final TxDelta delta = new TxDelta();
    delta.addEdge(edge(1L));
    delta.cancelAddedEdge(new RID(4, 1L));

    delta.clear();

    assertThat(delta.isEmpty()).isTrue();
    delta.addEdge(edge(1L));
    assertThat(kept(delta)).containsExactly(new RID(4, 1L));
    assertThat(delta.cancelAddedEdge(new RID(4, 1L))).isTrue();
  }

  @Test
  void aCreationAfterTheIndexWasBuiltIsFoundToo() {
    final TxDelta delta = new TxDelta();
    delta.addEdge(edge(1L));
    // the first deletion builds the index of the created edges
    assertThat(delta.cancelAddedEdge(new RID(4, 99L))).isFalse();

    delta.addEdge(edge(2L));
    assertThat(delta.cancelAddedEdge(new RID(4, 2L))).isTrue();
    assertThat(kept(delta)).containsExactly(new RID(4, 1L));
  }
}
