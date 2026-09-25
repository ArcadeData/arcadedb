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

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reconciliation {@link BuildWatch} gives a CSR build (issue #8378), pinned deterministically on ONE pair that
 * several racing changes touch: some the scan read, some it did not. The randomized race test reaches this shape only
 * by chance.
 * <p>
 * A captured addition and a missed deletion on the same pair cancel in the pair's multiplicity: answered through it,
 * the deletion's budget was spent against a live edge and the view held one edge too few. The watch answers edge by
 * edge instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BuildWatchReconciliationTest extends TestHelper {
  private RID a;
  private RID b;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
    database.transaction(() -> {
      a = database.newVertex("P").save().getIdentity();
      b = database.newVertex("P").save().getIdentity();
    });
  }

  @Test
  void aSupersedingBuildTakesOverTheBufferedDeltas() {
    // A blocking build() may scan inside the caller's REPEATABLE_READ transaction and miss a commit that preceded
    // it: the delta the superseded build buffered is then the only way that commit reaches the new CSR
    final BuildWatch superseded = new BuildWatch(1_000);
    superseded.watchSource(a);
    final TxDelta delta = added(newEdge());
    assertThat(superseded.offer(delta)).isTrue();

    final BuildWatch next = new BuildWatch(1_000);
    superseded.handOverTo(next);

    assertThat(next.isWatched(a)).isTrue();
    assertThat(next.close()).containsExactly(delta);
  }

  @Test
  void aCapturedAdditionDeliveredAfterABudgetedDeletionIsNotAddedTwice() {
    // Commit callbacks can arrive out of commit order: the deletion of e0 (after the scan) is delivered before the
    // addition of e1 (before the scan)
    final RID e0 = newEdge();
    final BuildWatch watch = new BuildWatch(1_000);
    watch.watchSource(a);
    final RID e1 = newEdge();

    final CSRBuilder builder = new CSRBuilder(database);
    builder.setScanObserver(watch);
    final CSRBuilder.CSRResult result = builder.build(new String[] { "P" }, new String[] { "K" });
    deleteEdge(e0);

    watch.close();
    watch.bindTo(result.getCsrPerType());
    final int src = result.getMapping().getGlobalId(a);
    final int fresh = result.getCsrPerType().get("K").forwardEdgeCount(src, result.getMapping().getGlobalId(b));

    DeltaOverlay overlay = new DeltaOverlay(result.getMapping().size());
    for (final TxDelta delta : List.of(deleted(e0), added(e1))) {
      watch.account(delta);
      overlay = overlay.merge(delta, result.getMapping(), result.getCsrPerType(), watch);
    }
    assertThat(overlay.getAddedOutNeighbors(src, "K")).isEmpty();
    assertThat(fresh + overlay.getDeltaEdgeCount()).isEqualTo(liveEdges()).isEqualTo(1);
  }

  @Test
  void severalChangesToOnePairAreEachCountedOnce() {
    // Before the scan: e0 exists. The transactions below registered 'a' before committing, so before the scan read it
    final RID e0 = newEdge();
    final RID deletedBeforeScan = newEdge();
    final BuildWatch watch = new BuildWatch(1_000);
    watch.watchSource(a);

    // Committed before the scan reached 'a': the scan sees e1 and no longer sees the deleted edge
    final RID e1 = newEdge();
    deleteEdge(deletedBeforeScan);

    final CSRBuilder builder = new CSRBuilder(database);
    builder.setScanObserver(watch);
    final CSRBuilder.CSRResult result = builder.build(new String[] { "P" }, new String[] { "K" });

    // Committed after the scan read 'a': two parallel additions and the deletion of the base edge e0
    final RID e2 = newEdge();
    final RID e3 = newEdge();
    deleteEdge(e0);

    final List<TxDelta> buffered = new ArrayList<>();
    buffered.add(added(e1));
    buffered.add(deleted(deletedBeforeScan));
    buffered.add(added(e2));
    buffered.add(added(e3));
    buffered.add(deleted(e0));
    watch.close();
    watch.bindTo(result.getCsrPerType());

    final int src = result.getMapping().getGlobalId(a);
    final int tgt = result.getMapping().getGlobalId(b);
    final int fresh = result.getCsrPerType().get("K").forwardEdgeCount(src, tgt);
    assertThat(fresh).as("the scan read e0 and e1").isEqualTo(2);

    DeltaOverlay overlay = new DeltaOverlay(result.getMapping().size());
    for (final TxDelta delta : buffered) {
      watch.account(delta);
      overlay = overlay.merge(delta, result.getMapping(), result.getCsrPerType(), watch);
    }

    // Live now: e1, e2, e3. The base holds 2 (e0, e1); e2 and e3 are added, e0's deletion is budgeted, e1's capture
    // and the swallowed deletion cost nothing
    assertThat(overlay.getAddedOutNeighbors(src, "K")).hasSize(2);
    assertThat(fresh + overlay.getDeltaEdgeCount()).isEqualTo(3);
    assertThat(fresh + overlay.getDeltaEdgeCount()).isEqualTo(liveEdges());
  }

  private RID newEdge() {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableVertex source = a.asVertex().modify();
      rid[0] = source.newEdge("K", b).getIdentity();
    });
    return rid[0];
  }

  private void deleteEdge(final RID edge) {
    database.transaction(() -> edge.asEdge().delete());
  }

  private long liveEdges() {
    return a.asVertex().countEdges(Vertex.DIRECTION.OUT, "K");
  }

  private TxDelta added(final RID edge) {
    final TxDelta delta = new TxDelta();
    delta.addedEdges.add(new TxDelta.EdgeDelta("K", a, b, edge));
    return delta;
  }

  private TxDelta deleted(final RID edge) {
    final TxDelta delta = new TxDelta();
    delta.deletedEdges.add(new TxDelta.EdgeDelta("K", a, b, edge));
    return delta;
  }
}
