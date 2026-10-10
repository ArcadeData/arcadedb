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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9572: the reconciliation a CSR build makes with the commits its scan raced ({@link BuildWatch}, issue #8378)
 * for lightweight edges. The scan reads every lightweight edge of a type as the same bucket marker, so the watch cannot
 * tell them apart by identity and counts them per far end instead. Each case drives one ordering of the changes
 * against the scan deterministically and checks the published base plus the overlay against the live graph.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9572LightEdgeBuildWatchTest extends TestHelper {
  private RID a;
  private RID b;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K LIGHTWEIGHT");
    database.transaction(() -> {
      a = database.newVertex("P").save().getIdentity();
      b = database.newVertex("P").save().getIdentity();
    });
  }

  @Test
  void anAdditionTheScanReadIsNotAddedAgain() {
    final BuildWatch watch = openWatch();
    final TxDelta added = newEdge();
    final Scan scan = scan(watch);

    assertThat(scan.fresh()).isEqualTo(1);
    assertReconciled(scan, watch, List.of(added), 1);
  }

  @Test
  void anAdditionAfterTheScanIsAdded() {
    final BuildWatch watch = openWatch();
    final Scan scan = scan(watch);
    final TxDelta added = newEdge();

    assertThat(scan.fresh()).isZero();
    assertReconciled(scan, watch, List.of(added), 1);
  }

  @Test
  void aDeletionTheScanAlreadyMissedSpendsNoBudget() {
    newEdge();
    final BuildWatch watch = openWatch();
    final TxDelta deleted = deleteEdge();
    final Scan scan = scan(watch);
    final TxDelta added = newEdge();

    assertThat(scan.fresh()).isZero();
    assertReconciled(scan, watch, List.of(deleted, added), 1);
  }

  @Test
  void aDeletionAfterTheScanIsBudgetedAgainstTheBase() {
    newEdge();
    final BuildWatch watch = openWatch();
    final Scan scan = scan(watch);
    final TxDelta deleted = deleteEdge();
    final TxDelta added = newEdge();

    assertThat(scan.fresh()).isEqualTo(1);
    assertReconciled(scan, watch, List.of(deleted, added), 1);
  }

  @Test
  void aReadAdditionThenADeletionAndAnAdditionAfterTheScan() {
    final BuildWatch watch = openWatch();
    final TxDelta first = newEdge();
    final Scan scan = scan(watch);
    final TxDelta deleted = deleteEdge();
    final TxDelta second = newEdge();

    assertThat(scan.fresh()).isEqualTo(1);
    assertReconciled(scan, watch, List.of(first, deleted, second), 1);
  }

  /** A deletion delivered before the addition it follows, which a commit callback arriving late can do. */
  @Test
  void anAdditionAfterTheScanDeletedAgainLeavesNothing() {
    final BuildWatch watch = openWatch();
    final Scan scan = scan(watch);
    final TxDelta added = newEdge();
    final TxDelta deleted = deleteEdge();

    assertThat(scan.fresh()).isZero();
    assertReconciled(scan, watch, List.of(added, deleted), 0);
  }

  /**
   * Duplicated copies the counts can still tell apart: every copy is one of the buffered changes, so the copy the scan
   * read is the first addition and the duplicate added after the scan is added.
   */
  @Test
  void aDuplicateCopyAddedAfterTheScanIsAddedWhenEveryCopyIsBuffered() {
    final BuildWatch watch = openWatch();
    final TxDelta first = newEdge();
    final Scan scan = scan(watch);
    final TxDelta twin = newEdge();

    assertThat(scan.fresh()).isEqualTo(1);
    assertReconciled(scan, watch, List.of(first, twin), 2);
  }

  /**
   * The documented limit: a copy that existed before the build and a duplicate added after the scan read its source
   * look, to the counts, exactly like no earlier copy and a duplicate the scan read. Any copy older than the watch is
   * counted as if it were one of the buffered additions. Nothing recorded tells the two
   * apart, so the answer errs towards "read" and the view holds one copy too few until its next rebuild or compaction.
   * Only duplicated copies of one pair, which the {@code UNIQUE} flag rules out, reach this.
   */
  @Test
  void aDuplicateCopyAddedAfterTheScanIsTakenAsRead() {
    newEdge();
    final BuildWatch watch = openWatch();
    final Scan scan = scan(watch);
    final TxDelta twin = newEdge();

    assertThat(scan.fresh()).isEqualTo(1);
    watch.close();
    watch.bindTo(scan.result().getCsrPerType());
    watch.account(twin);
    final DeltaOverlay overlay = new DeltaOverlay(scan.result().getMapping().size())
        .merge(twin, scan.result().getMapping(), scan.result().getCsrPerType(), watch);
    assertThat(liveEdges()).isEqualTo(2);
    assertThat(scan.fresh() + overlay.getDeltaEdgeCount()).isEqualTo(1);
    // Which is why the pair is handed to the view, to be checked against the graph before it is served
    assertThat(watch.takeRacedLightPairs())
        .containsExactly(new BuildWatch.RacedLightPair(a, database.getSchema().getType("K").getFirstBucketId(), b));
    assertThat(watch.takeRacedLightPairs()).as("handed over once").isEmpty();
  }

  /**
   * A change reported after the watch closed cannot have been read by the scan, so it is answered exactly, without
   * counting: here a duplicate of a copy older than the build, the case counting cannot settle, is added.
   */
  @Test
  void aDuplicateCopyReportedAfterTheWatchClosedIsAdded() {
    newEdge();
    final BuildWatch watch = openWatch();
    final Scan scan = scan(watch);
    watch.close();
    watch.bindTo(scan.result().getCsrPerType());
    final TxDelta twin = newEdge();

    DeltaOverlay overlay = new DeltaOverlay(scan.result().getMapping().size());
    watch.account(twin);
    overlay = overlay.merge(twin, scan.result().getMapping(), scan.result().getCsrPerType(), watch);
    assertThat(liveEdges()).isEqualTo(2);
    assertThat(scan.fresh() + overlay.getDeltaEdgeCount()).isEqualTo(2);

    // And a deletion reported after it removes a copy, never one the counts take as already missing
    final TxDelta deleted = deleteEdge();
    final TxDelta deletedAgain = deleteEdge();
    for (final TxDelta delta : List.of(deleted, deletedAgain)) {
      watch.account(delta);
      overlay = overlay.merge(delta, scan.result().getMapping(), scan.result().getCsrPerType(), watch);
    }
    assertThat(liveEdges()).isZero();
    assertThat(scan.fresh() + overlay.getDeltaEdgeCount()).isZero();
    assertThat(watch.takeRacedLightPairs()).as("nothing for the view to check").isEmpty();
  }

  private BuildWatch openWatch() {
    final BuildWatch watch = new BuildWatch(1_000);
    // What the record listener does before every change of a's out-edges commits
    watch.watchSource(a);
    return watch;
  }

  private record Scan(CSRBuilder.CSRResult result, int fresh) {
  }

  private Scan scan(final BuildWatch watch) {
    final CSRBuilder builder = new CSRBuilder(database);
    builder.setScanObserver(watch);
    final CSRBuilder.CSRResult result = builder.build(new String[] { "P" }, new String[] { "K" });
    final CSRAdjacencyIndex csr = result.getCsrPerType().get("K");
    final int fresh = csr == null ? 0 :
        csr.forwardEdgeCount(result.getMapping().getGlobalId(a), result.getMapping().getGlobalId(b));
    return new Scan(result, fresh);
  }

  private void assertReconciled(final Scan scan, final BuildWatch watch, final List<TxDelta> buffered, final long expected) {
    watch.close();
    watch.bindTo(scan.result().getCsrPerType());
    DeltaOverlay overlay = new DeltaOverlay(scan.result().getMapping().size());
    for (final TxDelta delta : buffered) {
      watch.account(delta);
      overlay = overlay.merge(delta, scan.result().getMapping(), scan.result().getCsrPerType(), watch);
    }
    assertThat(liveEdges()).isEqualTo(expected);
    assertThat(scan.fresh() + overlay.getDeltaEdgeCount()).as("the published base plus its overlay").isEqualTo(expected);
  }

  private TxDelta newEdge() {
    database.transaction(() -> a.asVertex().modify().newEdge("K", b));
    return change(true);
  }

  private TxDelta deleteEdge() {
    database.transaction(() -> {
      final List<Edge> edges = new ArrayList<>();
      a.asVertex().getEdges(Vertex.DIRECTION.OUT, "K").forEach(edges::add);
      edges.getFirst().delete();
    });
    return change(false);
  }

  /** The delta the collector reports for one lightweight edge change of a -> b. */
  private TxDelta change(final boolean addition) {
    final TxDelta delta = new TxDelta();
    final TxDelta.EdgeDelta edge = new TxDelta.EdgeDelta("K", a, b,
        TxDelta.lightEdgeKey(database.getSchema().getType("K").getFirstBucketId()));
    (addition ? delta.addedEdges : delta.deletedEdges).add(edge);
    return delta;
  }

  private long liveEdges() {
    return a.asVertex().countEdges(Vertex.DIRECTION.OUT, "K");
  }
}
