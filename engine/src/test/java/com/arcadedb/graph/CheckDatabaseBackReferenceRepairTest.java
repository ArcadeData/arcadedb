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
package com.arcadedb.graph;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6136 (2): the two repairs {@code CHECK DATABASE ... FIX} used to perform from INSIDE the vertex scan.
 * <p>
 * #6131 bounded every post-scan repair loop with {@code arcadedb.checkDatabaseRepairBatchPages} and deliberately left
 * the in-scan ones alone, because {@code LocalDatabase.scanType} holds the database read lock for the length of the
 * scan and the chunk iterator being walked would not survive a commit taken under it. That left the back-reference
 * fix-up - one write to a FAR vertex per edge found "not connected from the other side", each able to allocate a
 * fresh edge-list chunk - accumulating in a transaction nothing could split. On a replicated database that
 * transaction is one Raft log entry, and a large enough one is rejected outright with
 * {@code ReplicatedEntryTooLargeException}, which is not a {@code NeedRetryException}.
 * <p>
 * The repair is now PLANNED during the scan and applied after it, so it goes through the same page budget as
 * everything else. Both directions are pinned: a run over the budget must produce more than one transaction, and
 * both settings must repair the graph identically - a deferral bug that silently dropped repairs would otherwise
 * satisfy the commit-count assertion on its own.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseBackReferenceRepairTest extends TestHelper {
  private static final String VERTEX_TYPE = "Node";
  private static final String EDGE_TYPE   = "Link";
  /**
   * One damaged PAIR per unit, not one damaged hub: the repair writes to the far vertex of each defective edge, so
   * the page volume comes from how many DISTINCT far vertices need a chunk, not from how many edges one of them has.
   */
  private static final int    PAIRS       = 60;

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    // The fixture leaves a deliberately half-connected graph; the post-test check would (correctly) flag it.
    return false;
  }

  /**
   * A back-reference repair whose page footprint exceeds the budget must be split across several transactions, and
   * must still re-link every edge.
   */
  @Test
  void theBackReferenceRepairIsSplitAcrossTransactions() {
    final List<RID> targets = createHalfConnectedPairs();

    GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES.setValue(4);
    final int commits = countCommitsDuring(this::repairVertices);

    assertThat(commits)
        .as("a back-reference repair bigger than the page budget must not be one single transaction")
        .isGreaterThan(1);

    assertRepaired(targets);
  }

  /** The budget disabled restores the historical single-transaction behaviour, and repairs exactly the same graph. */
  @Test
  void theBackReferenceRepairIsWholeWithTheBudgetDisabled() {
    final List<RID> targets = createHalfConnectedPairs();

    GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES.setValue(0);
    final int commits = countCommitsDuring(this::repairVertices);

    assertThat(commits).as("with the budget disabled the whole type repair is one transaction").isEqualTo(1);

    assertRepaired(targets);
  }

  /**
   * The repair is reported as its own kind (issue #6136, item 3) rather than being invisible: it never contributed
   * to {@code autoFix} and, before the breakdown existed, an operator could only find it in the warnings.
   */
  @Test
  void theRepairIsCountedAsReconnectedEdges() {
    createHalfConnectedPairs();

    GlobalConfiguration.CHECK_DATABASE_REPAIR_BATCH_PAGES.setValue(0);
    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
        .checkVertices(VERTEX_TYPE, true, 0);

    assertThat((Long) stats.get("reconnectedEdges"))
        .as("every re-linked back-reference must be counted").isEqualTo(PAIRS);
    assertThat((Long) stats.get("autoFix"))
        .as("nothing was deleted and nothing was pruned, so autoFix stays zero - which is exactly why the "
            + "breakdown had to be added").isZero();
  }

  /** The vertex arm on its own: a full CHECK DATABASE FIX runs several passes that each commit. */
  private void repairVertices() {
    new GraphDatabaseChecker((DatabaseInternal) database).checkVertices(VERTEX_TYPE, true, 0);
  }

  /** Counts the transactions that actually wrote WAL while {@code block} ran. */
  private int countCommitsDuring(final Runnable block) {
    final AtomicInteger commits = new AtomicInteger();
    final DatabaseInternal db = (DatabaseInternal) database;
    final Callable<Void> counter = () -> {
      commits.incrementAndGet();
      return null;
    };
    db.registerCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, counter);
    try {
      block.run();
    } finally {
      db.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.TX_AFTER_WAL_WRITE, counter);
    }
    return commits.get();
  }

  /**
   * {@link #PAIRS} source-target pairs joined by one edge each, with every TARGET's incoming chain detached
   * afterwards.
   * <p>
   * That is the exact shape of the defect: the edge record and the source's outgoing entry are both intact, so the
   * scan reads a perfectly healthy edge and finds that the vertex on the other end does not list it - which is what
   * {@code isConnectedTo} answers when the head chunk is simply absent. Nothing here is unreadable, so no chain is
   * rebuilt and no record is deleted, and the back-reference fix-up is the ONLY repair the run performs.
   *
   * @return the target RIDs, in creation order
   */
  private List<RID> createHalfConnectedPairs() {
    // A SMALLER page size, set before the buckets are created: the budget counts PAGES, and at the 64KB default the
    // whole fixture packs into a couple of them, so no assertion about batching could mean anything.
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(16_384);

    database.transaction(() -> {
      database.getSchema().createVertexType(VERTEX_TYPE);
      database.getSchema().createEdgeType(EDGE_TYPE);
    });

    // A payload per vertex, so the vertex records span several pages instead of packing into one.
    final String payload = "x".repeat(1_500);

    final List<RID> targets = new ArrayList<>(PAIRS);
    for (int p = 0; p < PAIRS; p++) {
      final int pair = p;
      final RID[] holder = new RID[1];
      database.transaction(() -> {
        final MutableVertex target = database.newVertex(VERTEX_TYPE).set("name", "target" + pair).set("payload", payload)
            .save();
        database.newVertex(VERTEX_TYPE).set("name", "source" + pair).set("payload", payload).save()
            .newEdge(EDGE_TYPE, target);
        holder[0] = target.getIdentity();
      });
      targets.add(holder[0]);
    }

    // Detach every target's IN chain. Not corruption of the chunk - the pointer is simply dropped, which is what a
    // half-applied write leaves behind and what makes isConnectedTo answer "no" instead of throwing.
    database.transaction(() -> {
      for (final RID target : targets) {
        final MutableVertex mutable = target.asVertex(true).modify();
        mutable.setInEdgesHeadChunk(null);
        mutable.save();
      }
    });

    database.transaction(() -> {
      for (final RID target : targets)
        assertThat(target.asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE))
            .as("the fixture must actually leave target %s unconnected from its side", target).isZero();
    });

    return targets;
  }

  /** Every target lists its edge again, and a second check finds nothing left to re-link. */
  private void assertRepaired(final List<RID> targets) {
    database.transaction(() -> {
      for (final RID target : targets)
        assertThat(target.asVertex(true).countEdges(Vertex.DIRECTION.IN, EDGE_TYPE))
            .as("target %s must be connected from its own side again", target).isEqualTo(1);
    });

    final Map<String, Object> second = new GraphDatabaseChecker((DatabaseInternal) database)
        .checkVertices(VERTEX_TYPE, false, 0);
    assertThat((Long) second.get("totalWarnings"))
        .as("a repaired graph must give the checker nothing left to say").isZero();
  }
}
