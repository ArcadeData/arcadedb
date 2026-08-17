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
package com.arcadedb.query.opencypher.procedures.algo;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6295 - the phase an {@code algo.*} call actually spends its time in is not always
 * the loop its knob drives, and the phases <em>after</em> that loop are where #6216 and #6264 stopped looking.
 * <p>
 * Both of those issues hunted knobs whose value is unbounded. {@code algo.hashgnn}'s {@code embeddingDimension}
 * is not one - #6065 capped it at 4096 - which is exactly why the lens walked past it. The MinHash reduction runs
 * after the {@code iterations} loop and costs {@code embeddingDimension x numFeatures} = 4 x
 * {@code embeddingDimension²} operations <em>per node</em>, and that bounded per-node figure is then multiplied
 * by an unbounded node count. Measured on {@code main}: 112,988 ms against a 1000 ms
 * {@code arcadedb.command.timeout}, and the call returned a result rather than aborting.
 * <p>
 * The question that finds this shape is not "which knob has no ceiling" but <b>"which loop over the node count
 * has no checkpoint"</b>. Asking it across the package turned up {@code algo.slpa}'s post-processing (one boxed
 * map merge per remembered label, {@code nodeCount x (iterations + 1)} of them, after the propagation rounds),
 * {@code algo.slpa}'s {@code mostFrequent} (O(degree²) for one listener, between two per-node checkpoints), and
 * the pre-loop initialisation of {@code algo.hashgnn}, {@code algo.fastrp} and {@code algo.graphsage}.
 * <p>
 * <b>Only {@code algo.hashgnn}'s reduction is pinned as a binary outcome here</b>, and the reason the others are
 * not is worth recording rather than papering over. A test of that kind needs a fixture where the unguarded phase
 * outlasts a deadline that the guarded phase before it comfortably survives, and only the reduction gives that: it
 * is three orders of magnitude larger than the message-passing round beside it. The rest are the same defect at a
 * far smaller scale, and their cost sits within a small factor of the guarded phase that precedes them - SLPA's
 * propagation and its post-processing are both O(nodeCount x iterations) and were measured at the same order, and
 * the initialisation phases of {@code algo.fastrp} and {@code algo.graphsage} are O(nodeCount x dimension) beside a
 * first layer that costs the same. Any threshold picked between two phases that close together would be measuring
 * the fixture and the runner, not the checkpoint. The issue's own numbers say as much: 113x over the deadline for
 * the reduction against 1086 ms over a 1000 ms deadline for SLPA. They are still fixed - a checkpoint that cannot
 * be isolated by a test is not a checkpoint that does nothing - and {@link #theGuardedPhasesStillProduceTheirResult}
 * pins that adding them changed no answer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6295AlgoUnguardedPhaseTest {
  private Database database;

  @AfterEach
  void teardown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
      database = null;
    }
  }

  /**
   * {@code algo.hashgnn}'s MinHash reduction, with the {@code iterations} loop deliberately reduced to a single
   * round so that the checkpoint #6264 put inside it runs once, before any real work, and cannot be what fires.
   * <p>
   * 300 nodes at {@code embeddingDimension: 2048} is 300 x 2048 x 8192 ≈ 5e9 inner iterations in the reduction
   * against 300 x 2 x 8192 ≈ 5e6 in the single message-passing round - three orders of magnitude apart, so a 1 s
   * deadline can only land in the reduction. Without a checkpoint there the call grinds through all of it and
   * returns embeddings; with one it gives up about a second in.
   */
  @Test
  @Tag("slow")
  @Timeout(300)
  void hashGnnObservesTheDeadlineInsideTheMinHashReduction() {
    cycle("test-issue-6295-hashgnn-minhash", 300);
    database.getConfiguration().setValue(GlobalConfiguration.COMMAND_TIMEOUT, 1_000L);

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();
    assertThatThrownBy(() -> drain(
        "CALL algo.hashgnn({embeddingDimension: 2048, iterations: 1, seed: 1}) YIELD node RETURN node"))
        .as("the reduction is where an algo.hashgnn call spends its time, so it is where the deadline has to be seen")
        .hasStackTraceContaining(GlobalConfiguration.COMMAND_TIMEOUT.getKey());

    stopwatch.assertGaveUpWithin(120_000L, "a reduction aborted from inside, not one run to completion");
  }

  /**
   * The counterweight: with no deadline configured, every procedure the sweep touched still produces its result at
   * an ordinary size. A checkpoint that aborted a healthy run, or one placed where it changes the answer, would
   * satisfy the test above and fail here - which is what covers the phases that cannot be isolated by a deadline.
   */
  @Test
  @Timeout(120)
  void theGuardedPhasesStillProduceTheirResult() {
    cycle("test-issue-6295-unguarded-phases-happy-path", 40);

    assertThat(drain("CALL algo.hashgnn({embeddingDimension: 16, iterations: 2, seed: 1}) YIELD node, embedding "
        + "RETURN node, embedding")).hasSize(40);
    assertThat(drain("CALL algo.slpa({iterations: 5, seed: 1}) YIELD node, communities RETURN node, communities"))
        .hasSize(40);
    assertThat(drain("CALL algo.fastrp({dimensions: 16, iterations: 2, seed: 1}) YIELD node RETURN node"))
        .hasSize(40);
    assertThat(drain("CALL algo.graphsage({embeddingDimension: 16, layers: 2, seed: 1}) YIELD node RETURN node"))
        .hasSize(40);
  }

  // ── Fixtures ─────────────────────────────────────────────────────────────

  /** A directed cycle of {@code nodeCount} nodes: every node has one out-edge and one in-edge. */
  private void cycle(final String name, final int nodeCount) {
    create(name);
    database.transaction(() -> {
      final List<MutableVertex> nodes = new ArrayList<>(nodeCount);
      for (int i = 0; i < nodeCount; i++)
        nodes.add(database.newVertex("Node").set("idx", i).save());
      for (int i = 0; i < nodeCount; i++)
        nodes.get(i).newEdge("LINK", nodes.get((i + 1) % nodeCount), true, (Object[]) null).save();
    });
  }

  private void create(final String name) {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/" + name);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");
  }

  private List<Object> drain(final String query) {
    final List<Object> rows = new ArrayList<>();
    final ResultSet rs = database.query("opencypher", query);
    while (rs.hasNext())
      rows.add(rs.next());
    return rows;
  }
}
