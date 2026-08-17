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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #6263 - the dense working set of the OpenCypher {@code algo.*} procedures.
 * <p>
 * #6065 capped every embedding-dimension knob at {@code MAX_EMBEDDING_DIMENSION} and #6216 priced the
 * random-walk buffers against a heap budget. Between them they left the largest allocation of these procedures
 * outside every budget: the matrices themselves. A dimension cap bounds one embedding <em>row</em> at 32 KB and
 * says nothing about a {@code nodeCount x dimension} matrix - at the default dimension of 128 the pair of them
 * {@code algo.node2vec} keeps alive costs about 2 KB per node, the same order as the walk buffer that was
 * already checked, and 2 GB at a million nodes. The {@code nodeCount x nodeCount} matrices of
 * {@code algo.apsp}, {@code algo.simRank}, {@code algo.maxFlow} and {@code algo.kShortestPaths} have no knob
 * at all: the graph alone sizes them, and {@code algo.steinerTree}'s are sized by a caller-supplied list that
 * nothing bounds.
 * <p>
 * The budget of #6216 was therefore generalised from walks to the whole working set of one call
 * ({@code arcadedb.cypher.algoMaxWorkingMemory}), and reservations accumulate over the call, so a procedure
 * holding several of these at once is bounded by their sum rather than by whichever one happens to be largest.
 * The failure mode being closed is an {@code OutOfMemoryError}; what replaces it is a client error naming the
 * component, the counts that sized it and the setting to raise.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6263AlgoWorkingMemoryBudgetTest {
  private Database database;

  @BeforeEach
  void setup() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6263-algo-working-memory");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createVertexType("Node");
    database.getSchema().createEdgeType("LINK");

    // Directed cycle A→B→C→D→A: every node has an outgoing edge, so no walk ever dead-ends and every pair is
    // reachable. Four nodes make the matrices small, so the budget is set below them explicitly - what is under
    // test is the accounting, and a graph big enough to breach the default budget would be a graph big enough
    // to make the test itself allocate gigabytes.
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Node").set("name", "A").save();
      final MutableVertex b = database.newVertex("Node").set("name", "B").save();
      final MutableVertex c = database.newVertex("Node").set("name", "C").save();
      final MutableVertex d = database.newVertex("Node").set("name", "D").save();
      a.newEdge("LINK", b, true, (Object[]) null).save();
      b.newEdge("LINK", c, true, (Object[]) null).save();
      c.newEdge("LINK", d, true, (Object[]) null).save();
      d.newEdge("LINK", a, true, (Object[]) null).save();
    });
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  // ── The embedding matrices ───────────────────────────────────────────────

  @Test
  void fastRpRejectsEmbeddingMatricesLargerThanTheWorkingMemoryBudget() {
    // algo.fastrp is the procedure with nothing else to price: it has no walk buffer, so before this change no
    // budget of any kind applied to it. Two matrices of 4 nodes x 128 doubles are 8448 bytes with row headers.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1024L);

    assertThatThrownBy(() -> drain("CALL algo.fastrp({seed: 42}) YIELD node RETURN node"))
        .as("embedding matrices over the budget must be refused before allocating")
        .hasStackTraceContaining("the embedding matrices would need 8448 bytes (2 matrices of 4 nodes x dimensions=128)")
        .hasStackTraceContaining("more than the 1024 bytes allowed")
        .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  @Test
  void fastRpRunsWhenTheEmbeddingMatricesFitTheBudget() {
    // Over-reach guard: the same call with a budget above the estimate must be untouched by the check.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1024L * 1024L);

    assertThat(drain("CALL algo.fastrp({dimensions: 8, seed: 42}) YIELD node RETURN node")).hasSize(4);
  }

  @Test
  void fastRpIsUnboundedWhenTheBudgetIsDisabled() {
    // Negative means no limit, and that has to keep meaning no limit for the newly priced components too.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);

    assertThat(drain("CALL algo.fastrp({dimensions: 128, seed: 42}) YIELD node RETURN node")).hasSize(4);
  }

  @Test
  void node2VecChargesTheEmbeddingMatricesOnTopOfTheWalkBuffer() {
    // The reason the budget accumulates rather than checking each component alone. On this graph the default
    // knobs need 14240 bytes of walk buffers and 8448 bytes of embedding matrices, and both are alive at the
    // same time - the walks are what the Skip-gram trains over. A budget of 20000 fits either one on its own
    // and neither pair, so a per-component check would let the call through at 22688 bytes.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 20000L);

    assertThatThrownBy(() -> drain("CALL algo.node2vec({seed: 42}) YIELD node RETURN node"))
        .as("the working set of the call is the sum of its components, not its largest one")
        .hasStackTraceContaining(
            "the embedding matrices would need 8448 bytes (2 matrices of 4 nodes x embeddingDimension=128)")
        .hasStackTraceContaining("on top of the 14240 bytes this call already reserved")
        .hasStackTraceContaining("more than the 20000 bytes allowed");
  }

  @Test
  void node2VecRejectsAnOversizedEmbeddingEvenWhenTheWalkBufferFits() {
    // The dimension is the knob #6065 capped at 4096, which bounds one embedding row at 32 KB and the matrix at
    // nothing: here the walk buffer of a single 2-step walk per node costs 176 bytes and the embeddings 262400.
    // The reservation is made before phase 1 rather than next to the allocation it covers, so a run that cannot
    // afford its embeddings does not first spend the time to generate walks it will throw away.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 100_000L);

    assertThatThrownBy(() -> drain("CALL algo.node2vec({embeddingDimension: 4096, walksPerNode: 1, walkLength: 2, "
        + "seed: 42}) YIELD node RETURN node"))
        .hasStackTraceContaining("the embedding matrices would need 262400 bytes")
        .hasStackTraceContaining("embeddingDimension=4096");
  }

  @Test
  void hashGnnRejectsFeatureMatricesLargerThanTheBudget() {
    // The feature matrices, not the embedding one, are the larger pair here: they are four times as wide, so
    // even stored as booleans they cost half a byte per dimension per node against the embedding's eight.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1024L);

    assertThatThrownBy(() -> drain("CALL algo.hashgnn({seed: 42}) YIELD node RETURN node"))
        .hasStackTraceContaining(
            "the feature matrices would need 4352 bytes (2 matrices of 4 nodes x 512 features (embeddingDimension=128 x 4))")
        .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  @Test
  void hashGnnChargesTheEmbeddingMatrixOnTopOfTheFeatureMatrices() {
    // 4352 bytes of feature matrices fit a 6000-byte budget; the 4224-byte embedding matrix on top does not.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 6000L);

    assertThatThrownBy(() -> drain("CALL algo.hashgnn({seed: 42}) YIELD node RETURN node"))
        .hasStackTraceContaining("the embedding matrix would need 4224 bytes (4 nodes x embeddingDimension=128)")
        .hasStackTraceContaining("on top of the 4352 bytes this call already reserved");
  }

  @Test
  void graphSageRejectsTheNodeFeatureMatrixLargerThanTheBudget() {
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1024L);

    assertThatThrownBy(() -> drain("CALL algo.graphsage({seed: 42}) YIELD node RETURN node"))
        .hasStackTraceContaining("the node feature matrix would need 2176 bytes (4 nodes x 64 initial features)")
        .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  @Test
  void graphSageChargesTheLayerMatricesOnTopOfTheNodeFeatures() {
    // The projection matrix is the surprise in this one: outDim x 2*initDim doubles is 67584 bytes at the
    // defaults, dwarfing both node-scaled matrices on a graph this small. Peak is per-layer, not per-run: each
    // layer drops the matrix it read, so `layers` does not multiply the reservation.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 3000L);

    assertThatThrownBy(() -> drain("CALL algo.graphsage({layers: 2, seed: 42}) YIELD node RETURN node"))
        .hasStackTraceContaining("the layer matrices would need 69760 bytes "
            + "(4 nodes x embeddingDimension=64 plus a projection of 64 x 128)")
        .hasStackTraceContaining("on top of the 2176 bytes this call already reserved");
  }

  // ── The square matrices, which no knob sizes at all ──────────────────────

  @Test
  void apspRejectsADistanceMatrixLargerThanTheBudget() {
    // Floyd-Warshall's matrix is nodeCount x nodeCount with no parameter involved: 800 MB at 10 000 nodes. The
    // procedure documents itself as suitable "up to a few thousand vertices", which until now was advice
    // rather than a bound.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 100L);

    assertThatThrownBy(() -> drain("CALL algo.apsp() YIELD source, target, distance RETURN distance"))
        .hasStackTraceContaining("the distance matrix would need 256 bytes (4 x 4 nodes)")
        .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  @Test
  void simRankRejectsSimilarityMatricesLargerThanTheBudget() {
    // A question about two nodes that allocates two full nodeCount x nodeCount matrices, because the
    // similarity of one pair is defined recursively over every pair.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 100L);

    assertThatThrownBy(() -> drain("""
        MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) \
        CALL algo.simRank(a, c) YIELD similarity \
        RETURN similarity"""))
        .hasStackTraceContaining("the similarity matrices would need 512 bytes (2 matrices of 4 x 4 nodes)")
        .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  @Test
  void maxFlowRejectsCapacityMatricesLargerThanTheBudget() {
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 100L);

    assertThatThrownBy(() -> drain("""
        MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) \
        CALL algo.maxFlow(a, c) YIELD maxFlow \
        RETURN maxFlow"""))
        .hasStackTraceContaining("the capacity and residual matrices would need 512 bytes (2 matrices of 4 x 4 nodes)")
        .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  @Test
  void kShortestPathsRejectsTheWeightMatrixLargerThanTheBudget() {
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 100L);

    assertThatThrownBy(() -> drain("""
        MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) \
        CALL algo.kShortestPaths(a, c, 2) YIELD weight \
        RETURN weight"""))
        // 328 rather than the 400 this reservation quoted before #6289: the removed-edge mask used to be
        // priced as a square matrix, because it was allocated as one - once per spur node. It is now two
        // node-sized masks allocated once for the whole call.
        .hasStackTraceContaining("the weight matrix and the spur masks would need 328 bytes "
            + "(a double matrix of 4 x 4 nodes and two boolean masks of 4 nodes)")
        .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  // ── The one knob supplied as data rather than as a number ────────────────

  @Test
  void steinerTreeRejectsDijkstraTablesLargerThanTheBudget() {
    // `terminalNodes` is a list, so unlike every other knob in the package it has no numeric form to validate:
    // its length sizes a terminals x nodeCount pair of tables, and nothing bounds it - not even the node count,
    // since repeating the same vertex is accepted. Two terminals over 4 nodes are 128 + 96 bytes.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 100L);

    assertThatThrownBy(() -> drain("""
        MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) \
        CALL algo.steinerTree([a, c]) YIELD weight \
        RETURN weight"""))
        .hasStackTraceContaining("the per-terminal Dijkstra tables would need 224 bytes (2 terminals x 4 nodes)")
        .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  @Test
  void steinerTreeRejectsATerminalListWhosePairCountWrapsInt() {
    // 46342 terminals is the first length at which `t * (t - 1)` wraps int. The wrap is NOT avoided by the
    // result fitting an int - the division by 2 happens after the product - so the old expression came back
    // negative and `new int[pairCount]` died as a bare NegativeArraySizeException naming nothing. In long the
    // count is 1073767311 pairs, ~43 GB across the four parallel arrays, which the DEFAULT budget refuses:
    // this is the one case where the two defects are the same defect, and quoting the true pair count in the
    // message is what proves the arithmetic no longer wraps.
    assertThatThrownBy(() -> drain("""
        UNWIND range(1, 46342) AS i \
        MATCH (a:Node {name: 'A'}) \
        WITH collect(a) AS terminals \
        CALL algo.steinerTree(terminals) YIELD weight \
        RETURN weight"""))
        .as("the terminal-pair count must be computed in long and priced, not wrapped into the allocator")
        .hasStackTraceContaining("the terminal-pair arrays would need")
        .hasStackTraceContaining("1073767311 pairs over 46342 terminals")
        .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
  }

  @Test
  void steinerTreeRejectsMoreTerminalPairsThanAJavaArrayCanHoldEvenWithTheBudgetDisabled() {
    // The budget is what normally catches an oversized terminal list, but past 65536 terminals the pair count
    // exceeds Integer.MAX_VALUE and no heap setting makes those array entries legal.
    database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);

    assertThatThrownBy(() -> drain("""
        UNWIND range(1, 70000) AS i \
        MATCH (a:Node {name: 'A'}) \
        WITH collect(a) AS terminals \
        CALL algo.steinerTree(terminals) YIELD weight \
        RETURN weight"""))
        .hasStackTraceContaining("70000 terminalNodes make 2449965000 terminal pairs, more than the 2147483647");
  }

  // ── Nothing legitimate got refused ───────────────────────────────────────

  @Test
  void everyPricedProcedureStillRunsUnderTheDefaultBudget() {
    // The counterweight to all of the above: at the default budget - never touched by this test - none of the
    // newly priced procedures may refuse a call it used to serve. A bound that also rejects the legitimate run
    // is not a fix.
    assertThat(drain("CALL algo.fastrp({seed: 42}) YIELD node RETURN node")).hasSize(4);
    assertThat(drain("CALL algo.hashgnn({seed: 42}) YIELD node RETURN node")).hasSize(4);
    assertThat(drain("CALL algo.graphsage({seed: 42}) YIELD node RETURN node")).hasSize(4);
    assertThat(drain("CALL algo.node2vec({walkLength: 5, walksPerNode: 2, seed: 42}) YIELD node RETURN node")).hasSize(4);
    assertThat(drain("CALL algo.apsp() YIELD distance RETURN distance")).isNotEmpty();
    assertThat(drain("""
        MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) \
        CALL algo.simRank(a, c) YIELD similarity \
        RETURN similarity""")).hasSize(1);
    assertThat(drain("""
        MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) \
        CALL algo.maxFlow(a, c) YIELD maxFlow \
        RETURN maxFlow""")).hasSize(1);
    assertThat(drain("""
        MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) \
        CALL algo.kShortestPaths(a, c, 2) YIELD weight \
        RETURN weight""")).isNotEmpty();
    assertThat(drain("""
        MATCH (a:Node {name: 'A'}) \
        CALL algo.randomWalk(a, 10) YIELD steps \
        RETURN steps""")).hasSize(1);
    assertThat(drain("""
        MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) \
        CALL algo.steinerTree([a, c]) YIELD weight \
        RETURN weight""")).isNotEmpty();
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

  private List<Result> drain(final String query) {
    final ResultSet rs = database.query("opencypher", query);
    final List<Result> results = new ArrayList<>();
    while (rs.hasNext())
      results.add(rs.next());
    return results;
  }
}
