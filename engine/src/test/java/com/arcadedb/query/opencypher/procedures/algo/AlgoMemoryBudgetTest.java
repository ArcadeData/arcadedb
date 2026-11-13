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
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PageManager;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.olap.GraphAnalyticalView;
import com.arcadedb.query.opencypher.procedures.algo.AbstractAlgoProcedure.GraphData.WeightedAdjacency;
import com.arcadedb.query.opencypher.procedures.algo.AbstractAlgoProcedure.GraphData;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.HeapUsage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The working-memory budget of the {@code algo.*} procedures: what each dense working set, graph load and edge array reserves against it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AlgoMemoryBudgetTest {
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
   */
  @Nested
  class WorkingMemoryBudget {
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
          // 14624 rather than 14240: since #6317 the graph itself is priced, and the 4-node fixture costs 384 bytes.
          .hasStackTraceContaining("on top of the 14624 bytes this call already reserved")
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
    void hashGnnRejectsSampleMatricesLargerThanTheBudget() {
      // Since #11144 the working set is sized by embeddingDensity (default embeddingDimension / 8 = 16), not by the
      // dimension: two int matrices of 4 nodes x 16 samples are 2 x (4 x (32 + 16 x 4)) = 768 bytes with row headers.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 512L);

      assertThatThrownBy(() -> drain("CALL algo.hashgnn({seed: 42}) YIELD node RETURN node"))
          .hasStackTraceContaining(
              "the sample matrices would need 768 bytes (2 matrices of 4 nodes x embeddingDensity=16)")
          .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
    }

    @Test
    void hashGnnChargesTheRaceMatricesOnTopOfTheSampleMatrices() {
      // 768 bytes of sample matrices and the 384 of the loaded graph fit a 2000-byte budget; the 1280 bytes of race
      // times on top (doubles, 2 x (4 x (32 + 16 x 8))) do not.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 2000L);

      assertThatThrownBy(() -> drain("CALL algo.hashgnn({seed: 42}) YIELD node RETURN node"))
          .hasStackTraceContaining(
              "the race-time matrices would need 1280 bytes (2 matrices of 4 nodes x embeddingDensity=16)")
          .hasStackTraceContaining("on top of the 1152 bytes this call already reserved");
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
          // 2560 rather than 2176: the 384 bytes of the loaded graph, priced since #6317.
          .hasStackTraceContaining("on top of the 2560 bytes this call already reserved");
    }

    // ── The square matrices, which no knob sizes at all ──────────────────────

    @Test
    void apspRejectsADistanceMatrixLargerThanTheBudget() {
      // Floyd-Warshall's matrix is nodeCount x nodeCount with no parameter involved: 800 MB at 10 000 nodes. The
      // procedure documents itself as suitable "up to a few thousand vertices", which until now was advice
      // rather than a bound.
      // 500 rather than 100: since #6317 the graph is the first thing reserved, and the 4-node fixture costs 384
      // bytes, so a budget below that refuses the load and never reaches the matrix this test is about.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 500L);

      assertThatThrownBy(() -> drain("CALL algo.apsp() YIELD source, target, distance RETURN distance"))
          .hasStackTraceContaining("the distance matrix would need 256 bytes (4 x 4 nodes)")
          .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
    }

    @Test
    void simRankRejectsSimilarityMatricesLargerThanTheBudget() {
      // A question about two nodes that allocates two full nodeCount x nodeCount matrices, because the
      // similarity of one pair is defined recursively over every pair.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 500L);

      assertThatThrownBy(() -> drain("""
          MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) \
          CALL algo.simRank(a, c) YIELD similarity \
          RETURN similarity"""))
          .hasStackTraceContaining("the similarity matrices would need 512 bytes (2 matrices of 4 x 4 nodes)")
          .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
    }

    @Test
    void maxFlowRejectsCapacityMatricesLargerThanTheBudget() {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 500L);

      assertThatThrownBy(() -> drain("""
          MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) \
          CALL algo.maxFlow(a, c) YIELD maxFlow \
          RETURN maxFlow"""))
          .hasStackTraceContaining("the capacity and residual matrices would need 512 bytes (2 matrices of 4 x 4 nodes)")
          .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
    }

    @Test
    void kShortestPathsRejectsTheSearchStateLargerThanTheBudget() {
      // 400 bytes admit the loaded graph (4 x OLTP_VERTEX_BYTES = 384) and refuse what Yen's holds beside it.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 400L);

      assertThatThrownBy(() -> drain("""
          MATCH (a:Node {name: 'A'}), (c:Node {name: 'C'}) \
          CALL algo.kShortestPaths(a, c, 2) YIELD weight \
          RETURN weight"""))
          // 72 bytes: before #9914 this was a 4 x 4 weight matrix beside the masks (328 bytes, and 400 before #6289,
          // when the removed-edge mask was a square matrix too). Yen's now runs on the adjacency lists, and what it
          // holds beside them is node-sized: a distance, a predecessor and a visit slot per node, and the two masks.
          .hasStackTraceContaining("the path search state would need 72 bytes "
              + "(distances, predecessors, a visit stack and two boolean masks of 4 nodes)")
          .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
    }

    // ── The one knob supplied as data rather than as a number ────────────────

    @Test
    void steinerTreeRejectsDijkstraTablesLargerThanTheBudget() {
      // `terminalNodes` is a list, so unlike every other knob in the package it has no numeric form to validate:
      // its length sizes a terminals x nodeCount pair of tables, and nothing bounds it - not even the node count,
      // since repeating the same vertex is accepted. Two terminals over 4 nodes are 128 + 96 bytes.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 500L);

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

  /**
   * Issue #11011: the default working-memory budget of an algorithm call was a fixed eighth of the maximum heap, so on a
   * 2 GB heap {@code algo.wcc()} was refused an adjacency of 280 MB that the heap had room for. Left at its default (0) the
   * budget is what the heap can give the call: half of the heap free after the last collection plus the page cache, which
   * the call takes back before it allocates, and never less than the fixed budget it replaces. A configured value is still
   * a fixed budget.
   */
  @Nested
  class WorkingMemoryAutoBudget extends TestHelper {

    @AfterEach
    void restoreDefault() {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
          GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
    }

    @Test
    void theDefaultIsSizedFromTheHeap() {
      assertThat(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue()).isEqualTo(0L);
    }

    /**
     * The oracle of the fix, driven with a fixed heap reading (issue #11030): it used to read the live heap behind an
     * {@code assumeTrue}, which aborted - and so passed - on a small or crowded test JVM, with the fix reverted or not.
     */
    @Test
    void theDefaultAdmitsAWorkingSetPastTheFixedBudgetWhenTheHeapHasRoom() {
      final long floor = AbstractAlgoProcedure.fixedBudgetFloor();
      final long free = 8 * floor;

      // Reserving allocates nothing: the budget only decides. Half of the free heap is 4 floors, within reach without
      // taking a page of the cache back, so the page cache this JVM holds does not enter the verdict either
      final AbstractAlgoProcedure.MemoryBudget budget = new AlgoWCC().newMemoryBudget(database, () -> free);
      budget.reserve(floor + 1024 * 1024, "the adjacency list", "test");
      budget.reserve(2 * floor, "the degree buffer", "test");

      // Past half of the free heap and the cache it is refused, and the message says what the limit was sized from
      assertThatThrownBy(() -> budget.reserve(free + PageManager.INSTANCE.getReadCacheRAM(), "the visited vertices", "test"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("(half of the " + free + " bytes of heap free after the last collection");
    }

    @Test
    void anUnknownHeapReadingKeepsTheFixedBudget() {
      final long floor = AbstractAlgoProcedure.fixedBudgetFloor();
      final AbstractAlgoProcedure.MemoryBudget budget = new AlgoWCC().newMemoryBudget(database, () -> -1L);
      budget.reserve(floor, "the adjacency list", "test");
      assertThatThrownBy(() -> budget.reserve(1, "the degree buffer", "test"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("more than the " + floor + " bytes allowed.");
    }

    @Test
    void aConfiguredValueIgnoresTheHeapReading() {
      final long floor = AbstractAlgoProcedure.fixedBudgetFloor();
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, floor);
      final AbstractAlgoProcedure.MemoryBudget budget = new AlgoWCC().newMemoryBudget(database, () -> 100 * floor);
      assertThatThrownBy(() -> budget.reserve(floor + 1, "the adjacency list", "test"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("more than the " + floor + " bytes allowed.");
    }

    @Test
    void theDefaultStillRefusesWhatTheHeapCannotHold() {
      final AbstractAlgoProcedure.MemoryBudget budget = new AlgoWCC().newMemoryBudget(database);
      assertThatThrownBy(() -> budget.reserve(HeapUsage.maxHeapBytes() * 2, "the adjacency list", "test"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("the adjacency list would need")
          .hasMessageContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
    }

    @Test
    void aConfiguredValueIsAFixedBudget() {
      final long floor = AbstractAlgoProcedure.fixedBudgetFloor();
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, floor);
      final AbstractAlgoProcedure.MemoryBudget budget = new AlgoWCC().newMemoryBudget(database);
      budget.reserve(floor, "the adjacency list", "test");
      assertThatThrownBy(() -> budget.reserve(1, "the degree buffer", "test"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("more than the " + floor + " bytes allowed.");
    }

    @Test
    void aWalkExtendsItsCapacityWithoutReservingUntilTheLimit() {
      final long budgetBytes = 1000L;
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, budgetBytes);
      final AbstractAlgoProcedure.MemoryBudget budget = new AlgoWCC().newMemoryBudget(database);
      assertThat(budget.capacityFor(10)).isEqualTo(100);
      // Extending reserves nothing: the walk reserves what it holds at the end, and the whole budget is still there for it
      assertThat(budget.extendCapacity(100, 10, "the visited vertices", "test")).isGreaterThanOrEqualTo(100);
      budget.reserve(budgetBytes, "the visited vertices", "test");
      assertThatThrownBy(() -> budget.extendCapacity(1, 10, "the visited vertices", "at least 1 nodes"))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("the visited vertices would need 10 bytes (at least 1 nodes)");
    }

    @Test
    void theOldGenerationPoolsAreTheOnesReadAtTheirCurrentOccupancy() {
      assertThat(HeapUsage.isOldGenerationPool("G1 Old Gen")).isTrue();
      assertThat(HeapUsage.isOldGenerationPool("PS Old Gen")).isTrue();
      assertThat(HeapUsage.isOldGenerationPool("Tenured Gen")).isTrue();
      assertThat(HeapUsage.isOldGenerationPool("ZGC Old Generation")).isTrue();
      assertThat(HeapUsage.isOldGenerationPool("G1 Eden Space")).isFalse();
      assertThat(HeapUsage.isOldGenerationPool("ZGC Young Generation")).isFalse();
      assertThat(HeapUsage.isOldGenerationPool("ZHeap")).isFalse();
      assertThat(HeapUsage.isOldGenerationPool(null)).isFalse();
    }

    @Test
    void theFloorIsAnEighthOfTheHeapAndNeverBelow64MB() {
      assertThat(AbstractAlgoProcedure.fixedBudgetFloor()).isEqualTo(
          Math.max(64L * 1024 * 1024, HeapUsage.maxHeapBytes() / 8));
    }

    @Test
    void aWorkingSetPastTheFreeHeapTakesItsShareBackFromThePageCache() {
      final long mb = 1024 * 1024;
      // Fits without the cache: nothing to give up
      assertThat(AbstractAlgoProcedure.pageCacheToReclaim(300 * mb, 1000 * mb, 500 * mb, 0)).isZero();
      // 700 MB on 1000 MB free: twice the working set is 1400 MB, so 400 MB of cache goes first
      assertThat(AbstractAlgoProcedure.pageCacheToReclaim(700 * mb, 1000 * mb, 500 * mb, 0)).isEqualTo(400 * mb);
      // A later reservation of the same call only takes what the earlier ones did not
      assertThat(AbstractAlgoProcedure.pageCacheToReclaim(800 * mb, 1000 * mb, 500 * mb, 400 * mb)).isEqualTo(200 * mb);
      // A cache that grew by 100 MB since the sizing gives its growth back first: 140 MB on 200 MB free needs a net reduction
      // of 80 MB, so 180 MB are evicted, not 80 MB that would leave the cache larger than it was counted
      assertThat(AbstractAlgoProcedure.pageCacheToReclaim(140 * mb, 200 * mb, 100 * mb, -100 * mb)).isEqualTo(180 * mb);
      // An unknown heap reading reclaims nothing: the budget stayed at its floor
      assertThat(AbstractAlgoProcedure.pageCacheToReclaim(800 * mb, -1, Long.MAX_VALUE, 0)).isZero();
    }
  }

  /**
   * Issue #6317: the graph an {@code algo.*} call loads is the first allocation of every procedure in the package
   * and was priced by no budget - so a call could be refused for a 64 MB matrix having already allocated a
   * multi-gigabyte graph to measure it against, and a call that was accepted held that graph for its whole
   * duration with {@code arcadedb.cypher.algoMaxWorkingMemory} none the wiser.
   * <p>
   * Three allocations, now all reserved: the {@code List<Vertex>} of loaded records, the {@code Map<RID, Integer>}
   * index built beside it (a boxed {@code Integer} and a {@code HashMap.Node} per vertex), and the {@code int[][]}
   * adjacency, which is the larger of the two - 4 bytes per edge plus a row header per node, with
   * {@code Vertex.DIRECTION.BOTH} materialising each edge twice.
   * <p>
   * Both are bounded where the count becomes known rather than after the structure exists: the vertex walk carries
   * its own stopping rule through {@code MemoryBudget.capacityFor}, and the adjacency build already counts every
   * entry before it allocates a single row, so the total is handed to the budget at the one moment the result is
   * still free to refuse. That placement is the difference between refusing a call and paying in full for a
   * traversal before refusing it - the same argument {@code algo.mst} settled in issue #6300.
   * <p>
   * The budget is also now one budget per call rather than one per component: the graph is charged first and every
   * later reservation accumulates on top of it, which is what {@link #theWorkingSetAccumulatesOnTopOfTheGraph()}
   * holds it to.
   */
  @Nested
  class GraphLoadBudget {
    /** Eleven nodes, so the loaded graph is a figure the assertions below can name exactly. */
    private static final int NODE_COUNT = 11;

    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6317-graph-load-budget");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");

      // A path, so the graph is connected and every procedure below has something to traverse.
      database.transaction(() -> {
        final List<MutableVertex> nodes = new ArrayList<>(NODE_COUNT);
        for (int i = 0; i < NODE_COUNT; i++)
          nodes.add(database.newVertex("Node").set("idx", i).set("name", "N" + i).save());
        for (int i = 0; i < NODE_COUNT - 1; i++)
          nodes.get(i).newEdge("LINK", nodes.get(i + 1), true, new Object[] { "w", (double) (i + 1) }).save();
      });
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
            GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
        database.drop();
        database = null;
      }
    }

    // ── The graph itself ─────────────────────────────────────────────────────

    @Test
    void theLoadedGraphIsRefusedWhenItDoesNotFitTheBudget() {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 100L);

      assertThatThrownBy(() -> drain("CALL algo.pageRank() YIELD node RETURN node"))
          .as("the graph is the largest thing the working set holds and must be priced like any other component")
          .hasStackTraceContaining("algo.pagerank(): the loaded graph would need")
          .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
    }

    @Test
    void theRefusalNamesTheNodeCountItStoppedAt() {
      // The walk stops when the budget runs out, so the message quotes the count reached rather than the graph's
      // total - there is no total to quote yet. That is the observable difference between bounding the walk and
      // reserving after it, and it is the whole point of the placement.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 100L);

      assertThatThrownBy(() -> drain("CALL algo.pageRank() YIELD node RETURN node"))
          .hasStackTraceContaining("at least 2 nodes");
    }

    @Test
    void theProceduresThatLoadTheirOwnVertexListArePricedToo() {
      // algo.maxFlow, algo.kShortestPaths and algo.mst build the list and the RID index by hand rather than
      // through loadGraph, and went on bypassing the budget when loadGraph stopped doing so.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 100L);

      for (final String[] call : new String[][] {
          { "algo.maxFlow", "MATCH (a:Node {idx: 0}), (z:Node {idx: 10}) CALL algo.maxFlow(a, z) YIELD maxFlow RETURN maxFlow" },
          { "algo.kShortestPaths",
              "MATCH (a:Node {idx: 0}), (z:Node {idx: 10}) CALL algo.kShortestPaths(a, z, 2) YIELD weight RETURN weight" },
          { "algo.mst", "CALL algo.mst(null, 'w') YIELD source RETURN source" } })
        assertThatThrownBy(() -> drain(call[1]))
            .as("%s loads the same graph and must price it", call[0])
            .hasStackTraceContaining(call[0] + "(): the loaded graph would need");
    }

    // ── The adjacency ────────────────────────────────────────────────────────

    @Test
    void theAdjacencyListIsRefusedWhenItDoesNotFitTheBudget() {
      // 11 nodes cost 1056 bytes, so a budget just above that admits the graph and leaves nothing for the
      // adjacency - which is the larger of the two on any real graph. algo.wcc rather than algo.pagerank here
      // because it takes its neighbour lists through GraphData.adjacency, which is where the copy is priced.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1100L);

      assertThatThrownBy(() -> drain("CALL algo.wcc() YIELD node RETURN node"))
          .hasStackTraceContaining("the adjacency list would need")
          .hasStackTraceContaining(NODE_COUNT + " nodes, ")
          .hasStackTraceContaining("edge entries");
    }

    @Test
    void theWorkingSetAccumulatesOnTopOfTheGraph() {
      // One budget per call, not one per component: the distance matrix is charged on top of the graph and the
      // adjacency, so the message names what was already reserved.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1500L);

      assertThatThrownBy(() -> drain("CALL algo.apsp() YIELD distance RETURN distance"))
          .hasStackTraceContaining("bytes this call already reserved");
    }

    // ── The counterweights ───────────────────────────────────────────────────

    @Test
    void aBudgetThatFitsLetsTheCallThrough() {
      // The reservation must not refuse a graph it can serve: at the default budget this one is trivial.
      assertThat(drain("CALL algo.pageRank() YIELD node RETURN node")).hasSize(NODE_COUNT);
      assertThat(drain("CALL algo.wcc() YIELD node RETURN node")).hasSize(NODE_COUNT);
    }

    @Test
    void aDisabledBudgetPricesNothing() {
      // Negative means no limit, and that has to reach the graph load and the adjacency too.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);

      assertThat(drain("CALL algo.pageRank() YIELD node RETURN node")).hasSize(NODE_COUNT);
      assertThat(drain("CALL algo.mst(null, 'w') YIELD source RETURN source")).hasSize(NODE_COUNT - 1);
    }

    private List<Object> drain(final String query) {
      final List<Object> rows = new ArrayList<>();
      try (final ResultSet resultSet = database.query("opencypher", query)) {
        while (resultSet.hasNext())
          rows.add(resultSet.next());
      }
      return rows;
    }
  }

  /**
   * Issue #6375: five algorithms built a {@code nodeCount x nodeCount} {@code BitSet} matrix up front,
   * unconditionally, without reserving against {@code arcadedb.cypher.algoMaxWorkingMemory} - the budget whose own
   * documentation names the {@code nodeCount x nodeCount} matrices as exactly what it exists to cap.
   * <p>
   * {@code new BitSet(n)} allocates {@code ceil(n/64)} longs whatever the node's real degree turns out to be, so a
   * {@code BitSet[n]} of them is a genuine {@code n²/8}-byte structure. Eight times denser per element than the
   * {@code double[n][n]} matrices the budget already refused, it slipped through to a far larger graph and then
   * exhausted the heap - the {@code OutOfMemoryError} the budget exists to replace with a client error naming the
   * component and the setting.
   * <p>
   * The build loops were also the unguarded phase of an otherwise abortable call, the same blind spot issue #6295
   * closed for the SLPA/GraphSAGE/HashGNN initialisation loops: the consumption loops carried the checkpoint and
   * the {@code O(n²/64)} build ahead of them did not.
   * <p>
   * {@code algo.triangleCount} was the fifth, and no longer builds one at all: since issue #10203 it intersects
   * sorted neighbour sets, so its working memory is the adjacency it already prices.
   */
  @Nested
  class BitsetMatrixBudget {
    /**
     * A budget that admits the four-node graph and its adjacency and refuses the bit matrix on top of them. The
     * matrix itself is 288 bytes at this size; what matters to the assertions below is only that it is the
     * component the refusal names.
     */
    private static final long BUDGET_ABOVE_THE_GRAPH_BELOW_THE_MATRIX = 700L;

    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6375-bitset-budget");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");

      // A directed cycle over four nodes: every node has a neighbour in both directions, so each of the four
      // procedures has a matrix to build and something to find in it.
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("Node").set("name", "A").save();
        final MutableVertex b = database.newVertex("Node").set("name", "B").save();
        final MutableVertex c = database.newVertex("Node").set("name", "C").save();
        final MutableVertex d = database.newVertex("Node").set("name", "D").save();
        a.newEdge("LINK", b, true, (Object[]) null).save();
        b.newEdge("LINK", c, true, (Object[]) null).save();
        c.newEdge("LINK", d, true, (Object[]) null).save();
        d.newEdge("LINK", a, true, (Object[]) null).save();
        a.newEdge("LINK", c, true, (Object[]) null).save();
      });
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
            GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
        database.drop();
        database = null;
      }
    }

    /** The four procedures, each with a call that reaches its bit matrix. */
    private static final String[][] CALLS = {
        { "algo.knn", "CALL algo.knn(2) YIELD node RETURN node" },
        { "algo.kTruss", "CALL algo.kTruss() YIELD node RETURN node" },
        { "algo.clique", "CALL algo.clique() YIELD clique RETURN clique" },
        { "algo.hierarchicalClustering", "CALL algo.hierarchicalClustering(null, 2) YIELD node RETURN node" } };

    @Test
    void everyBitsetMatrixIsRefusedWhenItDoesNotFitTheBudget() {
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
          BUDGET_ABOVE_THE_GRAPH_BELOW_THE_MATRIX);

      for (final String[] call : CALLS)
        assertThatThrownBy(() -> drain(call[1]))
            .as("%s builds a nodeCount x nodeCount bit matrix and must price it", call[0])
            .hasStackTraceContaining(call[0] + "(): the neighbour bitsets")
            .hasStackTraceContaining("(4 x 4 nodes)")
            .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
    }

    @Test
    void triangleCountNeedsNoMatrixAnyMore() {
      // The budget that refuses every bit matrix above serves a sorted-merge triangle count: it holds the adjacency
      // and the result, both already admitted (issue #10203).
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
          BUDGET_ABOVE_THE_GRAPH_BELOW_THE_MATRIX);

      assertThat(drain("CALL algo.triangleCount() YIELD node RETURN node")).hasSize(4);
    }

    @Test
    void kTrussPricesItsSecondMatrixSeparately() {
      // computeFullTrussDecomposition builds a second bit matrix while the first is still alive, so the peak is
      // twice the shape - which is what a running total is for.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1000L);

      assertThatThrownBy(() -> drain("CALL algo.kTruss() YIELD node RETURN node"))
          .hasStackTraceContaining("the neighbour bitsets of the truss decomposition")
          .hasStackTraceContaining("(4 x 4 nodes)");
    }

    @Test
    void cliquePricesItsSearchStackAtThePeakDepth() {
      // The Bron-Kerbosch frames are five bit sets each and the stack is as deep as the largest clique reached,
      // so at its peak the stack is a second nodeCount x nodeCount structure.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1000L);

      assertThatThrownBy(() -> drain("CALL algo.clique() YIELD clique RETURN clique"))
          .hasStackTraceContaining("the clique search stack")
          .hasStackTraceContaining("frame");
    }

    @Test
    void aBudgetThatFitsLetsEveryCallThrough() {
      // The counterweight: the reservation must not refuse a graph it can serve. At the default budget none of
      // these four is anywhere near the limit.
      for (final String[] call : CALLS)
        assertThat(drain(call[1])).as("%s must still run at the default budget", call[0]).isNotNull();
    }

    @Test
    void aDisabledBudgetPricesNothing() {
      // Negative means no limit, and that has to keep meaning no limit for the newly priced component too.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);

      for (final String[] call : CALLS)
        assertThat(drain(call[1])).as("%s must be unbounded when the budget is disabled", call[0]).isNotNull();
    }

    private List<Object> drain(final String query) {
      final List<Object> rows = new ArrayList<>();
      try (final ResultSet resultSet = database.query("opencypher", query)) {
        while (resultSet.hasNext())
          rows.add(resultSet.next());
      }
      return rows;
    }
  }

  /**
   * Regression tests for issue #6300 - {@code algo.mst} was the one dense {@code algo.*} path the #6263
   * working-memory budget did not price.
   * <p>
   * #6263 reserved the dense working set of every procedure that builds one, choosing them by two criteria: sized by
   * a knob, or quadratic in the node count. {@code algo.mst} is neither. It is linear in the <em>edge</em> count -
   * three parallel arrays plus the index sort, 24 bytes per edge - which reads like the graph paying for itself.
   * But linear in the edge count is not small: the edge count is the largest linear dimension a graph has, usually
   * an order of magnitude above the node count, and 100M edges is ~2.4 GB requested with no check and no error
   * naming what asked for it. "Linear" was never the criterion; the criterion is whether the caller can predict a
   * ceiling, and here there is none.
   * <p>
   * Originally the reservation was made as a dedicated counting pass ran, rather than once it had finished, so a
   * refusal could stop before materialising a traversal it would then throw away. Issue #6316 moved edge collection
   * onto {@code GraphData.weightedAdjacency} - the shared helper every other weighted {@code algo.*} procedure
   * already reads edges through - and that dedicated pass went with it. A PR #6714 review round found this had
   * silently dropped the protection: {@code weightedAdjacency} itself reserved nothing, so the (potentially huge)
   * neighbour/weight arrays were fully materialised before {@code algo.mst}'s own {@code reserve()} call ever ran.
   * {@code weightedAdjacency} now reserves incrementally as it builds - a row-header reservation up front, then
   * per-edge-entry reservations as the walk proceeds, mirroring {@code GraphData.adjacency()}'s existing
   * {@code reserveAdjacency} pattern - so the refusal happens inside {@code weightedAdjacency} itself, and for a
   * graph this small it fires at the row-header reservation, before a single edge is read.
   */
  @Nested
  class MSTEdgeBudget {
    /** Ten edges, so a budget sized for four of them is refused while a default budget is not. */
    private static final int EDGE_COUNT = 10;

    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6300-mst-edge-budget");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      // Declared so a Graph Analytical View can materialise "w" as an edge property column (Issue6301's setup
      // note: without a schema-known property the view falls back to edge records, which would leave the
      // columnar reservation path - weightedAdjacencyFromColumns - untested by
      // theWeightedAdjacencyRowHeadersAreRefusedOnTheCSRColumnarPathToo).
      database.getSchema().createEdgeType("LINK").createProperty("w", Type.DOUBLE);

      // A path of EDGE_COUNT + 1 nodes: connected, so the MST spans every node and no edge is redundant.
      database.transaction(() -> {
        final List<MutableVertex> nodes = new ArrayList<>(EDGE_COUNT + 1);
        for (int i = 0; i <= EDGE_COUNT; i++)
          nodes.add(database.newVertex("Node").set("idx", i).save());
        for (int i = 0; i < EDGE_COUNT; i++)
          nodes.get(i).newEdge("LINK", nodes.get(i + 1), true, new Object[] { "w", (double) (i + 1) }).save();
      });
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
            GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }

    @Test
    void theWeightedAdjacencyRowHeadersAreRefusedWhenTheyDoNotFitTheBudget() {
      // The graph is priced first since #6317 - 11 nodes at OLTP_VERTEX_BYTES is 1056 bytes. weightedAdjacency
      // reserves its row headers next, up front, before reading a single edge: 2 row headers per node (one for
      // neighbors[i], one for weights[i]) at MATRIX_ROW_OVERHEAD_BYTES=32 each, so 11 nodes cost 704 bytes -
      // 1056 + 704 = 1760, over the 1156-byte budget before any edge is even looked at.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1156L);

      assertThatThrownBy(() -> drain("CALL algo.mst(null, 'w') YIELD source, target RETURN source, target"))
          .as("the weighted adjacency list is the dense working set of an algo.mst call and must be priced like any other")
          .hasStackTraceContaining("algo.mst(): the weighted adjacency list would need")
          .hasStackTraceContaining("0 edge entries")
          .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
    }

    /**
     * PR #6714 review round 13: the tests around this one only exercise {@code weightedAdjacencyFromRecords}
     * (the OLTP fallback) since no Graph Analytical View is built - {@code weightedAdjacencyFromColumns}, the
     * path actually used once a view is ready and the one repeatedly patched for reservation-ordering bugs across
     * this PR's earlier review rounds, was never exercised by a budget-refusal test. {@code CSR_ACCELERATED_VAR}
     * confirms the columnar path was actually taken, not silently the OLTP one the test above already covers.
     * <p>
     * The budget cannot reuse {@link #theWeightedAdjacencyRowHeadersAreRefusedWhenTheyDoNotFitTheBudget}'s 1156:
     * a CSR-backed {@code GraphData} is built straight from the view's node count with no {@code loadVertices}
     * walk, so it never pays that test's 1056-byte OLTP_VERTEX_BYTES graph-loading charge. Only the row headers
     * (2 x 32 bytes/node) are common to both paths - 11 nodes is 704 bytes - so the budget here is sized to that
     * alone.
     */
    @Test
    void theWeightedAdjacencyRowHeadersAreRefusedOnTheCSRColumnarPathToo() {
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName("mst-budget-csr-view")
          .withVertexTypes("Node")
          .withEdgeTypes("LINK")
          .withEdgeProperties("w")
          .build();
      try {
        database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 500L);

        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        database.begin();
        try {
          assertThatThrownBy(() -> new AlgoMST().execute(new Object[] { null, "w" }, null, context))
              .as("weightedAdjacencyFromColumns must be priced the same way weightedAdjacencyFromRecords is")
              .hasStackTraceContaining("algo.mst(): the weighted adjacency list would need")
              .hasStackTraceContaining("0 edge entries")
              .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
          assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
              .as("the view must actually back the call, or this test pins nothing beyond the OLTP path already covered above")
              .isEqualTo(true);
        } finally {
          database.rollback();
        }
      } finally {
        view.drop();
      }
    }

    @Test
    void theRefusalNamesTheActualEdgeCountOnceTheRowHeadersFit() {
      // A budget wide enough for the graph (1056) plus the row headers (704) - 1760 - but not for the 10 edges'
      // entries on top of that (12 bytes each: an int neighbour id plus a double weight, 120 bytes total) fires the
      // second reservation, which quotes the edge count it actually reached.
      //
      // Issue #6795: before that fix, weightedAdjacencyFromRecords (the OLTP path this 11-node/no-view graph takes)
      // checkpointed only every 1024 nodes or 1_048_576 entries - neither ever reached here - so every edge was
      // already read into the arrays by the time the single post-loop reservation fired, reporting the full 10.
      // The fix makes it also consult capacityFor(), recomputed after every reservation exactly like the columnar
      // path already did: with this budget only 40 bytes remain once the row headers are admitted, so the interval
      // shrinks fast and the walk is refused after just 1 more edge entry - proof the checkpoint now fires mid-walk
      // instead of only once the whole adjacency list is already built.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1800L);

      // "1 edge entries" (not the full EDGE_COUNT): the record path must refuse well before materialising all 10
      // edges, mirroring the columnar path's own capacityFor-scaled checkpoint.
      assertThatThrownBy(() -> drain("CALL algo.mst(null, 'w') YIELD source RETURN source"))
          .hasStackTraceContaining("algo.mst(): the weighted adjacency list would need")
          .hasStackTraceContaining("more than the 1800 bytes allowed")
          .hasStackTraceContaining("1 edge entries");
    }

    @Test
    void aBudgetThatFitsLetsTheCallThrough() {
      // The counterweight: the reservation must not refuse a graph it can serve. 11 nodes cost 1056 bytes and
      // 10 edges at 24 bytes is 240, so 4 KB is comfortable, and the MST of a path is the path itself - every
      // edge, at its own weight.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 4096L);

      final List<Object> rows = drain("CALL algo.mst(null, 'w') YIELD source, weight, totalWeight RETURN source, weight, totalWeight");
      assertThat(rows).hasSize(EDGE_COUNT);
    }

    @Test
    void aDisabledBudgetPricesNothing() {
      // A negative limit means "no limit" throughout the budget, and it must reach the per-edge capacity too -
      // a capacity computed by dividing a negative limit would refuse every graph instead of accepting them all.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);

      assertThat(drain("CALL algo.mst(null, 'w') YIELD source RETURN source")).hasSize(EDGE_COUNT);
    }

    @Test
    void theMinimumSpanningTreeIsUnchanged() {
      // The reservation and the checkpoints must not move the answer. On a path every edge is a bridge, so the MST
      // is the whole path and its total weight is 1 + 2 + ... + EDGE_COUNT.
      final ResultSet rs = database.query("opencypher",
          "CALL algo.mst(null, 'w') YIELD weight, totalWeight RETURN weight, totalWeight");
      double sum = 0.0;
      int rows = 0;
      double reportedTotal = -1.0;
      while (rs.hasNext()) {
        final var row = rs.next();
        sum += ((Number) row.getProperty("weight")).doubleValue();
        reportedTotal = ((Number) row.getProperty("totalWeight")).doubleValue();
        rows++;
      }

      assertThat(rows).isEqualTo(EDGE_COUNT);
      assertThat(sum).isEqualTo(EDGE_COUNT * (EDGE_COUNT + 1) / 2.0);
      assertThat(reportedTotal).isEqualTo(sum);
    }

    private List<Object> drain(final String query) {
      final List<Object> rows = new ArrayList<>();
      final ResultSet rs = database.query("opencypher", query);
      while (rs.hasNext())
        rows.add(rs.next());
      return rows;
    }

    /**
     * PR #6714 review round 3 coverage gap: none of the tests above exercise a graph large enough for
     * {@code weightedAdjacency}'s incremental checkpoint (every 1024 nodes, or every {@code ADJACENCY_CHECKPOINT_ENTRIES}
     * entries) to fire mid-walk rather than only at the row-header reservation or the final one. A 3000-node chain
     * is enough: the checkpoint right after node 1023 reserves only the ~1024 edges read so far, and a budget sized
     * to admit the row headers plus that first batch but not the full 2999 edges refuses there - which this test
     * confirms not by timing but by measuring the call's own allocation (issue #6289's approach): if the refusal
     * actually happened partway through the walk rather than after materialising the whole graph, the allocated
     * bytes stay a small fraction (roughly 1024/3000) of what building the full {@code int[][]}/{@code double[][]}
     * pair for all 2999 edges would cost.
     */
    @Test
    @Tag("performance")
    void aLargeGraphIsRefusedPartwayThroughTheWalkNotAfterMaterialisingAllOfIt() {
      final com.sun.management.ThreadMXBean threads = threadAllocationBean();
      assumeTrue(threads != null, "JVM does not expose per-thread allocation counters");

      final int nodeCount = 3000;
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6300-mst-large-graph-refusal");
      if (factory.exists())
        factory.open().drop();
      final Database large = factory.create();
      try {
        large.getSchema().createVertexType("Node");
        large.getSchema().createEdgeType("LINK");
        large.transaction(() -> {
          final List<MutableVertex> nodes = new ArrayList<>(nodeCount);
          for (int i = 0; i < nodeCount; i++)
            nodes.add(large.newVertex("Node").set("idx", i).save());
          for (int i = 0; i < nodeCount - 1; i++)
            nodes.get(i).newEdge("LINK", nodes.get(i + 1), true, new Object[] { "w", (double) (i + 1) }).save();
        });

        // Graph load (96 bytes/node) plus weightedAdjacency's row headers (64 bytes/node) is 480 000 bytes; the
        // checkpoint right after node 1023 adds ~1024 edges x 12 bytes = ~12 288 more. 490 000 sits between the
        // two, so the call refuses right there - after ~1024 of 3000 nodes, not after all of them.
        final long refusalBudget = 490_000L;

        // Warm up both call shapes (JIT, class loading) before either measurement, so neither is biased by being
        // the first invocation.
        large.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, refusalBudget);
        assertThatThrownBy(() -> drain(large, "CALL algo.mst(null, 'w') YIELD source RETURN source"))
            .hasStackTraceContaining("algo.mst(): the weighted adjacency list would need");
        large.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);
        assertThat(drain(large, "CALL algo.mst(null, 'w') YIELD source RETURN source")).isNotEmpty();

        large.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);
        final long unboundedAllocated = measure(threads, () -> assertThat(drain(large, "CALL algo.mst(null, 'w') YIELD source RETURN source")).isNotEmpty());

        large.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, refusalBudget);
        final long refusedAllocated = measure(threads, () -> assertThatThrownBy(() -> drain(large, "CALL algo.mst(null, 'w') YIELD source RETURN source"))
            .hasStackTraceContaining("algo.mst(): the weighted adjacency list would need"));

        // A refusal partway through the walk must allocate much less than materialising the whole graph does - if
        // it did not stop mid-walk, the two would be close to equal instead.
        assertThat(refusedAllocated)
            .as("a refusal partway through the walk (refused=" + refusedAllocated + " bytes) must allocate much "
                + "less than materialising the whole graph does (unbounded=" + unboundedAllocated + " bytes)")
            .isLessThan(unboundedAllocated / 2);
      } finally {
        large.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
            GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
        large.drop();
      }
    }

    private static List<Object> drain(final Database db, final String query) {
      final List<Object> rows = new ArrayList<>();
      final ResultSet rs = db.query("opencypher", query);
      while (rs.hasNext())
        rows.add(rs.next());
      return rows;
    }

    private static long measure(final com.sun.management.ThreadMXBean threads, final Runnable body) {
      final long id = Thread.currentThread().threadId();
      final long before = threads.getThreadAllocatedBytes(id);
      body.run();
      return threads.getThreadAllocatedBytes(id) - before;
    }

    private static com.sun.management.ThreadMXBean threadAllocationBean() {
      if (!(ManagementFactory.getThreadMXBean() instanceof final com.sun.management.ThreadMXBean bean))
        return null;
      if (!bean.isThreadAllocatedMemorySupported())
        return null;
      bean.setThreadAllocatedMemoryEnabled(true);
      return bean.isThreadAllocatedMemoryEnabled() ? bean : null;
    }
  }

  /**
   * Regression tests for issue #6300, the {@code algo.msa} half of a PR #6714 review round-6 finding: this
   * procedure allocates its own flat {@code eFrom}/{@code eTo}/{@code eW} copy of the edges - the same shape as
   * {@code algo.mst}'s {@code eu}/{@code ev}/{@code ew} - but, unlike {@code algo.mst}, never reserved anything for
   * it. {@code weightedAdjacency}'s own incremental reservation (added in an earlier round of this same PR, see
   * {@link Issue6300AlgoMSTEdgeBudgetTest}) prices its own {@code int[][]}/{@code double[][]} pair, but says
   * nothing about a caller's additional copy on top of it - so a graph sized just under that threshold could still
   * take an unpriced ~16 bytes/edge, the same "OutOfMemoryError instead of a clean refusal" failure mode #6300 was
   * written to close.
   */
  @Nested
  class MSAEdgeBudget {
    /** Ten edges in a rooted chain, so the arborescence spans every node and no edge is redundant. */
    private static final int EDGE_COUNT = 10;

    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6300-msa-edge-budget");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      // Declared so a Graph Analytical View can materialise "w" as an edge property column (Issue6301's setup
      // note: without a schema-known property the view falls back to edge records, which would leave the
      // columnar reservation path - weightedAdjacencyFromColumns - untested by
      // theWeightedAdjacencyRowHeadersAreRefusedOnTheCSRColumnarPathToo).
      database.getSchema().createEdgeType("LINK").createProperty("w", Type.DOUBLE);

      // A directed chain of EDGE_COUNT + 1 nodes rooted at node 0: every non-root vertex has exactly one incoming
      // edge, so the arborescence is the whole chain and no edge is redundant.
      database.transaction(() -> {
        final List<MutableVertex> nodes = new ArrayList<>(EDGE_COUNT + 1);
        for (int i = 0; i <= EDGE_COUNT; i++)
          nodes.add(database.newVertex("Node").set("idx", i).save());
        for (int i = 0; i < EDGE_COUNT; i++)
          nodes.get(i).newEdge("LINK", nodes.get(i + 1), true, new Object[] { "w", (double) (i + 1) }).save();
      });
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY,
            GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getDefValue());
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }

    @Test
    void theEdgeArraysAreRefusedWhenTheyDoNotFitTheBudget() {
      // 11 nodes at OLTP_VERTEX_BYTES=96 is 1056, weightedAdjacency's row headers (2 x 32 bytes/node) are 704, and
      // its 10 edge entries (12 bytes each) are 120 - 1880 total, all of which fits comfortably below this budget.
      // What does not fit is algo.msa's own eFrom/eTo/eW copy on top: 10 edges x 16 bytes = 160, taking the total
      // to 2040 - over a 1900-byte budget, refusing at this procedure's own reservation rather than
      // weightedAdjacency's.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 1900L);

      assertThatThrownBy(() -> drain("MATCH (r:Node {idx: 0}) CALL algo.msa(r, 'LINK', 'w') YIELD source RETURN source"))
          .as("algo.msa's own edge arrays are additional working set on top of weightedAdjacency's and must be priced too")
          .hasStackTraceContaining("algo.msa(): the edge arrays would need")
          .hasStackTraceContaining(EDGE_COUNT + " edges")
          .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
    }

    /**
     * PR #6714 review round 13: the tests around this one only exercise {@code weightedAdjacencyFromRecords}
     * (the OLTP fallback) since no Graph Analytical View is built - {@code weightedAdjacencyFromColumns}, the
     * path actually used once a view is ready, was never exercised by a budget-refusal test on the {@code
     * algo.msa} side either (see {@code Issue6300AlgoMSTEdgeBudgetTest#theWeightedAdjacencyRowHeadersAreRefusedOnTheCSRColumnarPathToo}
     * for the {@code algo.mst} counterpart and the reasoning behind the budget). {@code CSR_ACCELERATED_VAR}
     * confirms the columnar path was actually taken.
     * <p>
     * A CSR-backed {@code GraphData} pays none of {@link #theEdgeArraysAreRefusedWhenTheyDoNotFitTheBudget}'s
     * 1056-byte OLTP_VERTEX_BYTES graph-loading charge, so the budget here is sized only to the row headers common
     * to both paths - 11 nodes x 2 x 32 bytes/node = 704 bytes.
     */
    @Test
    void theWeightedAdjacencyRowHeadersAreRefusedOnTheCSRColumnarPathToo() {
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName("msa-budget-csr-view")
          .withVertexTypes("Node")
          .withEdgeTypes("LINK")
          .withEdgeProperties("w")
          .build();
      try {
        database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 500L);

        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        database.begin();
        try {
          final Vertex root;
          try (final ResultSet rs = database.query("sql", "SELECT FROM Node WHERE idx = 0")) {
            root = rs.next().getVertex().orElseThrow();
          }
          assertThatThrownBy(() -> new AlgoMinSpanningArborescence().execute(new Object[] { root, "LINK", "w" }, null, context))
              .as("weightedAdjacencyFromColumns must be priced the same way weightedAdjacencyFromRecords is")
              .hasStackTraceContaining("algo.msa(): the weighted adjacency list would need")
              .hasStackTraceContaining("0 edge entries")
              .hasStackTraceContaining(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY.getKey());
          assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
              .as("the view must actually back the call, or this test pins nothing beyond the OLTP path already covered above")
              .isEqualTo(true);
        } finally {
          database.rollback();
        }
      } finally {
        view.drop();
      }
    }

    @Test
    void aBudgetThatFitsLetsTheCallThrough() {
      // The counterweight: the reservation must not refuse a graph it can serve. 1056 (graph) + 704 (row headers)
      // + 120 (weightedAdjacency entries) + 160 (this procedure's own copy) = 2040, comfortable under 4 KB.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, 4096L);

      final List<Object> rows = drain(
          "MATCH (r:Node {idx: 0}) CALL algo.msa(r, 'LINK', 'w') YIELD source, weight, totalWeight RETURN source, weight, totalWeight");
      assertThat(rows).hasSize(EDGE_COUNT);
    }

    @Test
    void aDisabledBudgetPricesNothing() {
      // A negative limit means "no limit" throughout the budget, and it must reach this procedure's own edge-array
      // capacity too - a capacity computed by dividing a negative limit would refuse every graph instead of
      // accepting them all.
      database.getConfiguration().setValue(GlobalConfiguration.CYPHER_ALGO_MAX_WORKING_MEMORY, -1L);

      assertThat(drain("MATCH (r:Node {idx: 0}) CALL algo.msa(r, 'LINK', 'w') YIELD source RETURN source")).hasSize(EDGE_COUNT);
    }

    private List<Object> drain(final String query) {
      final List<Object> rows = new ArrayList<>();
      final ResultSet rs = database.query("opencypher", query);
      while (rs.hasNext())
        rows.add(rs.next());
      return rows;
    }
  }

  /**
   * Issue #6721: {@code GraphData.weightedAdjacency()}'s unit-weight branch summed {@code neighbors[i].length} across
   * every node to size its own memory reservation, even though {@code adjacency()}/{@code reserveAdjacency} had
   * already computed (and discarded) that same total while building the array. {@code AlgoMST}/{@code
   * AlgoMinSpanningArborescence} then each summed it again after {@code weightedAdjacency()} returned, to size their
   * own flat edge arrays - the same node-count-sized total recomputed two or three times per call.
   * <p>
   * The fix threads a {@code totalEntries} field through {@link WeightedAdjacency} itself, computed once - in the
   * same walk that already builds or prices the rows, on whichever of the three internal paths runs (unit-weight,
   * CSR-backed columnar edge properties, or OLTP edge records) - so callers read it instead of re-summing. These
   * tests pin that value against an independently-counted expected total on each of the three paths, and
   * {@code AlgoMST}/{@code AlgoMinSpanningArborescence}'s own tests (unchanged by this fix) continue to cover that
   * the edge arrays sized from it are the right size.
   */
  @Nested
  class WeightedAdjacencyTotalEntries {
    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6721-weightedadjacency-totalentries");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("N");
      database.getSchema().createEdgeType("E").createProperty("w", Type.DOUBLE);
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }

    private BasicCommandContext newContext() {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      return context;
    }

    /**
     * Asymmetric out-degrees (3 + 1 + 0 + 1 = 5 total OUT edges), so a totalEntries that accidentally summed IN
     * degree, or double-counted a row, would not go unnoticed.
     */
    private void buildGraph() {
      database.transaction(() -> {
        final Vertex a = database.newVertex("N").set("name", "A").save();
        final Vertex b = database.newVertex("N").set("name", "B").save();
        final Vertex c = database.newVertex("N").set("name", "C").save();
        final Vertex d = database.newVertex("N").set("name", "D").save();
        a.newEdge("E", b, true, new Object[] { "w", 1.0 }).save();
        a.newEdge("E", c, true, new Object[] { "w", 2.0 }).save();
        a.newEdge("E", d, true, new Object[] { "w", 3.0 }).save();
        b.newEdge("E", c, true, new Object[] { "w", 4.0 }).save();
        c.newEdge("E", d, true, new Object[] { "w", 5.0 }).save();
      });
    }

    @Test
    void unitWeightBranchReportsTheCorrectTotal() {
      buildGraph();
      final CommandContext context = newContext();
      final AlgoMST algo = new AlgoMST();
      final GraphData graph = algo.loadGraph(database, null, null, context);
      final WorkGuard guard = algo.newWorkGuard(context);

      final WeightedAdjacency weighted = graph.weightedAdjacency(guard, Vertex.DIRECTION.OUT, null);

      assertThat(weighted.totalEntries()).isEqualTo(sumOfRowLengths(weighted));
      assertThat(weighted.totalEntries()).isEqualTo(5L);
    }

    @Test
    void recordBackedBranchReportsTheCorrectTotal() {
      buildGraph();
      final CommandContext context = newContext();
      final AlgoMST algo = new AlgoMST();
      final GraphData graph = algo.loadGraph(database, null, null, context);
      final WorkGuard guard = algo.newWorkGuard(context);

      // No Graph Analytical View registered, so this falls onto the edge-record path (servesEdgeProperty has
      // nothing to consult) rather than the columnar one.
      final WeightedAdjacency weighted = graph.weightedAdjacency(guard, Vertex.DIRECTION.OUT, "w");

      assertThat(weighted.totalEntries()).isEqualTo(sumOfRowLengths(weighted));
      assertThat(weighted.totalEntries()).isEqualTo(5L);
    }

    @Test
    void columnarCsrBackedBranchReportsTheCorrectTotal() {
      buildGraph();
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName("issue-6721-view")
          .withVertexTypes("N")
          .withEdgeTypes("E")
          .withEdgeProperties("w")
          .build();
      try {
        final CommandContext context = newContext();
        final AlgoMST algo = new AlgoMST();
        final GraphData graph = algo.loadGraph(database, null, null, context);
        final WorkGuard guard = algo.newWorkGuard(context);

        assertThat(graph.isCSRBacked()).as("the view must actually back this call, or the columnar path is untested").isTrue();

        final WeightedAdjacency weighted = graph.weightedAdjacency(guard, Vertex.DIRECTION.OUT, "w");

        assertThat(weighted.totalEntries()).isEqualTo(sumOfRowLengths(weighted));
        assertThat(weighted.totalEntries()).isEqualTo(5L);
      } finally {
        view.drop();
      }
    }

    private static long sumOfRowLengths(final WeightedAdjacency weighted) {
      long sum = 0;
      for (final int[] row : weighted.neighbors())
        sum += row.length;
      return sum;
    }

    /**
     * Code-review follow-up on this same PR: {@code AlgoMST}/{@code AlgoMinSpanningArborescence} narrowed
     * {@code totalEntries()} to {@code int} with a plain cast and no bounds check - harmless in practice (the
     * pre-existing code had the identical ceiling, and the working-memory budget refuses long before a real graph
     * could reach anywhere near {@link Integer#MAX_VALUE} edges), but a silent wraparound is still the wrong failure
     * mode for a total that does not fit. Both procedures now narrow through the shared
     * {@link AbstractAlgoProcedure#checkedEdgeCount}, tested directly here since driving a real graph to
     * 2+ billion edges is not practical in a unit test.
     */
    @Test
    void checkedEdgeCountNarrowsAFittingTotal() {
      assertThat(AbstractAlgoProcedure.checkedEdgeCount("algo.mst", 4, 5L)).isEqualTo(5);
      assertThat(AbstractAlgoProcedure.checkedEdgeCount("algo.mst", 4, 0L)).isEqualTo(0);
      assertThat(AbstractAlgoProcedure.checkedEdgeCount("algo.mst", 4, (long) Integer.MAX_VALUE)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void checkedEdgeCountRefusesATotalThatDoesNotFitAnInt() {
      final long tooBig = (long) Integer.MAX_VALUE + 1;
      assertThatThrownBy(() -> AbstractAlgoProcedure.checkedEdgeCount("algo.mst", 4, tooBig))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("algo.mst")
          .hasMessageContaining(String.valueOf(tooBig))
          .hasMessageContaining(String.valueOf(Integer.MAX_VALUE));
    }
  }

  /**
   * Regression tests for issue #6296 - {@code algo.apsp} used to materialise every one of its {@code n² - n} rows
   * before the caller saw the first one.
   * <p>
   * #6263 bounded the <em>working set</em> of an {@code algo.*} call: {@code algo.apsp}'s {@code n x n} distance
   * matrix is now reserved against {@code arcadedb.cypher.algoMaxWorkingMemory} before it is allocated. That left
   * the larger allocation of the same call outside every budget. At the 64 MB floor of that setting the matrix
   * check admits about 2890 nodes, and a connected graph of 2890 nodes produced ~8.3 million {@code ResultInternal}
   * rows - each with a three-entry property map, all alive at once, well over a gigabyte - against the 64 MB the
   * budget had just finished enforcing on the matrix beside it. The budget did its job and the call still died, of
   * an allocation its error message would never have mentioned.
   * <p>
   * Nothing required the rows to exist together: Floyd-Warshall completes the matrix before the first row is
   * emitted, so the rows are a pure projection of it. They are now produced lazily, which makes the row-side
   * footprint O(1) and puts the decision of how many to hold where it belongs - with the consumer.
   * <p>
   * The allocation assertions carry {@code @Tag("performance")} rather than {@code @Tag("benchmark")}, and that is
   * deliberate: {@code benchmark} is one of the three lanes CI <em>excludes</em> from the normal build
   * ({@code -DexcludedGroups=slow,benchmark,vector}), and a regression guard that never runs guards nothing. These
   * are not throughput measurements - they read {@code ThreadMXBean}'s per-thread allocation counter, which no GC
   * and no concurrent test can move - so they belong in the default lane, where {@code performance} leaves them.
   * The tag is a label for the reader, matching {@code RidScoreMinHeapTest}; the partition CLAUDE.md describes is
   * untouched by it.
   */
  @Nested
  class APSPLazyRows {
    /**
     * A directed cycle: the cheapest shape in edges that makes every ordered pair reachable, so {@code n} edges
     * buy the full {@code n² - n} result set the issue is about.
     */
    private static final int CYCLE_NODES = 200;

    private Database database;

    @BeforeEach
    void setup() {
      final DatabaseFactory factory = new DatabaseFactory("./target/databases/test-issue-6296-apsp-lazy-rows");
      if (factory.exists())
        factory.open().drop();
      database = factory.create();
      database.getSchema().createVertexType("Node");
      database.getSchema().createEdgeType("LINK");

      database.transaction(() -> {
        final MutableVertex[] nodes = new MutableVertex[CYCLE_NODES];
        for (int i = 0; i < CYCLE_NODES; i++)
          nodes[i] = database.newVertex("Node").set("name", "N" + i).save();
        for (int i = 0; i < CYCLE_NODES; i++)
          nodes[i].newEdge("LINK", nodes[(i + 1) % CYCLE_NODES], true, (Object[]) null).save();
      });
    }

    @AfterEach
    void teardown() {
      if (database != null) {
        if (database.isTransactionActive())
          database.rollback();
        database.drop();
      }
    }

    @Test
    void everyReachablePairIsStillReturnedExactlyOnce() {
      // The counterweight to the laziness assertions: a stream that emits fewer rows, or the same row twice, is
      // not a cheaper version of the old behaviour. On a directed cycle every ordered pair (i != j) is reachable,
      // so the row count is exactly n² - n and each (source, target) appears once.
      final Set<String> pairs = new HashSet<>();
      int rows = 0;
      final ResultSet rs = database.query("opencypher",
          "CALL algo.apsp() YIELD source, target, distance RETURN source.name AS src, target.name AS tgt, distance");
      while (rs.hasNext()) {
        final Result result = rs.next();
        rows++;
        assertThat(pairs.add(result.getProperty("src") + "->" + result.getProperty("tgt")))
            .as("each ordered pair must be emitted once")
            .isTrue();
      }

      assertThat(rows).isEqualTo(CYCLE_NODES * CYCLE_NODES - CYCLE_NODES);
    }

    @Test
    void distancesAroundTheCycleAreUnchanged() {
      // Hop count around a directed cycle, which the eager and the lazy form must agree on: N0 -> Nk costs k.
      final ResultSet rs = database.query("opencypher", """
          CALL algo.apsp() YIELD source, target, distance \
          WITH source, target, distance WHERE source.name = 'N0' \
          RETURN target.name AS tgt, distance""");

      int seen = 0;
      while (rs.hasNext()) {
        final Result result = rs.next();
        final int target = Integer.parseInt(((String) result.getProperty("tgt")).substring(1));
        assertThat(((Number) result.getProperty("distance")).doubleValue())
            .as("distance from N0 to N" + target)
            .isEqualTo(target);
        seen++;
      }

      assertThat(seen).isEqualTo(CYCLE_NODES - 1);
    }

    @Test
    void aPartiallyReadStreamStillYieldsTheRestOnDemand() {
      // The shape of the fix without a measurement: one row is readable on its own, and the remaining
      // n² - n - 1 are still produced for a caller that goes on reading. A lazy stream that quietly drops its
      // tail would satisfy "cheap" and fail the procedure's contract.
      database.begin();
      try {
        final Iterator<Result> iterator = apspRows().iterator();

        assertThat(iterator.hasNext()).isTrue();
        final Result first = iterator.next();
        assertThat(first.<Object>getProperty("source")).isNotNull();
        assertThat(first.<Object>getProperty("target")).isNotNull();

        int remaining = 0;
        while (iterator.hasNext()) {
          iterator.next();
          remaining++;
        }
        assertThat(remaining).isEqualTo(CYCLE_NODES * CYCLE_NODES - CYCLE_NODES - 1);
      } finally {
        database.rollback();
      }
    }

    @Test
    void theRowsAreTheSameWhenTheGraphIsCSRBacked() {
      // The one thing making the rows lazy actually moves: `graph.getRID(i)` used to run inside execute() and
      // now runs as the row is read. On the OLTP-backed GraphData that is `vertices.get(i).getIdentity()`, an
      // in-memory field read that cannot care when it happens. On the CSR-backed one it goes through the
      // GraphAnalyticalView's node mapping, which takes a database reference - so that is the path worth
      // pinning, and it is the path the other tests here never take.
      //
      // Deferring it is safe because the stream is drained inside the same query execution that produced it
      // (CallStep keeps the iterator for a read procedure and the pipeline pulls it to completion before the
      // transaction is torn down), which is the contract every other lazily-returning algo.* procedure already
      // relies on - algo.mst, algo.fastrp and the rest have streamed from IntStream since before this change.
      final GraphAnalyticalView view = GraphAnalyticalView.builder(database)
          .withName("apsp-view")
          .withVertexTypes("Node")
          .withEdgeTypes("LINK")
          .build();
      try {
        database.begin();
        try {
          final BasicCommandContext context = new BasicCommandContext();
          context.setDatabase(database);
          final Stream<Result> rows = new AlgoAPSP().execute(new Object[0], null, context);

          assertThat(context.getVariable(CommandContext.CSR_ACCELERATED_VAR))
              .as("the view must actually be used, or this test pins the OLTP path a second time")
              .isEqualTo(true);

          // Same shape as the OLTP runs above: every ordered pair once, at its hop distance round the cycle.
          final Set<String> pairs = new HashSet<>();
          int fromFirstNode = 0;
          for (final Iterator<Result> it = rows.iterator(); it.hasNext(); ) {
            final Result row = it.next();
            final String source = nameOf(row.getProperty("source"));
            final String target = nameOf(row.getProperty("target"));
            assertThat(pairs.add(source + "->" + target)).isTrue();
            if ("N0".equals(source)) {
              assertThat(((Number) row.getProperty("distance")).doubleValue())
                  .as("distance from N0 to " + target)
                  .isEqualTo(Integer.parseInt(target.substring(1)));
              fromFirstNode++;
            }
          }

          assertThat(pairs).hasSize(CYCLE_NODES * CYCLE_NODES - CYCLE_NODES);
          assertThat(fromFirstNode).isEqualTo(CYCLE_NODES - 1);
        } finally {
          database.rollback();
        }
      } finally {
        view.drop();
      }
    }

    @Test
    @Tag("performance")
    void buildingTheStreamCostsTheMatrixRatherThanTheRows() {
      // The measurement the issue asks for, taken from the thread's own allocation counter so that neither a GC
      // nor another test's work can move it.
      //
      // Two phases of one call: execute(), which runs Floyd-Warshall and hands back the stream, and the drain
      // that reads the rows out of it. Eagerly, the rows were built inside execute() and the drain allocated
      // nothing worth counting - the two numbers below were the other way round. Lazily, execute() pays for the
      // n x n matrix (320 KB here) and the drain pays for the ~40 000 rows, so the drain must dominate.
      final com.sun.management.ThreadMXBean threads = threadAllocationBean();
      assumeTrue(threads != null, "JVM does not expose per-thread allocation counters");

      database.begin();
      try {
        // Warm the whole path past the interpreter, so the first measured round is not measuring class loading.
        for (int i = 0; i < 3; i++)
          assertThat(drain(apspRows())).isPositive();

        final Stream<Result>[] held = newStreamHolder();
        final long buildBytes = measure(threads, () -> held[0] = apspRows());
        final long drainBytes = measure(threads, () -> assertThat(drain(held[0])).isPositive());

        assertThat(drainBytes)
            .as("the rows are the bulk of an apsp call and must be paid for as they are read, not up front "
                + "(build=" + buildBytes + " bytes, drain=" + drainBytes + " bytes)")
            .isGreaterThan(4 * buildBytes);
      } finally {
        database.rollback();
      }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Calls the procedure directly rather than through Cypher: what is under measurement is where the rows are
     * built, and the planner and the CALL pipeline would add allocation to both phases that has nothing to do
     * with it. Requires an active transaction, like every other direct use of the graph API.
     */
    private Stream<Result> apspRows() {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);
      return new AlgoAPSP().execute(new Object[0], null, context);
    }

    /** The name of the vertex a `source`/`target` row property points at, whichever form the RID arrives in. */
    private String nameOf(final Object rid) {
      return ((RID) rid).asVertex().getString("name");
    }

    private static int drain(final Stream<Result> rows) {
      int count = 0;
      for (final Iterator<Result> it = rows.iterator(); it.hasNext(); ) {
        it.next();
        count++;
      }
      return count;
    }

    @SuppressWarnings("unchecked")
    private static Stream<Result>[] newStreamHolder() {
      return new Stream[1];
    }

    private static long measure(final com.sun.management.ThreadMXBean threads, final Runnable body) {
      final long id = Thread.currentThread().threadId();
      final long before = threads.getThreadAllocatedBytes(id);
      body.run();
      return threads.getThreadAllocatedBytes(id) - before;
    }

    private static com.sun.management.ThreadMXBean threadAllocationBean() {
      if (!(ManagementFactory.getThreadMXBean() instanceof final com.sun.management.ThreadMXBean bean))
        return null;
      if (!bean.isThreadAllocatedMemorySupported())
        return null;
      bean.setThreadAllocatedMemoryEnabled(true);
      return bean.isThreadAllocatedMemoryEnabled() ? bean : null;
    }
  }
}
