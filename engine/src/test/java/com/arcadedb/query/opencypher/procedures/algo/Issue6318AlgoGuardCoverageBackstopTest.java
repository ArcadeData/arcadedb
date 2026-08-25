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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Backstop for issue #6318 part 2 - {@code Issue6302AlgoGraphDrivenWorkGuardTest}'s parameterised table is the
 * only thing that notices an {@code algo.*} procedure shipping without a {@link com.arcadedb.query.sql.executor.WorkGuard}/
 * {@link com.arcadedb.graph.olap.WorkCheckpoint}, and only if somebody remembers to add a row for it. This test
 * instead discovers every procedure source file on disk, so a new one is caught automatically rather than by
 * memory.
 * <p>
 * The judgement call this cannot make mechanically is whether a procedure's dominant loop is actually superlinear
 * in the graph (needs a guard) or a single O(V + E) pass (does not - "one pass costs what loading the graph and
 * emitting the rows already cost", per {@code Issue6302AlgoGraphDrivenWorkGuardTest}). That judgement is recorded
 * once, here, as {@link #SINGLE_PASS_EXEMPT}: every procedure file that references no guard at all must be either
 * self-guarded (the common case: {@code WorkGuard}/{@code newWorkGuard}/{@code guard.check}/{@code guard::check}
 * somewhere in its source) or explicitly listed with a one-line reason. A new procedure that is neither fails this
 * test, which is the point - it turns "someone has to remember" into "the build says so".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6318AlgoGuardCoverageBackstopTest {

  /**
   * Procedures whose dominant work is a single O(V + E) pass (each vertex visited once, each edge read off that
   * vertex's own adjacency - never a second sweep of all vertices per vertex), a knob-bounded computation with no
   * graph-driven multiplier, or a query against a caller-supplied pair rather than the whole graph. Verified by
   * reading each file's {@code execute()} at the time this test was written (issue #6318); a procedure whose shape
   * changes to something superlinear must move off this list and gain a guard, not the other way round.
   */
  private static final Map<String, String> SINGLE_PASS_EXEMPT = Map.ofEntries(
      Map.entry("AlgoAdamicAdar.java", "one node's own adjacency plus each neighbour's own adjacency: O(deg + sum of neighbour degrees), not the graph"),
      Map.entry("AlgoArticulationPoints.java", "Tarjan-style single DFS, O(V + E)"),
      Map.entry("AlgoAssortativity.java", "two O(V + E) passes: degree computation, then one edge-endpoint-degree sum"),
      Map.entry("AlgoAStar.java", "single priority-queue search bounded by the path found, not the whole graph"),
      Map.entry("AlgoBFS.java", "single breadth-first traversal, O(V + E)"),
      Map.entry("AlgoBiconnectedComponents.java", "DFS forest over unvisited starts, each vertex/edge visited once overall, O(V + E)"),
      Map.entry("AlgoBipartiteCheck.java", "single two-colouring BFS/DFS, O(V + E)"),
      Map.entry("AlgoBridges.java", "single low-link DFS, O(V + E)"),
      Map.entry("AlgoCommonNeighbors.java", "one node's own adjacency plus one other node's own adjacency: O(deg(a) + deg(b))"),
      Map.entry("AlgoConductance.java", "a handful of O(V) / O(V + E) passes over precomputed degree and community arrays"),
      Map.entry("AlgoCycleDetection.java", "single DFS with a recursion-stack colour array, O(V + E)"),
      Map.entry("AlgoDegreeCentrality.java", "one degree lookup per node, O(V) (or O(1) per node CSR-backed, issue #6316)"),
      Map.entry("AlgoDFS.java", "single depth-first traversal, O(V + E)"),
      Map.entry("AlgoDijkstra.java", "single priority-queue shortest path, O((V + E) log V)"),
      Map.entry("AlgoDijkstraSingleSource.java", "single priority-queue search from one source, O((V + E) log V)"),
      Map.entry("AlgoGraphColoring.java", "single greedy pass, one node's own adjacency scanned once each, O(V + E)"),
      Map.entry("AlgoGraphSummary.java", "a handful of O(V) / O(V + E) aggregate passes over the loaded graph"),
      Map.entry("AlgoJaccardSimilarity.java", "one node's own adjacency plus one other node's own adjacency: O(deg(a) + deg(b))"),
      Map.entry("AlgoKCore.java", "bucket-queue peeling, each vertex removed once and each edge relaxed once, O(V + E)"),
      Map.entry("AlgoLongestPathDAG.java", "topological order plus one relaxation pass over each vertex's own adjacency, O(V + E)"),
      Map.entry("AlgoModularityScore.java", "a handful of O(V) / O(V + E) passes over precomputed degree and community arrays"),
      Map.entry("AlgoPreferentialAttachment.java", "O(1) product of two precomputed degrees"),
      Map.entry("AlgoResourceAllocation.java", "one node's own adjacency plus one other node's own adjacency: O(deg(a) + deg(b))"),
      Map.entry("AlgoSameCommunity.java", "O(1) lookup of two precomputed community labels"),
      Map.entry("AlgoSCC.java", "single Tarjan/Kosaraju pass, O(V + E)"),
      Map.entry("AlgoTopologicalSort.java", "single Kahn's-algorithm pass, O(V + E)"),
      Map.entry("AlgoTotalNeighbors.java", "one node's own adjacency plus one other node's own adjacency: O(deg(a) + deg(b))"),
      Map.entry("AlgoWCC.java", "single union-find pass over every edge, O(V + E)")
  );

  // Call-site patterns, not bare "WorkGuard"/"WorkCheckpoint" tokens (PR #6714 review round 11): the earlier,
  // looser regex would have passed a procedure that merely mentions "WorkGuard" in a comment without actually
  // wiring a checkpoint into its dominant loop. newWorkGuard(/guard.check cover a procedure guarding itself
  // directly; delegating to a checkpointed GraphAlgorithms kernel (the LCC CSR path) passes the guard as a
  // WorkCheckpoint-shaped lambda, which the guard::check method-reference pattern already matches.
  private static final Pattern GUARD_REFERENCE = Pattern.compile("newWorkGuard\\(|guard\\.check|guard::check");

  @Test
  void everyAlgoProcedureIsEitherSelfGuardedOrAJustifiedSinglePassExemption() throws IOException {
    final Path dir = algoProcedurePackageDir();

    final List<String> unguardedAndUnlisted = new ArrayList<>();
    try (Stream<Path> files = Files.list(dir)) {
      for (final Path file : files.sorted().toList()) {
        final String name = file.getFileName().toString();
        if (!name.startsWith("Algo") || !name.endsWith(".java") || name.equals("AbstractAlgoProcedure.java"))
          continue;

        final String source = Files.readString(file);
        if (GUARD_REFERENCE.matcher(source).find())
          continue; // self-guarded (directly, or by delegating to a GraphAlgorithms(..., checkpoint, ...) kernel)

        if (!SINGLE_PASS_EXEMPT.containsKey(name))
          unguardedAndUnlisted.add(name);
      }
    }

    assertThat(unguardedAndUnlisted)
        .as("every algo.* procedure must either reference a WorkGuard/WorkCheckpoint or be added to "
            + "SINGLE_PASS_EXEMPT with a one-line reason its dominant loop is not superlinear in the graph (issue #6318)")
        .isEmpty();
  }

  /**
   * Also pins the allow-list itself against drift: an entry for a file that no longer exists, or that has since
   * grown a guard of its own, is dead weight nobody will notice needs removing.
   */
  @Test
  void everyExemptEntryNamesAFileThatExistsAndIsStillUnguarded() throws IOException {
    final Path dir = algoProcedurePackageDir();
    final List<String> stale = new ArrayList<>();
    for (final Map.Entry<String, String> entry : SINGLE_PASS_EXEMPT.entrySet()) {
      final Path file = dir.resolve(entry.getKey());
      if (!Files.exists(file)) {
        stale.add(entry.getKey() + " (file no longer exists)");
        continue;
      }
      if (GUARD_REFERENCE.matcher(Files.readString(file)).find())
        stale.add(entry.getKey() + " (now references a guard - remove from the exemption list)");
    }
    assertThat(stale).as("SINGLE_PASS_EXEMPT entries that no longer apply").isEmpty();
  }

  private static Path algoProcedurePackageDir() {
    // Resolved relative to the module root, the way surefire runs (working directory = engine/), rather than
    // hard-coding an absolute path that would break outside this checkout.
    return Path.of("src/main/java/com/arcadedb/query/opencypher/procedures/algo");
  }
}
